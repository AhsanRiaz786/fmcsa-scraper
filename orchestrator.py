#!/usr/bin/env python3
"""
Main async orchestrator for high-throughput FMCSA scraping.
Implements producer-consumer pattern with queue-based batching.
"""

import asyncio
import aiohttp
import asyncpg
import pandas as pd
from typing import List, Set, Optional
from config import (
    DATABASE_DSN, CONCURRENCY, BATCH_SIZE, INPUT_FILE, TEST_MODE, TEST_LIMIT
)
from network import fetch_html
from parser import parse_fmcsa_response
from database import bulk_insert_batch

# Global queues
job_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
write_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

# Statistics
stats = {
    "scraped": 0,
    "failed": 0,
    "saved": 0,
    "errors": 0
}


async def load_todo_list() -> List[str]:
    """
    Load USDOT numbers to scrape.
    Reads CSV, checks DB for existing records, returns todo list.
    
    Returns:
        List of USDOT numbers (as strings) to scrape
    """
    print("Loading CSV file...")
    df = pd.read_csv(INPUT_FILE)
    all_usdots = set(df['DOT_NUMBER'].astype(str).unique())
    print(f"Total USDOT numbers in CSV: {len(all_usdots)}")
    
    # Check database for existing records
    print("Checking database for existing records...")
    try:
        conn = await asyncpg.connect(DATABASE_DSN)
        existing_rows = await conn.fetch("SELECT usdot_number::text FROM carriers")
        existing_set = {row['usdot_number'] for row in existing_rows}
        await conn.close()
        print(f"Found {len(existing_set)} existing records in database")
    except Exception as e:
        print(f"Warning: Could not check database: {e}")
        print("Proceeding with all records from CSV...")
        existing_set = set()
    
    todo_list = list(all_usdots - existing_set)
    print(f"Total: {len(all_usdots)}, Already Done: {len(existing_set)}, To Do: {len(todo_list)}")
    
    # In test mode, limit to first N records
    if TEST_MODE and TEST_LIMIT and len(todo_list) > TEST_LIMIT:
        print(f"Test mode: Limiting to first {TEST_LIMIT} records")
        todo_list = todo_list[:TEST_LIMIT]
    
    return todo_list


async def db_writer_worker():
    """
    Consumer: Pulls parsed data from write_queue and bulk inserts into DB.
    Uses ONE connection for all writes.
    """
    print("DB writer worker started")
    conn = await asyncpg.connect(DATABASE_DSN)
    batch = []
    
    try:
        while True:
            # Get item from queue
            record = await write_queue.get()
            
            # Check for sentinel (shutdown signal)
            if record is None:
                write_queue.task_done()
                # Flush remaining batch before shutdown
                if batch:
                    try:
                        count = await bulk_insert_batch(conn, batch)
                        stats["saved"] += count
                        print(f"DB writer: Final flush of {len(batch)} records to DB")
                        batch = []
                    except Exception as e:
                        print(f"DB WRITE ERROR (final flush): {e}")
                        stats["errors"] += 1
                break
            
            batch.append(record)
            write_queue.task_done()
            
            # Flush batch when it reaches BATCH_SIZE or queue is empty (drain mode)
            if len(batch) >= BATCH_SIZE or (write_queue.empty() and batch):
                try:
                    count = await bulk_insert_batch(conn, batch)
                    stats["saved"] += count
                    print(f"DB writer: Flushed {len(batch)} records to DB (Total saved: {stats['saved']})")
                    batch = []
                except Exception as e:
                    print(f"DB WRITE ERROR: {e}")
                    stats["errors"] += 1
                    # In production, you might want to dump failed batch to a retry file
                    batch = []  # Clear batch to avoid retrying same data
                    
    finally:
        await conn.close()
        print("DB writer worker stopped")


async def scraper_worker(worker_id: int):
    """
    Consumer: Pulls USDOT from job_queue, fetches, parses, pushes to write_queue.
    
    Args:
        worker_id: Unique identifier for this worker (for logging)
    """
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=500, limit_per_host=100)
    ) as session:
        while True:
            usdot = await job_queue.get()
            
            # Check for sentinel (shutdown signal)
            if usdot is None:
                job_queue.task_done()
                break
            
            try:
                html = await fetch_html(session, usdot)
                if html:
                    data = parse_fmcsa_response(html)
                    # Validate parsed data
                    if data and data.get("record_metadata", {}).get("usdot_number"):
                        await write_queue.put(data)
                        stats["scraped"] += 1
                        if stats["scraped"] % 100 == 0:
                            print(f"[Worker {worker_id}] Scraped {usdot} (Total: {stats['scraped']}, Failed: {stats['failed']}, Saved: {stats['saved']})")
                    else:
                        stats["failed"] += 1
                        print(f"[Worker {worker_id}] Failed to parse {usdot}")
                else:
                    stats["failed"] += 1
                    # In test mode, log first few failures with details
                    if TEST_MODE and stats["failed"] <= 5:
                        print(f"[Worker {worker_id}] Failed to fetch {usdot} - no HTML returned (check debug logs above)")
                    elif stats["failed"] % 100 == 0:
                        print(f"[Worker {worker_id}] Failed to fetch {usdot} (Total failed: {stats['failed']})")
            except Exception as e:
                stats["failed"] += 1
                stats["errors"] += 1
                print(f"[Worker {worker_id}] Error processing {usdot}: {e}")
            
            job_queue.task_done()


async def progress_monitor():
    """Periodically print progress statistics."""
    mode_indicator = "[TEST MODE] " if TEST_MODE else ""
    while True:
        await asyncio.sleep(10)
        print(f"{mode_indicator}Progress: Scraped={stats['scraped']}, Failed={stats['failed']}, Saved={stats['saved']}, Errors={stats['errors']}, "
              f"Job Queue={job_queue.qsize()}, Write Queue={write_queue.qsize()}")


async def main():
    """Main orchestration function."""
    print("=" * 60)
    print("FMCSA High-Throughput Async Scraper")
    if TEST_MODE:
        print("*** TEST MODE ENABLED (No Proxy, Low Concurrency) ***")
    print("=" * 60)
    
    # 1. Load todo list
    todo_list = await load_todo_list()
    
    if not todo_list:
        print("No records to scrape. Exiting.")
        return
    
    # 2. Start DB writer worker
    print("Starting DB writer worker...")
    writer_task = asyncio.create_task(db_writer_worker())
    
    # 3. Start progress monitor
    monitor_task = asyncio.create_task(progress_monitor())
    
    # 4. Start scraper workers
    mode_note = " (test mode)" if TEST_MODE else ""
    print(f"Starting {CONCURRENCY} scraper workers{mode_note}...")
    worker_tasks = [asyncio.create_task(scraper_worker(i)) for i in range(CONCURRENCY)]
    
    # 5. Feed the job queue
    print(f"Feeding {len(todo_list)} jobs to queue...")
    for usdot in todo_list:
        await job_queue.put(usdot)
    
    print("All jobs queued. Waiting for workers to complete...")
    
    # 6. Send shutdown signals to scraper workers
    for _ in range(CONCURRENCY):
        await job_queue.put(None)
    
    # 7. Wait for all scraping to complete
    await job_queue.join()
    print("All scraping tasks completed. Waiting for workers to finish...")
    
    # 8. Wait for all scraper workers to finish
    await asyncio.gather(*worker_tasks)
    print("All scraper workers stopped.")
    
    # 9. Cancel progress monitor
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass
    
    # 10. Send shutdown signal to DB writer
    await write_queue.put(None)
    await write_queue.join()
    
    # 11. Wait for DB writer to finish
    await writer_task
    print("DB writer stopped.")
    
    # 12. Final statistics
    print("=" * 60)
    print("Final Statistics:")
    print(f"  Scraped: {stats['scraped']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Saved to DB: {stats['saved']}")
    print(f"  Errors: {stats['errors']}")
    print("=" * 60)
    print("Done!")


if __name__ == "__main__":
    import sys
    import os
    
    # Check for --test flag to override TEST_MODE
    if "--test" in sys.argv or "-t" in sys.argv:
        os.environ["TEST_MODE"] = "True"
        # Reload config module to pick up the change
        import importlib
        import config
        importlib.reload(config)
        # Re-import the updated values
        from config import TEST_MODE, CONCURRENCY
        print(f"Test mode enabled via command-line flag (Concurrency: {CONCURRENCY})")
    
    # Try to use uvloop for better performance (Linux/Mac only)
    try:
        import uvloop
        uvloop.install()
        print("Using uvloop for enhanced performance")
    except ImportError:
        print("uvloop not available, using default event loop")
        pass
    
    asyncio.run(main())

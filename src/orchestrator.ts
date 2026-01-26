/**
 * Main async orchestrator for high-throughput FMCSA scraping.
 * Implements producer-consumer pattern with queue-based batching.
 */

import { Pool } from 'pg';
import PQueue from 'p-queue';
import * as fs from 'fs';
import * as readline from 'readline';
import { createReadStream } from 'fs';
import { parseHtmlToSnapshot } from './parser';
import { fetchHtml } from './network';
import { bulkInsertBatch } from './database';
import {
  DB_CONFIG,
  CONCURRENCY,
  BATCH_SIZE,
  INPUT_FILE,
  TEST_MODE,
  TEST_LIMIT,
  PROXY_URL,
  PROXY_USER_BASE,
  PROXY_PASS,
  PROXY_MAX_CONCURRENCY,
  MAX_RETRIES,
  REQUEST_TIMEOUT,
} from './config';
import type { Snapshot } from './types/carrier.types';

// Statistics
const stats = {
  scraped: 0,
  failed: 0,
  saved: 0,
  errors: 0,
};

/**
 * Load USDOT numbers to scrape.
 * Reads CSV, checks DB for existing records, returns todo list.
 */
async function loadTodoList(pool: Pool): Promise<string[]> {
  console.log('Loading CSV file...');
  const allUsdots = new Set<string>();

  // Read CSV line by line
  const fileStream = createReadStream(INPUT_FILE);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let isFirstLine = true;
  for await (const line of rl) {
    // Skip header if present
    if (isFirstLine && line.trim().toUpperCase() === 'DOT_NUMBER') {
      isFirstLine = false;
      continue;
    }
    isFirstLine = false;

    const usdot = line.trim();
    if (usdot && !isNaN(Number(usdot))) {
      allUsdots.add(usdot);
    }
  }

  console.log(`Total USDOT numbers in CSV: ${allUsdots.size}`);

  // Check database for existing records
  console.log('Checking database for existing records...');
  let existingSet = new Set<string>();
  try {
    const result = await pool.query('SELECT usdot_number::text FROM snapshots');
    existingSet = new Set(result.rows.map((row) => row.usdot_number));
    console.log(`Found ${existingSet.size} existing records in database`);
  } catch (error) {
    console.log(`Warning: Could not check database: ${error}`);
    console.log('Proceeding with all records from CSV...');
  }

  let todoList = Array.from(allUsdots).filter((usdot) => !existingSet.has(usdot));
  console.log(`Total: ${allUsdots.size}, Already Done: ${existingSet.size}, To Do: ${todoList.length}`);

  // In test mode, limit to first N records
  if (TEST_MODE && TEST_LIMIT && todoList.length > TEST_LIMIT) {
    console.log(`Test mode: Limiting to first ${TEST_LIMIT} records`);
    todoList = todoList.slice(0, TEST_LIMIT);
  }

  return todoList;
}

/**
 * DB writer: Processes write batch and flushes to database.
 * Uses a shared batch array that gets flushed periodically.
 */
let writeBatch: Snapshot[] = [];
let dbWriterStop: (() => void) | null = null;

async function dbWriterWorker(pool: Pool): Promise<void> {
  console.log('DB writer worker started');
  
  let lastFlushTime = Date.now();
  let flushing = false;
  let stopped = false;
  const FLUSH_INTERVAL_MS = TEST_MODE ? 2000 : 5000;
  
  const flushBatch = async (reason: string = 'size threshold') => {
    if (flushing || writeBatch.length === 0) return;
    flushing = true;
    // Copy and clear immediately so we never flush the same records twice
    const toInsert = writeBatch.splice(0, writeBatch.length);
    const batchSize = toInsert.length;
    try {
      const count = await bulkInsertBatch(pool, toInsert);
      stats.saved += count;
      lastFlushTime = Date.now();
    } catch (error) {
      console.log(`DB WRITE ERROR: ${error}`);
      console.log(`DB WRITE ERROR: Failed to save ${batchSize} records`);
      if (error instanceof Error) {
        console.log(`DB WRITE ERROR: ${error.message}`);
        if (error.stack) {
          console.log(`DB WRITE ERROR: Stack: ${error.stack.split('\n').slice(0, 5).join('\n')}`);
        }
      }
      stats.errors += 1;
      lastFlushTime = Date.now();
      // Optionally re-queue failed records? For now we drop them to avoid loops.
    } finally {
      flushing = false;
    }
  };

  const flushInterval = setInterval(async () => {
    const now = Date.now();
    const timeSinceLastFlush = now - lastFlushTime;
    if (writeBatch.length >= BATCH_SIZE) {
      await flushBatch('batch size reached');
    } else if (writeBatch.length > 0 && timeSinceLastFlush >= FLUSH_INTERVAL_MS) {
      await flushBatch('time interval');
    }
  }, 1000);

  // Handle graceful shutdown
  const handleShutdown = async () => {
    if (stopped) return;
    stopped = true;
    clearInterval(flushInterval);
    while (true) {
      if (flushing) {
        await new Promise((r) => setTimeout(r, 50));
        continue;
      }
      if (writeBatch.length === 0) break;
      await flushBatch('shutdown');
    }
    console.log('DB writer worker stopped');
  };

  // Return a promise that resolves when we should stop
  return new Promise<void>((resolve) => {
    dbWriterStop = async () => {
      await handleShutdown();
      resolve();
    };
    
    // Also handle process termination
    process.on('SIGINT', async () => {
      await handleShutdown();
      resolve();
    });
    process.on('SIGTERM', async () => {
      await handleShutdown();
      resolve();
    });
  });
}

/**
 * Process a single USDOT number: fetch, parse, and add to write batch.
 */
async function processUsdot(usdot: string): Promise<void> {
  try {
    const html = await fetchHtml(usdot);
    if (html) {
      const snapshot = parseHtmlToSnapshot(html);
      // Validate parsed data
      if (snapshot && snapshot.usdot_number) {
        // Add to write batch (will be flushed by DB writer)
        writeBatch.push(snapshot);
        stats.scraped += 1;
        
        if (TEST_MODE && stats.scraped <= 10) {
          console.log(`[PARSER] USDOT ${usdot}: Parsed successfully, added to batch (batch size: ${writeBatch.length})`);
        }
        
        if (stats.scraped % 1000 === 0) {
          const processed = stats.scraped + stats.failed;
          console.log(
            `Milestone: Scraped=${stats.scraped} | Failed=${stats.failed} | Processed=${processed} | Saved=${stats.saved} | Batch=${writeBatch.length}`
          );
        }
      } else {
        stats.failed += 1;
        if (TEST_MODE) {
          console.log(`[PARSER] USDOT ${usdot}: Failed to parse - snapshot is null or missing usdot_number`);
        }
      }
    } else {
      stats.failed += 1;
      if (TEST_MODE && stats.failed <= 5) {
        console.log(`Failed to fetch ${usdot} - no HTML returned`);
      } else if (stats.failed % 1000 === 0) {
        console.log(`Failed to fetch (Total failed: ${stats.failed})`);
      }
    }
  } catch (error) {
    stats.failed += 1;
    stats.errors += 1;
    // Only log errors in test mode or every 100 errors
    if (TEST_MODE || stats.errors % 100 === 0) {
      console.log(`[ERROR] Error processing ${usdot}: ${error}`);
    }
  }
}

/**
 * Progress monitor: Periodically print progress statistics.
 */
function startProgressMonitor(): NodeJS.Timeout {
  const modeIndicator = TEST_MODE ? '[TEST MODE] ' : '';
  return setInterval(() => {
    const processed = stats.scraped + stats.failed;
    console.log(
      `${modeIndicator}Progress: Processed=${processed} | Scraped=${stats.scraped} | Failed=${stats.failed} | Saved=${stats.saved} | Errors=${stats.errors}`
    );
  }, 30000); // Reduced from 10s to 30s
}

/**
 * Main orchestration function.
 */
async function main(): Promise<void> {
  console.log('='.repeat(60));
  console.log('FMCSA High-Throughput Async Scraper (TypeScript)');
  if (TEST_MODE) {
    console.log('*** TEST MODE ENABLED (No Proxy, Low Concurrency) ***');
  }
  console.log('='.repeat(60));
  console.log('Network Configuration:');
  console.log(`  Request Timeout: ${REQUEST_TIMEOUT}ms`);
  console.log(`  Max Retries: ${MAX_RETRIES}`);
  console.log(`  Concurrency: ${CONCURRENCY}${PROXY_URL && !TEST_MODE ? ` (proxy; max ${PROXY_MAX_CONCURRENCY}, set PROXY_MAX_CONCURRENCY to override)` : ''}`);
  console.log(`  Proxy Enabled: ${!TEST_MODE && PROXY_URL && PROXY_USER_BASE && PROXY_PASS ? 'Yes' : 'No'}`);
  if (PROXY_URL) {
    console.log(`  Proxy URL: ${PROXY_URL}`);
    console.log(`  Proxy User Base: ${PROXY_USER_BASE ? PROXY_USER_BASE.substring(0, 20) + '...' : 'Not set'}`);
  } else {
    console.log(`  Proxy URL: Not configured`);
  }
  console.log('='.repeat(60));

  // Create database pool
  const pool = new Pool(DB_CONFIG);
  pool.on('error', (err) => {
    console.error('Unexpected database error:', err);
  });

  try {
    // 1. Load todo list
    const todoList = await loadTodoList(pool);

    if (todoList.length === 0) {
      console.log('No records to scrape. Exiting.');
      await pool.end();
      return;
    }

    // 2. Create job queue
    const jobQueue = new PQueue({ concurrency: CONCURRENCY });

    // 3. Start progress monitor
    const monitorInterval = startProgressMonitor();

    // 4. Start DB writer (monitors writeBatch and flushes)
    const dbWriterPromise = dbWriterWorker(pool);

    // 5. Feed jobs to queue and process them
    console.log(`Feeding ${todoList.length} jobs to queue...`);
    console.log(`Starting ${CONCURRENCY} scraper workers${TEST_MODE ? ' (test mode)' : ''}...`);

    // Add all jobs without collecting promises (avoids "Too many elements" in Promise.all)
    for (const usdot of todoList) {
      jobQueue.add(() => processUsdot(usdot));
    }

    // Wait for queue to drain
    await jobQueue.onIdle();
    console.log('All scraping tasks completed.');

    // Stop progress monitor
    clearInterval(monitorInterval);

    // Signal DB writer to stop and flush final batch
    if (dbWriterStop) {
      dbWriterStop();
    }
    await dbWriterPromise;

    // 6. Final statistics
    const finalProcessed = stats.scraped + stats.failed;
    console.log('='.repeat(60));
    console.log('Final Statistics:');
    console.log(`  Processed: ${finalProcessed} (Scraped: ${stats.scraped}, Failed: ${stats.failed})`);
    console.log(`  Saved to DB: ${stats.saved}`);
    console.log(`  Errors: ${stats.errors}`);
    console.log('='.repeat(60));
    console.log('Done!');
  } finally {
    await pool.end();
  }
}

// Run main function
if (require.main === module) {
  // Check for --test flag
  if (process.argv.includes('--test') || process.argv.includes('-t')) {
    process.env.TEST_MODE = 'true';
    console.log('Test mode enabled via command-line flag');
  }

  main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

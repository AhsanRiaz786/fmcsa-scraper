# FMCSA High-Throughput Async Scraper

High-performance async scraper for FMCSA SAFER database with support for 100+ requests per second.

## Architecture

The system uses a **3-tier async producer-consumer pattern**:

1. **Feeder**: Reads CSV, filters already-scraped records, feeds job queue
2. **Scraper Workers** (200-500 concurrent): Pull from job queue, fetch HTML via proxy, parse, push to write queue
3. **Database Writer** (single worker): Batches records from write queue, bulk inserts using 1 DB connection

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure `.env` file:
```env
DB_NAME=fmcsa_safer
DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_AVAILABLE=True

PROXY_URL=your_proxy_url
PROXY_USER_BASE=your_user_base
PROXY_PASS=your_proxy_password

CONCURRENCY=200
BATCH_SIZE=1000
MAX_RETRIES=3
REQUEST_TIMEOUT=15
```

3. Ensure PostgreSQL database is set up with the schema (see plan for SQL schema)

## Usage

### Production (High-Throughput)

Run the async orchestrator:
```bash
python orchestrator.py
```

### Test Mode (No Proxy, Low Concurrency)

For testing without proxies and with lower concurrency, you have two options:

**Option 1: Command-line flag (recommended for quick testing)**
```bash
python orchestrator.py --test
# or
python orchestrator.py -t
```

**Option 2: Environment variable**
1. Set `TEST_MODE=True` in `.env`:
```env
TEST_MODE=True
```

2. Run the orchestrator:
```bash
python orchestrator.py
```

Test mode will:
- Skip proxy usage (direct connections to FMCSA)
- Use concurrency of 10 (instead of 200) - can be overridden with CONCURRENCY env var
- Still save to database (if configured)
- Display "TEST MODE ENABLED" banner when starting

This will:
- Load USDOT numbers from `dot_numbers.csv`
- Check database for existing records (resume capability)
- Start 200 concurrent scraper workers (configurable via `CONCURRENCY`)
- Batch insert to database in chunks of 1000 (configurable via `BATCH_SIZE`)
- Display progress statistics every 10 seconds



## Performance Tuning

- **CONCURRENCY**: Number of concurrent scraper workers. Start with 200, increase if CPU allows.
- **BATCH_SIZE**: Database write batch size. 1000 is optimal for most cases.
- **REQUEST_TIMEOUT**: HTTP request timeout in seconds. 15 is recommended.

## Monitoring

The orchestrator prints progress every 10 seconds:
- Scraped: Number of successfully parsed records
- Failed: Number of failed requests/parses
- Saved: Number of records written to database
- Errors: Number of errors encountered
- Queue sizes: Current job queue and write queue sizes

## Resume Capability

The system automatically checks the database for existing records at startup and skips them. You can safely stop and restart the scraper - it will continue from where it left off.

## Files

- `orchestrator.py` - Main async coordinator (use for production)
- `main.py` - Legacy synchronous version (use for testing)
- `config.py` - Configuration loader
- `network.py` - Async HTTP client with proxy rotation
- `parser.py` - HTML parsing logic
- `database.py` - Batch database operations

## Notes

- Uses `uvloop` for enhanced performance on Linux/Mac (optional)
- Proxy rotation: Each request gets a unique session ID to force IP rotation
- Database connections: Only 1 connection used for all writes (respects 20-connection limit)
- Error handling: Failed batches are logged, network errors are retried automatically

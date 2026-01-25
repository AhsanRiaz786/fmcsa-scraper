# FMCSA High-Throughput Async Scraper (TypeScript/Node.js)

High-performance async scraper for FMCSA SAFER database with support for 100+ requests per second.

## Architecture

The system uses a **3-tier async producer-consumer pattern**:

1. **Feeder**: Reads CSV, filters already-scraped records, feeds job queue
2. **Scraper Workers** (200-500 concurrent): Pull from job queue, fetch HTML via proxy, parse, push to write queue
3. **Database Writer** (single worker): Batches records from write queue, bulk inserts using 1 DB connection

## Setup

1. Install dependencies:
```bash
npm install
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
PROXY_USER_BASE=your_proxy_user_base
PROXY_PASS=your_proxy_password

CONCURRENCY=200
BATCH_SIZE=1000
MAX_RETRIES=3
REQUEST_TIMEOUT=15
TEST_MODE=False
TEST_LIMIT=100
```

3. Set up PostgreSQL database:
```bash
psql -U your_user -d fmcsa_safer -f src/schema.sql
```

## Usage

### Production (High-Throughput)

Build and run:
```bash
npm run build
npm start
```

Or run directly with ts-node:
```bash
npm run dev
```

This will:
- Load USDOT numbers from `dot_numbers.csv`
- Check database for existing records (resume capability)
- Start 200 concurrent scraper workers (configurable via `CONCURRENCY`)
- Batch insert to database in chunks of 1000 (configurable via `BATCH_SIZE`)
- Display progress statistics every 10 seconds

### Test Mode (No Proxy, Low Concurrency)

For testing without proxies and with lower concurrency:

**Option 1: Command-line flag**
```bash
npm run dev -- --test
# or
npm run dev -- -t
```

**Option 2: Environment variable**
Set in `.env`:
```env
TEST_MODE=True
```

Test mode will:
- Skip proxy usage (direct connections to FMCSA)
- Use concurrency of 10 (instead of 200)
- Process only first N records (set via `TEST_LIMIT`, default 100)
- Still save to database (if configured)
- Display "TEST MODE ENABLED" banner when starting

**Note:** FMCSA blocks direct connections (returns 403). Test mode without proxy will show 403 errors.

## Performance Tuning

- **CONCURRENCY**: Number of concurrent scraper workers. Start with 200, increase if CPU allows.
- **BATCH_SIZE**: Database write batch size. 1000 is optimal for most cases.
- **REQUEST_TIMEOUT**: HTTP request timeout in milliseconds. 15000 (15s) is recommended.

## Monitoring

The orchestrator prints progress every 10 seconds:
- Scraped: Number of successfully parsed records
- Failed: Number of failed requests/parses
- Saved: Number of records written to database
- Errors: Number of errors encountered

## Resume Capability

The system automatically checks the database for existing records at startup and skips them. You can safely stop and restart the scraper - it will continue from where it left off.

## Files

- `src/orchestrator.ts` - Main async coordinator (use for production)
- `src/config.ts` - Configuration loader
- `src/network.ts` - Async HTTP client with proxy rotation
- `src/parser.ts` - HTML parsing logic (maps to carrier.types.ts)
- `src/database.ts` - Batch database operations
- `src/types/carrier.types.ts` - TypeScript type definitions
- `src/schema.sql` - PostgreSQL schema (flattened structure)

## TypeScript Types

The scraper outputs data matching the exact `Snapshot` type from `carrier.types.ts`:
- All fields match the type definitions
- Arrays and nested objects are properly typed
- Only non-empty inspection summaries are included (per type comments)
- Only non-empty safety ratings are included (per type comments)

## Notes

- Uses `p-queue` for concurrency control
- Proxy rotation: Each request gets a unique session ID to force IP rotation
- Database connections: Uses connection pooling (respects connection limits)
- Error handling: Failed batches are logged, network errors are retried automatically

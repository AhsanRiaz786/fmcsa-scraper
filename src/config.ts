/**
 * Centralized configuration loader for FMCSA scraper.
 * Reads from .env file and validates required settings.
 */

import * as dotenv from 'dotenv';
import * as os from 'os';

// Load environment variables
dotenv.config();

// Database configuration
const DB_NAME = process.env.DB_NAME;
const DB_USER = process.env.DB_USER;
const DB_PASSWORD = process.env.DB_PASSWORD;
const DB_HOST = process.env.DB_HOST || 'localhost';
const DB_PORT = process.env.DB_PORT || '5432';
export const DB_AVAILABLE = process.env.DB_AVAILABLE?.toLowerCase() === 'true';

// Build database connection config (pg library uses separate params, not DSN)
export const DB_CONFIG = {
  host: DB_HOST,
  port: parseInt(DB_PORT, 10),
  database: DB_NAME,
  user: DB_USER,
  password: DB_PASSWORD,
};

// Also provide DSN for compatibility
export const DATABASE_DSN = DB_NAME && DB_USER && DB_PASSWORD
  ? `postgresql://${encodeURIComponent(DB_USER)}:${encodeURIComponent(DB_PASSWORD)}@${DB_HOST}:${DB_PORT}/${DB_NAME}`
  : undefined;

// Proxy configuration
export const PROXY_URL = process.env.PROXY_URL;
export const PROXY_USER_BASE = process.env.PROXY_USER_BASE;
export const PROXY_PASS = process.env.PROXY_PASS;
/** Session time in seconds for SOCKS5 sticky IP (e.g. 30). */
export const PROXY_SESS_TIME = parseInt(process.env.PROXY_SESS_TIME || '30', 10);
/** Max concurrency when using proxy; cap to avoid ECONNRESET (default 25). */
export const PROXY_MAX_CONCURRENCY = parseInt(process.env.PROXY_MAX_CONCURRENCY || '25', 10);

// Test mode (disables proxy, uses low concurrency)
export const TEST_MODE = process.env.TEST_MODE?.toLowerCase() === 'true';

// Performance settings
const rawConcurrency = parseInt(process.env.CONCURRENCY || '200', 10);
const useProxy = !TEST_MODE && PROXY_URL && PROXY_USER_BASE && PROXY_PASS;
export const CONCURRENCY = TEST_MODE
  ? 1
  : useProxy
    ? Math.min(rawConcurrency, PROXY_MAX_CONCURRENCY)
    : rawConcurrency;

export const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '1000', 10);
export const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '3', 10);
export const REQUEST_TIMEOUT = parseInt(process.env.REQUEST_TIMEOUT || '15000', 10);

// Input file
export const INPUT_FILE = process.env.INPUT_FILE || 'dot_numbers.csv';

// Test mode limit (only process first N records in test mode)
export const TEST_LIMIT = TEST_MODE
  ? parseInt(process.env.TEST_LIMIT || '100', 10)
  : undefined;

// Validation
export function validateConfig(): void {
  const errors: string[] = [];

  if (!DB_NAME || !DB_USER || !DB_PASSWORD) {
    errors.push('Database configuration incomplete (DB_NAME, DB_USER, DB_PASSWORD required)');
  }

  // Only require proxy settings if not in test mode
  if (!TEST_MODE) {
    if (!PROXY_URL) {
      errors.push('PROXY_URL not set');
    }
    if (!PROXY_USER_BASE) {
      errors.push('PROXY_USER_BASE not set');
    }
    if (!PROXY_PASS) {
      errors.push('PROXY_PASS not set');
    }
  }

  if (errors.length > 0) {
    throw new Error(`Configuration errors:\n${errors.map(e => `  - ${e}`).join('\n')}`);
  }
}

// Validate on import (can be disabled for testing)
if (require.main !== module) {
  try {
    validateConfig();
  } catch (error) {
    // Allow import to succeed even if config is incomplete (for testing)
  }
}

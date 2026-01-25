/**
 * Async HTTP client with proxy rotation and retry logic.
 * Handles FMCSA API requests with intelligent retry strategies.
 */

import axios, { AxiosError } from 'axios';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { PROXY_URL, PROXY_USER_BASE, PROXY_PASS, MAX_RETRIES, REQUEST_TIMEOUT, TEST_MODE } from './config';

/**
 * Generate a unique session ID for every request to force IP rotation.
 * Format: {PROXY_USER_BASE}-{random8chars}:{PROXY_PASS}
 */
function getRandomSessionId(): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < 8; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return `${PROXY_USER_BASE}-${result}`;
}

/**
 * Get proxy agent for axios request.
 * Returns null if no proxy should be used (test mode).
 */
function getProxyAgent(): HttpsProxyAgent<string> | null {
  if (TEST_MODE || !PROXY_URL || !PROXY_USER_BASE || !PROXY_PASS) {
    return null;
  }
  
  const sessionUser = getRandomSessionId();
  // PROXY_URL is like "http://aus.360s5.com:3600"
  // We need: "http://user:pass@host:port"
  const proxyHost = PROXY_URL.replace(/^https?:\/\//, '');
  const proxyUrl = `http://${sessionUser}:${PROXY_PASS}@${proxyHost}`;
  return new HttpsProxyAgent(proxyUrl);
}

/**
 * Fetch HTML from FMCSA SAFER database for a given USDOT number.
 * 
 * @param usdot - USDOT number as string
 * @returns HTML content as string, or null if request failed or 404
 */
export async function fetchHtml(usdot: string): Promise<string | null> {
  const url = 'https://safer.fmcsa.dot.gov/query.asp';
  
  const payload = new URLSearchParams({
    searchtype: 'ANY',
    query_type: 'queryCarrierSnapshot',
    query_param: 'USDOT',
    query_string: usdot,
  });

  const headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  };

  const proxyAgent = getProxyAgent();
  const httpsAgent = proxyAgent || undefined;

  // Retry loop
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const response = await axios.post(url, payload.toString(), {
        headers,
        httpsAgent,
        timeout: REQUEST_TIMEOUT,
        validateStatus: (status) => status < 500, // Don't throw on 4xx
      });

      const status = response.status;
      
      if (status === 200) {
        return response.data;
      } else if (status === 404) {
        // Valid request, but no data found - don't retry
        return null;
      } else if (status === 403 || status === 429) {
        // Banned/throttled - in test mode, log and return None
        if (TEST_MODE && attempt === 0) {
          console.log(`[DEBUG] USDOT ${usdot}: Got ${status} (blocked/throttled). FMCSA may block direct connections.`);
        }
        if (attempt < MAX_RETRIES - 1) {
          await new Promise(resolve => setTimeout(resolve, 1000)); // Brief delay before retry
          continue;
        }
        return null;
      } else if (status >= 500) {
        // Server error - retry
        if (TEST_MODE && attempt === 0) {
          console.log(`[DEBUG] USDOT ${usdot}: Got ${status} (server error)`);
        }
        if (attempt < MAX_RETRIES - 1) {
          await new Promise(resolve => setTimeout(resolve, 1000));
          continue;
        }
        return null;
      } else {
        // Other status codes - log in test mode
        if (TEST_MODE && attempt === 0) {
          console.log(`[DEBUG] USDOT ${usdot}: Got unexpected status ${status}`);
        }
        return null;
      }
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const axiosError = error as AxiosError;
        
        // Timeout
        if (axiosError.code === 'ECONNABORTED' || axiosError.message.includes('timeout')) {
          if (TEST_MODE && attempt === 0) {
            console.log(`[DEBUG] USDOT ${usdot}: Request timeout`);
          }
          if (attempt < MAX_RETRIES - 1) {
            await new Promise(resolve => setTimeout(resolve, 1000));
            continue;
          }
          return null;
        }
        
        // Client errors (connection issues, etc.)
        if (TEST_MODE && attempt === 0) {
          console.log(`[DEBUG] USDOT ${usdot}: Client error: ${axiosError.code || axiosError.message}`);
        }
        if (attempt < MAX_RETRIES - 1) {
          await new Promise(resolve => setTimeout(resolve, 1000));
          continue;
        }
        return null;
      }
      
      // Other exceptions - retry
      if (TEST_MODE && attempt === 0) {
        console.log(`[DEBUG] USDOT ${usdot}: Unexpected error: ${error instanceof Error ? error.message : String(error)}`);
      }
      if (attempt < MAX_RETRIES - 1) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }
      return null;
    }
  }

  return null; // Failed after all retries
}

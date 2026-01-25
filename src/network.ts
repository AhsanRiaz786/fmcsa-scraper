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
    if (TEST_MODE) {
      console.log(`[NETWORK] Proxy disabled: TEST_MODE is enabled`);
    } else {
      console.log(`[NETWORK] Proxy disabled: Missing config (PROXY_URL=${!!PROXY_URL}, PROXY_USER_BASE=${!!PROXY_USER_BASE}, PROXY_PASS=${!!PROXY_PASS})`);
    }
    return null;
  }
  
  const sessionUser = getRandomSessionId();
  // PROXY_URL is like "http://aus.360s5.com:3600"
  // We need: "http://user:pass@host:port"
  const proxyHost = PROXY_URL.replace(/^https?:\/\//, '');
  const proxyUrl = `http://${sessionUser}:${PROXY_PASS}@${proxyHost}`;
  console.log(`[NETWORK] Using proxy: ${proxyHost} with session: ${sessionUser.substring(0, 20)}...`);
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

  // Log request configuration
  console.log(`[NETWORK] USDOT ${usdot}: Starting request (timeout=${REQUEST_TIMEOUT}ms, maxRetries=${MAX_RETRIES}, proxy=${!!httpsAgent})`);

  // Retry loop
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    const attemptStartTime = Date.now();
    const attemptNum = attempt + 1;
    
    if (attemptNum > 1) {
      console.log(`[NETWORK] USDOT ${usdot}: Retry attempt ${attemptNum}/${MAX_RETRIES}`);
    }

    try {
      console.log(`[NETWORK] USDOT ${usdot}: Sending POST to ${url} (attempt ${attemptNum})`);
      const requestStartTime = Date.now();
      
      const response = await axios.post(url, payload.toString(), {
        headers,
        httpsAgent,
        timeout: REQUEST_TIMEOUT,
        validateStatus: (status) => status < 500, // Don't throw on 4xx
      });

      const requestDuration = Date.now() - requestStartTime;
      const status = response.status;
      const contentLength = response.headers['content-length'] || 'unknown';
      
      console.log(`[NETWORK] USDOT ${usdot}: Response received in ${requestDuration}ms - Status: ${status}, Content-Length: ${contentLength}`);
      
      if (status === 200) {
        const dataLength = typeof response.data === 'string' ? response.data.length : 'unknown';
        console.log(`[NETWORK] USDOT ${usdot}: ✓ Success (${dataLength} bytes in ${requestDuration}ms)`);
        return response.data;
      } else if (status === 404) {
        // Valid request, but no data found - don't retry
        console.log(`[NETWORK] USDOT ${usdot}: 404 Not Found (no data for this USDOT)`);
        return null;
      } else if (status === 403 || status === 429) {
        // Banned/throttled
        console.log(`[NETWORK] USDOT ${usdot}: ✗ ${status} (blocked/throttled) - ${status === 403 ? 'Forbidden - may need proxy' : 'Rate limited'} - attempt ${attemptNum}`);
        if (attempt < MAX_RETRIES - 1) {
          const retryDelay = 1000;
          console.log(`[NETWORK] USDOT ${usdot}: Waiting ${retryDelay}ms before retry...`);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
          continue;
        }
        console.log(`[NETWORK] USDOT ${usdot}: ✗ Failed after ${MAX_RETRIES} attempts (${status})`);
        return null;
      } else if (status >= 500) {
        // Server error - retry
        console.log(`[NETWORK] USDOT ${usdot}: ✗ ${status} (server error) - attempt ${attemptNum}`);
        if (attempt < MAX_RETRIES - 1) {
          const retryDelay = 1000;
          console.log(`[NETWORK] USDOT ${usdot}: Waiting ${retryDelay}ms before retry...`);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
          continue;
        }
        console.log(`[NETWORK] USDOT ${usdot}: ✗ Failed after ${MAX_RETRIES} attempts (${status})`);
        return null;
      } else {
        // Other status codes
        console.log(`[NETWORK] USDOT ${usdot}: ✗ Unexpected status ${status} - attempt ${attemptNum}`);
        return null;
      }
    } catch (error) {
      const requestDuration = Date.now() - attemptStartTime;
      
      if (axios.isAxiosError(error)) {
        const axiosError = error as AxiosError;
        
        // Timeout
        if (axiosError.code === 'ECONNABORTED' || axiosError.message.includes('timeout')) {
          console.log(`[NETWORK] USDOT ${usdot}: ✗ TIMEOUT after ${requestDuration}ms (configured timeout: ${REQUEST_TIMEOUT}ms) - attempt ${attemptNum}`);
          console.log(`[NETWORK] USDOT ${usdot}:   Error code: ${axiosError.code || 'none'}, Message: ${axiosError.message}`);
          
          if (httpsAgent) {
            console.log(`[NETWORK] USDOT ${usdot}:   Using proxy - connection may be slow or proxy may be down`);
          } else {
            console.log(`[NETWORK] USDOT ${usdot}:   No proxy - direct connection may be blocked by FMCSA`);
          }
          
          if (attempt < MAX_RETRIES - 1) {
            const retryDelay = 1000;
            console.log(`[NETWORK] USDOT ${usdot}:   Waiting ${retryDelay}ms before retry...`);
            await new Promise(resolve => setTimeout(resolve, retryDelay));
            continue;
          }
          console.log(`[NETWORK] USDOT ${usdot}: ✗ Failed after ${MAX_RETRIES} attempts (timeout)`);
          return null;
        }
        
        // Connection errors
        if (axiosError.code === 'ECONNREFUSED' || axiosError.code === 'ENOTFOUND' || axiosError.code === 'ETIMEDOUT') {
          console.log(`[NETWORK] USDOT ${usdot}: ✗ Connection error after ${requestDuration}ms - attempt ${attemptNum}`);
          console.log(`[NETWORK] USDOT ${usdot}:   Error code: ${axiosError.code}, Message: ${axiosError.message}`);
          if (httpsAgent) {
            console.log(`[NETWORK] USDOT ${usdot}:   Proxy connection failed - check proxy settings`);
          } else {
            console.log(`[NETWORK] USDOT ${usdot}:   Direct connection failed - check network/FMCSA availability`);
          }
          
          if (attempt < MAX_RETRIES - 1) {
            const retryDelay = 1000;
            console.log(`[NETWORK] USDOT ${usdot}:   Waiting ${retryDelay}ms before retry...`);
            await new Promise(resolve => setTimeout(resolve, retryDelay));
            continue;
          }
          console.log(`[NETWORK] USDOT ${usdot}: ✗ Failed after ${MAX_RETRIES} attempts (connection error)`);
          return null;
        }
        
        // Other Axios errors
        console.log(`[NETWORK] USDOT ${usdot}: ✗ Axios error after ${requestDuration}ms - attempt ${attemptNum}`);
        console.log(`[NETWORK] USDOT ${usdot}:   Code: ${axiosError.code || 'none'}, Message: ${axiosError.message}`);
        if (axiosError.response) {
          console.log(`[NETWORK] USDOT ${usdot}:   Response status: ${axiosError.response.status}, Headers: ${JSON.stringify(axiosError.response.headers)}`);
        }
        if (axiosError.request) {
          console.log(`[NETWORK] USDOT ${usdot}:   Request was made but no response received`);
        }
        
        if (attempt < MAX_RETRIES - 1) {
          const retryDelay = 1000;
          console.log(`[NETWORK] USDOT ${usdot}:   Waiting ${retryDelay}ms before retry...`);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
          continue;
        }
        console.log(`[NETWORK] USDOT ${usdot}: ✗ Failed after ${MAX_RETRIES} attempts (axios error)`);
        return null;
      }
      
      // Other exceptions
      console.log(`[NETWORK] USDOT ${usdot}: ✗ Unexpected error after ${requestDuration}ms - attempt ${attemptNum}`);
      console.log(`[NETWORK] USDOT ${usdot}:   Error: ${error instanceof Error ? error.message : String(error)}`);
      if (error instanceof Error && error.stack) {
        console.log(`[NETWORK] USDOT ${usdot}:   Stack: ${error.stack.split('\n').slice(0, 3).join('\n')}`);
      }
      
      if (attempt < MAX_RETRIES - 1) {
        const retryDelay = 1000;
        console.log(`[NETWORK] USDOT ${usdot}:   Waiting ${retryDelay}ms before retry...`);
        await new Promise(resolve => setTimeout(resolve, retryDelay));
        continue;
      }
      console.log(`[NETWORK] USDOT ${usdot}: ✗ Failed after ${MAX_RETRIES} attempts (unexpected error)`);
      return null;
    }
  }

  console.log(`[NETWORK] USDOT ${usdot}: ✗ Failed after ${MAX_RETRIES} attempts (exhausted retries)`);
  return null; // Failed after all retries
}

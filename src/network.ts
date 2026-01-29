/**
 * Async HTTP client with proxy rotation and retry logic.
 * Uses SOCKS5 to bypass FMCSA Firewall "Bot" detection.
 */

import axios from 'axios';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { randomBytes } from 'crypto';
import {
  PROXY_URL,
  PROXY_USER_BASE,
  PROXY_PASS,
  PROXY_SESS_TIME,
  MAX_RETRIES,
  REQUEST_TIMEOUT,
  TEST_MODE,
} from './config';

/**
 * Generate a unique session user for every request to force IP rotation.
 * Format: {BASE}-region-us-sessid-{RANDOM}-sessTime-{TIME}
 */
function getSocksProxyUrl(): string {
  const sessId = randomBytes(4).toString('hex');

  // 1. Construct Username with "-region-us" (CRITICAL for US IPs)
  const username = `${PROXY_USER_BASE}-region-us-sessid-${sessId}-sessTime-${PROXY_SESS_TIME}`;

  // 2. Extract Host (remove http/socks prefixes)
  const host = PROXY_URL!.replace(/^(https?|socks5?):\/\//, '');

  // 3. Return full SOCKS5 URL
  return `socks5://${username}:${PROXY_PASS}@${host}`;
}

export async function fetchHtml(usdot: string): Promise<string | null> {
  const url = 'https://safer.fmcsa.dot.gov/query.asp';

  const payload = new URLSearchParams({
    searchtype: 'ANY',
    query_type: 'queryCarrierSnapshot',
    query_param: 'USDOT',
    query_string: usdot,
  });

  const headers: Record<string, string> = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    Accept:
      'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    Origin: 'https://safer.fmcsa.dot.gov',
    Referer: 'https://safer.fmcsa.dot.gov/CompanySnapshot.aspx',
    Connection: 'close',
  };

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    let agent: SocksProxyAgent | undefined;

    if (!TEST_MODE && PROXY_URL && PROXY_USER_BASE && PROXY_PASS) {
      // SOCKS5 Agent with Legacy SSL Settings (Matches Python LegacySSLAdapter)
      agent = new SocksProxyAgent(getSocksProxyUrl(), {
        timeout: REQUEST_TIMEOUT,
        rejectUnauthorized: false,
        minVersion: 'TLSv1',
        ciphers: 'DEFAULT:@SECLEVEL=0',
      } as import('socks-proxy-agent').SocksProxyAgentOptions);
    }

    try {
      const response = await axios.post(url, payload.toString(), {
        headers,
        httpsAgent: agent,
        httpAgent: agent,
        proxy: false,
        timeout: REQUEST_TIMEOUT,
        validateStatus: (s) => s < 500,
        maxRedirects: 5,
      });

      if (response.status === 200) {
        if (TEST_MODE) console.log(`[NETWORK] USDOT ${usdot}: ✓ Success`);
        return response.data;
      }

      if (response.status === 404) return null;

      // Throw for 403/429 to trigger retry with new IP
      throw new Error(`HTTP ${response.status}`);
    } catch (err: unknown) {
      const isLast = attempt >= MAX_RETRIES - 1;
      const msg = axios.isAxiosError(err) ? (err.code || err.message) : String(err);

      if (!isLast) {
        const delay =
          typeof msg === 'string' &&
          (msg.includes('RESET') || msg.includes('EPROTO') || msg.includes(' closed'))
            ? 200
            : 2000;
        await new Promise((r) => setTimeout(r, delay));
      }
      // Log network failures on the final attempt so we can see all hard fails
      if (isLast) {
        console.log(
          `[NETWORK FAIL] USDOT ${usdot}: Failed after ${MAX_RETRIES} attempts. Last error: ${msg}`
        );
      }
    }
  }

  return null;
}

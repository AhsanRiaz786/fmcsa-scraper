#!/usr/bin/env python3
"""
Async HTTP client with proxy rotation and retry logic.
Handles FMCSA API requests with intelligent retry strategies.
"""

import asyncio
import random
import string
import aiohttp
from typing import Optional
from config import PROXY_URL, PROXY_USER_BASE, PROXY_PASS, MAX_RETRIES, REQUEST_TIMEOUT, TEST_MODE


def get_proxy_auth() -> aiohttp.BasicAuth:
    """
    Generate a unique session ID for every request to force IP rotation.
    Format: {PROXY_USER_BASE}-{random8chars}:{PROXY_PASS}
    """
    # Generate random 8-character string for session ID
    rand_sess = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    username = f"{PROXY_USER_BASE}-{rand_sess}"
    return aiohttp.BasicAuth(username, PROXY_PASS)


async def fetch_html(session: aiohttp.ClientSession, usdot: str) -> Optional[str]:
    """
    Fetch HTML from FMCSA SAFER database for a given USDOT number.
    
    Args:
        session: aiohttp ClientSession (reused for connection pooling)
        usdot: USDOT number as string
        
    Returns:
        HTML content as string, or None if request failed or 404
    """
    url = 'https://safer.fmcsa.dot.gov/query.asp'
    
    payload = {
        'searchtype': 'ANY',
        'query_type': 'queryCarrierSnapshot',
        'query_param': 'USDOT',
        'query_string': str(usdot)
    }
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    # Retry loop
    for attempt in range(MAX_RETRIES):
        try:
            # Build request kwargs
            request_kwargs = {
                'url': url,
                'data': payload,
                'headers': headers,
                'timeout': aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            }
            
            # Only use proxy if not in test mode
            if not TEST_MODE and PROXY_URL:
                request_kwargs['proxy'] = PROXY_URL
                request_kwargs['proxy_auth'] = get_proxy_auth()  # Rotate IP on every request
            
            async with session.post(**request_kwargs) as response:
                status = response.status
                if status == 200:
                    return await response.text()
                elif status == 404:
                    # Valid request, but no data found - don't retry
                    return None
                elif status in (403, 429):
                    # Banned/throttled - in test mode, log and return None
                    if TEST_MODE and attempt == 0:
                        print(f"[DEBUG] USDOT {usdot}: Got {status} (blocked/throttled). FMCSA may block direct connections.")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(1)  # Brief delay before retry
                        continue
                    return None
                elif status >= 500:
                    # Server error - retry
                    if TEST_MODE and attempt == 0:
                        print(f"[DEBUG] USDOT {usdot}: Got {status} (server error)")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(1)
                        continue
                    return None
                else:
                    # Other status codes - log in test mode
                    if TEST_MODE and attempt == 0:
                        print(f"[DEBUG] USDOT {usdot}: Got unexpected status {status}")
                    return None
                    
        except asyncio.TimeoutError:
            # Timeout - retry
            if TEST_MODE and attempt == 0:
                print(f"[DEBUG] USDOT {usdot}: Request timeout")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(1)
                continue
            return None
        except aiohttp.ClientError as e:
            # Client errors (connection issues, etc.)
            if TEST_MODE and attempt == 0:
                print(f"[DEBUG] USDOT {usdot}: Client error: {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(1)
                continue
            return None
        except Exception as e:
            # Other exceptions - retry
            if TEST_MODE and attempt == 0:
                print(f"[DEBUG] USDOT {usdot}: Unexpected error: {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(1)
                continue
            return None
    
    return None  # Failed after all retries

#!/usr/bin/env python3
"""
Centralized configuration loader for FMCSA scraper.
Reads from .env file and validates required settings.
"""

import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_AVAILABLE = os.getenv("DB_AVAILABLE", "False").lower() == "true"

# Build database DSN (URL-encode username and password to handle special characters)
if DB_NAME and DB_USER and DB_PASSWORD:
    encoded_user = quote_plus(DB_USER)
    encoded_password = quote_plus(DB_PASSWORD)
    DATABASE_DSN = f"postgresql://{encoded_user}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    DATABASE_DSN = None

# Proxy configuration
PROXY_URL = os.getenv("PROXY_URL")
PROXY_USER_BASE = os.getenv("PROXY_USER_BASE")
PROXY_PASS = os.getenv("PROXY_PASS")

# Test mode (disables proxy, uses low concurrency)
TEST_MODE = os.getenv("TEST_MODE", "False").lower() == "true"

# Performance settings
# In test mode, force lower concurrency (ignore CONCURRENCY env var)
if TEST_MODE:
    CONCURRENCY = 10  # Always use 10 in test mode
else:
    CONCURRENCY = int(os.getenv("CONCURRENCY", "200"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))

# Input file
INPUT_FILE = os.getenv("INPUT_FILE", "dot_numbers.csv")

# Test mode limit (only process first N records in test mode)
TEST_LIMIT = int(os.getenv("TEST_LIMIT", "100")) if TEST_MODE else None

# Validation
def validate_config():
    """Validate that all required configuration is present."""
    errors = []
    
    if not DATABASE_DSN:
        errors.append("Database configuration incomplete (DB_NAME, DB_USER, DB_PASSWORD required)")
    
    # Only require proxy settings if not in test mode
    if not TEST_MODE:
        if not PROXY_URL:
            errors.append("PROXY_URL not set")
        
        if not PROXY_USER_BASE:
            errors.append("PROXY_USER_BASE not set")
        
        if not PROXY_PASS:
            errors.append("PROXY_PASS not set")
    
    if errors:
        raise ValueError("Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors))
    
    return True

# Validate on import (can be disabled for testing)
if __name__ != "__main__":
    try:
        validate_config()
    except ValueError:
        # Allow import to succeed even if config is incomplete (for testing)
        pass

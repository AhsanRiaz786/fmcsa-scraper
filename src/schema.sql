-- FMCSA Scraper Database Schema
-- Based on carrier.types.ts (flattened structure)

-- Enable UUID extension if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Main snapshots table (flattened structure matching TypeScript types)
CREATE TABLE snapshots (
  usdot_number VARCHAR(100) PRIMARY KEY,
  
  -- Basic info
  entity_type VARCHAR(100),
  usdot_status VARCHAR(100),
  out_of_service_date DATE,
  state_carrier_id_number VARCHAR(100),
  
  -- MCS-150 data
  mcs_150_form_date DATE,
  mcs_150_mileage_year INTEGER,
  mcs_150_mileage BIGINT,
  
  -- Operating authority
  operating_authority_status TEXT,
  mc_number VARCHAR(100),
  mx_number VARCHAR(100),
  ff_number VARCHAR(100),
  
  -- Company info
  legal_name TEXT,
  dba_name TEXT,
  
  -- Physical address (flattened)
  physical_address_street TEXT,
  physical_address_city VARCHAR(100),
  physical_address_state VARCHAR(100),
  physical_address_zip VARCHAR(100),
  physical_address_country VARCHAR(100) DEFAULT 'US',
  
  -- Mailing address (flattened)
  mailing_address_street TEXT,
  mailing_address_city VARCHAR(100),
  mailing_address_state VARCHAR(100),
  mailing_address_zip VARCHAR(100),
  mailing_address_country VARCHAR(100) DEFAULT 'US',
  
  -- Contact
  phone VARCHAR(100),
  duns_number VARCHAR(100),
  
  -- Fleet size
  power_units INTEGER,
  non_cmv_units INTEGER,
  drivers INTEGER,
  
  -- JSON columns for arrays and nested objects
  operation_classification JSONB,
  carrier_operation JSONB,
  cargo_carried JSONB,
  cargo_carried_other TEXT,
  us_inspection_summary_24mo JSONB,
  canadian_inspection_summary_24mo JSONB,
  carrier_safety_rating JSONB,
  
  -- Metadata
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common lookups
CREATE INDEX idx_snapshots_entity_type ON snapshots(entity_type);
CREATE INDEX idx_snapshots_state ON snapshots(physical_address_state);
CREATE INDEX idx_snapshots_mc_number ON snapshots(mc_number) WHERE mc_number IS NOT NULL;
CREATE INDEX idx_snapshots_legal_name ON snapshots(legal_name) WHERE legal_name IS NOT NULL;

-- GIN indexes for JSONB columns (for efficient JSON queries)
CREATE INDEX idx_snapshots_operation_classification ON snapshots USING GIN (operation_classification);
CREATE INDEX idx_snapshots_cargo_carried ON snapshots USING GIN (cargo_carried);

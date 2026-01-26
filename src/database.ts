/**
 * Async batch database operations using pg (node-postgres).
 * Handles bulk inserts for FMCSA carrier snapshots.
 */

import { Pool, PoolClient } from 'pg';
import { DB_CONFIG } from './config';
import type { Snapshot } from './types/carrier.types';

function formatDateForDB(date: Date | null | undefined): string | null {
  if (!date) return null;
  try {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  } catch {
    return null;
  }
}

/** Truncate string to maxLen for VARCHAR columns. Returns null if input is null/empty. */
function truncate(s: string | null | undefined, maxLen: number): string | null {
  if (s == null || s === '') return null;
  const t = String(s).trim();
  return t === '' ? null : t.length > maxLen ? t.slice(0, maxLen) : t;
}

const COLS = 35;

function recordToValues(record: Snapshot): unknown[] | null {
  const usdot = record.usdot_number != null ? String(record.usdot_number).trim() : '';
  if (!usdot) return null;
  return [
    truncate(usdot, 50) ?? usdot,
    truncate(record.entity_type, 50),
    truncate(record.usdot_status, 50),
    formatDateForDB(record.out_of_service_date),
    truncate(record.state_carrier_id_number, 50),
    formatDateForDB(record.mcs_150_form_date),
    record.mcs_150_mileage_year ?? null,
    record.mcs_150_mileage ?? null,
    record.operating_authority_status ?? null,
    truncate(record.mc_number, 50),
    truncate(record.mx_number, 50),
    truncate(record.ff_number, 50),
    record.legal_name ?? null,
    record.dba_name ?? null,
    record.physical_address?.street ?? null,
    truncate(record.physical_address?.city, 100),
    truncate(record.physical_address?.state, 50),
    truncate(record.physical_address?.zip, 20),
    truncate(record.physical_address?.country, 50),
    record.mailing_address?.street ?? null,
    truncate(record.mailing_address?.city, 100),
    truncate(record.mailing_address?.state, 50),
    truncate(record.mailing_address?.zip, 20),
    truncate(record.mailing_address?.country, 50),
    truncate(record.phone, 50),
    truncate(record.duns_number, 50),
    record.power_units ?? null,
    record.non_cmv_units ?? null,
    record.drivers ?? null,
    record.operation_classification ? JSON.stringify(record.operation_classification) : null,
    record.carrier_operation ? JSON.stringify(record.carrier_operation) : null,
    record.cargo_carried ? JSON.stringify(record.cargo_carried) : null,
    record.us_inspection_summary_24mo ? JSON.stringify(record.us_inspection_summary_24mo) : null,
    record.canadian_inspection_summary_24mo ? JSON.stringify(record.canadian_inspection_summary_24mo) : null,
    record.carrier_safety_rating ? JSON.stringify(record.carrier_safety_rating) : null,
  ];
}

/**
 * Bulk insert/update snapshots table using a single multi-row INSERT.
 * Much faster than per-row inserts (one round-trip per batch).
 *
 * @param client - pg PoolClient (from transaction)
 * @param records - List of Snapshot objects
 * @returns Number of records processed
 */
export async function bulkInsertSnapshots(
  client: PoolClient,
  records: Snapshot[]
): Promise<number> {
  if (records.length === 0) return 0;

  const rows: unknown[][] = [];
  for (const r of records) {
    const v = recordToValues(r);
    if (v) rows.push(v);
  }
  if (rows.length === 0) return 0;

  const flat = rows.flat();
  const placeholders: string[] = [];
  for (let i = 0; i < rows.length; i++) {
    const start = i * COLS + 1;
    placeholders.push(
      `(${Array.from({ length: COLS }, (_, j) => `$${start + j}`).join(', ')})`
    );
  }

  const query = `
    INSERT INTO snapshots (
      usdot_number, entity_type, usdot_status, out_of_service_date,
      state_carrier_id_number, mcs_150_form_date, mcs_150_mileage_year, mcs_150_mileage,
      operating_authority_status, mc_number, mx_number, ff_number,
      legal_name, dba_name,
      physical_address_street, physical_address_city, physical_address_state, physical_address_zip, physical_address_country,
      mailing_address_street, mailing_address_city, mailing_address_state, mailing_address_zip, mailing_address_country,
      phone, duns_number, power_units, non_cmv_units, drivers,
      operation_classification, carrier_operation, cargo_carried,
      us_inspection_summary_24mo, canadian_inspection_summary_24mo, carrier_safety_rating
    ) VALUES ${placeholders.join(', ')}
    ON CONFLICT (usdot_number) DO UPDATE SET
      entity_type = EXCLUDED.entity_type,
      usdot_status = EXCLUDED.usdot_status,
      out_of_service_date = EXCLUDED.out_of_service_date,
      state_carrier_id_number = EXCLUDED.state_carrier_id_number,
      mcs_150_form_date = EXCLUDED.mcs_150_form_date,
      mcs_150_mileage_year = EXCLUDED.mcs_150_mileage_year,
      mcs_150_mileage = EXCLUDED.mcs_150_mileage,
      operating_authority_status = EXCLUDED.operating_authority_status,
      mc_number = EXCLUDED.mc_number,
      mx_number = EXCLUDED.mx_number,
      ff_number = EXCLUDED.ff_number,
      legal_name = EXCLUDED.legal_name,
      dba_name = EXCLUDED.dba_name,
      physical_address_street = EXCLUDED.physical_address_street,
      physical_address_city = EXCLUDED.physical_address_city,
      physical_address_state = EXCLUDED.physical_address_state,
      physical_address_zip = EXCLUDED.physical_address_zip,
      physical_address_country = EXCLUDED.physical_address_country,
      mailing_address_street = EXCLUDED.mailing_address_street,
      mailing_address_city = EXCLUDED.mailing_address_city,
      mailing_address_state = EXCLUDED.mailing_address_state,
      mailing_address_zip = EXCLUDED.mailing_address_zip,
      mailing_address_country = EXCLUDED.mailing_address_country,
      phone = EXCLUDED.phone,
      duns_number = EXCLUDED.duns_number,
      power_units = EXCLUDED.power_units,
      non_cmv_units = EXCLUDED.non_cmv_units,
      drivers = EXCLUDED.drivers,
      operation_classification = EXCLUDED.operation_classification,
      carrier_operation = EXCLUDED.carrier_operation,
      cargo_carried = EXCLUDED.cargo_carried,
      us_inspection_summary_24mo = EXCLUDED.us_inspection_summary_24mo,
      canadian_inspection_summary_24mo = EXCLUDED.canadian_inspection_summary_24mo,
      carrier_safety_rating = EXCLUDED.carrier_safety_rating,
      updated_at = CURRENT_TIMESTAMP
  `;

  try {
    await client.query(query, flat);
    return rows.length;
  } catch (error) {
    console.log(`Error inserting batch (${rows.length} records): ${error}`);
    throw error;
  }
}

/**
 * Insert a batch of records into the database.
 * Wraps everything in a transaction for atomicity.
 * 
 * @param pool - pg Pool
 * @param records - List of Snapshot objects
 * @returns Number of records processed
 */
export async function bulkInsertBatch(
  pool: Pool,
  records: Snapshot[]
): Promise<number> {
  if (records.length === 0) return 0;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const count = await bulkInsertSnapshots(client, records);
    await client.query('COMMIT');
    return count;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Async batch database operations using pg (node-postgres).
 * Handles bulk inserts for FMCSA carrier snapshots.
 */

import { Pool, PoolClient } from 'pg';
import { DB_CONFIG, BATCH_SIZE } from './config';
import type { Snapshot } from './types/carrier.types';

/**
 * Parse date to PostgreSQL DATE format (YYYY-MM-DD string)
 */
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

/**
 * Bulk insert/update snapshots table using individual queries in a transaction.
 * This is simpler and more reliable than building a huge parameterized query.
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
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35)
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

  // Execute each record individually (pg handles batching efficiently)
  let count = 0;
  for (const record of records) {
    if (!record.usdot_number) continue;

    const values = [
      record.usdot_number,
      record.entity_type || null,
      record.usdot_status || null,
      formatDateForDB(record.out_of_service_date),
      record.state_carrier_id_number || null,
      formatDateForDB(record.mcs_150_form_date),
      record.mcs_150_mileage_year || null,
      record.mcs_150_mileage || null,
      record.operating_authority_status || null,
      record.mc_number || null,
      record.mx_number || null,
      record.ff_number || null,
      record.legal_name || null,
      record.dba_name || null,
      record.physical_address?.street || null,
      record.physical_address?.city || null,
      record.physical_address?.state || null,
      record.physical_address?.zip || null,
      record.physical_address?.country || null,
      record.mailing_address?.street || null,
      record.mailing_address?.city || null,
      record.mailing_address?.state || null,
      record.mailing_address?.zip || null,
      record.mailing_address?.country || null,
      record.phone || null,
      record.duns_number || null,
      record.power_units || null,
      record.non_cmv_units || null,
      record.drivers || null,
      record.operation_classification ? JSON.stringify(record.operation_classification) : null,
      record.carrier_operation ? JSON.stringify(record.carrier_operation) : null,
      record.cargo_carried ? JSON.stringify(record.cargo_carried) : null,
      record.us_inspection_summary_24mo ? JSON.stringify(record.us_inspection_summary_24mo) : null,
      record.canadian_inspection_summary_24mo ? JSON.stringify(record.canadian_inspection_summary_24mo) : null,
      record.carrier_safety_rating ? JSON.stringify(record.carrier_safety_rating) : null,
    ];

    try {
      await client.query(query, values);
      count++;
    } catch (error) {
      console.log(`Error inserting record ${record.usdot_number}: ${error}`);
      // Continue with next record
    }
  }

  return count;
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

#!/usr/bin/env python3
"""
Async batch database operations using asyncpg.
Handles bulk inserts for all FMCSA carrier data tables.
"""

import asyncpg
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from config import DATABASE_DSN, BATCH_SIZE


def parse_date(date_str: Optional[str]) -> Optional[date]:
    """Parse YYYY-MM-DD string to date object."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


async def bulk_insert_carriers(conn: asyncpg.Connection, records: List[Dict[str, Any]]) -> int:
    """
    Bulk insert/update carriers table.
    
    Args:
        conn: asyncpg connection
        records: List of parsed JSON dictionaries
        
    Returns:
        Number of records processed
    """
    if not records:
        return 0
    
    data = []
    for r in records:
        metadata = r.get("record_metadata", {})
        identity = r.get("company_identity", {})
        contact = r.get("contact_info", {})
        status = r.get("operating_status", {})
        operations = r.get("operations", {})
        
        usdot = metadata.get("usdot_number")
        if not usdot:
            continue  # Skip records without USDOT
        
        # Parse dates
        snapshot_date = parse_date(metadata.get("snapshot_date"))
        oos_date = parse_date(status.get("out_of_service_date"))
        mcs_date = parse_date(status.get("mcs_150_form_date"))
        
        data.append((
            usdot,
            metadata.get("entity_type"),
            snapshot_date,
            identity.get("legal_name"),
            identity.get("dba_name"),
            identity.get("duns_number"),
            contact.get("phone"),
            status.get("usdot_status"),
            status.get("operating_authority_status"),
            oos_date,
            mcs_date,
            status.get("mcs_150_mileage"),
            status.get("mcs_150_mileage_year"),
            operations.get("fleet_size", {}).get("power_units"),
            operations.get("fleet_size", {}).get("drivers"),
        ))
    
    if not data:
        return 0
    
    # Bulk upsert
    await conn.executemany("""
        INSERT INTO carriers (
            usdot_number, entity_type, snapshot_date,
            legal_name, dba_name, duns_number, phone,
            usdot_status, operating_authority_status, out_of_service_date,
            mcs_150_form_date, mcs_150_mileage, mcs_150_mileage_year,
            power_units, drivers, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, CURRENT_TIMESTAMP
        )
        ON CONFLICT (usdot_number) DO UPDATE SET
            entity_type = EXCLUDED.entity_type,
            snapshot_date = EXCLUDED.snapshot_date,
            legal_name = EXCLUDED.legal_name,
            dba_name = EXCLUDED.dba_name,
            duns_number = EXCLUDED.duns_number,
            phone = EXCLUDED.phone,
            usdot_status = EXCLUDED.usdot_status,
            operating_authority_status = EXCLUDED.operating_authority_status,
            out_of_service_date = EXCLUDED.out_of_service_date,
            mcs_150_form_date = EXCLUDED.mcs_150_form_date,
            mcs_150_mileage = EXCLUDED.mcs_150_mileage,
            mcs_150_mileage_year = EXCLUDED.mcs_150_mileage_year,
            power_units = EXCLUDED.power_units,
            drivers = EXCLUDED.drivers,
            updated_at = CURRENT_TIMESTAMP
    """, data)
    
    return len(data)


async def bulk_insert_related_tables(conn: asyncpg.Connection, records: List[Dict[str, Any]]) -> None:
    """
    Bulk insert all related tables (authority_numbers, addresses, classifications, cargo, inspections, crashes, safety_ratings).
    
    Args:
        conn: asyncpg connection
        records: List of parsed JSON dictionaries
    """
    if not records:
        return
    
    # Collect all related data grouped by USDOT
    authority_data = []
    address_data = []
    classification_data = []
    cargo_data = []
    inspection_data = []
    crash_data = []
    safety_rating_data = []
    
    for r in records:
        metadata = r.get("record_metadata", {})
        usdot = metadata.get("usdot_number")
        if not usdot:
            continue
        
        identity = r.get("company_identity", {})
        contact = r.get("contact_info", {})
        operations = r.get("operations", {})
        safety = r.get("safety_record", {})
        
        # Authority numbers (MC/MX/FF)
        for num in identity.get("mc_mx_ff_numbers", []):
            authority_data.append((usdot, num))
        
        # Addresses
        physical_addr = contact.get("physical_address", {})
        mailing_addr = contact.get("mailing_address", {})
        address_data.append((
            usdot, "PHYSICAL",
            physical_addr.get("street"),
            physical_addr.get("city"),
            physical_addr.get("state"),
            physical_addr.get("zip_code"),
            physical_addr.get("country", "US")
        ))
        address_data.append((
            usdot, "MAILING",
            mailing_addr.get("street"),
            mailing_addr.get("city"),
            mailing_addr.get("state"),
            mailing_addr.get("zip_code"),
            mailing_addr.get("country", "US")
        ))
        
        # Classifications
        for cls in operations.get("operation_classifications", []):
            classification_data.append((usdot, cls))
        
        # Cargo
        for cargo in operations.get("cargo_carried", []):
            cargo_data.append((usdot, cargo))
        
        # Inspections (US and Canada)
        us_insp = safety.get("us_inspections", {})
        us_breakdown = us_insp.get("breakdown", {})
        vehicle = us_breakdown.get("vehicle", {})
        driver = us_breakdown.get("driver", {})
        hazmat = us_breakdown.get("hazmat", {})
        iep = us_breakdown.get("iep", {})
        
        inspection_data.append((
            usdot, "US",
            us_insp.get("total_inspections", 0),
            us_insp.get("total_iep_inspections", 0),
            vehicle.get("inspections", 0),
            vehicle.get("out_of_service", 0),
            vehicle.get("out_of_service_rate_pct"),
            driver.get("inspections", 0),
            driver.get("out_of_service", 0),
            driver.get("out_of_service_rate_pct"),
            hazmat.get("inspections", 0),
            hazmat.get("out_of_service", 0),
            hazmat.get("out_of_service_rate_pct"),
            iep.get("inspections", 0),
            iep.get("out_of_service", 0),
            iep.get("out_of_service_rate_pct"),
        ))
        
        ca_insp = safety.get("canada_inspections", {})
        ca_breakdown = ca_insp.get("breakdown", {})
        ca_vehicle = ca_breakdown.get("vehicle", {})
        ca_driver = ca_breakdown.get("driver", {})
        
        inspection_data.append((
            usdot, "CANADA",
            ca_insp.get("total_inspections", 0),
            0,  # No IEP for Canada
            ca_vehicle.get("inspections", 0),
            ca_vehicle.get("out_of_service", 0),
            ca_vehicle.get("out_of_service_rate_pct"),
            ca_driver.get("inspections", 0),
            ca_driver.get("out_of_service", 0),
            ca_driver.get("out_of_service_rate_pct"),
            0, 0, None,  # No hazmat for Canada
            0, 0, None,  # No IEP for Canada
        ))
        
        # Crashes (US and Canada)
        us_crash = safety.get("us_crashes", {})
        crash_data.append((
            usdot, "US",
            us_crash.get("fatal", 0),
            us_crash.get("injury", 0),
            us_crash.get("tow", 0),
            us_crash.get("total", 0),
        ))
        
        ca_crash = safety.get("canada_crashes", {})
        crash_data.append((
            usdot, "CANADA",
            ca_crash.get("fatal", 0),
            ca_crash.get("injury", 0),
            ca_crash.get("tow", 0),
            ca_crash.get("total", 0),
        ))
        
        # Safety rating
        rating = safety.get("safety_rating", {})
        rating_date = parse_date(rating.get("rating_date"))
        review_date = parse_date(rating.get("review_date"))
        safety_rating_data.append((
            usdot,
            rating.get("rating"),
            rating_date,
            review_date,
            rating.get("type"),
        ))
    
    # Delete existing records for this batch of USDOTs, then bulk insert
    usdots = [r.get("record_metadata", {}).get("usdot_number") for r in records if r.get("record_metadata", {}).get("usdot_number")]
    
    if usdots:
        # Delete existing related records
        await conn.execute("DELETE FROM carrier_authority_numbers WHERE usdot_number = ANY($1::bigint[])", usdots)
        await conn.execute("DELETE FROM carrier_addresses WHERE usdot_number = ANY($1::bigint[])", usdots)
        await conn.execute("DELETE FROM carrier_classifications WHERE usdot_number = ANY($1::bigint[])", usdots)
        await conn.execute("DELETE FROM carrier_cargo WHERE usdot_number = ANY($1::bigint[])", usdots)
        await conn.execute("DELETE FROM carrier_inspections WHERE usdot_number = ANY($1::bigint[])", usdots)
        await conn.execute("DELETE FROM carrier_crashes WHERE usdot_number = ANY($1::bigint[])", usdots)
        await conn.execute("DELETE FROM carrier_safety_ratings WHERE usdot_number = ANY($1::bigint[])", usdots)
        
        # Bulk insert new records
        if authority_data:
            await conn.executemany("""
                INSERT INTO carrier_authority_numbers (usdot_number, authority_number)
                VALUES ($1, $2)
                ON CONFLICT (usdot_number, authority_number) DO NOTHING
            """, authority_data)
        
        if address_data:
            await conn.executemany("""
                INSERT INTO carrier_addresses (usdot_number, address_type, street, city, state, zip_code, country)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (usdot_number, address_type) DO UPDATE SET
                    street = EXCLUDED.street,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    country = EXCLUDED.country
            """, address_data)
        
        if classification_data:
            await conn.executemany("""
                INSERT INTO carrier_classifications (usdot_number, classification)
                VALUES ($1, $2)
                ON CONFLICT (usdot_number, classification) DO NOTHING
            """, classification_data)
        
        if cargo_data:
            await conn.executemany("""
                INSERT INTO carrier_cargo (usdot_number, cargo_type)
                VALUES ($1, $2)
                ON CONFLICT (usdot_number, cargo_type) DO NOTHING
            """, cargo_data)
        
        if inspection_data:
            await conn.executemany("""
                INSERT INTO carrier_inspections (
                    usdot_number, region,
                    total_inspections, total_iep_inspections,
                    vehicle_inspections, vehicle_oos, vehicle_oos_rate,
                    driver_inspections, driver_oos, driver_oos_rate,
                    hazmat_inspections, hazmat_oos, hazmat_oos_rate,
                    iep_inspections, iep_oos, iep_oos_rate
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ON CONFLICT (usdot_number, region) DO UPDATE SET
                    total_inspections = EXCLUDED.total_inspections,
                    total_iep_inspections = EXCLUDED.total_iep_inspections,
                    vehicle_inspections = EXCLUDED.vehicle_inspections,
                    vehicle_oos = EXCLUDED.vehicle_oos,
                    vehicle_oos_rate = EXCLUDED.vehicle_oos_rate,
                    driver_inspections = EXCLUDED.driver_inspections,
                    driver_oos = EXCLUDED.driver_oos,
                    driver_oos_rate = EXCLUDED.driver_oos_rate,
                    hazmat_inspections = EXCLUDED.hazmat_inspections,
                    hazmat_oos = EXCLUDED.hazmat_oos,
                    hazmat_oos_rate = EXCLUDED.hazmat_oos_rate,
                    iep_inspections = EXCLUDED.iep_inspections,
                    iep_oos = EXCLUDED.iep_oos,
                    iep_oos_rate = EXCLUDED.iep_oos_rate
            """, inspection_data)
        
        if crash_data:
            await conn.executemany("""
                INSERT INTO carrier_crashes (usdot_number, region, fatal, injury, tow, total)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (usdot_number, region) DO UPDATE SET
                    fatal = EXCLUDED.fatal,
                    injury = EXCLUDED.injury,
                    tow = EXCLUDED.tow,
                    total = EXCLUDED.total
            """, crash_data)
        
        if safety_rating_data:
            await conn.executemany("""
                INSERT INTO carrier_safety_ratings (usdot_number, rating, rating_date, review_date, rating_type)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (usdot_number) DO UPDATE SET
                    rating = EXCLUDED.rating,
                    rating_date = EXCLUDED.rating_date,
                    review_date = EXCLUDED.review_date,
                    rating_type = EXCLUDED.rating_type
            """, safety_rating_data)


async def bulk_insert_batch(conn: asyncpg.Connection, records: List[Dict[str, Any]]) -> int:
    """
    Insert a batch of records into all tables.
    Wraps everything in a transaction for atomicity.
    
    Args:
        conn: asyncpg connection
        records: List of parsed JSON dictionaries
        
    Returns:
        Number of carriers processed
    """
    if not records:
        return 0
    
    async with conn.transaction():
        count = await bulk_insert_carriers(conn, records)
        await bulk_insert_related_tables(conn, records)
        return count

#!/usr/bin/env python3
"""
FMCSA Scraper - Fetches and parses carrier information from FMCSA SAFER database
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import os
from typing import Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DATABASE_NAME")
DB_USER = os.getenv("DATABASE_USER")
DB_PASSWORD = os.getenv("DATABASE_PASSWORD")
DB_HOST = os.getenv("DATABASE_HOST")
DB_PORT = os.getenv("DATABASE_PORT")
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_AVAILABLE = os.getenv("DB_AVAILABLE") == "True"




def clean_text(text: str) -> str:
    """Clean extracted text by removing extra whitespace and HTML entities."""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    text = text.replace('&nbsp;', ' ').strip()
    return text


def extract_table_value(soup: BeautifulSoup, label: str, default: str = "") -> str:
    """Extract value from a table row by label."""
    try:
        th = soup.find('th', string=re.compile(label, re.I))
        if th:
            td = th.find_next_sibling('td')
            if td:
                return clean_text(td.get_text())
    except Exception:
        pass
    return default


def extract_checked_items(soup: BeautifulSoup, section_label: str) -> list:
    """Extract items that are checked (marked with X) in a section."""
    checked_items = []
    try:
        # 1. Find the header link (e.g. <A ...>Cargo Carried:</A>) or text
        # This is more specific than just finding any TD with the text
        header = soup.find('a', string=re.compile(section_label, re.I))
        
        if not header:
            # Fallback for text without link
            header = soup.find(string=re.compile(section_label, re.I))
            
        if header:
            # The structure is usually: Header -> Parent TD -> Parent TR -> Next TR -> TD -> Table
            # We need to find the main container table for this section
            
            # Navigate up to the containing row of the header
            header_row = header.find_parent('tr')
            
            # The check-box table is usually in the NEXT row
            content_row = header_row.find_next_sibling('tr')
            
            if content_row:
                # Find all 'X' marks within this specific content row
                cells = content_row.find_all('td', class_='queryfield', string='X')
                
                for cell in cells:
                    # The label is in the next sibling TD
                    next_td = cell.find_next_sibling('td')
                    if next_td:
                        text = clean_text(next_td.get_text())
                        if text:
                            checked_items.append(text)
    except Exception as e:
        # Optional: print(f"Error extracting {section_label}: {e}")
        pass
        
    return checked_items


def _td_text_with_br(td) -> str:
    """Get TD text with <br> preserved as newline (for address parsing)."""
    if not td:
        return ""
    raw = str(td)
    for br in ("<br>", "<br/>", "<br />", "<BR>", "<BR/>", "<BR />"):
        raw = raw.replace(br, "\n")
    s = BeautifulSoup(raw, "html.parser").get_text()
    s = s.replace("&nbsp;", " ")
    s = re.sub(r"[ \t]+", " ", s)  # collapse spaces/tabs, keep newlines
    return s.strip()


def parse_address(address_text: str) -> Dict[str, Any]:
    """Parse address text into structured format. FMCSA uses line1=street, line2='CITY, ST ZIP'."""
    if not address_text:
        return {"street": None, "city": None, "state": None, "zip_code": None, "country": "US"}
    
    # Preserve newlines (from <br>); normalize spaces within lines
    lines = []
    for line in address_text.replace("&nbsp;", " ").split("\n"):
        line = re.sub(r"[ \t]+", " ", line).strip()
        if line:
            lines.append(line)
    
    street = None
    city = None
    state = None
    zip_code = None
    
    if len(lines) >= 2:
        # FMCSA format: line1 = street, line2 = "CITY, ST  ZIP"
        street = lines[0]
        rest = " ".join(lines[1:])
        match = re.search(r"([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$", rest, re.I)
        if match:
            state = match.group(1).upper()
            zip_code = match.group(2)
            remaining = rest[: match.start()].strip()
            if remaining.endswith(","):
                remaining = remaining[:-1].strip()
            city = remaining if remaining else None
        else:
            street = " ".join(lines)
    elif lines:
        # Single line: use state/zip + comma logic
        text = lines[0]
        match = re.search(r"([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$", text, re.I)
        street = text
        if match:
            state = match.group(1).upper()
            zip_code = match.group(2)
            remaining = text[: match.start()].strip()
            if remaining.endswith(","):
                remaining = remaining[:-1].strip()
            if "," in remaining:
                parts = remaining.rsplit(",", 1)
                street = parts[0].strip()
                city = parts[1].strip() if parts[1].strip() else None
            else:
                street = remaining

    return {
        "street": street,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "country": "US",
    }


def parse_fmcsa_response(html_content: str) -> Dict[str, Any]:
    """Parse the FMCSA HTML response and extract structured data."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract snapshot date from the page
    snapshot_date = None
    date_text = soup.find(string=re.compile(r"The information below reflects.*as of", re.I))
    if date_text:
        match = re.search(r'(\d{2}/\d{2}/\d{4})', str(date_text))
        if match:
            date_obj = datetime.strptime(match.group(1), '%m/%d/%Y')
            snapshot_date = date_obj.strftime('%Y-%m-%d')
    
    # Extract basic company info
    company_name = ""
    try:
        company_header = soup.find('font', size="3", face="arial")
        if company_header:
            b_tag = company_header.find('b')
            if b_tag:
                company_name = clean_text(b_tag.get_text())
    except Exception:
        pass
    
    usdot_number = extract_table_value(soup, "USDOT Number")
    entity_type = extract_table_value(soup, "Entity Type")
    
    # Extract MC/MX/FF Numbers
    mc_numbers = []
    mc_td = soup.find('th', string=re.compile("MC/MX/FF Number", re.I))
    if mc_td:
        td = mc_td.find_next_sibling('td')
        if td:
            links = td.find_all('a')
            for link in links:
                text = clean_text(link.get_text())
                if text:
                    mc_numbers.append(text)
    
    # Extract addresses (preserve <br> as newline so street vs city lines are kept)
    physical_addr_text = ""
    physical_addr_td = soup.find('td', id='physicaladdressvalue')
    if physical_addr_td:
        physical_addr_text = _td_text_with_br(physical_addr_td)
    else:
        physical_addr_text = extract_table_value(soup, "Physical Address")
    
    mailing_addr_text = ""
    mailing_addr_td = soup.find('td', id='mailingaddressvalue')
    if mailing_addr_td:
        mailing_addr_text = _td_text_with_br(mailing_addr_td)
    else:
        mailing_addr_text = extract_table_value(soup, "Mailing Address")
    
    # Extract DBA Name
    dba_name = extract_table_value(soup, "DBA Name")
    if not dba_name or dba_name in ['', '&nbsp;', 'None']:
        dba_name = None
    
    # Extract DUNS
    duns = extract_table_value(soup, "DUNS Number")
    if not duns or duns in ['--', '', 'None']:
        duns = None
    
    # Extract operating status
    usdot_status = extract_table_value(soup, "USDOT Status")
    
    oa_status = extract_table_value(soup, "Operating Authority Status")
    if "NOT AUTHORIZED" in oa_status.upper():
        oa_status = "NOT AUTHORIZED"
    elif "AUTHORIZED" in oa_status.upper():
        oa_status = oa_status
    
    oos_date = extract_table_value(soup, "Out of Service Date")
    if oos_date and oos_date.lower() in ['none', '']:
        oos_date = None
    
    mcs150_date = extract_table_value(soup, "MCS-150 Form Date")
    if mcs150_date:
        try:
            date_obj = datetime.strptime(mcs150_date, '%m/%d/%Y')
            mcs150_date = date_obj.strftime('%Y-%m-%d')
        except:
            pass
    
    # Extract MCS-150 Mileage
    mileage = None
    mileage_year = None
    mileage_text = extract_table_value(soup, "MCS-150 Mileage")
    if mileage_text:
        match = re.search(r'([\d,]+)\s*\((\d{4})\)', mileage_text)
        if match:
            mileage = int(match.group(1).replace(',', ''))
            mileage_year = int(match.group(2))
    
    # Extract fleet size
    power_units = extract_table_value(soup, "Power Units")
    power_units = int(power_units) if power_units and power_units.isdigit() else None
    
    drivers = extract_table_value(soup, "Drivers")
    drivers = int(drivers) if drivers and drivers.isdigit() else None
    
    # Extract operation classifications and cargo
    operation_classifications = extract_checked_items(soup, "Operation Classification")
    cargo_carried = extract_checked_items(soup, "Cargo Carried")
    
    # Extract inspections data
    us_inspections = {
        "total_inspections": 0,
        "total_iep_inspections": 0,
        "breakdown": {
            "vehicle": {"inspections": 0, "out_of_service": 0, "out_of_service_rate_pct": 0},
            "driver": {"inspections": 0, "out_of_service": 0, "out_of_service_rate_pct": 0},
            "hazmat": {"inspections": 0, "out_of_service": 0, "out_of_service_rate_pct": 0},
            "iep": {"inspections": 0, "out_of_service": 0, "out_of_service_rate_pct": 0}
        }
    }
    
    try:
        inspection_tables = soup.find_all('table', summary='Inspections')
        if inspection_tables:
            us_table = inspection_tables[0]
            rows = us_table.find_all('tr')
            if len(rows) >= 2:
                data_row = rows[1]
                cells = data_row.find_all('td', class_='queryfield')
                if len(cells) >= 4:
                    us_inspections["breakdown"]["vehicle"]["inspections"] = int(cells[0].get_text().strip() or 0)
                    us_inspections["breakdown"]["driver"]["inspections"] = int(cells[1].get_text().strip() or 0)
                    us_inspections["breakdown"]["hazmat"]["inspections"] = int(cells[2].get_text().strip() or 0)
                    us_inspections["breakdown"]["iep"]["inspections"] = int(cells[3].get_text().strip() or 0)
                
                if len(rows) >= 3:
                    oos_row = rows[2]
                    oos_cells = oos_row.find_all('td', class_='queryfield')
                    if len(oos_cells) >= 4:
                        us_inspections["breakdown"]["vehicle"]["out_of_service"] = int(oos_cells[0].get_text().strip() or 0)
                        us_inspections["breakdown"]["driver"]["out_of_service"] = int(oos_cells[1].get_text().strip() or 0)
                        us_inspections["breakdown"]["hazmat"]["out_of_service"] = int(oos_cells[2].get_text().strip() or 0)
                        us_inspections["breakdown"]["iep"]["out_of_service"] = int(oos_cells[3].get_text().strip() or 0)
                
                if len(rows) >= 4:
                    percent_row = rows[3]
                    percent_cells = percent_row.find_all('td', class_='queryfield')
                    if len(percent_cells) >= 4:
                        for i, cell in enumerate(percent_cells[:4]):
                            percent_text = cell.get_text().strip().replace('%', '')
                            if percent_text:
                                try:
                                    percent_val = float(percent_text)
                                    if i == 0:
                                        us_inspections["breakdown"]["vehicle"]["out_of_service_rate_pct"] = percent_val
                                    elif i == 1:
                                        us_inspections["breakdown"]["driver"]["out_of_service_rate_pct"] = percent_val
                                    elif i == 2:
                                        us_inspections["breakdown"]["hazmat"]["out_of_service_rate_pct"] = percent_val
                                    elif i == 3:
                                        us_inspections["breakdown"]["iep"]["out_of_service_rate_pct"] = percent_val
                                except ValueError:
                                    pass
        
        total_insp_text = soup.find(string=re.compile("Total Inspections:", re.I))
        if total_insp_text:
            parent = total_insp_text.find_parent('font')
            if parent:
                match = re.search(r'Total Inspections:\s*<FONT[^>]*>(\d+)</FONT>', str(parent))
                if match:
                    us_inspections["total_inspections"] = int(match.group(1))
        
        total_iep_text = soup.find(string=re.compile("Total IEP Inspections:", re.I))
        if total_iep_text:
            parent = total_iep_text.find_parent('font')
            if parent:
                match = re.search(r'Total IEP Inspections:\s*<FONT[^>]*>(\d+)</FONT>', str(parent))
                if match:
                    us_inspections["total_iep_inspections"] = int(match.group(1))
    except Exception as e:
        print(f"Error parsing US inspections: {e}")
    
    # Extract US crashes
    us_crashes = {"fatal": 0, "injury": 0, "tow": 0, "total": 0}
    try:
        crash_tables = soup.find_all('table', summary='Crashes')
        if crash_tables:
            us_crash_table = crash_tables[0]
            rows = us_crash_table.find_all('tr')
            if len(rows) >= 2:
                data_row = rows[1]
                cells = data_row.find_all('td', class_='queryfield')
                if len(cells) >= 4:
                    us_crashes["fatal"] = int(cells[0].get_text().strip() or 0)
                    us_crashes["injury"] = int(cells[1].get_text().strip() or 0)
                    us_crashes["tow"] = int(cells[2].get_text().strip() or 0)
                    us_crashes["total"] = int(cells[3].get_text().strip() or 0)
    except Exception as e:
        print(f"Error parsing US crashes: {e}")
    
    # Extract Canada inspections
    canada_inspections = {
        "total_inspections": 0,
        "breakdown": {
            "vehicle": {"inspections": 0, "out_of_service": 0, "out_of_service_rate_pct": 0},
            "driver": {"inspections": 0, "out_of_service": 0, "out_of_service_rate_pct": 0}
        }
    }
    
    try:
        inspection_tables = soup.find_all('table', summary='Inspections')
        if len(inspection_tables) >= 2:
            canada_table = inspection_tables[1]
            rows = canada_table.find_all('tr')
            if len(rows) >= 2:
                data_row = rows[1]
                cells = data_row.find_all('td', class_='queryfield')
                if len(cells) >= 2:
                    canada_inspections["breakdown"]["vehicle"]["inspections"] = int(cells[0].get_text().strip() or 0)
                    canada_inspections["breakdown"]["driver"]["inspections"] = int(cells[1].get_text().strip() or 0)
                
                if len(rows) >= 3:
                    oos_row = rows[2]
                    oos_cells = oos_row.find_all('td', class_='queryfield')
                    if len(oos_cells) >= 2:
                        canada_inspections["breakdown"]["vehicle"]["out_of_service"] = int(oos_cells[0].get_text().strip() or 0)
                        canada_inspections["breakdown"]["driver"]["out_of_service"] = int(oos_cells[1].get_text().strip() or 0)
                
                if len(rows) >= 4:
                    percent_row = rows[3]
                    percent_cells = percent_row.find_all('td', class_='queryfield')
                    if len(percent_cells) >= 2:
                        for i, cell in enumerate(percent_cells[:2]):
                            percent_text = cell.get_text().strip().replace('%', '')
                            if percent_text:
                                try:
                                    percent_val = float(percent_text)
                                    if i == 0:
                                        canada_inspections["breakdown"]["vehicle"]["out_of_service_rate_pct"] = percent_val
                                    elif i == 1:
                                        canada_inspections["breakdown"]["driver"]["out_of_service_rate_pct"] = percent_val
                                except ValueError:
                                    pass
        
        canada_total_text = soup.find(string=re.compile("Total inspections:", re.I))
        if canada_total_text:
            parent = canada_total_text.find_parent('font')
            if parent:
                match = re.search(r'Total inspections:\s*<FONT[^>]*>(\d+)</FONT>', str(parent))
                if match:
                    canada_inspections["total_inspections"] = int(match.group(1))
    except Exception as e:
        print(f"Error parsing Canada inspections: {e}")
    
    # Extract Canada crashes
    canada_crashes = {"fatal": 0, "injury": 0, "tow": 0, "total": 0}
    try:
        crash_tables = soup.find_all('table', summary='Crashes')
        if len(crash_tables) >= 2:
            canada_crash_table = crash_tables[1]
            rows = canada_crash_table.find_all('tr')
            if len(rows) >= 2:
                data_row = rows[1]
                cells = data_row.find_all('td', class_='queryfield')
                if len(cells) >= 4:
                    canada_crashes["fatal"] = int(cells[0].get_text().strip() or 0)
                    canada_crashes["injury"] = int(cells[1].get_text().strip() or 0)
                    canada_crashes["tow"] = int(cells[2].get_text().strip() or 0)
                    canada_crashes["total"] = int(cells[3].get_text().strip() or 0)
    except Exception as e:
        print(f"Error parsing Canada crashes: {e}")
    
    # Extract Safety Rating
    safety_rating = {"rating": None, "rating_date": None, "review_date": None, "type": None}
    try:
        rating_table = soup.find('table', summary='Review Information')
        if rating_table:
            rows = rating_table.find_all('tr')
            for row in rows:
                ths = row.find_all('th', class_='querylabelbkg')
                tds = row.find_all('td', class_='queryfield')
                for i, th in enumerate(ths):
                    label = clean_text(th.get_text())
                    if i < len(tds):
                        value = clean_text(tds[i].get_text())
                        if value.lower() == "none":
                            value = None
                        if "Rating Date" in label:
                            safety_rating["rating_date"] = value
                        elif "Review Date" in label:
                            safety_rating["review_date"] = value
                        elif "Rating" in label and "Date" not in label:
                            safety_rating["rating"] = value
                        elif "Type" in label:
                            safety_rating["type"] = value
    except Exception as e:
        print(f"Error parsing safety rating: {e}")
    
    # Build final structured output
    result = {
        "record_metadata": {
            "source": "FMCSA SAFER",
            "snapshot_date": snapshot_date,
            "usdot_number": int(usdot_number) if usdot_number and usdot_number.isdigit() else None,
            "entity_type": entity_type if entity_type else None
        },
        "company_identity": {
            "legal_name": company_name if company_name else None,
            "dba_name": dba_name,
            "mc_mx_ff_numbers": mc_numbers if mc_numbers else [],
            "duns_number": duns,
            "state_carrier_id": None
        },
        "contact_info": {
            "phone": extract_table_value(soup, "Phone") or None,
            "physical_address": parse_address(physical_addr_text),
            "mailing_address": parse_address(mailing_addr_text)
        },
        "operating_status": {
            "usdot_status": usdot_status if usdot_status else None,
            "operating_authority_status": oa_status if oa_status else None,
            "out_of_service_date": oos_date,
            "mcs_150_form_date": mcs150_date if mcs150_date else None,
            "mcs_150_mileage": mileage,
            "mcs_150_mileage_year": mileage_year
        },
        "operations": {
            "fleet_size": {
                "power_units": power_units,
                "drivers": drivers
            },
            "operation_classifications": operation_classifications,
            "cargo_carried": cargo_carried
        },
        "safety_record": {
            "us_inspections": us_inspections,
            "us_crashes": us_crashes,
            "canada_inspections": canada_inspections,
            "canada_crashes": canada_crashes,
            "safety_rating": safety_rating
        }
    }
    
    return result


def fetch_carrier_info(usdot_number: str) -> Dict[str, Any]:
    """
    Fetch carrier information from FMCSA SAFER database.
    
    Args:
        usdot_number: USDOT number to query
        
    Returns:
        Dictionary containing parsed carrier information
    """
    url = 'https://safer.fmcsa.dot.gov/query.asp'
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    data = {
        'searchtype': 'ANY',
        'query_type': 'queryCarrierSnapshot',
        'query_param': 'USDOT',
        'query_string': usdot_number
    }
    
    try:
        response = requests.post(url, headers=headers, data=data, timeout=30)
        response.raise_for_status()
        
        parsed_data = parse_fmcsa_response(response.text)
        
        return parsed_data
    
    except requests.exceptions.RequestException as e:
        return {"error": f"Request failed: {str(e)}"}
    except Exception as e:
        return {"error": f"Parsing failed: {str(e)}"}


def save_to_database(data: Dict[str, Any], db_connection_string: Optional[str] = None) -> bool:
    """
    Save carrier data to PostgreSQL database.
    
    Args:
        data: Parsed carrier data dictionary
        db_connection_string: PostgreSQL connection string (or use env var DATABASE_URL)
        
    Returns:
        True if successful, False otherwise
    """
    if not DB_AVAILABLE:
        print("Warning: psycopg2 not installed. Database save skipped.")
        return False
    
    if "error" in data:
        print(f"Error in data, skipping database save: {data['error']}")
        return False
    
    # Get connection string from parameter or environment
    conn_str = db_connection_string or os.getenv("DATABASE_URL")
    if not conn_str:
        print("Warning: No database connection string provided. Set DATABASE_URL env var or pass db_connection_string.")
        return False
    
    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        
        metadata = data.get("record_metadata", {})
        identity = data.get("company_identity", {})
        contact = data.get("contact_info", {})
        status = data.get("operating_status", {})
        operations = data.get("operations", {})
        safety = data.get("safety_record", {})
        
        usdot = metadata.get("usdot_number")
        if not usdot:
            print("Error: No USDOT number in data")
            return False
        
        # Parse dates
        def parse_date(date_str):
            if not date_str:
                return None
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except:
                return None
        
        snapshot_date = parse_date(metadata.get("snapshot_date"))
        oos_date = parse_date(status.get("out_of_service_date"))
        mcs_date = parse_date(status.get("mcs_150_form_date"))
        
        # 1. Upsert main carrier record
        cur.execute("""
            INSERT INTO carriers (
                usdot_number, entity_type, snapshot_date,
                legal_name, dba_name, duns_number, phone,
                usdot_status, operating_authority_status, out_of_service_date,
                mcs_150_form_date, mcs_150_mileage, mcs_150_mileage_year,
                power_units, drivers, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
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
        """, (
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
        
        # 2. Upsert authority numbers (MC/MX/FF)
        cur.execute("DELETE FROM carrier_authority_numbers WHERE usdot_number = %s", (usdot,))
        mc_numbers = identity.get("mc_mx_ff_numbers", [])
        if mc_numbers:
            values = [(usdot, num) for num in mc_numbers]
            execute_values(cur, """
                INSERT INTO carrier_authority_numbers (usdot_number, authority_number)
                VALUES %s
                ON CONFLICT (usdot_number, authority_number) DO NOTHING
            """, values)
        
        # 3. Upsert addresses
        for addr_type, addr_data in [("PHYSICAL", contact.get("physical_address", {})),
                                      ("MAILING", contact.get("mailing_address", {}))]:
            cur.execute("""
                INSERT INTO carrier_addresses (
                    usdot_number, address_type, street, city, state, zip_code, country
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (usdot_number, address_type) DO UPDATE SET
                    street = EXCLUDED.street,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    country = EXCLUDED.country
            """, (
                usdot, addr_type,
                addr_data.get("street"),
                addr_data.get("city"),
                addr_data.get("state"),
                addr_data.get("zip_code"),
                addr_data.get("country", "US"),
            ))
        
        # 4. Upsert classifications
        cur.execute("DELETE FROM carrier_classifications WHERE usdot_number = %s", (usdot,))
        classifications = operations.get("operation_classifications", [])
        if classifications:
            values = [(usdot, cls) for cls in classifications]
            execute_values(cur, """
                INSERT INTO carrier_classifications (usdot_number, classification)
                VALUES %s
                ON CONFLICT (usdot_number, classification) DO NOTHING
            """, values)
        
        # 5. Upsert cargo
        cur.execute("DELETE FROM carrier_cargo WHERE usdot_number = %s", (usdot,))
        cargo = operations.get("cargo_carried", [])
        if cargo:
            values = [(usdot, c) for c in cargo]
            execute_values(cur, """
                INSERT INTO carrier_cargo (usdot_number, cargo_type)
                VALUES %s
                ON CONFLICT (usdot_number, cargo_type) DO NOTHING
            """, values)
        
        # 6. Upsert inspections (US and Canada)
        for region, insp_data in [("US", safety.get("us_inspections", {})),
                                   ("CANADA", safety.get("canada_inspections", {}))]:
            breakdown = insp_data.get("breakdown", {})
            vehicle = breakdown.get("vehicle", {})
            driver = breakdown.get("driver", {})
            hazmat = breakdown.get("hazmat", {})
            iep = breakdown.get("iep", {})
            
            cur.execute("""
                INSERT INTO carrier_inspections (
                    usdot_number, region,
                    total_inspections, total_iep_inspections,
                    vehicle_inspections, vehicle_oos, vehicle_oos_rate,
                    driver_inspections, driver_oos, driver_oos_rate,
                    hazmat_inspections, hazmat_oos, hazmat_oos_rate,
                    iep_inspections, iep_oos, iep_oos_rate
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
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
            """, (
                usdot, region,
                insp_data.get("total_inspections", 0),
                insp_data.get("total_iep_inspections", 0) if region == "US" else 0,
                vehicle.get("inspections", 0),
                vehicle.get("out_of_service", 0),
                vehicle.get("out_of_service_rate_pct"),
                driver.get("inspections", 0),
                driver.get("out_of_service", 0),
                driver.get("out_of_service_rate_pct"),
                hazmat.get("inspections", 0) if region == "US" else 0,
                hazmat.get("out_of_service", 0) if region == "US" else 0,
                hazmat.get("out_of_service_rate_pct") if region == "US" else None,
                iep.get("inspections", 0) if region == "US" else 0,
                iep.get("out_of_service", 0) if region == "US" else 0,
                iep.get("out_of_service_rate_pct") if region == "US" else None,
            ))
        
        # 7. Upsert crashes (US and Canada)
        for region, crash_data in [("US", safety.get("us_crashes", {})),
                                    ("CANADA", safety.get("canada_crashes", {}))]:
            cur.execute("""
                INSERT INTO carrier_crashes (
                    usdot_number, region, fatal, injury, tow, total
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (usdot_number, region) DO UPDATE SET
                    fatal = EXCLUDED.fatal,
                    injury = EXCLUDED.injury,
                    tow = EXCLUDED.tow,
                    total = EXCLUDED.total
            """, (
                usdot, region,
                crash_data.get("fatal", 0),
                crash_data.get("injury", 0),
                crash_data.get("tow", 0),
                crash_data.get("total", 0),
            ))
        
        # 8. Upsert safety rating
        rating_data = safety.get("safety_rating", {})
        rating_date = parse_date(rating_data.get("rating_date"))
        review_date = parse_date(rating_data.get("review_date"))
        
        cur.execute("""
            INSERT INTO carrier_safety_ratings (
                usdot_number, rating, rating_date, review_date, rating_type
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (usdot_number) DO UPDATE SET
                rating = EXCLUDED.rating,
                rating_date = EXCLUDED.rating_date,
                review_date = EXCLUDED.review_date,
                rating_type = EXCLUDED.rating_type
        """, (
            usdot,
            rating_data.get("rating"),
            rating_date,
            review_date,
            rating_data.get("type"),
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✓ Successfully saved carrier {usdot} to database")
        return True
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False
    except Exception as e:
        print(f"Error saving to database: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False


def main():
    """Main function to run the scraper."""
    import sys
    
    usdot_number = "4187979"
    save_to_db = False
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        usdot_number = sys.argv[1]
    if len(sys.argv) > 2 and sys.argv[2].lower() in ('--save', '--save-db', '-s'):
        save_to_db = True
    
    print(f"Fetching information for USDOT: {usdot_number}")
    
    result = fetch_carrier_info(usdot_number)
    
    if "error" in result:
        print(f"Error: {result['error']}")
        return result
    
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    if save_to_db:
        save_to_database(result)
    
    return result


if __name__ == "__main__":
    main()

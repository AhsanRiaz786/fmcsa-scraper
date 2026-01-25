/**
 * HTML parsing logic for FMCSA SAFER database responses.
 * Maps HTML to TypeScript types from carrier.types.ts
 */

import * as cheerio from 'cheerio';
import type {
  Snapshot,
  OperationClassificationValue,
  CarrierOperationValue,
  CargoCarriedValue,
  UsInspectionSummary24Mo,
  CanadianInspectionSummary24Mo,
  CarrierSafetyRating,
} from './types/carrier.types';

// --- Helper Functions ---

function cleanText(text: string | undefined | null): string | null {
  if (!text) return null;
  const cleaned = text.replace(/\s+/g, ' ').replace(/&nbsp;/g, ' ').trim();
  return cleaned === '' || cleaned === 'None' || cleaned === '--' ? null : cleaned;
}

function parseDate(dateStr: string | null | undefined): Date | null {
  if (!dateStr) return null;
  try {
    // Parse MM/DD/YYYY format
    const parts = dateStr.trim().split('/');
    if (parts.length !== 3) return null;
    const month = parseInt(parts[0], 10) - 1; // Month is 0-indexed
    const day = parseInt(parts[1], 10);
    const year = parseInt(parts[2], 10);
    const date = new Date(year, month, day);
    if (isNaN(date.getTime())) return null;
    return date;
  } catch {
    return null;
  }
}

function parseNumber(numStr: string | null | undefined): number | null {
  if (!numStr) return null;
  const cleaned = numStr.replace(/,/g, '').trim();
  const num = parseInt(cleaned, 10);
  return isNaN(num) ? null : num;
}

function parsePercent(percentStr: string | null | undefined): string | null {
  if (!percentStr) return null;
  const cleaned = percentStr.trim().replace(/\s+/g, '');
  // Ensure it ends with %
  if (cleaned && !cleaned.endsWith('%')) {
    return `${cleaned}%`;
  }
  return cleaned || null;
}

// --- Type Mappings ---

const OPERATION_CLASS_MAP: Record<string, OperationClassificationValue> = {
  'Auth. For Hire': 'AUTHORIZED_FOR_HIRE',
  'Exempt For Hire': 'EXEMPT_FOR_HIRE',
  'Private(Property)': 'PRIVATE_PROPERTY',
  'Priv. Pass. (Business)': 'PRIVATE_PASSENGERS_BUSINESS',
  'Priv. Pass.(Non-business)': 'PRIVATE_PASSENGERS_NON_BUSINESS',
  'Migrant': 'MIGRANT',
  'U.S. Mail': 'US_MAIL',
  "Fed. Gov't": 'FEDERAL_GOVERNMENT',
  "State Gov't": 'STATE_GOVERNMENT',
  "Local Gov't": 'LOCAL_GOVERNMENT',
  'Indian Nation': 'INDIAN_NATION',
};

const CARRIER_OPERATION_MAP: Record<string, CarrierOperationValue> = {
  'Interstate': 'INTERSTATE',
  'Intrastate Only (HM)': 'INTRASTATE_ONLY_HM',
  'Intrastate Only (Non-HM)': 'INTRASTATE_ONLY_NON_HM',
};

const CARGO_MAP: Record<string, CargoCarriedValue> = {
  'General Freight': 'GENERAL_FREIGHT',
  'Household Goods': 'HOUSEHOLD_GOODS',
  'Metal: sheets, coils, rolls': 'METAL_SHEETS_COILS_ROLLS',
  'Motor Vehicles': 'MOTOR_VEHICLES',
  'Drive/Tow away': 'DRIVE_TOW_AWAY',
  'Logs, Poles, Beams, Lumber': 'LOGS_POLES_BEAMS_LUMBER',
  'Building Materials': 'BUILDING_MATERIALS',
  'Mobile Homes': 'MOBILE_HOMES',
  'Machinery, Large Objects': 'MACHINERY_LARGE_OBJECTS',
  'Fresh Produce': 'FRESH_PRODUCE',
  'Liquids/Gases': 'LIQUIDS_GASES',
  'Intermodal Cont.': 'INTERMODAL_CONTAINERS',
  'Passengers': 'PASSENGERS',
  'Oilfield Equipment': 'OILFIELD_EQUIPMENT',
  'Livestock': 'LIVESTOCK',
  'Grain, Feed, Hay': 'GRAIN_FEED_HAY',
  'Coal/Coke': 'COAL_COKE',
  'Meat': 'MEAT',
  'Garbage/Refuse': 'GARBAGE_REFUSE',
  'US Mail': 'US_MAIL',
  'Chemicals': 'CHEMICALS',
  'Commodities Dry Bulk': 'COMMODITIES_DRY_BULK',
  'Refrigerated Food': 'REFRIGERATED_FOOD',
  'Beverages': 'BEVERAGES',
  'Paper Products': 'PAPER_PRODUCTS',
  'Utilities': 'UTILITIES',
  'Agricultural/Farm Supplies': 'AGRICULTURAL_FARM_SUPPLIES',
  'Construction': 'CONSTRUCTION',
  'Water Well': 'WATER_WELL',
  'MOTOR VEHICLES': 'MOTOR_VEHICLES', // Handle uppercase variant
};

// --- Address Parser ---

function parseAddress(raw: string | null | undefined): {
  street: string | null;
  city: string | null;
  state: string | null;
  zip: string | null;
  country: string | null;
} | null {
  if (!raw) return null;

  // Preserve newlines (from <br>); normalize spaces within lines
  const lines: string[] = [];
  for (const line of raw.replace(/&nbsp;/g, ' ').split('\n')) {
    const cleaned = line.replace(/[ \t]+/g, ' ').trim();
    if (cleaned) {
      lines.push(cleaned);
    }
  }

  let street: string | null = null;
  let city: string | null = null;
  let state: string | null = null;
  let zip: string | null = null;

  if (lines.length >= 2) {
    // FMCSA format: line1 = street, line2 = "CITY, ST  ZIP"
    street = lines[0];
    const rest = lines.slice(1).join(' ');
    const match = rest.match(/([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$/i);
    if (match) {
      state = match[1].toUpperCase();
      zip = match[2];
      let remaining = rest.substring(0, match.index).trim();
      if (remaining.endsWith(',')) {
        remaining = remaining.slice(0, -1).trim();
      }
      city = remaining || null;
    } else {
      street = lines.join(' ');
    }
  } else if (lines.length === 1) {
    // Single line: use state/zip + comma logic
    const text = lines[0];
    const match = text.match(/([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$/i);
    street = text;
    if (match) {
      state = match[1].toUpperCase();
      zip = match[2];
      let remaining = text.substring(0, match.index).trim();
      if (remaining.endsWith(',')) {
        remaining = remaining.slice(0, -1).trim();
      }
      if (remaining.includes(',')) {
        const parts = remaining.split(',').map(s => s.trim());
        street = parts.slice(0, -1).join(', ').trim() || null;
        city = parts[parts.length - 1] || null;
      } else {
        street = remaining;
      }
    }
  }

  return {
    street,
    city,
    state,
    zip,
    country: 'US',
  };
}

function tdTextWithBr(td: cheerio.Cheerio): string {
  if (!td.length) return '';
  let raw = td.html() || '';
  for (const br of ['<br>', '<br/>', '<br />', '<BR>', '<BR/>', '<BR />']) {
    raw = raw.replace(new RegExp(br, 'gi'), '\n');
  }
  const $temp = cheerio.load(raw);
  let s = $temp('body').text();
  s = s.replace(/&nbsp;/g, ' ');
  s = s.replace(/[ \t]+/g, ' '); // collapse spaces/tabs, keep newlines
  return s.trim();
}

// --- Table Extraction Helpers ---

function extractTableValue($: cheerio.CheerioAPI, label: string): string | null {
  try {
    const th = $('th').filter((_, el) => {
      const text = $(el).text();
      return new RegExp(label, 'i').test(text);
    }).first();

    if (th.length) {
      const td = th.next('td');
      if (td.length) {
        return cleanText(td.text());
      }
    }
  } catch {
    // Ignore errors
  }
  return null;
}

function extractCheckedItems<T>(
  $: cheerio.CheerioAPI,
  sectionLabel: string,
  map: Record<string, T>
): T[] {
  const results: T[] = [];
  try {
    // Find the header link (e.g. <A ...>Cargo Carried:</A>) or text
    let header = $('a').filter((_, el) => {
      return new RegExp(sectionLabel, 'i').test($(el).text());
    }).first();

    if (!header.length) {
      // Fallback: find text node
      $('*').each((_, el) => {
        const text = $(el).text();
        if (new RegExp(sectionLabel, 'i').test(text)) {
          header = $(el);
          return false; // break
        }
      });
    }

    if (header.length) {
      // Navigate up to the containing row
      const headerRow = header.closest('tr');
      if (headerRow.length) {
        // The check-box table is usually in the NEXT row
        const contentRow = headerRow.next('tr');
        if (contentRow.length) {
          // Find all 'X' marks within this specific content row
          contentRow.find('td.queryfield').each((_, cell) => {
            const cellText = $(cell).text().trim();
            if (cellText === 'X') {
              const labelTd = $(cell).next('td');
              if (labelTd.length) {
                const label = cleanText(labelTd.text());
                if (label && map[label]) {
                  results.push(map[label]);
                }
              }
            }
          });
        }
      }
    }
  } catch {
    // Ignore errors
  }
  return results;
}

// --- Inspection Table Parser ---

function parseInspectionTable(
  $: cheerio.CheerioAPI,
  sectionLabel: string,
  isUS: boolean
): UsInspectionSummary24Mo | CanadianInspectionSummary24Mo | null {
  try {
    // Find the section header
    const sectionHeader = $('a').filter((_, el) => {
      return new RegExp(sectionLabel, 'i').test($(el).text());
    }).first();

    if (!sectionHeader.length) return null;

    // Find the "as of" date
    let asOfDate = new Date(); // Default to today
    const dateText = $('b').filter((_, el) => {
      return /prior to:/i.test($(el).text());
    }).first().text();
    const dateMatch = dateText.match(/(\d{2}\/\d{2}\/\d{4})/);
    if (dateMatch) {
      const parsed = parseDate(dateMatch[1]);
      if (parsed) asOfDate = parsed;
    }

    // Find the Inspections table
    const inspectionTables = $('table[summary="Inspections"]');
    const tableIndex = isUS ? 0 : 1;
    const inspTable = inspectionTables.eq(tableIndex);

    if (!inspTable.length) return null;

    const rows = inspTable.find('tr');
    if (rows.length < 2) return null;

    // Row 1: Inspections (index 1, since 0 is header)
    const dataRow = rows.eq(1);
    const cells = dataRow.find('td.queryfield');

    // Row 2: Out of Service
    const oosRow = rows.eq(2);
    const oosCells = oosRow.find('td.queryfield');

    // Row 3: Out of Service %
    const percentRow = rows.eq(3);
    const percentCells = percentRow.find('td.queryfield');

    if (isUS) {
      // US table has 4 columns: Vehicle, Driver, Hazmat, IEP
      const summary: UsInspectionSummary24Mo = {
        as_of_date: asOfDate,
        inspections: {
          vehicle: parseNumber(cells.eq(0).text()) || null,
          driver: parseNumber(cells.eq(1).text()) || null,
          hazmat: parseNumber(cells.eq(2).text()) || null,
          iep: parseNumber(cells.eq(3).text()) || null,
        },
        out_of_service: {
          vehicle: parseNumber(oosCells.eq(0).text()) || null,
          driver: parseNumber(oosCells.eq(1).text()) || null,
          hazmat: parseNumber(oosCells.eq(2).text()) || null,
          iep: parseNumber(oosCells.eq(3).text()) || null,
        },
        out_of_service_pct: {
          vehicle: parsePercent(percentCells.eq(0).text()) || null,
          driver: parsePercent(percentCells.eq(1).text()) || null,
          hazmat: parsePercent(percentCells.eq(2).text()) || null,
          iep: parsePercent(percentCells.eq(3).text()) || null,
        },
      };

      // Extract total inspections
      const totalInspText = $('*').filter((_, el) => {
        return /Total Inspections:/i.test($(el).text());
      }).first().text();
      const totalMatch = totalInspText.match(/Total Inspections:\s*<FONT[^>]*>(\d+)<\/FONT>/i);
      if (totalMatch) {
        summary.total_inspections = parseInt(totalMatch[1], 10) || null;
      }

      // Extract total IEP inspections
      const totalIepText = $('*').filter((_, el) => {
        return /Total IEP Inspections:/i.test($(el).text());
      }).first().text();
      const totalIepMatch = totalIepText.match(/Total IEP Inspections:\s*<FONT[^>]*>(\d+)<\/FONT>/i);
      if (totalIepMatch) {
        summary.total_iep_inspections = parseInt(totalIepMatch[1], 10) || null;
      }

      // Extract national average (if present)
      const natAvgRow = rows.eq(4);
      if (natAvgRow.length) {
        const natAvgCells = natAvgRow.find('td');
        const dateText = natAvgRow.find('span').text();
        const dateMatch = dateText.match(/(\d{2}\/\d{2}\/\d{4})/);
        if (dateMatch) {
          const date = parseDate(dateMatch[1]);
          summary.national_average_as_of_date = date ? date.toISOString().split('T')[0] : null;
        }
        const natAvgPercents = natAvgRow.find('font');
        if (natAvgPercents.length >= 4) {
          summary.national_average_pct = {
            vehicle: parsePercent(natAvgPercents.eq(0).text()) || null,
            driver: parsePercent(natAvgPercents.eq(1).text()) || null,
            hazmat: parsePercent(natAvgPercents.eq(2).text()) || null,
            iep: parsePercent(natAvgPercents.eq(3).text()) || null,
          };
        }
      }

      return summary;
    } else {
      // Canada table has 2 columns: Vehicle, Driver
      const summary: CanadianInspectionSummary24Mo = {
        as_of_date: asOfDate,
        inspections: {
          vehicle: parseNumber(cells.eq(0).text()) || null,
          driver: parseNumber(cells.eq(1).text()) || null,
        },
        out_of_service: {
          vehicle: parseNumber(oosCells.eq(0).text()) || null,
          driver: parseNumber(oosCells.eq(1).text()) || null,
        },
        out_of_service_pct: {
          vehicle: parsePercent(percentCells.eq(0).text()) || null,
          driver: parsePercent(percentCells.eq(1).text()) || null,
        },
      };

      // Extract total inspections
      const totalInspText = $('*').filter((_, el) => {
        return /Total inspections:/i.test($(el).text());
      }).first().text();
      const totalMatch = totalInspText.match(/Total inspections:\s*<FONT[^>]*>(\d+)<\/FONT>/i);
      if (totalMatch) {
        summary.total_inspections = parseInt(totalMatch[1], 10) || null;
      }

      return summary;
    }
  } catch {
    return null;
  }
}

function parseCrashTable($: cheerio.CheerioAPI, isUS: boolean): {
  fatal: number | null;
  injury: number | null;
  tow: number | null;
  total: number | null;
} | null {
  try {
    const crashTables = $('table[summary="Crashes"]');
    const tableIndex = isUS ? 0 : 1;
    const crashTable = crashTables.eq(tableIndex);

    if (!crashTable.length) return null;

    const rows = crashTable.find('tr');
    if (rows.length < 2) return null;

    const dataRow = rows.eq(1);
    const cells = dataRow.find('td.queryfield');

    if (cells.length >= 4) {
      return {
        fatal: parseNumber(cells.eq(0).text()) || null,
        injury: parseNumber(cells.eq(1).text()) || null,
        tow: parseNumber(cells.eq(2).text()) || null,
        total: parseNumber(cells.eq(3).text()) || null,
      };
    }
  } catch {
    // Ignore errors
  }
  return null;
}

/**
 * Recursively check if the object contains ANY number > 0.
 * Client requirement: only include inspection summary if at least one value is non-zero.
 */
function hasNonZeroValue(obj: unknown): boolean {
  if (obj === null || obj === undefined) return false;

  if (typeof obj === 'number') {
    return obj > 0;
  }

  if (typeof obj === 'object') {
    return Object.values(obj).some((val) => hasNonZeroValue(val));
  }

  return false;
}

// --- Main Parser Function ---

export function parseHtmlToSnapshot(html: string): Snapshot | null {
  const $ = cheerio.load(html) as cheerio.CheerioAPI;

  // Extract USDOT number (required field)
  const usdotNumber = extractTableValue($, 'USDOT Number:');
  if (!usdotNumber) return null; // Invalid scrape

  // Extract basic info
  const entityType = extractTableValue($, 'Entity Type:') || 'UNKNOWN';
  const usdotStatus = extractTableValue($, 'USDOT Status:') as Snapshot['usdot_status'] | null;
  const outOfServiceDate = parseDate(extractTableValue($, 'Out of Service Date:'));
  const stateCarrierId = extractTableValue($, 'State Carrier ID Number:');
  const mcs150FormDate = parseDate(extractTableValue($, 'MCS-150 Form Date:'));
  const rawAuth = extractTableValue($, 'Operating Authority Status:');
  let operatingAuthorityStatus: string | null = null;
  if (rawAuth) {
    let cleaned = rawAuth
      .split('*')[0]
      .replace(/\s+Please Note:.*$/i, '')
      .replace(/\s+For Licensing and Insurance details click here\.?\s*$/i, '')
      .replace(/\s+For Licensing.*$/i, '')
      .trim();
    operatingAuthorityStatus = cleaned || null;
  }

  // Extract MCS-150 Mileage
  const mileageText = extractTableValue($, 'MCS-150 Mileage');
  let mcs150Mileage: number | null = null;
  let mcs150MileageYear: number | null = null;
  if (mileageText) {
    const match = mileageText.match(/([\d,]+)\s*\((\d{4})\)/);
    if (match) {
      mcs150Mileage = parseNumber(match[1]);
      mcs150MileageYear = parseNumber(match[2]);
    }
  }

  // Extract MC/MX/FF Numbers
  let mcNumber: string | null = null;
  let mxNumber: string | null = null;
  let ffNumber: string | null = null;
  const mcTd = $('th').filter((_, el) => /MC\/MX\/FF Number/i.test($(el).text())).first();
  if (mcTd.length) {
    const td = mcTd.next('td');
    td.find('a').each((_, link) => {
      const text = cleanText($(link).text());
      if (text) {
        if (text.startsWith('MC-')) mcNumber = text;
        else if (text.startsWith('MX-')) mxNumber = text;
        else if (text.startsWith('FF-')) ffNumber = text;
      }
    });
  }

  // Extract company name
  let legalName: string | null = null;
  try {
    const companyHeader = $('font[size="3"][face="arial"]').first();
    if (companyHeader.length) {
      const bTag = companyHeader.find('b').first();
      if (bTag.length) {
        legalName = cleanText(bTag.text());
      }
    }
  } catch {
    // Ignore
  }

  const dbaName = cleanText(extractTableValue($, 'DBA Name:'));
  const dunsNumber = cleanText(extractTableValue($, 'DUNS Number:'));
  const phone = cleanText(extractTableValue($, 'Phone:'));

  // Extract addresses
  let physicalAddressText = '';
  const physicalAddrTd = $('#physicaladdressvalue');
  if (physicalAddrTd.length) {
    physicalAddressText = tdTextWithBr(physicalAddrTd);
  } else {
    physicalAddressText = extractTableValue($, 'Physical Address:') || '';
  }

  let mailingAddressText = '';
  const mailingAddrTd = $('#mailingaddressvalue');
  if (mailingAddrTd.length) {
    mailingAddressText = tdTextWithBr(mailingAddrTd);
  } else {
    mailingAddressText = extractTableValue($, 'Mailing Address:') || '';
  }

  // Extract fleet size
  const powerUnits = parseNumber(extractTableValue($, 'Power Units:'));
  const drivers = parseNumber(extractTableValue($, 'Drivers:'));
  const nonCmvUnits = parseNumber(extractTableValue($, 'Non-CMV Units:'));

  // Extract operation classifications and cargo
  const operationClassification = extractCheckedItems($, 'Operation Classification', OPERATION_CLASS_MAP);
  const carrierOperation = extractCheckedItems($, 'Carrier Operation', CARRIER_OPERATION_MAP);
  const cargoCarried = extractCheckedItems($, 'Cargo Carried', CARGO_MAP);

  // Extract inspection summaries (client: only include if at least one value is non-zero)
  const usSummary = parseInspectionTable($, 'Inspections/Crashes In US', true) as UsInspectionSummary24Mo | null;
  const canSummary = parseInspectionTable($, 'Inspections/Crashes In Canada', false) as CanadianInspectionSummary24Mo | null;

  const usCrashes = parseCrashTable($, true);
  const canadianCrashes = parseCrashTable($, false);

  if (usSummary && usCrashes) usSummary.crashes = usCrashes;
  if (canSummary && canadianCrashes) canSummary.crashes = canadianCrashes;

  const usInspectionSummary = usSummary && hasNonZeroValue(usSummary) ? usSummary : undefined;
  const canadianInspectionSummary = canSummary && hasNonZeroValue(canSummary) ? canSummary : undefined;

  // Extract safety rating
  let carrierSafetyRating: CarrierSafetyRating | null = null;
  try {
    const ratingTable = $('table[summary="Review Information"]');
    if (ratingTable.length) {
      let rating: string | null = null;
      let ratingDate: Date | null = null;
      let reviewDate: Date | null = null;
      let type: string | null = null;
      let currentAsOfDate = new Date();

      // Find "current as of" date
      const currentAsOfText = $('b').filter((_, el) => {
        return /The rating below is current as of:/i.test($(el).text());
      }).first().text();
      const dateMatch = currentAsOfText.match(/(\d{2}\/\d{2}\/\d{4})/);
      if (dateMatch) {
        const parsed = parseDate(dateMatch[1]);
        if (parsed) currentAsOfDate = parsed;
      }

      ratingTable.find('tr').each((_, row) => {
        const ths = $(row).find('th.querylabelbkg');
        const tds = $(row).find('td.queryfield');
        ths.each((i, th) => {
          const label = cleanText($(th).text());
          const value = cleanText(tds.eq(i).text());
          if (label && value && value.toLowerCase() !== 'none') {
            if (/Rating Date:/i.test(label)) {
              ratingDate = parseDate(value);
            } else if (/Review Date:/i.test(label)) {
              reviewDate = parseDate(value);
            } else if (/^Rating:$/i.test(label)) {
              rating = value;
            } else if (/^Type:$/i.test(label)) {
              type = value;
            }
          }
        });
      });

      // Only include if rating is not "N/A" or "None"
      if (rating !== null && rating !== undefined) {
        const ratingStr = String(rating);
        const ratingLower = ratingStr.toLowerCase();
        if (ratingLower !== 'n/a' && ratingLower !== 'none') {
          carrierSafetyRating = {
            current_as_of_date: currentAsOfDate,
            rating: ratingStr,
            rating_date: ratingDate,
            review_date: reviewDate,
            type,
          };
        }
      }
    }
  } catch {
    // Ignore errors
  }

  // Build snapshot
  const snapshot: Snapshot = {
    usdot_number: usdotNumber,
    entity_type: entityType,
    usdot_status: usdotStatus || undefined,
    out_of_service_date: outOfServiceDate || undefined,
    state_carrier_id_number: stateCarrierId || undefined,
    mcs_150_form_date: mcs150FormDate || undefined,
    mcs_150_mileage_year: mcs150MileageYear || undefined,
    mcs_150_mileage: mcs150Mileage || undefined,
    operating_authority_status: operatingAuthorityStatus || undefined,
    mc_number: mcNumber || undefined,
    mx_number: mxNumber || undefined,
    ff_number: ffNumber || undefined,
    legal_name: legalName || undefined,
    dba_name: dbaName || undefined,
    physical_address: parseAddress(physicalAddressText),
    mailing_address: parseAddress(mailingAddressText),
    phone: phone || undefined,
    duns_number: dunsNumber || undefined,
    power_units: powerUnits || undefined,
    non_cmv_units: nonCmvUnits || undefined,
    drivers: drivers || undefined,
    operation_classification: operationClassification.length > 0 ? operationClassification : undefined,
    carrier_operation: carrierOperation.length > 0 ? carrierOperation : undefined,
    cargo_carried: cargoCarried.length > 0 ? cargoCarried : undefined,
    us_inspection_summary_24mo: usInspectionSummary,
    canadian_inspection_summary_24mo: canadianInspectionSummary,
    carrier_safety_rating: carrierSafetyRating || undefined,
  };

  return snapshot;
}

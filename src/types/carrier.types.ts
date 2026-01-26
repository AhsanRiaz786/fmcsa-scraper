export type Snapshot = {
  /** FMCSA: "Entity Type" (e.g., BROKER, CARRIER) */
  entity_type: string;

  /** FMCSA: "USDOT Status" */
  usdot_status?:
  | 'ACTIVE'
  | 'OUT-OF-SERVICE'
  | 'NOT AUTHORIZED'
  | `AUTHORIZED FOR ${string}`
  | 'INACTIVE USDOT NUMBER'
  | 'Inactive USDOT Number';

  /** FMCSA: "Out of Service Date" (None => null/undefined) */
  out_of_service_date?: Date | null;

  /** FMCSA: "USDOT Number" */
  usdot_number: string;

  /** FMCSA: "State Carrier ID Number" (often blank) */
  state_carrier_id_number?: string | null;

  /** FMCSA: "MCS-150 Form Date" (often blank) */
  mcs_150_form_date?: Date | null;

  /** FMCSA: "MCS-150 Mileage (Year)" (often blank) */
  mcs_150_mileage_year?: number | null;

  /** FMCSA: "MCS-150 Mileage" (not always present, but commonly paired with mileage year) */
  mcs_150_mileage?: number | null;

  /** FMCSA: "Operating Authority Status" */
  operating_authority_status?: string | null;

  /** FMCSA: "MC/MX/FF Number(s)" */
  mc_number?: string | null;
  mx_number?: string | null;
  ff_number?: string | null;

  /** COMPANY INFORMATION */
  legal_name?: string | null;
  dba_name?: string | null;

  physical_address?: {
    street?: string | null;
    city?: string | null;
    state?: string | null;
    zip?: string | null;
    country?: string | null;
  } | null;

  mailing_address?: {
    street?: string | null;
    city?: string | null;
    state?: string | null;
    zip?: string | null;
    country?: string | null;
  } | null;

  phone?: string | null;

  /** FMCSA: "DUNS Number" ("--" => null/undefined) */
  duns_number?: string | null;

  /** FMCSA: "Power Units" */
  power_units?: number | null;

  /** FMCSA: "Non-CMV Units" (often blank) */
  non_cmv_units?: number | null;

  /** FMCSA: "Drivers" (often blank) */
  drivers?: number | null;

  /** FMCSA: "Operation Classification" (checked items only) */
  operation_classification?: OperationClassificationValue[] | null;

  /** FMCSA: "Carrier Operation" (checked items only) */
  carrier_operation?: CarrierOperationValue[] | null;

  /** FMCSA: "Cargo Carried" (checked items only) */
  cargo_carried?: CargoCarriedValue[] | null;

  /** FMCSA: "Cargo Carried" custom/other cargo (e.g., "Compost", "Horse Manure") */
  cargo_carried_other?: string | null;

  /**
   * FMCSA: "US Inspection results for 24 months prior to: <date>"
   * Percentages are stored as strings (e.g., "0%") to match the FMCSA display.
   *
   * Note: only include this object if at least one of the values is non-zero.
   */
  us_inspection_summary_24mo?: UsInspectionSummary24Mo | null;

  /**
   * FMCSA: "Canadian Inspection results for 24 months prior to: <date>"
   * Percentages are stored as strings (e.g., "0%") to match the FMCSA display.
   *
   * Note: only include this object if at least one of the values is non-zero.
   */
  canadian_inspection_summary_24mo?: CanadianInspectionSummary24Mo | null;

  /**
   * FMCSA: "Carrier Safety Rating"
   * The rating block is current as of the date shown on the FMCSA page.
   *
   * Note: only include this object if the rating is not "N/A" or "None".
   */
  carrier_safety_rating?: CarrierSafetyRating | null;
};

export type OperationClassificationValue =
  | 'AUTHORIZED_FOR_HIRE'
  | 'EXEMPT_FOR_HIRE'
  | 'PRIVATE_PROPERTY'
  | 'PRIVATE_PASSENGERS_BUSINESS'
  | 'PRIVATE_PASSENGERS_NON_BUSINESS'
  | 'MIGRANT'
  | 'US_MAIL'
  | 'FEDERAL_GOVERNMENT'
  | 'STATE_GOVERNMENT'
  | 'LOCAL_GOVERNMENT'
  | 'INDIAN_NATION';

export type CarrierOperationValue =
  | 'INTERSTATE'
  | 'INTRASTATE_ONLY_HM'
  | 'INTRASTATE_ONLY_NON_HM';

export type CargoCarriedValue =
  | 'GENERAL_FREIGHT'
  | 'HOUSEHOLD_GOODS'
  | 'METAL_SHEETS_COILS_ROLLS'
  | 'MOTOR_VEHICLES'
  | 'DRIVE_TOW_AWAY'
  | 'LOGS_POLES_BEAMS_LUMBER'
  | 'BUILDING_MATERIALS'
  | 'MOBILE_HOMES'
  | 'MACHINERY_LARGE_OBJECTS'
  | 'FRESH_PRODUCE'
  | 'LIQUIDS_GASES'
  | 'INTERMODAL_CONTAINERS'
  | 'PASSENGERS'
  | 'OILFIELD_EQUIPMENT'
  | 'LIVESTOCK'
  | 'GRAIN_FEED_HAY'
  | 'COAL_COKE'
  | 'MEAT'
  | 'GARBAGE_REFUSE'
  | 'US_MAIL'
  | 'CHEMICALS'
  | 'COMMODITIES_DRY_BULK'
  | 'REFRIGERATED_FOOD'
  | 'BEVERAGES'
  | 'PAPER_PRODUCTS'
  | 'UTILITIES'
  | 'AGRICULTURAL_FARM_SUPPLIES'
  | 'CONSTRUCTION'
  | 'WATER_WELL';

export type UsInspectionSummary24Mo = {
  /** The “prior to” date, e.g. 01/24/2026 */
  as_of_date: Date;

  total_inspections?: number | null;
  total_iep_inspections?: number | null;

  /** Row: Inspections */
  inspections?: {
    vehicle?: number | null; // e.g. 10
    driver?: number | null; // e.g. 10
    hazmat?: number | null; // e.g. 10
    iep?: number | null; // e.g. 10
  };

  /** Row: Out of Service */
  out_of_service?: {
    vehicle?: number | null; // e.g. 10
    driver?: number | null; // e.g. 10
    hazmat?: number | null; // e.g. 10
    iep?: number | null; // e.g. 10
  };

  /** Row: Out of Service % */
  out_of_service_pct?: {
    vehicle?: string | null; // e.g. "22.26%"
    driver?: string | null; // e.g. "22.26%"
    hazmat?: string | null; // e.g. "22.26%"
    iep?: string | null; // e.g. "22.26%"
  };

  /** Row: Nat'l Average % */
  national_average_as_of_date?: string | null; // e.g. "2026-01-24"
  national_average_pct?: {
    vehicle?: string | null; // e.g. "22.26%"
    driver?: string | null; // e.g. "22.26%"
    hazmat?: string | null; // e.g. "22.26%"
    iep?: string | null; // e.g. "22.26%"
  };

  /**
   * FMCSA: "Crashes reported to FMCSA by states for 24 months prior to: <date>"
   * Table: Type | Fatal | Injury | Tow | Total
   */
  crashes?: {
    fatal?: number | null;
    injury?: number | null;
    tow?: number | null;
    total?: number | null;
  };
};

export type CanadianInspectionSummary24Mo = {
  /** The “prior to” date, e.g. 01/24/2026 */
  as_of_date: Date;

  total_inspections?: number | null;

  /** Row: Inspections */
  inspections?: {
    vehicle?: number | null; // e.g. 0
    driver?: number | null; // e.g. 0
  };

  /** Row: Out of Service */
  out_of_service?: {
    vehicle?: number | null; // e.g. 0
    driver?: number | null; // e.g. 0
  };

  /** Row: Out of Service % */
  out_of_service_pct?: {
    vehicle?: string | null; // e.g. "0%"
    driver?: string | null; // e.g. "0%"
  };

  /**
   * FMCSA: "Crashes results for 24 months prior to: <date>"
   * Table: Type | Fatal | Injury | Tow | Total
   */
  crashes?: {
    fatal?: number | null;
    injury?: number | null;
    tow?: number | null;
    total?: number | null;
  };
};

export type CarrierSafetyRating = {
  /** “The rating below is current as of: …” */
  current_as_of_date: Date;

  /** “Rating Date: …” (None => null) */
  rating_date?: Date | null;

  /** “Review Date: …” (None => null) */
  review_date?: Date | null;

  /** “Rating: …” (None => null) */
  rating?: string | null;

  /** “Type: …” (None => null) */
  type?: string | null;
};

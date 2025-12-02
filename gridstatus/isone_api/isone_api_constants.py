ISONE_CAPACITY_FORECAST_7_DAY_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Publish Time",
    "High Temperature Boston",
    "Dew Point Boston",
    "High Temperature Hartford",
    "Dew Point Hartford",
    "Generating Capacity Position",
    "Total Capacity Supply Obligation",
    "Anticipated Cold Weather Outages",
    "Other Generation Outages",
    "Anticipated Delist MW Offered",
    "Total Generation Available",
    "Import at Time of Peak",
    "Total Available Generation and Imports",
    "Projected Peak Load",
    "Replacement Reserve Requirement",
    "Required Reserve",
    "Required Reserve Including Replacement",
    "Total Load Plus Required Reserve",
    "Projected Surplus or Deficiency",
    "Available Demand Response Resources",
    "Available Realtime Emergency Generation",
    "Load Relief Actions Anticipated",
    "Power Watch",
    "Power Warning",
    "Cold Weather Watch",
    "Cold Weather Warning",
    "Cold Weather Event",
]

ISONE_CONSTRAINT_DAY_AHEAD_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Constraint Name",
    "Contingency Name",
    "Interface Flag",
    "Marginal Value",
]

ISONE_CONSTRAINT_FIVE_MIN_PRELIM_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Constraint Name",
    "Contingency Name",
    "Marginal Value",
]

ISONE_CONSTRAINT_FIVE_MIN_FINAL_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Constraint Name",
    "Marginal Value",
]

ISONE_CONSTRAINT_FIFTEEN_MIN_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Constraint Name",
    "Marginal Value",
]

# Reserve zone price columns used across multiple real-time methods
ISONE_RESERVE_ZONE_FLOAT_COLUMNS = [
    "Ten Min Spin Requirement",
    "TMNSR Clearing Price",
    "TMNSR Designated MW",
    "TMOR Clearing Price",
    "TMOR Designated MW",
    "TMSR Clearing Price",
    "TMSR Designated MW",
    "Total 10 Min Requirement",
    "Total 30 Min Requirement",
]

ISONE_RESERVE_ZONE_ALL_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Reserve Zone Id",
    "Reserve Zone Name",
] + ISONE_RESERVE_ZONE_FLOAT_COLUMNS

# Column mapping for reserve zone data (used across multiple methods)
ISONE_RESERVE_ZONE_COLUMN_MAP = {
    "ReserveZoneId": "Reserve Zone Id",
    "ReserveZoneName": "Reserve Zone Name",
    "TenMinSpinRequirement": "Ten Min Spin Requirement",
    "TmnsrClearingPrice": "TMNSR Clearing Price",
    "TmnsrDesignatedMw": "TMNSR Designated MW",
    "TmorClearingPrice": "TMOR Clearing Price",
    "TmorDesignatedMw": "TMOR Designated MW",
    "TmsrClearingPrice": "TMSR Clearing Price",
    "TmsrDesignatedMw": "TMSR Designated MW",
    "Total10MinRequirement": "Total 10 Min Requirement",
    "Total30MinRequirement": "Total 30 Min Requirement",
}

ISONE_FCM_RECONFIGURATION_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Location Type",
    "Location ID",
    "Location Name",
    "Capacity Zone Type",
    "Total Supply Offers Submitted",
    "Total Demand Bids Submitted",
    "Total Supply Offers Cleared",
    "Total Demand Bids Cleared",
    "Net Capacity Cleared",
    "Clearing Price",
]

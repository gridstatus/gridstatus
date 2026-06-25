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

ISONE_CONSTRAINT_FIVE_MIN_COLUMNS = [
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

ISONE_FIVE_MIN_ESTIMATED_ZONAL_LOAD_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Load Zone ID",
    "Load Zone Name",
    "Estimated Load",
    "Estimated BTM Solar",
]

ISONE_FIVE_MIN_ZONAL_LOAD_FORECAST_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Publish Time",
    "Load Zone ID",
    "Load Zone Name",
    "Load Forecast",
    "BTM Solar Forecast",
]

ISONE_TOTAL_DEMAND_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Total Load",
    "Native Load",
    "Storage Load",
    "Total Load With Estimated Solar",
    "Native Load With Estimated Solar",
]

ISONE_MORNING_REPORT_TIE_NAMES = [
    "Highgate",
    "NB",
    "NECEC",
    "NYISO AC",
    "NYISO CSC",
    "NYISO NNC",
    "Phase 2",
]

ISONE_MORNING_REPORT_TIE_ALIASES = {
    "highgate": "Highgate",
    "nb": "NB",
    "necec": "NECEC",
    "nyiso ac": "NYISO AC",
    "nyiso csc": "NYISO CSC",
    "nyiso nnc": "NYISO NNC",
    "phase 2": "Phase 2",
    "phase ii": "Phase 2",
}

ISONE_MORNING_REPORT_SCALAR_MAP = {
    "PeakLoadYesterdayMw": "Prior Day Peak MW",
    "CsoMw": "Capacity Supply Obligation",
    "CapAdditionsMw": "Capacity Additions EcoMax Above CSO",
    "GenOutagesReductionMW": "Generation Outages and Reductions Planned and Forced",
    "GenPlannedOutagesReductionMW": "Generation Outages and Reductions Planned",
    "GenForcedOutagesReductionMW": "Generation Outages and Reductions Forced",
    "UncommittedAvailGenMw": "Uncommitted Available Generation Non Fast Start",
    "UncommittedGenBeforePeakMw": (
        "Uncommitted Available Generation Non Fast Start Before Peak"
    ),
    "DRRCapacityMw": "DRR Capacity",
    "UncommitedAvailDRRMw": "Uncommitted Available DRR",
    "NetCapDeliveryMw": "Net Capacity Deliveries",
    "TotAvailCapMw": "Total Available Capacity",
    "PeakLoadTodayMw": "Peak Hour Peak Load Forecast",
    "TotalOperReserveReqMw": "Total Operating Reserve Requirement",
    "CapRequiredMw": "Capacity Required",
    "SurplusDeficiencyMw": "Surplus or Deficiency",
    "ReplReserveRequiredMw": "Replacement Reserve Requirement",
    "ExcessCommitMw": "Excess Commitment Surplus or Deficiency",
    "LargestFirstContMw": "Largest First Contingency MW",
    "AmsPeakLoadExpMw": "Annual Maintenance Schedule Peak Load Exposure MW",
    "TenMinReserveReqMw": "Ten Minute Reserve Requirement",
    "TenMinReserveEstMw": "Ten Minute Reserve Estimate",
    "ThirtyMinReserveReqMw": "Thirty Minute Reserve Requirement",
    "ThirtyMinReserveEstMw": "Thirty Minute Reserve Estimate",
    "ExpActOp4Mw": "Expected Actions of OP 4",
    "AddlCapAvailOp4ActMw": "Additional Capacity Available from OP 4 Actions",
    "UnitCommMinOrrCount": "Units Committed for Minimum OR and RR",
    "NonMrktSensComnts": "Comments",
}

ISONE_MORNING_REPORT_TIE_DELIVERY_FIELD = {
    "TieFlowMw": "Capacity Deliveries",
}

ISONE_MORNING_REPORT_INTERCHANGE_FIELDS = {
    "ImportLimitInMw": "Import Limit MW",
    "ExportLimitOutMw": "Export Limit MW",
    "ScheduledMw": "Scheduled Contract MW",
}

ISONE_MORNING_REPORT_CITY_FIELDS = {
    "WeatherConditions": "Conditions",
    "WindDirSpeed": "Wind",
    "HighTemperature": "High Temperature",
}

ISONE_MORNING_REPORT_COLUMNS = [
    "Report Date",
    "Prior Day",
    "Prior Day Peak Hour",
    "Prior Day Peak MW",
    "Capacity Supply Obligation",
    "Capacity Additions EcoMax Above CSO",
    "Generation Outages and Reductions Planned and Forced",
    "Generation Outages and Reductions Planned",
    "Generation Outages and Reductions Forced",
    "Uncommitted Available Generation Non Fast Start",
    "Uncommitted Available Generation Non Fast Start Before Peak",
    "DRR Capacity",
    "Uncommitted Available DRR",
    "Highgate Capacity Deliveries",
    "NB Capacity Deliveries",
    "NECEC Capacity Deliveries",
    "NYISO AC Capacity Deliveries",
    "NYISO CSC Capacity Deliveries",
    "NYISO NNC Capacity Deliveries",
    "Phase 2 Capacity Deliveries",
    "Net Capacity Deliveries",
    "Total Available Capacity",
    "Peak Hour Peak Load Forecast",
    "Total Operating Reserve Requirement",
    "Capacity Required",
    "Surplus or Deficiency",
    "Replacement Reserve Requirement",
    "Excess Commitment Surplus or Deficiency",
    "Largest First Contingency MW",
    "Annual Maintenance Schedule Peak Load Exposure MW",
    "Ten Minute Reserve Requirement",
    "Ten Minute Reserve Estimate",
    "Thirty Minute Reserve Requirement",
    "Thirty Minute Reserve Estimate",
    "Expected Actions of OP 4",
    "Additional Capacity Available from OP 4 Actions",
    "Highgate Import Limit MW",
    "NB Import Limit MW",
    "NECEC Import Limit MW",
    "NYISO AC Import Limit MW",
    "NYISO CSC Import Limit MW",
    "NYISO NNC Import Limit MW",
    "Phase 2 Import Limit MW",
    "Highgate Export Limit MW",
    "NB Export Limit MW",
    "NECEC Export Limit MW",
    "NYISO AC Export Limit MW",
    "NYISO CSC Export Limit MW",
    "NYISO NNC Export Limit MW",
    "Phase 2 Export Limit MW",
    "Highgate Scheduled Contract MW",
    "NB Scheduled Contract MW",
    "NECEC Scheduled Contract MW",
    "NYISO AC Scheduled Contract MW",
    "NYISO CSC Scheduled Contract MW",
    "NYISO NNC Scheduled Contract MW",
    "Phase 2 Scheduled Contract MW",
    "Boston Conditions",
    "Boston Wind",
    "Boston High Temperature",
    "Hartford Conditions",
    "Hartford Wind",
    "Hartford High Temperature",
    "Units Committed for Minimum OR and RR",
    "Comments",
]

ISONE_FCM_RECONFIGURATION_COLUMNS = [
    "Interval Start",
    "Interval End",
    "ARA",
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

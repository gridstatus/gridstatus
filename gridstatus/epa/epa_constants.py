HOURLY_COLUMN_RENAMES = {
    "stateCode": "State",
    "facilityName": "Facility Name",
    "facilityId": "Facility ID",
    "unitId": "Unit ID",
    "associatedStacks": "Associated Stacks",
    "opTime": "Op Time",
    "grossLoad": "Gross Load",
    "steamLoad": "Steam Load",
    "so2Mass": "SO2 Mass",
    "so2MassMeasureFlg": "SO2 Mass Measure Flag",
    "so2Rate": "SO2 Rate",
    "so2RateMeasureFlg": "SO2 Rate Measure Flag",
    "co2Mass": "CO2 Mass",
    "co2MassMeasureFlg": "CO2 Mass Measure Flag",
    "co2Rate": "CO2 Rate",
    "co2RateMeasureFlg": "CO2 Rate Measure Flag",
    "noxMass": "NOx Mass",
    "noxMassMeasureFlg": "NOx Mass Measure Flag",
    "noxRate": "NOx Rate",
    "noxRateMeasureFlg": "NOx Rate Measure Flag",
    "heatInput": "Heat Input",
    "heatInputMeasureFlg": "Heat Input Measure Flag",
    "primaryFuelInfo": "Primary Fuel Info",
    "secondaryFuelInfo": "Secondary Fuel Info",
    "unitType": "Unit Type",
    "so2ControlInfo": "SO2 Control Info",
    "noxControlInfo": "NOx Control Info",
    "pmControlInfo": "PM Control Info",
    "hgControlInfo": "HG Control Info",
    "programCodeInfo": "Program Code Info",
}

ATTRIBUTE_COLUMN_RENAMES = {
    "latitude": "Latitude",
    "longitude": "Longitude",
    "county": "County",
    "countyCode": "County Code",
    "fipsCode": "FIPS Code",
    "epaRegion": "EPA Region",
    "nercRegion": "NERC Region",
    "sourceCategory": "Source Category",
    "ownerOperator": "Owner Operator",
    "so2Phase": "SO2 Phase",
    "noxPhase": "NOx Phase",
    "commercialOperationDate": "Commercial Operation Date",
    "operatingStatus": "Operating Status",
    "maxHourlyHIRate": "Max Hourly HI Rate",
    "associatedGeneratorsAndNameplateCapacity": (
        "Associated Generators and Nameplate Capacity"
    ),
}

ATTRIBUTE_COLUMNS = [
    "facilityId",
    "unitId",
    *ATTRIBUTE_COLUMN_RENAMES.keys(),
]

CLEAN_AIR_MARKETS_PROGRAM_DATA_COLUMNS = [
    "Interval Start",
    "Interval End",
    *HOURLY_COLUMN_RENAMES.values(),
    *ATTRIBUTE_COLUMN_RENAMES.values(),
]

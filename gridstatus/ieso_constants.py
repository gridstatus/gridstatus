import frozendict

PUBLIC_REPORTS_URL_PREFIX = "https://reports-public.ieso.ca/public"
ONTARIO_LOCATION = "ONZP"

"""LOAD CONSTANTS"""
# Load hourly files go back 30 days
MAXIMUM_DAYS_IN_PAST_FOR_LOAD: int = 30
LOAD_INDEX_URL: str = "https://reports-public.ieso.ca/public/RealtimeConstTotals"

# Each load file covers one hour. We have to use the xml instead of the csv because
# the csv does not have demand for Ontario.
LOAD_TEMPLATE_URL: str = f"{LOAD_INDEX_URL}/PUB_RealtimeConstTotals_YYYYMMDDHH.xml"


"""LOAD FORECAST CONSTANTS"""
# There's only one load forecast for Ontario. This data covers from 5 days ago
# through tomorrow
LOAD_FORECAST_URL: str = (
    "https://www.ieso.ca/-/media/Files/IESO/Power-Data/Ontario-Demand-multiday.ashx"
)


# The farthest in the past that forecast files are available
MAXIMUM_DAYS_IN_PAST_FOR_ZONAL_LOAD_FORECAST: int = 90
# The farthest in the future that forecasts are available. Note that there are not
# files for these future forecasts, they are in the current day's file.
MAXIMUM_DAYS_IN_FUTURE_FOR_ZONAL_LOAD_FORECAST: int = 34

"""REAL TIME FUEL MIX CONSTANTS"""
FUEL_MIX_INDEX_URL: str = "https://reports-public.ieso.ca/public/GenOutputCapability/"

# Updated every hour and each file has data for one day.
# The most recent version does not have the date in the filename.
FUEL_MIX_TEMPLATE_URL: str = (
    f"{FUEL_MIX_INDEX_URL}/PUB_GenOutputCapability_YYYYMMDD.xml"
)

# Number of past days for which the complete generator report is available.
# Before this date, only total by fuel type is available.
MAXIMUM_DAYS_IN_PAST_FOR_COMPLETE_GENERATOR_REPORT: int = 90

"""HISTORICAL FUEL MIX CONSTANTS"""
HISTORICAL_FUEL_MIX_INDEX_URL: str = (
    "https://reports-public.ieso.ca/public/GenOutputbyFuelHourly/"
)

# Updated once a day and each file contains data for an entire year.
HISTORICAL_FUEL_MIX_TEMPLATE_URL: str = (
    f"{HISTORICAL_FUEL_MIX_INDEX_URL}/PUB_GenOutputbyFuelHourly_YYYY.xml"
)


MINUTES_INTERVAL: int = 5
HOUR_INTERVAL: int = 1

# Default namespace used in the XML files
NAMESPACES_FOR_XML: dict[str, str] = {"": "http://www.ieso.ca/schema"}

# Maps abbreviations used in the MCP real time data to the full names
# used in the historical data
IESO_ZONE_MAPPING: dict[str, str] = {
    "MBSI": "Manitoba",
    "PQSK": "Manitoba SK",
    "MISI": "Michigan",
    "MNSI": "Minnesota",
    "NYSI": "New-York",
    "ONZN": "Ontario",
    "PQAT": "Quebec AT",
    "PQBE": "Quebec B5D.B31L",
    "PQDZ": "Quebec D4Z",
    "PQDA": "Quebec D5A",
    "PQHZ": "Quebec H4Z",
    "PQHA": "Quebec H9A",
    "PQPC": "Quebec P33C",
    "PQQC": "Quebec Q4C",
    "PQXY": "Quebec X2Y",
}


INTERTIE_ACTUAL_SCHEDULE_FLOW_HOURLY_COLUMNS: list[str] = [
    "Interval Start",
    "Interval End",
    "Publish Time",
    "Total Export",
    "Total Flow",
    "Total Import",
    "Manitoba Export",
    "Manitoba Flow",
    "Manitoba Import",
    "Manitoba Sk Export",
    "Manitoba Sk Flow",
    "Manitoba Sk Import",
    "Michigan Export",
    "Michigan Flow",
    "Michigan Import",
    "Minnesota Export",
    "Minnesota Flow",
    "Minnesota Import",
    "New York Export",
    "New York Flow",
    "New York Import",
    "PQAT Export",
    "PQAT Flow",
    "PQAT Import",
    "PQB5DB31L Export",
    "PQB5DB31L Flow",
    "PQB5DB31L Import",
    "PQD4Z Export",
    "PQD4Z Flow",
    "PQD4Z Import",
    "PQD5A Export",
    "PQD5A Flow",
    "PQD5A Import",
    "PQH4Z Export",
    "PQH4Z Flow",
    "PQH4Z Import",
    "PQH9A Export",
    "PQH9A Flow",
    "PQH9A Import",
    "PQP33C Export",
    "PQP33C Flow",
    "PQP33C Import",
    "PQQ4C Export",
    "PQQ4C Flow",
    "PQQ4C Import",
    "PQX2Y Export",
    "PQX2Y Flow",
    "PQX2Y Import",
]

INTERTIE_FLOW_5_MIN_COLUMNS: list[str] = [
    "Interval Start",
    "Interval End",
    "Publish Time",
    "Total Flow",
    "Manitoba Flow",
    "Manitoba Sk Flow",
    "Michigan Flow",
    "Minnesota Flow",
    "New York Flow",
    "PQAT Flow",
    "PQB5DB31L Flow",
    "PQD4Z Flow",
    "PQD5A Flow",
    "PQH4Z Flow",
    "PQH9A Flow",
    "PQP33C Flow",
    "PQQ4C Flow",
    "PQX2Y Flow",
]


ZONAL_LOAD_COLUMNS: list[str] = [
    "Interval Start",
    "Interval End",
    "Ontario Demand",
    "Northwest",
    "Northeast",
    "Ottawa",
    "East",
    "Toronto",
    "Essa",
    "Bruce",
    "Southwest",
    "Niagara",
    "West",
    "Zones Total",
    "Diff",
]

RESOURCE_ADEQUACY_REPORT_BASE_URL: str = (
    "https://reports-public.ieso.ca/public/Adequacy3"
)

RESOURCE_ADEQUACY_REPORT_DATA_STRUCTURE_MAP = frozendict.frozendict(
    {
        "supply": {
            "hourly": {
                "Forecast Supply Capacity": {
                    "path": ["ForecastSupply", "Capacities", "Capacity"],
                    "value_key": "EnergyMW",
                },
                "Forecast Supply Energy MWh": {
                    "path": ["ForecastSupply", "Energies", "Energy"],
                    "value_key": "EnergyMWhr",
                },
                "Forecast Supply Bottled Capacity": {
                    "path": ["ForecastSupply", "BottledCapacities", "Capacity"],
                    "value_key": "EnergyMW",
                },
                "Forecast Supply Regulation": {
                    "path": ["ForecastSupply", "Regulations", "Regulation"],
                    "value_key": "EnergyMW",
                },
                "Total Forecast Supply": {
                    "path": ["ForecastSupply", "TotalSupplies", "Supply"],
                    "value_key": "EnergyMW",
                },
                "Total Requirement": {
                    "path": ["ForecastDemand", "TotalRequirements", "Requirement"],
                    "value_key": "EnergyMW",
                },
                "Capacity Excess Shortfall": {
                    "path": ["ForecastDemand", "ExcessCapacities", "Capacity"],
                    "value_key": "EnergyMW",
                },
                "Energy Excess Shortfall MWh": {
                    "path": ["ForecastDemand", "ExcessEnergies", "Energy"],
                    "value_key": "EnergyMWhr",
                },
                "Offered Capacity Excess Shortfall": {
                    "path": [
                        "ForecastDemand",
                        "ExcessOfferedCapacities",
                        "Capacity",
                    ],
                    "value_key": "EnergyMW",
                },
                "Resources Not Scheduled": {
                    "path": [
                        "ForecastDemand",
                        "UnscheduledResources",
                        "UnscheduledResource",
                    ],
                    "value_key": "EnergyMW",
                },
                "Imports Not Scheduled": {
                    "path": [
                        "ForecastDemand",
                        "UnscheduledImports",
                        "UnscheduledImport",
                    ],
                    "value_key": "EnergyMW",
                },
            },
            "fuel_type_hourly": {
                "path": ["ForecastSupply", "InternalResources", "InternalResource"],
                "resources": {
                    "Nuclear": {
                        "Capacity": {
                            "path": ["Capacities", "Capacity"],
                            "value_key": "EnergyMW",
                        },
                        "Outages": {
                            "path": ["Outages", "Outage"],
                            "value_key": "EnergyMW",
                        },
                        "Offered": {
                            "path": ["Offers", "Offer"],
                            "value_key": "EnergyMW",
                        },
                        "Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Gas": {
                        "Capacity": {
                            "path": ["Capacities", "Capacity"],
                            "value_key": "EnergyMW",
                        },
                        "Outages": {
                            "path": ["Outages", "Outage"],
                            "value_key": "EnergyMW",
                        },
                        "Offered": {
                            "path": ["Offers", "Offer"],
                            "value_key": "EnergyMW",
                        },
                        "Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Hydro": {
                        "Capacity": {
                            "path": ["Capacities", "Capacity"],
                            "value_key": "EnergyMW",
                        },
                        "Outages": {
                            "path": ["Outages", "Outage"],
                            "value_key": "EnergyMW",
                        },
                        "Forecasted MWh": {
                            "path": ["ForecastEnergies", "ForecastEnergy"],
                            "value_key": "EnergyMWhr",
                        },
                        "Offered": {
                            "path": ["Offers", "Offer"],
                            "value_key": "EnergyMW",
                        },
                        "Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Wind": {
                        "Capacity": {
                            "path": ["Capacities", "Capacity"],
                            "value_key": "EnergyMW",
                        },
                        "Outages": {
                            "path": ["Outages", "Outage"],
                            "value_key": "EnergyMW",
                        },
                        "Forecasted": {
                            "path": ["Forecasts", "Forecast"],
                            "value_key": "EnergyMW",
                        },
                        "Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Solar": {
                        "Capacity": {
                            "path": ["Capacities", "Capacity"],
                            "value_key": "EnergyMW",
                        },
                        "Outages": {
                            "path": ["Outages", "Outage"],
                            "value_key": "EnergyMW",
                        },
                        "Forecasted": {
                            "path": ["Forecasts", "Forecast"],
                            "value_key": "EnergyMW",
                        },
                        "Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Biofuel": {
                        "Capacity": {
                            "path": ["Capacities", "Capacity"],
                            "value_key": "EnergyMW",
                        },
                        "Outages": {
                            "path": ["Outages", "Outage"],
                            "value_key": "EnergyMW",
                        },
                        "Offered": {
                            "path": ["Offers", "Offer"],
                            "value_key": "EnergyMW",
                        },
                        "Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Other": {
                        "Capacity": {
                            "path": ["Capacities", "Capacity"],
                            "value_key": "EnergyMW",
                        },
                        "Outages": {
                            "path": ["Outages", "Outage"],
                            "value_key": "EnergyMW",
                        },
                        "Offered Forecasted": {
                            "path": ["OfferForecasts", "OfferForecast"],
                            "value_key": "EnergyMW",
                        },
                        "Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                },
            },
            "total_internal_resources": {
                "path": [
                    "ForecastSupply",
                    "InternalResources",
                    "TotalInternalResources",
                ],
                "sections": {
                    "Total Internal Resources Outages": {
                        "path": ["Outages", "Outage"],
                        "value_key": "EnergyMW",
                    },
                    "Total Internal Resources Offered Forecasted": {
                        "path": ["OfferForecasts", "OfferForecast"],
                        "value_key": "EnergyMW",
                    },
                    "Total Internal Resources Scheduled": {
                        "path": ["Schedules", "Schedule"],
                        "value_key": "EnergyMW",
                    },
                },
            },
            "zonal_import_hourly": {
                "path": ["ForecastSupply", "ZonalImports", "ZonalImport"],
                "zones": {
                    "Manitoba": {
                        "Imports Offered": {
                            "path": ["Offers", "Offer"],
                            "value_key": "EnergyMW",
                        },
                        "Imports Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Minnesota": {
                        "Imports Offered": {
                            "path": ["Offers", "Offer"],
                            "value_key": "EnergyMW",
                        },
                        "Imports Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Michigan": {
                        "Imports Offered": {
                            "path": ["Offers", "Offer"],
                            "value_key": "EnergyMW",
                        },
                        "Imports Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "New York": {
                        "Imports Offered": {
                            "path": ["Offers", "Offer"],
                            "value_key": "EnergyMW",
                        },
                        "Imports Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Quebec": {
                        "Imports Offered": {
                            "path": ["Offers", "Offer"],
                            "value_key": "EnergyMW",
                        },
                        "Imports Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                },
            },
            "total_imports": {
                "path": ["ForecastSupply", "ZonalImports", "TotalImports"],
                "metrics": {
                    "Offers": {
                        "path": ["Offers", "Offer"],
                        "value_key": "EnergyMW",
                    },
                    "Scheduled": {
                        "path": ["Schedules", "Schedule"],
                        "value_key": "EnergyMW",
                    },
                    "Estimated": {
                        "path": ["Estimates", "Estimate"],
                        "value_key": "EnergyMW",
                    },
                    "Capacity": {
                        "path": ["Capacities", "Capacity"],
                        "value_key": "EnergyMW",
                    },
                },
            },
        },
        "demand": {
            "ontario_demand": {
                "path": ["ForecastDemand", "OntarioDemand"],
                "sections": {
                    "Ontario Demand Forecast": {
                        "path": ["ForecastOntDemand", "Demand"],
                        "value_key": "EnergyMW",
                    },
                    "Ontario Peak Demand": {
                        "path": ["PeakDemand", "Demand"],
                        "value_key": "EnergyMW",
                    },
                    "Ontario Average Demand": {
                        "path": ["AverageDemand", "Demand"],
                        "value_key": "EnergyMW",
                    },
                    "Ontario Northeast Peak Demand": {
                        "path": ["AreaPeakDemand", "NortheastPeakDemand", "Demand"],
                        "value_key": "PkDemand",
                    },
                    "Ontario Southwest Peak Demand": {
                        "path": ["AreaPeakDemand", "SouthwestPeakDemand", "Demand"],
                        "value_key": "PkDemand",
                    },
                    "Ontario Northwest Peak Demand": {
                        "path": ["AreaPeakDemand", "NorthwestPeakDemand", "Demand"],
                        "value_key": "PkDemand",
                    },
                    "Ontario Southeast Peak Demand": {
                        "path": ["AreaPeakDemand", "SoutheastPeakDemand", "Demand"],
                        "value_key": "PkDemand",
                    },
                    "Ontario Northeast Average Demand": {
                        "path": [
                            "AreaAverageDemand",
                            "NortheastAverageDemand",
                            "Demand",
                        ],
                        "value_key": "AvgDemand",
                    },
                    "Ontario Southwest Average Demand": {
                        "path": [
                            "AreaAverageDemand",
                            "SouthwestAverageDemand",
                            "Demand",
                        ],
                        "value_key": "AvgDemand",
                    },
                    "Ontario Northwest Average Demand": {
                        "path": [
                            "AreaAverageDemand",
                            "NorthwestAverageDemand",
                            "Demand",
                        ],
                        "value_key": "AvgDemand",
                    },
                    "Ontario Southeast Average Demand": {
                        "path": [
                            "AreaAverageDemand",
                            "SoutheastAverageDemand",
                            "Demand",
                        ],
                        "value_key": "AvgDemand",
                    },
                    "Ontario Wind Embedded Forecast": {
                        "path": ["WindEmbedded", "Embedded"],
                        "value_key": "EnergyMW",
                    },
                    "Ontario Solar Embedded Forecast": {
                        "path": ["SolarEmbedded", "Embedded"],
                        "value_key": "EnergyMW",
                    },
                    "Dispatchable Load": {
                        "sections": {
                            "Ontario Dispatchable Load Capacity": {
                                "path": ["Capacities", "Capacity"],
                                "value_key": "EnergyMW",
                            },
                            "Ontario Dispatchable Load Bid Forecasted": {
                                "path": ["BidForecasts", "BidForecast"],
                                "value_key": "EnergyMW",
                            },
                            "Ontario Dispatchable Load Scheduled ON": {
                                "path": ["ScheduledON", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                            "Ontario Dispatchable Load Scheduled OFF": {
                                "path": ["ScheduledOFF", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                        },
                    },
                    "Hourly Demand Response": {
                        "sections": {
                            "Ontario Hourly Demand Response Bid Forecasted": {
                                "path": ["Bids", "Bid"],
                                "value_key": "EnergyMW",
                            },
                            "Ontario Hourly Demand Response Scheduled": {
                                "path": ["Schedules", "Schedule"],
                                "value_key": "EnergyMW",
                            },
                            "Ontario Hourly Demand Response Curtailed": {
                                "path": ["Curtailed", "Curtail"],
                                "value_key": "EnergyMW",
                            },
                        },
                    },
                },
            },
            "zonal_export_hourly": {
                "path": ["ForecastDemand", "ZonalExports", "ZonalExport"],
                "zones": {
                    "Manitoba": {
                        "Exports Offered": {
                            "path": ["Bids", "Bid"],
                            "value_key": "EnergyMW",
                        },
                        "Exports Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Minnesota": {
                        "Exports Offered": {
                            "path": ["Bids", "Bid"],
                            "value_key": "EnergyMW",
                        },
                        "Exports Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Michigan": {
                        "Exports Offered": {
                            "path": ["Bids", "Bid"],
                            "value_key": "EnergyMW",
                        },
                        "Exports Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "New York": {
                        "Exports Offered": {
                            "path": ["Bids", "Bid"],
                            "value_key": "EnergyMW",
                        },
                        "Exports Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                    "Quebec": {
                        "Exports Offered": {
                            "path": ["Bids", "Bid"],
                            "value_key": "EnergyMW",
                        },
                        "Exports Scheduled": {
                            "path": ["Schedules", "Schedule"],
                            "value_key": "EnergyMW",
                        },
                    },
                },
            },
            "total_exports": {
                "path": ["ForecastDemand", "ZonalExports", "TotalExports"],
                "metrics": {
                    "Bids": {
                        "path": ["Bids", "Bid"],
                        "value_key": "EnergyMW",
                    },
                    "Scheduled": {
                        "path": ["Schedules", "Schedule"],
                        "value_key": "EnergyMW",
                    },
                    "Capacity": {
                        "path": ["Capacities", "Capacity"],
                        "value_key": "EnergyMW",
                    },
                },
            },
            "reserves": {
                "path": ["ForecastDemand", "GenerationReserveHoldback"],
                "sections": {
                    "Total Operating Reserve": {
                        "path": ["TotalORReserve", "ORReserve"],
                        "value_key": "EnergyMW",
                    },
                    "Minimum 10 Minute Operating Reserve": {
                        "path": ["Min10MinOR", "Min10OR"],
                        "value_key": "EnergyMW",
                    },
                    "Minimum 10 Minute Spin OR": {
                        "path": ["Min10MinSpinOR", "Min10SpinOR"],
                        "value_key": "EnergyMW",
                    },
                    "Load Forecast Uncertainties": {
                        "path": ["LoadForecastUncertainties", "Uncertainty"],
                        "value_key": "EnergyMW",
                    },
                    "Additional Contingency Allowances": {
                        "path": ["ContingencyAllowances", "Allowance"],
                        "value_key": "EnergyMW",
                    },
                },
            },
        },
    },
)

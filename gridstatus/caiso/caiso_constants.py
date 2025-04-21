CURRENT_BASE = "https://www.caiso.com/outlook/current"
HISTORY_BASE = "https://www.caiso.com/outlook/history"

DAY_AHEAD_MARKET_MARKET_RUN_ID = "DAM"
REAL_TIME_DISPATCH_MARKET_RUN_ID = "RTD"
REAL_TIME_DISPATCH_15_MIN_MARKET_RUN_ID = "RTPD"

OASIS_DATASET_CONFIG = {
    "transmission_interface_usage": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "TRNS_USAGE",
            "version": 1,
        },
        "params": {
            "market_run_id": ["DAM", "HASP", "RRPD"],
            # you can also specify a specific interface
            "ti_id": "ALL",
            "ti_direction": ["ALL", "E", "I"],
        },
    },
    "schedule_by_tie": {
        "query": {
            "path": "GroupZip",
            "resultformat": 6,
            "version": 12,
        },
        "params": {
            "groupid": [
                "RTD_ENE_SCH_BY_TIE_GRP",
                "DAM_ENE_SCH_BY_TIE_GRP",
                "RUC_ENE_SCH_BY_TIE_GRP",
                "RTPD_ENE_SCH_BY_TIE_GRP",
            ],
        },
        "meta": {
            "max_query_frequency": "1d",
        },
    },
    "as_requirements": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "AS_REQ",
            "version": 1,
        },
        "params": {
            "market_run_id": ["DAM", "HASP", "RTM", "2DA"],
            "anc_type": ["ALL", "NR", "RD", "RU", "SR", "RMD", "RMU"],
            "anc_region": [
                "ALL",
                "AS_CAISO",
                "AS_CAISO_EXP",
                "AS_NP26",
                "AS_NP26_EXP",
                "AS_SP26",
                "AS_SP26_EXP",
            ],
        },
    },
    "as_clearing_prices": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_AS",
            "version": 12,
        },
        "params": {
            "market_run_id": ["DAM", "HASP"],
            "anc_type": ["ALL", "NR", "RD", "RMD", "RMU", "RU", "SR"],
            "anc_region": [
                "ALL",
                "AS_CAISO",
                "AS_SP26_EXP",
                "AS_SP26",
                "AS_CAISO_EXP",
                "AS_NP26_EXP",
                "AS_NP26",
            ],
        },
    },
    "fuel_prices": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_FUEL",
            "version": 1,
        },
        "params": {
            "fuel_region_id": "ALL",
        },
    },
    "ghg_allowance": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_GHG_ALLOWANCE",
            "version": 1,
        },
        "params": {},
    },
    "wind_and_solar_forecast": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "SLD_REN_FCST",
            "version": 1,
        },
        "params": {"market_run_id": "DAM"},
    },
    "pnode_map": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "ATL_PNODE_MAP",
            "version": 1,
        },
        "params": {
            "pnode_id": "ALL",
        },
    },
    "lmp_day_ahead_hourly": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_LMP",
            "version": 12,
        },
        "params": {
            "market_run_id": "DAM",
            "node": None,
            "grp_type": [None, "ALL", "ALL_APNODES"],
        },
    },
    "lmp_real_time_5_min": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_INTVL_LMP",
            "version": 3,
        },
        "params": {
            "market_run_id": "RTM",
            "node": None,
            "grp_type": [None, "ALL", "ALL_APNODES"],
        },
    },
    "lmp_real_time_15_min": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_RTPD_LMP",
            "version": 3,
        },
        "params": {
            "market_run_id": "RTPD",
            "node": None,
            "grp_type": [None, "ALL", "ALL_APNODES"],
        },
    },
    "lmp_scheduling_point_tie_combination_5_min": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_SPTIE_LMP",
            "version": 5,
        },
        "params": {
            "market_run_id": "RTD",
            "node": None,
            "grp_type": [None, "ALL", "ALL_APNODES"],
        },
        "meta": {
            "max_query_frequency": "1h",
        },
    },
    "lmp_scheduling_point_tie_combination_15_min": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_SPTIE_LMP",
            "version": 5,
        },
        "params": {
            "market_run_id": "RTPD",
            "node": None,
            "grp_type": [None, "ALL", "ALL_APNODES"],
        },
        "meta": {
            "max_query_frequency": "1h",
        },
    },
    "lmp_scheduling_point_tie_combination_hourly": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_SPTIE_LMP",
            "version": 5,
        },
        "params": {
            "market_run_id": "DAM",
            "node": None,
            "grp_type": [None, "ALL", "ALL_APNODES"],
        },
        "meta": {
            "max_query_frequency": "1d",
        },
    },
    "lmp_hasp_15_min": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_HASP_LMP",
            "version": 3,
        },
        "params": {
            "node": None,
            "grp_type": [None, "ALL", "ALL_APNODES"],
        },
        "meta": {
            "max_query_frequency": "1h",
        },
    },
    "demand_forecast": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "SLD_FCST",
            "version": 1,
        },
        "params": {
            "market_run_id": ["7DA", "2DA", "DAM", "ACTUAL", "RTM"],
            "execution_type": [None, "RTPD", "RTD"],
        },
    },
    "as_results": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "AS_RESULTS",
            "version": 1,
        },
        "params": {
            "market_run_id": ["DAM", "HASP", "RTM"],
            "anc_type": ["ALL", "NR", "RD", "RU", "SR", "RMD", "RMU"],
            "anc_region": [
                "ALL",
                "AS_CAISO",
                "AS_CAISO_EXP",
                "AS_NP26",
                "AS_NP26_EXP",
                "AS_SP26",
                "AS_SP26_EXP",
            ],
        },
    },
    "excess_btm_production": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "ENE_EBTMP_PERF_DATA",
            "version": 11,
        },
        "params": {},
        "meta": {
            "publish_delay": "3 months",
        },
    },
    "public_bids": {
        "query": {
            "path": "GroupZip",
            "resultformat": 6,
            "version": 3,
        },
        "params": {
            "groupid": ["PUB_DAM_GRP", "PUB_RTM_GRP"],
        },
        "meta": {
            "publish_delay": "90 days",
            "max_query_frequency": "1d",
        },
    },
    "tie_flows_real_time": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "ENE_EIM_TRANSFER_TIE",
            "version": 4,
        },
        "params": {
            "baa_grp_id": "ALL",
            "market_run_id": REAL_TIME_DISPATCH_MARKET_RUN_ID,
        },
    },
    "tie_flows_real_time_15_min": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "ENE_EIM_TRANSFER_TIE",
            "version": 4,
        },
        "params": {
            "baa_grp_id": "ALL",
            "market_run_id": REAL_TIME_DISPATCH_15_MIN_MARKET_RUN_ID,
        },
    },
    "tie_schedule_day_ahead_hourly": {
        "query": {
            "path": "GroupZip",
            "resultformat": 6,
            "version": 12,
        },
        "params": {
            "groupid": ["DAM_ENE_SCH_BY_TIE_GRP"],
            "market_run_id": DAY_AHEAD_MARKET_MARKET_RUN_ID,
        },
    },
    "hasp_renewable_forecast_hourly": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "SLD_REN_FCST",
            "version": 1,
        },
        "params": {
            "market_run_id": "HASP",
        },
        "meta": {
            "max_query_frequency": "1d",
        },
    },
}

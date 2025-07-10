import pandas as pd

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
    "renewables": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "SLD_REN_FCST",
            "version": 1,
        },
        "params": {"market_run_id": "ACTUAL"},
    },
    "renewables_forecast_dam": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "SLD_REN_FCST",
            "version": 1,
        },
        "params": {"market_run_id": "DAM"},
    },
    "renewables_forecast_hasp": {
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
    "renewables_forecast_rtd": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "SLD_REN_FCST",
            "version": 1,
        },
        "params": {
            "market_run_id": "RTD",
        },
        "meta": {
            "max_query_frequency": "1d",
        },
    },
    "renewables_forecast_rtpd": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "SLD_REN_FCST",
            "version": 1,
        },
        "params": {
            "market_run_id": "RTPD",
        },
        "meta": {
            "max_query_frequency": "1d",
        },
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
    "nomogram_branch_shadow_prices": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_NOMOGRAM",
            "version": 1,
        },
        "params": {
            "market_run_id": ["DAM", "HASP", "RTM"],
        },
    },
    "interval_nomogram_branch_shadow_prices": {
        "query": {
            "path": "SingleZip",
            "resultformat": 6,
            "queryname": "PRC_RTM_NOMOGRAM",
            "version": 1,
        },
        "params": {},
    },
}


def get_dataframe_config_for_renewables_report(
    base_date: pd.Timestamp,
    timezone: str,
) -> list:
    def generate_timestamps(
        base_date: pd.Timestamp,
        periods: int,
        freq: str,
        offset_days: int = 0,
    ) -> pd.DatetimeIndex:
        start_date = (
            base_date - pd.Timedelta(days=offset_days) if offset_days else base_date
        )

        return pd.date_range(
            start=start_date,
            periods=periods,
            freq=freq,
            tz=timezone,
        )

    # 5-minute timestamps for the day
    rtd_timestamps = generate_timestamps(base_date, 288, "5min")
    # 1 hour timestamps for the day
    hourly_timestamps = generate_timestamps(base_date, 24, "1h")
    # Daily timestamps for the day and past 14 days
    daily_timestamps = generate_timestamps(base_date, 15, "1d", offset_days=14)
    # Monthly timestamps for the current year up to the current month
    month_ytd_timestamps = generate_timestamps(
        base_date.replace(month=1, day=1),
        base_date.month,
        "MS",
    )
    # Monthly timestamps for the past 13 months including the current month
    monthly_rolling_timestamps = generate_timestamps(
        base_date - pd.DateOffset(months=13),
        13,
        "MS",
    )

    # DataFrame configurations
    # df_name, timestamps, duration, unit, column_mapping
    dataframe_configs = [
        # 5-minute generation data
        (
            "solar_generation_caiso_5_min",
            rtd_timestamps,
            5,
            "minute",
            {
                "RTD MW": "tot_gen_solar_iso_rtd",
                "FMM MW": "tot_gen_solar_iso_rtpd",
                "IFM MW": "tot_gen_solar_iso_ifm",
                "Actual MW": "tot_gen_solar_iso_telem",
                "DA Forecast MW": "tot_gen_solar_iso_forec",
            },
        ),
        (
            "solar_generation_weim_5_min",
            rtd_timestamps,
            5,
            "minute",
            {
                "RTD MW": "tot_gen_solar_weim_rtd",
                "FMM MW": "tot_gen_solar_weim_rtpd",
                "Base Schedule MW": "tot_gen_solar_weim_rtbs",
                "Actual MW": "tot_gen_solar_weim_telem",
            },
        ),
        (
            "wind_generation_caiso_5_min",
            rtd_timestamps,
            5,
            "minute",
            {
                "RTD MW": "tot_gen_wind_iso_rtd",
                "FMM MW": "tot_gen_wind_iso_rtpd",
                "IFM MW": "tot_gen_wind_iso_ifm",
                "Actual MW": "tot_gen_wind_iso_telem",
                "DA Forecast MW": "tot_gen_wind_iso_forec",
            },
        ),
        (
            "wind_generation_weim_5_min",
            rtd_timestamps,
            5,
            "minute",
            {
                "RTD MW": "tot_gen_wind_weim_rtd",
                "FMM MW": "tot_gen_wind_weim_rtpd",
                "Base Schedule MW": "tot_gen_wind_weim_rtbs",
                "Actual MW": "tot_gen_wind_weim_telem",
            },
        ),
        # Hourly curtailment data
        (
            "ver_generation_curtailment_energy_hourly",
            hourly_timestamps,
            1,
            "hour",
            {
                "Economic Local MWH": "curt_hourly_econ_local",
                "Economic System MWH": "curt_hourly_econ_system",
                "SelfSchCut Local MWH": "curt_hourly_ss_local",
                "SelfSchCut System MWH": "curt_hourly_ss_system",
                "OperatorInstruction Local MWH": "curt_hourly_oi_local",
                "OperatorInstruction System MWH": "curt_hourly_oi_system",
            },
        ),
        (
            "ver_generation_curtailment_maximum_hourly",
            hourly_timestamps,
            1,
            "hour",
            {
                "Economic Local MW": "curt_hourly_max_econ_local",
                "Economic System MW": "curt_hourly_max_econ_system",
                "SelfSchCut Local MW": "curt_hourly_max_ss_local",
                "SelfSchCut System MW": "curt_hourly_max_ss_system",
                "OperatorInstruction Local MW": "curt_hourly_max_oi_local",
                "OperatorInstruction System MW": "curt_hourly_max_oi_system",
            },
        ),
        # Daily curtailment data
        (
            "ver_generation_curtailment_energy_daily",
            daily_timestamps,
            1,
            "day",
            {
                "Economic Local MWH": "curt_daily_econ_local_mwh",
                "Economic System MWH": "curt_daily_econ_system_mwh",
                "SelfSchCut Local MWH": "curt_daily_ss_local_mwh",
                "SelfSchCut System MWH": "curt_daily_ss_system_mwh",
                "OperatorInstruction Local MWH": "curt_daily_oi_local_mwh",
                "OperatorInstruction System MWH": "curt_daily_oi_system_mwh",
            },
        ),
        (
            "ver_generation_curtailment_maximum_daily",
            daily_timestamps,
            1,
            "day",
            {
                "Economic Local MW": "curt_daily_econ_local_mw",
                "Economic System MW": "curt_daily_econ_system_mw",
                "SelfSchCut Local MW": "curt_daily_ss_local_mw",
                "SelfSchCut System MW": "curt_daily_ss_system_mw",
                "OperatorInstruction Local MW": "curt_daily_oi_local_mw",
                "OperatorInstruction System MW": "curt_daily_oi_system_mw",
            },
        ),
        # Fuel-specific hourly curtailment data
        (
            "solar_curtailment_maximum_hourly",
            hourly_timestamps,
            1,
            "hour",
            {
                "Economic Local MW": "curt_hr_max_solar_econ_local_mw",
                "Economic System MW": "curt_hr_max_solar_econ_system_mw",
                "SelfSchCut Local MW": "curt_hr_max_solar_ss_local_mw",
                "SelfSchCut System MW": "curt_hr_max_solar_ss_system_mw",
                "OperatorInstruction Local MW": "curt_hr_max_solar_oi_local_mw",
                "OperatorInstruction System MW": "curt_hr_max_solar_oi_system_mw",
            },
        ),
        (
            "wind_curtailment_maximum_hourly",
            hourly_timestamps,
            1,
            "hour",
            {
                "Economic Local MW": "curt_hr_max_wind_econ_local_mw",
                "Economic System MW": "curt_hr_max_wind_econ_system_mw",
                "SelfSchCut Local MW": "curt_hr_max_wind_ss_local_mw",
                "SelfSchCut System MW": "curt_hr_max_wind_ss_system_mw",
                "OperatorInstruction Local MW": "curt_hr_max_wind_oi_local_mw",
                "OperatorInstruction System MW": "curt_hr_max_wind_oi_system_mw",
            },
        ),
        (
            "solar_curtailment_total_hourly",
            hourly_timestamps,
            1,
            "hour",
            {
                "Economic Local MWH": "curt_hr_tot_solar_econ_local_mwh",
                "Economic System MWH": "curt_hr_tot_solar_econ_system_mwh",
                "SelfSchCut Local MWH": "curt_hr_tot_solar_ss_local_mwh",
                "SelfSchCut System MWH": "curt_hr_tot_solar_ss_system_mwh",
                "OperatorInstruction Local MWH": "curt_hr_tot_solar_oi_local_mwh",
                "OperatorInstruction System MWH": "curt_hr_tot_solar_oi_system_mwh",
            },
        ),
        (
            "wind_curtailment_total_hourly",
            hourly_timestamps,
            1,
            "hour",
            {
                "Economic Local MWH": "curt_hr_tot_wind_econ_local_mwh",
                "Economic System MWH": "curt_hr_tot_wind_econ_system_mwh",
                "SelfSchCut Local MWH": "curt_hr_tot_wind_ss_local_mwh",
                "SelfSchCut System MWH": "curt_hr_tot_wind_ss_system_mwh",
                "OperatorInstruction Local MWH": "curt_hr_tot_wind_oi_local_mwh",
                "OperatorInstruction System MWH": "curt_hr_tot_wind_oi_system_mwh",
            },
        ),
        # Year-to-date and monthly data
        (
            "curtailment_year_to_date_profile_hourly",
            hourly_timestamps,
            1,
            "hour",
            {
                "Economic Local MWH": "curt_hourly_ytd_econ_local_mwh",
                "Economic System MWH": "curt_hourly_ytd_econ_system_mwh",
                "SelfSchCut Local MWH": "curt_hourly_ytd_ss_local_mwh",
                "SelfSchCut System MWH": "curt_hourly_ytd_ss_system_mwh",
                "OperatorInstruction Local MWH": "curt_hourly_ytd_oi_local_mwh",
                "OperatorInstruction System MWH": "curt_hourly_ytd_oi_system_mwh",
            },
        ),
        (
            "curtailment_year_to_date_total_monthly",
            month_ytd_timestamps,
            1,
            "month",
            {
                "Economic Local MWH": "curt_monthly_ytd_econ_local_mwh",
                "Economic System MWH": "curt_monthly_ytd_econ_system_mwh",
                "SelfSchCut Local MWH": "curt_monthly_ytd_ss_local_mwh",
                "SelfSchCut System MWH": "curt_monthly_ytd_ss_system_mwh",
                "OperatorInstruction Local MWH": "curt_monthly_ytd_oi_local_mwh",
                "OperatorInstruction System MWH": "curt_monthly_ytd_oi_system_mwh",
            },
        ),
        (
            "curtailment_percentage_monthly",
            monthly_rolling_timestamps,
            1,
            "month",
            {
                "Percent": "curt_monthly_ytd_perc_mwh",
            },
        ),
    ]

    return dataframe_configs

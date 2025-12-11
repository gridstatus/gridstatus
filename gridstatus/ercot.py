import datetime
import io
import time
from dataclasses import dataclass
from enum import Enum
from typing import BinaryIO, Callable, List
from zipfile import ZipFile

import pandas as pd
import pytz
import requests
import tqdm
from bs4 import BeautifulSoup
from pytz.exceptions import NonExistentTimeError

from gridstatus import utils
from gridstatus.base import (
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NoDataFoundException,
    NotSupported,
)
from gridstatus.decorators import support_date_range
from gridstatus.ercot_60d_utils import (
    DAM_ENERGY_BID_AWARDS_KEY,
    DAM_ENERGY_BIDS_KEY,
    DAM_ENERGY_ONLY_OFFER_AWARDS_KEY,
    DAM_ENERGY_ONLY_OFFERS_KEY,
    DAM_GEN_RESOURCE_AS_OFFERS_KEY,
    DAM_GEN_RESOURCE_KEY,
    DAM_LOAD_RESOURCE_AS_OFFERS_KEY,
    DAM_LOAD_RESOURCE_KEY,
    DAM_PTP_OBLIGATION_BID_AWARDS_KEY,
    DAM_PTP_OBLIGATION_BIDS_KEY,
    DAM_PTP_OBLIGATION_OPTION_AWARDS_KEY,
    DAM_PTP_OBLIGATION_OPTION_KEY,
    SCED_GEN_RESOURCE_KEY,
    SCED_LOAD_RESOURCE_KEY,
    SCED_SMNE_KEY,
    process_dam_energy_bid_awards,
    process_dam_energy_bids,
    process_dam_energy_only_offer_awards,
    process_dam_energy_only_offers,
    process_dam_gen,
    process_dam_load,
    process_dam_or_gen_load_as_offers,
    process_dam_ptp_obligation_bid_awards,
    process_dam_ptp_obligation_bids,
    process_dam_ptp_obligation_option,
    process_dam_ptp_obligation_option_awards,
    process_sced_gen,
    process_sced_load,
)
from gridstatus.ercot_constants import (
    SOLAR_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS,
    SOLAR_ACTUAL_AND_FORECAST_COLUMNS,
    WIND_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS,
    WIND_ACTUAL_AND_FORECAST_COLUMNS,
)
from gridstatus.gs_logging import logger
from gridstatus.lmp_config import lmp_config

LOCATION_TYPE_HUB = "Trading Hub"
LOCATION_TYPE_RESOURCE_NODE = "Resource Node"
LOCATION_TYPE_ZONE = "Load Zone"
LOCATION_TYPE_ZONE_EW = "Load Zone Energy Weighted"
LOCATION_TYPE_ZONE_DC = "Load Zone DC Tie"
LOCATION_TYPE_ZONE_DC_EW = "Load Zone DC Tie Energy Weighted"

ELECTRICAL_BUS_LOCATION_TYPE = "Electrical Bus"
SETTLEMENT_POINT_LOCATION_TYPE = "Settlement Point"

"""
Report Type IDs
"""
# DAM System Lambda
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-523-CD
DAM_SYSTEM_LAMBDA_RTID = 13113

# SCED System Lambda
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-322-CD
SCED_SYSTEM_LAMBDA_RTID = 13114

# DAM Clearing Prices for Capacity
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-188-CD
DAM_CLEARING_PRICES_FOR_CAPACITY_RTID = 12329

# DAM Ancillary Service Plan
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-33-CD
DAM_ANCILLARY_SERVICE_PLAN_RTID = 12316

# DAM Settlement Point Prices
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-190-CD
DAM_SETTLEMENT_POINT_PRICES_RTID = 12331

# GIS Report
# https://www.ercot.com/mp/data-products/data-product-details?id=PG7-200-ER
GIS_REPORT_RTID = 15933

# Historical RTM Load Zone and Hub Prices
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-785-ER
HISTORICAL_RTM_LOAD_ZONE_AND_HUB_PRICES_RTID = 13061

# Historical DAM Load Zone and Hub Prices
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-180-ER
HISTORICAL_DAM_LOAD_ZONE_AND_HUB_PRICES_RTID = 13060

# Settlement Points List and Electrical Buses Mapping
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-160-SG
SETTLEMENT_POINTS_LIST_AND_ELECTRICAL_BUSES_MAPPING_RTID = 10008

# Settlement Point Prices at Resource Nodes, Hubs and Load Zones
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD
SETTLEMENT_POINT_PRICES_AT_RESOURCE_NODES_HUBS_AND_LOAD_ZONES_RTID = 12301

# RTM Price Corrections
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-197-M
RTM_PRICE_CORRECTIONS_RTID = 13045

# DAM Price Corrections
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-196-M
DAM_PRICE_CORRECTIONS_RTID = 13044

# LMPs by Electrical Bus
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-787-CD
LMPS_BY_ELECTRICAL_BUS_RTID = 11485

# LMPs by Resource Nodes, Load Zones and Trading Hubs
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-788-CD
LMPS_BY_SETTLEMENT_POINT_RTID = 12300

# System-wide actuals
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-235-CD
SYSTEM_WIDE_ACTUALS_RTID = 12340

# Short term system adequacy report
# https://www.ercot.com/mp/data-products/data-product-details?id=NP3-763-CD
SHORT_TERM_SYSTEM_ADEQUACY_REPORT_RTID = 12315


# Real-Time ORDC and Reliability Deployment Price Adders and Reserves by SCED Interval
# (ORDC = Operating Reserve Demand Curve)
# Adders and reserves stopped being published on December 5
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-323-CD
REAL_TIME_ADDERS_AND_RESERVES_RTID = 13221

# Real-Time ORDC and Reliability Deployment Price Adders
# Adders only started being published on December 5
REAL_TIME_ADDERS_RTID = 13221

# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-722-CD
TEMPERATURE_FORECAST_BY_WEATHER_ZONE_RTID = 12325

# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-970-CD
# https://data.ercot.com/data-product-archive/NP6-970-CD - for historical data
ERCOT_INDICATIVE_LMP_BY_SETTLEMENT_POINT_RTID = 13073

# https://www.ercot.com/mp/data-products/data-product-details?id=np4-192-cd
DAM_TOTAL_ENERGY_PURCHASED_RTID = 12333

# https://www.ercot.com/mp/data-products/data-product-details?id=np4-193-cd
DAM_TOTAL_ENERGY_SOLD_RTID = 12334

# https://www.ercot.com/mp/data-products/data-product-details?id=np1-301
COP_ADJUSTMENT_PERIOD_SNAPSHOT_RTID = 10038

# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-332-CD
REAL_TIME_CLEARING_PRICES_FOR_CAPACITY_BY_SCED_INTERVAL_RTID = 24891

# https://www.ercot.com/mp/data-products/data-product-details?id=np6-788-rtcmt
REAL_TIME_CLEARING_LMPS_BY_RESOURCE_NODES_LOAD_ZONES_AND_TRADING_HUBS_RTD = 4104

# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-331-CD
REAL_TIME_CLEARING_PRICES_FOR_CAPACITY_15_MIN_RTID = 24898

# https://www.ercot.com/mp/data-products/data-product-details?id=np4-212-cd
DAM_AND_SCED_ANCILLARY_SERVICE_DEMAND_CURVES_RTID = 24893

# https://www.ercot.com/mp/data-products/data-product-details?id=NP5-526-CD
PROJECTED_ANCILLARY_SERVICE_DEPLOYMENTS_FACTORS_RTID = 24886

# Weekly RUC Ancillary Service Deployment Factors
# https://www.ercot.com/mp/data-products/data-product-details?id=np5-525-cd
WEEKLY_RUC_AS_DEPLOYMENT_FACTORS_RTID = 24897

# Daily RUC Ancillary Service Deployment Factors
# https://www.ercot.com/mp/data-products/data-product-details?id=NP5-527-CD
DAILY_RUC_AS_DEPLOYMENT_FACTORS_RTID = 24895

# Hourly RUC Ancillary Service Deployment Factors
# https://www.ercot.com/mp/data-products/data-product-details?id=NP5-528-CD
HOURLY_RUC_AS_DEPLOYMENT_FACTORS_RTID = 24896

# Hourly RUC Ancillary Service Demand Curves
# https://www.ercot.com/mp/data-products/data-product-details?id=np4-213-cd
HOURLY_RUC_AS_DEMAND_CURVES_RTID = 26382

# Daily RUC Ancillary Service Demand Curves
# https://www.ercot.com/mp/data-products/data-product-details?id=np4-214-cd
DAILY_RUC_AS_DEMAND_CURVES_RTID = 26383

# Weekly RUC Ancillary Service Demand Curves
# https://www.ercot.com/mp/data-products/data-product-details?id=np4-215-cd
WEEKLY_RUC_AS_DEMAND_CURVES_RTID = 26384

# DAM Total Ancillary Services Sold
# https://www.ercot.com/mp/data-products/data-product-details?id=np4-532-cd
DAM_TOTAL_AS_SOLD_RTID = 24888

# RTD Indicative Real-Time MCPC
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-329-CD
RTD_INDICATIVE_REAL_TIME_MCPC_RTID = 24889

# Total Capability of Resources Available to Provide Ancillary Service
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-328-CD
TOTAL_CAPABILITY_OF_RESOURCES_AS_RTID = 24887


class ERCOTSevenDayLoadForecastReport(Enum):
    """
    Enum class for the Medium Term (Seven Day) Load Forecasts.
    The values are the report IDs.
    """

    # Seven-Day Load Forecast by Forecast Zone
    # https://www.ercot.com/mp/data-products/data-product-details?id=NP3-560-CD
    BY_FORECAST_ZONE = 12311

    # Seven-Day Load Forecast by Weather Zone
    # https://www.ercot.com/mp/data-products/data-product-details?id=NP3-561-CD
    BY_WEATHER_ZONE = 12312

    # Seven-Day Load Forecast by Model and Weather Zone
    # https://www.ercot.com/mp/data-products/data-product-details?id=NP3-565-CD
    BY_MODEL_AND_WEATHER_ZONE = 14837

    # Seven-Day Load Forecast by Model and Study Area
    # https://www.ercot.com/mp/data-products/data-product-details?id=NP3-566-CD
    BY_MODEL_AND_STUDY_AREA = 15953

    # intrahour https://www.ercot.com/mp/data-products/data-product-details?id=NP3-562-CD
    # there are a few days of historical data for the forecast


# Actual System Load by Weather Zone
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-345-CD
ACTUAL_SYSTEM_LOAD_BY_WEATHER_ZONE = 13101

# Actual System Load by Forecast Zone
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-346-CD
ACTUAL_SYSTEM_LOAD_BY_FORECAST_ZONE = 14836

# 60-Day DAM Disclosure Reports
# https://www.ercot.com/mp/data-products/data-product-details?id=NP3-966-ER
SIXTY_DAY_DAM_DISCLOSURE_REPORTS_RTID = 13051

# 60-Day SCED Disclosure Reports
# https://www.ercot.com/mp/data-products/data-product-details?id=NP3-965-ER
SIXTY_DAY_SCED_DISCLOSURE_REPORTS_RTID = 13052

# Unplanned Resource Outages Report
# https://www.ercot.com/mp/data-products/data-product-details?id=NP1-346-ER
UNPLANNED_RESOURCE_OUTAGES_REPORT_RTID = 22912

# 3-Day Highest Price AS Offer Selected
# https://www.ercot.com/mp/data-products/data-product-details?id=NP3-915-EX
THREE_DAY_HIGHEST_PRICE_AS_OFFER_SELECTED_RTID = 13018

# 2-Day Ancillary Services Reports (DAM)
# https://www.ercot.com/mp/data-products/data-product-details?id=NP3-911-ER
TWO_DAY_ANCILLARY_SERVICES_REPORTS_RTID = 13057

# 2-Day SCED Ancillary Service Disclosure
# https://www.ercot.com/mp/data-products/data-product-details?id=np3-906-ex
TWO_DAY_SCED_ANCILLARY_SERVICES_REPORTS_RTID = 25814

# Ancillary Services products - used across multiple AS report methods
AS_PRODUCTS = [
    "RRSPFR",
    "RRSUFR",
    "RRSFFR",
    "ECRSM",
    "ECRSS",
    "REGUP",
    "REGDN",
    "NSPIN",
    "NSPNM",
]

# Products that are not present in earlier data (before certain product launches)
AS_EXCLUDE_PRODUCTS = [
    "ECRSM",
    "ECRSS",
    "NSPNM",
    "RRSFFR",
    "RRSUFR",
    "RRSPFR",
]

# Hourly Resource Outage Capacity
# https://www.ercot.com/mp/data-products/data-product-details?id=NP3-233-CD
HOURLY_RESOURCE_OUTAGE_CAPACITY_RTID = 13103

# Wind Power Production - Hourly Averaged Actual and Forecasted Values
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-732-CD
WIND_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_RTID = 13028

# Wind Power Production - Hourly Averaged Actual and Forecasted Values by Geographical Region  # noqa
# https://www.ercot.com/mp/data-products/data-product-details?id=np4-742-cd
WIND_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_BY_GEOGRAPHICAL_REGION_RTID = (  # noqa
    14787
)

# Solar Power Production - Hourly Averaged Actual and Forecasted Values by Geographical Region # noqa
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-745-CD
SOLAR_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_BY_GEOGRAPHICAL_REGION_RTID = (  # noqa
    21809
)

# Solar Power Production - Hourly Averaged Actual and Forecasted Values
# https://www.ercot.com/mp/data-products/data-product-details?id=np4-737-cd
SOLAR_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_RTID = 13483

"""
Settlement	Point Type	Description
==========	==========	===========
Resource Node	RN	Resource Node for normal resource
Resource Node	PCCRN	Physical Resource Node for combined cycle units
Resource Node	LCCRN	Logical Resource Node for combined cycle plant
Resource Node	PUN	Private Area Network Resource Node
Load Zone	LZ	Congestion Load Zone
Load Zone	LZ_DC	DCTIE Load Zone
Hub	HU	Hub
Hub	SH	ERCOT_345KV_HUBBUSES_AVG
Hub	AH	ERCOT_HUB_AVG
============================================================
Source: https://www.ercot.com/files/docs/2009/10/26/07_tests_for_rsnable_lmps_overview_of_price_valid_tool_09102.ppt
"""  # noqa
RESOURCE_NODE_SETTLEMENT_TYPES = ["RN", "PCCRN", "LCCRN", "PUN"]
LOAD_ZONE_SETTLEMENT_TYPES = ["LZ", "LZ_DC"]
HUB_SETTLEMENT_TYPES = ["HU", "SH", "AH"]


@dataclass
class Document:
    url: str
    publish_date: pd.Timestamp
    constructed_name: str
    friendly_name: str
    friendly_name_timestamp: pd.Timestamp


def parse_timestamp_from_friendly_name(friendly_name: str) -> pd.Timestamp:
    parts = friendly_name.replace("_retry", "").split("_")
    date_str = parts[1]
    time_str = parts[2]
    # Add a colon between hours, minutes, and seconds for pandas to parse
    if len(time_str) > 4:
        second_str = time_str[4:6]
    else:
        second_str = "00"

    time_str_formatted = time_str[:2] + ":" + time_str[2:4] + ":" + second_str

    # Combine date and time strings
    datetime_str = date_str + " " + time_str_formatted
    # Convert to pandas timestampp
    try:
        timestamp = pd.to_datetime(datetime_str, format="%Y%m%d %H:%M:%S").tz_localize(
            Ercot.default_timezone,
            ambiguous=False,
        )
    except:  # noqa
        raise
    return timestamp


class Ercot(ISOBase):
    """Electric Reliability Council of Texas (ERCOT)"""

    name = "Electric Reliability Council of Texas"
    iso_id = "ercot"
    default_timezone = "US/Central"

    status_homepage = "https://www.ercot.com/gridmktinfo/dashboards/gridconditions"
    interconnection_homepage = (
        "http://mis.ercot.com/misapp/GetReports.do?reportTypeId=15933"
    )

    markets = [
        Markets.REAL_TIME_15_MIN,
        Markets.DAY_AHEAD_HOURLY,
    ]

    location_types = [
        LOCATION_TYPE_HUB,
        LOCATION_TYPE_ZONE,
        LOCATION_TYPE_RESOURCE_NODE,
    ]

    BASE = "https://www.ercot.com/api/1/services/read/dashboards"
    ACTUAL_LOADS_FORECAST_ZONES_URL_FORMAT = "https://www.ercot.com/content/cdr/html/{timestamp}_actual_loads_of_forecast_zones.html"  # noqa
    ACTUAL_LOADS_WEATHER_ZONES_URL_FORMAT = "https://www.ercot.com/content/cdr/html/{timestamp}_actual_loads_of_weather_zones.html"  # noqa
    LOAD_HISTORICAL_MAX_DAYS = 14

    def get_status(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns status of grid"""
        if date != "latest":
            raise NotSupported()

        r = self._get_json(self.BASE + "/daily-prc.json", verbose=verbose)

        time = (
            pd.to_datetime(r["current_condition"]["datetime"], unit="s")
            .tz_localize("UTC")
            .tz_convert(self.default_timezone)
        )
        status = r["current_condition"]["state"]
        reserves = float(r["current_condition"]["prc_value"].replace(",", ""))

        if status == "normal":
            status = "Normal"

        notes = [r["current_condition"]["condition_note"]]

        return GridStatus(
            time=time,
            status=status,
            reserves=reserves,
            iso=self,
            notes=notes,
        )

    def get_energy_storage_resources(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = "latest",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get energy storage resources.
        Always returns data from previous and current day"""
        url = self.BASE + "/energy-storage-resources.json"
        data = self._get_json(url, verbose=verbose)

        df = pd.DataFrame(data["previousDay"]["data"] + data["currentDay"]["data"])

        # TODO(kanter): fix this for future DST dates
        for timestamp in ["2024-11-03 02:00:00-0600", "2025-11-02 02:00:00-0600"]:
            if timestamp in df["timestamp"].values:
                # ERCOT publishes two intervals with 2am timestamp
                # during CDT to CST transition
                # but skips the repeated 1am timestamp
                # let's manually fix this before further timestamp parsing
                df.loc[
                    (df["timestamp"] == timestamp) & (df["dstFlag"] == "N"),
                    "timestamp",
                ] = timestamp.replace("02:00:00", "01:00:00")

        df = df[["timestamp", "totalCharging", "totalDischarging", "netOutput"]]

        # Parse in UTC to avoid issues with DST transition
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(
            self.default_timezone,
        )

        df = df.rename(
            columns={
                "timestamp": "Time",
                "totalCharging": "Total Charging",
                "totalDischarging": "Total Discharging",
                "netOutput": "Net Output",
            },
        )

        df = df.sort_values("Time").reset_index(drop=True)
        return df

    def get_fuel_mix(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get fuel mix 5 minute intervals

        Arguments:
            date (datetime.date, str): "latest", "today",
                and yesterday's date are supported.

            verbose(bool): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with columns; Time and columns for each fuel \
                type
        """
        data = self._get_fuel_mix(date, verbose=verbose)

        dfs = []
        for day in data["data"].keys():
            df = pd.DataFrame(data["data"][day])
            # Only care about the gen for this method
            df_transformed = df.apply(
                lambda col: col.apply(
                    lambda x: x.get("gen") if isinstance(x, dict) else pd.NA,
                ),
            ).T
            dfs.append(df_transformed)

        mix = pd.concat(dfs)

        return self._handle_fuel_mix(
            date,
            mix,
            [
                "Time",
                "Coal and Lignite",
                "Hydro",
                "Nuclear",
                "Power Storage",
                "Solar",
                "Wind",
                "Natural Gas",
                "Other",
            ],
        )

    def _get_fuel_mix(
        self,
        date: str | datetime.datetime | pd.Timestamp,
        verbose: bool,
    ):
        if date != "latest":
            if not (
                utils.is_today(date, tz=self.default_timezone)
                or utils.is_yesterday(date, tz=self.default_timezone)
            ):
                raise NotSupported()

        url = self.BASE + "/fuel-mix.json"
        data = self._get_json(url, verbose=verbose)

        return data

    def _handle_fuel_mix(
        self,
        date: str | datetime.datetime | pd.Timestamp,
        data: pd.DataFrame,
        columns: List[str],
    ):
        data.index.name = "Time"
        data = data.reset_index()

        # need to use apply since there can be mixed
        # fixed offsets during dst transition
        # that result in object dtypes in pandas
        data["Time"] = data["Time"].apply(lambda x: pd.to_datetime(x).tz_convert("UTC"))

        # most timestamps are a few seconds off round 5 minute ticks
        # round to nearest minute. must do in utc to avoid dst issues
        data["Time"] = data["Time"].round("min").dt.tz_convert(self.default_timezone)

        data = data[columns]

        if date == "latest":
            return data

        parsed_date = utils._handle_date(date, self.default_timezone)

        return data[data["Time"].dt.date == parsed_date.date()].reset_index(drop=True)

    def get_fuel_mix_detailed(
        self,
        date: str | datetime.datetime | pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """The fuel mix with gen, hsl, and seasonal capacity for each fuel type."""
        data = self._get_fuel_mix(date, verbose=verbose)

        capacity = data["monthlyCapacity"]

        dfs = []

        for day in data["data"].keys():
            df = pd.DataFrame(data["data"][day])
            df_transformed = df.T
            dfs.append(df_transformed)

        mix = pd.concat(dfs)

        # Each col is a tuple of (gen, hsl, seasonalCapacity). We want to split this
        # to separate columns
        cols_to_drop = []
        for col in mix.columns:
            mix[col + " Gen"] = mix[col].apply(lambda x: x.get("gen"))
            mix[col + " HSL"] = mix[col].apply(lambda x: x.get("hsl"))
            mix[col + " Seasonal Capacity"] = capacity[col]
            cols_to_drop.append(col)

        mix = mix.drop(columns=cols_to_drop)

        return self._handle_fuel_mix(
            date,
            mix,
            [
                "Time",
                "Coal and Lignite Gen",
                "Coal and Lignite HSL",
                "Coal and Lignite Seasonal Capacity",
                "Hydro Gen",
                "Hydro HSL",
                "Hydro Seasonal Capacity",
                "Nuclear Gen",
                "Nuclear HSL",
                "Nuclear Seasonal Capacity",
                "Power Storage Gen",
                "Power Storage HSL",
                "Power Storage Seasonal Capacity",
                "Solar Gen",
                "Solar HSL",
                "Solar Seasonal Capacity",
                "Wind Gen",
                "Wind HSL",
                "Wind Seasonal Capacity",
                "Natural Gas Gen",
                "Natural Gas HSL",
                "Natural Gas Seasonal Capacity",
                "Other Gen",
                "Other HSL",
                "Other Seasonal Capacity",
            ],
        )

    @support_date_range("DAY_START")
    def get_load(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get load for a date

        Arguments:
            date (datetime.date, str): "latest", "today", or a date string
                are supported.


        """
        if date == "latest":
            return self.get_load("today", verbose=verbose)

        elif utils.is_today(date, tz=self.default_timezone):
            df = self._get_todays_outlook_non_forecast(date, verbose=verbose)
            df = df.rename(columns={"demand": "Load"})
            return df[["Time", "Interval Start", "Interval End", "Load"]]

        elif utils.is_within_last_days(
            date,
            self.LOAD_HISTORICAL_MAX_DAYS,
            tz=self.default_timezone,
        ):
            df = self._get_forecast_zone_load_html(date, verbose).rename(
                columns={"TOTAL": "Load"},
            )
            return df[["Time", "Interval Start", "Interval End", "Load"]]

        else:
            raise NotSupported()

    @support_date_range("DAY_START")
    def get_load_by_weather_zone(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get hourly load for ERCOT weather zones

        Arguments:
            date (datetime.date, str):  "today", or a date string
                are supported.
            verbose(bool): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame

        """
        # Use the html page for both today and yesterday to ensure all the
        # data is retrieved. The html page is updated every hour at 20 mins
        # past the hour but the report is only published once per dat at 0550 UTC.
        if utils.is_today(date, tz=self.default_timezone) or utils.is_yesterday(
            date,
            tz=self.default_timezone,
        ):
            df = self._get_weather_zone_load_html(date, verbose=verbose)
        else:
            doc_info = self._get_document(
                report_type_id=ACTUAL_SYSTEM_LOAD_BY_WEATHER_ZONE,
                date=date + pd.DateOffset(days=1),  # published day after
                constructed_name_contains="csv.zip",
                verbose=verbose,
            )

            df = self.read_doc(doc_info, verbose=verbose)

        # Clean up columns to match load_forecast_by_weather_zone
        df.columns = df.columns.map(lambda x: x.replace("_", " ").title())

        df = df.rename(
            columns=self._weather_zone_column_name_mapping(),
        ).sort_values("Interval Start")

        df = utils.move_cols_to_front(
            df,
            [
                "Time",
                "Interval Start",
                "Interval End",
            ]
            + self._weather_zone_column_name_order()
            + ["System Total"],
        )

        return df

    @support_date_range("DAY_START")
    def get_load_by_forecast_zone(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get hourly load for ERCOT forecast zones

        Arguments:
            date (datetime.date, str):  "today", or a date string
                are supported.

            verbose(bool): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame
        """
        # Use the html page for both today and yesterday to ensure all the
        # data is retrieved.
        if utils.is_today(date, tz=self.default_timezone) or utils.is_yesterday(
            date,
            tz=self.default_timezone,
        ):
            df = self._get_forecast_zone_load_html(date, verbose=verbose)
        else:
            doc_info = self._get_document(
                report_type_id=ACTUAL_SYSTEM_LOAD_BY_FORECAST_ZONE,
                date=date + pd.DateOffset(days=1),  # published day after
                constructed_name_contains="csv.zip",
                verbose=verbose,
            )

            df = self.read_doc(doc_info, verbose=verbose)
        return df

    def _get_forecast_zone_load_html(
        self,
        when: pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns load for currentDay or previousDay"""
        url = self.ACTUAL_LOADS_FORECAST_ZONES_URL_FORMAT.format(
            timestamp=when.strftime("%Y%m%d"),
        )
        df = self._read_html_display(url=url, verbose=verbose)
        return df

    def _get_weather_zone_load_html(
        self,
        when: pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns load for currentDay or previousDay"""
        url = self.ACTUAL_LOADS_WEATHER_ZONES_URL_FORMAT.format(
            timestamp=when.strftime("%Y%m%d"),
        )
        df = self._read_html_display(
            url=url,
            verbose=verbose,
        )
        return df

    def _read_html_display(self, url: str, verbose: bool = False) -> pd.DataFrame:
        logger.info(f"Fetching {url}")

        dfs = pd.read_html(url, header=0)
        df = dfs[0]

        if df["Hour Ending"].dtype == "object":
            df["RepeatedHourFlag"] = df["Hour Ending"].str.contains("*", regex=False)
            df["Hour Ending"] = (
                df["Hour Ending"].str.replace("*", "", regex=False).str.strip()
            ).astype(int)
        else:
            # non dst transition day
            # so no repeated hours
            df["RepeatedHourFlag"] = False

        df["Interval Start"] = pd.to_datetime(df["Oper Day"]) + (
            df["Hour Ending"] / 100 - 1
        ).astype("timedelta64[h]")
        df["Interval Start"] = df["Interval Start"].dt.tz_localize(
            self.default_timezone,
            # Prevent linting to is False
            ambiguous=df["RepeatedHourFlag"] == False,  # noqa
        )
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        df["Time"] = df["Interval Start"]

        df = utils.move_cols_to_front(
            df,
            [
                "Time",
                "Interval Start",
                "Interval End",
            ],
        )

        to_drop = ["Oper Day", "Hour Ending", "RepeatedHourFlag"]
        df = df.drop(to_drop, axis=1)

        return df

    @support_date_range(frequency="YEAR_START")
    def get_hourly_load_post_settlements(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get historical hourly load data from ERCOT's load archives.

        Downloads zip files from https://www.ercot.com/gridinfo/load/load_hist
        and parses the historical load data by weather zones.

        Arguments:
            date (str, datetime): Year to download data for, or "latest" for most recent data
            end (str, datetime): End date for range, or None for single date
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame
        """
        if date == "latest":
            # NB: Gets the most recent year available, since they are published as annual files
            current_year = pd.Timestamp.now().year
            date = pd.Timestamp(f"{current_year}-01-01")
            end = pd.Timestamp(f"{current_year + 1}-01-01")

        date = utils._handle_date(date, self.default_timezone)
        end = utils._handle_date(end, self.default_timezone)

        logger.info(
            f"Fetching historical load data for year {date.year}",
        )
        return self._download_post_settlements_load_file(date.year)

    def _download_post_settlements_load_file(
        self,
        year: int,
    ) -> pd.DataFrame:
        """Download and parse ERCOT historical load data for a specific year."""

        page_url = "https://www.ercot.com/gridinfo/load/load_hist"
        response = requests.get(page_url)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        year_link = None

        for link in soup.find_all("a"):
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if str(year) in text and ("zip" in href.lower() or "xls" in href.lower()):
                year_link = href
                break

        if year_link.endswith(".zip"):
            zip_file = utils.get_zip_folder(year_link)
            filename = zip_file.namelist()[0]
            df = pd.read_excel(zip_file.open(filename))
        elif year_link.endswith(".xls") or year_link.endswith(".xlsx"):
            response = requests.get(year_link)
            response.raise_for_status()
            df = pd.read_excel(io.BytesIO(response.content))
        df = self._process_post_settlements_load_data(df)

        return df

    def _process_post_settlements_load_data(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        df.columns = df.columns.str.strip()

        # Not all of these columns are in all of the files,
        # but this process ignores the ones that aren't so we can
        # just maintain one list and not have a bunch of if/else
        # to parse the columns that are there.
        column_mapping = {
            "HOUR_ENDING": "Interval End",
            "Hour Ending": "Interval End",
            "Hour_End": "Interval End",
            "HourEnding": "Interval End",
            "COAST": "Coast",
            "EAST": "East",
            "FWEST": "Far West",
            "FAR_WEST": "Far West",
            "NORTH": "North",
            "NCENT": "North Central",
            "NORTH_C": "North Central",
            "SOUTH": "South",
            "SOUTHERN": "South",
            "SCENT": "South Central",
            "SOUTH_C": "South Central",
            "WEST": "West",
            "ERCOT": "ERCOT",
            "TOTAL": "ERCOT",
        }

        existing_columns = [col for col in column_mapping.keys() if col in df.columns]
        rename_dict = {col: column_mapping[col] for col in existing_columns}
        df = df.rename(columns=rename_dict)

        if pd.api.types.is_datetime64_any_dtype(df["Interval End"]):
            if df["Interval End"].dt.tz is not None:
                df["Interval End"] = df["Interval End"].dt.tz_convert(
                    self.default_timezone,
                    ambiguous=True,
                    nonexistent="shift_forward",
                )
            else:
                df["Interval End"] = df["Interval End"].dt.round("h")
                df["Interval End"] = df["Interval End"].dt.tz_localize(
                    self.default_timezone,
                    ambiguous=True,
                    nonexistent="shift_forward",
                )

        else:
            df["Interval End"] = df["Interval End"].astype(str)

            # Convert 24:00 to next day's 00:00 directly in string format
            # Doing so avoids the DST transition issue of converting 24:00 to 00:00
            # and then adding a day to the interval end.
            def convert_24_hour(date_str: str) -> str:
                if " 24:00" in date_str:
                    # Parse the date part and add one day
                    date_part = date_str.split(" ")[0]
                    parsed_date = pd.to_datetime(date_part) + pd.Timedelta(days=1)
                    return parsed_date.strftime("%m/%d/%Y") + " 00:00"
                return date_str

            df["Interval End"] = df["Interval End"].apply(convert_24_hour)
            df["Interval End"] = df["Interval End"].str.replace(" DST", "")
            df["Interval End"] = pd.to_datetime(df["Interval End"], errors="coerce")
            df["Interval End"] = df["Interval End"].dt.round("h")
            df["Interval End"] = df["Interval End"].dt.tz_localize(
                self.default_timezone,
                ambiguous=True,
                nonexistent="shift_forward",
            )

        df["Interval Start"] = df["Interval End"] - pd.Timedelta(hours=1)

        # Fix DST fall-back duplicates - find any October/November date with duplicate 1 AM hours
        fall_dst_mask = (df["Interval Start"].dt.month.isin([10, 11])) & (
            df["Interval Start"].dt.hour == 1
        )
        if fall_dst_mask.any():
            # Group by date to find dates with exactly 2 occurrences of 1 AM
            fall_dates = df[fall_dst_mask]["Interval Start"].dt.date.value_counts()
            duplicate_dates = fall_dates[fall_dates == 2].index

            for dup_date in duplicate_dates:
                date_mask = (df["Interval Start"].dt.date == dup_date) & (
                    df["Interval Start"].dt.hour == 1
                )
                duplicate_indices = df[date_mask].index
                second_idx = duplicate_indices[1]
                logger.debug(
                    f"Changing timezone for DST duplicate at {df.loc[second_idx, 'Interval Start']}",
                )
                # Change from -06:00 to -05:00 by subtracting 1 hour
                df.loc[second_idx, "Interval Start"] = df.loc[
                    second_idx,
                    "Interval Start",
                ] - pd.Timedelta(hours=1)
                df.loc[second_idx, "Interval End"] = df.loc[
                    second_idx,
                    "Interval End",
                ] - pd.Timedelta(hours=1)

        numeric_columns = [
            "Coast",
            "East",
            "Far West",
            "North",
            "North Central",
            "South",
            "South Central",
            "West",
            "ERCOT",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce",
                )

        expected_columns = [
            "Interval Start",
            "Interval End",
            "Coast",
            "East",
            "Far West",
            "North",
            "North Central",
            "South",
            "South Central",
            "West",
            "ERCOT",
        ]
        df = df.dropna(subset=["Interval Start", "Interval End"])
        return df[expected_columns].sort_values("Interval Start").reset_index(drop=True)

    def _get_supply_demand_json(self) -> dict:
        url = self.BASE + "/supply-demand.json"
        logger.info(f"Fetching {url}")

        return self._get_json(url)

    def _get_update_timestamp_from_supply_demand_json(
        self,
        supply_demand_json: dict,
    ) -> pd.Timestamp:
        return pd.to_datetime(supply_demand_json["lastUpdated"])

    def _get_todays_outlook_non_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        assert date == "latest" or utils.is_today(
            date,
            self.default_timezone,
        ), "Only today's data is supported"

        supply_demand_json = self._get_supply_demand_json()
        data = pd.DataFrame(supply_demand_json["data"])

        # Parse in UTC to then convert to local to avoid DST transition issues because
        # of mixed timezones
        data["Interval End"] = pd.to_datetime(
            data["epoch"],
            unit="ms",
            utc=True,
        ).dt.tz_convert(self.default_timezone)

        data["Interval Start"] = data["Interval End"] - pd.Timedelta(minutes=5)
        data["Time"] = data["Interval Start"]

        data = data[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "demand",
                "forecast",
                "capacity",
            ]
        ]

        data = data[data["forecast"] == 0]  # only keep non forecast rows

        return data.reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        forecast_type: ERCOTSevenDayLoadForecastReport = ERCOTSevenDayLoadForecastReport.BY_FORECAST_ZONE,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns load forecast of specified forecast type.

        If date range provided, returns all hourly reports published within.

        Note: only limited historical data is available


        Arguments:
            date (str, datetime): datetime to download. If `end` not provided,
                returns last hourly report published before. if "latest",
                returns most recent hourly report. if `end` provided,
                returns all hourly reports published after this date
                and before `end`.

            end (str, datetime,): if provided, returns all hourly reports published
                after `date` and before `end`


            forecast_type (ERCOTSevenDayLoadForecastReport): The load forecast type.
                Enum of possible values.
            verbose (bool, optional): print verbose output. Defaults to False.
        """
        # TODO: migrate to _get_hourly_report
        if end is None:
            doc = self._get_document(
                report_type_id=forecast_type.value,
                published_before=date,
                constructed_name_contains="csv.zip",
                verbose=verbose,
            )
            docs = [doc]
        else:
            docs = self._get_documents(
                report_type_id=forecast_type.value,
                published_after=date,
                published_before=end,
                constructed_name_contains="csv.zip",
                verbose=verbose,
            )

        all_df = []
        for doc in docs:
            df = self._handle_load_forecast(
                doc,
                forecast_type=forecast_type,
                verbose=verbose,
            )

            all_df.append(df)

        df = pd.concat(all_df)

        df = df.sort_values("Publish Time")

        return df

    def _handle_load_forecast(
        self,
        doc: Document,
        forecast_type: ERCOTSevenDayLoadForecastReport,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Function to handle the different types of load forecast parsing.

        """
        df = self.read_doc(doc, verbose=verbose)

        df["Publish Time"] = doc.publish_date

        df = df.rename(columns={"SystemTotal": "System Total"})

        cols_to_move = [
            "Time",
            "Interval Start",
            "Interval End",
            "Publish Time",
        ]

        if forecast_type.value == ERCOTSevenDayLoadForecastReport.BY_WEATHER_ZONE.value:
            df = df.rename(
                columns=self._weather_zone_column_name_mapping(),
            )

            cols_to_move += self._weather_zone_column_name_order() + ["System Total"]

        df = utils.move_cols_to_front(df, cols_to_move)

        return df

    def get_capacity_committed(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = "latest",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the actual committed capacity (the amount of power available from
        generating units that were on-line or providing operating reserves).

        Data is ephemeral and does not support past days.
        """
        data = self._get_capacity_dataset(verbose=verbose)

        return (
            data.loc[
                # Actual values
                data["forecast"] == 0,
                ["Interval Start", "Interval End", "capacity"],
            ]
            .rename(columns={"capacity": "Capacity"})
            .reset_index(drop=True)
        )

    def get_capacity_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = "latest",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the forecasted committed capacity (Committed Capacity) and the
        forecasted available capacity (Available Capacity) for the current day.

        Data is ephemeral and does not support past days.
        """
        data = self._get_capacity_dataset(verbose=verbose)

        # Forecast values
        return (
            data.loc[
                data["forecast"] == 1,
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "capacity",
                    "available",
                ],
            ]
            .rename(
                columns={
                    "capacity": "Committed Capacity",
                    "available": "Available Capacity",
                },
            )
            .reset_index(drop=True)
        )

    def _get_capacity_dataset(self, verbose: bool = False) -> pd.DataFrame:
        supply_demand_json = self._get_supply_demand_json()

        data = pd.DataFrame(supply_demand_json["data"])

        data.loc[
            :,
            "Publish Time",
        ] = self._get_update_timestamp_from_supply_demand_json(supply_demand_json)

        data["Interval Start"] = pd.to_datetime(
            data["epoch"],
            unit="ms",
            utc=True,
        ).dt.tz_convert(self.default_timezone)
        data["Interval End"] = data["Interval Start"] + pd.Timedelta(minutes=5)

        return data[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "capacity",
                "forecast",
                "available",
            ]
        ].sort_values("Interval Start")

    def get_available_seasonal_capacity_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = "latest",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the forecasted demand (Load Forecast) and the forecasted available
        seasonal capacity (Available Capacity) for the next 6 days.

        Data is ephemeral and does not support past days.
        """
        supply_demand_json = self._get_supply_demand_json()
        data = pd.DataFrame(supply_demand_json["forecast"])

        # Use epoch to get the UTC timestamps then convert to local to avoid issues
        # around DST transitions
        data["Interval End"] = pd.to_datetime(
            data["epoch"],
            unit="ms",
            utc=True,
        ).dt.tz_convert(self.default_timezone)
        data["Interval Start"] = data["Interval End"] - pd.Timedelta(hours=1)

        data.loc[
            :,
            "Publish Time",
        ] = self._get_update_timestamp_from_supply_demand_json(supply_demand_json)

        return (
            data[
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "availCapGen",
                    "forecastedDemand",
                ]
            ]
            .rename(
                columns={
                    "availCapGen": "Available Capacity",
                    "forecastedDemand": "Load Forecast",
                },
            )
            .sort_values("Interval Start")
            .reset_index(drop=True)
        )

    def get_rtm_spp(self, year: int, verbose: bool = False) -> pd.DataFrame:
        """Get Historical RTM Settlement Point Prices(SPPs)
            for each of the Hubs and Load Zones

        Arguments:
            year(int): year to get data for
                Starting 2011, returns data for the entire year

        Source:
            https://www.ercot.com/mp/data-products/data-product-details?id=NP6-785-ER
        """  # noqa
        doc_info = self._get_document(
            report_type_id=HISTORICAL_RTM_LOAD_ZONE_AND_HUB_PRICES_RTID,
            constructed_name_contains=f"{year}.zip",
            verbose=verbose,
        )

        x = utils.get_zip_file(doc_info.url, verbose=verbose)
        all_sheets = pd.read_excel(x, sheet_name=None)
        df = pd.concat(all_sheets.values())

        # fix parsing error where no data is present
        # should only be 1 row per year
        count = df[["Delivery Hour", "Delivery Interval"]].isnull().all(axis=1).sum()
        if count == 1:
            df = df.dropna(
                subset=["Delivery Hour", "Delivery Interval"],
                how="all",
            )
        elif count > 1:
            raise ValueError(
                "Parsing error, more than expected null rows found",
            )

        df["Delivery Interval"] = df["Delivery Interval"].astype("Int64")
        df = self.parse_doc(df, verbose=verbose)
        return self._finalize_spp_df(
            df,
            market=Markets.REAL_TIME_15_MIN,
            verbose=verbose,
        )

    def get_dam_spp(self, year: int, verbose: bool = False) -> pd.DataFrame:
        """Get Historical DAM Settlement Point Prices(SPPs)
        for each of the Hubs and Load Zones

        Arguments:
            year(int): year to get data for.
                Starting 2011, returns data for the entire year


        Source:
            https://www.ercot.com/mp/data-products/data-product-details?id=NP4-180-ER
        """
        doc_info = self._get_document(
            report_type_id=HISTORICAL_DAM_LOAD_ZONE_AND_HUB_PRICES_RTID,
            constructed_name_contains=f"{year}.zip",
            verbose=verbose,
        )

        x = utils.get_zip_file(doc_info.url, verbose=verbose)
        all_sheets = pd.read_excel(x, sheet_name=None)
        df = pd.concat(all_sheets.values())
        # filter where DSTFlag == 10
        df = self.parse_doc(df, verbose=verbose)
        return self._finalize_spp_df(
            df,
            market=Markets.DAY_AHEAD_HOURLY,
            verbose=verbose,
        )

    def get_raw_interconnection_queue(self, verbose: bool = False) -> BinaryIO:
        doc_info = self._get_document(
            report_type_id=GIS_REPORT_RTID,
            constructed_name_contains="GIS_Report",
            verbose=verbose,
        )
        logger.info(f"Downloading interconnection queue from: {doc_info.url} ")
        response = requests.get(doc_info.url)
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose: bool = False) -> pd.DataFrame:
        """
        Get interconnection queue for ERCOT

        Monthly historical data available here:
            http://mis.ercot.com/misapp/GetReports.do?reportTypeId=15933&reportTitle=GIS%20Report&showHTMLView=&mimicKey
        """  # noqa
        raw_data = self.get_raw_interconnection_queue(verbose)
        # TODO other sheets for small projects, inactive, and cancelled project
        # TODO see if this data matches up with summaries in excel file
        # TODO historical data available as well

        # skip rows and handle header
        queue = pd.read_excel(
            raw_data,
            sheet_name="Project Details - Large Gen",
            skiprows=30,
        ).iloc[4:]

        queue["State"] = "Texas"
        queue["Queue Date"] = queue["Screening Study Started"]

        fuel_type_map = {
            "BIO": "Biomass",
            "COA": "Coal",
            "GAS": "Gas",
            "GEO": "Geothermal",
            "HYD": "Hydrogen",
            "NUC": "Nuclear",
            "OIL": "Fuel Oil",
            "OTH": "Other",
            "PET": "Petcoke",
            "SOL": "Solar",
            "WAT": "Water",
            "WIN": "Wind",
        }

        technology_type_map = {
            "BA": "Battery Energy Storage",
            "CC": "Combined-Cycle",
            "CE": "Compressed Air Energy Storage",
            "CP": "Concentrated Solar Power",
            "EN": "Energy Storage",
            "FC": "Fuel Cell",
            "GT": "Combustion (gas) Turbine, but not part of a Combined-Cycle",
            "HY": "Hydroelectric Turbine",
            "IC": "Internal Combustion Engine, eg. Reciprocating",
            "OT": "Other",
            "PV": "Photovoltaic Solar",
            "ST": "Steam Turbine other than Combined-Cycle",
            "WT": "Wind Turbine",
        }

        queue["Fuel"] = queue["Fuel"].map(fuel_type_map)
        queue["Technology"] = queue["Technology"].map(technology_type_map)

        queue["Generation Type"] = queue["Fuel"] + " - " + queue["Technology"]

        queue["Status"] = (
            queue["IA Signed"]
            .isna()
            .map(
                {
                    True: InterconnectionQueueStatus.ACTIVE.value,
                    False: InterconnectionQueueStatus.COMPLETED.value,
                },
            )
        )

        queue["Actual Completion Date"] = queue["Approved for Synchronization"]

        rename = {
            "INR": "Queue ID",
            "Project Name": "Project Name",
            "Interconnecting Entity": "Interconnecting Entity",
            "Projected COD": "Proposed Completion Date",
            "POI Location": "Interconnection Location",
            "County": "County",
            "State": "State",
            "Capacity (MW)": "Capacity (MW)",
            "Queue Date": "Queue Date",
            "Generation Type": "Generation Type",
            "Actual Completion Date": "Actual Completion Date",
            "Status": "Status",
        }

        # todo: there are a few columns being parsed
        # as "unamed" that aren't being included but should
        extra_columns = [
            "Fuel",
            "Technology",
            "GIM Study Phase",
            "Screening Study Started",
            "Screening Study Complete",
            "FIS Requested",
            "FIS Approved",
            "Economic Study Required",
            "IA Signed",
            "Air Permit",
            "GHG Permit",
            "Water Availability",
            "Meets Planning",
            "Meets All Planning",
            "CDR Reporting Zone",
            # "Construction Start", # all null
            # "Construction End", # all null
            "Approved for Energization",
            "Approved for Synchronization",
            "Comment",
        ]

        missing = [
            # todo the actual complettion date can be calculated by
            # looking at status and other date columns
            "Withdrawal Comment",
            "Transmission Owner",
            "Summer Capacity (MW)",
            "Winter Capacity (MW)",
            "Withdrawn Date",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        return queue

    @support_date_range(frequency=None)
    def get_lmp(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        location_type: str = SETTLEMENT_POINT_LOCATION_TYPE,  # TODO: support 'ALL'
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get LMP data for ERCOT normally produced by SCED every five minutes

        Can specify the location type to return "electrical bus"
        or "settlement point" data. Defaults to "settlement point"

        """
        if location_type.lower() == ELECTRICAL_BUS_LOCATION_TYPE.lower():
            report = LMPS_BY_ELECTRICAL_BUS_RTID
        elif location_type.lower() == SETTLEMENT_POINT_LOCATION_TYPE.lower():
            report = LMPS_BY_SETTLEMENT_POINT_RTID
        else:
            raise ValueError(
                f"Invalid location type: {location_type}. Must be 'settlement point' or 'electrical bus'",  # noqa
            )

        # if end is None, assume requesting one day
        if end is None:
            start = None
            end = None
            date = date
        else:
            start = date
            end = end
            date = None

        docs = self._get_documents(
            report_type_id=report,
            date=date,
            friendly_name_timestamp_after=start,
            friendly_name_timestamp_before=end,
            extension="csv",
            verbose=verbose,
        )

        return self._handle_lmp(docs=docs, verbose=verbose)

    def _handle_lmp(
        self,
        docs: list[Document],
        verbose: bool = False,
        sced: bool = True,
    ) -> pd.DataFrame:
        df = self.read_docs(
            docs,
            parse=False,
            # need to return a DF that works with the
            # logic in rest of function
            empty_df=pd.DataFrame(
                columns=[
                    "SCEDTimestamp",
                    "RepeatedHourFlag",
                    "Location",
                    "Location Type",
                    "LMP",
                ],
            ),
            verbose=verbose,
        )

        return self._handle_lmp_df(df, verbose=verbose, sced=sced)

    def _handle_lmp_df(
        self,
        df: pd.DataFrame,
        verbose: bool = False,
        sced: bool = True,
    ) -> pd.DataFrame:
        df = self._handle_sced_timestamp(df=df, verbose=verbose)

        if "SettlementPoint" in df.columns:
            df = self._handle_settlement_point_name_and_type(df, verbose=verbose)
        elif "ElectricalBus" in df.columns:
            # do same thing as settlement point but for electrical bus
            df = df.rename(
                columns={
                    "ElectricalBus": "Location",
                },
            )
            df["Location Type"] = ELECTRICAL_BUS_LOCATION_TYPE
            # make Location string and location type category
            df["Location"] = df["Location"].astype("string")
            df["Location Type"] = df["Location Type"].astype("category")

        df["Market"] = (
            Markets.REAL_TIME_SCED.value if sced else Markets.DAY_AHEAD_HOURLY.value
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "SCED Timestamp",
                "Market",
                "Location",
                "Location Type",
                "LMP",
            ]
        ]
        # sort by SCED Timestamp and Location
        df = df.sort_values(
            [
                "SCED Timestamp",
                "Location",
            ],
        ).reset_index(drop=True)

        return df

    @lmp_config(
        supports={
            Markets.REAL_TIME_15_MIN: ["latest", "today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["latest", "today", "historical"],
        },
    )
    @support_date_range(frequency=None)
    def get_spp(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        market: str = None,
        locations: list = "ALL",
        location_type: str = "ALL",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get SPP data for ERCOT

        Supported Markets:
            - ``REAL_TIME_15_MIN``
            - ``DAY_AHEAD_HOURLY``

        Supported Location Types:
            - ``Load Zone``
            - ``Trading Hub``
            - ``Resource Node``
        """

        publish_date = None
        published_before = None
        published_after = None
        friendly_name_timestamp_before = None
        friendly_name_timestamp_after = None

        if market == Markets.REAL_TIME_15_MIN:
            if date == "latest":
                publish_date = "latest"
            elif end is None:
                # no end, so assume requesting one day
                # use the timestamp from the friendly name
                friendly_name_timestamp_after = date.normalize()
                friendly_name_timestamp_before = (
                    friendly_name_timestamp_after + pd.DateOffset(days=1)
                )

            else:
                friendly_name_timestamp_after = date
                friendly_name_timestamp_before = end

            report = SETTLEMENT_POINT_PRICES_AT_RESOURCE_NODES_HUBS_AND_LOAD_ZONES_RTID
        elif market == Markets.DAY_AHEAD_HOURLY:
            if date == "latest":
                publish_date = "latest"
            elif end is None:
                # no end, so assume requesting one day
                # data is publish one day prior
                publish_date = date.normalize() - pd.DateOffset(days=1)
            else:
                published_before = end
                published_after = date
            report = DAM_SETTLEMENT_POINT_PRICES_RTID

        docs = self._get_documents(
            report_type_id=report,
            date=publish_date,
            published_before=published_before,
            published_after=published_after,
            friendly_name_timestamp_before=friendly_name_timestamp_before,
            friendly_name_timestamp_after=friendly_name_timestamp_after,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )

        df = self.read_docs(
            docs,
            empty_df=pd.DataFrame(
                columns=[
                    "Time",
                    "Interval Start",
                    "Interval End",
                    "Location",
                    "Location Type",
                    "Market",
                    "SPP",
                ],
            ),
            verbose=verbose,
        )

        return self._finalize_spp_df(
            df,
            locations=locations,
            location_type=location_type,
            market=market,
            verbose=verbose,
        )

    def _handle_settlement_point_name_and_type(
        self,
        df: pd.DataFrame,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = df.rename(
            columns={
                "SettlementPoint": "Location",
                "Settlement Point": "Location",
                "SettlementPointName": "Location",
                "Settlement Point Name": "Location",
            },
        )

        # todo is this needed if we are defaulting to resource node?
        mapping_df = self._get_settlement_point_mapping(verbose=verbose)
        resource_node = mapping_df["RESOURCE_NODE"].dropna().unique()

        # Create boolean masks for each location type
        is_hub = df["Location"].str.startswith("HB_")
        is_load_zone = df["Location"].str.startswith("LZ_")
        is_load_zone_dc_tie = df["Location"].str.startswith("DC_")
        is_resource_node = df["Location"].isin(resource_node)

        # Assign location types based on the boolean masks
        df.loc[is_hub, "Location Type"] = LOCATION_TYPE_HUB
        df.loc[is_load_zone, "Location Type"] = LOCATION_TYPE_ZONE
        df.loc[is_load_zone_dc_tie, "Location Type"] = LOCATION_TYPE_ZONE_DC
        df.loc[is_resource_node, "Location Type"] = LOCATION_TYPE_RESOURCE_NODE
        # If a location type is not found, default to LOCATION_TYPE_RESOURCE_NODE
        df["Location Type"] = df["Location Type"].fillna(LOCATION_TYPE_RESOURCE_NODE)

        # energy weighted only exists in real time data
        # since depends on energy usage
        if "SettlementPointType" in df.columns:
            is_load_zone_energy_weighted = df["SettlementPointType"] == "LZEW"
            is_load_zone_dc_tie_energy_weighted = df["SettlementPointType"] == "LZ_DCEW"

            df.loc[
                is_load_zone_energy_weighted,
                "Location Type",
            ] = LOCATION_TYPE_ZONE_EW
            df.loc[
                is_load_zone_dc_tie_energy_weighted,
                "Location Type",
            ] = LOCATION_TYPE_ZONE_DC_EW

            # append "_EW" to the end of the location name if it is energy weighted
            df.loc[is_load_zone_energy_weighted, "Location"] = (
                df.loc[is_load_zone_energy_weighted, "Location"] + "_EW"
            )

            df.loc[is_load_zone_dc_tie_energy_weighted, "Location"] = (
                df.loc[is_load_zone_dc_tie_energy_weighted, "Location"] + "_EW"
            )

        df["Location"] = df["Location"].astype("string")
        df["Location Type"] = df["Location Type"].astype("category")

        return df

    def _finalize_spp_df(
        self,
        df: pd.DataFrame,
        market: str,
        locations: list = None,
        location_type: str = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self._handle_settlement_point_name_and_type(df, verbose=verbose)

        df["Market"] = market.value

        df = df.rename(
            columns={
                "SettlementPointPrice": "SPP",
                "Settlement Point Price": "SPP",
            },
        )

        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "Market",
                "SPP",
            ]
        ]

        df = utils.filter_lmp_locations(
            df=df,
            locations=locations,
            location_type=location_type,
        )

        df = df.sort_values(by="Interval Start")
        df = df.reset_index(drop=True)

        return df

    @support_date_range(frequency="DAY_START")
    def get_as_prices(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get ancillary service clearing prices in hourly intervals in Day Ahead Market

        Arguments:
            date (datetime.date, str): date of delivery for AS services

            end (datetime.date, str, optional): if declared, function will return
                data as a range, from "date" to "end"

            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:

            pandas.DataFrame: A DataFrame with prices for "Non-Spinning Reserves", \
                "Regulation Up", "Regulation Down", "Responsive Reserves", \
                "ERCOT Contingency Reserve Service"
        """

        # subtract one day since it's the day ahead market happens on the day
        # before for the delivery day

        date = date - pd.DateOffset(days=1)

        doc_info = self._get_document(
            report_type_id=DAM_CLEARING_PRICES_FOR_CAPACITY_RTID,
            date=date,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )

        logger.info(f"Downloading {doc_info.url}")

        doc = self.read_doc(doc_info, verbose=verbose)

        df = self._finalize_as_price_df(
            doc,
            pivot=True,
        )

        return df

    @support_date_range(frequency="DAY_START")
    def get_as_plan(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Ancillary Service requirements by type and quantity for each hour of the
        current day plus the next 6 days

        Arguments:
            date (datetime.date, str): date of delivery for AS services

            end (datetime.date, str, optional): if declared, function will return
                data as a range, from "date" to "end"

            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with prices for ECRS, NSPIN, REGDN, REGUP, RRS
        """
        if date == "latest":
            return self.get_as_plan("today", verbose=verbose)

        doc_info = self._get_document(
            report_type_id=DAM_ANCILLARY_SERVICE_PLAN_RTID,
            date=date,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )

        logger.info(f"Downloading {doc_info.url}")

        doc = self.read_doc(doc_info, verbose=verbose).drop(columns=["Time"])
        doc["Publish Time"] = doc_info.publish_date

        return self._handle_as_plan(doc)

    def _handle_as_plan(self, doc: Document) -> pd.DataFrame:
        df = doc.pivot(
            index=["Interval Start", "Interval End", "Publish Time"],
            columns="AncillaryType",
            values="Quantity",
        ).reset_index()

        # For some hours where there are no values, the data has "Not Applicable"
        # which becomes a column in the pivot. We want to drop this column
        if "Not Applicable" in df.columns:
            df = df.drop(columns=["Not Applicable"])

        # ECRS went live 2023-06-10 and isn't present in the data before then
        if "ECRS" not in df.columns:
            df["ECRS"] = pd.NA

        # Put ECRS at the end to match as_prices
        df = utils.move_cols_to_front(
            df,
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "NSPIN",
                "REGDN",
                "REGUP",
                "RRS",
                "ECRS",
            ],
        ).sort_values(["Interval Start", "Publish Time"])

        df.columns.name = None

        return df

    @support_date_range("DAY_START")
    def get_60_day_sced_disclosure(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        process: bool = False,
        verbose: bool = False,
    ) -> dict:
        """Get 60 day SCED Disclosure data

        Arguments:
            date (datetime.date, str): date to return
            end (datetime.date, str, optional): if declared, function will return
                data as a range, from "date" to "end"
            process (bool, optional): if True, will process the data into
                standardized format. if False, will return raw data
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            dict: dictionary with keys "sced_load_resource", "sced_gen_resource", and
                "sced_smne", mapping to pandas.DataFrame objects
        """

        report_date = date + pd.DateOffset(days=60)

        doc_info = self._get_document(
            report_type_id=SIXTY_DAY_SCED_DISCLOSURE_REPORTS_RTID,
            date=report_date,
            constructed_name_contains="60_Day_SCED_Disclosure.zip",
            verbose=verbose,
        )
        z = utils.get_zip_folder(doc_info.url, verbose=verbose)

        data = self._handle_60_day_sced_disclosure(z, process=process, verbose=verbose)

        return data

    def _handle_60_day_sced_disclosure(
        self,
        z: ZipFile,
        process: bool = False,
        verbose: bool = False,
    ) -> dict:
        # TODO: there are other files in the zip folder
        load_resource_file = None
        gen_resource_file = None
        smne_file = None
        for file in z.namelist():
            cleaned_file = file.replace(" ", "_")
            if "60d_Load_Resource_Data_in_SCED" in cleaned_file:
                load_resource_file = file
            elif "60d_SCED_Gen_Resource_Data" in cleaned_file:
                gen_resource_file = file
            elif "60d_SCED_SMNE_GEN_RES" in cleaned_file:
                smne_file = file

        assert load_resource_file, "Could not find load resource file"
        assert gen_resource_file, "Could not find gen resource file"
        assert smne_file, "Could not find smne file"

        load_resource = pd.read_csv(z.open(load_resource_file))
        gen_resource = pd.read_csv(z.open(gen_resource_file))
        smne = pd.read_csv(z.open(smne_file))

        def handle_time(
            df: pd.DataFrame,
            time_col: str,
            is_interval_end: bool = False,
        ) -> pd.DataFrame:
            df[time_col] = pd.to_datetime(df[time_col])

            if "Repeated Hour Flag" in df.columns:
                # Repeated Hour Flag is Y during the repeated hour
                # So, it's N during DST And Y during Standard Time
                # Pandas wants True for DST and False for Standard Time
                # during ambiguous times
                df[time_col] = df[time_col].dt.tz_localize(
                    self.default_timezone,
                    ambiguous=df["Repeated Hour Flag"] == "N",
                )
                interval_start = df[time_col].dt.round(
                    "15min",
                    ambiguous=df["Repeated Hour Flag"] == "N",
                )

            else:
                # for SMNE data
                df[time_col] = (
                    df.sort_values("Interval Number", ascending=True)
                    .groupby("Resource Code")[time_col]
                    .transform(
                        lambda x: x.dt.tz_localize(
                            self.default_timezone,
                            ambiguous="infer",
                        ),
                    )
                )

                # convert to utc
                # bc round doesn't work with dst changes
                # without Repeated Hour Flag
                interval_start = (
                    df[time_col]
                    .dt.tz_convert("utc")
                    .dt.round("15min")
                    .dt.tz_convert(self.default_timezone)
                )

            interval_length = pd.Timedelta(minutes=15)
            if is_interval_end:
                interval_end = interval_start
                interval_start = interval_start - interval_length
            else:
                interval_end = interval_start + interval_length

            df.insert(0, "Interval Start", interval_start)
            df.insert(
                1,
                "Interval End",
                interval_end,
            )

            return df

        load_resource = load_resource.rename(
            columns={"SCED Time Stamp": "SCED Timestamp"},
        )
        gen_resource = gen_resource.rename(
            columns={"SCED Time Stamp": "SCED Timestamp"},
        )

        load_resource = handle_time(load_resource, time_col="SCED Timestamp")
        gen_resource = handle_time(gen_resource, time_col="SCED Timestamp")
        # no repeated hour flag like other ERCOT data
        # likely will error on DST change
        smne = handle_time(smne, time_col="Interval Time", is_interval_end=True)

        if process:
            logger.info("Processing 60 day SCED disclosure data")
            load_resource = process_sced_load(load_resource)
            gen_resource = process_sced_gen(gen_resource)
            smne = smne.rename(
                columns={
                    "Resource Code": "Resource Name",
                },
            )

        return {
            SCED_LOAD_RESOURCE_KEY: load_resource,
            SCED_GEN_RESOURCE_KEY: gen_resource,
            SCED_SMNE_KEY: smne,
        }

    @support_date_range("DAY_START")
    def get_60_day_dam_disclosure(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        process: bool = False,
        verbose: bool = False,
    ) -> dict:
        """Get 60 day DAM Disclosure data. Returns a dict with keys

        - "dam_gen_resource"
        - "dam_gen_resource_as_offers"
        - "dam_load_resource"
        - "dam_load_resource_as_offers"
        - "dam_energy_only_offer_awards"
        - "dam_energy_only_offers"
        - "dam_ptp_obligation_bid_awards"
        - "dam_ptp_obligation_bids"
        - "dam_energy_bid_awards"
        - "dam_energy_bids"
        - "dam_ptp_obligation_option"
        - "dam_ptp_obligation_option_awards"

        and values as pandas.DataFrame objects

        The date passed in should be the report date. Since reports are delayed by 60
        days, the passed date should not be fewer than 60 days in the past.
        """

        report_date = date + pd.DateOffset(days=60)

        doc_info = self._get_document(
            report_type_id=SIXTY_DAY_DAM_DISCLOSURE_REPORTS_RTID,
            date=report_date,
            constructed_name_contains="60_Day_DAM_Disclosure.zip",
            verbose=verbose,
        )

        z = utils.get_zip_folder(doc_info.url, verbose=verbose)

        data = self._handle_60_day_dam_disclosure(z, process=process, verbose=verbose)

        return data

    def _handle_60_day_dam_disclosure(
        self,
        z: ZipFile,
        process: bool = False,
        verbose: bool = False,
        files_prefix: dict = None,
    ) -> dict:
        if not files_prefix:
            files_prefix = {
                DAM_GEN_RESOURCE_KEY: "60d_DAM_Gen_Resource_Data-",
                DAM_GEN_RESOURCE_AS_OFFERS_KEY: "60d_DAM_Generation_Resource_ASOffers-",
                DAM_LOAD_RESOURCE_KEY: "60d_DAM_Load_Resource_Data-",
                DAM_LOAD_RESOURCE_AS_OFFERS_KEY: "60d_DAM_Load_Resource_ASOffers-",
                DAM_ENERGY_ONLY_OFFER_AWARDS_KEY: "60d_DAM_EnergyOnlyOfferAwards-",
                DAM_ENERGY_ONLY_OFFERS_KEY: "60d_DAM_EnergyOnlyOffers-",
                DAM_PTP_OBLIGATION_BID_AWARDS_KEY: "60d_DAM_PTPObligationBidAwards-",
                DAM_PTP_OBLIGATION_BIDS_KEY: "60d_DAM_PTPObligationBids-",
                DAM_ENERGY_BID_AWARDS_KEY: "60d_DAM_EnergyBidAwards-",
                DAM_ENERGY_BIDS_KEY: "60d_DAM_EnergyBids-",
                DAM_PTP_OBLIGATION_OPTION_KEY: "60d_DAM_PTP_Obligation_Option-",
                DAM_PTP_OBLIGATION_OPTION_AWARDS_KEY: "60d_DAM_PTP_Obligation_OptionAwards-",  # noqa
            }

        files = {}

        # find files in zip folder
        for key, file in files_prefix.items():
            for f in z.namelist():
                if file in f:
                    files[key] = f

        assert len(files) == len(files_prefix), "Missing files"

        data = {}

        for key, file in files.items():
            doc = pd.read_csv(z.open(file))
            # weird that these files dont have this column like all other ERCOT files
            # add so we can parse
            doc["DSTFlag"] = "N"
            data[key] = self.parse_doc(doc, verbose=verbose)

        if process:
            file_to_function = {
                DAM_GEN_RESOURCE_KEY: process_dam_gen,
                DAM_LOAD_RESOURCE_KEY: process_dam_load,
                DAM_GEN_RESOURCE_AS_OFFERS_KEY: process_dam_or_gen_load_as_offers,
                DAM_LOAD_RESOURCE_AS_OFFERS_KEY: process_dam_or_gen_load_as_offers,
                DAM_ENERGY_ONLY_OFFER_AWARDS_KEY: process_dam_energy_only_offer_awards,
                DAM_ENERGY_ONLY_OFFERS_KEY: process_dam_energy_only_offers,
                DAM_PTP_OBLIGATION_BID_AWARDS_KEY: process_dam_ptp_obligation_bid_awards,  # noqa
                DAM_PTP_OBLIGATION_BIDS_KEY: process_dam_ptp_obligation_bids,
                DAM_ENERGY_BID_AWARDS_KEY: process_dam_energy_bid_awards,
                DAM_ENERGY_BIDS_KEY: process_dam_energy_bids,
                DAM_PTP_OBLIGATION_OPTION_KEY: process_dam_ptp_obligation_option,
                DAM_PTP_OBLIGATION_OPTION_AWARDS_KEY: process_dam_ptp_obligation_option_awards,  # noqa
            }

            for file_name, process_func in file_to_function.items():
                if file_name in data:
                    data[file_name] = process_func(data[file_name])

        return data

    def get_sara(
        self,
        url: str = "https://www.ercot.com/files/docs/2023/05/05/SARA_Summer2023_Revised.xlsx",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Parse SARA data from url.

        Seasonal Assessment of Resource Adequacy for the ERCOT Region (SARA)

        Arguments:
            url (str, optional): url to download SARA data from. Defaults to
                Summer 2023 SARA data.

        """

        # only reading SummerCapacities right now
        # TODO: parse more sheets
        logger.info(f"Getting SARA data from {url}")
        df = pd.read_excel(url, sheet_name="SummerCapacities", header=1)

        # drop cols Unnamed: 0
        df = df.drop("Unnamed: 0", axis=1)

        df = df.rename(
            columns={
                "UNIT NAME": "Unit Name",
                "GENERATION INTERCONNECTION PROJECT CODE": "Generation Interconnection Project Code",  # noqa: E501
                "UNIT CODE": "Unit Code",
                "COUNTY": "County",
                "FUEL": "Fuel",
                "ZONE": "Zone",
                "IN SERVICE YEAR": "In Service Year",
                "INSTALLED CAPACITY RATING": "Installed Capacity Rating",
                "SUMMER\nCAPACITY\n(MW)": "Summer Capacity (MW)",
                "NEW PLANNED PROJECT ADDITIONS TO REPORT": "New Planned Project Additions to Report",  # noqa: E501
            },
        )
        # every unit should have this defined
        df = df.dropna(subset=["Fuel"])

        df["In Service Year"] = df["In Service Year"].astype("Int64")

        category_cols = ["County", "Fuel", "Zone"]
        for col in category_cols:
            df[col] = df[col].astype("category")

        return df

    def _finalize_as_price_df(
        self,
        doc: pd.DataFrame,
        pivot: bool = False,
    ) -> pd.DataFrame:
        doc["Market"] = "DAM"

        # recent daily files need to be pivoted
        if pivot:
            doc = doc.pivot_table(
                index=["Time", "Interval Start", "Interval End", "Market"],
                columns="AncillaryType",
                values="MCPC",
            ).reset_index()

            doc.columns.name = None

        # some columns from workbook contain trailing/leading whitespace
        doc.columns = [x.strip() for x in doc.columns]

        # NSPIN  REGDN  REGUP  RRS  ECRS
        rename = {
            "NSPIN": "Non-Spinning Reserves",
            "REGDN": "Regulation Down",
            "REGUP": "Regulation Up",
            "RRS": "Responsive Reserves",
            "ECRS": "ERCOT Contingency Reserve Service",
        }

        col_order = [
            "Time",
            "Interval Start",
            "Interval End",
            "Market",
            "Non-Spinning Reserves",
            "Regulation Down",
            "Regulation Up",
            "Responsive Reserves",
            "ERCOT Contingency Reserve Service",
        ]

        if "ECRS" not in doc.columns:
            doc["ECRS"] = None

        doc.rename(columns=rename, inplace=True)

        return doc[col_order]

    def get_as_monitor(
        self,
        date: str = "latest",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Ancillary Service Capacity Monitor.

        Parses table from
        https://www.ercot.com/content/cdr/html/as_capacity_monitor.html

        Arguments:
            date (str): only supports "latest"
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with ancillary service capacity monitor data
        """

        url = "https://www.ercot.com/content/cdr/html/as_capacity_monitor.html"
        logger.info(f"Getting Ancillary Service Capacity Monitor from {url}")
        html_content = requests.get(url).content
        df = self._parse_html_table(html_content)

        return df

    def get_system_as_capacity_monitor(
        self,
        date: str | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get System Ancillary Service Capacity Monitor.

        Fetches real-time ancillary service capacity data from
        https://www.ercot.com/api/1/services/read/dashboards/ancillary-service-capacity-monitor.json

        Arguments:
            date (str): only supports "latest"
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with system AS capacity monitor data
        """
        if date is not None and date != "latest":
            logger.warning(
                "date argument to get_system_as_capacity_monitor is ignored; only None or 'latest' are supported",
            )

        url = self.BASE + "/ancillary-service-capacity-monitor.json"
        logger.info(f"Getting System Capacity AS Monitor from {url}...")
        json_data = self._get_json(url, verbose=verbose)
        return self._parse_system_as_capacity_monitor(json_data)

    def _parse_system_as_capacity_monitor(self, json_data: dict) -> pd.DataFrame:
        """Parse JSON response from System Ancillary Service Capacity Monitor API.

        Arguments:
            json_data: Raw JSON response from the API

        Returns:
            pandas.DataFrame: Parsed data with standardized column names
        """
        key_to_column = {
            "rrcCapPfrGenEsr": "RRS Capability PFR Gen and ESR",
            "rrcCapLrWoClr": "RRS Capability Load Ex Controllable Load",
            "rrcCapLr": "RRS Capability PFR Controllable Load",
            "rrcCapFfr": "RRS Capability FFR Capable Ex ESR",
            "rrcCapFfrEsr": "RRS Capability FFR ESR",
            "regUpCap": "Reg Capability Reg Up",
            "regDownCap": "Reg Capability Reg Down",
            "regUpUndeployed": "Reg Capability Undeployed Reg Up",
            "regDownUndeployed": "Reg Capability Undeployed Reg Down",
            "regUpDeployed": "Reg Capability Deployed Reg Up",
            "regDownDeployed": "Reg Capability Deployed Reg Down",
            "rrAwdGen": "RRS Awards PFR Gen and ESR",
            "rrAwdNonClr": "RRS Awards UFR Load Ex Controllable Load",
            "rrAwdClr": "RRS Awards PFR Controllable Load",
            "rrAwdFfr": "RRS Awards FFR Capable",
            "regUpAwd": "Reg Awards Reg Up",
            "regDownAwd": "Reg Awards Reg Down",
            "ecrsCapGen": "ECRS Capability Gen",
            "ecrsCapNclr": "ECRS Capability Load Ex Controllable Load",
            "ecrsCapClr": "ECRS Capability Controllable Load",
            "ecrsCapQs": "ECRS Capability Quick Start Gen",
            "ecrsCapEsr": "ECRS Capability ESR",
            "ecrsCapDeployedGenLr": "ECRS Capability Manually Deployed ONSC Status",
            "capClrDecreaseBp": "Capacity From CLRS Available To Decrease Base Points In SCED",
            "capClrIncreaseBp": "Capacity From CLRS Available To Increase Base Points In SCED",
            "capWEoIncreaseBp": "Capacity With Energy Offer Curves To Increase Genres BP In SCED",
            "capWEoDecreaseBp": "Capacity With Energy Offer Curves To Decrease Genres BP In SCED",
            "capWoEoIncreaseBp": "Capacity Without Energy Offers To Increase Genres BP In SCED",
            "capWoEoDecreaseBp": "Capacity Without Energy Offers To Decrease Genres BP In SCED",
            "esrCapWEoIncreaseBp": "Capacity with energy offers to increase ESR BP in SCED",
            "esrCapWEoDecreaseBp": "Capacity with energy offers to decrease ESR BP in SCED",
            "esrCapWoEoIncreaseBp": "Capacity without energy offers to increase ESR BP in SCED",
            "esrCapWoEoDecreaseBp": "Capacity without energy offers to decrease ESR BP in SCED",
            "capIncreaseGenBp": "Capacity To Increase Genres BP In Next Five Minutes In SCED HDL",
            "capDecreaseGenBp": "Capacity To Decrease Genres BP In Next Five Minutes In SCED LDL",
            "sumCapResRegUpRrs": "Capacity to provide Reg Up RRS or Both",
            "sumCapResRegUpRrsEcrs": "Capacity to provide Reg Up RRS ECRS or any combo",
            "sumCapResRegUpRrsEcrsNsr": "Capacity to provide Reg Up RRS ECRS NSpin any combination",
            "ecrsAwdGen": "ECRS Awards Gen",
            "ecrsAwdNonClr": "ECRS Awards Load Ex Controllable Load",
            "ecrsAwdClr": "ECRS Awards Controllable Load",
            "ecrsAwdQs": "ECRS Awards Quick Start Gen",
            "ecrsAwdEsr": "ECRS Awards ESR",
            "prc": "PRC",
            "nsrCapOnGenWoEo": "Nspin Capability On Line Gen with Energy Offers",
            "nsrCapOffResWOs": "Nspin Capability Resources with Output Schedules",
            "nsrCapUndeployedLr": "Nspin Capability Undeployed Load",
            "nsrCapOffGen": "Nspin Capability Offline Gen Ex QSGR Online Gen with power aug",
            "nsrCapEsr": "Nspin Capability ESR",
            "rtReserveOnline": "ORDC Online",
            "rtReserveOnOffline": "ORDC Online and Offline",
            "nsrAwdGenWEo": "NSpin Awards On Line Gen with Energy Offer Curves",
            "nsrAwdGenWOs": "NSpin Awards On Line Gen with Output Schedules",
            "nsrAwdLr": "NSPin Awards Load",
            "nsrAwdOffGen": "NSpin Awards Offline Gen Ex QSGR Including power aug",
            "nsrAwdQs": "NSpin Awards Quick Start Gen",
            "nsrAwdAs": "NSpin Awards ESR",
            "telemHslEmr": "Telemetered HSL Capacity Resource Status EMR",
            "telemHslOut": "Telemetered HSL Capacity Resource Status OUT",
            "telemHslOutl": "Telemetered Net Consumption Resource status OUTL",
        }

        row_data = {}

        for group_name, group_data in json_data.get("data", {}).items():
            if not isinstance(group_data, list) or len(group_data) < 2:
                continue
            for item in group_data[1:]:
                if len(item) >= 2:
                    key, value = item[0], item[1]
                    if key in key_to_column:
                        row_data[key_to_column[key]] = value

        last_updated = json_data.get("lastUpdated")
        if last_updated:
            time = pd.to_datetime(last_updated).tz_convert(self.default_timezone)
        else:
            time = pd.Timestamp.now(tz=self.default_timezone)

        row_data["Time"] = time

        df = pd.DataFrame([row_data])
        df = utils.move_cols_to_front(df, ["Time"])

        return df

    def get_real_time_system_conditions(
        self,
        date: str = "latest",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Real-Time System Conditions.

        Parses table from
        https://www.ercot.com/content/cdr/html/real_time_system_conditions.html

        Arguments:
            date (str): only supports "latest"
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with real-time system conditions
        """

        url = "https://www.ercot.com/content/cdr/html/real_time_system_conditions.html"
        logger.info(f"Getting Real-Time System Conditions from {url}")
        html_content = requests.get(url).content
        df = self._parse_html_table(html_content)
        df = df.rename(
            columns={
                "Frequency - Current Frequency": "Current Frequency",
                "Real-Time Data - Actual System Demand": "Actual System Demand",
                "Real-Time Data - Average Net Load": "Average Net Load",
                "Real-Time Data - Total System Capacity (not including Ancillary Services)": "Total System Capacity excluding Ancillary Services",  # noqa: E501
                "Real-Time Data - Total Wind Output": "Total Wind Output",
                "Real-Time Data - Total PVGR Output": "Total PVGR Output",
                "Real-Time Data - Current System Inertia": "Current System Inertia",
            },
        )

        return df

    def _parse_html_table(self, html_content: bytes) -> pd.DataFrame:
        logger.info("Parsing HTML table")
        soup = BeautifulSoup(html_content, "html.parser")

        table = soup.find("table", attrs={"class": "tableStyle"})

        data = {}
        header = None
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if cells[0].get("class") == ["headerValueClass"]:
                header = cells[0].text.strip()  # new header for new dataframe
            else:
                category = cells[0].text.strip()
                value = cells[1].text.strip()
                header_prepend = header
                if " (MW)" in header:
                    header_prepend = header_prepend.replace(" (MW)", "")
                    category = f"{category} (MW)"

                parsed_value = value.replace(",", "")
                try:
                    parsed_value = int(parsed_value)
                except ValueError:
                    parsed_value = float(parsed_value)

                data[f"{header_prepend} - {category}"] = parsed_value

        df = pd.DataFrame([data])

        time_div = soup.find("div", attrs={"class": "schedTime rightAlign"})
        time_text = time_div.text.split(": ")[
            1
        ]  # Split the string on ': ' to get just the time part

        now = pd.Timestamp.now(tz=self.default_timezone)

        # Determine if during the repeated DST hour. Pandas wants ambiguous=True if the
        # time is DST during the repeated hour. US/Central is UTC-6 during standard time
        # and UTC-5 during DST. Outside the repeated hour, Pandas doesn't care about
        # ambiguous=True or ambiguous=False.
        ambiguous = (now.utcoffset().total_seconds() / 3600) == -5.0

        df.insert(
            0,
            "Time",
            pd.to_datetime(time_text).tz_localize(
                self.default_timezone,
                ambiguous=ambiguous,
            ),
        )

        return df

    @support_date_range(frequency=None)
    def get_wind_actual_and_forecast_hourly(
        self,
        date: str | datetime.date,
        end: str | datetime.date = None,
        verbose: bool = False,
    ):
        """Get Hourly Wind Report.

        This report is posted every hour and includes System-wide and Regional
        actual hourly averaged wind power production, STWPF, WGRPP and COP
        HSLs for On-Line WGRs for a rolling historical 48-hour period as
        well as the System-wide and Regional STWPF, WGRPP and
        COP HSLs for On-Line WGRs for the rolling future
        168-hour period. Our forecasts attempt to predict HSL,
        which is uncurtailed power generation potential.

        Arguments:
            date (str): date to get report for. Supports "latest"
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with hourly wind report data
        """
        df = self._get_hourly_report(
            start=date,
            end=end,
            report_type_id=WIND_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_RTID,  # noqa: E501
            extension="csv",
            handle_doc=self._handle_hourly_wind_or_solar_report,
            verbose=True,
        )

        return df[WIND_ACTUAL_AND_FORECAST_COLUMNS].sort_values(
            ["Interval Start", "Publish Time"],
        )

    @support_date_range(frequency=None)
    def get_wind_actual_and_forecast_by_geographical_region_hourly(
        self,
        date: str | datetime.date,
        end: str | datetime.date = None,
        verbose: bool = False,
    ):
        """Get Hourly Wind Report by geographical region

        Arguments:
            date (str): date to get report for. Supports "latest"
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with hourly wind report data
        """
        df = self._get_hourly_report(
            start=date,
            end=end,
            report_type_id=WIND_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_BY_GEOGRAPHICAL_REGION_RTID,  # noqa: E501
            extension="csv",
            handle_doc=self._handle_hourly_wind_or_solar_report,
            verbose=True,
        )

        return df[WIND_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS].sort_values(
            ["Interval Start", "Publish Time"],
        )

    @support_date_range(frequency=None)
    def get_solar_actual_and_forecast_hourly(
        self,
        date: str | datetime.date,
        end: str | datetime.date = None,
        verbose: bool = False,
    ):
        """Get Hourly Solar Report.

        Arguments:
            date (str): date to get report for. Supports "latest" or a date string
            end (str, optional): end date for date range. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with hourly solar report data
        """
        df = self._get_hourly_report(
            start=date,
            end=end,
            report_type_id=SOLAR_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_RTID,  # noqa: E501
            extension="csv",
            handle_doc=self._handle_hourly_wind_or_solar_report,
            verbose=True,
        )

        return df[SOLAR_ACTUAL_AND_FORECAST_COLUMNS].sort_values(
            ["Interval Start", "Publish Time"],
        )

    @support_date_range(frequency=None)
    def get_solar_actual_and_forecast_by_geographical_region_hourly(
        self,
        date: str | datetime.date,
        end: str | datetime.date = None,
        verbose: bool = False,
    ):
        """Get Hourly Solar Report by geographical region

        Posted every hour and includes System-wide and geographic regional
        hourly averaged solar power production, STPPF, PVGRPP, and COP HSL
        for On-Line PVGRs for a rolling historical 48-hour period as well
        as the system-wide and regional STPPF, PVGRPP, and COP HSL for
        On-Line PVGRs for the rolling future 168-hour period.

        Arguments:
            date (str): date to get report for. Supports "latest" or a date string
            end (str, optional): end date for date range. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with hourly solar report data
        """
        df = self._get_hourly_report(
            start=date,
            end=end,
            report_type_id=SOLAR_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_BY_GEOGRAPHICAL_REGION_RTID,  # noqa: E501
            extension="csv",
            handle_doc=self._handle_hourly_wind_or_solar_report,
            verbose=True,
        )

        return df[SOLAR_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS].sort_values(
            ["Interval Start", "Publish Time"],
        )

    def _handle_hourly_wind_or_solar_report(
        self,
        doc: Document,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self.read_doc(doc, verbose=verbose)
        df.insert(
            0,
            "Publish Time",
            pd.to_datetime(doc.publish_date).tz_convert(self.default_timezone),
        )
        # replace _ in column names with spaces
        df.columns = df.columns.str.replace("_", " ")

        return self._rename_hourly_wind_or_solar_report(df)

    def _rename_hourly_wind_or_solar_report(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                # on Sept 26, 2024 ercot added this column
                "SYSTEM WIDE HSL": "HSL SYSTEM WIDE",
                # on Sept 26, 2024 ercot renamed these columns
                # let's rename new to the old
                # since it's more consistent with the rest of the data
                "SYSTEM WIDE GEN": "GEN SYSTEM WIDE",
                # on Sept 26, 2024 ercot renamed these columns in wind report
                # let's rename old names to new names
                # since it's more consistent with solar report
                "ACTUAL SYSTEM WIDE": "GEN SYSTEM WIDE",
                "ACTUAL LZ SOUTH HOUSTON": "GEN LZ SOUTH HOUSTON",
                "ACTUAL LZ WEST": "GEN LZ WEST",
                "ACTUAL LZ NORTH": "GEN LZ NORTH",
                # Older versions of wind power production by geographical region use
                # "ACTUAL" instead of "GEN"
                # https://data.ercot.com/data-product-archive/NP4-742-CD
                "ACTUAL PANHANDLE": "GEN PANHANDLE",
                "ACTUAL COASTAL": "GEN COASTAL",
                "ACTUAL SOUTH": "GEN SOUTH",
                "ACTUAL WEST": "GEN WEST",
                "ACTUAL NORTH": "GEN NORTH",
            },
        )

        # Add HSL SYSTEM WIDE if it is not in the data (older data may not have it)
        if "HSL SYSTEM WIDE" not in df:
            df["HSL SYSTEM WIDE"] = pd.NA

        return df

    def get_reported_outages(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieves the 5-minute data behind this dashboard:
        https://www.ercot.com/gridmktinfo/dashboards/generationoutages

        Data available at
        https://www.ercot.com/api/1/services/read/dashboards/generation-outages.json

        This data is ephemeral in that there is only one file available that is
        constantly updated. There is no historical data.
        """

        logger.info("Downloading ERCOT reported outages data")

        json = requests.get(
            "https://www.ercot.com/api/1/services/read/dashboards/generation-outages.json",  # noqa: E501
        ).json()

        current = json["current"]
        previous = json["previous"]

        def flatten_dict(data: dict, prefix: str = "") -> dict:
            """
            Recursive function to flatten nested dictionaries with prefix handling.
            Returns a new dictionary with the flattened data.
            """
            flat_data = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    flat_data.update(flatten_dict(value, f"{prefix}{key} "))
                else:
                    flat_data[(prefix + key).title()] = value
            return flat_data

        # Flatten each dictionary in the list
        previous_data = [flatten_dict(data) for data in previous.values()]
        current_data = [flatten_dict(data) for data in current.values()]

        df = pd.DataFrame.from_dict(current_data + previous_data)

        # need to use apply since there can be mixed
        # fixed offsets during dst transition
        # that result in object dtypes in pandas
        df["Time"] = df["Deliverytime"].apply(
            lambda x: pd.to_datetime(x).tz_convert("UTC"),
        )
        df["Time"] = df["Time"].dt.tz_convert(self.default_timezone)

        df = utils.move_cols_to_front(df, ["Time"]).drop(
            columns=["Deliverytime", "Dstflag"],
        )

        return df.sort_values("Time").reset_index(drop=True)

    @support_date_range(frequency=None)
    def get_hourly_resource_outage_capacity(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Hourly Resource Outage Capacity report sourced
        from the Outage Scheduler (OS).

        Returns outage data for for next 7 days.

        Total Resource MW doesn't include IRR, New Equipment outages,
        retirement of old equipment, seasonal
        mothballed (during the outage season),
        and mothballed.

        As such, it is a proxy for thermal outages.

        Arguments:
            date (str, pd.Timestamp): time to download. Returns last hourly report
                before this time. Supports "latest"
            end (str, pd.Timestamp, optional): end time to download. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with hourly resource outage capacity data
        """

        df = self._get_hourly_report(
            start=date,
            end=end,
            report_type_id=HOURLY_RESOURCE_OUTAGE_CAPACITY_RTID,
            extension="csv",
            handle_doc=self._handle_hourly_resource_outage_capacity,
        )

        return df

    def _handle_hourly_resource_outage_capacity(
        self,
        doc: Document,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self.read_doc(doc, parse=False, verbose=verbose)
        # there is no DST flag column
        # and the data set ignores DST
        # so, we will default to assuming it is DST. We will also
        # set nonexistent times to NaT and drop them
        df = self.parse_doc(
            df,
            dst_ambiguous_default=True,
            nonexistent="NaT",
            verbose=verbose,
        )

        df = df.dropna(subset=["Interval Start"])

        df.insert(
            0,
            "Publish Time",
            pd.to_datetime(doc.publish_date).tz_convert(self.default_timezone),
        )

        return self._handle_hourly_resource_outage_capacity_df(df)

    def _handle_hourly_resource_outage_capacity_df(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        outage_types = ["Total Resource", "Total IRR", "Total New Equip Resource"]

        # Earlier data doesn't have these columns
        if all(
            col in df.columns
            for col in [
                "TotalResourceMWZoneSouth",
                "TotalResourceMWZoneNorth",
                "TotalResourceMWZoneWest",
                "TotalResourceMWZoneHouston",
            ]
        ):
            for t in outage_types:
                t_no_space = t.replace(" ", "")
                df = df.rename(
                    columns={
                        f"{t_no_space}MWZoneSouth": f"{t} MW Zone South",
                        f"{t_no_space}MWZoneNorth": f"{t} MW Zone North",
                        f"{t_no_space}MWZoneWest": f"{t} MW Zone West",
                        f"{t_no_space}MWZoneHouston": f"{t} MW Zone Houston",
                    },
                )

                df.insert(
                    df.columns.tolist().index(f"{t} MW Zone Houston") + 1,
                    f"{t} MW",
                    (
                        df[f"{t} MW Zone South"]
                        + df[f"{t} MW Zone North"]
                        + df[f"{t} MW Zone West"]
                        + df[f"{t} MW Zone Houston"]
                    ),
                )
        else:
            df = df.rename(
                columns={
                    "TotalResourceMW": "Total Resource MW",
                    "TotalIRRMW": "Total IRR MW",
                    "TotalNewEquipResourceMW": "Total New Equip Resource MW",
                },
            )

        return df

    @support_date_range(frequency=None)
    def get_unplanned_resource_outages(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Unplanned Resource Outages.

        Data published at ~5am central on the 3rd day after the day of interest. Since
        the date argument is the publish date, if you want to get data for a specific
        date, pass in the date of interest - 3 days.

        Arguments:
            date (str, datetime): publish date of the report
            end (str, datetime, optional): end date to download. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with unplanned resource outages

        """
        docs = self._get_documents(
            report_type_id=UNPLANNED_RESOURCE_OUTAGES_REPORT_RTID,
            published_after=date,
            # If no end provided, use the date plus 1 day
            published_before=end or date + pd.DateOffset(days=1),
            verbose=verbose,
        )

        dfs = []

        for doc in docs:
            xls = utils.get_zip_file(doc.url, verbose=verbose)
            df = self._handle_unplanned_resource_outages_file(doc, xls)
            dfs.append(df)

        complete_df = pd.concat(dfs, ignore_index=True)

        return complete_df

    def _handle_unplanned_resource_outages_file(
        self,
        doc: Document,
        xls: ZipFile,
    ) -> pd.DataFrame:
        as_of = pd.to_datetime(
            pd.read_excel(
                xls,
                sheet_name="Unplanned Resource Outages",
                skiprows=2,
                nrows=1,
            )
            .values[0][0]
            .split(": ")[1],
        ).tz_localize(self.default_timezone)
        df = pd.read_excel(
            xls,
            sheet_name="Unplanned Resource Outages",
            skiprows=4,
            skipfooter=1,
        )

        # Already has local timezone
        df["Publish Time"] = pd.to_datetime(doc.publish_date)

        df.insert(0, "Current As Of", as_of)

        time_cols = ["Actual Outage Start", "Planned End Date", "Actual End Date"]
        for col in time_cols:
            # data doesn't have DST info. So just assume it is DST
            # when ambiguous \_(-_-)_/
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(
                self.default_timezone,
                ambiguous=True,
            )

        df = utils.move_cols_to_front(
            df,
            [
                "Current As Of",
                "Publish Time",
                "Actual Outage Start",
                "Planned End Date",
                "Actual End Date",
                "Resource Name",
                "Resource Unit Code",
                "Fuel Type",
                "Outage Type",
                "Nature Of Work",
                "Available MW Maximum",
                "Available MW During Outage",
                "Effective MW Reduction Due to Outage",
            ],
        )

        return df

    @support_date_range("DAY_START")
    def get_as_reports(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Ancillary Services Reports.

        Published with a 2 day delay around 3am central
        """
        # This method is not supported starting with the file published on 2025-12-08
        # (with data for 2025-12-06)
        if date >= pd.Timestamp("2025-12-06", tz=self.default_timezone):
            raise ValueError(
                "This method is not supported starting with the file published on 2025-12-08 (with data for 2025-12-06) because the data significantly changed with the launch of ERCOT RTC+B. Please use get_reports_as_dam on or after this date.",
            )

        report_date = date.normalize() + pd.DateOffset(days=2)

        doc = self._get_document(
            report_type_id=TWO_DAY_ANCILLARY_SERVICES_REPORTS_RTID,
            date=report_date,
            verbose=verbose,
        )

        return self._handle_as_reports_file(doc.url, verbose=verbose)

    def _handle_as_reports_file(
        self,
        file_path: str,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        z = utils.get_zip_folder(file_path, verbose=verbose, **kwargs)

        # extract the date from the file name
        date_str = z.namelist()[0][-13:-4]

        # Legacy method uses slightly different product lists
        self_arranged_products = AS_PRODUCTS
        cleared_products = [p for p in AS_PRODUCTS if p != "NSPNM"]
        offers_products = [
            "RRSPFR",
            "RRSUFR",
            "RRSFFR",
            "ECRSM",
            "ECRSS",
            "REGUP",
            "REGDN",
            "ONNS",
            "OFFNS",
        ]

        prefix = self._get_as_report_prefix(z)

        all_dfs = []
        for as_name in cleared_products:
            suffix = f"{as_name}-{date_str}.csv"
            cleared = f"{prefix}_Cleared_DAM_AS_{suffix}"

            if as_name in AS_EXCLUDE_PRODUCTS and cleared not in z.namelist():
                continue

            df_cleared = pd.read_csv(z.open(cleared))
            all_dfs.append(df_cleared)

        for as_name in self_arranged_products:
            suffix = f"{as_name}-{date_str}.csv"
            self_arranged = f"{prefix}_Self_Arranged_AS_{suffix}"

            if as_name in AS_EXCLUDE_PRODUCTS and self_arranged not in z.namelist():
                continue

            df_self_arranged = pd.read_csv(z.open(self_arranged))
            all_dfs.append(df_self_arranged)

        def _make_bid_curve(df: pd.DataFrame) -> list[list[float]]:
            return [
                list(x)
                for x in df[["MW Offered", f"{as_name} Offer Price"]].values.tolist()
            ]

        for as_name in offers_products:
            suffix = f"{as_name}-{date_str}.csv"
            # Starting 2025-12-08, files have DAM in the name
            offers = f"{prefix}_Agg_AS_Offers_{suffix}"

            if as_name in AS_EXCLUDE_PRODUCTS and offers not in z.namelist():
                continue

            df_offers = pd.read_csv(z.open(offers))
            name = f"Bid Curve - {as_name}"
            if df_offers.empty:
                # use last df to get the index
                # and set values to None
                df_offers_hourly = all_dfs[0].rename(
                    columns={
                        all_dfs[0].columns[-1]: name,
                    },
                )
                df_offers_hourly[name] = None

            else:
                df_offers_hourly = (
                    df_offers.groupby(["Delivery Date", "Hour Ending"])
                    .apply(_make_bid_curve, include_groups=False)
                    .reset_index(name=name)
                )
            all_dfs.append(df_offers_hourly)

        df = pd.concat(
            [df.set_index(["Delivery Date", "Hour Ending"]) for df in all_dfs],
            axis=1,
        ).reset_index()

        return self.parse_doc(df, verbose=verbose)

    def _get_as_report_document(
        self,
        date: str | pd.Timestamp,
        report_type_id: str,
        verbose: bool = False,
    ) -> Document:
        """Get the AS report document for a given date.

        Handles "latest" date and applies the 2-day delay offset.

        Arguments:
            date: date to fetch reports for (or "latest")
            report_type_id: RTID of the report type to fetch
            verbose: print verbose output

        Returns:
            Document: The document for the requested date
        """
        if date == "latest":
            date = self.local_now().normalize() - pd.DateOffset(days=2)

        report_date = date.normalize() + pd.DateOffset(days=2)

        return self._get_document(
            report_type_id=report_type_id,
            date=report_date,
            verbose=verbose,
        )

    def _get_as_report_prefix(self, z: ZipFile) -> str:
        """Determine the file prefix for AS report files.

        Earlier files use '48h' prefix, newer files use '2d'.

        Arguments:
            z: ZipFile containing the AS report files

        Returns:
            str: The prefix ('2d' or '48h')
        """
        if z.namelist()[0].split("_")[0] == "48h":
            return "48h"
        return "2d"

    @staticmethod
    def _make_offer_curve(
        group_df: pd.DataFrame,
        mw_col: str = "MW Offered",
        price_col: str = "Offer Price",
    ) -> list[list[float]]:
        """Create an offer curve as a list of [MW, Price] pairs.

        Arguments:
            group_df: DataFrame containing MW and price columns
            mw_col: Name of the MW column
            price_col: Name of the price column

        Returns:
            list[list[float]]: List of [MW, Price] pairs
        """
        return [[mw, price] for mw, price in zip(group_df[mw_col], group_df[price_col])]

    @support_date_range("DAY_START")
    def get_as_reports_dam(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Day-Ahead Market Ancillary Services Reports.

        Published with a 2 day delay around 3am central.

        Contains cleared, self-arranged, and bid curve data for each AS product.

        Arguments:
            date: date to fetch reports for
            verbose: print verbose output

        Returns:
            pandas.DataFrame: A DataFrame with DAM ancillary services reports
        """
        doc = self._get_as_report_document(
            date=date,
            report_type_id=TWO_DAY_ANCILLARY_SERVICES_REPORTS_RTID,
            verbose=verbose,
        )

        return self._handle_as_reports_dam_file(doc.url, verbose=verbose)

    def _handle_as_reports_dam_file(
        self,
        file_path: str,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """Parse DAM AS reports into long format with columns:
        interval_start, interval_end, as_type, cleared, self_arranged, offer_curve
        """
        z = utils.get_zip_folder(file_path, verbose=verbose, **kwargs)

        # extract the date from the file name
        date_str = z.namelist()[0][-13:-4]

        prefix = self._get_as_report_prefix(z)

        # Process each AS product
        product_dfs = []

        for as_name in AS_PRODUCTS:
            suffix = f"{as_name}-{date_str}.csv"
            cleared_file = f"{prefix}_Cleared_DAM_AS_{suffix}"
            self_arranged_file = f"{prefix}_Self_Arranged_AS_{suffix}"
            # Files before 2025-12-08 (before RTC+B) do not have DAM in the name.
            offers_file = (
                f"{prefix}_Agg_DAM_AS_Offers_{suffix}"
                if pd.Timestamp(date_str, tz=self.default_timezone)
                >= pd.Timestamp("2025-12-08", tz=self.default_timezone)
                else f"{prefix}_Agg_AS_Offers_{suffix}"
            )

            # Skip if product not in this file
            if as_name in AS_EXCLUDE_PRODUCTS and cleared_file not in z.namelist():
                continue

            # Read cleared data
            df_cleared = None
            if cleared_file in z.namelist():
                df_cleared = pd.read_csv(z.open(cleared_file))
                df_cleared = df_cleared.rename(
                    columns={df_cleared.columns[-1]: "Cleared"},
                )

            # Read self-arranged data
            df_self = None
            if self_arranged_file in z.namelist():
                df_self = pd.read_csv(z.open(self_arranged_file))
                df_self = df_self.rename(
                    columns={df_self.columns[-1]: "Self Arranged"},
                )

            # Read offers data and create bid curves
            df_offers = None
            if offers_file in z.namelist():
                df_offers_raw = pd.read_csv(z.open(offers_file))
                if not df_offers_raw.empty:
                    # Create offer curve as list of [MW, Price] pairs
                    def _make_offer_curve(group_df: pd.DataFrame) -> list[list[float]]:
                        return [
                            [mw, price]
                            for mw, price in zip(
                                group_df["MW Offered"],
                                group_df[f"{as_name} Offer Price"],
                            )
                        ]

                    df_offers = (
                        df_offers_raw.groupby(["Delivery Date", "Hour Ending"])
                        .apply(_make_offer_curve, include_groups=False)
                        .reset_index(name="Offer Curve")
                    )

            # Merge cleared, self-arranged, and offers data
            df_product = None
            if df_cleared is not None:
                df_product = df_cleared.copy()
            if df_self is not None:
                if df_product is None:
                    df_product = df_self.copy()
                else:
                    df_product = df_product.merge(
                        df_self[["Delivery Date", "Hour Ending", "Self Arranged"]],
                        on=["Delivery Date", "Hour Ending"],
                        how="outer",
                    )
            if df_offers is not None:
                if df_product is None:
                    df_product = df_offers.copy()
                else:
                    df_product = df_product.merge(
                        df_offers,
                        on=["Delivery Date", "Hour Ending"],
                        how="outer",
                    )

            if df_product is not None:
                # Add AS Type column
                df_product["AS Type"] = as_name
                product_dfs.append(df_product)

        if not product_dfs:
            raise NoDataFoundException("No DAM AS reports found in zip file")

        # Combine all products into long format
        df = pd.concat(product_dfs, ignore_index=True)

        # Select and order columns
        df = df[
            [
                "Delivery Date",
                "Hour Ending",
                "AS Type",
                "Cleared",
                "Self Arranged",
                "Offer Curve",
            ]
        ]

        return self.parse_doc(df, verbose=verbose)

    @support_date_range("DAY_START")
    def get_as_reports_sced(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get 2-Day SCED Ancillary Service Disclosure Reports.

        Published with a 2 day delay around 3am central.

        Contains offer curves (MW offered and price) for each AS product
        at each SCED timestamp.

        Output columns: SCED Timestamp, AS Type, Offer Curve

        Arguments:
            date: date to fetch reports for
            verbose: print verbose output

        Returns:
            pandas.DataFrame: A DataFrame with SCED ancillary services offers
        """
        doc = self._get_as_report_document(
            date=date,
            report_type_id=TWO_DAY_SCED_ANCILLARY_SERVICES_REPORTS_RTID,
            verbose=verbose,
        )

        return self._handle_as_reports_sced_file(doc.url, verbose=verbose)

    def _handle_as_reports_sced_file(
        self,
        file_path: str,
        verbose: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """Parse SCED AS reports with columns:
        sced_timestamp, as_type, offer_curve (list of [MW, Price] pairs)

        Based on primary key: sced_timestamp, as_type, mw_offered
        But output format uses offer_curve column containing list of pairs
        """
        z = utils.get_zip_folder(file_path, verbose=verbose, **kwargs)

        all_dfs = []

        for file_name in z.namelist():
            # Skip non-CSV files
            if not file_name.endswith(".csv"):
                continue

            # Determine AS type from file name
            as_type = None
            for product in AS_PRODUCTS:
                if product in file_name.upper():
                    as_type = product
                    break

            if as_type is None:
                continue

            df = pd.read_csv(z.open(file_name))

            if df.empty:
                continue

            # Add AS Type column
            df["AS Type"] = as_type

            # Find the price column - it should contain "Offer Price"
            price_col = None
            for col in df.columns:
                if "Offer Price" in col:
                    price_col = col
                    break

            if price_col is None:
                continue

            # Rename columns to standardized names
            df = df.rename(columns={price_col: "Offer Price"})

            # Create offer curve as list of [MW, Price] pairs for each SCED timestamp
            def _make_offer_curve(group_df: pd.DataFrame) -> list[list[float]]:
                return [
                    [mw, price]
                    for mw, price in zip(
                        group_df["MW Offered"],
                        group_df["Offer Price"],
                    )
                ]

            df_grouped = (
                df.groupby(["SCED Timestamp", "AS Type"])
                .apply(_make_offer_curve, include_groups=False)
                .reset_index(name="Offer Curve")
            )

            all_dfs.append(df_grouped)

        if not all_dfs:
            raise NoDataFoundException("No SCED AS reports found in zip file")

        df = pd.concat(all_dfs, ignore_index=True)

        # Parse SCED Timestamp directly (it's already a timestamp, not date + hour)
        df["SCED Timestamp"] = pd.to_datetime(df["SCED Timestamp"])

        # Convert to local timezone
        if df["SCED Timestamp"].dt.tz is None:
            df["SCED Timestamp"] = df["SCED Timestamp"].dt.tz_localize(
                self.default_timezone,
            )
        else:
            df["SCED Timestamp"] = df["SCED Timestamp"].dt.tz_convert(
                self.default_timezone,
            )

        df = df[["SCED Timestamp", "AS Type", "Offer Curve"]]
        df = df.sort_values("SCED Timestamp").reset_index(drop=True)

        return df

    @support_date_range("DAY_START")
    def get_dam_system_lambda(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Day-Ahead Market System Lambda

        File is typically published around 12:30 pm for the day ahead

        https://www.ercot.com/mp/data-products/data-product-details?id=NP4-523-CD

        Arguments:
            date (str, datetime): date to get data for
            end (str, datetime, optional): end time to get data for. If None,
                return 1 day of data. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with day-ahead market system lambda data
        """
        # Subtract one day since this is the day ahead market
        date = date if date == "latest" else date - pd.DateOffset(days=1)

        doc = self._get_document(
            report_type_id=DAM_SYSTEM_LAMBDA_RTID,
            date=date,
            verbose=verbose,
        )

        return self._handle_dam_system_lambda_file(doc, verbose=verbose)

    def _handle_dam_system_lambda_file(
        self,
        doc: Document,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self.read_doc(doc, parse=True, verbose=verbose)

        # Set the publish time from the document metadata
        df["Publish Time"] = pd.to_datetime(doc.publish_date)
        df["Market"] = "DAM"

        df = utils.move_cols_to_front(
            df.drop(columns=["Time"]).rename(columns={"SystemLambda": "System Lambda"}),
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Market",
                "System Lambda",
            ],
        )

        return df

    @support_date_range(frequency=None)
    def get_sced_system_lambda(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get System lambda of each successful SCED

        Normally published every 5 minutes

        Arguments:
            date (str, datetime, pd.Timestamp): date or start time to get data for
            end (str, datetime, optional): end time to get data for. If None,
                return 1 day of data. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame

        """

        # no end, so assume requesting one day
        # use the timestamp from the friendly name
        if date == "latest":
            date = date
            friendly_name_timestamp_after = None
            friendly_name_timestamp_before = None
        elif end is None:
            friendly_name_timestamp_after = date.normalize()
            friendly_name_timestamp_before = (
                friendly_name_timestamp_after + pd.DateOffset(days=1)
            )
            date = None
        else:
            friendly_name_timestamp_after = date
            friendly_name_timestamp_before = end
            date = None

        docs = self._get_documents(
            report_type_id=SCED_SYSTEM_LAMBDA_RTID,
            date=date,
            friendly_name_timestamp_after=friendly_name_timestamp_after,
            friendly_name_timestamp_before=friendly_name_timestamp_before,
            verbose=verbose,
            constructed_name_contains="csv.zip",
        )

        df = self._handle_sced_system_lambda(docs, verbose=verbose)

        return df

    def _handle_sced_timestamp(
        self,
        df: pd.DataFrame,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = df.rename(
            columns={
                "RepeatHourFlag": "RepeatedHourFlag",
                "SCEDTimeStamp": "SCED Timestamp",
                "SCEDTimestamp": "SCED Timestamp",
            },
        )

        # Some files have errors with the timestamp
        df["SCED Timestamp"] = pd.to_datetime(
            df["SCED Timestamp"],
            errors="coerce",
        ).dt.tz_localize(
            self.default_timezone,
            ambiguous=df["RepeatedHourFlag"] == "N",
        )

        df = df.dropna(subset=["SCED Timestamp"])

        # SCED runs at least every 5 minutes. These values are only approximations,
        # not exact.
        # Round to nearest 5 minutes
        df["Interval Start"] = df["SCED Timestamp"].dt.floor(
            "5min",
            ambiguous=df["RepeatedHourFlag"] == "N",
        )

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=5)

        df = df.drop("RepeatedHourFlag", axis=1)

        return df

    def _handle_sced_system_lambda(
        self,
        docs: list[Document],
        verbose: bool = False,
    ) -> pd.DataFrame:
        all_dfs = []
        for doc in tqdm.tqdm(
            docs,
            desc="Reading SCED System Lambda files",
            disable=not verbose,
        ):
            logger.info(f"Reading {doc.url}")
            df = pd.read_csv(doc.url, compression="zip")
            all_dfs.append(df)

        if len(all_dfs) == 0:
            df = pd.DataFrame(
                columns=["SCEDTimeStamp", "RepeatedHourFlag", "SystemLambda"],
            )
        else:
            df = pd.concat(all_dfs)

        df = self._handle_sced_timestamp(df, verbose=verbose)

        df["SystemLambda"] = df["SystemLambda"].astype("float64")

        df = df.rename(
            columns={
                "SystemLambda": "System Lambda",
            },
        )

        df.sort_values("SCED Timestamp", inplace=True)
        return df[["Interval Start", "Interval End", "SCED Timestamp", "System Lambda"]]

    @support_date_range("DAY_START")
    def get_highest_price_as_offer_selected(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get the offer price and the name of the Entity submitting
        the offer for the highest-priced Ancillary Service (AS) Offer.

        Published with 3 day delay

        Arguments:
            date (str, datetime): date to get data for
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrameq
        """
        report_date = date.normalize() + pd.DateOffset(days=3)

        doc = self._get_document(
            report_type_id=THREE_DAY_HIGHEST_PRICE_AS_OFFER_SELECTED_RTID,
            date=report_date,
            verbose=verbose,
        )

        df = self._handle_three_day_highest_price_as_offer_selected_file(doc, verbose)

        return df

    def _handle_three_day_highest_price_as_offer_selected_file(
        self,
        doc: Document,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self.read_doc(doc, verbose=verbose, parse=False)
        is_dst_end = 25 in df["Hour Ending"]

        df = df.rename(
            columns={
                "Resource Name with Highest-Priced Offer Selected in DAM and SASMs": "Resource Name",  # noqa: E501
                # Older data
                "Resource Name with Highest-Priced Offer Selected in DAM": "Resource Name",  # noqa: E501
            },
        )

        if not is_dst_end:
            df = self.parse_doc(df)
        else:
            # Hours go up to 25. Assume hour 2 is CDT and hour 3 is CST
            df["Interval Start"] = (
                pd.to_datetime(df["Delivery Date"])
                + pd.to_timedelta(df["Hour Ending"] - 1, unit="h")
            ).dt.tz_localize(self.default_timezone, ambiguous=df["Hour Ending"] == 2)

            df.loc[df["Hour Ending"] >= 3, "Interval Start"] = df.loc[
                df["Hour Ending"] >= 3,
                "Interval Start",
            ] - pd.Timedelta(hours=1)

            df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)
            # Needed for the groupby
            df["Time"] = df["Interval Start"]

        def _handle_offers(df: pd.DataFrame) -> pd.Series:
            return pd.Series(
                {
                    "Offered Price": df["Offered Price"].iloc[0],
                    "Total Offered Quantity": df["Offered Quantity"].sum(),
                    "Offered Quantities": df["Offered Quantity"].tolist(),
                },
            )

        df = (
            df.groupby(
                [
                    "Time",
                    "Interval Start",
                    "Interval End",
                    "Market",
                    "QSE",
                    "DME",
                    "Resource Name",
                    "AS Type",
                    "Block Indicator",
                ],
                dropna=False,  # Have to include missing because older data has missing
                # values in some columns
            )
            .apply(_handle_offers, include_groups=False)
            .reset_index()
        )

        return df

    def get_dam_price_corrections(
        self,
        dam_type: str,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get RTM Price Corrections

        Arguments:
            rtm_type (str): 'DAM_SPP', 'DAM_MCPC', 'DAM_EBLMP'

        """
        docs = self._get_documents(
            report_type_id=DAM_PRICE_CORRECTIONS_RTID,
            constructed_name_contains=dam_type,
            extension="csv",
            verbose=verbose,
        )

        df = self._handle_price_corrections(docs, verbose=verbose)

        return df

    def get_rtm_price_corrections(
        self,
        rtm_type: str,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Get RTM Price Corrections

        Arguments:
            rtm_type (str): 'RTM_SPP', 'RTM_SPLMP', 'RTM_EBLMP',
                'RTM_ShadowPrice', 'RTM_SOGLMP', 'RTM_SOGPRICE'

        """
        docs = self._get_documents(
            report_type_id=RTM_PRICE_CORRECTIONS_RTID,
            constructed_name_contains=rtm_type,
            extension="csv",
            verbose=verbose,
        )

        df = self._handle_price_corrections(docs, verbose=verbose)

        return df

    def _handle_price_corrections(
        self,
        docs: list[Document],
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self.read_docs(docs, verbose=verbose)

        df = self._handle_settlement_point_name_and_type(df)

        df = df.rename(
            columns={
                "SettlementPointName": "Settlement Point Name",
                "SettlementPoint": "Settlement Point Name",
                "SettlementPointType": "Settlement Point Type",
                "SPPOriginal": "SPP Original",
                "SPPCorrected": "SPP Corrected",
                "PriceCorrectionTime": "Price Correction Time",
            },
        )

        df["Price Correction Time"] = pd.to_datetime(
            df["Price Correction Time"],
        ).dt.tz_localize(self.default_timezone)

        df = df[
            [
                "Price Correction Time",
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "SPP Original",
                "SPP Corrected",
            ]
        ]

        return df

    @support_date_range(frequency=None)
    def get_system_wide_actual_load(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get 15-minute system-wide actual load.

        This report is posted every hour five minutes after the hour.

        Args:
            date (str, datetime): date to get data for
            end (str, datetime, optional): end time to get data for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with system actuals data
        """
        report_type_id = SYSTEM_WIDE_ACTUALS_RTID

        if date == "latest":
            # Go back one hour to ensure we have data
            date = pd.Timestamp.now(tz=self.default_timezone).floor("h") - pd.Timedelta(
                hours=1,
            )

        if end is None:
            doc = self._get_document(
                report_type_id=report_type_id,
                published_after=date + pd.Timedelta(hours=1),
                published_before=date + pd.Timedelta(hours=2),
                extension="csv",
                verbose=verbose,
            )
            docs = [doc]
        else:
            docs = self._get_documents(
                report_type_id=report_type_id,
                published_after=date + pd.Timedelta(hours=1),
                published_before=end + pd.Timedelta(hours=1),
                extension="csv",
                verbose=verbose,
            )

        all_df = [
            self._handle_system_wide_actual_load(doc, verbose=verbose) for doc in docs
        ]

        return pd.concat(all_df).sort_values("Interval Start")

    def _handle_system_wide_actual_load(self, doc, verbose=False):
        return self.read_doc(doc, verbose=verbose)

    @support_date_range("HOUR_START")
    def get_short_term_system_adequacy(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Short Term System Adequacy published between date and end.

        Arguments:
            date (str, datetime): date to get data for
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with system adequacy data
        """
        return self._get_hourly_report(
            start=date,
            end=end,
            report_type_id=SHORT_TERM_SYSTEM_ADEQUACY_REPORT_RTID,
            handle_doc=self._handle_short_term_system_adequacy_file,
            verbose=verbose,
            extension="csv",
        ).sort_values(["Interval Start", "Publish Time"])

    def _handle_short_term_system_adequacy_file(
        self,
        doc: Document,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self.read_doc(doc, verbose=verbose)

        df["Publish Time"] = doc.publish_date

        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End", "Publish Time"],
        ).drop(columns=["Time"])

        df = df.rename(
            columns={
                "CapGenResSouth": "Capacity Generation Resource South",
                "CapGenResNorth": "Capacity Generation Resource North",
                "CapGenResWest": "Capacity Generation Resource West",
                "CapGenResHouston": "Capacity Generation Resource Houston",
                "CapLoadResSouth": "Capacity Load Resource South",
                "CapLoadResNorth": "Capacity Load Resource North",
                "CapLoadResWest": "Capacity Load Resource West",
                "CapLoadResHouston": "Capacity Load Resource Houston",
                "OfflineAvailableMWSouth": "Offline Available MW South",
                "OfflineAvailableMWNorth": "Offline Available MW North",
                "OfflineAvailableMWWest": "Offline Available MW West",
                "OfflineAvailableMWHouston": "Offline Available MW Houston",
                "AvailCapGen": "Available Capacity Generation",
                "AvailCapReserve": "Available Capacity Reserve",
                "CapGenResTotal": "Capacity Generation Resource Total",
                "CapLoadResTotal": "Capacity Load Resource Total",
                "OfflineAvailableMWTotal": "Offline Available MW Total",
                "CapREGUPTotal": "Capacity Reg Up Total",
                "CapREGDNTotal": "Capacity Reg Down Total",
                "CapRRSTotal": "Capacity RRS Total",
                "CapECRSTotal": "Capacity ECRS Total",
                "CapNSPINTotal": "Capacity NSPIN Total",
                "CapREGUP_RRSTotal": "Capacity Reg Up RRS Total",
                "CapREGUP_RRS_ECRSTotal": "Capacity Reg Up RRS ECRS Total",
                "CapREGUP_RRS_ECRS_NSPINTotal": "Capacity Reg Up RRS ECRS NSPIN Total",
            },
        )

        return df

    @support_date_range(frequency=None)
    def get_real_time_adders_and_reserves(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Real-Time ORDC and Reliability Deployment Price Adders and
            Reserves by SCED Interval

        At: https://www.ercot.com/mp/data-products/data-product-details?id=NP6-323-CD

        Arguments:
            date (str, datetime): date to get data for
            end (str, datetime): end date to get data for
            verbose (bool, optional): print verbose output. Defaults to False.
        Returns:
            pandas.DataFrame: A DataFrame with ORDC data

        NOTE: data only goes back 5 days
        """
        if date == "latest":
            docs = [
                self._get_document(
                    report_type_id=REAL_TIME_ADDERS_AND_RESERVES_RTID,
                    published_before=date,
                    verbose=verbose,
                ),
            ]
        else:
            # Set date to get a full day of published data
            if not end:
                end = date + pd.DateOffset(days=1)

            docs = self._get_documents(
                report_type_id=REAL_TIME_ADDERS_AND_RESERVES_RTID,
                published_after=date,
                published_before=end,
                extension="csv",
                verbose=verbose,
            )

        return self._handle_real_time_adders_and_reserves_docs(docs, verbose=verbose)

    def _handle_real_time_adders_and_reserves_docs(
        self,
        docs: list[Document],
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self.read_docs(docs, parse=False, verbose=verbose)
        df = self._handle_sced_timestamp(df)

        df = utils.move_cols_to_front(
            df,
            ["SCED Timestamp", "Interval Start", "Interval End", "BatchID"],
        )

        df = df.rename(columns={"SystemLambda": "System Lambda"})

        return df.sort_values("SCED Timestamp")

    @support_date_range(frequency=None)
    def get_temperature_forecast_by_weather_zone(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get temperature forecast by weather zone in hourly intervals. Published
        once a day at 5 am central.

        Arguments:
            date (str, datetime): date to get data for
            end (str, datetime, optional): end time to get data for. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with temperature forecast data
        """
        if date == "latest":
            return self.get_temperature_forecast_by_weather_zone(
                "today",
                verbose=verbose,
            )
        else:
            # Set end to get a full day of published data
            if not end:
                end = date + pd.DateOffset(days=1)

            docs = self._get_documents(
                report_type_id=TEMPERATURE_FORECAST_BY_WEATHER_ZONE_RTID,
                extension="csv",
                published_after=date,
                published_before=end,
            )

        return self._handle_temperature_forecast_by_weather_zone_docs(docs, verbose)

    def _handle_temperature_forecast_by_weather_zone_docs(
        self,
        docs: list[Document],
        verbose: bool = False,
    ) -> pd.DataFrame:
        # Process files in a loop to add the publish time for each doc
        df = pd.concat(
            [
                self.read_doc(doc, verbose=verbose, parse=False).assign(
                    **{"Publish Time": doc.publish_date},
                )
                for doc in docs
            ],
        )

        # For the 2025 DST end transition, the raw data looks like
        # DeliveryDate,HourEnding,Coast,East,FarWest,North,NorthCentral,SouthCentral,Southern,West,DSTFlag
        # 11/02/2025,01:00,63.3, 58, 60, 57, 60, 63.5, 71.4, 59,N
        # 11/02/2025,02:00,124.3, 113, 114, 111, 116.25, 120.5, 140.6, 112.8,N
        # 11/02/2025,03:00,60.9, 55, 54, 54, 56.25, 58, 68.8, 54.2,Y
        # 11/02/2025,03:00,60.9, 55, 54, 54, 56.25, 58, 68.8, 54.2,N
        # The 3:00 ending hour is duplicated when it should be the 2:00 ending hour
        # (We will not correct the obviously wrong temperature values)
        dst_transition_date_2025 = "11/02/2025"
        if dst_transition_date_2025 in df["DeliveryDate"].unique():
            mask = (
                (df["DeliveryDate"] == "11/02/2025")
                & (df["HourEnding"] == "03:00")
                & (df["DSTFlag"] == "Y")
            )
            df.loc[mask, "HourEnding"] = "2:00"

        df = self.parse_doc(df)

        df = df.drop(columns=["Time"]).rename(
            columns=self._weather_zone_column_name_mapping(),
        )

        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End", "Publish Time"]
            + list(self._weather_zone_column_name_order()),
        )

        # NOTE(kladar): ERCOT is currently publishing a duplicate for the Fall 2024 DST transition
        # we will remove the duplicates here and adjust the times to be our best guess at what is correct
        dst_transition_date = pd.Timestamp("2024-11-03")
        if dst_transition_date.date() in df["Interval Start"].dt.date.values:
            logger.info("Problematic DST transition detected, fixing duplicate hour")

            # take half the duplicate rows and adjust them to 1:00 to fix missing interval
            duplicate_mask = df["Interval Start"] == pd.Timestamp(
                "2024-11-03 02:00:00-0600",
            )
            duplicate_indices = df[duplicate_mask].index
            first_half_indices = duplicate_indices[: len(duplicate_indices) // 2]
            df.loc[first_half_indices, "Interval Start"] = pd.Timestamp(
                "2024-11-03 01:00:00-0600",
            )
            df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

            # after the correction, the straight duplicate intervals remain, so we remove them
            df = df.drop_duplicates(subset=["Interval Start", "Publish Time"])

        return df.sort_values("Interval Start")

    def _get_document(
        self,
        report_type_id: int,
        date: str | None = None,
        published_after: str | None = None,
        published_before: str | None = None,
        constructed_name_contains: str | None = None,
        extension: str | None = None,
        verbose: bool = False,
        base_url: str = "www.ercot.com",
        request_kwargs: dict | None = None,
    ) -> Document:
        """Searches by Report Type ID, filtering for date and/or constructed name

        Raises a ValueError if no document matches

        Returns:
            Latest Document by publish_date
        """

        # no need to pass this on
        # since this only returns the latest
        # document anyways
        if date == "latest":
            date = None

        documents = self._get_documents(
            report_type_id=report_type_id,
            date=date,
            published_after=published_after,
            published_before=published_before,
            constructed_name_contains=constructed_name_contains,
            extension=extension,
            verbose=verbose,
            base_url=base_url,
            request_kwargs=request_kwargs,
        )

        return max(documents, key=lambda x: x.publish_date)

    def _get_documents(
        self,
        report_type_id: int,
        date: str | None = None,
        published_after: str | None = None,
        published_before: str | None = None,
        friendly_name_timestamp_after: str | None = None,
        friendly_name_timestamp_before: str | None = None,
        constructed_name_contains: str | None = None,
        extension: str | None = None,
        verbose: bool = False,
        base_url: str = "www.ercot.com",
        request_kwargs: dict | None = None,
    ) -> list:
        """Searches by Report Type ID, filtering for date and/or constructed name

        Returns:
            list of Document with URL and Publish Date
        """
        # Include a cache buster to ensure we get the latest data
        url = f"https://{base_url}/misapp/servlets/IceDocListJsonWS?reportTypeId={report_type_id}&_{int(time.time())}"  # noqa

        logger.info(f"Fetching document {url}")

        # if latest, we dont need to filter
        # so we can set to None
        if published_before == "latest":
            published_before = None

        docs = self._get_json(url, verbose=verbose, **(request_kwargs or {}))[
            "ListDocsByRptTypeRes"
        ]["DocumentList"]

        matches = []
        for doc in docs:
            match = True

            doc_url = f"https://{base_url}/misdownload/servlets/mirDownload?doclookupId={doc['Document']['DocID']}"  # noqa
            # make sure to handle retry files
            # e.g SPPHLZNP6905_retry_20230608_1545_csv
            try:
                friendly_name_timestamp = parse_timestamp_from_friendly_name(
                    doc["Document"]["FriendlyName"],
                )
            except Exception:
                friendly_name_timestamp = None

            friendly_name = doc["Document"]["FriendlyName"]

            # ERCOT adds xhr to the second set of file names during the repeated hour
            # for DST end. However, ERCOT may get the timezone offset wrong in the
            # PublishDate. Therefore, we remove the ERCOT provided timezone offset then
            # re-add the offset accounting for the repeated hour.
            # https://lists.ercot.com/cgi-bin/wa?A3=1111&L=NOTICE_TRAINING&E=quoted-printable&P=4519&B=--_000_B117FDA9B7BC68479362C1197F77D8790950ADCPW0005ercotcom_&T=text%2Fhtml;%20charset=us-ascii&XSS=3&header=1
            publish_date = (
                pd.Timestamp(doc["Document"]["PublishDate"])
                .tz_localize(None)
                .tz_localize(
                    self.default_timezone,
                    # Pandas wants ambiguous to be True when DST is True (Pandas only
                    # uses ambiguous during the repeated hour) The "xhr" file occurs
                    # after the clock has been set back an hour so is not in DST.
                    ambiguous="xhr" not in friendly_name,
                )
            )

            doc_obj = Document(
                url=doc_url,
                publish_date=publish_date,
                constructed_name=doc["Document"]["ConstructedName"],
                friendly_name=friendly_name,
                friendly_name_timestamp=friendly_name_timestamp,
            )

            if published_after:
                match = match and doc_obj.publish_date > published_after

            if published_before:
                match = match and doc_obj.publish_date <= published_before

            if doc_obj.friendly_name_timestamp:
                if friendly_name_timestamp_after:
                    match = (
                        match
                        and doc_obj.friendly_name_timestamp
                        > friendly_name_timestamp_after
                    )

                if friendly_name_timestamp_before:
                    match = (
                        match
                        and doc_obj.friendly_name_timestamp
                        <= friendly_name_timestamp_before
                    )

            if date and date != "latest":
                match = match and doc_obj.publish_date.date() == date.date()

            if extension:
                match = match and doc_obj.friendly_name.endswith(extension)

            if constructed_name_contains:
                match = match and constructed_name_contains in doc_obj.constructed_name

            if match:
                matches.append(doc_obj)

        if date == "latest":
            return [max(matches, key=lambda x: x.publish_date)]

        if not matches:
            params = {
                k: v
                for k, v in locals().items()
                if k not in ["self", "msg", "url", "docs"]
            }
            raise NoDataFoundException(
                f"No documents found with the given parameters: {params}",  # noqa
            )

        return matches

    def _get_hourly_report(
        self,
        start: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None,
        report_type_id: int,
        handle_doc: Callable[[Document, bool], pd.DataFrame],
        extension: str | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if start == "latest":
            # _get_document can handle "latest"
            doc = self._get_document(
                report_type_id=report_type_id,
                extension=extension,
                published_before=start,
                verbose=verbose,
            )
            docs = [doc]
        else:
            # Set end to get a full day of published data
            if end is None:
                end = start + pd.DateOffset(days=1)

            docs = self._get_documents(
                report_type_id=report_type_id,
                extension=extension,
                published_before=end,
                published_after=start,
                verbose=verbose,
            )

        all_df = []
        for doc in docs:
            df = handle_doc(doc, verbose=verbose)
            all_df.append(df)

        df = pd.concat(all_df)

        df = df.sort_values("Publish Time")

        return df

    def _handle_json_data(self, df: pd.DataFrame, columns: dict) -> pd.DataFrame:
        df["Time"] = (
            pd.to_datetime(df["epoch"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert(self.default_timezone)
        )

        cols_to_keep = ["Time"] + list(columns.keys())
        return df[cols_to_keep].rename(columns=columns)

    def _get_settlement_point_mapping(self, verbose: bool = False) -> pd.DataFrame:
        """Get DataFrame whose columns can help us filter out values"""

        doc_info = self._get_document(
            report_type_id=SETTLEMENT_POINTS_LIST_AND_ELECTRICAL_BUSES_MAPPING_RTID,
            extension=None,
            verbose=verbose,
        )
        doc_url = doc_info.url

        logger.info(f"Fetching {doc_url}")

        r = requests.get(doc_url)
        z = ZipFile(io.BytesIO(r.content))
        names = z.namelist()
        settlement_points_file = [
            name for name in names if "Settlement_Points" in name
        ][0]
        df = pd.read_csv(z.open(settlement_points_file))
        return df

    def read_doc(
        self,
        doc: Document,
        parse: bool = True,
        verbose: bool = False,
        request_kwargs: dict | None = None,
        read_csv_kwargs: dict | None = None,
    ) -> pd.DataFrame:
        logger.debug(f"Reading {doc.url}")

        if request_kwargs:
            response = requests.get(doc.url, **(request_kwargs or {})).content
            df = pd.read_csv(
                io.BytesIO(response),
                compression="zip",
                **(read_csv_kwargs or {}),
            )
        else:
            df = pd.read_csv(doc.url, compression="zip", **(read_csv_kwargs or {}))

        if parse:
            df = self.parse_doc(df, verbose=verbose)
        return df

    def read_docs(
        self,
        docs: list[Document],
        parse: bool = True,
        empty_df: pd.DataFrame | None = None,
        verbose: bool = False,
        request_kwargs: dict | None = None,
    ) -> pd.DataFrame:
        if len(docs) == 0:
            return empty_df

        dfs = []
        for doc in tqdm.tqdm(docs, desc="Reading files", disable=not verbose):
            dfs.append(
                self.read_doc(
                    doc,
                    parse=parse,
                    verbose=verbose,
                    request_kwargs=request_kwargs,
                ),
            )
        return pd.concat(dfs).reset_index(drop=True)

    def ambiguous_based_on_dstflag(self, df: pd.DataFrame) -> pd.Series:
        # DSTFlag is Y during the repeated hour (after the clock has been set back)
        # so it's False/N during DST And True/Y during Standard Time.
        # For ambiguous, Pandas wants True for DST and False for Standard Time
        # during repeated hours. Therefore, ambgiuous should be True when
        # DSTFlag is False/N

        # Some ERCOT datasets use a boolean, some use a string
        if df["DSTFlag"].dtype == bool:
            return ~df["DSTFlag"]
        # Assume that if the DSTFlag column is a string, it's "Y" or "N"
        else:
            assert set(df["DSTFlag"].unique()).issubset({"Y", "N"})
            return df["DSTFlag"] == "N"

    def parse_doc(
        self,
        doc: pd.DataFrame,
        dst_ambiguous_default: str = "infer",
        verbose: bool = False,
        nonexistent: str = "raise",
    ) -> pd.DataFrame:
        # files sometimes have different naming conventions
        # a more elegant solution would be nice

        doc.rename(
            columns={
                "deliveryDate": "DeliveryDate",
                "Delivery Date": "DeliveryDate",
                "DELIVERY_DATE": "DeliveryDate",
                "OperDay": "DeliveryDate",
                "hourEnding": "HourEnding",
                "Hour Ending": "HourEnding",
                "HOUR_ENDING": "HourEnding",
                "Repeated Hour Flag": "DSTFlag",
                "RepeatedHourFlag": "DSTFlag",
                "Date": "DeliveryDate",
                "DeliveryHour": "HourEnding",
                "Delivery Hour": "HourEnding",
                "Delivery Interval": "DeliveryInterval",
                # fix whitespace in column name
                "DSTFlag    ": "DSTFlag",
            },
            inplace=True,
        )

        original_cols = doc.columns.tolist()

        ending_time_col_name = "HourEnding"

        ambiguous = dst_ambiguous_default
        if "DSTFlag" in doc.columns:
            ambiguous = self.ambiguous_based_on_dstflag(doc)

        # i think DeliveryInterval only shows up
        # in 15 minute data along with DeliveryHour
        if "DeliveryInterval" in original_cols:
            interval_length = pd.Timedelta(minutes=15)

            doc["HourBeginning"] = doc[ending_time_col_name] - 1

            doc["Interval Start"] = (
                pd.to_datetime(doc["DeliveryDate"])
                + doc["HourBeginning"].astype("timedelta64[h]")
                + ((doc["DeliveryInterval"] - 1) * interval_length)
            )

        # 15-minute system wide actuals
        elif "TimeEnding" in original_cols:
            ending_time_col_name = "TimeEnding"
            interval_length = pd.Timedelta(minutes=15)

            doc["Interval End"] = pd.to_datetime(
                doc["DeliveryDate"] + " " + doc["TimeEnding"] + ":00",
            )
            doc["Interval End"] = doc["Interval End"].dt.tz_localize(
                self.default_timezone,
                ambiguous=ambiguous,
            )
            doc["Interval Start"] = doc["Interval End"] - interval_length

        else:
            interval_length = pd.Timedelta(hours=1)
            doc["HourBeginning"] = (
                doc[ending_time_col_name]
                .astype(str)
                .str.split(
                    ":",
                )
                .str[0]
                .astype(int)
                - 1
            )
            doc["Interval Start"] = pd.to_datetime(doc["DeliveryDate"]) + doc[
                "HourBeginning"
            ].astype("timedelta64[h]")

        if "TimeEnding" not in original_cols:
            try:
                doc["Interval Start"] = doc["Interval Start"].dt.tz_localize(
                    self.default_timezone,
                    ambiguous=ambiguous,
                    nonexistent=nonexistent,
                )
            except NonExistentTimeError:
                # this handles how ercot does labels the instant
                # of the DST transition differently than
                # pandas does
                doc["Interval Start"] = doc["Interval Start"] + pd.Timedelta(hours=1)
                doc["Interval Start"] = doc["Interval Start"].dt.tz_localize(
                    self.default_timezone,
                    ambiguous=ambiguous,
                ) - pd.Timedelta(hours=1)
            except pytz.AmbiguousTimeError as e:
                # Sometimes ERCOT handles DST end by putting 25 hours in HourEnding
                # which makes IntervalStart where HourEnding >= 3 an hour later than
                # they should be. We correct this by subtracting an hour.
                assert doc["HourEnding"].max() == 25, (
                    f"Time parsing error. Did not find HourEnding = 25. {e}"
                )
                doc.loc[doc["HourEnding"] >= 3, "Interval Start"] = doc.loc[
                    doc["HourEnding"] >= 3,
                    "Interval Start",
                ] - pd.Timedelta(hours=1)

                # Now there will be a repeated hour and Pandas can infer
                # the ambiguous value
                doc["Interval Start"] = doc["Interval Start"].dt.tz_localize(
                    self.default_timezone,
                    ambiguous="infer",
                )

            doc["Interval End"] = doc["Interval Start"] + interval_length

        doc["Time"] = doc["Interval Start"]
        doc = doc.sort_values("Time", ascending=True)

        cols_to_keep = [
            "Time",
            "Interval Start",
            "Interval End",
        ] + original_cols

        # TODO: try to clean up this logic
        doc = doc[cols_to_keep]
        doc = doc.drop(
            columns=["DeliveryDate", ending_time_col_name],
        )

        optional_drop = ["DSTFlag", "DeliveryInterval"]

        for col in optional_drop:
            if col in doc.columns:
                doc = doc.drop(columns=[col])

        return doc

    def _weather_zone_column_name_mapping(self) -> dict[str, str]:
        return {
            "Coast": "Coast",
            "East": "East",
            "FarWest": "Far West",
            "North": "North",
            "NorthCentral": "North Central",
            "North C": "North Central",
            "SouthCentral": "South Central",
            "South C": "South Central",
            "Southern": "Southern",
            "West": "West",
            "Total": "System Total",
        }

    def _weather_zone_column_name_order(self) -> list[str]:
        return [
            "Coast",
            "East",
            "Far West",
            "North",
            "North Central",
            "South Central",
            "Southern",
            "West",
        ]

    @support_date_range(frequency=None)
    def get_indicative_lmp_by_settlement_point(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            self.get_indicative_lmp_by_settlement_point(date="today")
        if not end:
            end = date + pd.DateOffset(days=1)

        docs = self._get_documents(
            report_type_id=ERCOT_INDICATIVE_LMP_BY_SETTLEMENT_POINT_RTID,
            extension="csv",
            published_before=end,
            published_after=date,
            verbose=verbose,
        )
        df = self.read_docs(docs, parse=False, verbose=verbose)
        return self._handle_indicative_lmp_by_settlement_point(df)

    def _handle_indicative_lmp_by_settlement_point(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        columns_to_rename = {
            "RTDTimestamp": "RTD Timestamp",
            "IntervalEnding": "Interval End",
            "SettlementPoint": "Location",
            "SettlementPointType": "Location Type",
            "LMP": "LMP",
        }
        df.rename(columns=columns_to_rename, inplace=True)
        assert set(df["RepeatedHourFlag"].unique()).issubset({"Y", "N"})
        assert set(df["IntervalRepeatedHourFlag"].unique()).issubset({"Y", "N"})

        df["RTD Timestamp"] = pd.to_datetime(df["RTD Timestamp"]).dt.tz_localize(
            self.default_timezone,
            ambiguous=df["RepeatedHourFlag"] == "N",
            nonexistent="shift_forward",
        )
        df["Interval End"] = pd.to_datetime(df["Interval End"]).dt.tz_localize(
            self.default_timezone,
            ambiguous=df["IntervalRepeatedHourFlag"] == "N",
            nonexistent="shift_forward",
        )

        df["Interval Start"] = df["Interval End"] - pd.Timedelta(minutes=5)
        df = df.sort_values("Interval Start").reset_index(drop=True)
        return df[
            [
                "RTD Timestamp",
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
            ]
        ]

    @support_date_range(frequency="DAY_START")
    def get_dam_total_energy_purchased(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get DAM Total Energy Purchased

        Arguments:
            date (str, datetime): date to get data for
            end (str, datetime): end time to get data for
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with DAM total energy purchased data
        """
        if date == "latest":
            return self.get_dam_total_energy_purchased(
                date="today",
                verbose=verbose,
            )

        # DAM data so subtract one from the date
        doc = self._get_document(
            report_type_id=DAM_TOTAL_ENERGY_PURCHASED_RTID,
            date=date - pd.DateOffset(days=1),
            extension="csv",
        )

        return self._process_dam_total_energy(
            doc,
            verbose=verbose,
        )

    def _process_dam_total_energy(
        self,
        doc: Document,
        verbose: bool = False,
    ) -> pd.DataFrame:
        return (
            self.read_doc(doc, verbose=verbose)
            .rename(
                columns={
                    "Settlement_Point": "Location",
                    "TotalDAMEnergySold": "Total",
                    "Total_DAM_Energy_Bought": "Total",
                },
            )
            .drop(
                columns=["Time"],
            )
            .sort_values(["Interval Start", "Location"])
            .reset_index(drop=True)
        )

    @support_date_range(frequency="DAY_START")
    def get_dam_total_energy_sold(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get DAM Total Energy Sold

        Arguments:
            date (str, datetime): date to get data for
            end (str, datetime): end time to get data for
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with DAM total energy sold data
        """
        if date == "latest":
            return self.get_dam_total_energy_sold(
                date="today",
                verbose=verbose,
            )

        # DAM data so subtract one from the date
        doc = self._get_document(
            report_type_id=DAM_TOTAL_ENERGY_SOLD_RTID,
            date=date - pd.DateOffset(days=1),
            extension="csv",
        )

        return self._process_dam_total_energy(
            doc,
            verbose=verbose,
        )

    @support_date_range(frequency="DAY_START")
    def get_cop_adjustment_period_snapshot_60_day(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest" or date > pd.Timestamp.now(
            tz=self.default_timezone,
        ) - pd.DateOffset(days=60):
            raise ValueError(
                "Cannot get COP Adjustment Period Snapshot for date < 60 days in the past",
            )

        # Data is delayed by 60 days. To get the report for a given day, we have to
        # look 60 days in the future relative to the target date
        report_date = date + pd.DateOffset(days=60)

        # Delayed by 60 days
        doc = self._get_document(
            report_type_id=COP_ADJUSTMENT_PERIOD_SNAPSHOT_RTID,
            date=report_date,
        )

        data = self.read_doc(doc, verbose=verbose)

        return self._process_cop_adjustment_period_snapshot_60_day_data(
            data,
            verbose=verbose,
        )

    def _process_cop_adjustment_period_snapshot_60_day_data(
        self,
        data: pd.DataFrame,
        verbose: bool = False,
    ) -> pd.DataFrame:
        data = (
            data.rename(columns={"QSE Name": "QSE"})
            .drop(
                columns=["Time"],
            )
            .sort_values(["Interval Start", "Resource Name"])
            .reset_index(drop=True)
        )

        # Columns not in older data files or not in newer data files.
        for col in [
            # Present in old data, but not new
            "RRS",
            # These four columns are present only in new data
            "RRSPFR",
            "RRSFFR",
            "RRSUFR",
            "ECRS",
            # These three columns first have data on 2024-06-28
            "Minimum SOC",
            "Maximum SOC",
            "Hour Beginning Planned SOC",
        ]:
            if col not in data.columns:
                data[col] = pd.NA

        data = data[
            [
                "Interval Start",
                "Interval End",
                "Resource Name",
                "QSE",
                "Status",
                "High Sustained Limit",
                "Low Sustained Limit",
                "High Emergency Limit",
                "Low Emergency Limit",
                "Reg Up",
                "Reg Down",
                "RRS",
                "RRSPFR",
                "RRSFFR",
                "RRSUFR",
                "NSPIN",
                "ECRS",
                "Minimum SOC",
                "Maximum SOC",
                "Hour Beginning Planned SOC",
            ]
        ]

        return data

    # Published every SCED interval
    @support_date_range(frequency=None)
    def get_mcpc_sced(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Market Clearing Prices for Capacity by SCED interval"""
        if date == "latest":
            docs = self._get_documents(
                report_type_id=REAL_TIME_CLEARING_PRICES_FOR_CAPACITY_BY_SCED_INTERVAL_RTID,
                extension="csv",
                date=date,
                verbose=verbose,
            )
        else:
            if end is None:
                # Assume getting data for one day
                end = date + pd.DateOffset(days=1)

            published_before = end
            published_after = date

            docs = self._get_documents(
                report_type_id=REAL_TIME_CLEARING_PRICES_FOR_CAPACITY_BY_SCED_INTERVAL_RTID,
                extension="csv",
                published_before=published_before,
                published_after=published_after,
                verbose=verbose,
            )

        df = self.read_docs(docs, parse=False, verbose=verbose)
        return self._handle_mcpc_sced(df)

    def _handle_mcpc_sced(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={"ASType": "AS Type"})
        df = self._handle_sced_timestamp(df)

        df["MCPC"] = pd.to_numeric(df["MCPC"], errors="coerce")

        return (
            # Only need the SCED Timestamps
            df[["SCED Timestamp", "AS Type", "MCPC"]]
            .sort_values(["SCED Timestamp", "AS Type"])
            .reset_index(drop=True)
        )

    # Published every 15 minutes for the past 15 minutes.
    @support_date_range(frequency=None)
    def get_mcpc_real_time_15_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Market Clearing Prices for Capacity by 15-minute interval"""
        if date == "latest":
            docs = self._get_documents(
                report_type_id=REAL_TIME_CLEARING_PRICES_FOR_CAPACITY_15_MIN_RTID,
                extension="csv",
                date=date,
                verbose=verbose,
            )

        else:
            # Assume getting data for one day
            if not end:
                end = date + pd.DateOffset(days=1)

            published_before = end + pd.Timedelta(minutes=15)
            published_after = date + pd.Timedelta(minutes=15)

            docs = self._get_documents(
                report_type_id=REAL_TIME_CLEARING_PRICES_FOR_CAPACITY_15_MIN_RTID,
                extension="csv",
                published_before=published_before,
                published_after=published_after,
                verbose=verbose,
            )

        df = self.read_docs(docs, parse=False, verbose=verbose)
        return self._handle_mcpc_real_time_15_min(df)

    def _handle_mcpc_real_time_15_min(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        df = df.rename(
            columns={"ASType": "AS Type", "RepeatedHourFlag": "DSTFlag"},
        )

        df = self.parse_doc(df)

        df["MCPC"] = pd.to_numeric(df["MCPC"], errors="coerce")

        return (
            df[["Interval Start", "Interval End", "AS Type", "MCPC"]]
            .sort_values(["Interval Start", "AS Type"])
            .reset_index(drop=True)
        )

    # Published once per day for today and tomorrow in the same file
    @support_date_range(frequency="DAY_START")
    def get_as_demand_curves_dam_and_sced(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Ancillary Service Demand Curves"""
        docs = self._get_documents(
            report_type_id=DAM_AND_SCED_ANCILLARY_SERVICE_DEMAND_CURVES_RTID,
            extension="csv",
            date=date,
            verbose=verbose,
        )

        df = pd.concat(
            [
                self.read_doc(doc, parse=False, verbose=verbose).assign(
                    **{"Publish Time": doc.publish_date},
                )
                for doc in docs
            ],
        )

        return self._handle_as_demand_curves_dam_and_sced(df)

    def _handle_as_demand_curves_dam_and_sced(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "ASType": "AS Type",
                "DemandCurvePoint": "Demand Curve Point",
                "RepeatedHourFlag": "DSTFlag",
            },
        )
        df = self.parse_doc(df)

        for col in ["Quantity", "Price", "Demand Curve Point"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "AS Type",
                    "Demand Curve Point",
                    "Quantity",
                    "Price",
                ]
            ]
            .sort_values(
                ["Interval Start", "Publish Time", "AS Type", "Demand Curve Point"],
            )
            .reset_index(drop=True)
        )

    # Published once per day for tomorrow
    @support_date_range(frequency=None)
    def get_as_deployment_factors_projected(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Projected Ancillary Service Deployment Factors"""
        if date == "latest":
            docs = self._get_documents(
                report_type_id=PROJECTED_ANCILLARY_SERVICE_DEPLOYMENTS_FACTORS_RTID,
                extension="csv",
                date=date,
                verbose=verbose,
            )
        else:
            if end is None:
                end = date + pd.DateOffset(days=1)

            docs = self._get_documents(
                report_type_id=PROJECTED_ANCILLARY_SERVICE_DEPLOYMENTS_FACTORS_RTID,
                extension="csv",
                published_before=end,
                published_after=date,
                verbose=verbose,
            )

        return self._handle_as_deployment_factors_projected(docs, verbose=verbose)

    def _handle_as_deployment_factors_projected(
        self,
        docs: list[Document],
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self.read_docs(docs, parse=False, verbose=verbose)

        df = df.rename(
            columns={
                "ASType": "AS Type",
                "ASDeploymentFactors": "AS Deployment Factors",
                "RepeatedHourFlag": "DSTFlag",
            },
        )

        df = self.parse_doc(df)

        df["AS Deployment Factors"] = pd.to_numeric(
            df["AS Deployment Factors"],
            errors="coerce",
        )

        return (
            df[["Interval Start", "Interval End", "AS Type", "AS Deployment Factors"]]
            .sort_values("Interval Start")
            .reset_index(drop=True)
        )

    # Published per WRUC run (once per day) for the next 5 days
    @support_date_range(frequency=None)
    def get_as_deployment_factors_weekly_ruc(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Weekly RUC Ancillary Service Deployment Factors

        Retrieves ancillary service deployment factors used by the Weekly
        Reliability Unit Commitment (WRUC) process for each hour in the RUC
        Study Period.

        Args:
            date: Date to retrieve data for. Can be a string or pandas Timestamp.
            end: Optional end date for date range queries.
            verbose: If True, print verbose output.

        Returns:
            DataFrame with columns: Interval Start, Interval End, RUC Timestamp,
            AS Type, and AS Deployment Factors.
        """
        if date == "latest":
            docs = self._get_documents(
                report_type_id=WEEKLY_RUC_AS_DEPLOYMENT_FACTORS_RTID,
                date=date,
                constructed_name_contains="csv",
                verbose=verbose,
            )
        else:
            if not end:
                end = date + pd.DateOffset(days=1)

            docs = self._get_documents(
                report_type_id=WEEKLY_RUC_AS_DEPLOYMENT_FACTORS_RTID,
                constructed_name_contains="csv",
                published_after=date,
                published_before=end,
                verbose=verbose,
            )

        return self._handle_as_deployment_factors_ruc(docs, verbose=verbose)

    # Published per DRUC run (once per day) for the next day
    @support_date_range(frequency=None)
    def get_as_deployment_factors_daily_ruc(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Daily RUC Ancillary Service Deployment Factors"""
        if date == "latest":
            docs = self._get_documents(
                report_type_id=DAILY_RUC_AS_DEPLOYMENT_FACTORS_RTID,
                date=date,
                constructed_name_contains="csv",
                verbose=verbose,
            )
        else:
            if not end:
                end = date + pd.DateOffset(days=1)

            docs = self._get_documents(
                report_type_id=DAILY_RUC_AS_DEPLOYMENT_FACTORS_RTID,
                constructed_name_contains="csv",
                published_after=date,
                published_before=end,
                verbose=verbose,
            )

        return self._handle_as_deployment_factors_ruc(docs, verbose=verbose)

    # Published per HRUC run (once per hour) for the rest of the current day (so each
    # file can have a differing number of intervals)
    @support_date_range(frequency=None)
    def get_as_deployment_factors_hourly_ruc(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Hourly RUC Ancillary Service Deployment Factors"""
        if date == "latest":
            docs = self._get_documents(
                report_type_id=HOURLY_RUC_AS_DEPLOYMENT_FACTORS_RTID,
                date=date,
                constructed_name_contains="csv",
                verbose=verbose,
            )
        else:
            if not end:
                end = date + pd.DateOffset(days=1)

            docs = self._get_documents(
                report_type_id=HOURLY_RUC_AS_DEPLOYMENT_FACTORS_RTID,
                constructed_name_contains="csv",
                published_after=date,
                published_before=end,
                verbose=verbose,
            )

        return self._handle_as_deployment_factors_ruc(docs, verbose=verbose)

    def _handle_as_deployment_factors_ruc(
        self,
        docs: list[Document],
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self.read_docs(docs, parse=False, verbose=verbose)
        df = df.rename(
            columns={
                "RUCTimestamp": "RUC Timestamp",
                "ASType": "AS Type",
                "ASDeploymentFactors": "AS Deployment Factors",
                "RepeatedHourFlag": "DSTFlag",
            },
        )

        df = self.parse_doc(df)

        # Parse RUC Timestamp
        df["RUC Timestamp"] = pd.to_datetime(
            df["RUC Timestamp"],
        ).dt.tz_localize(self.default_timezone)

        df["AS Deployment Factors"] = pd.to_numeric(
            df["AS Deployment Factors"],
            errors="coerce",
        )

        return (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "RUC Timestamp",
                    "AS Type",
                    "AS Deployment Factors",
                ]
            ]
            .sort_values(["Interval Start", "RUC Timestamp", "AS Type"])
            .reset_index(drop=True)
        )

    # Published per HRUC run (every hour) for the rest of the day
    @support_date_range(frequency=None)
    def get_as_demand_curves_hourly_ruc(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Hourly RUC Ancillary Service Demand Curves"""
        if date == "latest":
            docs = self._get_documents(
                report_type_id=HOURLY_RUC_AS_DEMAND_CURVES_RTID,
                extension="csv",
                date=date,
                verbose=verbose,
            )
        else:
            if not end:
                end = date + pd.DateOffset(days=1)

            docs = self._get_documents(
                report_type_id=HOURLY_RUC_AS_DEMAND_CURVES_RTID,
                published_before=end,
                published_after=date,
                extension="csv",
                verbose=verbose,
            )

        df = self.read_docs(docs, parse=False, verbose=verbose)
        return self._handle_ruc_as_demand_curves(df)

    # Published per DRUC run (once per day) for the next day
    @support_date_range(frequency=None)
    def get_as_demand_curves_daily_ruc(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Daily RUC Ancillary Service Demand Curves"""
        if date == "latest":
            docs = self._get_documents(
                report_type_id=DAILY_RUC_AS_DEMAND_CURVES_RTID,
                extension="csv",
                date=date,
                verbose=verbose,
            )
        else:
            if not end:
                end = date + pd.DateOffset(days=1)

            docs = self._get_documents(
                report_type_id=DAILY_RUC_AS_DEMAND_CURVES_RTID,
                published_before=end,
                published_after=date,
                extension="csv",
                verbose=verbose,
            )

        df = self.read_docs(docs, parse=False, verbose=verbose)
        return self._handle_ruc_as_demand_curves(df)

    # Published per WRUC run (once per day) for the next five days
    @support_date_range(frequency=None)
    def get_as_demand_curves_weekly_ruc(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Weekly RUC Ancillary Service Demand Curves"""
        if date == "latest":
            docs = self._get_documents(
                report_type_id=WEEKLY_RUC_AS_DEMAND_CURVES_RTID,
                extension="csv",
                date=date,
                verbose=verbose,
            )
        else:
            if not end:
                end = date + pd.DateOffset(days=1)

            docs = self._get_documents(
                report_type_id=WEEKLY_RUC_AS_DEMAND_CURVES_RTID,
                published_before=end,
                published_after=date,
                extension="csv",
                verbose=verbose,
            )

        df = self.read_docs(docs, parse=False, verbose=verbose)
        return self._handle_ruc_as_demand_curves(df)

    def _handle_ruc_as_demand_curves(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        df = df.rename(
            columns={
                "RUCTimeStamp": "RUC Timestamp",
                "ASType": "AS Type",
                "DemandCurvePoint": "Demand Curve Point",
                "RepeatedHourFlag": "DSTFlag",
            },
        )

        df = self.parse_doc(df)

        # Parse RUC Timestamp
        df["RUC Timestamp"] = pd.to_datetime(
            df["RUC Timestamp"],
        ).dt.tz_localize(self.default_timezone)

        for col in ["Quantity", "Price", "Demand Curve Point"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "RUC Timestamp",
                    "AS Type",
                    "Demand Curve Point",
                    "Quantity",
                    "Price",
                ]
            ]
            .sort_values(
                ["Interval Start", "RUC Timestamp", "AS Type", "Demand Curve Point"],
            )
            .reset_index(drop=True)
        )

    # Published per DAM run for the next day
    @support_date_range(frequency="DAY_START")
    def get_dam_total_as_sold(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get DAM Total Ancillary Services Sold"""
        if date != "latest":
            date -= pd.DateOffset(days=1)

        docs = self._get_documents(
            report_type_id=DAM_TOTAL_AS_SOLD_RTID,
            date=date,
            constructed_name_contains="csv",
            verbose=verbose,
        )

        df = self.read_docs(docs, parse=False, verbose=verbose)
        return self._handle_dam_total_as_sold(df)

    def _handle_dam_total_as_sold(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle DAM Total Ancillary Services Sold data."""
        df = df.rename(
            columns={
                "ASType": "AS Type",
                "Quantity": "Quantity",
                "RepeatedHourFlag": "DSTFlag",
            },
        )

        df = self.parse_doc(df)
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")

        return (
            df[
                [
                    "Interval Start",
                    "Interval End",
                    "AS Type",
                    "Quantity",
                ]
            ]
            .sort_values(["Interval Start", "AS Type"])
            .reset_index(drop=True)
        )

    # Published per RTD run for the next 55 minutes (11 intervals per file)
    @support_date_range(frequency=None)
    def get_indicative_mcpc_rtd(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get RTD Indicative Real-Time Market Clearing Prices for Capacity"""
        if date == "latest":
            docs = self._get_documents(
                report_type_id=RTD_INDICATIVE_REAL_TIME_MCPC_RTID,
                extension="csv",
                date=date,
                verbose=verbose,
            )
        else:
            if not end:
                end = date + pd.DateOffset(days=1)

            published_before = end
            published_after = date

            docs = self._get_documents(
                report_type_id=RTD_INDICATIVE_REAL_TIME_MCPC_RTID,
                extension="csv",
                published_before=published_before,
                published_after=published_after,
                verbose=verbose,
            )

        df = self.read_docs(docs, parse=False, verbose=verbose)
        return self._handle_indicative_mcpc_rtd(df)

    def _handle_indicative_mcpc_rtd(self, df: pd.DataFrame) -> pd.DataFrame:
        # Parse timestamps with DST handling
        df["Interval End"] = pd.to_datetime(df["IntervalEnding"]).dt.tz_localize(
            self.default_timezone,
            ambiguous=self.ambiguous_based_on_dstflag(
                df.rename(columns={"IntervalEndingRepeatedHourFlag": "DSTFlag"}),
            ),
        )

        df["Interval Start"] = df["Interval End"] - pd.Timedelta(minutes=5)

        df["RTD Timestamp"] = pd.to_datetime(df["RTDTimestamp"]).dt.tz_localize(
            self.default_timezone,
            ambiguous=self.ambiguous_based_on_dstflag(
                df.rename(columns={"RepeatedHourFlag": "DSTFlag"}),
            ),
        )

        # Convert price columns to numeric (float64)
        price_cols = ["REGUP", "REGDN", "RRS", "ECRS", "NSPIN"]
        for col in price_cols:
            df[col] = df[col].astype("float64")

        return (
            df[["Interval Start", "Interval End", "RTD Timestamp"] + price_cols]
            .sort_values(["Interval Start", "RTD Timestamp"])
            .reset_index(drop=True)
        )

    # Published every SCED interval
    @support_date_range(frequency=None)
    def get_as_total_capability(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Total Capability of Resources Available to Provide Ancillary Service"""
        if date == "latest":
            docs = self._get_documents(
                report_type_id=TOTAL_CAPABILITY_OF_RESOURCES_AS_RTID,
                extension="csv",
                date=date,
                verbose=verbose,
            )
        else:
            if not end:
                end = date + pd.DateOffset(days=1)

            published_before = end
            published_after = date

            docs = self._get_documents(
                report_type_id=TOTAL_CAPABILITY_OF_RESOURCES_AS_RTID,
                extension="csv",
                published_before=published_before,
                published_after=published_after,
                verbose=verbose,
            )

        df = self.read_docs(docs, parse=False, verbose=verbose)
        return self._handle_as_total_capability(df)

    def _handle_as_total_capability(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "SCEDTimestamp": "SCED Timestamp",
                "CapREGUPTotal": "Cap RegUp Total",
                "CapREGDNTotal": "Cap RegDn Total",
                "CapRRSTotal": "Cap RRS Total",
                "CapECRSTotal": "Cap ECRS Total",
                "CapNSPINTotal": "Cap NonSpin Total",
                "CapREGUP_RRSTotal": "Cap RegUp RRS Total",
                "CapREGUP_RRS_ECRSTotal": "Cap RegUp RRS ECRS Total",
                "CapREGUP_RRS_ECRS_NSPINTotal": "Cap RegUp RRS ECRS NonSpin Total",
                "DSTFlag": "RepeatedHourFlag",
            },
        )

        df = self._handle_sced_timestamp(df)

        # Convert capability columns to numeric
        cap_cols = [
            "Cap RegUp Total",
            "Cap RegDn Total",
            "Cap RRS Total",
            "Cap ECRS Total",
            "Cap NonSpin Total",
            "Cap RegUp RRS Total",
            "Cap RegUp RRS ECRS Total",
            "Cap RegUp RRS ECRS NonSpin Total",
        ]

        for col in cap_cols:
            df[col] = df[col].astype(float)

        return (
            df[["SCED Timestamp"] + cap_cols]
            .sort_values("SCED Timestamp")
            .reset_index(drop=True)
        )

    # Published every SCED interval
    @support_date_range(frequency=None)
    def get_real_time_adders(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get Real-Time ORDC and Reliability Deployment
        Price Adders and Reserves by SCED Interval produced by SCED every five minutes.

        Arguments:
            date: date to get data for
            end: end date to get data for. If None, defaults to date + 1 day
            verbose: print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with ORDC price adders data
        """
        if date == "latest":
            docs = self._get_documents(
                report_type_id=REAL_TIME_ADDERS_RTID,
                extension="csv",
                date=date,
                verbose=verbose,
            )
        else:
            if not end:
                # Assume getting data for one day
                end = date + pd.DateOffset(days=1)

            published_before = end
            published_after = date

            docs = self._get_documents(
                report_type_id=REAL_TIME_ADDERS_RTID,
                published_after=published_after,
                published_before=published_before,
                extension="csv",
                verbose=verbose,
            )

        return self._handle_real_time_adders(docs, verbose=verbose)

    def _handle_real_time_adders(
        self,
        docs: list[Document],
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = self.read_docs(docs, parse=False, verbose=verbose)
        df = self._handle_sced_timestamp(df)

        df = utils.move_cols_to_front(
            df,
            ["SCED Timestamp", "Interval Start", "Interval End"],
        )
        df = df.rename(columns={"SystemLambda": "System Lambda"})

        return df.sort_values("SCED Timestamp").reset_index(drop=True)

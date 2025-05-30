import datetime
import io
import time
from dataclasses import dataclass
from enum import Enum
from typing import List
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
from gridstatus.gs_logging import log, logger
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
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-323-CD
REAL_TIME_ADDERS_AND_RESERVES_RTID = 13221

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

# 2-Day Ancillary Services Reports
# https://www.ercot.com/mp/data-products/data-product-details?id=NP3-911-ER
TWO_DAY_ANCILLARY_SERVICES_REPORTS_RTID = 13057

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


def parse_timestamp_from_friendly_name(friendly_name):
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

    def get_status(self, date, verbose=False):
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

    def get_energy_storage_resources(self, date="latest", verbose=False):
        """Get energy storage resources.
        Always returns data from previous and current day"""
        url = self.BASE + "/energy-storage-resources.json"
        data = self._get_json(url, verbose=verbose)

        df = pd.DataFrame(data["previousDay"]["data"] + data["currentDay"]["data"])

        # todo(kanter): fix this for future DST dates
        if "2024-11-03 02:00:00-0600" in df["timestamp"].values:
            # ERCOT publishes two intervals with 2am timestamp
            # during CDT to CST transition
            # but skips the repeated 1am timestamp
            # let's manually fix this before further timestamp parsing
            df.loc[
                (df["timestamp"] == "2024-11-03 02:00:00-0600")
                & (df["dstFlag"] == "N"),
                "timestamp",
            ] = "2024-11-03 01:00:00-0600"

        df = df[["timestamp", "totalCharging", "totalDischarging", "netOutput"]]

        # need to use apply since there can be mixed
        # fixed offsets during dst transition
        # that result in object dtypes in pandas
        df["timestamp"] = df["timestamp"].apply(
            lambda x: pd.to_datetime(x).tz_convert("UTC"),
        )
        df["timestamp"] = df["timestamp"].dt.tz_convert(self.default_timezone)

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

    def get_fuel_mix(self, date, verbose=False):
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
    def get_load(self, date, end=None, verbose=False):
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
    def get_load_by_weather_zone(self, date, verbose=False):
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
    def get_load_by_forecast_zone(self, date, verbose=False):
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

    def _get_forecast_zone_load_html(self, when, verbose=False):
        """Returns load for currentDay or previousDay"""
        url = self.ACTUAL_LOADS_FORECAST_ZONES_URL_FORMAT.format(
            timestamp=when.strftime("%Y%m%d"),
        )
        df = self._read_html_display(url=url, verbose=verbose)
        return df

    def _get_weather_zone_load_html(self, when, verbose=False):
        """Returns load for currentDay or previousDay"""
        url = self.ACTUAL_LOADS_WEATHER_ZONES_URL_FORMAT.format(
            timestamp=when.strftime("%Y%m%d"),
        )
        df = self._read_html_display(
            url=url,
            verbose=verbose,
        )
        return df

    def _read_html_display(self, url, verbose=False):
        msg = f"Fetching {url}"
        log(msg, verbose)

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

    def _get_supply_demand_json(self, verbose=False):
        url = self.BASE + "/supply-demand.json"
        msg = f"Fetching {url}"
        log(msg, verbose)

        return self._get_json(url)

    def _get_update_timestamp_from_supply_demand_json(self, supply_demand_json):
        return pd.to_datetime(supply_demand_json["lastUpdated"])

    def _get_todays_outlook_non_forecast(self, date, verbose=False):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        assert date == "latest" or utils.is_today(
            date,
            self.default_timezone,
        ), "Only today's data is supported"

        supply_demand_json = self._get_supply_demand_json(verbose=verbose)
        data = pd.DataFrame(supply_demand_json["data"])

        # need to use apply since there can be mixed
        # fixed offsets during dst transition
        # that result in object dtypes in pandas
        data["Interval End"] = data["timestamp"].apply(
            lambda x: pd.to_datetime(x).tz_convert("UTC"),
        )
        data["Interval End"] = data["Interval End"].dt.tz_convert(self.default_timezone)

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
        date,
        end=None,
        forecast_type=ERCOTSevenDayLoadForecastReport.BY_FORECAST_ZONE,
        verbose=False,
    ):
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
        # todo migrate to _get_hourly_report
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

    def _handle_load_forecast(self, doc, forecast_type, verbose=False):
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

    def get_capacity_committed(self, date="latest", verbose=False):
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

    def get_capacity_forecast(self, date="latest", verbose=False):
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

    def _get_capacity_dataset(self, verbose=False):
        supply_demand_json = self._get_supply_demand_json(verbose=verbose)

        data = pd.DataFrame(supply_demand_json["data"])

        data.loc[
            :,
            "Publish Time",
        ] = self._get_update_timestamp_from_supply_demand_json(supply_demand_json)

        data["Interval Start"] = pd.to_datetime(data["timestamp"])
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

    def get_available_seasonal_capacity_forecast(self, date="latest", verbose=False):
        """
        Retrieves the forecasted demand (Load Forecast) and the forecasted available
        seasonal capacity (Available Capacity) for the next 6 days.

        Data is ephemeral and does not support past days.
        """
        supply_demand_json = self._get_supply_demand_json(verbose=verbose)
        data = pd.DataFrame(supply_demand_json["forecast"])
        data = self.parse_doc(data)

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

    def get_rtm_spp(self, year, verbose=False):
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

    def get_dam_spp(self, year, verbose=False):
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

    def get_raw_interconnection_queue(self, verbose=False):
        doc_info = self._get_document(
            report_type_id=GIS_REPORT_RTID,
            constructed_name_contains="GIS_Report",
            verbose=verbose,
        )
        msg = f"Downloading interconnection queue from: {doc_info.url} "
        log(msg, verbose)
        response = requests.get(doc_info.url)
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose=False):
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
        date,
        end=None,
        location_type: str = SETTLEMENT_POINT_LOCATION_TYPE,  # TODO: support 'ALL'
        verbose=False,
    ):
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

    def _handle_lmp(self, docs, verbose=False, sced=True):
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

    def _handle_lmp_df(self, df, verbose=False, sced=True):
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
        date,
        end=None,
        market: str = None,
        locations: list = "ALL",
        location_type: str = "ALL",
        verbose=False,
    ):
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

    def _handle_settlement_point_name_and_type(self, df, verbose=False):
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
        df,
        market,
        locations=None,
        location_type=None,
        verbose=False,
    ):
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
        date,
        end=None,
        verbose=False,
    ):
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

        msg = f"Downloading {doc_info.url}"
        log(msg, verbose)

        doc = self.read_doc(doc_info, verbose=verbose)

        df = self._finalize_as_price_df(
            doc,
            pivot=True,
        )

        return df

    @support_date_range(frequency="DAY_START")
    def get_as_plan(
        self,
        date,
        end=None,
        verbose=False,
    ):
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

        msg = f"Downloading {doc_info.url}"
        log(msg, verbose)

        doc = self.read_doc(doc_info, verbose=verbose).drop(columns=["Time"])
        doc["Publish Time"] = doc_info.publish_date

        return self._handle_as_plan(doc)

    def _handle_as_plan(self, doc):
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
    def get_60_day_sced_disclosure(self, date, end=None, process=False, verbose=False):
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

    def _handle_60_day_sced_disclosure(self, z, process=False, verbose=False):
        # todo there are other files in the zip folder
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

        def handle_time(df, time_col, is_interval_end=False):
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
            log("Processing 60 day SCED disclosure data", verbose=verbose)
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
    def get_60_day_dam_disclosure(self, date, end=None, process=False, verbose=False):
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
        z,
        process=False,
        verbose=False,
        files_prefix: dict = None,
    ):
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
        url="https://www.ercot.com/files/docs/2023/05/05/SARA_Summer2023_Revised.xlsx",
        verbose=False,
    ):
        """Parse SARA data from url.

        Seasonal Assessment of Resource Adequacy for the ERCOT Region (SARA)

        Arguments:
            url (str, optional): url to download SARA data from. Defaults to
                Summer 2023 SARA data.

        """

        # only reading SummerCapacities right now
        # todo parse more sheets
        log("Getting SARA data from {}".format(url), verbose=verbose)
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

    def _finalize_as_price_df(self, doc, pivot=False):
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

    def get_as_monitor(self, date="latest", verbose=False):
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

        df = self._download_html_table(url, verbose=verbose)

        return df

    def get_real_time_system_conditions(self, date="latest", verbose=False):
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
        df = self._download_html_table(url, verbose=verbose)
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

    def _download_html_table(self, url, verbose=False):
        log(f"Downloading {url}", verbose)

        html = requests.get(url).content

        soup = BeautifulSoup(html, "html.parser")

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

    def _handle_hourly_wind_or_solar_report(self, doc, verbose=False):
        df = self.read_doc(doc, verbose=verbose)
        df.insert(
            0,
            "Publish Time",
            pd.to_datetime(doc.publish_date).tz_convert(self.default_timezone),
        )
        # replace _ in column names with spaces
        df.columns = df.columns.str.replace("_", " ")

        return self._rename_hourly_wind_or_solar_report(df)

    def _rename_hourly_wind_or_solar_report(self, df):
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

    def get_reported_outages(self, date=None, end=None, verbose=False):
        """
        Retrieves the 5-minute data behind this dashboard:
        https://www.ercot.com/gridmktinfo/dashboards/generationoutages

        Data available at
        https://www.ercot.com/api/1/services/read/dashboards/generation-outages.json

        This data is ephemeral in that there is only one file available that is
        constantly updated. There is no historical data.
        """

        log("Downloading ERCOT reported outages data", verbose=verbose)

        json = requests.get(
            "https://www.ercot.com/api/1/services/read/dashboards/generation-outages.json",  # noqa: E501
        ).json()

        current = json["current"]
        previous = json["previous"]

        def flatten_dict(data, prefix=""):
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
    def get_as_reports(self, date, verbose=False):
        """Get Ancillary Services Reports.

        Published with a 2 day delay around 3am central
        """
        report_date = date.normalize() + pd.DateOffset(days=2)

        doc = self._get_document(
            report_type_id=TWO_DAY_ANCILLARY_SERVICES_REPORTS_RTID,
            date=report_date,
            verbose=verbose,
        )

        return self._handle_as_reports_file(doc.url, verbose=verbose)

    def _handle_as_reports_file(self, file_path, verbose, **kwargs):
        z = utils.get_zip_folder(file_path, verbose=verbose, **kwargs)

        # extract the date from the file name
        date_str = z.namelist()[0][-13:-4]

        self_arranged_products = [
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
        cleared_products = [
            "RRSPFR",
            "RRSUFR",
            "RRSFFR",
            "ECRSM",
            "ECRSS",
            "REGUP",
            "REGDN",
            "NSPIN",
        ]
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

        # Some of these produces are not in earlier data
        exclude_products = [
            "ECRSM",
            "ECRSS",
            "NSPNM",
            "RRSFFR",
            "RRSUFR",
            "RRSPFR",
        ]

        prefix = "2d"

        # Earlier prefixes are 48h
        if z.namelist()[0].split("_")[0] == "48h":
            prefix = "48h"

        all_dfs = []
        for as_name in cleared_products:
            suffix = f"{as_name}-{date_str}.csv"
            cleared = f"{prefix}_Cleared_DAM_AS_{suffix}"

            if as_name in exclude_products and cleared not in z.namelist():
                continue

            df_cleared = pd.read_csv(z.open(cleared))
            all_dfs.append(df_cleared)

        for as_name in self_arranged_products:
            suffix = f"{as_name}-{date_str}.csv"
            self_arranged = f"{prefix}_Self_Arranged_AS_{suffix}"

            if as_name in exclude_products and self_arranged not in z.namelist():
                continue

            df_self_arranged = pd.read_csv(z.open(self_arranged))
            all_dfs.append(df_self_arranged)

        def _make_bid_curve(df):
            return [
                list(x)
                for x in df[["MW Offered", f"{as_name} Offer Price"]].values.tolist()
            ]

        for as_name in offers_products:
            suffix = f"{as_name}-{date_str}.csv"
            offers = f"{prefix}_Agg_AS_Offers_{suffix}"

            if as_name in exclude_products and offers not in z.namelist():
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

    @support_date_range("DAY_START")
    def get_dam_system_lambda(self, date, end=None, verbose=False):
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

    def _handle_dam_system_lambda_file(self, doc, verbose):
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
    def get_sced_system_lambda(self, date, end=None, verbose=False):
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

    def _handle_sced_timestamp(self, df, verbose=False):
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

    def _handle_sced_system_lambda(self, docs, verbose):
        all_dfs = []
        for doc in tqdm.tqdm(
            docs,
            desc="Reading SCED System Lambda files",
            disable=not verbose,
        ):
            log(f"Reading {doc.url}", verbose)
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
    def get_highest_price_as_offer_selected(self, date, verbose=False):
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

    def _handle_three_day_highest_price_as_offer_selected_file(self, doc, verbose):
        df = self.read_doc(doc, verbose=verbose)

        df = df.rename(
            columns={
                "Resource Name with Highest-Priced Offer Selected in DAM and SASMs": "Resource Name",  # noqa: E501
                # Older data
                "Resource Name with Highest-Priced Offer Selected in DAM": "Resource Name",  # noqa: E501
            },
        )

        def _handle_offers(df):
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

    def get_dam_price_corrections(self, dam_type, verbose=False):
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

    def get_rtm_price_corrections(self, rtm_type, verbose=False):
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

    def _handle_price_corrections(self, docs, verbose=False):
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
    def get_system_wide_actual_load(self, date, end=None, verbose=False):
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
    def get_short_term_system_adequacy(self, date, end=None, verbose=False):
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

    def _handle_short_term_system_adequacy_file(self, doc, verbose=False):
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
            },
        )

        return df

    @support_date_range(frequency=None)
    def get_real_time_adders_and_reserves(self, date, end=None, verbose=False):
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

    def _handle_real_time_adders_and_reserves_docs(self, docs, verbose=False):
        df = self.read_docs(docs, parse=False, verbose=verbose)
        df = self._handle_sced_timestamp(df)

        df = utils.move_cols_to_front(
            df,
            ["SCED Timestamp", "Interval Start", "Interval End", "BatchID"],
        )

        df = df.rename(columns={"SystemLambda": "System Lambda"})

        return df.sort_values("SCED Timestamp")

    @support_date_range(frequency=None)
    def get_temperature_forecast_by_weather_zone(self, date, end=None, verbose=False):
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

    def _handle_temperature_forecast_by_weather_zone_docs(self, docs, verbose=False):
        # Process files in a loop to add the publish time for each doc
        df = pd.concat(
            [
                self.read_doc(doc, verbose=verbose).assign(
                    **{"Publish Time": doc.publish_date}
                )
                for doc in docs
            ],
        )

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
        start,
        end,
        report_type_id,
        handle_doc,
        extension,
        verbose=False,
    ):
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

    def _handle_json_data(self, df, columns):
        df["Time"] = (
            pd.to_datetime(df["epoch"], unit="ms")
            .dt.tz_localize("UTC")
            .dt.tz_convert(self.default_timezone)
        )

        cols_to_keep = ["Time"] + list(columns.keys())
        return df[cols_to_keep].rename(columns=columns)

    def _get_settlement_point_mapping(self, verbose=False):
        """Get DataFrame whose columns can help us filter out values"""

        doc_info = self._get_document(
            report_type_id=SETTLEMENT_POINTS_LIST_AND_ELECTRICAL_BUSES_MAPPING_RTID,
            extension=None,
            verbose=verbose,
        )
        doc_url = doc_info.url

        msg = f"Fetching {doc_url}"
        log(msg, verbose)

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
    ):
        logger.debug(f"Reading {doc.url}")

        if request_kwargs:
            response = requests.get(doc.url, **(request_kwargs or {})).content
            df = pd.read_csv(
                io.BytesIO(response), compression="zip", **(read_csv_kwargs or {})
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
    ):
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
    ):
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

                # Not there will be a repeated hour and Pandas can infer
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

        # todo try to clean up this logic
        doc = doc[cols_to_keep]
        doc = doc.drop(
            columns=["DeliveryDate", ending_time_col_name],
        )

        optional_drop = ["DSTFlag", "DeliveryInterval"]

        for col in optional_drop:
            if col in doc.columns:
                doc = doc.drop(columns=[col])

        return doc

    def _weather_zone_column_name_mapping(self):
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

    def _weather_zone_column_name_order(self):
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

    def _handle_indicative_lmp_by_settlement_point(self, df: pd.DataFrame):
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
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
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
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
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
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
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

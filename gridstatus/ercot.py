import io
from dataclasses import dataclass
from enum import Enum
from zipfile import ZipFile

import pandas as pd
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
    NotSupported,
)
from gridstatus.decorators import ercot_update_dates, support_date_range
from gridstatus.ercot_60d_utils import (
    process_dam_gen,
    process_dam_load,
    process_dam_load_as_offers,
    process_sced_gen,
    process_sced_load,
)
from gridstatus.gs_logging import log
from gridstatus.lmp_config import lmp_config

LOCATION_TYPE_HUB = "Trading Hub"
LOCATION_TYPE_RESOURCE_NODE = "Resource Node"
LOCATION_TYPE_ZONE = "Load Zone"
LOCATION_TYPE_ZONE_EW = "Load Zone Energy Weighted"
LOCATION_TYPE_ZONE_DC = "Load Zone DC Tie"
LOCATION_TYPE_ZONE_DC_EW = "Load Zone DC Tie Energy Weighted"

"""
Report Type IDs
"""
# DAM Clearing Prices for Capacity
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-188-CD
DAM_CLEARING_PRICES_FOR_CAPACITY_RTID = 12329

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

# Historical DAM Clearing Prices for Capacity
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-181-ER
HISTORICAL_DAM_CLEARING_PRICES_FOR_CAPACITY_RTID = 13091

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

# Solar Power Production - Hourly Averaged Actual and Forecasted Values by Geographical Region # noqa
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-745-CD
SOLAR_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_BY_GEOGRAPHICAL_REGION_RTID = (  # noqa
    21809  # noqa
)

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
    AS_PRICES_HISTORICAL_MAX_DAYS = 30

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

        if date != "latest":
            date_parsed = utils._handle_date(date, tz=self.default_timezone)
            check_yesterday = date_parsed + pd.DateOffset(days=1)
            if not (
                utils.is_today(date, tz=self.default_timezone)
                or utils.is_today(check_yesterday, tz=self.default_timezone)
            ):
                raise NotSupported()

        url = self.BASE + "/fuel-mix.json"
        data = self._get_json(url, verbose=verbose)

        dfs = []
        for day in data["data"].keys():
            df = (
                pd.DataFrame(data["data"][day])
                .applymap(
                    lambda x: x["gen"],
                    na_action="ignore",
                )
                .T
            )
            dfs.append(df)

        mix = pd.concat(dfs)
        mix.index.name = "Time"
        mix = mix.reset_index()

        mix["Time"] = pd.to_datetime(mix["Time"]).dt.tz_localize(
            self.default_timezone,
            ambiguous="infer",
        )

        # most timestamps are a few seconds off round 5 minute ticks
        # round to nearest minute
        mix["Time"] = mix["Time"].round("min")

        mix = mix[
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
            ]
        ]

        if date == "latest":
            return mix

        # return where date_parsed matches mix["Time"]

        return mix[mix["Time"].dt.date == date_parsed.date()].reset_index(drop=True)

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
        if utils.is_today(date, tz=self.default_timezone):
            df = self._get_weather_zone_load_html(date, verbose=verbose)
        else:
            doc_info = self._get_document(
                report_type_id=ACTUAL_SYSTEM_LOAD_BY_WEATHER_ZONE,
                date=date + pd.DateOffset(days=1),  # published day after
                constructed_name_contains="csv.zip",
                verbose=verbose,
            )

            df = self.read_doc(doc_info, verbose=verbose)
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
        if utils.is_today(date, tz=self.default_timezone):
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

        df["Interval End"] = pd.to_datetime(df["Oper Day"]) + (
            df["Hour Ending"] / 100
        ).astype("timedelta64[h]")
        df["Interval End"] = df["Interval End"].dt.tz_localize(
            self.default_timezone,
        )
        df["Interval Start"] = df["Interval End"] - pd.DateOffset(hours=1)
        df["Time"] = df["Interval Start"]

        df = utils.move_cols_to_front(
            df,
            [
                "Time",
                "Interval Start",
                "Interval End",
            ],
        )

        to_drop = ["Oper Day", "Hour Ending"]
        df = df.drop(to_drop, axis=1)

        return df

    def _get_todays_outlook_non_forecast(self, date, verbose=False):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        assert date == "latest" or utils.is_today(
            date,
            self.default_timezone,
        ), "Only today's data is supported"
        url = self.BASE + "/supply-demand.json"

        msg = f"Fetching {url}"
        log(msg, verbose)

        r = self._get_json(url)

        date = pd.to_datetime(r["lastUpdated"][:10], format="%Y-%m-%d")

        data = pd.DataFrame(r["data"])

        data["Interval End"] = (
            date
            + data["hourEnding"].astype("timedelta64[h]")
            + data["interval"].astype("timedelta64[m]")
        )

        data["Interval End"] = data["Interval End"].dt.tz_localize(
            self.default_timezone,
            ambiguous="infer",
        )

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

    @support_date_range("HOUR_START")
    def get_load_forecast(
        self,
        date,
        forecast_type=ERCOTSevenDayLoadForecastReport.BY_FORECAST_ZONE,
        verbose=False,
    ):
        """Returns load forecast of specified regions.

        Only allows datetimes from today's date.

        Returns hourly report published immediately before the datetime.

        Arguments:
            date (str): datetime to download.
                Returns report published most recently before datetime.
            forecast_type (ERCOTSevenDayLoadForecastReport): The load forecast type.
                Enum of possible values.
            verbose (bool, optional): print verbose output. Defaults to False.
        """
        if date == "latest":
            pass
        else:
            if not utils.is_today(date, self.default_timezone):
                raise NotSupported()

        doc = self._get_document(
            report_type_id=forecast_type.value,
            published_before=None if date == "latest" else date,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )

        df = self._handle_load_forecast(
            doc,
            forecast_type=forecast_type,
            verbose=verbose,
        )

        return df

    def _handle_load_forecast(self, doc, forecast_type, verbose=False):
        """
        Function to handle the different types of load forecast parsing.

        """
        df = self.read_doc(doc, verbose=verbose)

        if forecast_type in [
            ERCOTSevenDayLoadForecastReport.BY_FORECAST_ZONE,
            ERCOTSevenDayLoadForecastReport.BY_WEATHER_ZONE,
            ERCOTSevenDayLoadForecastReport.BY_MODEL_AND_WEATHER_ZONE,
        ]:
            df["Load Forecast"] = df["SystemTotal"].copy()
        df["Forecast Time"] = doc.publish_date

        df = utils.move_cols_to_front(
            df,
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Forecast Time",
            ],
        )

        return df

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
        df["Market"] = Markets.REAL_TIME_15_MIN.value
        return self._finalize_spp_df(df, verbose=verbose)

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
        df["Market"] = Markets.DAY_AHEAD_HOURLY.value
        return self._finalize_spp_df(df, verbose=verbose)

    def get_interconnection_queue(self, verbose=False):
        """
        Get interconnection queue for ERCOT

        Monthly historical data available here:
            http://mis.ercot.com/misapp/GetReports.do?reportTypeId=15933&reportTitle=GIS%20Report&showHTMLView=&mimicKey
        """  # noqa

        doc_info = self._get_document(
            report_type_id=GIS_REPORT_RTID,
            constructed_name_contains="GIS_Report",
            verbose=verbose,
        )

        # TODO other sheets for small projects, inactive, and cancelled project
        # TODO see if this data matches up with summaries in excel file
        # TODO historical data available as well

        msg = f"Downloading interconnection queue from: {doc_info.url} "
        log(msg, verbose)

        # skip rows and handle header
        queue = pd.read_excel(
            doc_info.url,
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

    @lmp_config(
        supports={
            Markets.REAL_TIME_15_MIN: ["latest", "today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["latest", "today", "historical"],
        },
    )
    @support_date_range(frequency="DAY_START")
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
        if market == Markets.REAL_TIME_15_MIN:
            df = self._get_spp_rtm15(
                date,
                verbose,
            )
        elif market == Markets.DAY_AHEAD_HOURLY:
            df = self._get_spp_dam(date, verbose)

        return self._finalize_spp_df(
            df,
            locations=locations,
            location_type=location_type,
            verbose=verbose,
        )

    def _finalize_spp_df(self, df, locations=None, location_type=None, verbose=False):
        df = df.rename(
            columns={
                "SettlementPoint": "Location",
                "Settlement Point": "Location",
                "SettlementPointName": "Location",
                "Settlement Point Name": "Location",
            },
        )

        mapping_df = self._get_settlement_point_mapping(verbose=verbose)
        resource_node = mapping_df["RESOURCE_NODE"].dropna().unique()

        # if df[df.duplicated()].shape[0] > 0:
        #     import pdb
        #     pdb.set_trace()

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
        df["Location Type"].fillna(LOCATION_TYPE_RESOURCE_NODE, inplace=True)

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

    def _get_spp_dam(
        self,
        date,
        verbose=False,
    ):
        """Get day-ahead hourly Market SPP data for ERCOT"""
        if date == "latest":
            raise ValueError(
                "DAM is released daily, so use date='today' instead",
            )

        publish_date = utils._handle_date(date, self.default_timezone)
        # adjust for DAM since it's published a day ahead
        publish_date = publish_date.normalize() - pd.DateOffset(days=1)
        doc_info = self._get_document(
            report_type_id=DAM_SETTLEMENT_POINT_PRICES_RTID,
            date=publish_date,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )

        msg = f"Fetching {doc_info.url}"
        log(msg, verbose)

        df = self.read_doc(doc_info, verbose=verbose)

        # fetch mapping
        # todo move to finalize?
        df["Market"] = Markets.DAY_AHEAD_HOURLY.value

        return df

    def _get_spp_rtm15(
        self,
        date,
        verbose=False,
    ):
        """Get Real-time 15-minute Market SPP data for ERCOT

        https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD
        """
        # returns list of Document(url=,publish_date=)

        all_docs = self._get_documents(
            report_type_id=SETTLEMENT_POINT_PRICES_AT_RESOURCE_NODES_HUBS_AND_LOAD_ZONES_RTID,
            extension="csv",
            verbose=verbose,
        )

        docs = self._filter_spp_rtm_files(
            all_docs=all_docs,
            date=date,
        )
        if len(docs) == 0:
            raise ValueError(f"Could not fetch SPP data for {date}")

        all_dfs = []
        for doc_info in tqdm.tqdm(docs, disable=not verbose):
            doc_url = doc_info.url
            msg = f"Fetching {doc_url}"
            log(msg, verbose)
            df = self.read_doc(doc_info, verbose=verbose)
            all_dfs.append(df)

        df = pd.concat(all_dfs).reset_index(drop=True)

        df["Market"] = Markets.REAL_TIME_15_MIN.value
        return df

    def _filter_spp_rtm_files(self, all_docs, date):
        if date == "latest":
            # just pluck out the latest document based on publish_date
            return [max(all_docs, key=lambda x: x.publish_date)]
        query_date_str = date.strftime("%Y%m%d")
        docs = []
        for doc in all_docs:
            # make sure to handle retry files
            # e.g SPPHLZNP6905_retry_20230608_1545_csv
            if "SPPHLZNP6905_" not in doc.constructed_name:
                continue

            if query_date_str + "_0000" in doc.constructed_name:
                continue

            if (
                query_date_str in doc.constructed_name
                or f"{(date + pd.Timedelta(days=1)).strftime('%Y%m%d')}_0000"  # noqa: E501
                in doc.constructed_name
            ):
                docs.append(doc)

        return docs

    @support_date_range(frequency="1Y", update_dates=ercot_update_dates)
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

        Source:
            https://www.ercot.com/mp/data-products/data-product-details?id=NP4-181-ER
        """

        # use to check if we need to pull daily files
        if (
            date.date()
            >= (
                pd.Timestamp.now(tz=self.default_timezone)
                - pd.DateOffset(days=self.AS_PRICES_HISTORICAL_MAX_DAYS)
            ).date()
        ):
            return self._get_as_prices_recent(date, end=end)
        elif not end:
            end = date

        doc_info = self._get_document(
            report_type_id=HISTORICAL_DAM_CLEARING_PRICES_FOR_CAPACITY_RTID,
            constructed_name_contains=f"{date.year}.zip",
            verbose=verbose,
        )
        doc = self.read_doc(doc_info, verbose=verbose)

        doc = self._finalize_as_price_df(doc)

        max_date = doc.Time.max().date()

        df_list = [doc]

        # if last df date is less than our specified end
        # date, pull the remaining days. Will only be applicable
        # if end date is within today - 3days
        if max_date < end.date():
            df_list.append(
                self._get_as_prices_recent(
                    start=max_date,
                    end=end,
                ),
            )

        # join, sort, filter and reset data index
        data = pd.concat(df_list).sort_values(by="Interval Start")

        data = (
            data.loc[
                (data.Time.dt.date >= date.date()) & (data.Time.dt.date <= end.date())
            ]
            .drop_duplicates(subset=["Interval Start"])
            .reset_index(drop=True)
        )

        return data

    @support_date_range("DAY_START")
    def _get_as_prices_recent(self, date, verbose=False):
        """Get ancillary service clearing prices in hourly intervals in Day
            Ahead Market. This function can return the last 31 days
            of ancillary pricing.

        Arguments:
            date (datetime.date, str): date of delivery for AS services

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

        data = self._finalize_as_price_df(
            doc,
            pivot=True,
        )

        return data

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
                df[time_col] = df[time_col].dt.tz_localize(
                    self.default_timezone,
                    ambiguous=df["Repeated Hour Flag"] == "Y",
                )
                interval_start = df[time_col].dt.round(
                    "15min",
                    ambiguous=df["Repeated Hour Flag"] == "Y",
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

        load_resource = handle_time(load_resource, time_col="SCED Time Stamp")
        gen_resource = handle_time(gen_resource, time_col="SCED Time Stamp")
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
            "sced_load_resource": load_resource,
            "sced_gen_resource": gen_resource,
            "sced_smne": smne,
        }

    @support_date_range("DAY_START")
    def get_60_day_dam_disclosure(self, date, end=None, process=False, verbose=False):
        """Get 60 day DAM Disclosure data"""

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

    def _handle_60_day_dam_disclosure(self, z, process=False, verbose=False):
        files_prefix = {
            "dam_gen_resource": "60d_DAM_Gen_Resource_Data-",
            "dam_gen_resource_as_offers": "60d_DAM_Generation_Resource_ASOffers-",  # noqa: E501
            "dam_load_resource": "60d_DAM_Load_Resource_Data-",
            "dam_load_resource_as_offers": "60d_DAM_Load_Resource_ASOffers-",  # noqa: E501
            "dam_energy_bids": "60d_DAM_EnergyBids-",
            "dam_energy_bid_awards": "60d_DAM_EnergyBidAwards-",  # noqa: E501
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
            # weird that these files dont have this column like all other eroct files
            # add so we can parse
            doc["DSTFlag"] = "N"
            data[key] = self.parse_doc(doc)

        if process:
            data["dam_gen_resource"] = process_dam_gen(
                data["dam_gen_resource"],
            )

            data["dam_load_resource"] = process_dam_load(
                data["dam_load_resource"],
            )

            data["dam_load_resource_as_offers"] = process_dam_load_as_offers(
                data["dam_load_resource_as_offers"],
            )

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

        df.insert(
            0,
            "Time",
            pd.to_datetime(time_text).tz_localize(self.default_timezone),
        )

        return df

    @support_date_range("HOUR_START")
    def get_hourly_wind_report(self, date, end=None, verbose=False):
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
        doc = self._get_document(
            report_type_id=WIND_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_RTID,
            published_before=None if date == "latest" else date,
            extension="csv",
            verbose=verbose,
        )

        df = self._handle_hourly_wind_or_solar_report(doc, verbose=verbose)

        return df

    @support_date_range("HOUR_START")
    def get_hourly_solar_report(self, date, end=None, verbose=False):
        """Get Hourly Solar Report.

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

        doc = self._get_document(
            report_type_id=SOLAR_POWER_PRODUCTION_HOURLY_AVERAGED_ACTUAL_AND_FORECASTED_VALUES_BY_GEOGRAPHICAL_REGION_RTID,
            published_before=None if date == "latest" else date,
            extension="csv",
            verbose=verbose,
        )

        df = self._handle_hourly_wind_or_solar_report(doc, verbose=verbose)

        return df

    def _handle_hourly_wind_or_solar_report(self, doc, verbose=False):
        df = self.read_doc(doc, verbose=verbose)
        df.insert(
            0,
            "Publish Time",
            pd.to_datetime(doc.publish_date).tz_convert(self.default_timezone),
        )
        # replace _ in column names with spaces
        df.columns = df.columns.str.replace("_", " ")
        return df

    @support_date_range("HOUR_START")
    def get_hourly_resource_outage_capacity(self, date, end=None, verbose=False):
        """Hourly Resource Outage Capacity report sourced
        from the Outage Scheduler (OS).

        Returns outage data for for next 7 days.

        Total Resource MW doesn't include IRR, New Equipment outages,
        retirement of old equipment, seasonal
        mothballed (during the outage season),
        and mothballed.

        As such, it is a proxy for thermal outages.

        Arguments:
            date (str): time to download. Returns last hourly report
                before this time. Supports "latest"
            end (str, optional): end time to download. Defaults to None.
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with hourly resource outage capacity data


        """
        doc = self._get_document(
            report_type_id=HOURLY_RESOURCE_OUTAGE_CAPACITY_RTID,
            extension="csv",
            published_before=None if date == "latest" else date,
            verbose=verbose,
        )

        df = self._handle_hourly_resource_outage_capacity(doc, verbose=verbose)

        return df

    def _handle_hourly_resource_outage_capacity(self, doc, verbose=False):
        df = self.read_doc(doc, verbose=verbose)
        df.insert(
            0,
            "Publish Time",
            pd.to_datetime(doc.publish_date).tz_convert(self.default_timezone),
        )

        outage_types = ["Total Resource", "Total IRR", "Total New Equip Resource"]

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
        return df

    @support_date_range("DAY_START")
    def get_unplanned_resource_outages(self, date, verbose=False):
        """Get Unplanned Resource Outages.

        Data published at ~5am central on the 3rd day after the day of interest.

        Arguments:
            date (str, datetime): date to get data for
            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with unplanned resource outages

        """
        doc = self._get_document(
            report_type_id=UNPLANNED_RESOURCE_OUTAGES_REPORT_RTID,
            date=date.normalize() + pd.DateOffset(days=3),
            verbose=verbose,
        )

        xls = utils.get_zip_file(doc.url, verbose=verbose)

        df = self._handle_unplanned_resource_outages_file(xls)

        return df

    def _handle_unplanned_resource_outages_file(self, xls):
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

        df.insert(0, "Report Time", as_of)

        time_cols = ["Actual Outage Start", "Planned End Date", "Actual End Date"]
        for col in time_cols:
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(self.default_timezone)

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

    def _handle_as_reports_file(self, file_path, verbose):
        z = utils.get_zip_folder(file_path, verbose=verbose)

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

        all_dfs = []
        for as_name in cleared_products:
            suffix = f"{as_name}-{date_str}.csv"
            cleared = f"2d_Cleared_DAM_AS_{suffix}"

            if as_name in ["ECRSM", "ECRSS"] and cleared not in z.namelist():
                continue

            df_cleared = pd.read_csv(z.open(cleared))
            all_dfs.append(df_cleared)

        for as_name in self_arranged_products:
            suffix = f"{as_name}-{date_str}.csv"
            self_arranged = f"2d_Self_Arranged_AS_{suffix}"

            if as_name in ["ECRSM", "ECRSS"] and self_arranged not in z.namelist():
                continue

            df_self_arranged = pd.read_csv(z.open(self_arranged))
            all_dfs.append(df_self_arranged)

        def _make_bid_curve(df):
            return [
                tuple(x)
                for x in df[["MW Offered", f"{as_name} Offer Price"]].values.tolist()
            ]

        for as_name in offers_products:
            suffix = f"{as_name}-{date_str}.csv"
            offers = f"2d_Agg_AS_Offers_{suffix}"

            if as_name in ["ECRSM", "ECRSS"] and offers not in z.namelist():
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
                    .apply(_make_bid_curve)
                    .reset_index(name=name)
                )
            all_dfs.append(df_offers_hourly)

        df = pd.concat(
            [df.set_index(["Delivery Date", "Hour Ending"]) for df in all_dfs],
            axis=1,
        ).reset_index()

        return self.parse_doc(df, verbose=verbose)

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
            )
            .apply(_handle_offers)
            .reset_index()
        )

        return df

    def _get_document(
        self,
        report_type_id,
        date=None,
        published_before=None,
        constructed_name_contains=None,
        extension=None,
        verbose=False,
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
            published_before=published_before,
            constructed_name_contains=constructed_name_contains,
            extension=extension,
            verbose=verbose,
        )
        if len(documents) == 0:
            raise ValueError(
                f"No document found for {report_type_id} on {date}",
            )

        return max(documents, key=lambda x: x.publish_date)

    def _get_documents(
        self,
        report_type_id,
        date=None,
        published_before=None,
        constructed_name_contains=None,
        extension=None,
        verbose=False,
    ) -> list:
        """Searches by Report Type ID, filtering for date and/or constructed name

        Returns:
            list of Document with URL and Publish Date
        """
        url = f"https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId={report_type_id}"  # noqa

        msg = f"Fetching document {url}"
        log(msg, verbose)

        docs = self._get_json(url)["ListDocsByRptTypeRes"]["DocumentList"]
        matches = []
        for doc in docs:
            match = True

            publish_date = pd.Timestamp(doc["Document"]["PublishDate"]).tz_convert(
                self.default_timezone,
            )

            if published_before:
                match = match and publish_date <= published_before

            if date:
                match = match and publish_date.date() == date.date()

            if extension:
                match = match and doc["Document"]["FriendlyName"].endswith(extension)

            if constructed_name_contains:
                match = (
                    match
                    and constructed_name_contains in doc["Document"]["ConstructedName"]
                )

            if match:
                doc_id = doc["Document"]["DocID"]
                url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}"  # noqa
                matches.append(
                    Document(
                        url=url,
                        publish_date=publish_date,
                        constructed_name=doc["Document"]["ConstructedName"],
                        friendly_name=doc["Document"]["FriendlyName"],
                    ),
                )

        return matches

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

    def read_doc(self, doc, verbose=False):
        log(f"Reading {doc.url}", verbose)
        doc = pd.read_csv(doc.url, compression="zip")
        return self.parse_doc(doc, verbose=verbose)

    def parse_doc(self, doc, verbose=False):
        # files sometimes have different naming conventions
        # a more elegant solution would be nice
        doc.rename(
            columns={
                "Delivery Date": "DeliveryDate",
                "DELIVERY_DATE": "DeliveryDate",
                "OperDay": "DeliveryDate",
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

        # i think DeliveryInterval only shows up
        # in 15 minute data along with DeliveryHour
        if "DeliveryInterval" in original_cols:
            interval_length = pd.Timedelta(minutes=15)

            doc["HourBeginning"] = doc["HourEnding"] - 1

            doc["Interval Start"] = (
                pd.to_datetime(doc["DeliveryDate"])
                + doc["HourBeginning"].astype("timedelta64[h]")
                + ((doc["DeliveryInterval"] - 1) * interval_length)
            )

        else:
            interval_length = pd.Timedelta(hours=1)
            doc["HourBeginning"] = (
                doc["HourEnding"]
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

        ambiguous = "infer"
        if "DSTFlag" in doc.columns:
            ambiguous = doc["DSTFlag"] == "Y"

        try:
            doc["Interval Start"] = doc["Interval Start"].dt.tz_localize(
                self.default_timezone,
                ambiguous=ambiguous,
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
            columns=[
                "DeliveryDate",
                "HourEnding",
            ],
        )

        optional_drop = ["DSTFlag", "DeliveryInterval"]

        for col in optional_drop:
            if col in doc.columns:
                doc = doc.drop(columns=[col])

        return doc


if __name__ == "__main__":
    iso = Ercot()

    df = iso.get_rtm_spp(2011)

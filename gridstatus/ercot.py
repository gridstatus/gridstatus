import io
from dataclasses import dataclass
from zipfile import ZipFile

import pandas as pd
import requests
import tqdm
from bs4 import BeautifulSoup

from gridstatus import utils
from gridstatus.base import (
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NotSupported,
)
from gridstatus.decorators import ercot_update_dates, support_date_range
from gridstatus.gs_logging import log
from gridstatus.lmp_config import lmp_config

LOCATION_TYPE_HUB = "Trading Hub"
LOCATION_TYPE_RESOURCE_NODE = "Resource Node"
LOCATION_TYPE_ZONE = "Load Zone"

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

# Seven-Day Load Forecast by Forecast Zone
# https://www.ercot.com/mp/data-products/data-product-details?id=NP3-560-CD
SEVEN_DAY_LOAD_FORECAST_BY_FORECAST_ZONE_RTID = 12311

# Actual System Load by Weather Zone
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-345-CD
ACTUAL_SYSTEM_LOAD_BY_WEATHER_ZONE = 13101

# Actual System Load by Forecast Zone
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-346-CD
ACTUAL_SYSTEM_LOAD_BY_FORECAST_ZONE = 14836

# Historical DAM Clearing Prices for Capacity
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-181-ER
HISTORICAL_DAM_CLEARING_PRICES_FOR_CAPACITY_RTID = 13091

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

    @dataclass
    class Document:
        url: str
        publish_date: pd.Timestamp
        constructed_name: str
        friendly_name: str

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

        # keep today's data only
        data = data[
            data["Interval Start"].dt.normalize()
            == pd.Timestamp(
                date,
            )
            .tz_localize(self.default_timezone)
            .normalize()
        ]
        data = data[data["forecast"] == 0]  # only keep non forecast rows

        return data.reset_index(drop=True)

    def get_load_forecast(self, date, verbose=False):
        """Returns load forecast

        Currently only supports today's forecast
        """
        if not utils.is_today(date, self.default_timezone):
            raise NotSupported()

        # intrahour https://www.ercot.com/mp/data-products/data-product-details?id=NP3-562-CD
        # there are a few days of historical date for the forecast
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        doc_info = self._get_document(
            report_type_id=SEVEN_DAY_LOAD_FORECAST_BY_FORECAST_ZONE_RTID,
            date=today,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )

        doc = self.read_doc(doc_info, verbose=verbose)

        doc = doc.rename(columns={"SystemTotal": "Load Forecast"})
        doc["Forecast Time"] = doc_info.publish_date

        doc = doc[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Forecast Time",
                "Load Forecast",
            ]
        ]

        return doc

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
        is_resource_node = df["Location"].isin(resource_node)

        # Assign location types based on the boolean masks
        df.loc[is_hub, "Location Type"] = LOCATION_TYPE_HUB
        df.loc[is_load_zone, "Location Type"] = LOCATION_TYPE_ZONE
        df.loc[is_resource_node, "Location Type"] = LOCATION_TYPE_RESOURCE_NODE

        # If a location type is not found, default to LOCATION_TYPE_RESOURCE_NODE
        df["Location Type"].fillna(LOCATION_TYPE_RESOURCE_NODE, inplace=True)

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

        # todo figure out why
        # when you get rid of SettlementPointType some
        # rows are duplicated
        # For example, SettlementPointType LZ and LZEW
        df = df.drop_duplicates(
            subset=[
                "Time",
                "Interval Start",
                "Interval End",
                "Location",
            ],
        )

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

        print(data)

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
                "Real-Time Data - Total System Capacity (not including Ancillary Services)": "Total System Capacity (excluding Ancillary Services)",  # noqa: E501
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

    def _get_document(
        self,
        report_type_id,
        date=None,
        constructed_name_contains=None,
        verbose=False,
    ) -> Document:
        """Searches by Report Type ID, filtering for date and/or constructed name

        Raises a ValueError if no document matches

        Returns:
            Latest Document by publish_date
        """
        documents = self._get_documents(
            report_type_id=report_type_id,
            date=date,
            constructed_name_contains=constructed_name_contains,
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
                    self.Document(
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
        doc = pd.read_csv(doc.url, compression="zip")
        return self.parse_doc(doc, verbose=verbose)

    def parse_doc(self, doc, verbose=False):
        # files sometimes have different naming conventions
        # a more elegant solution would be nice
        doc.rename(
            columns={
                "Delivery Date": "DeliveryDate",
                "OperDay": "DeliveryDate",
                "Hour Ending": "HourEnding",
                "Repeated Hour Flag": "DSTFlag",
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

        doc["Interval Start"] = doc["Interval Start"].dt.tz_localize(
            self.default_timezone,
            ambiguous=doc["DSTFlag"] == "Y",
        )

        doc["Interval End"] = doc["Interval Start"] + interval_length

        doc["Time"] = doc["Interval Start"]

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
                "DSTFlag",
            ],
        )
        if "DeliveryInterval" in doc.columns:
            doc = doc.drop(columns=["DeliveryInterval"])

        return doc


if __name__ == "__main__":
    iso = Ercot()

    df = iso.get_rtm_spp(2011)

import io
from dataclasses import dataclass
from zipfile import ZipFile

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import (
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NotSupported,
)
from gridstatus.decorators import ercot_update_dates, support_date_range
from gridstatus.lmp_config import lmp_config
from gridstatus.logging import log

LOCATION_TYPE_HUB = "HUB"
LOCATION_TYPE_NODE = "NODE"
LOCATION_TYPE_ZONE = "ZONE"

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

# Settlement Points List and Electrical Buses Mapping
# https://www.ercot.com/mp/data-products/data-product-details?id=NP4-160-SG
SETTLEMENT_POINTS_LIST_AND_ELECTRICAL_BUSES_MAPPING_RTID = 10008

# Settlement Point Prices at Resource Nodes, Hubs and Load Zones
# https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD
SETTLEMENT_POINT_PRICES_AT_RESOURCE_NODES_HUBS_AND_LOAD_ZONES_RTID = 12301

# Seven-Day Load Forecast by Forecast Zone
# https://www.ercot.com/mp/data-products/data-product-details?id=NP3-560-CD
SEVEN_DAY_LOAD_FORECAST_BY_FORECAST_ZONE_RTID = 12311

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
        LOCATION_TYPE_NODE,
        LOCATION_TYPE_ZONE,
    ]

    BASE = "https://www.ercot.com/api/1/services/read/dashboards"
    ACTUAL_LOADS_URL_FORMAT = "https://www.ercot.com/content/cdr/html/{timestamp}_actual_loads_of_forecast_zones.html"  # noqa
    LOAD_HISTORICAL_MAX_DAYS = 14
    AS_PRICES_HISTORICAL_MAX_DAYS = 30

    @dataclass
    class Document:
        url: str
        publish_date: pd.Timestamp

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
            date (datetime.date, str): "latest", "today".
                historical data currently not supported

            verbose(bool): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with columns; Time and columns for each fuel \
                type (solar and wind)
        """

        if date == "latest":
            df = self.get_fuel_mix("today")
            return df.tail(1).reset_index(drop=True)

        # todo: can also support yesterday
        elif utils.is_today(date, tz=self.default_timezone):
            date = utils._handle_date(date, tz=self.default_timezone)
            url = self.BASE + "/fuel-mix.json"
            r = self._get_json(url, verbose=verbose)

            today_str = date.strftime("%Y-%m-%d")
            mix = (
                pd.DataFrame(r["data"][today_str])
                .applymap(
                    lambda x: x["gen"],
                    na_action="ignore",
                )
                .T
            )
            mix.index.name = "Interval End"
            mix = mix.reset_index()

            mix["Interval End"] = pd.to_datetime(mix["Interval End"]).dt.tz_localize(
                self.default_timezone,
                ambiguous="infer",
            )

            # most timestamps are a few seconds off round 5 minute ticks
            # round to nearest minute
            mix["Interval End"] = mix["Interval End"].round("min")
            mix["Interval Start"] = mix["Interval End"] - pd.Timedelta(minutes=5)
            mix["Time"] = mix["Interval Start"]

            mix = mix[
                [
                    "Time",
                    "Interval Start",
                    "Interval End",
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

            return mix

        else:
            raise NotSupported()

    @support_date_range("1D")
    def get_load(self, date, verbose=False):
        if date == "latest":
            today_load = self.get_load("today", verbose=verbose)
            latest = today_load.iloc[-1]
            return {"load": latest["Load"], "time": latest["Time"]}

        elif utils.is_today(date, tz=self.default_timezone):
            df = self._get_todays_outlook_non_forecast(date, verbose=verbose)
            df = df.rename(columns={"demand": "Load"})
            return df[["Time", "Interval Start", "Interval End", "Load"]]

        elif utils.is_within_last_days(
            date,
            self.LOAD_HISTORICAL_MAX_DAYS,
            tz=self.default_timezone,
        ):
            df = self._get_load_html(date, verbose)
            return df[["Time", "Interval Start", "Interval End", "Load"]]

        else:
            raise NotSupported()

    def _get_load_html(self, when, verbose=False):
        """Returns load for currentDay or previousDay"""
        url = self.ACTUAL_LOADS_URL_FORMAT.format(
            timestamp=when.strftime("%Y%m%d"),
        )

        msg = f"Fetching {url}"
        log(msg, verbose)

        dfs = pd.read_html(url, header=0)
        df = dfs[0]
        df = self._handle_html_data(df, {"TOTAL": "Load"})
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

    def get_rtm_spp(self, year):
        """Get Historical RTM Settlement Point Prices(SPPs)
            for each of the Hubs and Load Zones

        Arguments:
            year(int): year to get data for

        Source:
            https://www.ercot.com/mp/data-products/data-product-details?id=NP6-785-ER
        """  # noqa
        doc_info = self._get_document(
            report_type_id=HISTORICAL_RTM_LOAD_ZONE_AND_HUB_PRICES_RTID,
            constructed_name_contains=f"{year}.zip",
            verbose=True,
        )

        x = utils.get_zip_file(doc_info.url)
        all_sheets = pd.read_excel(x, sheet_name=None)
        df = pd.concat(all_sheets.values())
        return df

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
    @support_date_range(frequency="1D")
    def get_spp(
        self,
        date,
        end=None,
        market: str = None,
        locations: list = "ALL",
        location_type: str = LOCATION_TYPE_ZONE,
        verbose=False,
    ):
        """Get SPP data for ERCOT

        Supported Markets:
            - ``REAL_TIME_15_MIN``
            - ``DAY_AHEAD_HOURLY``

        Supported Location Types:
            - ``zone``
            - ``hub``
            - ``node``
        """
        if market == Markets.REAL_TIME_15_MIN:
            df = self._get_spp_rtm15(
                date,
                location_type,
                verbose,
            )
            settlement_point_field = "SettlementPointName"
        elif market == Markets.DAY_AHEAD_HOURLY:
            df = self._get_spp_dam(date, location_type, verbose)
            settlement_point_field = "SettlementPoint"
        else:
            raise NotSupported(
                f"Market {market} not supported for ERCOT",
            )
        return Ercot._finalize_spp_df(df, settlement_point_field, locations)

    def _get_spp_dam(
        self,
        date,
        location_type: str = None,
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
        df["Location Type"] = self._get_location_type_name(location_type)

        mapping_df = self._get_settlement_point_mapping(verbose=verbose)
        df = self._filter_by_location_type(df, mapping_df, location_type)

        return df

    @staticmethod
    def _finalize_spp_df(df, settlement_point_field, locations):
        """
        Finalizes DataFrame by:
        - filtering by locations list
        - renaming and ordering columns
        - and resetting the index

        Arguments:
            df (pandas.DataFrame): DataFrame with SPP data
            settlement_point_field (str): Field name of
                settlement point to rename to "Location"
        """
        df = df.rename(
            columns={
                "SettlementPointPrice": "SPP",
                settlement_point_field: "Location",
            },
        )
        df = utils.filter_lmp_locations(df, locations)
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
        df = df.sort_values(by="Interval Start")
        df = df.reset_index(drop=True)
        return df

    def _get_spp_rtm15(
        self,
        date,
        location_type: str = None,
        verbose=False,
    ):
        """Get Real-time 15-minute Market SPP data for ERCOT

        https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD
        """
        today = pd.Timestamp.now(tz=self.default_timezone).normalize()
        if date == "latest":
            publish_date = today
        else:
            publish_date = utils._handle_date(date, self.default_timezone)
        # returns list of Document(url=,publish_date=)
        docs = self._get_documents(
            report_type_id=SETTLEMENT_POINT_PRICES_AT_RESOURCE_NODES_HUBS_AND_LOAD_ZONES_RTID,
            date=publish_date,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )
        if date == "latest":
            # just pluck out the latest document based on publish_date
            docs = [max(docs, key=lambda x: x.publish_date)]
        if len(docs) == 0:
            raise ValueError(f"Could not fetch SPP data for {date}")

        all_dfs = []
        for doc_info in docs:
            doc_url = doc_info.url

            msg = f"Fetching {doc_url}"
            log(msg, verbose)
            df = self.read_doc(doc_info, verbose=verbose)
            all_dfs.append(df)
        df = pd.concat(all_dfs).reset_index(drop=True)
        df.drop

        df["Market"] = Markets.REAL_TIME_15_MIN.value
        df["Location Type"] = self._get_location_type_name(location_type)
        # Additional filter as the document may contain the last 15 minutes of yesterday
        df = df[df["Interval Start"].dt.date == publish_date.date()]
        df = self._filter_by_settlement_point_type(df, location_type)

        return df

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
                "Regulation Up", "Regulation Down", "Responsive Reserves".

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

    @support_date_range("1D")
    def _get_as_prices_recent(self, date, verbose=False):
        """Get ancillary service clearing prices in hourly intervals in Day
            Ahead Market. This function is can return the last 31 days
            of ancillary pricing.

        Arguments:
            date (datetime.date, str): date of delivery for AS services

            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:

            pandas.DataFrame: A DataFrame with prices for "Non-Spinning Reserves", \
                "Regulation Up", "Regulation Down", "Responsive Reserves".

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

        # NSPIN  REGDN  REGUP  RRS
        rename = {
            "NSPIN": "Non-Spinning Reserves",
            "REGDN": "Regulation Down",
            "REGUP": "Regulation Up",
            "RRS": "Responsive Reserves",
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
        ]

        doc.rename(columns=rename, inplace=True)

        return doc[col_order]

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

    def _handle_html_data(self, df, columns):
        df["Interval End"] = pd.to_datetime(df["Oper Day"]) + (
            df["Hour Ending"] / 100
        ).astype("timedelta64[h]")
        df["Interval End"] = df["Interval End"].dt.tz_localize(
            self.default_timezone,
        )
        df["Interval Start"] = df["Interval End"] - pd.DateOffset(hours=1)
        df["Time"] = df["Interval Start"]

        cols_to_keep = [
            "Time",
            "Interval Start",
            "Interval End",
        ] + list(columns.keys())
        return df[cols_to_keep].rename(columns=columns)

    def _filter_by_settlement_point_type(self, df, location_type):
        """Filter by settlement point type"""
        norm_location_type = location_type.upper()
        if norm_location_type == LOCATION_TYPE_NODE:
            df = df[
                df["SettlementPointType"].isin(
                    RESOURCE_NODE_SETTLEMENT_TYPES,
                )
            ]
        elif norm_location_type == LOCATION_TYPE_ZONE:
            df = df[df["SettlementPointType"].isin(LOAD_ZONE_SETTLEMENT_TYPES)]
        elif norm_location_type == LOCATION_TYPE_HUB:
            df = df[df["SettlementPointType"].isin(HUB_SETTLEMENT_TYPES)]
        else:
            raise ValueError(f"Invalid location_type: {location_type}")
        return df

    def _get_location_type_name(self, location_type):
        norm_location_type = location_type.upper()
        if norm_location_type == LOCATION_TYPE_NODE:
            return "Node"
        elif norm_location_type == LOCATION_TYPE_ZONE:
            return "Zone"
        elif norm_location_type == LOCATION_TYPE_HUB:
            return "Hub"
        else:
            raise ValueError(f"Invalid location_type: {location_type}")

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

    def _filter_by_location_type(self, df, mapping_df, location_type):
        """Filter by location type"""
        norm_location_type = location_type.upper()
        if norm_location_type == LOCATION_TYPE_NODE:
            valid_values = mapping_df["RESOURCE_NODE"].unique()
        elif norm_location_type == LOCATION_TYPE_ZONE:
            valid_values = mapping_df["SETTLEMENT_LOAD_ZONE"].unique()
        elif norm_location_type == LOCATION_TYPE_HUB:
            valid_values = mapping_df["HUB"].unique()
        else:
            raise ValueError(f"Invalid location_type: {location_type}")

        return df[df["SettlementPoint"].isin(valid_values)]

    def read_doc(self, doc, verbose=False):
        doc = pd.read_csv(doc.url, compression="zip")

        # files sometimes have different naming conventions
        # a more elegant solution would be nice
        doc.rename(
            columns={
                "Delivery Date": "DeliveryDate",
                "Hour Ending": "HourEnding",
                "Repeated Hour Flag": "DSTFlag",
                "DeliveryHour": "HourEnding",
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
            doc["Interval End"] = (
                pd.to_datetime(doc["DeliveryDate"])
                + doc["HourEnding"].astype("timedelta64[h]")
                + (doc["DeliveryInterval"] * interval_length)
            )

        else:
            interval_length = pd.Timedelta(hours=1)
            doc["Interval End"] = pd.to_datetime(doc["DeliveryDate"]) + (
                doc["HourEnding"].str.split(":").str[0].astype(int)
            ).astype("timedelta64[h]")

            # if there is a DST skip, add an hour to the previous row
            # for example, data has 2022-03-13 02:00:00,
            # but that should be 2022-03-13 03:00:00
            dst_skip_hour = doc[doc["Interval End"].diff() == pd.Timedelta(hours=2)]
            for i in dst_skip_hour.index:
                doc.loc[i - 1, "Interval End"] = doc.loc[
                    i - 1,
                    "Interval End",
                ] + pd.DateOffset(
                    hours=1,
                )  # noqa

        doc["Interval End"] = doc["Interval End"].dt.tz_localize(
            self.default_timezone,
            ambiguous=doc["DSTFlag"] == "Y",
        )

        doc["Interval Start"] = doc["Interval End"] - interval_length
        doc["Time"] = doc["Interval Start"]

        cols_to_keep = [
            "Time",
            "Interval Start",
            "Interval End",
        ] + original_cols

        # todo try to clean up this logic
        doc = doc[cols_to_keep]
        doc.drop(
            columns=[
                "DeliveryDate",
                "HourEnding",
                "DSTFlag",
            ],
            inplace=True,
        )
        if "DeliveryInterval" in doc.columns:
            doc.drop(columns=["DeliveryInterval"], inplace=True)

        return doc


if __name__ == "__main__":
    iso = Ercot()
    iso.get_fuel_mix("latest")

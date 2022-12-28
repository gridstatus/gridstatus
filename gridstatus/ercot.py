import io
import sys
from dataclasses import dataclass
from zipfile import ZipFile

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import (
    FuelMix,
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NotSupported,
)
from gridstatus.decorators import support_date_range

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
"""
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
    ACTUAL_LOADS_URL_FORMAT = "https://www.ercot.com/content/cdr/html/{timestamp}_actual_loads_of_forecast_zones.html"
    LOAD_HISTORICAL_MAX_DAYS = 14

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
            date(datetime or str): "latest", "today". historical data currently not supported

            verbose(bool): print verbose output. Defaults to False.

        Returns:
            pd.Dataframe: dataframe with columns: Time and columns for each fuel type (solar and wind)
        """

        if date == "latest":
            df = self.get_fuel_mix("today")
            latest = df.iloc[-1].to_dict()
            time = latest.pop("Time")
            return FuelMix(time=time, mix=latest, iso=self.name)

        # todo: can also support yesterday
        elif utils.is_today(date):
            date = utils._handle_date(date, tz=self.default_timezone)
            url = self.BASE + "/fuel-mix.json"
            r = self._get_json(url, verbose=verbose)

            today_str = date.strftime("%Y-%m-%d")

            mix = (
                pd.DataFrame(r["data"][today_str])
                .applymap(
                    lambda x: x["gen"],
                )
                .T
            )
            mix.index.name = "Time"
            mix = mix.reset_index()

            mix["Time"] = pd.to_datetime(mix["Time"]).dt.tz_localize(
                self.default_timezone,
                ambiguous="infer",
            )

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

            return mix

        else:
            raise NotSupported()

    @support_date_range("1D")
    def get_load(self, date, verbose=False):
        if date == "latest":
            today_load = self.get_load("today", verbose=verbose)
            latest = today_load.iloc[-1]
            return {"load": latest["Load"], "time": latest["Time"]}

        elif utils.is_today(date):
            df = self._get_todays_outlook_non_forecast(date, verbose=verbose)
            df = df.rename(columns={"demand": "Load"})
            return df[["Time", "Load"]]

        elif utils.is_within_last_days(date, self.LOAD_HISTORICAL_MAX_DAYS):
            return self._get_load_html(date)

        else:
            raise NotSupported()

    def _get_load_json(self, when):
        """Returns load for currentDay or previousDay"""
        # todo:
        # even more historical data. up to month back i think: https://www.ercot.com/mp/data-products/data-product-details?id=NP6-346-CD
        # hourly load archives: https://www.ercot.com/gridinfo/load/load_hist
        url = self.BASE + "/loadForecastVsActual.json"
        r = self._get_json(url)
        df = pd.DataFrame(r[when]["data"])
        df = df.dropna(subset=["systemLoad"])
        df = self._handle_json_data(df, {"systemLoad": "Load"})
        return df

    def _get_load_html(self, when):
        """Returns load for currentDay or previousDay"""
        url = self.ACTUAL_LOADS_URL_FORMAT.format(
            timestamp=when.strftime("%Y%m%d"),
        )
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
        url = self.BASE + "/todays-outlook.json"
        if verbose:
            print(f"Fetching {url}", file=sys.stderr)
        r = self._get_json(url)

        date = pd.to_datetime(r["lastUpdated"][:10], format="%Y-%m-%d")

        # ignore last row since that corresponds to midnight following day
        data = pd.DataFrame(r["data"][:-1])

        data["Time"] = pd.to_datetime(
            date.strftime("%Y-%m-%d")
            + " "
            + data["hourEnding"].astype(str).str.zfill(2)
            + ":"
            + data["interval"].astype(str).str.zfill(2),
        ).dt.tz_localize(self.default_timezone, ambiguous="infer")

        data = data[data["forecast"] == 0]  # only keep non forecast rows

        return data

    def get_load_forecast(self, date, verbose=False):
        """Returns load forecast

        Currently only supports today's forecast
        """
        if date != "today":
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
        doc = pd.read_csv(doc_info.url, compression="zip")

        doc["Time"] = pd.to_datetime(
            doc["DeliveryDate"]
            + " "
            + (doc["HourEnding"].str.split(":").str[0].astype(int) - 1)
            .astype(str)
            .str.zfill(2)
            + ":00",
        ).dt.tz_localize(self.default_timezone, ambiguous="infer")

        doc = doc.rename(columns={"SystemTotal": "Load Forecast"})
        doc["Forecast Time"] = doc_info.publish_date

        doc = doc[["Forecast Time", "Time", "Load Forecast"]]

        return doc

    @support_date_range("1D")
    def get_as_prices(self, date, verbose=False):
        """Get ancillary service clearing prices in hourly intervals in Day Ahead Market

        Arguments:
            date(datetime or str): date of delivery for AS services
            verbose(bool): print verbose output. Defaults to False.

        Returns:
            pd.Dataframe: dataframe with prices for "Non-Spinning Reserves", "Regulation Up", "Regulation Down", "Responsive Reserves",

        """
        # subtract one day since it's the day ahead market happens on the day before for the delivery day
        date = date - pd.Timedelta("1D")

        doc_info = self._get_document(
            report_type_id=DAM_CLEARING_PRICES_FOR_CAPACITY_RTID,
            date=date,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )

        if verbose:
            print("Downloading {}".format(doc_info.url))

        doc = pd.read_csv(doc_info.url, compression="zip")

        doc["Time"] = pd.to_datetime(
            doc["DeliveryDate"]
            + " "
            + (doc["HourEnding"].str.split(":").str[0].astype(int) - 1)
            .astype(str)
            .str.zfill(2)
            + ":00",
        ).dt.tz_localize(self.default_timezone, ambiguous=doc["DSTFlag"] == "Y")

        doc["Market"] = "DAM"

        # NSPIN  REGDN  REGUP    RRS
        rename = {
            "NSPIN": "Non-Spinning Reserves",
            "REGDN": "Regulation Down",
            "REGUP": "Regulation Up",
            "RRS": "Responsive Reserves",
        }
        data = (
            doc.pivot_table(
                index=["Time", "Market"],
                columns="AncillaryType",
                values="MCPC",
            )
            .rename(columns=rename)
            .reset_index()
        )

        data.columns.name = None

        return data

    def get_rtm_spp(self, year):
        """Get Historical RTM Settlement Point Prices(SPPs) for each of the Hubs and Load Zones

        Arguments:
            year(int): year to get data for

        Source: https: // www.ercot.com/mp/data-products/data-product-details?id = NP6-785-ER
        """
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
        """Get interconnection queue for ERCOT

        Monthly historical data available here: http: // mis.ercot.com/misapp/GetReports.do?reportTypeId = 15933 & reportTitle = GIS % 20Report & showHTMLView = &mimicKey
        """

        doc_info = self._get_document(
            report_type_id=GIS_REPORT_RTID,
            constructed_name_contains="GIS_Report",
            verbose=verbose,
        )

        # TODO other sheets for small projects, inactive, and cancelled project
        # TODO see if this data matches up with summaries in excel file
        # TODO historical data available as well

        if verbose:
            print("Downloading interconnection queue from: ", doc_info.url)

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

        # todo: there are a few columns being parsed as "unamed" that aren't being included but should
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
            # todo the actual complettion date can be calculated by looking at status and other date columns
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

        Supported Markets: REAL_TIME_15_MIN, DAY_AHEAD_HOURLY

        Supported Location Types: "zone", "hub", "node"
        """
        assert market is not None, "market must be specified"
        market = Markets(market)

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
        publish_date = publish_date - pd.Timedelta("1D")
        doc_info = self._get_document(
            report_type_id=DAM_SETTLEMENT_POINT_PRICES_RTID,
            date=publish_date,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )
        if verbose:
            print(f"Fetching {doc_info.url}", file=sys.stderr)
        df = pd.read_csv(doc_info.url, compression="zip")

        # fetch mapping
        df["Market"] = Markets.DAY_AHEAD_HOURLY.value
        df["Location Type"] = self._get_location_type_name(location_type)

        mapping_df = self._get_settlement_point_mapping(verbose=verbose)
        df = self._filter_by_location_type(df, mapping_df, location_type)

        df["Time"] = Ercot._parse_delivery_date_hour_ending(
            df,
            self.default_timezone,
        )
        return df

    @staticmethod
    def _finalize_spp_df(df, settlement_point_field, locations):
        """
        Finalizes DataFrame by:
        - filtering by locations list
        - renaming and ordering columns
        - and resetting the index

        Parameters:
            df (DataFrame): DataFrame with SPP data
            settlement_point_field (str): Field name of settlement point to rename to "Location"
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
                "Location",
                "Time",
                "Market",
                "Location Type",
                "SPP",
            ]
        ]
        df = df.reset_index(drop=True)
        return df

    @staticmethod
    def _parse_delivery_date_hour_ending(df, timezone):
        return pd.to_datetime(
            df["DeliveryDate"]
            + " "
            + (df["HourEnding"].str.split(":").str[0].astype(int) - 1)
            .astype(str)
            .str.zfill(2)
            + ":00",
        ).dt.tz_localize(timezone, ambiguous="infer")

    @staticmethod
    def _parse_delivery_date_hour_interval(df, timezone):
        return pd.to_datetime(
            df["DeliveryDate"]
            + "T"
            + (df["DeliveryHour"].astype(int) - 1).astype(str).str.zfill(2)
            + ":"
            + ((df["DeliveryInterval"].astype(int) - 1) * 15).astype(str).str.zfill(2),
            format="%m/%d/%YT%H:%M",
        ).dt.tz_localize(timezone, ambiguous="infer")

    @staticmethod
    def _parse_oper_day_hour_ending(df, timezone):
        return pd.to_datetime(
            df["Oper Day"] + "T"
            # Hour ending starts at 100 ("1:00") so we offset by -1 hour,
            # and zero fill to 4 characters, so strptime can parse it correctly
            + (df["Hour Ending"].astype(int) - 100).astype(str).str.zfill(4),
            format="%m/%d/%YT%H%M",
        ).dt.tz_localize(timezone, ambiguous="infer")

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
            if verbose:
                print(f"Fetching {doc_url}", file=sys.stderr)
            df = pd.read_csv(doc_url, compression="zip")
            all_dfs.append(df)
        df = pd.concat(all_dfs).reset_index(drop=True)

        df["Market"] = Markets.REAL_TIME_15_MIN.value
        df["Location Type"] = self._get_location_type_name(location_type)
        df["Time"] = Ercot._parse_delivery_date_hour_interval(
            df,
            self.default_timezone,
        )
        # Additional filter as the document may contain the last 15 minutes of yesterday
        df = df[df["Time"].dt.date == publish_date.date()]
        df = self._filter_by_settlement_point_type(df, location_type)

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
        verbose=False,
    ) -> list:
        """Searches by Report Type ID, filtering for date and/or constructed name

        Returns:
             list of Document with URL and Publish Date
        """
        url = f"https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId={report_type_id}"
        if verbose:
            print(f"Fetching document {url}", file=sys.stderr)
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
                url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}"
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
        df["Time"] = Ercot._parse_oper_day_hour_ending(
            df,
            self.default_timezone,
        )
        cols_to_keep = ["Time"] + list(columns.keys())
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
        """Get dataframe whose columns can help us filter out values"""

        doc_info = self._get_document(
            report_type_id=SETTLEMENT_POINTS_LIST_AND_ELECTRICAL_BUSES_MAPPING_RTID,
            verbose=verbose,
        )
        doc_url = doc_info.url
        if verbose:
            print(f"Fetching {doc_url}", file=sys.stderr)

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


if __name__ == "__main__":
    iso = Ercot()
    iso.get_fuel_mix("latest")

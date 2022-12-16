import io
import sys
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
DAM_CLEARING_PRICES_FOR_CAPACITY_RTID = 12329
DAM_SETTLEMENT_POINT_PRICES_RTID = 12331
GIS_REPORT_RTID = 15933
HISTORICAL_RTM_LOAD_ZONE_AND_HUB_PRICES_RTID = 13061
SETTLEMENT_POINTS_LIST_AND_ELECTRICAL_BUSES_MAPPING_RTID = 10008
SETTLEMENT_POINT_PRICES_AT_RESOURCE_NODES_HUBS_AND_LOAD_ZONES_RTID = 12301
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

    BASE = "https://www.ercot.com/api/1/services/read/dashboards"
    ACTUAL_LOADS_URL_FORMAT = "https://www.ercot.com/content/cdr/html/{timestamp}_actual_loads_of_forecast_zones.html"
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
            date = utils._handle_date(date)
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
            d = self._get_load_json("currentDay").iloc[-1]

            return {"time": d["Time"], "load": d["Load"]}

        elif utils.is_today(date):
            return self._get_load_json("currentDay")

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

    def _get_supply(self, date, verbose=False):
        """Returns most recent data point for supply in MW

        Updates every 5 minutes
        """
        assert date == "today", "Only today's data is supported"
        url = self.BASE + "/todays-outlook.json"
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

        data = data[["Time", "capacity"]].rename(
            columns={"capacity": "Supply"},
        )

        return data

    def get_load_forecast(self, date, verbose=False):
        """Returns load forecast

        Currently only supports today's forecast
        """
        if date != "today":
            raise NotSupported()

        # intrahour https://www.ercot.com/mp/data-products/data-product-details?id=NP3-562-CD
        # there are a few days of historical date for the forecast
        today = pd.Timestamp(pd.Timestamp.now(tz=self.default_timezone).date())
        doc_url, publish_date = self._get_document(
            report_type_id=SEVEN_DAY_LOAD_FORECAST_BY_FORECAST_ZONE_RTID,
            date=today,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )
        doc = pd.read_csv(doc_url, compression="zip")

        doc["Time"] = pd.to_datetime(
            doc["DeliveryDate"]
            + " "
            + (doc["HourEnding"].str.split(":").str[0].astype(int) - 1)
            .astype(str)
            .str.zfill(2)
            + ":00",
        ).dt.tz_localize(self.default_timezone, ambiguous="infer")

        doc = doc.rename(columns={"SystemTotal": "Load Forecast"})
        doc["Forecast Time"] = publish_date

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

        doc_url, date = self._get_document(
            report_type_id=DAM_CLEARING_PRICES_FOR_CAPACITY_RTID,
            date=date,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )

        if verbose:
            print("Downloading {}".format(doc_url))

        doc = pd.read_csv(doc_url, compression="zip")

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
        doc_url, date = self._get_document(
            report_type_id=HISTORICAL_RTM_LOAD_ZONE_AND_HUB_PRICES_RTID,
            constructed_name_contains=f"{year}.zip",
            verbose=True,
        )

        x = utils.get_zip_file(doc_url)
        all_sheets = pd.read_excel(x, sheet_name=None)
        df = pd.concat(all_sheets.values())
        return df

    def get_interconnection_queue(self, verbose=False):
        """Get interconnection queue for ERCOT

        Monthly historical data available here: http: // mis.ercot.com/misapp/GetReports.do?reportTypeId = 15933 & reportTitle = GIS % 20Report & showHTMLView = &mimicKey
        """

        doc_url, date = self._get_document(
            report_type_id=GIS_REPORT_RTID,
            constructed_name_contains="GIS_Report",
            verbose=verbose,
        )

        # TODO other sheets for small projects, inactive, and cancelled project
        # TODO see if this data matches up with summaries in excel file
        # TODO historical data available as well

        if verbose:
            print("Downloading interconnection queue from: ", doc_url)

        # skip rows and handle header
        queue = pd.read_excel(
            doc_url,
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

    def get_lmp(
        self,
        date,
        end=None,
        market: str = None,
        locations: list = None,
        location_type: str = None,
        verbose=False,
    ):
        """Get LMP data for ERCOT

        Supported Markets: REAL_TIME_15_MIN, DAY_AHEAD_HOURLY

        Supported Location Types: "zone", "hub", "node"
        """
        if not (date == "latest" or utils.is_today(date)):
            raise NotSupported(f"date={date} is not supported for LMP")

        if locations is None:
            locations = "ALL"

        if location_type is None:
            location_type = "hub"

        assert market is not None, "market must be specified"
        market = Markets(market)

        unsupported = False
        if market == Markets.REAL_TIME_15_MIN:
            if date == "latest":
                return self._get_lmp_rtm15_latest(
                    locations,
                    location_type,
                    verbose,
                )
            elif utils.is_today(date):
                return self._get_lmp_rtm15_today(
                    locations,
                    location_type,
                    verbose,
                )
            else:
                unsupported = True
        elif market == Markets.DAY_AHEAD_HOURLY:
            if date == "latest":
                return self._get_lmp_dam_latest(locations, location_type, verbose)
            elif utils.is_today(date):
                return self._get_lmp_dam_today(locations, location_type, verbose)
            else:
                unsupported = True
        else:
            unsupported = True

        if unsupported:
            raise NotSupported(
                f"Market {market} and/or date {date} are not supported for ERCOT",
            )

    def _get_lmp_dam_latest(
        self,
        locations: list = None,
        location_type: str = None,
        verbose=False,
    ):
        """Gets today's data and filters all rows matching the maximum time"""
        df = self._get_lmp_dam_today(
            locations,
            location_type,
            verbose,
        )
        max_time = df["Time"].max()
        df = df[df["Time"] == max_time]
        return df

    def _get_lmp_dam_today(
        self,
        locations: list = None,
        location_type: str = None,
        verbose=False,
    ):
        """Get day-ahead hourly Market LMP data for ERCOT"""
        date = pd.Timestamp(pd.Timestamp.now(tz=self.default_timezone).date())
        doc_url, publish_date = self._get_document(
            report_type_id=DAM_SETTLEMENT_POINT_PRICES_RTID,
            date=date,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )
        if verbose:
            print(f"Fetching {doc_url}", file=sys.stderr)
        df = pd.read_csv(doc_url, compression="zip")

        # fetch mapping
        df["Market"] = Markets.DAY_AHEAD_HOURLY.value
        df["Location Type"] = self._get_location_type_name(location_type)

        mapping_df = self._get_settlement_point_mapping(verbose=verbose)
        df = self._filter_by_location_type(df, mapping_df, location_type)
        df = self._filter_by_locations(df, "SettlementPoint", locations)

        df["Time"] = pd.to_datetime(
            df["DeliveryDate"]
            + " "
            + (df["HourEnding"].str.split(":").str[0].astype(int) - 1)
            .astype(str)
            .str.zfill(2)
            + ":00",
        ).dt.tz_localize(self.default_timezone, ambiguous="infer")

        df = df.rename(
            columns={
                "SettlementPointPrice": "LMP",
                "SettlementPoint": "Location",
            },
        )

        df = df[
            [
                "Location",
                "Time",
                "Market",
                "Location Type",
                "LMP",
            ]
        ]

        df = df.reset_index(drop=True)
        return df

    def _get_lmp_rtm15_latest(
        self,
        locations: list = None,
        location_type: str = None,
        verbose=False,
    ):
        """Get Real-time 15-minute Market LMP data for ERCOT

        https://www.ercot.com/mp/data-products/data-product-details?id=NP6-788-CD
        """
        today = pd.Timestamp(pd.Timestamp.now(tz=self.default_timezone).date())
        doc_url, publish_date = self._get_document(
            report_type_id=SETTLEMENT_POINT_PRICES_AT_RESOURCE_NODES_HUBS_AND_LOAD_ZONES_RTID,
            date=today,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )
        df = pd.read_csv(doc_url, compression="zip")
        df["Market"] = Markets.REAL_TIME_15_MIN.value
        df["Location Type"] = self._get_location_type_name(location_type)

        df["Time"] = pd.to_datetime(
            df["DeliveryDate"]
            + "T"
            + (df["DeliveryHour"].astype(int) - 1).astype(str).str.zfill(2)
            + ":"
            + ((df["DeliveryInterval"].astype(int) - 1) * 15).astype(str).str.zfill(2),
            format="%m/%d/%YT%H:%M",
        ).dt.tz_localize(self.default_timezone)

        df = self._filter_by_settlement_point_type(df, location_type)
        df = self._filter_by_locations(df, "SettlementPointName", locations)

        df = df.rename(
            columns={
                "SettlementPointPrice": "LMP",
                "SettlementPointName": "Location",
            },
        )

        df = df[
            [
                "Location",
                "Time",
                "Market",
                "Location Type",
                "LMP",
            ]
        ]

        df = df.reset_index(drop=True)
        return df

    def _get_lmp_rtm15_today(
        self,
        locations: list = None,
        location_type: str = None,
        verbose=False,
    ):
        """Get Real-time 15-minute Market LMP data for ERCOT

        https://www.ercot.com/mp/data-products/data-product-details?id=NP6-905-CD
        """
        today = pd.Timestamp(pd.Timestamp.now(tz=self.default_timezone).date())
        doc_urls = self._get_documents(
            report_type_id=SETTLEMENT_POINT_PRICES_AT_RESOURCE_NODES_HUBS_AND_LOAD_ZONES_RTID,
            date=today,
            constructed_name_contains="csv.zip",
            verbose=verbose,
        )

        all_df = []
        for doc_url in doc_urls:
            if verbose:
                print(f"Fetching {doc_url}", file=sys.stderr)
            df = pd.read_csv(doc_url, compression="zip")
            all_df.append(df)
        df = pd.concat(all_df).reset_index(drop=True)

        df["Market"] = Markets.REAL_TIME_15_MIN.value
        df["Location Type"] = self._get_location_type_name(location_type)

        df["Time"] = pd.to_datetime(
            df["DeliveryDate"]
            + "T"
            + (df["DeliveryHour"].astype(int) - 1).astype(str).str.zfill(2)
            + ":"
            + ((df["DeliveryInterval"].astype(int) - 1) * 15).astype(str).str.zfill(2),
            format="%m/%d/%YT%H:%M",
        ).dt.tz_localize(self.default_timezone)

        # Additional filter as the document may contain the last 15 minutes of yesterday
        df = df[df["Time"].dt.date == today.date()]

        df = self._filter_by_settlement_point_type(df, location_type)
        df = self._filter_by_locations(df, "SettlementPointName", locations)

        df = df.rename(
            columns={
                "SettlementPointPrice": "LMP",
                "SettlementPointName": "Location",
            },
        )

        df = df[
            [
                "Location",
                "Time",
                "Market",
                "Location Type",
                "LMP",
            ]
        ]
        df = df.reset_index(drop=True)
        return df

    def _get_document(
        self,
        report_type_id,
        date=None,
        constructed_name_contains=None,
        verbose=False,
    ):
        """Get document for a given report type id and date. If multiple document published return the latest"""
        url = f"https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId={report_type_id}"
        if verbose:
            print(f"Fetching document {url}", file=sys.stderr)
        docs = self._get_json(url)["ListDocsByRptTypeRes"]["DocumentList"]
        match = []
        for d in docs:
            doc_date = pd.Timestamp(d["Document"]["PublishDate"]).tz_convert(
                self.default_timezone,
            )

            # check do we need to check if same timezone?
            if date and doc_date.date() != date.date():
                continue

            if (
                constructed_name_contains
                and constructed_name_contains not in d["Document"]["ConstructedName"]
            ):
                continue

            match.append((doc_date, d["Document"]["DocID"]))

        if len(match) == 0:
            raise ValueError(
                f"No document found for {report_type_id} on {date}",
            )

        doc = max(match, key=lambda x: x[0])
        url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc[1]}"
        return url, doc[0]

    def _get_documents(
        self,
        report_type_id,
        date=None,
        constructed_name_contains=None,
        verbose=False,
    ):
        """Return list of URLs for a given Report Type ID and date

        Note the filtering & exception handling differ from _get_document"""
        url = f"https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId={report_type_id}"
        if verbose:
            print(f"Fetching document {url}", file=sys.stderr)
        docs = self._get_json(url)["ListDocsByRptTypeRes"]["DocumentList"]
        matches = []
        for doc in docs:
            match = True

            if date:
                doc_date = pd.Timestamp(doc["Document"]["PublishDate"]).tz_convert(
                    self.default_timezone,
                )
                match = match and doc_date.date() == date.date()

            if constructed_name_contains:
                match = (
                    match
                    and constructed_name_contains in doc["Document"]["ConstructedName"]
                )

            if match:
                doc_id = doc["Document"]["DocID"]
                url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}"
                matches.append(url)
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
        df["Time"] = pd.to_datetime(
            df["Oper Day"] + "T"
            # Hour ending starts at 100 ("1:00") so we offset by -1 hour,
            # and zero fill to 4 characters, so strptime can parse it correctly
            + (df["Hour Ending"].astype(int) - 100).astype(str).str.zfill(4),
            format="%m/%d/%YT%H%M",
        ).dt.tz_localize(self.default_timezone)

        cols_to_keep = ["Time"] + list(columns.keys())
        return df[cols_to_keep].rename(columns=columns)

    def _filter_by_settlement_point_type(self, df, location_type):
        """Filter by settlement point type"""
        norm_location_type = location_type.upper()
        if norm_location_type == LOCATION_TYPE_NODE:
            df = df[df["SettlementPointType"].isin(RESOURCE_NODE_SETTLEMENT_TYPES)]
        elif norm_location_type == LOCATION_TYPE_ZONE:
            df = df[df["SettlementPointType"].isin(LOAD_ZONE_SETTLEMENT_TYPES)]
        elif norm_location_type == LOCATION_TYPE_HUB:
            df = df[df["SettlementPointType"].isin(HUB_SETTLEMENT_TYPES)]
        else:
            raise ValueError(f"Invalid location_type: {location_type}")
        return df

    def _filter_by_locations(self, df, field_name, locations):
        """Filter settlement point name by locations list"""
        if isinstance(locations, list):
            df = df[df[field_name].isin(locations)]
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

        report_type_id = SETTLEMENT_POINTS_LIST_AND_ELECTRICAL_BUSES_MAPPING_RTID
        url = f"https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId={report_type_id}"
        if verbose:
            print(f"Fetching document {url}", file=sys.stderr)
        docs = self._get_json(url)["ListDocsByRptTypeRes"]["DocumentList"]
        latest_doc = sorted(docs, key=lambda x: x["Document"]["PublishDate"])[-1]
        doc_id = latest_doc["Document"]["DocID"]
        doc_url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}"
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

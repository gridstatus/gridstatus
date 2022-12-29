import io
import sys
from urllib.parse import urlencode

import pandas as pd
import requests
import tqdm
from bs4 import BeautifulSoup

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

FS_RTBM_LMP_BY_LOCATION = "rtbm-lmp-by-location"
FS_DAM_LMP_BY_LOCATION = "da-lmp-by-location"
MARKETPLACE_BASE_URL = "https://marketplace.spp.org"
FILE_BROWSER_API_URL = "https://marketplace.spp.org/file-browser-api/"

LOCATION_TYPE_HUB = "HUB"
LOCATION_TYPE_INTERFACE = "INTERFACE"
LOCATION_TYPE_SETTLEMENT_LOCATION = "SETTLEMENT_LOCATION"

QUERY_RTM5_HUBS_URL = "https://pricecontourmap.spp.org/arcgis/rest/services/MarketMaps/RTBM_FeatureData/MapServer/1/query"
QUERY_RTM5_INTERFACES_URL = "https://pricecontourmap.spp.org/arcgis/rest/services/MarketMaps/RTBM_FeatureData/MapServer/2/query"


class SPP(ISOBase):
    """Southwest Power Pool (SPP)"""

    name = "Southwest Power Pool"
    iso_id = "spp"

    default_timezone = "US/Central"

    status_homepage = "https://www.spp.org/markets-operations/current-grid-conditions/"
    interconnection_homepage = (
        "https://www.spp.org/engineering/generator-interconnection/"
    )

    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.DAY_AHEAD_HOURLY,
    ]

    location_types = [
        LOCATION_TYPE_HUB,
        LOCATION_TYPE_INTERFACE,
        LOCATION_TYPE_SETTLEMENT_LOCATION,
    ]

    def get_status(self, date=None, verbose=False):
        if date != "latest":
            raise NotSupported()

        url = "https://www.spp.org/markets-operations/current-grid-conditions/"
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, "html.parser")
        conditions_element = soup.find("h1")
        last_update_time = conditions_element.findNextSibling("p").text[14:-1]
        status_text = (
            conditions_element.findNextSibling(
                "p",
            )
            .findNextSibling("p")
            .text
        )

        date_str = last_update_time[: last_update_time.index(", at ")]
        if "a.m." in last_update_time:
            time_str = last_update_time[
                last_update_time.index(", at ") + 5 : last_update_time.index(" a.m.")
            ]
            hour, minute = map(int, time_str.split(":"))
        elif "p.m." in last_update_time:
            time_str = last_update_time[
                last_update_time.index(", at ") + 5 : last_update_time.index(" p.m.")
            ]
            hour, minute = map(int, time_str.split(":"))
            if hour < 12:
                hour += 12
        else:
            raise "Cannot parse time of status"

        date_obj = (
            pd.to_datetime(date_str)
            .replace(
                hour=hour,
                minute=minute,
            )
            .tz_localize(self.default_timezone)
        )

        if (
            status_text
            == "SPP is currently in Normal Operations with no effective advisories or alerts."
        ):
            status = "Normal"
            notes = [status_text]
        else:
            status = status_text
            notes = None

        return GridStatus(
            time=date_obj,
            status=status,
            notes=notes,
            reserves=None,
            iso=self,
        )

    def get_fuel_mix(self, date, verbose=False):

        if date != "latest":
            raise NotSupported

        url = "https://marketplace.spp.org/chart-api/gen-mix/asChart"
        r = self._get_json(url, verbose=verbose)["response"]

        data = {"Timestamp": r["labels"]}
        data.update((d["label"], d["data"]) for d in r["datasets"])

        historical_mix = pd.DataFrame(data)

        current_mix = historical_mix.iloc[-1].to_dict()

        time = pd.Timestamp(
            current_mix.pop("Timestamp"),
        ).tz_convert(self.default_timezone)

        return FuelMix(time=time, mix=current_mix, iso=self.name)

    def get_load(self, date, verbose=False):
        """Returns load for last 24hrs in 5 minute intervals"""

        if date == "latest":
            return self._latest_from_today(self.get_load)

        elif utils.is_today(date):
            df = self._get_load_and_forecast(verbose=verbose)

            df = df.dropna(subset=["Actual Load"])

            df = df.rename(columns={"Actual Load": "Load"})

            df = df[["Time", "Load"]]

            return df

        else:
            raise NotSupported()

    def get_load_forecast(self, date, forecast_type="MID_TERM", verbose=False):
        """

        type (str): MID_TERM is hourly for next 7 days or SHORT_TERM is every five minutes for a few hours
        """
        df = self._get_load_and_forecast(verbose=verbose)

        # gives forecast from before current day
        # only include forecasts start at current day
        last_actual = df.dropna(subset=["Actual Load"])["Time"].max()
        current_day = last_actual.replace(hour=0, minute=0)

        current_day_forecast = df[df["Time"] > current_day].copy()

        # assume forecast is made at last actual
        current_day_forecast["Forecast Time"] = last_actual

        if forecast_type == "MID_TERM":
            forecast_col = "Mid-Term Forecast"
        elif forecast_type == "SHORT_TERM":
            forecast_col = "Short-Term Forecast"
        else:
            raise RuntimeError("Invalid forecast type")

        # there will be empty rows regardless of forecast type since they dont align
        current_day_forecast = current_day_forecast.dropna(
            subset=[forecast_col],
        )

        current_day_forecast = current_day_forecast[
            ["Forecast Time", "Time", forecast_col]
        ].rename({forecast_col: "Load Forecast"}, axis=1)

        return current_day_forecast

    def _get_load_and_forecast(self, verbose=False):
        url = "https://marketplace.spp.org/chart-api/load-forecast/asChart"

        if verbose:
            print("Getting load and forecast from {}".format(url))

        r = self._get_json(url)["response"]

        data = {"Time": r["labels"]}
        for d in r["datasets"][:3]:
            if d["label"] == "Actual Load":
                data["Actual Load"] = d["data"]
            elif d["label"] == "Mid-Term Load Forecast":
                data["Mid-Term Forecast"] = d["data"]
            elif d["label"] == "Short-Term Load Forecast":
                data["Short-Term Forecast"] = d["data"]

        df = pd.DataFrame(data)

        df["Time"] = pd.to_datetime(
            df["Time"],
        ).dt.tz_convert(self.default_timezone)

        return df

        # todo where does date got in argument order
        # def get_historical_lmp(self, date, market: str, nodes: list):
        # 5 minute interal data
        # https://marketplace.spp.org/file-browser-api/download/rtbm-lmp-by-location?path=/2022/08/By_Interval/08/RTBM-LMP-SL-202208082125.csv

        # hub and interface prices
        # https://marketplace.spp.org/pages/hub-and-interface-prices

        # historical generation mix
        # https://marketplace.spp.org/pages/generation-mix-rolling-365
        # https://marketplace.spp.org/chart-api/gen-mix-365/asFile
        # 15mb file with five minute resolution

    def get_interconnection_queue(self, verbose=False):
        """Get interconnection queue

        Returns:
            pd.DataFrame: Interconnection queue


        """
        url = "https://opsportal.spp.org/Studies/GenerateActiveCSV"
        if verbose:
            print("Getting interconnection queue from {}".format(url))

        queue = pd.read_csv(url, skiprows=1)

        queue["Status (Original)"] = queue["Status"]

        queue["Status"] = queue["Status"].map(
            {
                "IA FULLY EXECUTED/COMMERCIAL OPERATION": InterconnectionQueueStatus.COMPLETED.value,
                "IA FULLY EXECUTED/ON SCHEDULE": InterconnectionQueueStatus.COMPLETED.value,
                "IA FULLY EXECUTED/ON SUSPENSION": InterconnectionQueueStatus.COMPLETED.value,
                "IA PENDING": InterconnectionQueueStatus.ACTIVE.value,
                "DISIS STAGE": InterconnectionQueueStatus.ACTIVE.value,
                "None": InterconnectionQueueStatus.ACTIVE.value,
            },
        )

        queue["Generation Type"] = queue[["Generation Type", "Fuel Type"]].apply(
            lambda x: " - ".join(x.dropna()),
            axis=1,
        )

        queue["Proposed Completion Date"] = queue["Commercial Operation Date"]

        rename = {
            "Generation Interconnection Number": "Queue ID",
            " Nearest Town or County": "County",
            "State": "State",
            "TO at POI": "Transmission Owner",
            "Capacity": "Capacity (MW)",
            "MAX Summer MW": "Summer Capacity (MW)",
            "MAX Winter MW": "Winter Capacity (MW)",
            "Generation Type": "Generation Type",
            "Request Received": "Queue Date",
            "Substation or Line": "Interconnection Location",
        }

        # todo: there are a few columns being parsed as "unamed" that aren't being included but should
        extra_columns = [
            "In-Service Date",
            "Commercial Operation Date",
            "Cessation Date",
            "Current Cluster",
            "Cluster Group",
            "Replacement Generator Commercial Op Date",
            "Service Type",
        ]

        missing = [
            "Project Name",
            "Interconnecting Entity",
            "Withdrawn Date",
            "Withdrawal Comment",
            "Actual Completion Date",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        return queue

    @support_date_range(frequency="1D")
    def get_lmp(
        self,
        date,
        end=None,
        market: str = None,
        locations: list = "ALL",
        location_type: str = LOCATION_TYPE_HUB,
        verbose=False,
    ):
        """Get LMP data

        Supported Markets: REAL_TIME_5_MIN, DAY_AHEAD_HOURLY

        Supported Location Types: "hub", "interface", "settlement_location"
        """
        market = Markets(market)
        if market not in self.markets:
            raise NotSupported(f"Market {market} not supported")
        location_type = self._normalize_location_type(location_type)
        if market == Markets.REAL_TIME_5_MIN:
            df = self._get_rtm5_lmp(
                date,
                end,
                market,
                locations,
                location_type,
                verbose,
            )
        elif market == Markets.DAY_AHEAD_HOURLY:
            df = self._get_dam_lmp(
                date,
                end,
                market,
                locations,
                location_type,
                verbose,
            )
        else:
            raise NotSupported(
                f"Market {market} is not supported",
            )

        return self._finalize_spp_df(
            df,
            market=market,
            locations=locations,
            location_type=location_type,
            verbose=verbose,
        )

    def _get_feature_data(self, base_url, verbose=False):
        """Fetches data from ArcGIS Map Service with Feature Data

        Returns:
            pd.DataFrame of features
        """
        args = {
            "f": "json",
            "where": "OBJECTID IS NOT NULL",
            "returnGeometry": "false",
            "outFields": "*",
        }
        doc = self._get_json(base_url, params=args, verbose=verbose)
        df = pd.DataFrame([feature["attributes"] for feature in doc["features"]])
        return df

    def _get_rtm5_lmp(
        self,
        date,
        end=None,
        market: str = None,
        locations: list = "ALL",
        location_type: str = LOCATION_TYPE_HUB,
        verbose=False,
    ):
        df = self._fetch_and_concat_csvs(
            self._fs_get_rtbm_lmp_by_location_paths(date, verbose=verbose),
            fs_name=FS_RTBM_LMP_BY_LOCATION,
            verbose=verbose,
        )
        df["Location"] = df["Settlement Location"]
        df["Time"] = SPP._parse_gmt_interval_end(
            df,
            pd.Timedelta(minutes=5),
            self.default_timezone,
        )
        return df

    def _get_dam_lmp(
        self,
        date,
        end=None,
        market: str = None,
        locations: list = "ALL",
        location_type: str = LOCATION_TYPE_HUB,
        verbose=False,
    ):
        df = self._fetch_and_concat_csvs(
            self._fs_get_dam_lmp_by_location_paths(date, verbose=verbose),
            fs_name=FS_DAM_LMP_BY_LOCATION,
            verbose=verbose,
        )
        df["Location"] = df["Settlement Location"]
        df["Time"] = SPP._parse_gmt_interval_end(
            df,
            pd.Timedelta(minutes=5),
            self.default_timezone,
        )
        return df

    def _finalize_spp_df(self, df, market, locations, location_type, verbose=False):
        """
        Finalizes DataFrame:

        - Sets Market
        - Filters by location type if needed
        - Sets location type
        - Renames and ordering columns
        - Filters by Location
        - Resets the index

        Parameters:
            df (DataFrame): DataFrame with SPP data
            market (str): Market
            locations (list): List of locations to filter by
            location_type (str): Location type
            verbose (bool): Verbose output
        """

        df["Market"] = market.value

        if location_type == LOCATION_TYPE_SETTLEMENT_LOCATION:
            # annotate instead of filter
            hubs = self._get_location_list(LOCATION_TYPE_HUB, verbose=verbose)
            hub_name = SPP._get_location_type_name(LOCATION_TYPE_HUB)

            interfaces = self._get_location_list(
                LOCATION_TYPE_INTERFACE,
                verbose=verbose,
            )
            interface_name = SPP._get_location_type_name(
                LOCATION_TYPE_INTERFACE,
            )

            # Determine Location Type by matching to a hub or interface.
            # Otherwise, fall back to a settlement location
            df["Location Type"] = df["Location"].apply(
                lambda location: SPP._lookup_match(
                    location,
                    {
                        hub_name: hubs,
                        interface_name: interfaces,
                    },
                    default_value=SPP._get_location_type_name(
                        LOCATION_TYPE_SETTLEMENT_LOCATION,
                    ),
                ),
            )
        else:
            # filter
            location_list = self._get_location_list(
                location_type,
                verbose=verbose,
            )
            df["Location Type"] = SPP._get_location_type_name(location_type)
            df = df[df["Location"].isin(location_list)]

        df = df.rename(
            columns={
                "LMP": "LMP",  # for posterity
                "MLC": "Loss",
                "MCC": "Congestion",
                "MEC": "Energy",
            },
        )
        df = utils.filter_lmp_locations(df, locations)
        df = df[
            [
                "Time",
                "Market",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]
        df = df.reset_index(drop=True)
        return df

    @staticmethod
    def _lookup_match(item, lookup, default_value):
        """Use a dictionary to find the first key-value pair
        where the value is a list containing the item
        """
        for key, values_list in lookup.items():
            if item in values_list:
                return key
        return default_value

    @staticmethod
    def _parse_gmt_interval_end(df, interval_duration: pd.Timedelta, timezone):
        return df["GMTIntervalEnd"].apply(
            lambda x: (
                pd.Timestamp(x, unit="ms", tz="UTC") - interval_duration
            ).tz_convert(timezone),
        )

    @staticmethod
    def _parse_day_ahead_hour_end(df, timezone):
        # 'DA_HOUREND': '12/26/2022 9:00:00 AM',
        return df["DA_HOUREND"].apply(
            lambda x: (pd.Timestamp(x, tz=timezone) - pd.Timedelta(hours=1)),
        )

    def _normalize_location_type(self, location_type):
        norm_location_type = location_type.upper()
        if norm_location_type in self.location_types:
            return norm_location_type
        else:
            raise NotSupported(f"Invalid location_type {location_type}")

    @staticmethod
    def _get_location_type_name(location_type):
        if location_type == LOCATION_TYPE_HUB:
            return "Hub"
        elif location_type == LOCATION_TYPE_INTERFACE:
            return "Interface"
        elif location_type == LOCATION_TYPE_SETTLEMENT_LOCATION:
            return "Settlement Location"
        else:
            raise ValueError(f"Invalid location_type: {location_type}")

    def _get_location_list(self, location_type, verbose=False):
        if location_type == LOCATION_TYPE_HUB:
            df = self._get_feature_data(QUERY_RTM5_HUBS_URL, verbose=verbose)
        elif location_type == LOCATION_TYPE_INTERFACE:
            df = self._get_feature_data(
                QUERY_RTM5_INTERFACES_URL,
                verbose=verbose,
            )
        else:
            raise ValueError(f"Invalid location_type: {location_type}")
        return df["SETTLEMENT_LOCATION"].unique().tolist()

    def _fs_get_rtbm_lmp_by_location_paths(self, date, verbose=False):
        """Lists files for Real-Time Balancing Market (RTBM), Locational Marginal Price (LMP) by Settlement Location (SL)"""
        if date == "latest":
            paths = ["/RTBM-LMP-SL-latestInterval.csv"]
        elif utils.is_today(date, self.default_timezone):
            files_df = self._file_browser_list(
                name=FS_RTBM_LMP_BY_LOCATION,
                fs_name=FS_RTBM_LMP_BY_LOCATION,
                type="folder",
                path=date.strftime("/%Y/%m/By_Interval/%d"),
            )
            paths = files_df["path"].tolist()
        if verbose:
            print(f"Found {len(paths)} files for {date}", file=sys.stderr)
        return paths

    def _fetch_and_concat_csvs(self, paths: list, fs_name: str, verbose: bool = False):
        all_dfs = []
        for path in tqdm.tqdm(paths):
            url = self._file_browser_download_url(
                fs_name,
                params={"path": path},
            )
            if verbose:
                print(f"Fetching {url}", file=sys.stderr)
            csv = requests.get(url)
            df = pd.read_csv(io.StringIO(csv.content.decode("UTF-8")))
            all_dfs.append(df)
        return pd.concat(all_dfs)

    def _fs_get_dam_lmp_by_location_paths(self, date, verbose=False):
        """Lists files for Day-ahead Market (DAM), Locational Marginal Price (LMP) by Settlement Location (SL)"""
        paths = []
        if date == "latest":
            raise ValueError(
                "DAM is released daily, so use date='today' instead",
            )
        elif not utils.is_today(date, self.default_timezone):
            raise NotSupported(
                "Historical DAM data is not supported currently",
            )

        date = pd.Timestamp.now(
            tz=self.default_timezone,
        ).normalize()
        # list files for this month
        files_df = self._file_browser_list(
            name=FS_DAM_LMP_BY_LOCATION,
            fs_name=FS_DAM_LMP_BY_LOCATION,
            type="folder",
            path=date.strftime("/%Y/%m/By_Day"),
        )
        max_name = max(files_df["name"])
        max_file = files_df[files_df["name"] == max_name]
        # get latest file
        paths = max_file["path"].tolist()

        if verbose:
            print(f"Found {len(paths)} files for {date}", file=sys.stderr)
        return paths

    def _get_marketplace_session(self) -> dict:
        """
        Returns a session object for the Marketplace API
        """
        html = requests.get(MARKETPLACE_BASE_URL)
        jsessionid = html.cookies.get("JSESSIONID")
        soup = BeautifulSoup(html.content, "html.parser")
        csrf_token = soup.find("meta", {"id": "_csrf"}).attrs["content"]
        csrf_token_header = soup.find(
            "meta",
            {"id": "_csrf_header"},
        ).attrs["content"]

        return {
            "cookies": {"JSESSIONID": jsessionid},
            "headers": {
                csrf_token_header: csrf_token,
            },
        }

    def _file_browser_list(self, name: str, fs_name: str, type: str, path: str):
        """Lists folders in a browser

        Returns: pd.DataFrame of files, or empty pd.DataFrame on error"""
        session = self._get_marketplace_session()
        json_payload = {
            "name": name,
            "fsName": fs_name,
            "type": type,
            "path": path,
        }
        list_results = requests.post(
            FILE_BROWSER_API_URL,
            json=json_payload,
            headers=session["headers"],
            cookies=session["cookies"],
        )
        if list_results.status_code == 200:
            df = pd.DataFrame(list_results.json())
            return df
        else:
            return pd.DataFrame()

    def _file_browser_download_url(self, fs_name, params=None):
        qs = "?" + urlencode(params) if params else ""
        return f"{FILE_BROWSER_API_URL}download/{fs_name}{qs}"


# historical generation mix
# https://marketplace.spp.org/pages/generation-mix-rolling-365
# https://marketplace.spp.org/chart-api/gen-mix-365/asFile
# 15mb file with five minute resolution

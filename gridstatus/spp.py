import pandas as pd
import requests
import tqdm
from bs4 import BeautifulSoup

from gridstatus import utils
from gridstatus.base import (
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NotSupported,
)
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import log
from gridstatus.lmp_config import lmp_config

FS_RTBM_LMP_BY_LOCATION = "rtbm-lmp-by-location"
FS_DAM_LMP_BY_LOCATION = "da-lmp-by-location"
MARKETPLACE_BASE_URL = "https://portal.spp.org"
FILE_BROWSER_API_URL = "https://portal.spp.org/file-browser-api/"
FILE_BROWSER_DOWNLOAD_URL = "https://portal.spp.org/file-browser-api/download"

LOCATION_TYPE_ALL = "ALL"
LOCATION_TYPE_HUB = "Hub"
LOCATION_TYPE_INTERFACE = "Interface"
LOCATION_TYPE_SETTLEMENT_LOCATION = "Settlement Location"

QUERY_RTM5_HUBS_URL = "https://pricecontourmap.spp.org/arcgis/rest/services/MarketMaps/RTBM_FeatureData/MapServer/1/query"  # noqa
QUERY_RTM5_INTERFACES_URL = "https://pricecontourmap.spp.org/arcgis/rest/services/MarketMaps/RTBM_FeatureData/MapServer/2/query"  # noqa

RELIABILITY_LEVELS = [
    "Normal Operations",
    "Weather Advisory",
    "Resource Advisory",
    "Conservative Operations Advisory",
    "Energy Emergency Alert Level 1",
    "Energy Emergency Alert Level 2",
    "Energy Emergency Alert Level 3",
    "Restoration Event",
]

LAST_UPDATED_KEYWORDS = [
    "last updated",
    "as of",
]

RELIABILITY_LEVELS_ALIASES = {
    "Normal Operations": "Normal",
}

STATUS_STOP_WORDS = [
    "as",
    "at",
    "ct",  # central time
    "eea",  # energy emergency alert
    "of",
    "on",
]


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
        LOCATION_TYPE_ALL,
        LOCATION_TYPE_HUB,
        LOCATION_TYPE_INTERFACE,
        LOCATION_TYPE_SETTLEMENT_LOCATION,
    ]

    def get_fuel_mix(self, date, detailed=False, verbose=False):
        """Get fuel mix

        Args:
            date: supports today and latest
            detailed: if True, breaks out self scheduled and market scheduled

        Note:
            if today, returns last 2 hours of data. maybe include previous day

        Returns:
            pd.DataFrame: fuel mix

        """
        if date == "latest":
            return self.get_fuel_mix(
                "today",
                detailed=detailed,
                verbose=verbose,
            ).reset_index(drop=True)

        if not utils.is_today(date, self.default_timezone):
            # https://marketplace.spp.org/pages/generation-mix-historical
            # many years of historical 5 minute data
            raise NotSupported

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/generation-mix-historical?path=/GenMix2Hour.csv"  # noqa
        df_raw = pd.read_csv(url)
        historical_mix = process_gen_mix(df_raw, detailed=detailed)

        historical_mix = historical_mix.drop(
            columns=["Short Term Load Forecast", "Average Actual Load"],
            errors="ignore",
        )

        return historical_mix

    def get_load(self, date, verbose=False):
        """Returns load for last 24hrs in 5 minute intervals"""
        original_date = date

        if date == "latest":
            date = "today"

        date = utils._handle_date(date, self.default_timezone)

        df = self._get_load_and_forecast(verbose=verbose)

        df = df.dropna(subset=["Actual Load"])

        df = df.rename(columns={"Actual Load": "Load"})

        df = df[["Time", "Load"]]
        df = df.reset_index(drop=True)
        df = add_interval(df, interval_min=5)

        if original_date == "latest":
            return df

        elif utils.is_today(original_date, tz=self.default_timezone):
            # returns two days, so make sure to only return current day's load
            df = df[df["Time"].dt.date == date.date()].reset_index(drop=True)
            return df

        else:
            # hourly historical zonal loads
            # https://marketplace.spp.org/pages/hourly-load
            # five minute actual load available here: https://portal.spp.org/pages/stlf-vs-actual#
            raise NotSupported()

    def get_load_forecast(self, date, forecast_type="MID_TERM", verbose=False):
        """Returns load forecast for next 7 days in hourly intervals

        Arguments:
            forecast_type (str): MID_TERM is hourly for next 7 days or SHORT_TERM is
                every five minutes for a few hours

        Returns:
            pd.DataFrame: forecast for current day
        """
        df = self._get_load_and_forecast(verbose=verbose)

        # gives forecast from before current day
        # only include forecasts starting at current day
        last_actual = df.dropna(subset=["Actual Load"])["Time"].max()
        current_day = last_actual.replace(hour=0, minute=0)

        current_day_forecast = df[df["Time"] >= current_day].copy()

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

        current_day_forecast = add_interval(
            current_day_forecast,
            interval_min=60,
        )

        return current_day_forecast

    def _handle_market_end_to_interval(self, df, column, interval_duration):
        """Converts market end time to interval end time"""

        df = df.rename(
            columns={
                column: "Interval End",
            },
        )

        df["Interval End"] = pd.to_datetime(df["Interval End"], utc=True).dt.tz_convert(
            self.default_timezone,
        )

        df["Interval Start"] = df["Interval End"] - interval_duration

        df["Time"] = df["Interval Start"]

        df = utils.move_cols_to_front(df, ["Time", "Interval Start", "Interval End"])

        return df

    def _process_ver_curtailments(self, df):
        df = df.rename(
            columns={
                "WindRedispatchCurtailments": "Wind Redispatch Curtailments",
                "WindManualCurtailments": "Wind Manual Curtailments",
                "WindCurtailedForEnergy": "Wind Curtailed For Energy",
                "SolarRedispatchCurtailments": "Solar Redispatch Curtailments",
                "SolarManualCurtailments": "Solar Manual Curtailments",
                "SolarCurtailedForEnergy": "Solar Curtailed For Energy",
            },
        )

        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnding",
            interval_duration=pd.Timedelta(minutes=5),
        )

        cols = [
            "Time",
            "Interval Start",
            "Interval End",
            "Wind Redispatch Curtailments",
            "Wind Manual Curtailments",
            "Wind Curtailed For Energy",
            "Solar Redispatch Curtailments",
            "Solar Manual Curtailments",
            "Solar Curtailed For Energy",
        ]

        # historical data doesnt have all columns
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA

        df = df[cols]

        return df

    @support_date_range("DAY_START")
    def get_capacity_of_generation_on_outage(self, date, end=None, verbose=False):
        """Get Capacity of Generation on Outage.

        Published daily at 8am CT for next 7 days

        Args:
            date: start date
            end: end date


        """
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/capacity-of-generation-on-outage?path=/{date.strftime('%Y')}/{date.strftime('%m')}/Capacity-Gen-Outage-{date.strftime('%Y%m%d')}.csv"  # noqa

        msg = f"Downloading {url}"
        log(msg, verbose)

        df = pd.read_csv(url)

        return self._process_capacity_of_generation_on_outage(df, publish_time=date)

    def get_capacity_of_generation_on_outage_annual(self, year, verbose=True):
        """Get VER Curtailments for a year. Starting 2014.
        Recent data use get_capacity_of_generation_on_outage

        Args:
            year: year to get data for
            verbose: print url

        Returns:
            pd.DataFrame: VER Curtailments
        """
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/capacity-of-generation-on-outage?path=/{year}/{year}.zip"  # noqa

        def process_csv(df, file_name):
            # infe date from '2020/01/Capacity-Gen-Outage-20200101.csv'

            publish_time_str = file_name.split(".")[0].split("-")[-1]
            publish_time = pd.to_datetime(publish_time_str).tz_localize(
                self.default_timezone,
            )

            df = self._process_capacity_of_generation_on_outage(df, publish_time)

            return df

        df = utils.download_csvs_from_zip_url(
            url,
            process_csv=process_csv,
            verbose=verbose,
        )

        df = df.sort_values("Interval Start")

        return df

    def _process_capacity_of_generation_on_outage(self, df, publish_time):
        # strip whitespace from column names
        df = df.rename(columns=lambda x: x.strip())

        df = self._handle_market_end_to_interval(
            df,
            column="Market Hour",
            interval_duration=pd.Timedelta(minutes=60),
        )

        df = df.rename(
            columns={
                "Outaged MW": "Total Outaged MW",
            },
        )

        publish_time = pd.to_datetime(publish_time.normalize())

        df.insert(0, "Publish Time", publish_time)

        # drop Time column
        df = df.drop(columns=["Time"])

        return df

    @support_date_range("DAY_START")
    def get_ver_curtailments(self, date, end=None, verbose=False):
        """Get VER Curtailments

        Supports recent data. For historical annual data use get_ver_curtailments_annual

        Args:
            date: start date
            end: end date


        """
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/ver-curtailments?path=/{date.strftime('%Y')}/{date.strftime('%m')}/VER-Curtailments-{date.strftime('%Y%m%d')}.csv"  # noqa

        msg = f"Downloading {url}"
        log(msg, verbose)
        df = pd.read_csv(url)

        return self._process_ver_curtailments(df)

    def get_ver_curtailments_annual(self, year, verbose=True):
        """Get VER Curtailments for a year. Starting 2014.
        Recent data use get_ver_curtailments

        Args:
            year: year to get data for
            verbose: print url

        Returns:
            pd.DataFrame: VER Curtailments
        """
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/ver-curtailments?path=/{year}/{year}.zip"  # noqa
        df = utils.download_csvs_from_zip_url(url, verbose=verbose)

        df = self._process_ver_curtailments(df)

        df = df[~df["Interval Start"].isnull()]

        df = df.sort_values("Time")

        return df

    def _get_load_and_forecast(self, verbose=False):
        url = f"{MARKETPLACE_BASE_URL}/chart-api/load-forecast/asChart"

        msg = f"Getting load and forecast from {url}"
        log(msg, verbose)

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
        # {FILE_BROWSER_API_URL}/rtbm-lmp-by-location?path=/2022/08/By_Interval/08/RTBM-LMP-SL-202208082125.csv

        # historical generation mix
        # https://marketplace.spp.org/pages/generation-mix-rolling-365
        # https://marketplace.spp.org/chart-api/gen-mix-365/asFile
        # 15mb file with five minute resolution

    def get_interconnection_queue(self, verbose=False):
        """Get interconnection queue

        Returns:
            pandas.DataFrame: Interconnection queue


        """
        url = "https://opsportal.spp.org/Studies/GenerateActiveCSV"

        msg = f"Getting interconnection queue from {url}"
        log(msg, verbose)

        queue = pd.read_csv(url, skiprows=1)

        queue["Status (Original)"] = queue["Status"]
        completed_val = InterconnectionQueueStatus.COMPLETED.value
        active_val = InterconnectionQueueStatus.ACTIVE.value
        queue["Status"] = queue["Status"].map(
            {
                "IA FULLY EXECUTED/COMMERCIAL OPERATION": completed_val,
                "IA FULLY EXECUTED/ON SCHEDULE": completed_val,
                "IA FULLY EXECUTED/ON SUSPENSION": completed_val,
                "IA PENDING": active_val,
                "DISIS STAGE": active_val,
                "None": active_val,
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

        # todo: there are a few columns being parsed
        # as "unamed" that aren't being included but should
        extra_columns = [
            "In-Service Date",
            "Commercial Operation Date",
            "Cessation Date",
            "Current Cluster",
            "Cluster Group",
            "Replacement Generator Commercial Op Date",
            "Service Type",
            "Status (Original)",
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

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["latest", "today", "historical"],
        },
    )
    @support_date_range(frequency="DAY_START")
    def get_lmp(
        self,
        date,
        end=None,
        market: str = None,
        location_type: str = LOCATION_TYPE_ALL,
        verbose=False,
    ):
        """Get LMP data

        Supported Markets:
            - ``REAL_TIME_5_MIN``
            - ``DAY_AHEAD_HOURLY``

        Supported Location Types:
            - ``Hub``
            - ``Interface``
            - ``ALL``
        """
        if market not in self.markets:
            raise NotSupported(f"Market {market} not supported")

        if location_type not in self.location_types:
            raise NotSupported(f"Location type {location_type} not supported")

        if market == Markets.REAL_TIME_5_MIN:
            df = self._get_rtm5_lmp(
                date,
                end,
                verbose,
            )
        elif market == Markets.DAY_AHEAD_HOURLY:
            if date == "latest":
                raise ValueError("Latest not supported for Day Ahead Hourly")
            df = self._get_dam_lmp(
                date,
                verbose,
            )

        return self._finalize_spp_df(
            df,
            market=market,
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
        verbose=False,
    ):
        if date == "latest":
            urls = [
                FILE_BROWSER_DOWNLOAD_URL
                + "/"
                + FS_RTBM_LMP_BY_LOCATION
                + "?path=%2FRTBM-LMP-SL-latestInterval.csv",
            ]
        else:
            urls = self._file_browser_list(
                fs_name=FS_RTBM_LMP_BY_LOCATION,
                type="folder",
                path=date.strftime("/%Y/%m/By_Interval/%d"),
            )["url"].tolist()

        msg = f"Found {len(urls)} files for {date}"
        log(msg, verbose)

        df = self._fetch_and_concat_csvs(urls, verbose=verbose)
        return df

    def _get_dam_lmp(
        self,
        date,
        verbose=False,
    ):
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/{FS_DAM_LMP_BY_LOCATION}?path=/{date.strftime('%Y')}/{date.strftime('%m')}/By_Day/DA-LMP-SL-{date.strftime('%Y%m%d')}0100.csv"  # noqa
        log(f"Downloading {url}", verbose=verbose)
        df = pd.read_csv(url)
        return df

    def _finalize_spp_df(self, df, market, location_type, verbose=False):
        """
        Finalizes DataFrame:

        - Sets Market
        - Filters by location type if needed
        - Sets location type
        - Renames and ordering columns
        - Filters by Location
        - Resets the index

        Arguments:
            pandas.DataFrame: DataFrame with SPP data
            market (str): Market
            location_type (str): Location type
            verbose (bool, optional): Verbose output
        """
        if market == Markets.REAL_TIME_5_MIN:
            interval_duration = pd.Timedelta(minutes=5)
        elif market == Markets.DAY_AHEAD_HOURLY:
            interval_duration = pd.Timedelta(hours=1)

        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=interval_duration,
        )

        df["Location"] = df["Settlement Location"]
        df["PNode"] = df["Pnode"]

        df["Market"] = market.value

        df["Location Type"] = LOCATION_TYPE_SETTLEMENT_LOCATION

        # Create boolean masks for each location type
        hubs = self._get_location_list(LOCATION_TYPE_HUB, verbose=verbose)
        interfaces = self._get_location_list(LOCATION_TYPE_INTERFACE, verbose=verbose)
        is_hub = df["Location"].isin(hubs)
        is_interface = df["Location"].isin(interfaces)
        df.loc[is_hub, "Location Type"] = LOCATION_TYPE_HUB
        df.loc[is_interface, "Location Type"] = LOCATION_TYPE_INTERFACE

        df = df.rename(
            columns={
                "LMP": "LMP",  # for posterity
                "MLC": "Loss",
                "MCC": "Congestion",
                "MEC": "Energy",
            },
        )

        df = utils.filter_lmp_locations(df, location_type=location_type)

        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Market",
                "Location",
                "Location Type",
                "PNode",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]
        df = df.reset_index(drop=True)
        return df

    @support_date_range("DAY_START")
    def get_lmp_real_time_weis(self, date, verbose=False):
        """Get LMP data for real time WEIS

        Args:
            date: date to get data for
        """

        # quick implementation using daily files
        # daily files publish with a few day delay
        # there are interval files that provide more real time data
        # also, there are also annual files to handle more more historical data

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/lmp-by-settlement-location-weis?path=/{date.strftime('%Y')}/{date.strftime('%m')}/By_Day/WEIS-RTBM-LMP-DAILY-SL-{date.strftime('%Y%m%d')}.csv"  # noqa
        msg = f"Downloading {url}"
        log(msg, verbose)
        df = pd.read_csv(url)

        return self._process_lmp_real_time_weis(df)

    def _process_lmp_real_time_weis(self, df):
        # strip whitespace from column names
        df = df.rename(columns=lambda x: x.strip())

        df = self._handle_market_end_to_interval(
            df,
            column="GMT Interval",
            interval_duration=pd.Timedelta(minutes=5),
        )

        df["Location Type"] = LOCATION_TYPE_SETTLEMENT_LOCATION
        df["Market"] = "REAL_TIME_WEIS"

        df = df.rename(
            columns={
                "Settlement Location Name": "Location",
                "PNODE Name": "PNode",
                "LMP": "LMP",  # for posterity
                "MLC": "Loss",
                "MCC": "Congestion",
                "MEC": "Energy",
            },
        )

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Market",
                "Location",
                "Location Type",
                "PNode",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
        ]

        return df

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

    def _fetch_and_concat_csvs(self, urls: list, verbose: bool = False):
        all_dfs = []
        for url in tqdm.tqdm(urls):
            msg = f"Fetching {url}"
            log(msg, verbose)
            df = pd.read_csv(url)
            all_dfs.append(df)
        return pd.concat(all_dfs)

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

    def _file_browser_list(self, fs_name: str, type: str, path: str):
        """Lists folders in a browser

        Returns: pd.DataFrame of files, or empty pd.DataFrame on error"""
        session = self._get_marketplace_session()
        json_payload = {
            "name": fs_name,
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
            df["url"] = (
                FILE_BROWSER_DOWNLOAD_URL + "/" + fs_name + "?path=" + df["path"]
            )
            return df
        else:
            return pd.DataFrame()

    @staticmethod
    def _match(
        needles,
        haystacks,
        needle_norm_fn=lambda x: x.lower(),
        haystack_norm_fn=lambda x: x.lower(),
    ):
        """Returns items from haystacks if any needles are in them"""
        return [
            haystack
            for haystack in haystacks
            if any(
                needle_norm_fn(needle) in haystack_norm_fn(haystack)
                for needle in needles
            )
        ]


def process_gen_mix(df, detailed=False):
    """Parse SPP generation mix data from
    https://marketplace.spp.org/pages/generation-mix-historical

    Args:
        df (pd.DataFrame): raw data
        detailed (bool): whether to combine market and self columns

    Returns:
        pd.DataFrame: processed data
    """
    new_df = df.copy()

    # remove whitespace from column names
    new_df.columns = new_df.columns.str.strip()

    # rename columns to standardize
    new_df = new_df.rename(
        columns={
            "GMTTime": "Time",
            "GMT MKT Interval": "Time",
            "Gas Self": "Natural Gas Self",
            # rename below is based on documenation
            "Load": "Short Term Load Forecast",
        },
    )

    # parse time
    new_df["Time"] = pd.to_datetime(new_df["Time"], utc=True).dt.tz_convert(
        SPP.default_timezone,
    )

    # combine market and self columns
    columns_to_combine = [
        "Coal",
        "Diesel Fuel Oil",
        "Hydro",
        "Natural Gas",
        "Nuclear",
        "Solar",
        "Waste Disposal Services",
        "Wind",
        "Waste Heat",
        "Other",
    ]

    if not detailed:
        for col in columns_to_combine:
            market_col = f"{col} Market"
            self_col = f"{col} Self"

            if market_col not in new_df.columns or self_col not in new_df.columns:
                continue

            new_df[col] = new_df[market_col] + new_df[self_col]
            new_df = new_df.drop([market_col, self_col], axis=1)

    new_df = add_interval(new_df, 5)

    return new_df


def add_interval(df, interval_min):
    """Adds Interval Start and Interval End columns to df"""
    df["Interval Start"] = df["Time"]
    df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=interval_min)

    df = utils.move_cols_to_front(
        df,
        ["Time", "Interval Start", "Interval End"],
    )

    return df

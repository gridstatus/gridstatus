import io
import re
from urllib.parse import urlencode

import pandas as pd
import requests
import tqdm
from bs4 import BeautifulSoup, Tag

from gridstatus import utils
from gridstatus.base import (
    GridStatus,
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NotSupported,
)
from gridstatus.decorators import support_date_range
from gridstatus.lmp_config import lmp_config
from gridstatus.logging import log

FS_RTBM_LMP_BY_LOCATION = "rtbm-lmp-by-location"
FS_DAM_LMP_BY_LOCATION = "da-lmp-by-location"
MARKETPLACE_BASE_URL = "https://marketplace.spp.org"
FILE_BROWSER_API_URL = "https://marketplace.spp.org/file-browser-api/"

LOCATION_TYPE_HUB = "HUB"
LOCATION_TYPE_INTERFACE = "INTERFACE"
LOCATION_TYPE_SETTLEMENT_LOCATION = "SETTLEMENT_LOCATION"

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
        LOCATION_TYPE_HUB,
        LOCATION_TYPE_INTERFACE,
        LOCATION_TYPE_SETTLEMENT_LOCATION,
    ]

    def get_status(self, date=None, verbose=False):
        if date != "latest":
            raise NotSupported()

        url = "https://www.spp.org/markets-operations/current-grid-conditions/"
        html_text = requests.get(url).content.decode("UTF-8")
        return self._get_status_from_html(html_text)

    def get_fuel_mix(self, date, verbose=False):
        """Get fuel mix

        Args:
            date: supports today and latest

        Note:
            if today, returns last 2 hours of data. maybe include previous day

        Returns:
            pd.DataFrame: fuel mix

        """
        if date == "latest":
            return (
                self.get_fuel_mix("today", verbose=verbose)
                .tail(1)
                .reset_index(drop=True)
            )

        if not utils.is_today(date, self.default_timezone):
            # https://marketplace.spp.org/pages/generation-mix-historical
            # many years of historical 5 minute data
            raise NotSupported

        url = "https://marketplace.spp.org/chart-api/gen-mix/asChart"
        r = self._get_json(url, verbose=verbose)["response"]

        data = {"Timestamp": r["labels"]}
        data.update((d["label"], d["data"]) for d in r["datasets"])

        historical_mix = pd.DataFrame(data)

        historical_mix["Timestamp"] = pd.to_datetime(
            historical_mix["Timestamp"],
        ).dt.tz_convert(
            self.default_timezone,
        )

        historical_mix.rename(
            columns={"Timestamp": "Time"},
            inplace=True,
        )

        historical_mix = add_interval(historical_mix, interval_min=5)

        return historical_mix

    def get_load(self, date, verbose=False):
        """Returns load for last 24hrs in 5 minute intervals"""

        if date == "latest":
            return self._latest_from_today(self.get_load)

        elif utils.is_today(date, tz=self.default_timezone):
            date = utils._handle_date(date, self.default_timezone)

            df = self._get_load_and_forecast(verbose=verbose)

            df = df.dropna(subset=["Actual Load"])

            df = df.rename(columns={"Actual Load": "Load"})

            df = df[["Time", "Load"]]

            # returns two days, so make sure to only return current day's load
            df = df[df["Time"].dt.date == date.date()]

            df = df.reset_index(drop=True)

            df = add_interval(df, interval_min=5)

            return df

        else:
            # hourly historical zonal loads
            # https://marketplace.spp.org/pages/hourly-load
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

    def _get_load_and_forecast(self, verbose=False):
        url = "https://marketplace.spp.org/chart-api/load-forecast/asChart"

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

        Supported Markets:
            - ``REAL_TIME_5_MIN``
            - ``DAY_AHEAD_HOURLY``

        Supported Location Types:
            - ``hub``
            - ``interface``
            - ``settlement_location``
        """
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

        Arguments:
            pandas.DataFrame: DataFrame with SPP data
            market (str): Market
            locations (list): List of locations to filter by
            location_type (str): Location type
            verbose (bool, optional): Verbose output
        """
        df["Interval End"] = pd.to_datetime(
            df["GMTIntervalEnd"],
            utc=True,
        ).dt.tz_convert(self.default_timezone)

        if market == Markets.REAL_TIME_5_MIN:
            interval_duration = pd.Timedelta(minutes=5)
        elif market == Markets.DAY_AHEAD_HOURLY:
            interval_duration = pd.Timedelta(hours=1)

        df["Interval Start"] = df["Interval End"] - interval_duration
        df["Time"] = df["Interval Start"]

        df["Location"] = df["Settlement Location"]

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
                "Interval Start",
                "Interval End",
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
        """
        Lists files for Real-Time Balancing Market (RTBM),
        Locational Marginal Price (LMP) by Settlement Location (SL)
        """
        if date == "latest":
            paths = ["/RTBM-LMP-SL-latestInterval.csv"]
        else:
            files_df = self._file_browser_list(
                name=FS_RTBM_LMP_BY_LOCATION,
                fs_name=FS_RTBM_LMP_BY_LOCATION,
                type="folder",
                path=date.strftime("/%Y/%m/By_Interval/%d"),
            )
            paths = files_df["path"].tolist()
        msg = f"Found {len(paths)} files for {date}"
        log(msg, verbose)
        return paths

    def _fetch_and_concat_csvs(self, paths: list, fs_name: str, verbose: bool = False):
        all_dfs = []
        for path in tqdm.tqdm(paths):
            url = self._file_browser_download_url(
                fs_name,
                params={"path": path},
            )
            msg = f"Fetching {url}"
            log(msg, verbose)

            csv = requests.get(url)
            df = pd.read_csv(io.StringIO(csv.content.decode("UTF-8")))
            all_dfs.append(df)
        return pd.concat(all_dfs)

    def _fs_get_dam_lmp_by_location_paths(self, date, verbose=False):
        """
        Lists files for Day-ahead Market (DAM),
        Locational Marginal Price (LMP) by Settlement Location (SL)
        """
        paths = []
        if date == "latest":
            raise ValueError(
                "DAM is released daily, so use date='today' instead",
            )

        date = date.normalize()

        # list files for this month
        files_df = self._file_browser_list(
            name=FS_DAM_LMP_BY_LOCATION,
            fs_name=FS_DAM_LMP_BY_LOCATION,
            type="folder",
            path=date.strftime("/%Y/%m/By_Day"),
        )

        files_df["date"] = files_df.name.apply(
            lambda x: pd.to_datetime(
                x.strip(".csv").split("-")[-1],
                format="%Y%m%d%H%M",
            )
            .normalize()
            .tz_localize(self.default_timezone),
        )

        matched_file = files_df[files_df["date"] == date]
        # get latest file
        paths = matched_file["path"].tolist()

        msg = f"Found {len(paths)} files for {date}"
        log(msg, verbose)
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

    @staticmethod
    def _clean_status_text(text):
        text = text.lower()

        # remove punctuation
        text = re.sub(r"[,\.\(\)]", "", text)
        # remove non-time colons
        text = re.sub(r":$", "", text)
        # drop time zone information
        text = re.sub(r"central time", "", text)
        # truncate starting with last updated
        text = re.sub(r".*last updated", "", text)

        # drop stop words
        tokens = text.split(" ")
        filtered_words = [
            token for token in tokens if token.lower() not in STATUS_STOP_WORDS
        ]
        text = " ".join(filtered_words)

        return text

    @staticmethod
    def _extract_timestamp(text, year_hint=None, tz=None):
        if year_hint is None:
            year_hint = pd.Timestamp.now(tz=tz).year
        text = SPP._clean_status_text(text)

        year_search = re.search(r"[0-9]{4}", text)
        if year_search is None:
            # append year hint
            text = f"{text} {year_hint}"

        timestamp = None

        try:
            # throw the remaining bits at pd.Timestamp
            timestamp = pd.Timestamp(text, tz=tz)
        except ValueError:
            pass
        if timestamp is pd.NaT:
            timestamp = None
        return timestamp

    @staticmethod
    def _extract_timestamps(texts, year_hint=None, tz=None):
        timestamps = [
            SPP._extract_timestamp(t, year_hint=year_hint, tz=tz) for t in texts
        ]
        return [t for t in timestamps if t is not None]

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

    @staticmethod
    def _get_leaf_elements(elems):
        """Returns leaf elements, i.e. elements without children"""
        accum = []
        for elem in elems:
            parent = False
            if isinstance(elem, Tag):
                children = list(elem.children)
                if len(children) > 0:
                    for child in children:
                        accum += SPP._get_leaf_elements([child])
                        parent = True
            if not parent:
                accum.append(elem)
        return accum

    def _get_status_candidate_texts(self, html):
        """Returns a list of text candidates for status and timestamp extraction"""
        # generic pre-Soup cleanup
        html = re.sub(r"<[/]?span>", "", html)
        html = re.sub(r"<br/>", "", html)
        html = re.sub(r"\xa0", "", html)
        soup = BeautifulSoup(html, "html.parser")
        # use <h1> as the north star
        conditions_element = soup.find("h1")
        # find all sibling paragraphs, and then their descendant leaves
        sibling_paragraphs = self._get_leaf_elements(
            conditions_element.parent.find_all("p"),
        )
        # just the text, please
        return [p.text for p in sibling_paragraphs]

    def _get_status_from_html(self, html_text, year_hint=None):
        """Extracts timestamp, status, and status notes from HTML"""
        candidate_texts = self._get_status_candidate_texts(html_text)
        timestamp = self._get_status_timestamp(
            candidate_texts,
            year_hint=year_hint,
        )
        status, notes = self._get_status_status_and_notes(candidate_texts)

        if timestamp is None:
            raise RuntimeError("Cannot parse time of status")

        return GridStatus(
            time=timestamp,
            status=status,
            notes=notes,
            reserves=None,
            iso=self,
        )

    def _get_status_timestamp(self, candidate_texts, year_hint=None):
        """Get timestamp from candidate texts

        Returns
            pd.Timestamp or None
        """
        timestamp_texts = self._match(
            LAST_UPDATED_KEYWORDS,
            candidate_texts,
        )

        new_list = []
        for text in timestamp_texts:
            """Truncate to immediately after reliability level,
            e.g. "blah blah Normal Operations 12:00 PM Central Time"
            -> "12:00 PM Central Time"
            """
            for keyword in RELIABILITY_LEVELS:
                pos = text.lower().find(keyword.lower())
                if pos > -1:
                    pos += len(keyword)
                    new_list.append(text[pos:])
            new_list.append(text)
        timestamp_texts = new_list

        last_updated_timestamps = self._extract_timestamps(
            timestamp_texts,
            year_hint=year_hint,
            tz=self.default_timezone,
        )
        return next(iter(last_updated_timestamps), None)

    def _get_status_status_and_notes(self, candidate_texts):
        """Extracts (status, notes,) tuple from candidates texts"""
        status_texts = self._match(
            RELIABILITY_LEVELS,
            candidate_texts,
            haystack_norm_fn=lambda x: self._clean_status_text(x),
        )

        status_text = None
        if len(status_texts) > 0:
            status_text = status_texts[0]

        status = status_text  # default
        notes = None

        norm_status_text = self._clean_status_text(status_text)
        for level in RELIABILITY_LEVELS:
            if level.lower() in norm_status_text:
                status = RELIABILITY_LEVELS_ALIASES.get(level, level)
                notes = [status_text]

        return (
            status,
            notes,
        )


def add_interval(df, interval_min):
    """Adds Interval Start and Interval End columns to df"""
    df["Interval Start"] = df["Time"]
    df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=interval_min)

    df = utils.move_cols_to_front(
        df,
        ["Time", "Interval Start", "Interval End"],
    )

    return df

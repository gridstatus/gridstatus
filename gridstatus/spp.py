from typing import BinaryIO

import pandas as pd
import requests
import tqdm

from gridstatus import utils
from gridstatus.base import InterconnectionQueueStatus, ISOBase, Markets, NotSupported
from gridstatus.decorators import FiveMinOffset, support_date_range
from gridstatus.gs_logging import log

RTBM_LMP_BY_BUS = "rtbm-lmp-by-bus"
FS_RTBM_LMP_BY_LOCATION = "rtbm-lmp-by-location"
FS_DAM_LMP_BY_LOCATION = "da-lmp-by-location"
LMP_BY_SETTLEMENT_LOCATION_WEIS = "lmp-by-settlement-location-weis"
OPERATING_RESERVES = "operating-reserves"

MARKETPLACE_BASE_URL = "https://portal.spp.org"
FILE_BROWSER_API_URL = "https://portal.spp.org/file-browser-api/"
FILE_BROWSER_DOWNLOAD_URL = "https://portal.spp.org/file-browser-api/download"

BASE_SOLAR_AND_WIND_SHORT_TERM_URL = (
    f"{FILE_BROWSER_DOWNLOAD_URL}/shortterm-resource-forecast?path="
)
BASE_SOLAR_AND_WIND_MID_TERM_URL = (
    f"{FILE_BROWSER_DOWNLOAD_URL}/midterm-resource-forecast?path="
)

BASE_LOAD_FORECAST_SHORT_TERM_URL = f"{FILE_BROWSER_DOWNLOAD_URL}/stlf-vs-actual?path="

BASE_LOAD_FORECAST_MID_TERM_URL = f"{FILE_BROWSER_DOWNLOAD_URL}/mtlf-vs-actual?path="


LOCATION_TYPE_ALL = "ALL"
LOCATION_TYPE_BUS = "Bus"
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
        LOCATION_TYPE_BUS,
        LOCATION_TYPE_HUB,
        LOCATION_TYPE_INTERFACE,
        LOCATION_TYPE_SETTLEMENT_LOCATION,
    ]

    @staticmethod
    def now():
        return pd.Timestamp.now(tz=SPP.default_timezone)

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

    @support_date_range("5_MIN")
    def get_load_forecast_short_term(self, date, end=None, verbose=False):
        """
        5-minute load forecast data for the SPP footprint (system-wide) for +/- 10
        minutes. Also includes actual load.

        Data from https://portal.spp.org/pages/stlf-vs-actual

        Arguments:
            date (pd.Timestamp|str): date to get data for. Supports "latest" and "today"
            verbose (bool): print info

        Returns:
            pd.DataFrame: forecast as dataframe.
        """
        # The short_term forecast is delayed up to 2 minutes.
        buffer_minutes = 2

        if date == "latest":
            date = self.now() - pd.Timedelta(minutes=buffer_minutes)

        # Files do not exist in the future
        if date > self.now():
            return

        url = self._short_term_load_forecast_url(date.floor("5min"))

        log(f"Downloading {url}", verbose=verbose)
        df = pd.read_csv(url)

        # According to the docs, the end time col should be GMTIntervalEnd, but it's
        # only GMTInterval in the data
        df = self._post_process_load_forecast(
            df,
            url,
            forecast_type="SHORT_TERM",
            forecast_col="STLF",
            end_time_col="GMTInterval",
            interval_duration=pd.Timedelta(minutes=5),
        )

        return df

    @support_date_range("HOUR_START")
    def get_load_forecast_mid_term(self, date, end=None, verbose=False):
        """
        Returns load forecast for +7 days in hourly intervals. Includes actual load
        for the past 24 hours. Data from https://portal.spp.org/pages/mtlf-vs-actual

        Arguments:
            date (pd.Timestamp|str): date to get data for. Supports "latest" and "today"
            verbose (bool): print info

        Returns:
            pd.DataFrame: forecast as dataframe.
        """
        # The MID_TERM forecast is delayed up to 10 minutes.
        buffer_minutes = 10

        if date == "latest":
            date = self.now() - pd.Timedelta(minutes=buffer_minutes)

        if date > self.now():
            return

        url = self._mid_term_load_forecast_url(date.floor("h"))

        log(f"Downloading {url}", verbose=verbose)
        df = pd.read_csv(url)

        df = self._post_process_load_forecast(
            df,
            url,
            forecast_type="MID_TERM",
            forecast_col="MTLF",
            end_time_col="GMTIntervalEnd",
            interval_duration=pd.Timedelta(hours=1),
        )

        return df

    def _post_process_load_forecast(
        self,
        df,
        url,
        forecast_type,
        forecast_col,
        end_time_col,
        interval_duration,
    ):
        df = self._handle_market_end_to_interval(df, end_time_col, interval_duration)

        # Assume the publish time is in the name of the file. There are different
        # times on the webpage, but these could be the posting time.
        df["Publish Time"] = pd.Timestamp(
            url.split("-")[-1].split(".")[0],
            tz=self.default_timezone,
        )

        df.columns = [col.strip() for col in df.columns]

        df["Forecast Type"] = forecast_type

        df = (
            utils.move_cols_to_front(
                df,
                ["Interval Start", "Interval End", "Publish Time", "Forecast Type"],
            )
            .drop(columns=["Time", "Interval"])
            .sort_values(["Interval Start", "Publish Time"])
        )

        return df.dropna(subset=[forecast_col]).reset_index(drop=True)

    @support_date_range("5_MIN")
    def get_solar_and_wind_forecast_short_term(self, date, end=None, verbose=False):
        """
        Returns solar and wind generation forecast for +4 hours in 5 minute intervals.
        Include actuals for past day in 5 minute intervals.

        Data from https://portal.spp.org/pages/shortterm-resource-forecast

        Arguments:
            date (pd.Timestamp|str): date to get data for. Supports "latest" and "today"
            verbose (bool): print info

        Returns:
            pd.DataFrame: forecast as dataframe.
        """
        # The short_term forecast is delayed up to 2 minutes.
        buffer_minutes = 2

        if date == "latest":
            date = self.now() - pd.Timedelta(minutes=buffer_minutes)

        # Files do not exist in the future
        if date > self.now():
            return

        url = self._short_term_solar_and_wind_url(date.floor("5min"))

        log(f"Downloading {url}", verbose=verbose)
        df = pd.read_csv(url)

        # According to the docs, the end time col should be GMTIntervalEnd, but it's
        # only GMTInterval in the data
        df = self._post_process_solar_and_wind_forecast(
            df,
            url,
            forecast_type="SHORT_TERM",
            end_time_col="GMTInterval",
            interval_duration=pd.Timedelta(minutes=5),
        )

        return df

    @support_date_range("HOUR_START")
    def get_solar_and_wind_forecast_mid_term(self, date, end=None, verbose=False):
        """
        Returns solar and wind generation forecast for +7 days in hourly intervals.

        Data from https://portal.spp.org/pages/midterm-resource-forecast.

        Arguments:
            date (pd.Timestamp|str): date to get data for. Supports "latest" and "today"
            verbose (bool): print info

        Returns:
            pd.DataFrame: forecast as dataframe.
        """
        # The MID_TERM forecast is delayed up to 10 minutes.
        buffer_minutes = 10

        if date == "latest":
            date = self.now() - pd.Timedelta(minutes=buffer_minutes)

        if date > self.now():
            return

        url = self._mid_term_solar_and_wind_url(date.floor("h"))

        log(f"Downloading {url}", verbose=verbose)
        df = pd.read_csv(url)

        df = self._post_process_solar_and_wind_forecast(
            df,
            url,
            forecast_type="MID_TERM",
            end_time_col="GMTIntervalEnd",
            interval_duration=pd.Timedelta(hours=1),
        )

        return df

    def _post_process_solar_and_wind_forecast(
        self,
        df,
        url,
        forecast_type,
        end_time_col,
        interval_duration,
    ):
        df = self._handle_market_end_to_interval(df, end_time_col, interval_duration)

        # Assume the publish time is in the name of the file. There are different
        # times on the webpage, but these could be the posting time.
        df["Publish Time"] = pd.Timestamp(
            url.split("-")[-1].split(".")[0],
            tz=self.default_timezone,
        )

        df.columns = [col.strip() for col in df.columns]

        df["Forecast Type"] = forecast_type

        df = (
            utils.move_cols_to_front(
                df,
                ["Interval Start", "Interval End", "Publish Time", "Forecast Type"],
            )
            .drop(columns=["Time", "Interval"])
            .sort_values(["Interval Start", "Publish Time"])
        )

        return df.dropna(subset=["Wind Forecast MW", "Solar Forecast MW"]).reset_index(
            drop=True,
        )

    def _short_term_solar_and_wind_url(self, date):
        hour = date.hour
        padded_hour = str(hour).zfill(2)
        padded_hour_plus_one = str((hour + 1) % 24).zfill(2)

        # The first hour in the URL is 1 after the hour in the filename.
        # Example 2024/01/01/02 has data for 01/01/2024 01:00:00 - 01/01/2024 01:55:00
        return BASE_SOLAR_AND_WIND_SHORT_TERM_URL + date.strftime(
            f"/%Y/%m/%d/{padded_hour_plus_one}/OP-STRF-%Y%m%d{padded_hour}%M.csv",
        )

    def _mid_term_solar_and_wind_url(self, date):
        # Explicitly set the minutes to 00.
        return BASE_SOLAR_AND_WIND_MID_TERM_URL + date.strftime(
            "/%Y/%m/%d/OP-MTRF-%Y%m%d%H00.csv",
        )

    def _short_term_load_forecast_url(self, date):
        hour = date.hour
        padded_hour = str(hour).zfill(2)
        padded_hour_plus_one = str((hour + 1) % 24).zfill(2)

        # The first hour in the URL is 1 after the hour in the filename.
        # Example 2024/01/01/02 has data for 01/01/2024 01:00:00 - 01/01/2024 01:55:00
        return BASE_LOAD_FORECAST_SHORT_TERM_URL + date.strftime(
            f"/%Y/%m/%d/{padded_hour_plus_one}/OP-STLF-%Y%m%d{padded_hour}%M.csv",
        )

    def _mid_term_load_forecast_url(self, date):
        # Explicitly set the minutes to 00.
        return BASE_LOAD_FORECAST_MID_TERM_URL + date.strftime(
            "/%Y/%m/%d/OP-MTLF-%Y%m%d%H00.csv",
        )

    def _handle_market_end_to_interval(
        self,
        df,
        column,
        interval_duration,
        format=None,
    ):
        """Converts market end time to interval end time"""

        df = df.rename(
            columns={
                column: "Interval End",
            },
        )

        df["Interval End"] = pd.to_datetime(
            df["Interval End"],
            utc=True,
            format=format,
        ).dt.tz_convert(self.default_timezone)

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
        # {FILE_BROWSER_API_URL}/rtbm-lmp-by-location?path=/2022/08/By_Interval/08
        # /RTBM-LMP-SL-202208082125.csv

        # historical generation mix

    # https://marketplace.spp.org/pages/generation-mix-rolling-365
    # https://marketplace.spp.org/chart-api/gen-mix-365/asFile
    # 15mb file with five minute resolution

    def get_raw_interconnection_queue(self, verbose=False) -> BinaryIO:
        url = "https://opsportal.spp.org/Studies/GenerateSummaryCSV"
        msg = f"Getting interconnection queue from {url}"
        log(msg, verbose)
        response = requests.get(url)
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose=False):
        """Get interconnection queue

        Returns:
            pandas.DataFrame: Interconnection queue
        """
        raw_data = self.get_raw_interconnection_queue(verbose)
        queue = pd.read_csv(raw_data, skiprows=1)

        queue["Status (Original)"] = queue["Status"]
        completed_val = InterconnectionQueueStatus.COMPLETED.value
        active_val = InterconnectionQueueStatus.ACTIVE.value
        withdrawn_val = InterconnectionQueueStatus.WITHDRAWN.value
        queue["Status"] = queue["Status"].map(
            {
                "IA FULLY EXECUTED/COMMERCIAL OPERATION": completed_val,
                "IA FULLY EXECUTED/ON SCHEDULE": completed_val,
                "IA FULLY EXECUTED/ON SUSPENSION": completed_val,
                "IA PENDING": active_val,
                "DISIS STAGE": active_val,
                "None": active_val,
                "WITHDRAWN": withdrawn_val,
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
            "Date Withdrawn": "Withdrawn Date",
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

    def get_lmp_real_time_5_min_by_location(
        self,
        date,
        end=None,
        location_type=LOCATION_TYPE_ALL,
        verbose=False,
    ):
        """Get LMP data by location for the Real-Time 5 Minute Market

        Args:
            date: date to get data for
            end: end date
            location_type: location type to get data for. Options are:
                - ``ALL`` (LOCATION_TYPE_ALL)
                - ``Hub`` (LOCATION_TYPE_HUB)
                - ``Interface`` (LOCATION_TYPE_INTERFACE)
                - ``Settlement Location`` (LOCATION_TYPE_SETTLEMENT_LOCATION)
            verbose: print url
        """
        return self._finalize_spp_df(
            self._get_real_time_5_min_data(
                date,
                end=end,
                location_type=location_type,
                verbose=verbose,
            ),
            market=Markets.REAL_TIME_5_MIN,
            location_type=location_type,
            verbose=verbose,
        )

    def get_lmp_real_time_5_min_by_bus(self, date, end=None, verbose=False):
        """Get LMP data by bus for the Real-Time 5 Minute Market

        Args:
            date: date to get data for
            end: end date
            verbose: print url

        NOTE: does not take a location_type argument because it always returns
        LOCATION_TYPE_BUS.
        """
        return self._finalize_spp_df(
            self._get_real_time_5_min_data(
                date,
                end=end,
                location_type=LOCATION_TYPE_BUS,
                verbose=verbose,
            ),
            market=Markets.REAL_TIME_5_MIN,
            location_type=LOCATION_TYPE_BUS,
            verbose=verbose,
        )

    @support_date_range(frequency="5_MIN")
    def _get_real_time_5_min_data(
        self,
        date,
        end=None,
        location_type=LOCATION_TYPE_ALL,
        verbose=False,
    ):
        """
        Internal function that consolidates logic for getting LMP data for the
        Real-Time 5 Minute Market
        """
        if location_type not in self.location_types:
            raise NotSupported(f"Location type {location_type} not supported")

        if location_type == LOCATION_TYPE_BUS:
            endpoint = RTBM_LMP_BY_BUS
            file_prefix = "RTBM-LMP-B"
        else:
            endpoint = FS_RTBM_LMP_BY_LOCATION
            file_prefix = "RTBM-LMP-SL"

        if date == "latest":
            url = f"https://portal.spp.org/file-browser-api/download/{endpoint}?path=%2F{file_prefix}-latestInterval.csv"

        else:
            url = self._format_5_min_url(date, end, endpoint, file_prefix)

        log(f"Getting data for {date} from {url}", verbose=verbose)

        df = pd.read_csv(url)

        return df

    @support_date_range(frequency="DAY_START")
    def get_lmp_day_ahead_hourly(
        self,
        date,
        end=None,
        location_type: str = LOCATION_TYPE_ALL,
        verbose=False,
    ):
        """Get day ahead hourly LMP data

        Supported Location Types:
            - ``Hub``
            - ``Interface``
            - ``ALL``
        """
        if location_type not in self.location_types:
            raise NotSupported(f"Location type {location_type} not supported")

        if date == "latest":
            raise NotSupported("Latest not supported for day ahead hourly")

        df = self._get_dam_lmp(date, verbose)

        return self._finalize_spp_df(
            df,
            market=Markets.DAY_AHEAD_HOURLY,
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
        doc = self._get_json(base_url, verbose=verbose, params=args)
        df = pd.DataFrame([feature["attributes"] for feature in doc["features"]])
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
        else:
            raise ValueError(f"Market {market} not supported")

        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=interval_duration,
        )

        df["Market"] = market.value

        if location_type != LOCATION_TYPE_BUS:
            df["Location"] = df["Settlement Location"]

            df["Location Type"] = LOCATION_TYPE_SETTLEMENT_LOCATION

            # Create boolean masks for each location type
            hubs = self._get_location_list(LOCATION_TYPE_HUB, verbose=verbose)
            interfaces = self._get_location_list(
                LOCATION_TYPE_INTERFACE,
                verbose=verbose,
            )
            is_hub = df["Location"].isin(hubs)
            is_interface = df["Location"].isin(interfaces)
            df.loc[is_hub, "Location Type"] = LOCATION_TYPE_HUB
            df.loc[is_interface, "Location Type"] = LOCATION_TYPE_INTERFACE
            df = utils.filter_lmp_locations(df, location_type=location_type)
        else:
            df["Location"] = df["Pnode"]
            df["Location Type"] = LOCATION_TYPE_BUS

        df = df.rename(
            columns={
                "Pnode": "PNode",
                "LMP": "LMP",  # for posterity
                "MLC": "Loss",
                "MCC": "Congestion",
                "MEC": "Energy",
            },
        )

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

        # Since Location = PNode for bus, we can drop PNode
        if location_type == LOCATION_TYPE_BUS:
            df = df.drop(columns=["PNode"])

        return df.sort_values(["Time", "Location"])

    @support_date_range("5_MIN")
    def get_operating_reserves(self, date, end=None, verbose=False):
        if date == "latest":
            url = f"{FILE_BROWSER_DOWNLOAD_URL}/operating-reserves?path=/RTBM-OR-latestInterval.csv"  # noqa
        else:
            url = self._format_5_min_url(
                date,
                end,
                OPERATING_RESERVES,
                "RTBM-OR",
                include_interval=False,
            )

        msg = f"Downloading {url}"
        log(msg, verbose)
        df = pd.read_csv(url)
        return self._process_operating_reserves(df)

    def _process_operating_reserves(self, df):
        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=pd.Timedelta(minutes=5),
        )

        # don't need this column
        df = df.drop(columns=["Interval"])

        df = df.rename(
            columns={
                "RegUP_Clr": "Reg_Up_Cleared",
                "RegDN_Clr": "Reg_Dn_Cleared",
                "RampUP_Clr": "Ramp_Up_Cleared",
                "RampDN_Clr": "Ramp_Dn_Cleared",
                "UncUP_Clr": "Unc_Up_Cleared",
                "STSUncUP_Clr": "STS_Unc_Up_Cleared",
                "Spin_Clr": "Spin_Cleared",
                "Supp_Clr": "Supp_Cleared",
            },
        )

        return df

    @support_date_range("DAY_START")
    def get_day_ahead_operating_reserve_prices(self, date, end=None, verbose=False):
        """Provides Marginal Clearing Price information by Reserve Zone for each
        Day-Ahead Market solution for each Operating Day.
        Posting is updated each day after the DA Market results are posted.
        Available at https://portal.spp.org/pages/da-mcp#

        Args:
            date: date to get data for
            end: end date
            verbose: print url

        Returns:
            pd.DataFrame: Day Ahead Marginal Clearing Prices
        """
        if date == "latest":
            raise ValueError(
                "Latest not supported for Day Ahead Marginal Clearing Prices",
            )

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/da-mcp?path=/{date.strftime('%Y')}/{date.strftime('%m')}/DA-MCP-{date.strftime('%Y%m%d')}0100.csv"  # noqa

        msg = f"Downloading {url}"
        log(msg, verbose)
        df = pd.read_csv(url)

        return self._process_day_ahead_operating_reserve_prices(df)

    def _process_day_ahead_operating_reserve_prices(self, df):
        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=pd.Timedelta(hours=1),
        ).assign(Market="DAM")

        column_mapping = {
            "RegUP": "Reg_Up",
            "RegDN": "Reg_Dn",
            "RampUP": "Ramp_Up",
            "RampDN": "Ramp_Dn",
            "Spin": "Spin",
            "Supp": "Supp",
            "UncUP": "Unc_Up",
        }

        df = df.rename(columns=column_mapping)

        cols_to_keep = [
            "Interval Start",
            "Interval End",
            "Market",
            "Reserve Zone",
        ] + list(
            column_mapping.values(),
        )

        # Older datasets might not have all the reserve types
        return df[[c for c in cols_to_keep if c in df]]

    @support_date_range("5_MIN")
    def get_lmp_real_time_weis(self, date, end=None, verbose=False):
        """Get LMP data for real time WEIS

        Args:
            date: date to get data for. if end is not provided, will get data for
                5 minute interval that date is in.
            end: end date
            verbose: print url
        """
        endpoint = "lmp-by-settlement-location-weis"

        # if no end, find nearest 5 minute interval end
        # to use
        if date == "latest":
            url = f"{FILE_BROWSER_DOWNLOAD_URL}/{endpoint}?path=/WEIS-RTBM-LMP-SL-latestInterval.csv"  # noqa
        else:
            url = self._format_5_min_url(date, end, endpoint, "WEIS-RTBM-LMP-SL")
            # todo before 2022 only annual files are available

        # TODO: sometimes there are missing interval files (example: https://portal.spp.org/pages/lmp-by-settlement-location-weis#%2F2024%2F01%2FBy_Interval%2F21) # noqa
        # We can't do anything in these cases but log a message
        msg = f"Downloading {url}"
        log(msg, verbose)

        try:
            df = pd.read_csv(url)
        except ConnectionResetError as e:
            log(f"Error downloading {url}: {e}", verbose)
            return pd.DataFrame()

        return self._process_lmp_real_time_weis(df)

    def _process_lmp_real_time_weis(self, df):
        # strip whitespace from column names
        df = df.rename(columns=lambda x: x.strip())

        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=pd.Timedelta(minutes=5),
        )

        df["Location Type"] = LOCATION_TYPE_SETTLEMENT_LOCATION
        df["Market"] = "REAL_TIME_WEIS"

        df = df.rename(
            columns={
                "Settlement Location": "Location",
                "Pnode": "PNode",
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

    def _format_5_min_url(
        self,
        start,
        end,
        endpoint,
        file_prefix,
        include_interval=True,
    ):
        # Folder path is based on start date. File name is based on end date.
        # As an example, the file with the name 202407010000 representing the interval
        # 2024-06-30 23:55:00 to 2024-07-01 00:00:00 is in the folder
        # 2024/06/By_Interval/30/

        end = start + FiveMinOffset() if end is None else end.ceil("5min")

        folder_year = start.strftime("%Y")
        folder_month = start.strftime("%m")
        folder_day = start.strftime("%d")

        interval_str = "/By_Interval" if include_interval else ""

        return f"{FILE_BROWSER_DOWNLOAD_URL}/{endpoint}?path=/{folder_year}/{folder_month}{interval_str}/{folder_day}/{file_prefix}-{end.strftime('%Y%m%d%H%M')}.csv"  # noqa

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
        html = requests.get(FILE_BROWSER_API_URL)
        jsessionid = html.cookies.get("JSESSIONID")
        xsrf_token = html.cookies.get("XSRF-TOKEN")

        return {
            "cookies": {"JSESSIONID": jsessionid, "XSRF-TOKEN": xsrf_token},
            "headers": {
                "X-XSRF-TOKEN": xsrf_token,
            },
        }

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

    @support_date_range("DAY_START")
    def get_hourly_load(self, date, end=None, verbose=False):
        """Get Hourly Load

        Supports recent data. For historical annual data use get_hourly_load_annual

        Args:
            date: start date
            end: end date

        Returns:
            pd.DataFrame: Hourly Load
        """
        if date in ["today", "latest"] or utils.is_today(
            date,
            tz=self.default_timezone,
        ):
            raise NotSupported("Only historical data is available for hourly load data")

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/hourly-load?path=/{date.strftime('%Y')}/DAILY_HOURLY_LOAD-{date.strftime('%Y%m%d')}.csv"  # noqa
        msg = f"Downloading {url}"
        log(msg, verbose)
        df = pd.read_csv(url)

        return self._process_hourly_load(df)

    def get_hourly_load_annual(self, year, verbose=True):
        """Get Hourly Load for a year. Starting 2011.
        For recent data use `get_hourly_load`

        Args:
            year: year to get data for
            verbose: print url

        Returns:
            pd.DataFrame: Hourly Load
        """
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/hourly-load?path=/{year}/{year}.zip"  # noqa
        df = utils.download_csvs_from_zip_url(
            url=url,
            verbose=verbose,
            strip_whitespace_from_cols=True,
        )

        return self._process_hourly_load(df)

        return df

    def _process_hourly_load(self, df):
        # Some column names contain leading whitespace in some files - remove it
        df = df.rename(columns=lambda x: x.strip())

        # Some files contain null rows. Drop them.
        df = df.dropna(how="all")

        # Some files don't have time 00:00 for the first interval in a day
        # for example 12/2/2016 instead of 12/2/2016 00:00. This causes datetime
        # conversion problems. This fixes it.
        def clean_market_hour(val):
            if val.endswith(":00"):
                return val
            return val + " 00:00"

        df["MarketHour"] = df["MarketHour"].apply(clean_market_hour)

        df = self._handle_market_end_to_interval(
            df,
            column="MarketHour",
            interval_duration=pd.Timedelta(minutes=60),
            format="mixed",
        )

        time_cols = [
            "Time",
            "Interval Start",
            "Interval End",
        ]

        load_cols = [
            "CSWS",
            "EDE",
            "GRDA",
            "INDN",
            "KACY",
            "KCPL",
            "LES",
            "MPS",
            "NPPD",
            "OKGE",
            "OPPD",
            "SECI",
            "SPRM",
            "SPS",
            "WAUE",
            "WFEC",
            "WR",
        ]
        all_cols = time_cols + load_cols

        # historical data doesn't have all columns
        for c in all_cols:
            if c not in df.columns:
                df[c] = pd.NA

        df = df[all_cols]

        df = df[~df["Interval Start"].isnull()].drop_duplicates()

        df = df.sort_values("Time")
        df["System Total"] = df[load_cols].sum(axis=1, skipna=True)

        return df


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

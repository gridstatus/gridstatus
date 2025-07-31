import io
import json
import re
import urllib
import warnings
import zipfile
from typing import BinaryIO

import pandas as pd
import requests

from gridstatus import utils
from gridstatus.base import ISOBase, Markets, NoDataFoundException, NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import logger
from gridstatus.lmp_config import lmp_config


def add_interval_end(df: pd.DataFrame, duration_min: int) -> pd.DataFrame:
    """Add an interval end column to a dataframe

    Args:
        df (pandas.DataFrame): Dataframe with a time column
        duration_min (int): Interval duration in minutes

    Returns:
        pandas.DataFrame: Dataframe with an interval end column
    """
    df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=duration_min)
    df["Time"] = df["Interval Start"]
    df = utils.move_cols_to_front(
        df,
        ["Time", "Interval Start", "Interval End"],
    )
    return df


"""
Notes

- Real-time 5-minute LMP data for current day, previous day available
https://api.misoenergy.org/MISORTWDBIReporter/Reporter.asmx?messageType=rollingmarketday&returnType=json

- market reports https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=
historical fuel mix: https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=%2FMarketReportType%3ASummary%2FMarketReportName%3AHistorical%20Generation%20Fuel%20Mix%20(xlsx)&t=10&p=0&s=MarketReportPublished&sd=desc

- ancillary services available in consolidate api

"""  # noqa


class MISO(ISOBase):
    """Midcontinent Independent System Operator (MISO)"""

    BASE = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx"

    interconnection_homepage = (
        "https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/"
    )

    name = "Midcontinent ISO"
    iso_id = "miso"

    # Parsing of raw data is done in EST since that is what api returns and what
    # MISO operates in
    # Source: https://www.rtoinsider.com/25291-ferc-oks-miso-use-of-eastern-standard-time-in-day-ahead-market/ # noqa
    default_timezone = "EST"

    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_5_MIN_FINAL,
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_HOURLY_FINAL,
        Markets.REAL_TIME_HOURLY_PRELIM,
    ]

    hubs = [
        "ILLINOIS.HUB",
        "INDIANA.HUB",
        "LOUISIANA.HUB",
        "MICHIGAN.HUB",
        "MINN.HUB",
        "MS.HUB",
        "TEXAS.HUB",
        "ARKANSAS.HUB",
    ]

    def get_fuel_mix(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get the fuel mix for a given day for a provided MISO.

        Arguments:
            date (datetime.date, str): "latest", "today", or an object
                that can be parsed as a datetime for the day to return data.

            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: DataFrame with columns "Time", "Load", "Fuel Mix"
        """
        if date != "latest":
            raise NotSupported()

        url = self.BASE + "?messageType=getfuelmix&returnType=json"
        r = self._get_json(url, verbose=verbose)

        time = pd.to_datetime(r["Fuel"]["Type"][0]["INTERVALEST"]).tz_localize(
            self.default_timezone,
        )

        mix = {}
        for fuel in r["Fuel"]["Type"]:
            amount = float(fuel["ACT"])
            mix[fuel["CATEGORY"]] = amount

        df = pd.DataFrame(mix, index=[time])
        df.index.name = "Interval Start"
        df = df.reset_index()
        df = add_interval_end(df, 5)
        return df

    def get_load(self, date: str | pd.Timestamp, verbose: bool = False) -> pd.DataFrame:
        if date == "latest":
            return self.get_load(date="today", verbose=verbose)

        elif utils.is_today(date, tz=self.default_timezone):
            r = self._get_load_data(verbose=verbose)

            date = pd.to_datetime(r["LoadInfo"]["RefId"].split(" ")[0])

            df = pd.DataFrame([x["Load"] for x in r["LoadInfo"]["FiveMinTotalLoad"]])

            df["Interval Start"] = df["Time"].apply(
                lambda x, date=date: date
                + pd.Timedelta(
                    hours=int(
                        x.split(":")[0],
                    ),
                    minutes=int(x.split(":")[1]),
                ),
            )
            df["Interval Start"] = df["Interval Start"].dt.tz_localize(
                self.default_timezone,
            )
            df = df.rename(columns={"Value": "Load"})
            df["Load"] = pd.to_numeric(df["Load"])
            df = add_interval_end(df, 5)
            return df

        else:
            raise NotSupported

    @support_date_range(frequency="DAY_START")
    def get_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        https://docs.misoenergy.org/marketreports/YYYYMMDD_df_al.xls
        """
        if date == "latest":
            return self.get_load_forecast(date="today", verbose=verbose)

        df = self._get_load_forecast_file(date)
        df = df.loc[
            df["Interval Start"] >= date,
            [col for col in df if "ActualLoad" not in col],
        ]
        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End", "Publish Time"],
        ).drop(columns=["Market Day", "HourEnding"])
        return df.sort_values("Interval Start").reset_index(drop=True)

    @support_date_range(frequency="DAY_START")
    def get_zonal_load_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        https://docs.misoenergy.org/marketreports/YYYYMMDD_df_al.xls
        """
        if date == "latest":
            yesterday = pd.Timestamp.today() - pd.Timedelta(days=1)
            return self.get_zonal_load_hourly(date=yesterday, verbose=verbose)

        if date.year < 2023:
            logger.info(
                f"Date is before 2023, getting historical zonal load data for {date.year}",
            )
            df = self.get_historical_zonal_load_hourly(date.year)
            if end is None:
                df = df[df["Interval Start"].dt.date == date.date()]
            else:
                df = df[(df["Interval Start"] >= date) & (df["Interval Start"] <= end)]
            return df.reset_index(drop=True)

        # NB: Report available is based on publish time, which is 12am the next day
        date = date + pd.Timedelta(days=1)
        df = self._get_load_forecast_file(date)

        df = df.rename(
            columns={
                "LRZ1 ActualLoad": "LRZ1",
                "LRZ2_7 ActualLoad": "LRZ2 7",
                "LRZ3_5 ActualLoad": "LRZ3 5",
                "LRZ4 ActualLoad": "LRZ4",
                "LRZ6 ActualLoad": "LRZ6",
                "LRZ8_9_10 ActualLoad": "LRZ8 9 10",
                "MISO ActualLoad": "MISO",
            },
        )

        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End"],
        ).drop(columns=["Market Day", "HourEnding"])

        df = df.sort_values("Interval Start").reset_index(drop=True)
        df = df.dropna()
        df = df.astype(
            {
                "LRZ1": float,
                "LRZ2 7": float,
                "LRZ3 5": float,
                "LRZ4": float,
                "LRZ6": float,
                "LRZ8 9 10": float,
                "MISO": float,
            },
        )
        return df[
            [
                "Interval Start",
                "Interval End",
                "LRZ1",
                "LRZ2 7",
                "LRZ3 5",
                "LRZ4",
                "LRZ6",
                "LRZ8 9 10",
                "MISO",
            ]
        ]

    def _get_load_forecast_file(self, date: str | pd.Timestamp) -> pd.DataFrame:
        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_df_al.xls"  # noqa
        logger.info(f"Downloading hourly load and load forecast data from {url}")
        df = pd.read_excel(url, sheet_name="Sheet1", skiprows=4, skipfooter=1)
        df = df.dropna(subset=["HourEnding"])
        df = df.loc[df["HourEnding"] != "HourEnding"]
        df.loc[:, "HourEnding"] = df["HourEnding"].astype(int)

        df["Interval End"] = (
            pd.to_datetime(df["Market Day"])
            + pd.to_timedelta(df["HourEnding"], unit="h")
        ).dt.tz_localize(self.default_timezone)

        df["Interval Start"] = df["Interval End"] - pd.Timedelta(hours=1)

        # Assume publish time is 12 am. MLTF is made every 15 minutes, but maybe
        # released only once a day
        # https://pubs.naruc.org/pub/64EABF52-1866-DAAC-99FB-ACEE7EEC8DAD
        df["Publish Time"] = date.normalize()

        df.columns = df.columns.map(lambda x: x.replace("(MWh)", "").strip())
        return df

    def _get_load_data(self, verbose: bool = False) -> dict:
        url = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=gettotalload&returnType=json"  # noqa
        r = self._get_json(url, verbose=verbose)
        return r

    def get_historical_zonal_load_hourly(self, year: int) -> pd.DataFrame:
        url = f"https://docs.misoenergy.org/marketreports/{year}12_dfal_HIST_xls.zip"
        logger.info(f"Downloading historical zonal load data from {url}")

        try:
            response = requests.get(url)
            if response.status_code == 404:
                raise NoDataFoundException(
                    f"No historical zonal load data found for year {year}",
                )

            zip_data = io.BytesIO(response.content)

            with zipfile.ZipFile(zip_data) as z:
                excel_filename = z.namelist()[0]
                with z.open(excel_filename) as excel_file:
                    df = pd.read_excel(
                        excel_file,
                        skiprows=5,
                        skipfooter=2,
                        dtype={"MarketDay": str, "HourEnding": str},
                    )
        except urllib.error.HTTPError:
            raise NoDataFoundException(
                f"No historical zonal load data found for year {year}",
            )
        except Exception as e:
            raise NoDataFoundException(
                f"Error reading historical zonal load data for year {year}: {str(e)}",
            )

        # NB: The first row is sometimes a header row, so we drop it
        df = df[df["MarketDay"] != "MarketDay"]
        df["MarketDay"] = pd.to_datetime(df["MarketDay"], format="%Y-%m-%d %H:%M:%S")
        df["HourEnding"] = df["HourEnding"].astype(int)
        df["Interval End"] = df["MarketDay"].dt.tz_localize(
            self.default_timezone,
        ) + pd.to_timedelta(df["HourEnding"], unit="h")
        df["Interval Start"] = df["Interval End"] - pd.Timedelta(hours=1)

        df_pivoted = df.pivot(
            index=["Interval Start", "Interval End"],
            columns="LoadResource Zone",
            values="ActualLoad (MWh)",
        ).reset_index()

        df_pivoted.columns.name = None
        if year in [2013, 2014]:
            df_pivoted["LRZ8_9_10"] = None

        df_pivoted = df_pivoted.rename(
            columns={
                "LRZ2_7": "LRZ2 7",
                "LRZ3_5": "LRZ3 5",
                "LRZ8_9_10": "LRZ8 9 10",
            },
        )
        df_pivoted = df_pivoted.astype(
            {
                "LRZ1": float,
                "LRZ2 7": float,
                "LRZ3 5": float,
                "LRZ4": float,
                "LRZ6": float,
                "LRZ8 9 10": float,
                "MISO": float,
            },
        )
        return (
            df_pivoted[
                [
                    "Interval Start",
                    "Interval End",
                    "LRZ1",
                    "LRZ2 7",
                    "LRZ3 5",
                    "LRZ4",
                    "LRZ6",
                    "LRZ8 9 10",
                    "MISO",
                ]
            ]
            .sort_values("Interval Start")
            .reset_index(drop=True)
        )

    # Older datasets do not have every region. In that case, we insert the column
    # as null
    solar_and_wind_forecast_region_cols = [
        "North",
        "Central",
        "South",
        "MISO",
    ]

    solar_and_wind_forecast_cols = [
        "Interval Start",
        "Interval End",
        "Publish Time",
        *solar_and_wind_forecast_region_cols,
    ]

    @support_date_range(frequency="DAY_START")
    def get_solar_forecast(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            return self.get_solar_forecast(date="today", verbose=verbose)

        return self._get_solar_and_wind_forecast_data(
            date,
            fuel="solar",
            verbose=verbose,
        )

    @support_date_range(frequency="DAY_START")
    def get_wind_forecast(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            return self.get_wind_forecast(date="today", verbose=verbose)

        return self._get_solar_and_wind_forecast_data(
            date,
            fuel="wind",
            verbose=verbose,
        )

    def _get_mom_forecast_report(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pd.ExcelFile:
        # Example url: https://docs.misoenergy.org/marketreports/20240327_mom.xlsx
        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_mom.xlsx"  # noqa

        logger.info(f"Downloading mom forecast data from {url}")

        try:
            # Ignore the UserWarning from openpyxl about styles
            warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
            excel_file = pd.ExcelFile(url, engine="openpyxl")
        except urllib.error.HTTPError as e:
            if e.status == 404:
                raise NoDataFoundException(
                    f"No solar or wind forecast found for {date}",
                )

        return excel_file

    def _get_solar_and_wind_forecast_data(
        self,
        date: str | pd.Timestamp,
        fuel: str,
        verbose: bool = False,
    ) -> pd.DataFrame:
        excel_file = self._get_mom_forecast_report(date, verbose)
        publish_time = pd.to_datetime(excel_file.book.properties.modified, utc=True)

        # The data schema changes on 2022-06-13
        skiprows = (
            4 if date > pd.Timestamp("2022-06-12", tz=self.default_timezone) else 3
        )

        df = (
            pd.read_excel(
                excel_file,
                sheet_name=f"{fuel.upper()} HOURLY",
                skiprows=skiprows,
                skipfooter=1,
            )
            .dropna(how="all")
            .assign(**{"Publish Time": publish_time})
        )

        # Handle older datasets
        df = df.rename(columns={"Day HE": "DAY HE"})

        # Convert column that looks like this **03/27/2024 1 **03/27/2024 24 or to
        # a valid datetime. Assume Hour Ending is in local time

        df["hour"] = df["DAY HE"].str.extract(r"(\d+)$").astype(int)
        df["date"] = pd.to_datetime(
            df["DAY HE"].str.replace("**", "").str.split(" ").str[0],
            format="%m/%d/%Y",
        )

        df["Interval Start"] = (
            pd.to_datetime(df["date"])
            + pd.to_timedelta(
                df["hour"] - 1,
                "h",
            )
            # This forecast does not handle DST changes
        ).dt.tz_localize(self.default_timezone)

        df["Interval End"] = df["Interval Start"] + pd.Timedelta(hours=1)

        for col in self.solar_and_wind_forecast_region_cols:
            if col not in df.columns:
                df[col] = pd.NA

        return df[self.solar_and_wind_forecast_cols]

    @support_date_range(frequency="W-MON")
    def get_lmp_real_time_5_min_final(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Retrieves real time final lmp data that includes price corrections to the
        preliminary real time data.

        Data from: https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=%2FMarketReportType%3AHistorical%20LMP%2FMarketReportName%3AWeekly%20Real-Time%205-Min%20LMP%20(zip)&t=10&p=0&s=MarketReportPublished&sd=desc
        """
        if date == "latest" or utils.is_today(date, tz=self.default_timezone):
            raise NotSupported("Only historical data is available for final LMPs")

        if not date.weekday() == 0:
            logger.warning("Weekly LMP data is only available for Mondays")
            logger.warning("Changing date to the previous Monday")
            date -= pd.DateOffset(days=date.weekday())

        # The data file contains data starting two weeks before the date and
        # ending one week before the date. To get data that covers the date, we
        # need to add two weeks to the date
        date += pd.DateOffset(weeks=2)

        download_url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_5MIN_LMP.zip"

        try:
            df = pd.read_csv(
                download_url,
                compression="zip",
                skiprows=4,
                skipfooter=1,
                # The c parser does not support skipfooter
                engine="python",
            )
        except urllib.error.HTTPError:
            raise NoDataFoundException(f"No LMP data found for {date}")

        return self._handle_lmp_real_time_5_min_final(df, verbose)

    def _handle_lmp_real_time_5_min_final(
        self,
        df: pd.DataFrame,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df["Interval Start"] = pd.to_datetime(df["MKTHOUR_EST"]).dt.tz_localize(
            self.default_timezone,
        )
        df = add_interval_end(df, 5).drop(columns=["Time"])

        df = df.rename(
            columns={
                "CON_LMP": "Congestion",
                "LOSS_LMP": "Loss",
                "PNODENAME": "Location",
            },
        )
        node_to_type = self._get_node_to_type_mapping(verbose)

        df = df.merge(
            node_to_type,
            left_on="Location",
            right_on="Node",
            how="left",
        )

        df["Energy"] = df["LMP"] - df["Loss"] - df["Congestion"]
        df["Market"] = Markets.REAL_TIME_5_MIN_FINAL.value

        df = utils.move_cols_to_front(
            df,
            [
                "Interval Start",
                "Interval End",
                "Market",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ],
        ).drop(columns=["MKTHOUR_EST", "Node"])

        return df.sort_values("Interval Start").reset_index(drop=True)

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["today", "historical"],
            Markets.REAL_TIME_HOURLY_FINAL: ["historical"],
            Markets.REAL_TIME_HOURLY_PRELIM: ["historical"],
        },
    )
    @support_date_range(frequency="DAY_START")
    def get_lmp(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        market: str = Markets.REAL_TIME_5_MIN,
        locations: list = "ALL",
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Supported Markets:
            - ``REAL_TIME_5_MIN`` - (Prelim ExPost 5 Minute)
            - ``DAY_AHEAD_HOURLY`` - (ExPost Day Ahead Hourly)
            - ``REAL_TIME_HOURLY_FINAL`` - (Final ExPost Real Time Hourly)
            - ``REAL_TIME_HOURLY_PRELIM`` - (Prelim ExPost Real Time Hourly)
                Only 4 days of data available, with the most recent being yesterday.
        """
        if market == Markets.REAL_TIME_5_MIN:
            latest_url = "https://api.misoenergy.org/MISORTWDBIReporter/Reporter.asmx?messageType=currentinterval&returnType=csv"  # noqa
            today_url = "https://api.misoenergy.org/MISORTWDBIReporter/Reporter.asmx?messageType=rollingmarketday&returnType=csv"  # noqa
            yesterday_url = "https://api.misoenergy.org/MISORTWDBIReporter/Reporter.asmx?messageType=previousmarketday&returnType=csv"  # noqa

            if date == "latest":
                url = latest_url
            elif utils.is_today(date, tz=self.default_timezone):
                url = today_url
            elif utils.is_yesterday(date, tz=self.default_timezone):
                url = yesterday_url
            else:
                raise NotSupported(
                    "Only today, yesterday, and latest are supported for 5 min LMPs",
                )

            logger.info(f"Downloading LMP data from {url}")
            data = pd.read_csv(url)

            data["Interval Start"] = pd.to_datetime(data["INTERVAL"]).dt.tz_localize(
                self.default_timezone,
            )

            node_to_type = self._get_node_to_type_mapping()

            data = data.merge(
                node_to_type,
                left_on="CPNODE",
                right_on="Node",
                how="left",
            )

            interval_duration = 5

        elif market in [
            Markets.REAL_TIME_HOURLY_FINAL,
            Markets.REAL_TIME_HOURLY_PRELIM,
            Markets.DAY_AHEAD_HOURLY,
        ]:
            date_str = date.strftime("%Y%m%d")

            if market == Markets.DAY_AHEAD_HOURLY:
                url = f"https://docs.misoenergy.org/marketreports/{date_str}_da_expost_lmp.csv"  # noqa
            elif market == Markets.REAL_TIME_HOURLY_FINAL:
                url = f"https://docs.misoenergy.org/marketreports/{date_str}_rt_lmp_final.csv"  # noqa
            elif market == Markets.REAL_TIME_HOURLY_PRELIM:
                url = f"https://docs.misoenergy.org/marketreports/{date_str}_rt_lmp_prelim.csv"

            logger.info(f"Downloading LMP data from {url}")
            raw_data = pd.read_csv(url, skiprows=4)
            data = self._handle_hourly_lmp(date, raw_data)
            interval_duration = 60

        data = data.sort_values(["Interval Start", "Node"])
        data = add_interval_end(data, interval_duration)

        data = data.rename(
            columns={
                "Node": "Location",
                "Type": "Location Type",
                "LMP": "LMP",
                "MLC": "Loss",
                "MCC": "Congestion",
            },
        )

        data[["LMP", "Loss", "Congestion"]] = data[["LMP", "Loss", "Congestion"]].apply(
            pd.to_numeric,
            errors="coerce",
        )

        data["Energy"] = data["LMP"] - data["Loss"] - data["Congestion"]
        data["Market"] = market.value

        data = data[
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

        data = utils.filter_lmp_locations(data, locations)

        return data

    def _handle_hourly_lmp(
        self,
        date: str | pd.Timestamp,
        raw_data: pd.DataFrame,
    ) -> pd.DataFrame:
        data_melted = raw_data.melt(
            id_vars=["Node", "Type", "Value"],
            value_vars=[col for col in raw_data.columns if col.startswith("HE")],
            var_name="HE",
            value_name="value",
        )

        data = data_melted.pivot_table(
            index=["Node", "Type", "HE"],
            columns="Value",
            values="value",
            aggfunc="first",
        ).reset_index()

        data["Interval Start"] = (
            data["HE"]
            .apply(
                lambda x: date.replace(tzinfo=None, hour=int(x.split(" ")[1]) - 1),
            )
            .dt.tz_localize(self.default_timezone)
        )

        return data

    def _get_node_to_type_mapping(self, verbose: bool = False) -> pd.DataFrame:
        # use dam to get location types
        today = utils._handle_date("today", self.default_timezone)
        url = f"https://docs.misoenergy.org/marketreports/{today.strftime('%Y%m%d')}_da_expost_lmp.csv"  # noqa
        logger.info(f"Downloading LMP data from {url}")
        today_dam_data = pd.read_csv(url, skiprows=4)
        node_to_type = (
            today_dam_data[["Node", "Type"]]
            .drop_duplicates()
            .rename(columns={"Type": "Location Type"})
        )

        return node_to_type

    def get_raw_interconnection_queue(self, verbose: bool = False) -> BinaryIO:
        url = "https://www.misoenergy.org/api/giqueue/getprojects"

        msg = f"Downloading interconnection queue from {url}"
        logger.info(msg)

        response = requests.get(url, headers="")
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose: bool = False) -> pd.DataFrame:
        """Get the interconnection queue

        Returns:
            pandas.DataFrame: Interconnection queue
        """
        raw_data = self.get_raw_interconnection_queue(verbose)
        data = json.loads(raw_data.read().decode("utf-8"))
        # todo there are also study documents available:  https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/gi-interactive-queue/
        # there is also a map that plots the locations of these projects:
        queue = pd.DataFrame(data)

        queue = queue.rename(
            columns={
                "postGIAStatus": "Post Generator Interconnection Agreement Status",
                "doneDate": "Interconnection Approval Date",
            },
        )

        queue["Capacity (MW)"] = queue[
            [
                "summerNetMW",
                "winterNetMW",
            ]
        ].max(axis=1)

        rename = {
            "projectNumber": "Queue ID",
            "county": "County",
            "state": "State",
            "transmissionOwner": "Transmission Owner",
            "poiName": "Interconnection Location",
            "queueDate": "Queue Date",
            "withdrawnDate": "Withdrawn Date",
            "applicationStatus": "Status",
            "Capacity (MW)": "Capacity (MW)",
            "summerNetMW": "Summer Capacity (MW)",
            "winterNetMW": "Winter Capacity (MW)",
            "negInService": "Proposed Completion Date",
            "fuelType": "Generation Type",
        }

        extra_columns = [
            "facilityType",
            "Post Generator Interconnection Agreement Status",
            "Interconnection Approval Date",
            "inService",
            "giaToExec",
            "studyCycle",
            "studyGroup",
            "studyPhase",
            "svcType",
            "dp1ErisMw",
            "dp1NrisMw",
            "dp2ErisMw",
            "dp2NrisMw",
            "sisPhase1",
        ]

        missing = [
            # todo the actual complettion date
            # can be calculated by looking at status and other date columns
            "Actual Completion Date",
            "Withdrawal Comment",
            "Project Name",
            "Interconnecting Entity",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        return queue

    @support_date_range(frequency="DAY_START")
    def get_generation_outages_forecast(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get the forecasted generation outages published on the date for the next
        seven days."""
        return self._get_generation_outages_data(date, type="forecast", verbose=verbose)

    @support_date_range(frequency="DAY_START")
    def get_generation_outages_estimated(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get the estimated generation outages published on the date for the past 30
        days. NOTE: since these are estimates, they change with each file published.
        """
        return self._get_generation_outages_data(date, type="actual", verbose=verbose)

    def _get_generation_outages_data(
        self,
        date: str | pd.Timestamp,
        type: str = "forecast",
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            # Latest available file is for yesterday
            date = pd.Timestamp.now(
                tz=self.default_timezone,
            ).normalize() - pd.DateOffset(days=1)

        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_mom.xlsx"  # noqa

        logger.info(f"Downloading outages {type} data from {url}")

        skiprows = 6
        nrows = 17

        if type == "actual":
            skiprows = 26

        # There's an unavoidable warning from openpyxl about styles so we suppress it
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                module=re.escape("openpyxl.styles.stylesheet"),
            )
            data = pd.read_excel(
                url,
                sheet_name="OUTAGE",
                skiprows=skiprows,
                nrows=nrows,
            )

        data.columns = [col.replace(" **", "").strip() for col in data.columns]
        data.columns = ["Region", "Type"] + list(data.columns[2:])

        # Some of the files have empty columns called "Unnamed x" that we need to drop
        data = data.drop(columns=[col for col in data.columns if "Unnamed" in col])

        data = data.melt(id_vars=["Region", "Type"], value_name="MW", var_name="Date")
        data = data.pivot(index=["Region", "Date"], columns=["Type"])

        data.columns = data.columns.droplevel(0)
        data.columns.name = None

        data = data.reset_index()

        data["Interval Start"] = pd.to_datetime(
            data["Date"],
            format="mixed",
        ).dt.tz_localize(self.default_timezone)

        data["Interval End"] = data["Interval Start"] + pd.DateOffset(days=1)
        data["Publish Time"] = date.tz_convert(self.default_timezone)

        rename_dict = {
            "Derated": "Derated Outages MW",
            "Forced": "Forced Outages MW",
            "Planned": "Planned Outages MW",
            "Unplanned": "Unplanned Outages MW",
        }

        return (
            data.rename(columns=rename_dict)[
                ["Interval Start", "Interval End", "Publish Time", "Region"]
                + list(rename_dict.values())
            ]
            .sort_values(["Interval Start", "Region"])
            .reset_index(drop=True)
        )

    @support_date_range(frequency="DAY_START")
    def get_binding_constraints_supplemental(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get the supplemental binding constraints data from MISO.

        Source URL: https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=%2FMarketReportType%3ADay-Ahead%2FMarketReportName%3ABinding Constraints Supplemental (xls)&t=10&p=0&s=MarketReportPublished&sd=desc

        Args:
            date (str | pd.Timestamp): Start date
            end (str | pd.Timestamp, optional): End date. Defaults to None.
            verbose (bool, optional): Verbosity. Defaults to False.

        Returns:
            pandas.DataFrame: Supplemental binding constraints data
        """
        query_date = date - pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_da_bcsf.xls"
        logger.info(f"Downloading supplemental binding constraints data from {url}")

        excel_file = pd.ExcelFile(url)
        market_date, publish_date = self._get_constraint_header_dates_from_excel(
            excel_file,
        )
        data = pd.read_excel(excel_file, skiprows=3)
        data["Date"] = market_date

        return data[
            [
                "Date",
                "Constraint ID",
                "Constraint Name",
                "Contingency Name",
                "Constraint Type",
                "Flowgate Name",
                "Device Type",
                "Key1",
                "Key2",
                "Key3",
                "Direction",
                "From Area",
                "To Area",
                "From Station",
                "To Station",
                "From KV",
                "To KV",
            ]
        ]

    @support_date_range(frequency="DAY_START")
    def get_binding_constraints_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        query_date = date - pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_da_bc.xls"
        logger.info(f"Downloading day-ahead binding constraints data from {url}")

        excel_file = pd.ExcelFile(url)
        market_date, publish_date = self._get_constraint_header_dates_from_excel(
            excel_file,
        )
        data = pd.read_excel(
            excel_file,
            skiprows=3,
            dtype={
                "Constraint Description": object,
                "Reason": object,
                "Shadow Price": float,
                "BP1": float,
                "PC1": float,
                "BP2": float,
                "PC2": float,
            },
        )

        data["Interval End"] = market_date + pd.to_timedelta(
            data["Hour of Occurrence"],
            unit="h",
        )
        data["Interval Start"] = data["Interval End"] - pd.Timedelta(hours=1)
        data = data.rename(
            columns={
                "Branch Name ( Branch Type / From CA / To CA )": "Branch Name",
                "Constraint_ID": "Constraint ID",
            },
        )

        return data[
            [
                "Interval Start",
                "Interval End",
                "Flowgate NERC ID",
                "Constraint ID",
                "Constraint Name",
                "Branch Name",
                "Contingency Description",
                "Shadow Price",
                "Constraint Description",
                "Override",
                "Curve Type",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
                "Reason",
            ]
        ]

    # NOTE(kladar): Mostly this method is used for efficient backfilling
    def get_binding_constraints_day_ahead_yearly_historical(
        self,
        year: int,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get the day-ahead binding constraints data from MISO for a given year.

        Args:
            year (int): Year
            verbose (bool, optional): Verbosity. Defaults to False.

        Returns:
            pandas.DataFrame: Historical day-ahead binding constraints data
        """
        url = f"https://docs.misoenergy.org/marketreports/{year}_da_bc_HIST.csv"
        logger.info(f"Downloading day-ahead binding constraints data from {url}")

        data = pd.read_csv(url)
        data["Interval End"] = pd.to_datetime(data["Market Date"]).dt.tz_localize(
            self.default_timezone,
        ) + pd.to_timedelta(data["Hour of Occurrence"], unit="h")
        data["Interval Start"] = data["Interval End"] - pd.Timedelta(hours=1)

        data = data.rename(
            columns={
                "Branch Name ( Branch Type / From CA / To CA )": "Branch Name",
            },
        )
        data = data.rename(
            columns={
                "Constraint_ID": "Constraint ID",
            },
        )

        return data[
            [
                "Interval Start",
                "Interval End",
                "Flowgate NERC ID",
                "Constraint ID",
                "Constraint Name",
                "Branch Name",
                "Contingency Description",
                "Shadow Price",
                "Constraint Description",
                "Override",
                "Curve Type",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
                "Reason",
            ]
        ]

    @support_date_range(frequency="DAY_START")
    def get_subregional_power_balance_constraints_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        query_date = date - pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_da_pbc.csv"
        logger.info(
            f"Downloading day-ahead subregional power balance constraints data from {url}",
        )

        data = pd.read_csv(
            url,
            skiprows=3,
            index_col=False,
            dtype={
                "PRELIMINARY_SHADOW_PRICE": float,
                "BP1": float,
                "PC1": float,
                "BP2": float,
                "PC2": float,
                "BP3": float,
                "PC3": float,
                "BP4": float,
                "PC4": float,
                "REASON": object,
            },
        )

        # NOTE(kladar): The last row is a text disclaimer, and there is a leading space
        # in the column names, so we clean it all up.
        data = data.iloc[:-1]
        data.columns = data.columns.str.strip()
        if data.empty:
            return data[
                [
                    "Interval Start",
                    "Interval End",
                    "CONSTRAINT_NAME",
                    "PRELIMINARY_SHADOW_PRICE",
                    "CURVETYPE",
                    "BP1",
                    "PC1",
                    "BP2",
                    "PC2",
                    "BP3",
                    "PC3",
                    "BP4",
                    "PC4",
                    "OVERRIDE",
                    "REASON",
                ]
            ]

        data["Interval End"] = pd.to_datetime(data["MARKET_HOUR_EST"]).dt.tz_localize(
            self.default_timezone,
        )
        data["Interval Start"] = data["Interval End"] - pd.Timedelta(hours=1)

        return data[
            [
                "Interval Start",
                "Interval End",
                "CONSTRAINT_NAME",
                "PRELIMINARY_SHADOW_PRICE",
                "CURVETYPE",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
                "BP3",
                "PC3",
                "BP4",
                "PC4",
                "OVERRIDE",
                "REASON",
            ]
        ]

    @support_date_range(frequency="DAY_START")
    def get_reserve_product_binding_constraints_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        query_date = date - pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_da_rpe.xls"
        logger.info(
            f"Downloading day-ahead reserve product binding constraints data from {url}",
        )

        excel_file = pd.ExcelFile(url)
        market_date, publish_date = self._get_constraint_header_dates_from_excel(
            excel_file,
        )
        data = pd.read_excel(excel_file, skiprows=3)
        data = data.iloc[:-1]
        print(data)
        print(market_date)
        data["Interval End"] = market_date + pd.to_timedelta(
            data[
                "Hour of Occurence"
            ],  # NOTE(kladar): sic, this is a persistent typo in the header from MISO
            unit="h",
        )

        if data.empty:
            return data[
                [
                    "Interval Start",
                    "Interval End",
                    "Constraint Name",
                    "Shadow Price",
                    "Constraint Description",
                ]
            ]

        data["Interval Start"] = data["Interval End"] - pd.Timedelta(hours=1)

        return data[
            [
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Shadow Price",
                "Constraint Description",
            ]
        ]

    @support_date_range(frequency="DAY_START")
    def get_binding_constraints_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        query_date = date + pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_rt_bc.xls"
        logger.info(f"Downloading real-time binding constraints data from {url}")

        excel_file = pd.ExcelFile(url)
        market_date, publish_date = self._get_constraint_header_dates_from_excel(
            excel_file,
        )
        data = pd.read_excel(
            excel_file,
            skiprows=3,
            dtype={
                "Constraint Description": object,
                "Preliminary Shadow Price": float,
                "BP1": float,
                "PC1": float,
                "BP2": float,
                "PC2": float,
            },
        )

        data["Interval End"] = pd.to_datetime(
            market_date.strftime("%Y-%m-%d")
            + " "
            + data[
                "Hour of  Occurrence"
            ],  # NOTE(kladar): sic, there are two spaces between "Hour of" and "Occurrence"
        ).dt.tz_localize(
            self.default_timezone,
        )

        data["Interval Start"] = data["Interval End"] - pd.Timedelta(minutes=5)
        data = data.rename(
            columns={
                "Branch Name ( Branch Type / From CA / To CA )": "Branch Name",
            },
        )

        return data[
            [
                "Interval Start",
                "Interval End",
                "Flowgate NERC ID",
                "Constraint ID",
                "Constraint Name",
                "Branch Name",
                "Contingency Description",
                "Preliminary Shadow Price",
                "Constraint Description",
                "Override",
                "Curve Type",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
            ]
        ]

    # NOTE(kladar): Mostly this method is used for efficient backfilling
    def get_binding_constraints_real_time_yearly_historical(
        self,
        year: int,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get the real-time binding constraints data from MISO for a given year.

        Args:
            year (int): Year
            verbose (bool, optional): Verbosity. Defaults to False.

        Returns:
            pandas.DataFrame: Historical real-time binding constraints data
        """
        url = f"https://docs.misoenergy.org/marketreports/{year}_rt_bc_HIST.csv"
        logger.info(f"Downloading real-time binding constraints data from {url}")

        data = pd.read_csv(
            url,
            skiprows=2,
            dtype={
                "Constraint Description": object,
            },
        )
        data = data.iloc[
            :-2
        ]  # NOTE(kladar): The last two rows are report descriptions and disclaimers
        data["Interval End"] = pd.to_datetime(
            data["Market Date"] + " " + data["Hour of Occurrence"],
        ).dt.tz_localize(
            self.default_timezone,
        )
        data["Interval Start"] = data["Interval End"] - pd.Timedelta(hours=1)

        data = data.rename(
            columns={
                "Branch Name ( Branch Type / From CA / To CA )": "Branch Name",
            },
        )
        data = data.rename(
            columns={
                "Flowgate NERCID": "Flowgate NERC ID",
                "Constraint_ID": "Constraint ID",
            },
        )

        return data[
            [
                "Interval Start",
                "Interval End",
                "Flowgate NERC ID",
                "Constraint ID",
                "Constraint Name",
                "Branch Name",
                "Contingency Description",
                "Preliminary Shadow Price",
                "Constraint Description",
                "Override",
                "Curve Type",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
            ]
        ]

    @support_date_range(frequency="DAY_START")
    def get_binding_constraint_overrides_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        query_date = date + pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_rt_or.xls"
        logger.info(
            f"Downloading real-time binding constraint overrides data from {url}",
        )

        excel_file = pd.ExcelFile(url)
        market_date, publish_date = self._get_constraint_header_dates_from_excel(
            excel_file,
        )
        data = pd.read_excel(
            excel_file,
            skiprows=3,
            dtype={
                "Constraint Description": object,
                "BP1": float,
                "PC1": float,
                "BP2": float,
                "PC2": float,
                "Reason": object,
            },
        )

        data["Interval End"] = pd.to_datetime(
            market_date.strftime("%Y-%m-%d")
            + " "
            + data[
                "Hour of  Occurrence"
            ],  # NOTE(kladar): sic, there are two spaces between "Hour of" and "Occurrence"
        ).dt.tz_localize(self.default_timezone)

        data["Interval Start"] = data["Interval End"] - pd.Timedelta(minutes=5)

        data = data.rename(
            columns={
                "Branch Name ( Branch Type / From CA / To CA )": "Branch Name",
            },
        )
        return data[
            [
                "Interval Start",
                "Interval End",
                "Flowgate NERC ID",
                "Constraint Name",
                "Branch Name",
                "Contingency Description",
                "Preliminary Shadow Price",
                "Constraint Description",
                "Override",
                "Curve Type",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
                "Reason",
            ]
        ]

    @support_date_range(frequency="DAY_START")
    def get_subregional_power_balance_constraints_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        query_date = date + pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_rt_pbc.csv"
        logger.info(
            f"Downloading real-time subregional power balance constraints data from {url}",
        )

        data = pd.read_csv(
            url,
            skiprows=3,
            index_col=False,
            dtype={
                "PRELIMINARY_SHADOW_PRICE": float,
                "BP1": float,
                "PC1": float,
                "BP2": float,
                "PC2": float,
                "BP3": float,
                "PC3": float,
                "BP4": float,
                "PC4": float,
                "REASON": object,
            },
        )

        # NOTE(kladar): The last row is a text disclaimer, and there is a leading space
        # in the column names, so we clean it all up.
        data = data.iloc[:-1]
        data.columns = data.columns.str.strip()
        if data.empty:
            return data[
                [
                    "Interval Start",
                    "Interval End",
                    "CONSTRAINT_NAME",
                    "PRELIMINARY_SHADOW_PRICE",
                    "CURVETYPE",
                    "BP1",
                    "PC1",
                    "BP2",
                    "PC2",
                    "BP3",
                    "PC3",
                    "BP4",
                    "PC4",
                    "OVERRIDE",
                    "REASON",
                ]
            ]

        data["Interval End"] = pd.to_datetime(data["MARKET_HOUR_EST"]).dt.tz_localize(
            self.default_timezone,
        )
        data["Interval Start"] = data["Interval End"] - pd.Timedelta(minutes=5)

        return data[
            [
                "Interval Start",
                "Interval End",
                "CONSTRAINT_NAME",
                "PRELIMINARY_SHADOW_PRICE",
                "CURVETYPE",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
                "BP3",
                "PC3",
                "BP4",
                "PC4",
                "OVERRIDE",
                "REASON",
            ]
        ]

    @support_date_range(frequency="DAY_START")
    def get_reserve_product_binding_constraints_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        query_date = date + pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_rt_rpe.xls"
        logger.info(
            f"Downloading real-time reserve product binding constraints data from {url}",
        )

        data = pd.read_excel(url, skiprows=3)

        # NOTE(kladar): The last row is a text disclaimer, and there is a leading space
        # in the column names, so we clean it all up.
        data = data.iloc[:-1]
        if data.empty:
            return data[
                [
                    "Interval Start",
                    "Interval End",
                    "Constraint Name",
                    "Shadow Price",
                    "Constraint Description",
                ]
            ]

        data["Interval End"] = pd.to_datetime(data["Time of Occurence"]).dt.tz_localize(
            self.default_timezone,
        )  # NOTE(kladar) sic, this is a persistent typo from MISO
        data["Interval Start"] = data["Interval End"] - pd.Timedelta(minutes=5)

        return data[
            [
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Shadow Price",
                "Constraint Description",
            ]
        ]

    def _get_constraint_header_dates_from_excel(
        self,
        file: pd.ExcelFile,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        header = pd.read_excel(file, nrows=2, usecols=[0])
        market_date = pd.to_datetime(header.iloc[0, 0].split(": ")[1]).tz_localize(
            self.default_timezone,
        )
        publish_date = pd.to_datetime(header.iloc[1, 0].split(": ")[1]).tz_localize(
            self.default_timezone,
        )
        return market_date, publish_date

    @support_date_range(frequency="DAY_START")
    def get_look_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            return self.get_look_ahead_hourly(date="today", verbose=verbose)

        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_sr_la_rg.csv"
        logger.info(f"Downloading look-ahead hourly data from {url}")

        publish_time = date.normalize()

        df = pd.read_csv(url, skiprows=3, skipfooter=15, engine="python")
        id_cols = ["Hourend_EST", "Region"]
        value_cols = [col for col in df.columns if col not in id_cols]

        df_melted = df.melt(
            id_vars=id_cols,
            value_vars=value_cols,
            var_name="date_col",
            value_name="value",
        )

        df_melted["date"] = df_melted["date_col"].str.extract(r"(\d{2}/\d{2}/\d{4})")
        df_melted["type"] = (
            df_melted["date_col"]
            .str.contains("Outage")
            .map({True: "Outage", False: "MTLF"})
        )

        df_wide = df_melted.pivot(
            index=["Hourend_EST", "Region", "date"],
            columns="type",
            values="value",
        ).reset_index()

        df_wide["Hour Ending"] = (
            df_wide["Hourend_EST"]
            .str.extract(r"Hour   (\d+)")[0]
            .astype(int)
            .sub(1)  # Subtract 1 to convert from 1-24 to 0-23
            .astype(str)
            .str.zfill(2)
        )

        df_wide["Interval End"] = pd.to_datetime(
            df_wide["date"] + " " + df_wide["Hour Ending"],
            format="mixed",
        ).dt.tz_localize(self.default_timezone, nonexistent="shift_forward")
        df_wide["Interval End"] = df_wide["Interval End"] + pd.Timedelta(
            hours=1,
        )  # Add back hour from 1-24 to 0-23 conversion
        df_wide["Interval Start"] = df_wide["Interval End"] - pd.Timedelta(hours=1)
        df_wide["Publish Time"] = publish_time
        df_wide["MTLF"] = df_wide["MTLF"] * 1000  # GW to MW
        df_wide["Outage"] = df_wide["Outage"] * 1000  # GW to MW

        final_df = (
            df_wide[
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Region",
                    "MTLF",
                    "Outage",
                ]
            ]
            .sort_values(["Interval Start", "Region"])
            .reset_index(drop=True)
        )

        return final_df

    @support_date_range(frequency=None)
    def get_interchange_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date != "latest":
            raise NotSupported(
                "Only latest MISO interchange data is available. Use 'latest' as date.",
            )

        # The actuals are only available as JSON (the csv is empty). Data in this file
        # is in UTC.
        actual_url = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=getimporttotal5&returnType=json"

        # Data in this file is in the default timezone
        scheduled_url = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=getNSI5&returnType=csv"

        logger.info(
            f"Downloading interchange data from {scheduled_url} and {actual_url}",
        )

        actual_data = pd.read_json(actual_url)

        actual_data["Time"] = pd.to_datetime(
            actual_data["Time"],
            utc=True,
            # Example: 2025-06-05 8:20:00 PM
            format="%Y-%m-%d %I:%M:%S %p",
        ).dt.tz_convert(self.default_timezone)

        scheduled_data = pd.read_csv(
            scheduled_url,
            skiprows=2,
        )

        scheduled_data["timestamp"] = pd.to_datetime(
            scheduled_data["timestamp"],
        ).dt.tz_localize(self.default_timezone)

        data = scheduled_data.merge(
            actual_data,
            left_on="timestamp",
            right_on="Time",
            how="outer",
        ).rename(
            columns={
                "timestamp": "Interval End",
                "Value": "Net Actual Interchange",
                "MISO": "Net Scheduled Interchange",
            },
        )

        data["Interval End"] = data["Interval End"].fillna(data["Time"])
        data = data.drop(columns=["Time"])

        data["Interval Start"] = data["Interval End"] - pd.Timedelta(minutes=5)

        # The actual data does not go as far back as the scheduled data so it has NAs.
        # The scheduled data lags the actual by 1 interval so it has NAs as well.
        # We must use Pandas nullable integer type to avoid issues with NAs.
        # https://pandas.pydata.org/docs/user_guide/integer_na.html
        for col in data:
            if col not in ["Interval Start", "Interval End"]:
                data[col] = data[col].astype("Int64")

        return (
            data[
                [
                    "Interval Start",
                    "Interval End",
                    "Net Scheduled Interchange",
                    "Net Actual Interchange",
                    "AECI",
                    "LGEE",
                    "MHEB",
                    "ONT",
                    "PJM",
                    "SOCO",
                    "SPA",
                    "SWPP",
                    "TVA",
                ]
            ]
            .sort_values("Interval Start")
            .reset_index(drop=True)
        )

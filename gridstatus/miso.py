import io
import json
import re
import urllib
import warnings
import zipfile
from typing import BinaryIO, Dict

import pandas as pd
import polars as pl
import requests

from gridstatus import utils
from gridstatus.base import ISOBase, Markets, NoDataFoundException, NotSupported
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import logger
from gridstatus.lmp_config import lmp_config


def add_interval_end(df: pl.DataFrame, duration_min: int) -> pl.DataFrame:
    """Add an interval end column to a dataframe

    Args:
        df (polars.DataFrame): Dataframe with a time column
        duration_min (int): Interval duration in minutes

    Returns:
        polars.DataFrame: Dataframe with an interval end column
    """
    return utils.move_cols_to_front(
        df.with_columns(
            (pl.col("Interval Start") + pl.duration(minutes=duration_min)).alias(
                "Interval End",
            ),
            pl.col("Interval Start").alias("Time"),
        ),
        ["Time", "Interval Start", "Interval End"],
    )


def _cast_constraint_description_utf8(df: pl.DataFrame) -> pl.DataFrame:
    if "Constraint Description" in df.columns:
        return df.with_columns(pl.col("Constraint Description").cast(pl.Utf8))
    return df


"""
Notes

- Real-time 5-minute LMP data for current day, previous day available
https://public-api.misoenergy.org/api/MarketPricing/GetRealTimeFiveMinExPost/Rolling

- market reports https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=
historical fuel mix: https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=%2FMarketReportType%3ASummary%2FMarketReportName%3AHistorical%20Generation%20Fuel%20Mix%20(xlsx)&t=10&p=0&s=MarketReportPublished&sd=desc

- ancillary services available in consolidate api

"""  # noqa


class MISO(ISOBase):
    """Midcontinent Independent System Operator (MISO)"""

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
    ) -> pl.DataFrame:
        """Get the fuel mix for a given day for a provided MISO.

        Arguments:
            date (datetime.date, str): "latest", "today", "yesterday", or an object
                that can be parsed as a datetime for the day to return data.

            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: DataFrame with columns "Time", "Load", "Fuel Mix"
        """
        if date == "latest":
            url = "https://public-api.misoenergy.org/api/FuelMix"
        elif utils.is_today(date, tz=self.default_timezone):
            url = "https://public-api.misoenergy.org/api/FuelMix/Today"
        elif utils.is_yesterday(date, tz=self.default_timezone):
            url = "https://public-api.misoenergy.org/api/FuelMix/Yesterday"
        else:
            raise NotSupported(
                "Only 'latest', 'today', and yesterday's date are supported",
            )

        response_json = self._get_json(url, verbose=verbose)
        df = self._parse_fuel_mix(response_json)
        if date == "latest" and df.height > 1:
            df = df.filter(pl.col("Interval Start") == pl.col("Interval Start").max())
        return df

    def _parse_fuel_mix(self, raw_json: Dict[str, dict]) -> pl.DataFrame:
        df = pl.from_pandas(pd.json_normalize(raw_json["Fuel"]["Type"]))
        df = df.with_columns(
            pl.col("INTERVALEST")
            .str.to_datetime(format="%Y-%m-%d %I:%M:%S %p")
            .dt.replace_time_zone(self.default_timezone)
            .alias("INTERVALEST"),
        )
        df_pivoted = df.pivot(
            "CATEGORY",
            index="INTERVALEST",
            values="ACT",
            aggregate_function="first",
        ).rename(
            {"INTERVALEST": "Interval Start"},
        )
        df_pivoted = add_interval_end(df_pivoted, 5)
        for col in [
            "Battery Storage",
            "Coal",
            "Imports",
            "Natural Gas",
            "Nuclear",
            "Other",
            "Solar",
            "Wind",
        ]:
            if col in df_pivoted.columns:
                df_pivoted = df_pivoted.with_columns(pl.col(col).cast(pl.Int64))
        output_cols = [
            "Time",
            "Interval Start",
            "Interval End",
            "Battery Storage",
            "Coal",
            "Imports",
            "Natural Gas",
            "Nuclear",
            "Other",
            "Solar",
            "Wind",
        ]
        return df_pivoted.select([c for c in output_cols if c in df_pivoted.columns])

    def get_load(self, date: str | pd.Timestamp, verbose: bool = False) -> pl.DataFrame:
        if date == "latest":
            return self.get_load(date="today", verbose=verbose)

        elif utils.is_today(date, tz=self.default_timezone):
            r = self._get_load_data(verbose=verbose)

            date = pd.to_datetime(r["LoadInfo"]["RefId"].split(" ")[0])

            df = pl.DataFrame([x["Load"] for x in r["LoadInfo"]["FiveMinTotalLoad"]])

            def _time_to_interval_start(time_str: str) -> pd.Timestamp:
                hours, minutes = map(int, time_str.split(":"))
                return (date + pd.Timedelta(hours=hours, minutes=minutes)).tz_localize(
                    self.default_timezone,
                )

            df = df.with_columns(
                pl.col("Time")
                .map_elements(
                    _time_to_interval_start,
                    return_dtype=pl.Datetime(time_zone=self.default_timezone),
                )
                .alias("Interval Start"),
                pl.col("Value").cast(pl.Float64).alias("Load"),
            )
            return add_interval_end(df.select(["Interval Start", "Load"]), 5)

        else:
            raise NotSupported

    @support_date_range(frequency="DAY_START")
    def get_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        https://docs.misoenergy.org/marketreports/YYYYMMDD_df_al.xls
        """
        if date == "latest":
            return self.get_load_forecast(date="today", verbose=verbose)

        df = self._get_load_forecast_file(date)
        cols = [col for col in df.columns if "ActualLoad" not in col]
        return (
            utils.move_cols_to_front(
                df.filter(pl.col("Interval Start") >= date).select(cols),
                ["Interval Start", "Interval End", "Publish Time"],
            )
            .drop(["Market Day", "HourEnding"])
            .sort("Interval Start")
        )

    @support_date_range(frequency="DAY_START")
    def get_zonal_load_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
                df = df.filter(pl.col("Interval Start").dt.date() == date.date())
            else:
                df = df.filter(
                    (pl.col("Interval Start") >= date)
                    & (pl.col("Interval Start") <= end),
                )
            return df

        # NB: Report available is based on publish time, which is 12am the next day
        date = date + pd.Timedelta(days=1)
        df = self._get_load_forecast_file(date)

        df = df.rename(
            {
                "LRZ1 ActualLoad": "LRZ1",
                "LRZ2_7 ActualLoad": "LRZ2 7",
                "LRZ3_5 ActualLoad": "LRZ3 5",
                "LRZ4 ActualLoad": "LRZ4",
                "LRZ6 ActualLoad": "LRZ6",
                "LRZ8_9_10 ActualLoad": "LRZ8 9 10",
                "MISO ActualLoad": "MISO",
            },
        )

        zonal_cols = [
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
        return (
            utils.move_cols_to_front(
                df,
                ["Interval Start", "Interval End"],
            )
            .drop(["Market Day", "HourEnding"])
            .sort("Interval Start")
            .drop_nulls()
            .with_columns(
                pl.col("LRZ1").cast(pl.Float64),
                pl.col("LRZ2 7").cast(pl.Float64),
                pl.col("LRZ3 5").cast(pl.Float64),
                pl.col("LRZ4").cast(pl.Float64),
                pl.col("LRZ6").cast(pl.Float64),
                pl.col("LRZ8 9 10").cast(pl.Float64),
                pl.col("MISO").cast(pl.Float64),
            )
            .select(zonal_cols)
        )

    def _get_load_forecast_file(self, date: str | pd.Timestamp) -> pl.DataFrame:
        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_df_al.xls"  # noqa
        logger.info(f"Downloading hourly load and load forecast data from {url}")
        # Locate the header row dynamically: MISO changed the file layout on
        # 2026-04-27 so the headers moved from row 4 to row 6, with extra blank
        # columns inserted from merged-cell artifacts.
        raw = pd.read_excel(url, sheet_name="Sheet1", header=None)
        header_row = raw.index[raw.iloc[:, 0].astype(str).str.strip() == "Market Day"][
            0
        ]
        df = raw.iloc[header_row + 1 :].copy()
        df.columns = raw.iloc[header_row].values
        df = df.loc[:, df.columns.notna()]
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
        return pl.from_pandas(df)

    def _get_load_data(self, verbose: bool = False) -> dict:
        url = "https://public-api.misoenergy.org/api/RealTimeTotalLoad"
        r = self._get_json(url, verbose=verbose)
        return r

    def get_historical_zonal_load_hourly(self, year: int) -> pl.DataFrame:
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
        zonal_cols = [
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
        return (
            pl.from_pandas(df_pivoted)
            .with_columns(
                pl.col("LRZ1").cast(pl.Float64),
                pl.col("LRZ2 7").cast(pl.Float64),
                pl.col("LRZ3 5").cast(pl.Float64),
                pl.col("LRZ4").cast(pl.Float64),
                pl.col("LRZ6").cast(pl.Float64),
                pl.col("LRZ8 9 10").cast(pl.Float64),
                pl.col("MISO").cast(pl.Float64),
            )
            .select(zonal_cols)
            .sort("Interval Start")
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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
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
    ) -> pl.DataFrame:
        excel_file = self._get_mom_forecast_report(date, verbose)
        publish_time = pd.to_datetime(excel_file.book.properties.modified, utc=True)

        skiprows = (
            4 if date > pd.Timestamp("2022-06-12", tz=self.default_timezone) else 3
        )

        def _process_forecast(df: pd.DataFrame) -> pd.DataFrame:
            return df.dropna(how="all")

        df = utils.read_excel_via_pandas(
            excel_file,
            sheet_name=f"{fuel.upper()} HOURLY",
            skiprows=skiprows,
            skipfooter=1,
            process=_process_forecast,
        )

        df = df.with_columns(pl.lit(publish_time).alias("Publish Time"))

        if "Day HE" in df.columns:
            df = df.rename({"Day HE": "DAY HE"})

        df = df.with_columns(
            pl.col("DAY HE").str.extract(r"(\d+)$", 1).cast(pl.Int64).alias("hour"),
            pl.col("DAY HE")
            .str.replace_all(r"\*\*", "")
            .str.split(" ")
            .list.first()
            .str.to_datetime(format="%m/%d/%Y")
            .alias("date"),
        )

        df = df.with_columns(
            (pl.col("date") + pl.duration(hours=pl.col("hour") - 1))
            .dt.replace_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )
        df = df.with_columns(
            (pl.col("Interval Start") + pl.duration(hours=1)).alias("Interval End"),
        )

        for col in self.solar_and_wind_forecast_region_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

        return df.select(self.solar_and_wind_forecast_cols)

    @support_date_range(frequency="W-MON")
    def get_lmp_real_time_5_min_final(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
            df = utils.read_csv_exotic_via_pandas(
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
        df: pl.DataFrame,
        verbose: bool = False,
    ) -> pl.DataFrame:
        df = df.with_columns(
            pl.from_pandas(
                pd.to_datetime(df["MKTHOUR_EST"].to_pandas()).dt.tz_localize(
                    self.default_timezone,
                ),
            ).alias("Interval Start"),
        )
        df = add_interval_end(df, 5).drop("Time")

        df = df.rename(
            {
                "CON_LMP": "Congestion",
                "LOSS_LMP": "Loss",
                "PNODENAME": "Location",
            },
        )
        node_to_type = self._get_node_to_type_mapping(verbose)

        df = df.join(
            node_to_type,
            left_on="Location",
            right_on="Node",
            how="left",
        )

        df = df.with_columns(
            (pl.col("LMP") - pl.col("Loss") - pl.col("Congestion")).alias("Energy"),
            pl.lit(Markets.REAL_TIME_5_MIN_FINAL.value).alias("Market"),
        )

        return (
            utils.move_cols_to_front(
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
            )
            .drop([c for c in ["MKTHOUR_EST", "Time", "Node"] if c in df.columns])
            .sort("Interval Start")
        )

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["today", "historical"],
            Markets.REAL_TIME_HOURLY_FINAL: ["historical"],
            Markets.REAL_TIME_HOURLY_PRELIM: ["historical"],
        },
    )
    @support_date_range(frequency="DAY_START")
    def _get_lmp(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        market: str = Markets.REAL_TIME_5_MIN,
        locations: list = "ALL",
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Supported Markets:
            - ``REAL_TIME_5_MIN`` - (Prelim ExPost 5 Minute)
            - ``DAY_AHEAD_HOURLY`` - (ExPost Day Ahead Hourly)
            - ``REAL_TIME_HOURLY_FINAL`` - (Final ExPost Real Time Hourly)
            - ``REAL_TIME_HOURLY_PRELIM`` - (Prelim ExPost Real Time Hourly)
                Only 4 days of data available, with the most recent being yesterday.
        """
        if market == Markets.REAL_TIME_5_MIN:
            latest_url = "https://public-api.misoenergy.org/api/MarketPricing/GetRealTimeFiveMinExPost/Current"
            today_url = "https://public-api.misoenergy.org/api/MarketPricing/GetRealTimeFiveMinExPost/Rolling"
            yesterday_url = "https://public-api.misoenergy.org/api/MarketPricing/GetRealTimeFiveMinExPost/Previous"

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
            response_json = self._get_json(url, verbose=verbose)

            data = pl.from_pandas(
                pd.DataFrame(response_json["data"], columns=response_json["headers"]),
            )

            data = data.with_columns(
                pl.col("INTERVAL")
                .str.to_datetime()
                .dt.replace_time_zone(self.default_timezone)
                .alias("Interval Start"),
            )

            node_to_type_mapping = self._get_node_to_type_mapping()
            data = data.join(
                node_to_type_mapping,
                left_on="CPNODE",
                right_on="Node",
                how="left",
            ).rename({"CPNODE": "Node"})

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
            raw_data = pl.read_csv(url, skip_rows=4)
            data = self._handle_hourly_lmp(date, raw_data)
            interval_duration = 60

        data = data.sort(["Interval Start", "Node"])
        data = add_interval_end(data, interval_duration)

        rename_map = {
            "Node": "Location",
            "LMP": "LMP",
            "MLC": "Loss",
            "MCC": "Congestion",
        }
        if "Type" in data.columns:
            rename_map["Type"] = "Location Type"
        data = data.rename(rename_map)

        data = data.with_columns(
            pl.col("LMP").cast(pl.Float64, strict=False),
            pl.col("Loss").cast(pl.Float64, strict=False),
            pl.col("Congestion").cast(pl.Float64, strict=False),
        )

        data = data.with_columns(
            (pl.col("LMP") - pl.col("Loss") - pl.col("Congestion")).alias("Energy"),
            pl.lit(market.value).alias("Market"),
        )

        data = data.select(
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
            ],
        )

        return utils.filter_lmp_locations(data, locations)

    @lmp_config(
        supports={
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
            Markets.DAY_AHEAD_HOURLY: ["today", "historical"],
            Markets.REAL_TIME_HOURLY_FINAL: ["historical"],
            Markets.REAL_TIME_HOURLY_PRELIM: ["historical"],
        },
    )
    def get_lmp(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        market: str = Markets.REAL_TIME_5_MIN,
        locations: list = "ALL",
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Deprecated. Use the per-dataset methods instead:
        :meth:`get_lmp_real_time_5_min`, :meth:`get_lmp_day_ahead_hourly`,
        :meth:`get_lmp_real_time_hourly_prelim`,
        :meth:`get_lmp_real_time_hourly_final`.
        """
        warnings.warn(
            "MISO.get_lmp is deprecated; use the per-dataset methods "
            "get_lmp_real_time_5_min, get_lmp_day_ahead_hourly, "
            "get_lmp_real_time_hourly_prelim, or get_lmp_real_time_hourly_final "
            "instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._get_lmp(
            date,
            end=end,
            market=market,
            locations=locations,
            verbose=verbose,
        )

    def get_lmp_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get prelim ExPost real-time 5-minute LMPs for all nodes.

        Only today, yesterday, and "latest" are supported.
        """
        return self._get_lmp(
            date,
            end=end,
            market=Markets.REAL_TIME_5_MIN,
            locations="ALL",
            verbose=verbose,
        )

    def get_lmp_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get ExPost day-ahead hourly LMPs for all nodes."""
        return self._get_lmp(
            date,
            end=end,
            market=Markets.DAY_AHEAD_HOURLY,
            locations="ALL",
            verbose=verbose,
        )

    def get_lmp_real_time_hourly_prelim(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get prelim ExPost real-time hourly LMPs for all nodes.

        Only 4 days of data available, with the most recent being yesterday.
        """
        return self._get_lmp(
            date,
            end=end,
            market=Markets.REAL_TIME_HOURLY_PRELIM,
            locations="ALL",
            verbose=verbose,
        )

    def get_lmp_real_time_hourly_final(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get final ExPost real-time hourly LMPs for all nodes."""
        return self._get_lmp(
            date,
            end=end,
            market=Markets.REAL_TIME_HOURLY_FINAL,
            locations="ALL",
            verbose=verbose,
        )

    def _handle_hourly_lmp(
        self,
        date: str | pd.Timestamp,
        raw_data: pl.DataFrame,
    ) -> pl.DataFrame:
        he_cols = [col for col in raw_data.columns if col.startswith("HE")]
        data_melted = raw_data.unpivot(
            index=["Node", "Type", "Value"],
            on=he_cols,
            variable_name="HE",
            value_name="value",
        )

        data = data_melted.pivot(
            "Value",
            index=["Node", "Type", "HE"],
            values="value",
            aggregate_function="first",
        )

        base = pl.lit(
            date.replace(tzinfo=None).replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            ),
        )
        data = data.with_columns(
            (
                base
                + pl.duration(
                    hours=pl.col("HE").str.split(" ").list.get(1).cast(pl.Int64) - 1,
                )
            )
            .dt.replace_time_zone(self.default_timezone)
            .alias("Interval Start"),
        )

        return data

    def _get_node_to_type_mapping(self, verbose: bool = False) -> pl.DataFrame:
        today = utils._handle_date("today", self.default_timezone)
        url = f"https://docs.misoenergy.org/marketreports/{today.strftime('%Y%m%d')}_da_expost_lmp.csv"  # noqa
        logger.info(f"Downloading LMP data from {url}")
        today_dam_data = pl.read_csv(url, skip_rows=4)
        return (
            today_dam_data.select(["Node", "Type"])
            .unique()
            .rename({"Type": "Location Type"})
        )

    def get_raw_interconnection_queue(self, verbose: bool = False) -> BinaryIO:
        url = "https://www.misoenergy.org/api/giqueue/getprojects"

        msg = f"Downloading interconnection queue from {url}"
        logger.info(msg)

        response = requests.get(url, headers="")
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose: bool = False) -> pl.DataFrame:
        """Get the interconnection queue

        Returns:
            polars.DataFrame: Interconnection queue
        """
        raw_data = self.get_raw_interconnection_queue(verbose)
        data = json.loads(raw_data.read().decode("utf-8"))
        queue = pl.from_pandas(pd.DataFrame(data))

        queue = queue.rename(
            {
                "postGIAStatus": "Post Generator Interconnection Agreement Status",
                "doneDate": "Interconnection Approval Date",
            },
        )

        queue = queue.with_columns(
            pl.max_horizontal("summerNetMW", "winterNetMW").alias("Capacity (MW)"),
        )

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
    ) -> pl.DataFrame:
        """Get the forecasted generation outages published on the date for the next
        seven days."""
        return self._get_generation_outages_data(date, type="forecast", verbose=verbose)

    @support_date_range(frequency="DAY_START")
    def get_generation_outages_estimated(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get the estimated generation outages published on the date for the past 30
        days. NOTE: since these are estimates, they change with each file published.
        """
        return self._get_generation_outages_data(date, type="actual", verbose=verbose)

    def _get_generation_outages_data(
        self,
        date: str | pd.Timestamp,
        type: str = "forecast",
        verbose: bool = False,
    ) -> pl.DataFrame:
        if date == "latest":
            date = pd.Timestamp.now(
                tz=self.default_timezone,
            ).normalize() - pd.DateOffset(days=1)

        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_mom.xlsx"  # noqa

        logger.info(f"Downloading outages {type} data from {url}")

        skiprows = 6
        nrows = 17

        if type == "actual":
            skiprows = 26

        def _process_outages(df: pd.DataFrame) -> pd.DataFrame:
            df.columns = [col.replace(" **", "").strip() for col in df.columns]
            df.columns = ["Region", "Type"] + list(df.columns[2:])
            return df.drop(columns=[col for col in df.columns if "Unnamed" in col])

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                module=re.escape("openpyxl.styles.stylesheet"),
            )
            data = utils.read_excel_via_pandas(
                url,
                sheet_name="OUTAGE",
                skiprows=skiprows,
                nrows=nrows,
                process=_process_outages,
            )

        date_cols = [c for c in data.columns if c not in ["Region", "Type"]]
        data = data.unpivot(
            index=["Region", "Type"],
            on=date_cols,
            variable_name="Date",
            value_name="MW",
        )
        data = data.pivot("Type", index=["Region", "Date"], values="MW")

        rename_dict = {
            "Derated": "Derated Outages MW",
            "Forced": "Forced Outages MW",
            "Planned": "Planned Outages MW",
            "Unplanned": "Unplanned Outages MW",
        }

        output_cols = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Region",
            *rename_dict.values(),
        ]

        return (
            data.with_columns(
                pl.col("Date")
                .str.to_datetime(format="%m/%d/%y")
                .dt.replace_time_zone(self.default_timezone)
                .alias("Interval Start"),
            )
            .with_columns(
                (pl.col("Interval Start") + pl.duration(days=1)).alias("Interval End"),
                pl.lit(date.tz_convert(self.default_timezone)).alias("Publish Time"),
            )
            .rename(rename_dict)
            .select(output_cols)
            .sort(["Interval Start", "Region"])
        )

    _SUBREGIONAL_PBC_COLS = [
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

    _RESERVE_PRODUCT_COLS = [
        "Interval Start",
        "Interval End",
        "Constraint Name",
        "Shadow Price",
        "Constraint Description",
    ]

    def _subregional_pbc_empty_df(self) -> pl.DataFrame:
        tz = self.default_timezone
        return pl.DataFrame(
            schema={
                "Interval Start": pl.Datetime("ns", tz),
                "Interval End": pl.Datetime("ns", tz),
                "CONSTRAINT_NAME": pl.Utf8,
                "PRELIMINARY_SHADOW_PRICE": pl.Float64,
                "CURVETYPE": pl.Utf8,
                "BP1": pl.Float64,
                "PC1": pl.Float64,
                "BP2": pl.Float64,
                "PC2": pl.Float64,
                "BP3": pl.Float64,
                "PC3": pl.Float64,
                "BP4": pl.Float64,
                "PC4": pl.Float64,
                "OVERRIDE": pl.Int64,
                "REASON": pl.Utf8,
            },
        )

    def _reserve_product_empty_df(self) -> pl.DataFrame:
        tz = self.default_timezone
        return pl.DataFrame(
            schema={
                "Interval Start": pl.Datetime("ns", tz),
                "Interval End": pl.Datetime("ns", tz),
                "Constraint Name": pl.Utf8,
                "Shadow Price": pl.Float64,
                "Constraint Description": pl.Utf8,
            },
        )

    def _finalize_reserve_product(self, data: pl.DataFrame) -> pl.DataFrame:
        tz = self.default_timezone
        return data.select(self._RESERVE_PRODUCT_COLS).with_columns(
            pl.col("Interval Start").cast(pl.Datetime("ns", tz)),
            pl.col("Interval End").cast(pl.Datetime("ns", tz)),
            pl.col("Constraint Name").cast(pl.Utf8),
            pl.col("Constraint Description").cast(pl.Utf8),
            pl.col("Shadow Price").cast(pl.Float64, strict=False),
        )

    def _read_subregional_pbc_csv(
        self,
        url: str,
        interval_minutes: int,
    ) -> pl.DataFrame:
        def _process_pbc(df: pd.DataFrame) -> pd.DataFrame:
            return df.iloc[:-1]

        data = utils.read_csv_exotic_via_pandas(
            url,
            skiprows=3,
            index_col=False,
            process=_process_pbc,
        )
        data = data.rename({c: c.strip() for c in data.columns})
        data = data.with_columns(
            pl.col("PRELIMINARY_SHADOW_PRICE").cast(pl.Float64, strict=False),
            pl.col("BP1").cast(pl.Float64, strict=False),
            pl.col("PC1").cast(pl.Float64, strict=False),
            pl.col("BP2").cast(pl.Float64, strict=False),
            pl.col("PC2").cast(pl.Float64, strict=False),
            pl.col("BP3").cast(pl.Float64, strict=False),
            pl.col("PC3").cast(pl.Float64, strict=False),
            pl.col("BP4").cast(pl.Float64, strict=False),
            pl.col("PC4").cast(pl.Float64, strict=False),
            pl.col("OVERRIDE").cast(pl.Int64, strict=False),
            pl.col("REASON").cast(pl.Utf8),
        )

        if data.is_empty():
            return self._subregional_pbc_empty_df()

        interval_end = pd.to_datetime(
            data["MARKET_HOUR_EST"].to_pandas(),
        ).dt.tz_localize(
            self.default_timezone,
        )
        data = data.with_columns(
            pl.from_pandas(interval_end).alias("Interval End"),
        )
        data = data.with_columns(
            (pl.col("Interval End") - pl.duration(minutes=interval_minutes)).alias(
                "Interval Start",
            ),
        )
        data = data.with_columns(
            pl.col("Interval End").cast(pl.Datetime("ns", self.default_timezone)),
            pl.col("Interval Start").cast(pl.Datetime("ns", self.default_timezone)),
        )
        return data.select(self._SUBREGIONAL_PBC_COLS)

    def _get_constraint_header_dates_from_url(
        self,
        url: str,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        header = utils.read_excel_via_pandas(url, nrows=2, usecols=[0])
        col = header.columns[0]
        market_date = pd.to_datetime(header[col][0].split(": ")[1]).tz_localize(
            self.default_timezone,
        )
        publish_date = pd.to_datetime(header[col][1].split(": ")[1]).tz_localize(
            self.default_timezone,
        )
        return market_date, publish_date

    @support_date_range(frequency="DAY_START")
    def get_binding_constraints_supplemental(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get the supplemental binding constraints data from MISO.

        Source URL: https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports/#nt=%2FMarketReportType%3ADay-Ahead%2FMarketReportName%3ABinding Constraints Supplemental (xls)&t=10&p=0&s=MarketReportPublished&sd=desc

        Args:
            date (str | pd.Timestamp): Start date
            end (str | pd.Timestamp, optional): End date. Defaults to None.
            verbose (bool, optional): Verbosity. Defaults to False.

        Returns:
            polars.DataFrame: Supplemental binding constraints data
        """
        query_date = date - pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_da_bcsf.xls"
        logger.info(f"Downloading supplemental binding constraints data from {url}")

        market_date, publish_date = self._get_constraint_header_dates_from_url(url)
        data = utils.read_excel_via_pandas(url, skiprows=3)
        data = data.with_columns(pl.lit(market_date).alias("Date"))

        return data.select(
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
            ],
        )

    @support_date_range(frequency="DAY_START")
    def get_binding_constraints_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        query_date = date - pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_da_bc.xls"
        logger.info(f"Downloading day-ahead binding constraints data from {url}")

        market_date, publish_date = self._get_constraint_header_dates_from_url(url)
        data = utils.read_excel_via_pandas(
            url,
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
        data = _cast_constraint_description_utf8(data)

        data = data.with_columns(
            (
                pl.lit(market_date) + pl.duration(hours=pl.col("Hour of Occurrence"))
            ).alias(
                "Interval End",
            ),
        )
        data = data.with_columns(
            (pl.col("Interval End") - pl.duration(hours=1)).alias("Interval Start"),
        )
        data = data.rename(
            {
                "Branch Name ( Branch Type / From CA / To CA )": "Branch Name",
                "Constraint_ID": "Constraint ID",
            },
        )

        return data.select(
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
            ],
        )

    def get_binding_constraints_day_ahead_yearly_historical(
        self,
        year: int,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get the day-ahead binding constraints data from MISO for a given year.

        Args:
            year (int): Year
            verbose (bool, optional): Verbosity. Defaults to False.

        Returns:
            polars.DataFrame: Historical day-ahead binding constraints data
        """
        url = f"https://docs.misoenergy.org/marketreports/{year}_da_bc_HIST.csv"
        logger.info(f"Downloading day-ahead binding constraints data from {url}")

        data = pl.read_csv(url)
        data = data.with_columns(
            (
                pl.col("Market Date")
                .str.to_datetime()
                .dt.replace_time_zone(
                    self.default_timezone,
                )
                + pl.duration(hours=pl.col("Hour of Occurrence"))
            ).alias("Interval End"),
        )
        data = data.with_columns(
            (pl.col("Interval End") - pl.duration(hours=1)).alias("Interval Start"),
        )
        data = data.rename(
            {
                "Branch Name ( Branch Type / From CA / To CA )": "Branch Name",
                "Constraint_ID": "Constraint ID",
            },
        )

        return data.select(
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
            ],
        )

    @support_date_range(frequency="DAY_START")
    def get_subregional_power_balance_constraints_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        query_date = date - pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_da_pbc.csv"
        logger.info(
            f"Downloading day-ahead subregional power balance constraints data from {url}",
        )

        return self._read_subregional_pbc_csv(url, interval_minutes=60)

    @support_date_range(frequency="DAY_START")
    def get_reserve_product_binding_constraints_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        query_date = date - pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_da_rpe.xls"
        logger.info(
            f"Downloading day-ahead reserve product binding constraints data from {url}",
        )

        market_date, publish_date = self._get_constraint_header_dates_from_url(url)

        def _process_rpe(df: pd.DataFrame) -> pd.DataFrame:
            return df.iloc[:-1]

        data = utils.read_excel_via_pandas(url, skiprows=3, process=_process_rpe)
        data = _cast_constraint_description_utf8(data)

        if data.is_empty():
            return self._reserve_product_empty_df()

        data = data.with_columns(
            (
                pl.lit(market_date) + pl.duration(hours=pl.col("Hour of Occurence"))
            ).alias(
                "Interval End",
            ),
        )
        data = data.with_columns(
            (pl.col("Interval End") - pl.duration(hours=1)).alias("Interval Start"),
        )

        return self._finalize_reserve_product(data)

    @support_date_range(frequency="DAY_START")
    def get_binding_constraints_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        query_date = date + pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_rt_bc.xls"
        logger.info(f"Downloading real-time binding constraints data from {url}")

        market_date, publish_date = self._get_constraint_header_dates_from_url(url)

        def _process_rt_bc(df: pd.DataFrame) -> pd.DataFrame:
            df["Interval End"] = pd.to_datetime(
                market_date.strftime("%Y-%m-%d") + " " + df["Hour of  Occurrence"],
            ).dt.tz_localize(self.default_timezone)
            df["Interval Start"] = df["Interval End"] - pd.Timedelta(minutes=5)
            return df

        data = utils.read_excel_via_pandas(
            url,
            skiprows=3,
            dtype={
                "Constraint Description": object,
                "Preliminary Shadow Price": float,
                "BP1": float,
                "PC1": float,
                "BP2": float,
                "PC2": float,
            },
            process=_process_rt_bc,
        )
        data = _cast_constraint_description_utf8(data)
        data = data.rename(
            {
                "Branch Name ( Branch Type / From CA / To CA )": "Branch Name",
            },
        )

        return data.select(
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
            ],
        )

    def get_binding_constraints_real_time_yearly_historical(
        self,
        year: int,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get the real-time binding constraints data from MISO for a given year.

        Args:
            year (int): Year
            verbose (bool, optional): Verbosity. Defaults to False.

        Returns:
            polars.DataFrame: Historical real-time binding constraints data
        """
        url = f"https://docs.misoenergy.org/marketreports/{year}_rt_bc_HIST.csv"
        logger.info(f"Downloading real-time binding constraints data from {url}")

        def _process_rt_hist(df: pd.DataFrame) -> pd.DataFrame:
            df = df.iloc[:-2].copy()
            df["Interval End"] = pd.to_datetime(
                df["Market Date"] + " " + df["Hour of Occurrence"],
            ).dt.tz_localize(self.default_timezone)
            df["Interval Start"] = df["Interval End"] - pd.Timedelta(hours=1)
            for col in [
                "Preliminary Shadow Price",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
                "Override",
                "Constraint ID",
                "Flowgate NERC ID",
            ]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            for col in [
                "Constraint Description",
                "Constraint Name",
                "Branch Name",
                "Contingency Description",
                "Curve Type",
            ]:
                if col in df.columns:
                    df[col] = df[col].astype("object")
            return df

        data = utils.read_csv_exotic_via_pandas(
            url,
            skiprows=2,
            dtype=str,
            process=_process_rt_hist,
        )
        data = _cast_constraint_description_utf8(data)
        data = data.rename(
            {
                "Branch Name ( Branch Type / From CA / To CA )": "Branch Name",
                "Flowgate NERCID": "Flowgate NERC ID",
                "Constraint_ID": "Constraint ID",
            },
        )

        return data.select(
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
            ],
        )

    @support_date_range(frequency="DAY_START")
    def get_binding_constraint_overrides_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        query_date = date + pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_rt_or.xls"
        logger.info(
            f"Downloading real-time binding constraint overrides data from {url}",
        )

        market_date, publish_date = self._get_constraint_header_dates_from_url(url)

        def _process_rt_or(df: pd.DataFrame) -> pd.DataFrame:
            df["Interval End"] = pd.to_datetime(
                market_date.strftime("%Y-%m-%d") + " " + df["Hour of  Occurrence"],
            ).dt.tz_localize(self.default_timezone)
            df["Interval Start"] = df["Interval End"] - pd.Timedelta(minutes=5)
            return df

        data = utils.read_excel_via_pandas(
            url,
            skiprows=3,
            dtype={
                "Constraint Description": object,
                "BP1": float,
                "PC1": float,
                "BP2": float,
                "PC2": float,
                "Reason": object,
            },
            process=_process_rt_or,
        )
        data = _cast_constraint_description_utf8(data)
        data = data.rename(
            {
                "Branch Name ( Branch Type / From CA / To CA )": "Branch Name",
            },
        )
        return data.select(
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
            ],
        )

    @support_date_range(frequency="DAY_START")
    def get_subregional_power_balance_constraints_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        query_date = date + pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_rt_pbc.csv"
        logger.info(
            f"Downloading real-time subregional power balance constraints data from {url}",
        )

        return self._read_subregional_pbc_csv(url, interval_minutes=5)

    @support_date_range(frequency="DAY_START")
    def get_reserve_product_binding_constraints_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        query_date = date + pd.Timedelta("1D")
        url = f"https://docs.misoenergy.org/marketreports/{query_date.strftime('%Y%m%d')}_rt_rpe.xls"
        logger.info(
            f"Downloading real-time reserve product binding constraints data from {url}",
        )

        def _process_rt_rpe(df: pd.DataFrame) -> pd.DataFrame:
            df = df.iloc[:-1].copy()
            if not df.empty:
                df["Interval End"] = pd.to_datetime(
                    df["Time of Occurence"],
                ).dt.tz_localize(self.default_timezone)
                df["Interval Start"] = df["Interval End"] - pd.Timedelta(minutes=5)
            return df

        data = utils.read_excel_via_pandas(url, skiprows=3, process=_process_rt_rpe)
        data = _cast_constraint_description_utf8(data)

        if data.is_empty():
            return self._reserve_product_empty_df()

        return self._finalize_reserve_product(data)

    @support_date_range(frequency=None)
    def get_binding_constraints_real_time_intraday(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get real-time binding constraints data from MISO's intraday API.

        This provides active real-time constraint data updated every 5 minutes.
        Only supports "latest" data.

        Args:
            date: Must be "latest".
            end: Not used.
            verbose: If True, prints additional information during data retrieval.

        Returns:
            DataFrame with real-time binding constraint data.
        """
        if date != "latest":
            raise NotSupported(
                "Only latest MISO real-time binding constraints data is available."
                " Use 'latest' as date.",
            )

        url = "https://public-api.misoenergy.org/api/BindingConstraints/RealTime"
        logger.info(f"Downloading real-time binding constraints data from {url}")

        response_json = self._get_json(url, verbose=verbose)
        return self._parse_binding_constraints_real_time_intraday(response_json)

    def _parse_binding_constraints_real_time_intraday(
        self,
        response_json: dict,
    ) -> pl.DataFrame:
        constraints = response_json.get("Constraint", [])

        if not constraints:
            raise NoDataFoundException("No real-time binding constraints data found")

        df = pl.from_pandas(pd.DataFrame(constraints))

        if (df["Name"] == "None").all():
            raise NoDataFoundException("No real-time binding constraints data found")

        df = df.with_columns(
            pl.col("Period")
            .str.to_datetime()
            .dt.replace_time_zone(self.default_timezone)
            .alias("Interval End"),
        )
        df = df.rename(
            {
                "Name": "Constraint Name",
                "Price": "Shadow Price",
                "OVERRIDE": "Override",
                "CURVETYPE": "Curve Type",
            },
        )
        df = df.with_columns(
            pl.col("Shadow Price").cast(pl.Float64, strict=False),
            pl.when(pl.col("Override") == "")
            .then(None)
            .otherwise(pl.col("Override"))
            .cast(pl.Int64)
            .alias("Override"),
            pl.when(pl.col("BP1") == "")
            .then(None)
            .otherwise(pl.col("BP1"))
            .cast(pl.Int64)
            .alias("BP1"),
            pl.when(pl.col("PC1") == "")
            .then(None)
            .otherwise(pl.col("PC1"))
            .cast(pl.Int64)
            .alias("PC1"),
            pl.when(pl.col("BP2") == "")
            .then(None)
            .otherwise(pl.col("BP2"))
            .cast(pl.Int64)
            .alias("BP2"),
            pl.when(pl.col("PC2") == "")
            .then(None)
            .otherwise(pl.col("PC2"))
            .cast(pl.Int64)
            .alias("PC2"),
        )
        df = df.with_columns(
            (pl.col("Interval End") - pl.duration(minutes=5)).alias("Interval Start"),
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Shadow Price",
                "Override",
                "Curve Type",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
            ],
        ).sort("Interval Start")

    @support_date_range(frequency="DAY_START")
    def get_look_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        if date == "latest":
            return self.get_look_ahead_hourly(date="today", verbose=verbose)

        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_sr_la_rg.csv"
        logger.info(f"Downloading look-ahead hourly data from {url}")

        publish_time = date.normalize()

        df = utils.read_csv_exotic_via_pandas(
            url,
            skiprows=3,
            skipfooter=15,
            engine="python",
        )
        id_cols = ["Hourend_EST", "Region"]
        value_cols = [col for col in df.columns if col not in id_cols]

        df_melted = df.unpivot(
            index=id_cols,
            on=value_cols,
            variable_name="date_col",
            value_name="value",
        )

        df_melted = df_melted.with_columns(
            pl.col("date_col").str.extract(r"(\d{2}/\d{2}/\d{4})", 1).alias("date"),
            pl.when(pl.col("date_col").str.contains("Outage"))
            .then(pl.lit("Outage"))
            .otherwise(pl.lit("MTLF"))
            .alias("type"),
        )

        df_wide = df_melted.pivot(
            "type",
            index=["Hourend_EST", "Region", "date"],
            values="value",
        )

        df_wide = df_wide.with_columns(
            pl.col("Hourend_EST")
            .str.extract(r"Hour   (\d+)", 1)
            .cast(pl.Int64)
            .sub(1)
            .cast(pl.Utf8)
            .str.zfill(2)
            .alias("Hour Ending"),
        )

        df_wide = df_wide.with_columns(
            (pl.col("date") + " " + pl.col("Hour Ending") + ":00")
            .str.to_datetime(format="%m/%d/%Y %H:%M")
            .alias("Interval End Naive"),
        )
        df_wide = utils.localize_shift_forward_polars(
            df_wide,
            "Interval End Naive",
            self.default_timezone,
        )
        df_wide = df_wide.with_columns(
            (pl.col("Interval End Naive") + pl.duration(hours=1)).alias("Interval End"),
        ).drop("Interval End Naive")
        df_wide = df_wide.with_columns(
            (pl.col("Interval End") - pl.duration(hours=1)).alias("Interval Start"),
            pl.lit(publish_time).alias("Publish Time"),
            (pl.col("MTLF") * 1000).alias("MTLF"),
            (pl.col("Outage") * 1000).alias("Outage"),
        )

        return df_wide.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Region",
                "MTLF",
                "Outage",
            ],
        ).sort(["Interval Start", "Region"])

    @support_date_range(frequency=None)
    def get_interchange_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        if date != "latest":
            raise NotSupported(
                "Only latest MISO interchange data is available. Use 'latest' as date.",
            )

        actual_url = "https://public-api.misoenergy.org/api/Interchange/GetNai/Imports"
        scheduled_url = (
            "https://public-api.misoenergy.org/api/Interchange/GetNsi/FiveMinute"
        )

        logger.info(
            f"Downloading interchange data from {scheduled_url} and {actual_url}",
        )

        actual_response = self._get_json(actual_url, verbose=verbose)
        actual_data = pl.from_pandas(pd.DataFrame(actual_response))
        actual_data = actual_data.with_columns(
            pl.col("Time")
            .str.to_datetime(format="%Y-%m-%d %I:%M:%S %p", time_zone="UTC")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Time"),
            pl.col("Value").cast(pl.Float64, strict=False).cast(pl.Int64),
        )

        scheduled_response = self._get_json(scheduled_url, verbose=verbose)
        scheduled_data = pl.from_pandas(pd.DataFrame(scheduled_response["instance"]))
        scheduled_data = scheduled_data.with_columns(
            pl.col("timestamp")
            .str.to_datetime()
            .dt.replace_time_zone(self.default_timezone)
            .alias("timestamp"),
            pl.col("MISO").cast(pl.Float64, strict=False).cast(pl.Int64),
        )
        for col in ["AECI", "LGEE", "MHEB", "ONT", "PJM", "SOCO", "SPA", "SWPP", "TVA"]:
            if col in scheduled_data.columns:
                scheduled_data = scheduled_data.with_columns(
                    pl.col(col).cast(pl.Float64, strict=False).cast(pl.Int64),
                )

        data = scheduled_data.join(
            actual_data,
            left_on="timestamp",
            right_on="Time",
            how="full",
            suffix="_actual",
        )
        data = data.with_columns(
            pl.coalesce(pl.col("timestamp"), pl.col("Time")).alias("Interval End"),
        ).drop(["timestamp", "Time"])
        data = data.rename(
            {
                "Value": "Net Actual Interchange",
                "MISO": "Net Scheduled Interchange",
            },
        )
        data = data.with_columns(
            (pl.col("Interval End") - pl.duration(minutes=5)).alias("Interval Start"),
        )

        output_cols = [
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
        for col in output_cols:
            if col not in ["Interval Start", "Interval End"] and col in data.columns:
                data = data.with_columns(pl.col(col).cast(pl.Int64, strict=False))

        return data.select(output_cols).sort("Interval Start")

    @support_date_range(frequency="DAY_START")
    def get_multiday_operating_margin(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get the multiday operating margin forecast.

        This data comes from the Multiday Operating Margin Forecast (MOMF) report
        published daily by MISO. The operating margin represents the difference
        between available resources and system obligations.

        Args:
            date: The date to retrieve data for.
            end: Optional end date for a date range.
            verbose: If True, prints additional information during data retrieval.

        Returns:
            DataFrame with system-wide operating margin forecast data including
            committed/uncommitted resources, renewable forecasts, load projections,
            and operating margin calculations.
        """
        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_mom.xlsx"  # noqa

        logger.info(f"Downloading multiday operating margin data from {url}")

        response = requests.get(url)
        response.raise_for_status()

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                module=re.escape("openpyxl.styles.stylesheet"),
            )
            sheet_data = pd.read_excel(
                io.BytesIO(response.content),
                sheet_name="MISO",
                skiprows=3,
            )

        return self._get_multiday_operating_margin_data(
            date,
            region="MISO",
            sheet_data=sheet_data,
            verbose=verbose,
        )

    @support_date_range(frequency="DAY_START")
    def get_multiday_operating_margin_regional(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get the multiday operating margin forecast for all regions.

        This data comes from the Multiday Operating Margin Forecast (MOMF) report
        published daily by MISO. The operating margin represents the difference
        between available resources and system obligations for each region.

        Args:
            date: The date to retrieve data for.
            end: Optional end date for a date range.
            verbose: If True, prints additional information during data retrieval.

        Returns:
            DataFrame with regional operating margin forecast data for all regions
            (NORTH, CENTRAL, NORTH+CENTRAL, SOUTH) including committed/uncommitted
            resources, renewable forecasts, load projections, and regional metrics.
        """
        url = f"https://docs.misoenergy.org/marketreports/{date.strftime('%Y%m%d')}_mom.xlsx"  # noqa

        logger.info(f"Downloading multiday operating margin data from {url}")

        # Download and parse file once, reading all sheets
        response = requests.get(url)
        response.raise_for_status()

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                module=re.escape("openpyxl.styles.stylesheet"),
            )
            all_sheets = pd.read_excel(
                io.BytesIO(response.content),
                sheet_name=None,
                skiprows=3,
            )

        regions = ["NORTH", "CENTRAL", "NORTH+CENTRAL", "SOUTH"]
        dfs = []

        for region in regions:
            df = self._get_multiday_operating_margin_data(
                date,
                region=region,
                sheet_data=all_sheets[region],
                verbose=verbose,
            )
            if df.height > 0:
                dfs.append(df)

        if not dfs:
            raise NoDataFoundException(
                f"No data found for any region on {date}",
            )

        return pl.concat(dfs, how="diagonal")

    def _get_multiday_operating_margin_data(
        self,
        date: str | pd.Timestamp,
        region: str = "MISO",
        sheet_data: pd.DataFrame | pl.DataFrame | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Internal helper to process multiday operating margin sheet data.

        Args:
            date: The date to retrieve data for.
            region: Region name for labeling.
            sheet_data: DataFrame containing the sheet data (already loaded).
            verbose: Whether to log verbose output.

        Returns:
            DataFrame with operating margin forecast data.
        """
        if isinstance(sheet_data, pl.DataFrame):
            data = sheet_data.to_pandas()
        else:
            data = sheet_data

        # Map sheet names to display names
        region_display_names = {
            "MISO": "MISO",
            "NORTH": "North",
            "CENTRAL": "Central",
            "NORTH+CENTRAL": "North and Central",
            "SOUTH": "South",
        }
        region_display = region_display_names.get(region, region)

        # Find the date row - it's the first row with date strings in columns.
        # The date row position varies by sheet (MISO has an extra blank row),
        # but it's always within the first 3 rows after skiprows=3.
        date_row_idx = None
        max_rows_to_search = 3
        for idx in range(min(max_rows_to_search, len(data))):
            row = data.iloc[idx]
            # Check if any column value looks like a date (contains "/" and "HE")
            has_dates = any(
                pd.notna(val) and isinstance(val, str) and "/" in val and "HE" in val
                for val in row
            )
            if has_dates:
                date_row_idx = idx
                break

        if date_row_idx is None:
            raise NoDataFoundException(
                f"Could not find date row in multiday operating margin data for {region}",
            )

        # Extract date columns
        date_row = data.iloc[date_row_idx]
        date_columns = []
        for col in data.columns:
            val = date_row[col]
            if pd.notna(val) and isinstance(val, str) and "/" in val and "HE" in val:
                date_columns.append((col, val))

        # Define the metrics to extract from the Excel file
        metric_mapping = {
            "RESOURCE COMMITTED": "Resource Committed",
            "RESOURCE UNCOMMITTED": "Resource Uncommitted",
            "Uncommitted >16 hr": "Uncommitted Greater than 16 Hours",
            "Uncommitted 12-16 hr": "Uncommitted 12 to 16 Hours",
            "Uncommitted 8-12 hr": "Uncommitted 8 to 12 Hours",
            "Uncommitted 4-8 hr": "Uncommitted 4 to 8 Hours",
            "Uncommitted < 4 hr": "Uncommitted Less than 4 Hours",
            "Renewable Forecast": "Renewable Forecast",
            "Wind Forecast": "Wind Forecast",
            "Solar Forecast": "Solar Forecast",
            "MISO resources available": "MISO Resources Available",
            "NSI (+ export, - import)": "NSI",
            "Total Resources Available": "Total Resources Available",
            "Projected Load": "Projected Load",
            "Operating Reserve Requirement": "Operating Reserve Requirement",
            "Obligation": "Obligation",
            "Resource Operating Margin *": "Resource Operating Margin",
            "N+C Resources above load": "Region Resources Above Load",
            "Max Possible RDT (S to N) *": "Max Possible RDT",
            "South Resources above load": "Region Resources Above Load",
            "Max possible RDT (N to S) *": "Max Possible RDT",
        }

        # Extract the rows we need
        result_data = []
        for col, date_str in date_columns:
            # Parse the date string (format: "M/D/YY HE HH")
            parts = date_str.replace("**", "").strip().split()
            date_part = parts[0]  # M/D/YY
            hour_part = int(parts[2]) if len(parts) >= 3 else 1  # HE HH

            # Parse the date and create peak hour timestamp
            peak_hour = pd.to_datetime(date_part, format="%m/%d/%y") + pd.Timedelta(
                hours=hour_part,
            )
            peak_hour = peak_hour.tz_localize(self.default_timezone)

            # Extract metrics for this date column
            row_data = {
                "Publish Date": date.date(),
                "Peak Hour": peak_hour,
                "Region": region_display,
            }

            # Find each metric in the data
            for orig_name, new_name in metric_mapping.items():
                # Find the row with this metric name
                metric_row = data[data.iloc[:, 0].astype(str).str.strip() == orig_name]
                if not metric_row.empty:
                    value = metric_row[col].iloc[0]
                    if pd.notna(value):
                        # Handle values with commas (e.g., "2,500")
                        if isinstance(value, str):
                            value = value.replace(",", "")
                        row_data[new_name] = float(value)
                    else:
                        row_data[new_name] = None

            # Handle "Additional Emergency Headroom" which appears 3 times:
            # 1. After RESOURCE COMMITTED -> "Committed Additional Emergency Headroom"
            # 2. After RESOURCE UNCOMMITTED -> "Uncommitted Additional Emergency Headroom"
            # 3. After EMERGENCY RESOURCES -> "Emergency Resources Additional Headroom"
            emergency_headroom_rows = data[
                data.iloc[:, 0].astype(str).str.strip()
                == "Additional Emergency Headroom"
            ]
            if len(emergency_headroom_rows) >= 1:
                val = emergency_headroom_rows.iloc[0][col]
                row_data["Committed Additional Emergency Headroom"] = (
                    float(val) if pd.notna(val) else None
                )
            if len(emergency_headroom_rows) >= 2:
                val = emergency_headroom_rows.iloc[1][col]
                row_data["Uncommitted Additional Emergency Headroom"] = (
                    float(val) if pd.notna(val) else None
                )
            if len(emergency_headroom_rows) >= 3:
                val = emergency_headroom_rows.iloc[2][col]
                row_data["Emergency Resources Additional Headroom"] = (
                    float(val) if pd.notna(val) else None
                )

            result_data.append(row_data)

        result_df = pd.DataFrame(result_data)

        # Filter by date range if specified
        if len(result_df) > 0:
            result_df = result_df[result_df["Peak Hour"] >= date]

            # Convert all numeric columns to float (handles None -> NaN conversion)
            numeric_cols = [
                col
                for col in result_df.columns
                if col not in ["Publish Date", "Peak Hour", "Region"]
            ]
            for col in numeric_cols:
                if col in result_df.columns:
                    result_df[col] = pd.to_numeric(result_df[col], errors="coerce")

            # Add calculated columns for regional (if not already present)
            if region != "MISO":
                # NSI and Total Resources Available only exist in some regional sheets
                if "NSI" not in result_df.columns:
                    result_df["NSI"] = pd.Series(
                        dtype="float64",
                        index=result_df.index,
                    )
                if "Total Resources Available" not in result_df.columns:
                    result_df["Total Resources Available"] = pd.Series(
                        dtype="float64",
                        index=result_df.index,
                    )

                # Region Resources Above Load = MISO Resources Available - Projected Load
                # (only calculate if not already in the data, e.g., NORTH+CENTRAL has this)
                if "Region Resources Above Load" not in result_df.columns:
                    if (
                        "MISO Resources Available" in result_df.columns
                        and "Projected Load" in result_df.columns
                    ):
                        result_df["Region Resources Above Load"] = (
                            result_df["MISO Resources Available"]
                            - result_df["Projected Load"]
                        )

                # Max Possible RDT - only add if not already present
                if "Max Possible RDT" not in result_df.columns:
                    result_df["Max Possible RDT"] = pd.Series(
                        dtype="float64",
                        index=result_df.index,
                    )

            # Reorder columns
            if region == "MISO":
                desired_order = [
                    "Publish Date",
                    "Peak Hour",
                    "Region",
                    "Resource Committed",
                    "Committed Additional Emergency Headroom",
                    "Resource Uncommitted",
                    "Uncommitted Greater than 16 Hours",
                    "Uncommitted 12 to 16 Hours",
                    "Uncommitted 8 to 12 Hours",
                    "Uncommitted 4 to 8 Hours",
                    "Uncommitted Less than 4 Hours",
                    "Uncommitted Additional Emergency Headroom",
                    "Emergency Resources Additional Headroom",
                    "Renewable Forecast",
                    "Wind Forecast",
                    "Solar Forecast",
                    "MISO Resources Available",
                    "NSI",
                    "Total Resources Available",
                    "Projected Load",
                    "Operating Reserve Requirement",
                    "Obligation",
                    "Resource Operating Margin",
                ]
            else:
                desired_order = [
                    "Publish Date",
                    "Peak Hour",
                    "Region",
                    "Resource Committed",
                    "Committed Additional Emergency Headroom",
                    "Resource Uncommitted",
                    "Uncommitted Greater than 16 Hours",
                    "Uncommitted 12 to 16 Hours",
                    "Uncommitted 8 to 12 Hours",
                    "Uncommitted 4 to 8 Hours",
                    "Uncommitted Less than 4 Hours",
                    "Uncommitted Additional Emergency Headroom",
                    "Emergency Resources Additional Headroom",
                    "Renewable Forecast",
                    "Wind Forecast",
                    "Solar Forecast",
                    "MISO Resources Available",
                    "NSI",
                    "Total Resources Available",
                    "Projected Load",
                    "Region Resources Above Load",
                    "Max Possible RDT",
                ]

            # Only include columns that exist
            result_df = result_df[
                [col for col in desired_order if col in result_df.columns]
            ]

        return pl.from_pandas(result_df).sort("Peak Hour")

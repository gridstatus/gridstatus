import copy
import io
import re
import time
import warnings
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pdfplumber
import requests
from tabulate import tabulate
from termcolor import colored

from gridstatus import utils
from gridstatus.base import (
    GridStatus,
    ISOBase,
    Markets,
    NoDataFoundException,
    NotSupported,
)
from gridstatus.caiso import caiso_utils
from gridstatus.caiso.caiso_constants import (
    CURRENT_BASE,
    HISTORY_BASE,
    OASIS_DATASET_CONFIG,
    get_dataframe_config_for_renewables_report,
)
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import logger
from gridstatus.lmp_config import lmp_config


def _determine_lmp_frequency(args: dict) -> str:
    """if querying all must use 1d frequency"""
    locations = args.get("locations", "")
    market = args.get("market", "")
    # due to limitations of OASIS api
    if isinstance(locations, str) and locations.lower() in ["all", "all_ap_nodes"]:
        if market == Markets.REAL_TIME_5_MIN:
            return "1h"
        elif market == Markets.REAL_TIME_15_MIN:
            return "1h"
        elif market == Markets.DAY_AHEAD_HOURLY:
            return "1D"
        else:
            raise NotSupported(f"Market {market} not supported")
    else:
        return "31D"


def _determine_oasis_frequency(args: dict) -> str:
    dataset_config = copy.deepcopy(OASIS_DATASET_CONFIG[args["dataset"]])
    # get meta if it exists. and then max_query_frequency if it exists
    meta = dataset_config.get("meta", {})
    max_query_frequency = meta.get("max_query_frequency", None)
    if max_query_frequency is not None:
        return max_query_frequency

    return "31D"


def _get_historical(
    file: str,
    date: str | pd.Timestamp,
    column: str,
    verbose: bool = False,
) -> pd.DataFrame:
    """Get the historical data file from CAISO given a data series name, formats, and returns a pandas dataframe.

    Args:
        file (str): The name of the data we are wanting, which is equivalent to the file to get from CAISO
        date (str | pd.Timestamp): The date of the data to get from CAISO
        column (str): The column to check for the latest value time
        verbose (bool, optional): Whether to print out the URL being fetched, defaults to False

    Returns:
        pd.DataFrame: A pandas dataframe of the data
    """
    # NOTE: The cache buster is necessary because CAISO will serve cached data from cloudfront on the same url if the url has not changed.
    cache_buster = int(pd.Timestamp.now(tz=CAISO.default_timezone).timestamp())
    if utils.is_today(date, CAISO.default_timezone):
        url: str = f"{CURRENT_BASE}/{file}.csv?_={cache_buster}"
        latest = True
    else:
        date_str: str = date.strftime("%Y%m%d")
        url: str = f"{HISTORY_BASE}/{date_str}/{file}.csv?_={cache_buster}"
        latest = False
    logger.info(f"Fetching URL: {url}")
    df = pd.read_csv(url)

    # sometimes there are extra rows at the end, so this lets us ignore them
    df = df.dropna(subset=["Time"])

    # drop every column after Time where values
    # are all null. this happens during spring DST
    # change and caiso keeps the non-existent hour
    # but has nulls for all other columns
    df = df.dropna(subset=df.columns[1:], how="all")

    # for the latest data, we want to check if the data is actually from the previous day and update the date accordingly
    if latest:
        latest_file_time = caiso_utils.check_latest_value_time(df, column)
        current_caiso_time = pd.Timestamp.now(tz=CAISO.default_timezone)

        if latest_file_time > current_caiso_time:
            date = date - pd.Timedelta(days=1)

    df["Time"] = df["Time"].apply(
        caiso_utils.make_timestamp,
        today=date,
        timezone=CAISO.default_timezone,
    )

    # sometimes returns midnight, which is technically the next day
    # to be careful, let's check if that is the case before dropping
    if df.iloc[-1]["Time"].hour == 0:
        df = df.iloc[:-1]

    # insert interval start/end columns
    df.insert(1, "Interval Start", df["Time"])

    # be careful if this is ever not 5 minutes
    df.insert(2, "Interval End", df["Time"] + pd.Timedelta(minutes=5))

    return df


def _caiso_handle_start_end(
    date: str | pd.Timestamp,
    end: str | pd.Timestamp | None = None,
) -> tuple[str, str]:
    start = date.tz_convert("UTC")

    if end:
        end = end
        end = end.tz_convert("UTC")
    else:
        end = start + pd.DateOffset(1)

    start = start.strftime("%Y%m%dT%H:%M-0000")
    end = end.strftime("%Y%m%dT%H:%M-0000")

    return start, end


class CAISO(ISOBase):
    """California Independent System Operator (CAISO)"""

    name = "California ISO"
    iso_id = "caiso"
    default_timezone = "US/Pacific"

    status_homepage = "https://www.caiso.com/TodaysOutlook/Pages/default.aspx"
    interconnection_homepage = "https://rimspub.caiso.com/rimsui/logon.do"

    # Markets PRC_INTVL_LMP, PRC_RTPD_LMP, PRC_LMP
    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_15_MIN,
        Markets.DAY_AHEAD_HOURLY,
    ]

    trading_hub_locations = [
        "TH_NP15_GEN-APND",
        "TH_SP15_GEN-APND",
        "TH_ZP26_GEN-APND",
    ]

    def _current_day(self):
        # get current date from stats api
        return self.get_status(date="latest").time.date()

    def get_stats(self, verbose: bool = False) -> dict:
        stats_url = CURRENT_BASE + "/stats.txt"
        r = self._get_json(stats_url, verbose=verbose)
        return r

    def get_status(self, date: str = "latest", verbose: bool = False) -> str:
        """Get Current Status of the Grid. Only date="latest" is supported

        Known possible values: Normal, Restricted Maintenance Operations, Flex Alert
        """

        if date == "latest":
            # todo is it possible for this to return more than one element?
            r = self.get_stats(verbose=verbose)

            time = pd.to_datetime(r["slotDate"]).tz_localize("US/Pacific")
            # can only store one value for status so we concat them together
            status = ", ".join(r["gridstatus"])
            reserves = r["Current_reserve"]

            return GridStatus(time=time, status=status, reserves=reserves, iso=self)
        else:
            raise NotSupported()

    def list_oasis_datasets(self, dataset: str | None = None):
        """List all available OASIS datasets and their parameters.

        Args:
            dataset (str, optional): dataset to return data for. If None, returns all datasets.
        """

        for dataset_name, config in OASIS_DATASET_CONFIG.items():
            if dataset is not None and dataset_name != dataset:
                continue
            print(colored(f"Dataset: {dataset_name}", "cyan"))
            if len(config["params"]) == 0:
                print("    No parameters")
            else:
                table_data = []
                for k, v in config["params"].items():
                    default = v[0] if isinstance(v, list) else v
                    possible_values = (
                        ", ".join(str(val) for val in v)
                        if isinstance(v, list)
                        else "N/A"
                    )
                    table_data.append([k, default, possible_values])

                print(
                    tabulate(
                        table_data,
                        headers=[
                            "Parameter",
                            "Default",
                            "Possible Values",
                        ],
                        tablefmt="grid",
                    ),
                )

            print("\n")

    @support_date_range(frequency=_determine_oasis_frequency)
    def get_oasis_dataset(
        self,
        dataset: str,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        params: dict | None = None,
        raw_data: bool = True,
        sleep: int = 5,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return data from OASIS for a given dataset

        Args:
            dataset (str): dataset to return data for. See CAISO.list_oasis_datasets
                for supported datasets
            date (str, pd.Timestamp): date to return data
            end (str, pd.Timestamp, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            params (dict): dictionary of parameters to pass to dataset.
                See CAISO.list_oasis_datasets for supported parameters
            raw_data (bool, optional): return raw data from OASIS. Defaults to True.
            sleep (int, optional): number of seconds to sleep between
                requests. Defaults to 5.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Raises:
            ValueError: if parameter is not supported for dataset
            ValueError: if parameter value is not supported for dataset

        Returns:
            pd.DataFrame: A DataFrame of data from OASIS
        """

        # deepcopy to avoid modifying original
        dataset_config = copy.deepcopy(OASIS_DATASET_CONFIG[dataset])
        logger.debug(f"Dataset config: {dataset_config}")

        if params is None:
            params = {}

        for p in params:
            if p not in dataset_config["params"]:
                raise ValueError(
                    f"Parameter {p} not supported for dataset {dataset}",
                )

            # if it's a list, make sure param value is in list
            if (
                isinstance(dataset_config["params"][p], list)
                and params[p] not in dataset_config["params"][p]
            ):
                raise ValueError(
                    f"Parameter {p} not supported for dataset {dataset}",
                )

            dataset_config["params"][p] = params[p]

        # if any dataset_config values are list,
        # take first as default
        for k, v in dataset_config["params"].items():
            if isinstance(v, list):
                dataset_config["params"][k] = v[0]

        # combine kv from query and params
        config_flat = {
            **dataset_config["query"],
            **dataset_config["params"],
        }

        # filter out null values
        config_flat = {k: v for k, v in config_flat.items() if v is not None}

        df = self._get_oasis(
            config=config_flat,
            start=date,
            end=end,
            raw_data=raw_data,
            verbose=verbose,
            sleep=sleep,
        )

        if df is None:
            if end:
                logger.warning(f"No data for {date} to {end}")
            else:
                logger.warning(f"No data for {date}")
            return pd.DataFrame()

        return df

    def _get_oasis(
        self,
        config: dict,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        raw_data: bool = False,
        verbose: bool = False,
        sleep: int = 5,
        max_retries: int = 3,
    ) -> pd.DataFrame | None:
        start, end = _caiso_handle_start_end(start, end)
        config = copy.deepcopy(config)
        config["startdatetime"] = start
        config["enddatetime"] = end

        base_url = f"http://oasis.caiso.com/oasisapi/{config.pop('path')}?"

        url = base_url + "&".join(
            [f"{k}={v}" for k, v in config.items()],
        )

        logger.info(f"Fetching URL: {url}")

        retry_num = 0
        while retry_num < max_retries:
            r = requests.get(url)

            if r.status_code == 200:
                break

            retry_num += 1
            logger.info(
                f"Failed to get data from CAISO. Error: {r.status_code}. Retrying...{retry_num} / {max_retries}",
            )

            time.sleep(sleep)
            sleep *= retry_num

        if r.status_code == 429:
            logger.warning(f"CAISO rate limit exceeded. Tried {retry_num} times.")
            return None

        # this is when no data is available
        if (
            "Content-Disposition" not in r.headers
            or ".xml.zip;" in r.headers["Content-Disposition"]
            or b".xml" in r.content
        ):
            # avoid rate limiting
            time.sleep(sleep)
            return None

        z = ZipFile(io.BytesIO(r.content))

        # parse and concat all files
        dfs = []
        logger.debug(f"Found {len(z.namelist())} files: {z.namelist()}")
        for f in z.namelist():
            logger.debug(f"Parsing file: {f}")
            df = pd.read_csv(z.open(f))
            dfs.append(df)

        df = pd.concat(dfs)

        # if col ends in _GMT, then try to parse as UTC
        for col in df.columns:
            if col.endswith("_GMT"):
                df[col] = pd.to_datetime(
                    df[col],
                    utc=True,
                )

        # handle different column names
        # across different datasets
        start_cols = [
            "INTERVALSTARTTIME_GMT",
            "INTERVAL_START_GMT",
            "STARTTIME_GMT",
            "START_DATE_GMT",
        ]
        end_cols = [
            "INTERVALENDTIME_GMT",
            "INTERVAL_END_GMT",
            "ENDTIME_GMT",
            "END_DATE_GMT",
        ]
        start_col = None
        end_col = None
        for col in start_cols:
            if col in df.columns:
                start_col = col
                df = df.sort_values(by=start_col)
                break
        for col in end_cols:
            if col in df.columns:
                end_col = col
                break

        if not raw_data and start_col in df.columns:
            df[start_col] = df[start_col].dt.tz_convert(
                CAISO.default_timezone,
            )

            df[end_col] = df[end_col].dt.tz_convert(
                CAISO.default_timezone,
            )

            df.rename(
                columns={
                    start_col: "Interval Start",
                    end_col: "Interval End",
                },
                inplace=True,
            )

            df.insert(0, "Time", df["Interval Start"])

        # avoid rate limiting
        time.sleep(sleep)

        return df

    @support_date_range(frequency="DAY_START")
    def get_fuel_mix(
        self,
        date: str | pd.Timestamp,
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get fuel mix in 5 minute intervals for a provided day

        Arguments:
            date (str, pd.Timestamp): "latest", "today", or an object
                that can be parsed as a datetime for the day to return data.

            start (str, pd.Timestamp): start of date range to return.
                alias for `date` parameter.
                Only specify one of `date` or `start`.

            end (str, pd.Timestamp): "today" or an object that can be parsed
                as a datetime for the day to return data.
                Only used if requesting a range of dates.

            verbose (bool, optional): print verbose output. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame with columns - 'Time' and columns \
                for each fuel type.
        """
        if date == "latest":
            mix = self.get_fuel_mix("today", verbose=verbose)
            return mix.tail(1).reset_index(drop=True)

        return self._get_historical_fuel_mix(date, verbose=verbose)

    def _get_historical_fuel_mix(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = _get_historical("fuelsource", date, column="Solar", verbose=verbose)

        # rename some inconsistent columns names to standardize across dates
        df = df.rename(
            columns={
                "Small hydro": "Small Hydro",
                "Natural gas": "Natural Gas",
                "Large hydro": "Large Hydro",
            },
        )

        # when day light savings time switches, there are na rows
        # maybe better way to do this in case there are other cases
        # where there are all na rows
        # ignore Time, Interval Start, Interval End columns
        subset = set(df.columns) - set(["Time", "Interval Start", "Interval End"])
        df = df.dropna(axis=0, how="all", subset=subset)

        return df

    @support_date_range(frequency="DAY_START")
    def get_load(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return load at a previous date in 5 minute intervals"""

        if date == "latest":
            return self.get_load("today", verbose=verbose)

        return self._get_historical_load(date, verbose=verbose)

    def _get_historical_load(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        df = _get_historical("demand", date, column="Current demand", verbose=verbose)

        df = df[["Time", "Interval Start", "Interval End", "Current demand"]]
        df = df.rename(columns={"Current demand": "Load"})
        df = df.dropna(subset=["Load"])
        return df

    # Deprecated in favor of the vintage-based functions, e.g. get_load_forecast_5_min
    @support_date_range(frequency="31D")
    def get_load_forecast(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "today" or date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).normalize()
        df = self.get_load_forecast_day_ahead(date, end=end)
        df["Time"] = df["Interval Start"]
        df = df[df["TAC Area Name"] == "CA ISO-TAC"]
        df = df.drop(columns=["TAC Area Name"])
        return df

    @support_date_range(frequency="31D")
    def get_load_forecast_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns 5-minute load forecast from the Real-Time Market

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: DataFrame with load forecast data
        """
        df = self.get_oasis_dataset(
            dataset="demand_forecast",
            start=date,
            end=end,
            raw_data=False,
            verbose=verbose,
            sleep=sleep,
            params={"market_run_id": "RTM", "execution_type": "RTD"},
        )

        df = df.rename(
            columns={"MW": "Load Forecast", "TAC_AREA_NAME": "TAC Area Name"},
        )

        df["Publish Time"] = df["Interval Start"] - pd.Timedelta(
            minutes=2.5,
        )

        df.sort_values(by="Interval Start", inplace=True)
        df = df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "TAC Area Name",
                "Load Forecast",
            ]
        ]

        return df

    @support_date_range(frequency="31D")
    def get_load_forecast_15_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns 15-minute load forecast from the Real-Time Pre-Dispatch Market

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: DataFrame with load forecast data
        """
        df = self.get_oasis_dataset(
            dataset="demand_forecast",
            start=date,
            end=end,
            raw_data=False,
            verbose=verbose,
            sleep=sleep,
            params={"market_run_id": "RTM", "execution_type": "RTPD"},
        )
        df = df.rename(
            columns={"MW": "Load Forecast", "TAC_AREA_NAME": "TAC Area Name"},
        )

        df["Publish Time"] = df["Interval Start"] - pd.Timedelta(
            minutes=22.5,
        )

        df.sort_values(by="Interval Start", inplace=True)
        df = df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "TAC Area Name",
                "Load Forecast",
            ]
        ]

        return df

    @support_date_range(frequency="31D")
    def get_load_forecast_day_ahead(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns hourly day-ahead load forecast

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return data.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: DataFrame with load forecast data
        """
        df = self.get_oasis_dataset(
            dataset="demand_forecast",
            start=date,
            end=end,
            raw_data=False,
            verbose=verbose,
            sleep=sleep,
            params={"market_run_id": "DAM"},
        )

        df = df.rename(
            columns={"MW": "Load Forecast", "TAC_AREA_NAME": "TAC Area Name"},
        )
        df = self._add_load_forecast_publish_time(df, day_offset=1)
        df.sort_values(by="Interval Start", inplace=True)

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "TAC Area Name",
                "Load Forecast",
            ]
        ]

        return df

    @support_date_range(frequency="31D")
    def get_load_forecast_two_day_ahead(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns hourly two-day-ahead load forecast

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return data.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: DataFrame with load forecast data
        """
        df = self.get_oasis_dataset(
            dataset="demand_forecast",
            start=date,
            end=end,
            raw_data=False,
            verbose=verbose,
            sleep=sleep,
            params={"market_run_id": "2DA"},
        )

        df = df.rename(
            columns={"MW": "Load Forecast", "TAC_AREA_NAME": "TAC Area Name"},
        )

        df = self._add_load_forecast_publish_time(df, day_offset=2)
        df.sort_values(by="Interval Start", inplace=True)

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "TAC Area Name",
                "Load Forecast",
            ]
        ]

        return df

    @support_date_range(frequency="31D")
    def get_load_forecast_seven_day_ahead(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns hourly seven-day-ahead load forecast

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return data.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: DataFrame with load forecast data
        """
        df = self.get_oasis_dataset(
            dataset="demand_forecast",
            start=date,
            end=end,
            raw_data=False,
            verbose=verbose,
            sleep=sleep,
            params={"market_run_id": "7DA"},
        )

        df = df.rename(
            columns={"MW": "Load Forecast", "TAC_AREA_NAME": "TAC Area Name"},
        )

        df = self._add_load_forecast_publish_time(df, day_offset=7)
        df.sort_values(by="Interval Start", inplace=True)

        df = df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "TAC Area Name",
                "Load Forecast",
            ]
        ]

        return df

    def _add_load_forecast_publish_time(self, df: pd.DataFrame, day_offset: int):
        """Adds a publish time to the load forecast data

        Args:
            df (pd.DataFrame): load forecast data
            day_offset (int): number of days before the forecast date that it was published

        Returns:
            pd.DataFrame: load forecast data with publish time
        """
        df["date"] = df["Interval Start"].dt.date
        unique_dates = sorted(df["date"].unique())

        # All daily forecasts are published at the same time each day, 9:10 AM PT
        # http://oasis.caiso.com/mrioasis/logon.do > Atlas Reference > Publications > OASIS Publications Schedule
        for forecast_date in unique_dates:
            publish_time = (
                pd.Timestamp(forecast_date, tz=self.default_timezone)
                - pd.Timedelta(days=day_offset)
            ).replace(
                hour=9,
                minute=10,
            )
            df.loc[df["date"] == forecast_date, "Publish Time"] = publish_time

        df = df.drop(columns=["date"])
        return df

    @support_date_range(frequency="31D")
    def get_load_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns actual load values

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pd.DataFrame: DataFrame with actual load data
        """
        df = self.get_oasis_dataset(
            dataset="demand_forecast",
            start=date,
            end=end,
            raw_data=False,
            verbose=verbose,
            sleep=sleep,
            params={"market_run_id": "ACTUAL"},
        )

        df = df.rename(
            columns={"MW": "Load", "TAC_AREA_NAME": "TAC Area Name"},
        )

        df.sort_values(by="Interval Start", inplace=True)
        df = df[
            [
                "Interval Start",
                "Interval End",
                "TAC Area Name",
                "Load",
            ]
        ]

        return df

    def get_renewables_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get wind and solar hourly actuals from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of wind and solar hourly actuals
        """
        if date == "latest":
            return self.get_renewables_hourly("today")

        df = self.get_oasis_dataset(
            dataset="renewables",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )
        return self._process_renewables_hourly(df)

    def get_renewables_forecast_dam(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return DAM renewable forecast in hourly intervals

        Data at: http://oasis.caiso.com/mrioasis/logon.do  at System Demand >
        DAM Renewable Forecast
        """
        if date == "latest":
            return self.get_renewables_forecast_dam("today", verbose=verbose)

        current_time = pd.Timestamp.now(tz=self.default_timezone)

        data = self.get_oasis_dataset(
            dataset="renewables_forecast_dam",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )

        return self._process_renewables_hourly(
            data,
            current_time,
            # Day-ahead hourly wind and solar forecast is published at 7:00 AM according
            # to OASIS.
            publish_time_offset_from_day_start=pd.Timedelta(hours=7),
        )

    def _process_renewables_hourly(
        self,
        data: pd.DataFrame,
        current_time: pd.Timestamp | None = None,
        publish_time_offset_from_day_start: pd.Timedelta | None = None,
    ):
        df = data[
            [
                "Interval Start",
                "Interval End",
                "TRADING_HUB",
                "RENEWABLE_TYPE",
                "MW",
            ]
        ]

        # Totals across all trading hubs for each renewable type at each interval
        totals = (
            df.groupby(
                ["RENEWABLE_TYPE", "Interval Start", "Interval End"],
            )["MW"]
            .sum()
            .reset_index()
        )

        totals["TRADING_HUB"] = "CAISO"

        df = pd.concat([df, totals])

        df = df.pivot_table(
            columns=["RENEWABLE_TYPE"],
            index=["Interval Start", "Interval End", "TRADING_HUB"],
            values="MW",
        ).reset_index()

        if publish_time_offset_from_day_start:
            df = self._add_forecast_publish_time(
                df,
                current_time=current_time,
                # Day-ahead hourly wind and solar forecast is published at 7:00 AM according
                # to OASIS.
                publish_time_offset_from_day_start=publish_time_offset_from_day_start,
            )

            df = utils.move_cols_to_front(
                df.rename(
                    columns={
                        "TRADING_HUB": "Location",
                        "Solar": "Solar MW",
                        "Wind": "Wind MW",
                    },
                ),
                ["Interval Start", "Interval End", "Publish Time", "Location"],
            )

            df.columns.name = None

            return df.sort_values(
                ["Interval Start", "Publish Time", "Location"],
            ).reset_index(drop=True)

        else:
            df = utils.move_cols_to_front(
                df.rename(
                    columns={
                        "TRADING_HUB": "Location",
                    },
                ),
                ["Interval Start", "Interval End", "Location"],
            )

            df.columns.name = None

            return df.sort_values(
                ["Interval Start", "Location"],
            ).reset_index(drop=True)

    def _add_forecast_publish_time(
        self,
        data: pd.DataFrame,
        current_time: pd.Timestamp,
        publish_time_offset_from_day_start: pd.Timedelta | None = None,
    ) -> pd.DataFrame:
        """
        Labels forecasts with a publish time using the logic:

        - If tomorrow or further in the future, the publish time is
            * Today's publish time if current time is after the publish time
            * Yesterday's publish time if current time is before the publish time
        - If today or earlier, the publish time is the previous day's publish time

        We assume the forecast was published the day before the forecasted day unless
        the forecast is in the future to avoid having publish times in the future.
        """
        hour_offset = publish_time_offset_from_day_start.components.hours
        minute_offset = publish_time_offset_from_day_start.components.minutes

        # Use replace to avoid DST issues
        todays_publish_time = current_time.normalize().replace(
            hour=hour_offset,
            minute=minute_offset,
        )
        if current_time > todays_publish_time:
            future_forecasts_publish_time = todays_publish_time
        else:
            future_forecasts_publish_time = todays_publish_time - pd.Timedelta(
                days=1,
            )

            # Forecasts tomorrow and later get the future forecasts publish time
            # Forecasts today and earlier get a publish time of the previous day at the
            # publish time offset

        # Default to existing DAM behavior for backward compatibility
        data["Publish Time"] = np.where(
            data["Interval Start"].dt.date > future_forecasts_publish_time.date(),
            future_forecasts_publish_time,
            data["Interval Start"].apply(
                lambda x: x.floor("D").replace(
                    hour=hour_offset,
                    minute=minute_offset,
                ),
            )
            - pd.Timedelta(days=1),
        )

        return data

    def _handle_renewables_forecast(
        self,
        df: pd.DataFrame,
        publish_time_offset: pd.Timedelta,
    ) -> pd.DataFrame:
        df = df.rename(
            columns={
                "TRADING_HUB": "Location",
                "RENEWABLE_TYPE": "Renewable Type",
            },
        )

        df = df.pivot_table(
            index=[
                "Interval Start",
                "Interval End",
                "Location",
            ],
            columns="Renewable Type",
            values="MW",
            aggfunc="first",
        ).reset_index()

        df.columns.name = None
        df["Publish Time"] = df["Interval Start"] - publish_time_offset
        return df[
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Location",
                "Solar",
                "Wind",
            ]
        ]

    def get_renewables_forecast_hasp(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get solar and wind generation HASP hourly data from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of solar and wind generation HASP hourly data
        """
        if date == "latest":
            try:
                return self.get_renewables_forecast_hasp(
                    pd.Timestamp.now(tz=self.default_timezone) + pd.Timedelta(hours=2),
                )  # NB: This is a hack to get the latest forecast
            except KeyError:
                return self.get_renewables_forecast_hasp(
                    pd.Timestamp.now(tz=self.default_timezone) + pd.Timedelta(hours=1),
                )

        df = self.get_oasis_dataset(
            dataset="renewables_forecast_hasp",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )
        return self._handle_renewables_forecast(
            df,
            publish_time_offset=pd.Timedelta(minutes=90),
        )

    def get_renewables_forecast_rtd(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get RTD renewable forecast from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of RTD renewable forecast
        """
        if date == "latest":
            return self.get_renewables_forecast_rtd(
                pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self.get_oasis_dataset(
            dataset="renewables_forecast_rtd",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )
        return self._handle_renewables_forecast(
            df,
            publish_time_offset=pd.Timedelta(minutes=2.5),
        )

    def get_renewables_forecast_rtpd(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get RTPD renewable forecast from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of RTPD renewable forecast
        """
        if date == "latest":
            return self.get_renewables_forecast_rtpd(
                pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self.get_oasis_dataset(
            dataset="renewables_forecast_rtpd",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )
        return self._handle_renewables_forecast(
            df,
            publish_time_offset=pd.Timedelta(minutes=22.5),
        )

    def get_pnodes(self, verbose: bool = False) -> pd.DataFrame:
        start = utils._handle_date("today")

        df = self.get_oasis_dataset(
            dataset="pnode_map",
            start=start,
            end=start + pd.Timedelta(days=1),
            verbose=verbose,
        )

        df = df.rename(
            columns={
                "APNODE_ID": "Aggregate PNode ID",
                "PNODE_ID": "PNode ID",
            },
        )
        return df

    @lmp_config(
        supports={
            Markets.DAY_AHEAD_HOURLY: ["latest", "today", "historical"],
            Markets.REAL_TIME_15_MIN: ["latest", "today", "historical"],
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
        },
    )
    @support_date_range(frequency=_determine_lmp_frequency)
    def get_lmp(
        self,
        date: str | pd.Timestamp,
        market: str,
        locations: list = None,
        sleep: int = 5,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ):
        """Get LMP pricing starting at supplied date for a list of locations.

        Arguments:
            date (datetime.date, str): date to return data

            market: market to return from. supports:

            locations (list): list of locations to get data from.
                If no locations are provided, defaults to NP15,
                SP15, and ZP26, which are the trading hub locations.
                USE "ALL_AP_NODES" for all Aggregate Pricing Node.
                Use "ALL" to get all nodes. For a list of locations,
                call ``CAISO.get_pnodes()``

            sleep (int): number of seconds to sleep before returning to
                avoid hitting rate limit in regular usage. Defaults to 5 seconds.

        Returns:
            pandas.DataFrame: A DataFrame of pricing data
        """
        if date == "latest":
            return self._latest_lmp_from_today(market=market, locations=locations)

        if locations is None:
            locations = self.trading_hub_locations

        assert isinstance(locations, list) or locations.lower() in [
            "all",
            "all_ap_nodes",
        ], "locations must be a list, 'ALL_AP_NODES', or 'ALL'"

        if market == Markets.DAY_AHEAD_HOURLY:
            dataset = "lmp_day_ahead_hourly"
            PRICE_COL = "MW"
        elif market == Markets.REAL_TIME_15_MIN:
            dataset = "lmp_real_time_15_min"
            PRICE_COL = "PRC"
        elif market == Markets.REAL_TIME_5_MIN:
            dataset = "lmp_real_time_5_min"
            PRICE_COL = "VALUE"
        else:
            raise RuntimeError("LMP Market is not supported")

        if isinstance(locations, list):
            nodes_str = ",".join(locations)
            params = {
                "node": nodes_str,
            }
        elif locations.lower() == "all":
            params = {
                "grp_type": "ALL",
            }
        elif locations.lower() == "all_ap_nodes":
            params = {
                "grp_type": "ALL_APNODES",
            }

        if (
            end is None
            and market in [Markets.REAL_TIME_15_MIN, Markets.REAL_TIME_5_MIN]
            and not isinstance(locations, list)
            and locations.lower() in ["all", "all_ap_nodes"]
        ):
            warnings.warn(
                "Only 1 hour of data will be returned for real time markets if end is "
                "not specified and all nodes are requested",
                # noqa
            )

        df = self.get_oasis_dataset(
            dataset=dataset,
            start=date,
            end=end,
            params=params,
            sleep=sleep,
            raw_data=False,
            verbose=verbose,
        )

        if df.empty:
            raise NoDataFoundException(
                f"No data found for start date: {date} and end date: {end}",
            )

        df = df.pivot_table(
            index=["Time", "Interval Start", "Interval End", "NODE"],
            columns="LMP_TYPE",
            values=PRICE_COL,
            aggfunc="first",
        )

        df = df.reset_index().rename(
            columns={
                "NODE": "Location",
                "LMP": "LMP",
                "MCE": "Energy",
                "MCC": "Congestion",
                "MCL": "Loss",
                "MGHG": "GHG",
            },
        )

        df["Market"] = market.value
        df["Location Type"] = "Node"

        # if -APND in location then "APND" Location Type

        df.loc[
            df["Location"].str.endswith("-APND"),
            "Location Type",
        ] = "AP Node"

        df.loc[
            df["Location"].isin(self.trading_hub_locations),
            "Location Type",
        ] = "Trading Hub"

        # if starts with "DLAP_" then "DLAP" Location Type
        df.loc[
            df["Location"].str.startswith("DLAP_"),
            "Location Type",
        ] = "DLAP"

        if market == Markets.DAY_AHEAD_HOURLY:
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
        else:
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
                    "GHG",
                ]
            ]

        # data = utils.filter_lmp_locations(df, locations=location_filter)
        data = df

        # clean up pivot name in header
        data.columns.name = None

        return data

    @support_date_range(frequency="DAY_START")
    def get_storage(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return storage charging or discharging for today in 5 minute intervals

        Negative means charging, positive means discharging

        Arguments:
            date (datetime.date, str): date to return data
        """
        if date == "latest":
            return self._latest_from_today(self.get_storage)

        df = _get_historical("storage", date, column="Total batteries", verbose=verbose)

        rename = {
            "Total batteries": "Supply",
            "Stand-alone batteries": "Stand-alone Batteries",
            "Hybrid batteries": "Hybrid Batteries",
        }

        # need to cast back to int since
        # _get_historical sometimes returns as float
        # because during DST switch there are nans
        # in the data that get dropped
        df[list(rename.keys())] = df[rename.keys()].astype(int)

        df = df.rename(
            columns=rename,
        )
        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Supply",
                "Stand-alone Batteries",
                "Hybrid Batteries",
            ]
        ]

        return df

    @support_date_range(frequency="31D")
    def get_gas_prices(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        fuel_region_id: str | list = "ALL",
        sleep: int = 4,
        verbose: bool = False,
    ):
        """Return gas prices at a previous date

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            fuel_region_id(str, or list): single fuel region id or list of fuel
                region ids to return data for. Defaults to ALL, which returns
                all fuel regions.

        Returns:
            pandas.DataFrame: A DataFrame of gas prices
        """

        if isinstance(fuel_region_id, list):
            fuel_region_id = ",".join(fuel_region_id)

        df = self.get_oasis_dataset(
            dataset="fuel_prices",
            start=date,
            end=end,
            params={
                "fuel_region_id": fuel_region_id,
            },
            raw_data=False,
            sleep=sleep,
        )

        df = df.rename(
            columns={
                "FUEL_REGION_ID": "Fuel Region Id",
                "PRC": "Price",
            },
        )
        df = (
            df.sort_values("Time")
            .sort_values(
                ["Fuel Region Id", "Time"],
            )
            .reset_index(drop=True)
        )

        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Fuel Region Id",
                "Price",
            ]
        ]
        return df

    def get_fuel_regions(self, verbose: bool = False) -> pd.DataFrame:
        """Retrieves the (mostly static) list of fuel regions with associated data.
        This file can be joined to the gas prices on Fuel Region Id"""
        url = (
            "https://www.caiso.com/documents/fuelregion_electricregiondefinitions.xlsx"  # noqa
        )

        logger.info(f"Fetching {url}")

        # Only want the "GPI_Fuel_Region" sheet
        return pd.read_excel(url, sheet_name="GPI_Fuel_Region").rename(
            columns={
                "Fuel Region": "Fuel Region Id",
                "Cap & Trade Credit": "Cap and Trade Credit",
            },
        )

    @support_date_range(frequency="31D")
    def get_ghg_allowance(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ):
        """Return ghg allowance at a previous date

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.
        """

        df = self.get_oasis_dataset(
            dataset="ghg_allowance",
            start=date,
            end=end,
            raw_data=False,
            sleep=sleep,
        )

        df = df.rename(
            columns={
                "GHG_PRC_IDX": "GHG Allowance Price",
            },
        )

        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "GHG Allowance Price",
            ]
        ]

        return df

    def get_raw_interconnection_queue(self, verbose: bool = False) -> pd.DataFrame:
        url = "http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx"

        logger.info(f"Downloading interconnection queue from {url}")
        response = requests.get(url)
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose: bool = False) -> pd.DataFrame:
        raw_data = self.get_raw_interconnection_queue(verbose)

        sheets = pd.read_excel(raw_data, skiprows=3, sheet_name=None)

        # remove legend at the bottom
        queued_projects = sheets["Grid GenerationQueue"][:-8]
        completed_projects = sheets["Completed Generation Projects"][:-2]
        withdrawn_projects = sheets["Withdrawn Generation Projects"][:-2].rename(
            columns={"Project Name - Confidential": "Project Name"},
        )

        queue = pd.concat(
            [queued_projects, completed_projects, withdrawn_projects],
        )

        queue = queue.rename(
            columns={
                "Interconnection Request\nReceive Date": (
                    "Interconnection Request Receive Date"
                ),
                "Actual\nOn-line Date": "Actual On-line Date",
                "Current\nOn-line Date": "Current On-line Date",
                "Interconnection Agreement \nStatus": (
                    "Interconnection Agreement Status"
                ),
                "Study\nProcess": "Study Process",
                "Proposed\nOn-line Date\n(as filed with IR)": (
                    "Proposed On-line Date (as filed with IR)"
                ),
                "System Impact Study or \nPhase I Cluster Study": (
                    "System Impact Study or Phase I Cluster Study"
                ),
                "Facilities Study (FAS) or \nPhase II Cluster Study": (
                    "Facilities Study (FAS) or Phase II Cluster Study"
                ),
                "Optional Study\n(OS)": "Optional Study (OS)",
            },
        )

        type_columns = ["Type-1", "Type-2", "Type-3"]
        queue["Generation Type"] = queue[type_columns].apply(
            lambda x: " + ".join(x.dropna()),
            axis=1,
        )

        rename = {
            "Queue Position": "Queue ID",
            "Project Name": "Project Name",
            "Generation Type": "Generation Type",
            "Queue Date": "Queue Date",
            "County": "County",
            "State": "State",
            "Application Status": "Status",
            "Current On-line Date": "Proposed Completion Date",
            "Actual On-line Date": "Actual Completion Date",
            "Reason for Withdrawal": "Withdrawal Comment",
            "Withdrawn Date": "Withdrawn Date",
            "Utility": "Transmission Owner",
            "Station or Transmission Line": "Interconnection Location",
            "Net MWs to Grid": "Capacity (MW)",
        }

        extra_columns = [
            "Type-1",
            "Type-2",
            "Type-3",
            "Fuel-1",
            "Fuel-2",
            "Fuel-3",
            "MW-1",
            "MW-2",
            "MW-3",
            "Interconnection Request Receive Date",
            "Interconnection Agreement Status",
            "Study Process",
            "Proposed On-line Date (as filed with IR)",
            "System Impact Study or Phase I Cluster Study",
            "Facilities Study (FAS) or Phase II Cluster Study",
            "Optional Study (OS)",
            "Full Capacity, Partial or Energy Only (FC/P/EO)",
            "Off-Peak Deliverability and Economic Only",
            "Feasibility Study or Supplemental Review",
        ]

        missing = [
            "Interconnecting Entity",
            "Summer Capacity (MW)",
            "Winter Capacity (MW)",
        ]

        queue = utils.format_interconnection_df(
            queue=queue,
            rename=rename,
            extra=extra_columns,
            missing=missing,
        )

        return queue

    @support_date_range(frequency="DAY_START")
    def get_curtailment_legacy(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return curtailment data for a given date

        Notes:
            * Data available from June 30, 2016 to May 31, 2025. For current data,
            please use `get_curtailment`.

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            verbose: print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of curtailment data
        """
        date = date.normalize()

        if date > pd.Timestamp("2025-05-31", tz=self.default_timezone):
            raise ValueError(
                "Curtailment data is only available until May 31, 2025. "
                "Please use `get_curtailment` for current data.",
            )

        # TODO: handle not always just 4th pge
        date_str = date.strftime("%b-%d-%Y").lower()

        if date < pd.Timestamp("2024-05-31", tz=date.tzinfo):
            base_url = "https://www.caiso.com/documents/wind_solarreal-timedispatchcurtailmentreport"
            date_str = date.strftime("%b%d_%Y").lower()
        else:
            base_url = "http://www.caiso.com/documents/wind-solar-real-time-dispatch-curtailment-report-"

        # Handle specific case where dec 02, 2021 has wrong year in file name
        if date_str == "dec02_2021":
            date_str = "02dec_2020"

        url = f"{base_url}{date_str}.pdf"
        logger.info(f"Fetching URL: {url}")

        r = requests.get(url)
        if r.status_code == 404:
            raise ValueError(f"Could not find curtailment PDF for {date}")

        pdf = io.BytesIO(r.content)
        if pdf is None:
            raise ValueError(f"Could not find curtailment PDF for {date}")

        tables = []
        header = None
        with pdfplumber.open(pdf) as pdf_doc:
            for page in pdf_doc.pages:
                extracted_tables = page.extract_tables()
                for table in extracted_tables:
                    if not table:
                        continue

                    # First page - get header and data
                    if any("FUEL TYPE" in str(col) for col in table[0]):
                        header = table[0]
                        df = pd.DataFrame(table[1:], columns=header)
                        tables.append(df)
                    # Subsequent pages - use saved header
                    elif header is not None:
                        df = pd.DataFrame(table, columns=header)
                        tables.append(df)

        if len(tables) == 0:
            raise ValueError("No tables found")

        df = pd.concat(tables).reset_index(drop=True)

        rename = {
            "DATE": "Date",
            "HOU\nR": "Hour",
            "HOUR": "Hour",
            "CURT TYPE": "Curtailment Type",
            "REASON": "Curtailment Reason",
            "FUEL TYPE": "Fuel Type",
            "CURTAILED MWH": "Curtailment (MWh)",
            "CURTAILED\rMWH": "Curtailment (MWh)",
            "CURTAILED MW": "Curtailment (MW)",
            "CURTAILED\rMW": "Curtailment (MW)",
        }

        df = df.rename(columns=rename)

        # convert from hour ending to hour beginning
        df["Hour"] = df["Hour"].astype(int) - 1
        df["Time"] = df["Hour"].apply(
            lambda x, date=date: date + pd.Timedelta(hours=x),
        )

        df["Interval Start"] = df["Time"]
        df["Interval End"] = df["Time"] + pd.Timedelta(hours=1)

        df = df.drop(columns=["Date", "Hour"])

        df["Fuel Type"] = df["Fuel Type"].map(
            {
                "SOLR": "Solar",
                "WIND": "Wind",
            },
        )

        df = df[
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Curtailment Type",
                "Curtailment Reason",
                "Fuel Type",
                "Curtailment (MWh)",
                "Curtailment (MW)",
            ]
        ]
        df = df.replace("", np.nan)
        return df

    @support_date_range(frequency="DAY_START")
    def get_as_prices(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        market: str = "DAM",
        sleep: int = 4,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return AS prices for a given date for each region

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            market (str): DAM or HASP. Defaults to DAM.

            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of AS prices
        """

        params = {
            "market_run_id": market,
        }

        df = self.get_oasis_dataset(
            dataset="as_clearing_prices",
            start=date,
            end=end,
            params=params,
            sleep=sleep,
            verbose=verbose,
            raw_data=False,
        )

        df = df.rename(
            columns={
                "ANC_REGION": "Region",
                "MARKET_RUN_ID": "Market",
            },
        )

        as_type_map = {
            "NR": "Non-Spinning Reserves",
            "RD": "Regulation Down",
            "RMD": "Regulation Mileage Down",
            "RMU": "Regulation Mileage Up",
            "RU": "Regulation Up",
            "SR": "Spinning Reserves",
        }
        df["ANC_TYPE"] = df["ANC_TYPE"].map(as_type_map)

        df = df.pivot_table(
            index=[
                "Time",
                "Interval Start",
                "Interval End",
                "Region",
                "Market",
            ],
            columns="ANC_TYPE",
            values="MW",
        ).reset_index()

        df = df.fillna(0)

        df.columns.name = None

        return df

    @support_date_range(frequency="DAY_START")
    def get_curtailed_non_operational_generator_report(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return curtailed non-operational generator report for a given date.
           Earliest available date is June 17, 2021.

        Arguments:
            date (str, pd.Timestamp): date to return data
            end (str, pd.Timestamp, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of curtailed non-operational generator report

        Notes:
            column glossary:
            http://www.caiso.com/market/Pages/OutageManagement/Curtailed
            -OperationalGeneratorReportGlossary.aspx

            if requesting multiple days, may want to run
            following to remove outages that get reported across multiple days:
            ```df.drop_duplicates(
                subset=["OUTAGE MRID", "CURTAILMENT START DATE TIME"], keep="last")```


        """

        # date must on or be after june 17, 2021
        if date.date() < pd.Timestamp("2021-06-17").date():
            raise ValueError(
                "Date must be on or after June 17, 2021",
            )

        # Between May 31, 2024 and Jan 13, 2025, the date format is
        # %b-%d-%Y.lower() (jun-01-2024)
        date_str = date.strftime("%b-%d-%Y").lower()

        # May 31, 2024 uses a unique format (2024-05-31)
        if date.date() == pd.Timestamp("2024-05-31").date():
            date_str = date.strftime("%Y-%m-%d").lower()
        # Before May 31, 2024 and after Jan 12, 2025 date format is %Y%m%d (20240530)
        elif (date < pd.Timestamp("2024-05-31", tz=date.tzinfo)) or (
            date > pd.Timestamp("2025-01-12", tz=date.tzinfo)
        ):
            date_str = date.strftime("%Y%m%d")

        url = f"https://www.caiso.com/documents/curtailed-non-operational-generator-prior-trade-date-report-{date_str}.xlsx"  # noqa

        # Jun 1, 2024 has an extra "and" in the url
        if date.date() == pd.Timestamp("2024-06-01").date():
            url = f"https://www.caiso.com/documents/curtailed-and-non-operational-generator-prior-trade-date-report-{date_str}.xlsx"  # noqa

        logger.info(f"Fetching {url}")
        # fetch this way to avoid having to
        # make request twice
        content = requests.get(url).content
        content_io = io.BytesIO(content)

        # find index of OUTAGE MRID
        test_parse = pd.read_excel(
            content_io,
            usecols="B:M",
            sheet_name="PREV_DAY_OUTAGES",
            engine="openpyxl",
        )
        first_col = test_parse[test_parse.columns[0]]
        outage_mrid_index = first_col[first_col == "OUTAGE MRID"].index[0] + 1

        # load again, but skip rows up to outage mrid
        df = pd.read_excel(
            content_io,
            usecols="B:M",
            skiprows=outage_mrid_index,
            sheet_name="PREV_DAY_OUTAGES",
            engine="openpyxl",
        )

        # drop columns where the name is nan
        # artifact of the excel file
        df = df.dropna(axis=1, how="all")

        # published day after
        publish_time = date.normalize() + pd.DateOffset(days=1)
        df.insert(0, "Publish Time", publish_time)

        df["CURTAILMENT START DATE TIME"] = pd.to_datetime(
            df["CURTAILMENT START DATE TIME"],
        ).dt.tz_localize(self.default_timezone, ambiguous=True)
        df["CURTAILMENT END DATE TIME"] = pd.to_datetime(
            df["CURTAILMENT END DATE TIME"],
        ).dt.tz_localize(self.default_timezone, ambiguous=True)

        # only some dates have this
        if "OUTAGE STATUS" in df.columns:
            df = df.drop(columns=["OUTAGE STATUS"])

        df = df.rename(
            columns={
                "OUTAGE MRID": "Outage MRID",
                "RESOURCE NAME": "Resource Name",
                "RESOURCE ID": "Resource ID",
                "OUTAGE TYPE": "Outage Type",
                "NATURE OF WORK": "Nature of Work",
                "CURTAILMENT START DATE TIME": "Curtailment Start Time",
                "CURTAILMENT END DATE TIME": "Curtailment End Time",
                "CURTAILMENT MW": "Curtailment MW",
                "RESOURCE PMAX MW": "Resource PMAX MW",
                "NET QUALIFYING CAPACITY MW": "Net Qualifying Capacity MW",
            },
        )

        # if there are duplicates, set trce
        if df.duplicated(subset=["Outage MRID", "Curtailment Start Time"]).any():
            # drop where start and end are the same and end time isnt null
            # this appears to fix
            df = df[
                ~(
                    (df["Curtailment Start Time"] == df["Curtailment End Time"])
                    & (df["Curtailment End Time"].notnull())
                )
            ]

            assert not df.duplicated(
                subset=["Outage MRID", "Curtailment Start Time"],
            ).any(), "There are still duplicates"

        return df

    @support_date_range(frequency="DAY_START")
    def get_tie_flows_real_time(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return real time tie flow data.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame of real time tie flow data
        """
        if date == "latest":
            date = pd.Timestamp.utcnow().round("5min")
            end = date + pd.Timedelta(minutes=5)

        df = self.get_oasis_dataset(
            dataset="tie_flows_real_time",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )

        return self._process_tie_flows_data(df)

    def get_tie_flows_real_time_15_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            date = pd.Timestamp.utcnow().round("15min")
            end = date + pd.Timedelta(minutes=15)

        df = self.get_oasis_dataset(
            dataset="tie_flows_real_time_15_min",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )

        return self._process_tie_flows_data(df)

    def _process_tie_flows_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop(
            columns=[
                "Time",
                "DATA_ITEM",
                "OPR_DT",
                "OPR_HR",
                "OPR_INTERVAL",
                "OASIS_REC_STAT",
                "UPD_DATE",
                "UPD_BY",
                "GROUP",
                # Same as FROM_BAA
                "BAA_GRP_ID",
            ],
        ).rename(columns={"MARKET_TYPE": "MARKET", "VALUE": "MW"})

        # Multiply imports by -1 to match convention of imports being negative
        df["MW"] = np.where(df["DIRECTION"] == "I", df["MW"] * -1, df["MW"])

        # Sum MW by Interval Start, TIE_NAME, FROM_BAA, TO_BAA so we can remove
        # the direction column
        df = (
            df.groupby(
                [
                    "Interval Start",
                    "Interval End",
                    "TIE_NAME",
                    "FROM_BAA",
                    "TO_BAA",
                    "MARKET",
                ],
            )["MW"]
            .sum()
            .reset_index()
        )

        df.columns = df.columns.map(
            lambda x: x.title()
            .replace("_", " ")
            .replace("Baa", "BAA")
            .replace("Mw", "MW"),
        )

        # Create an identifier column (separated by hyphens because some of the tie
        # names have underscores in them) to use for indexing
        df["Interface ID"] = df["Tie Name"] + "-" + df["From BAA"] + "-" + df["To BAA"]

        df = utils.move_cols_to_front(
            df,
            [
                "Interval Start",
                "Interval End",
                "Interface ID",
                "Tie Name",
                "From BAA",
                "To BAA",
                "Market",
                "MW",
            ],
        )

        return df.sort_values(
            ["Interval Start", "Interface ID"],
        )

    @support_date_range(frequency="31D")
    def get_as_procurement(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        market: str = "DAM",
        sleep: int = 4,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get ancillary services procurement data from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            market (str, optional): DAM or RTM. Defaults to "DAM".
            sleep (int, optional): number of seconds to sleep between requests. Defaults to 4.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of ancillary services data
        """

        assert market in ["DAM", "RTM"], "market must be DAM or RTM"

        df = self.get_oasis_dataset(
            dataset="as_results",
            start=date,
            end=end,
            params={
                "market_run_id": market,
            },
            sleep=sleep,
            verbose=verbose,
            raw_data=False,
        )

        df = df.rename(
            columns={
                "ANC_REGION": "Region",
                "MARKET_RUN_ID": "Market",
            },
        )

        as_type_map = {
            "NR": "Non-Spinning Reserves",
            "RD": "Regulation Down",
            "RMD": "Regulation Mileage Down",
            "RMU": "Regulation Mileage Up",
            "RU": "Regulation Up",
            "SR": "Spinning Reserves",
        }
        df["ANC_TYPE"] = df["ANC_TYPE"].map(as_type_map)

        result_type_map = {
            "AS_BUY_MW": "Procured (MW)",
            "AS_SELF_MW": "Self-Provided (MW)",
            "AS_MW": "Total (MW)",
            "AS_COST": "Total Cost",
        }
        df["RESULT_TYPE"] = df["RESULT_TYPE"].map(result_type_map)

        df["column"] = df["ANC_TYPE"] + " " + df["RESULT_TYPE"]

        df = df.pivot_table(
            index=[
                "Time",
                "Interval Start",
                "Interval End",
                "Region",
                "Market",
            ],
            columns="column",
            values="MW",
        ).reset_index()

        df.columns.name = None

        return df

    def get_lmp_scheduling_point_tie_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get LMP scheduling point tie combination 5-min data from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of LMP scheduling point tie combination 5-min data
        """
        if date == "latest":
            return self.get_lmp_scheduling_point_tie_real_time_5_min(
                pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self.get_oasis_dataset(
            dataset="lmp_scheduling_point_tie_combination_5_min",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )
        return self._handle_lmp_scheduling_point_tie_combination(df)

    def get_lmp_scheduling_point_tie_real_time_15_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            return self.get_lmp_scheduling_point_tie_real_time_15_min(
                pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self.get_oasis_dataset(
            dataset="lmp_scheduling_point_tie_combination_15_min",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )
        return self._handle_lmp_scheduling_point_tie_combination(df)

    def get_lmp_scheduling_point_tie_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        if date == "latest":
            try:
                df = self.get_lmp_scheduling_point_tie_day_ahead_hourly(
                    pd.Timestamp.now(tz=self.default_timezone).normalize()
                    + pd.Timedelta(days=1),
                )
            except KeyError:
                df = self.get_lmp_scheduling_point_tie_day_ahead_hourly(
                    "today",
                )

            return df

        df = self.get_oasis_dataset(
            dataset="lmp_scheduling_point_tie_combination_hourly",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )
        return self._handle_lmp_scheduling_point_tie_combination(df)

    def _handle_lmp_scheduling_point_tie_combination(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        df = df.rename(
            columns={
                "NODE": "Node",
                "TIE": "Tie",
                "MARKET_RUN_ID": "Market",
            },
        )

        df = df.pivot_table(
            index=[
                "Interval Start",
                "Interval End",
                "Node",
                "Tie",
                "Market",
            ],
            columns="LMP_TYPE",
            values="PRC",
            aggfunc="first",
        ).reset_index()

        df.columns.name = None
        df = df.rename(
            columns={
                "MCE": "Energy",
                "MCC": "Congestion",
                "MCL": "Loss",
                "MGHG": "GHG",
            },
        )

        df["Location"] = df["Node"] + " " + df["Tie"]
        return df[
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Market",
                "Node",
                "Tie",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
                "GHG",
            ]
        ]

    def get_lmp_hasp_15_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get LMP HASP 15-min data from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of LMP HASP 15-min data
        """
        if date == "latest":
            return self.get_lmp_hasp_15_min(
                pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self.get_oasis_dataset(
            dataset="lmp_hasp_15_min",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )
        return self._handle_lmp_hasp_15_min(df)

    def _handle_lmp_hasp_15_min(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={"NODE": "Location"},
        )
        df = df.pivot_table(
            index=[
                "Interval Start",
                "Interval End",
                "Location",
            ],
            columns="LMP_TYPE",
            values="MW",  # NB: This is likely a mistake from CAISO, should probably be PRC
            aggfunc="first",
        ).reset_index()

        df.columns.name = None
        df = df.rename(
            columns={
                "MCE": "Energy",
                "MCC": "Congestion",
                "MCL": "Loss",
                "MGHG": "GHG",
            },
        )
        return df[
            [
                "Interval Start",
                "Interval End",
                "Location",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
                "GHG",
            ]
        ]

    @support_date_range(frequency="31D")
    def get_nomogram_branch_shadow_prices_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns hourly day-ahead nomogram/branch shadow price forecast.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched.

        Returns:
            pandas.DataFrame: A DataFrame with the shadow price forecast
        """
        if date == "latest":
            return self.get_nomogram_branch_shadow_prices_day_ahead_hourly(
                pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self.get_oasis_dataset(
            dataset="nomogram_branch_shadow_prices",
            date=date,
            end=end,
            params={"market_run_id": "DAM"},
            verbose=verbose,
            raw_data=False,
        )

        df = df.rename(
            columns={
                "NOMOGRAM_ID": "Location",
                "PRC": "Price",
            },
        )

        return df[["Interval Start", "Interval End", "Location", "Price"]]

    def get_nomogram_branch_shadow_prices_hasp_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns nomogram/branch shadow price HASP hourly data from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched.

        Returns:
            pandas.DataFrame: A DataFrame with the shadow price HASP data
        """
        if date == "latest":
            return self.get_nomogram_branch_shadow_prices_hasp_hourly(
                pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self.get_oasis_dataset(
            dataset="nomogram_branch_shadow_prices",
            date=date,
            end=end,
            params={"market_run_id": "HASP"},
            verbose=verbose,
            raw_data=False,
        )

        df = df.rename(
            columns={
                "NOMOGRAM_ID": "Location",
                "PRC": "Price",
            },
        )

        return df[["Interval Start", "Interval End", "Location", "Price"]]

    def get_nomogram_branch_shadow_price_forecast_15_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Returns 15-minute nomogram/branch shadow price forecast from the Real-Time Pre-Dispatch Market.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched.

        Returns:
            pandas.DataFrame: A DataFrame with the shadow price forecast
        """
        if date == "latest":
            return self.get_nomogram_branch_shadow_price_forecast_15_min(
                pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self.get_oasis_dataset(
            dataset="nomogram_branch_shadow_prices",
            date=date,
            end=end,
            params={"market_run_id": "RTM"},
            verbose=verbose,
            raw_data=False,
        )

        df = df.rename(
            columns={
                "NOMOGRAM_ID": "Location",
                "PRC": "Price",
            },
        )

        return df[["Interval Start", "Interval End", "Location", "Price"]]

    def get_interval_nomogram_branch_shadow_prices_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Get 5-min nomogram/branch shadow prices from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched.

        Returns:
            pandas.DataFrame: A DataFrame with the shadow prices
        """
        if date == "latest":
            return self.get_interval_nomogram_branch_shadow_prices_real_time_5_min(
                pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self.get_oasis_dataset(
            dataset="interval_nomogram_branch_shadow_prices",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )

        df = df.rename(
            columns={
                "NOMOGRAM_ID": "Location",
                "PRC": "Price",
            },
        )

        return df[["Interval Start", "Interval End", "Location", "Price"]]

    @support_date_range(frequency="DAY_START")
    def get_curtailment(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """Return curtailment data for a given date

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            verbose: print out url being fetched. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame of curtailment data
        """
        if date == "latest":
            # Latest available curtailment data is usually the previous day because
            # data is released at 10 AM for the previous day.
            return self.get_curtailment(
                pd.Timestamp.now(tz=self.default_timezone) - pd.DateOffset(days=1),
            )

        dataframes = self.get_caiso_renewables_report(date)

        # Process all fuel types and units in one loop
        fuel_configs = [
            ("solar_curtailment_total_hourly", "Solar", "MWH"),
            ("wind_curtailment_total_hourly", "Wind", "MWH"),
            ("solar_curtailment_maximum_hourly", "Solar", "MW"),
            ("wind_curtailment_maximum_hourly", "Wind", "MW"),
        ]

        melted_dfs = {}
        for df_key, fuel_type, unit in fuel_configs:
            # Melt from a wide format to a long format
            df = dataframes[df_key].melt(
                id_vars=["Interval Start", "Interval End"],
                var_name="Curtailment Type",
                value_name=f"Curtailment {unit}",
            )

            # Curtailment Type format is "Curtailment Type Curtailment Reason MW/MWH"
            # Split it into two columns: Curtailment Type and Curtailment Reason
            curtailment_parts = df["Curtailment Type"].str.split(" ")
            df["Curtailment Type"] = curtailment_parts.str[0]
            df["Curtailment Reason"] = curtailment_parts.str[1]
            df["Fuel Type"] = fuel_type

            melted_dfs[f"{fuel_type} {unit}"] = df

        # Merge MWH and MW data for each fuel type
        merge_cols = [
            "Interval Start",
            "Interval End",
            "Curtailment Type",
            "Curtailment Reason",
            "Fuel Type",
        ]

        solar_df = melted_dfs["Solar MWH"].merge(
            melted_dfs["Solar MW"],
            on=merge_cols,
            how="outer",
        )
        wind_df = melted_dfs["Wind MWH"].merge(
            melted_dfs["Wind MW"],
            on=merge_cols,
            how="outer",
        )

        return (
            pd.concat([solar_df, wind_df])
            .reindex(columns=merge_cols + ["Curtailment MWH", "Curtailment MW"])
            .sort_values(merge_cols)
            .reset_index(drop=True)
        )

    def get_caiso_renewables_report(
        self,
        date: pd.Timestamp,
    ) -> dict[str, pd.DataFrame]:
        """
        Fetches the CAISO daily renewable report for a given date and extracts data from
        all the charts into wide dataframes.
        """
        report_url = f"https://www.caiso.com/documents/daily-renewable-report-{date.strftime('%b-%d-%Y').lower()}.html"

        response = requests.get(report_url)

        if response.status_code != 200:
            raise ValueError(
                f"Failed to fetch renewables report for {date.strftime('%Y-%m-%d')}: "
                f"HTTP {response.status_code}",
            )

        html_content = response.content.decode("utf-8")

        def extract_array(content: str, var_name: str) -> list:
            # Extracts a JavaScript array from the HTML content. Some of the arrays]
            # are wrapped in JSON.parse().
            pattern = rf'{var_name}\s*=\s*(?:JSON\.parse\(\["?)?\[([^\]]*)\]\)?'
            match = re.search(pattern, content, re.DOTALL)
            if not match:
                return []

            array_str = match.group(1)
            values = []
            for item in array_str.split(","):
                item = item.strip()
                if item in ('"NA"', "NA"):
                    values.append(np.nan)
                elif item.startswith('"') and item.endswith('"'):
                    values.append(item[1:-1])
                else:
                    try:
                        values.append(float(item))
                    except ValueError:
                        values.append(item)
            return values

        base_date = date.normalize()

        dataframe_configs = get_dataframe_config_for_renewables_report(
            base_date,
            self.default_timezone,
        )

        # Build all DataFrames using the configuration
        dataframes = {}
        for df_name, timestamps, duration, unit, column_mapping in dataframe_configs:
            interval_end_timedelta = pd.DateOffset(**{f"{unit}s": duration})

            data = {
                "Interval Start": timestamps,
                "Interval End": timestamps + interval_end_timedelta,
            }

            for col_name, var_name in column_mapping.items():
                data[col_name] = extract_array(html_content, var_name)

            dataframes[df_name] = pd.DataFrame(data)

        return dataframes

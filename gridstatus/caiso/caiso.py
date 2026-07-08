import copy
import io
import re
import time
import warnings
import xml.etree.ElementTree as ElementTree
from datetime import datetime
from typing import Literal
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pdfplumber
import polars as pl
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
from gridstatus.caiso import caiso_utils, daily_energy_storage
from gridstatus.caiso.caiso_constants import (
    CURRENT_BASE,
    HISTORY_BASE,
    OASIS_DATASET_CONFIG,
    get_dataframe_config_for_renewables_report,
)
from gridstatus.decorators import support_date_range
from gridstatus.gs_logging import logger
from gridstatus.lmp_config import lmp_config

# The PRC_CORR_GRP summary lists every market/correction-method combination for a
# trade date; combinations with no corrections carry this sentinel in the reason
# column and are dropped during parsing.
PRICE_CORRECTION_NO_RECORDS_REASON = "No records found for report."


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


def _collapse_group_to_array(
    df: pl.DataFrame,
    group_cols: list[str],
) -> pl.DataFrame:
    """Collapse multiple rows with different Group values into a single row
    with a Groups list column. The non-group columns must be identical across
    groups for a given combination of group_cols."""
    return (
        df.group_by(group_cols, maintain_order=True)
        .agg(
            pl.col("Group")
            .filter(pl.col("Group").is_not_nan())
            .drop_nulls()
            .cast(pl.Int64)
            .sort()
            .implode()
            .alias("_groups"),
        )
        .with_columns(
            pl.col("_groups")
            .map_elements(
                lambda groups: groups.to_list()
                if isinstance(groups, pl.Series)
                else list(groups),
                return_dtype=pl.Object,
            )
            .alias("Groups"),
        )
        .drop("_groups")
    )


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
) -> pl.DataFrame:
    """Get the historical data file from CAISO given a data series name, formats, and returns a polars dataframe.

    Args:
        file (str): The name of the data we are wanting, which is equivalent to the file to get from CAISO
        date (str | pd.Timestamp): The date of the data to get from CAISO
        column (str): The column to check for the latest value time
        verbose (bool, optional): Whether to print out the URL being fetched, defaults to False

    Returns:
        pl.DataFrame: A polars dataframe of the data
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
    pdf = pd.read_csv(url)
    pdf = pdf.dropna(subset=["Time"])
    pdf = pdf.dropna(subset=pdf.columns[1:], how="all")
    df = pl.from_pandas(pdf)

    if latest:
        latest_file_time = caiso_utils.check_latest_value_time(df, column)
        current_caiso_time = pd.Timestamp.now(tz=CAISO.default_timezone)

        if latest_file_time > current_caiso_time:
            date = date - pd.Timedelta(days=1)

    def _apply_make_timestamp(time_str: str) -> pd.Timestamp:
        return caiso_utils.make_timestamp(
            time_str,
            today=date,
            timezone=CAISO.default_timezone,
        )

    df = df.with_columns(
        pl.col("Time")
        .map_elements(
            _apply_make_timestamp,
            return_dtype=pl.Datetime(time_zone=CAISO.default_timezone),
        )
        .alias("Time"),
    )

    if df[-1, "Time"].hour == 0:
        df = df.head(df.height - 1)

    value_cols = [c for c in df.columns if c != "Time"]
    df = df.with_columns(
        pl.col("Time").alias("Interval Start"),
        (pl.col("Time") + pl.duration(minutes=5)).alias("Interval End"),
    )
    return df.select(["Time", "Interval Start", "Interval End", *value_cols])


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
    ) -> pl.DataFrame:
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
            pl.DataFrame: A DataFrame of data from OASIS
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
            return pl.DataFrame()

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
    ) -> pl.DataFrame | None:
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
            r = requests.get(url, verify=True)

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
        dfs: list[pl.DataFrame] = []
        logger.debug(f"Found {len(z.namelist())} files: {z.namelist()}")
        for f in z.namelist():
            logger.debug(f"Parsing file: {f}")
            pdf = pd.read_csv(z.open(f))
            dfs.append(pl.from_pandas(pdf))

        df = pl.concat(dfs, how="diagonal_relaxed")

        float_cols = [
            c
            for c in df.columns
            if c in {"PRC", "MW", "VALUE", "MARGINAL_CLEARING_PRICE"}
            or c.endswith("_PRICE")
        ]
        if float_cols:
            df = df.with_columns(
                *[pl.col(c).cast(pl.Float64, strict=False) for c in float_cols],
            )

        # if col ends in _GMT, then try to parse as UTC
        for col in df.columns:
            if col.endswith("_GMT"):
                dtype = df.schema[col]
                if dtype in (pl.Utf8, pl.String):
                    df = df.with_columns(
                        pl.col(col)
                        .str.to_datetime(time_unit="us", time_zone="UTC")
                        .alias(col),
                    )
                elif isinstance(dtype, pl.Datetime):
                    if dtype.time_zone is None:
                        df = df.with_columns(
                            pl.col(col).dt.replace_time_zone("UTC").alias(col),
                        )
                    elif dtype.time_zone != "UTC":
                        df = df.with_columns(
                            pl.col(col).dt.convert_time_zone("UTC").alias(col),
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
                df = df.sort(col)
                break
        for col in end_cols:
            if col in df.columns:
                end_col = col
                break

        if not raw_data and start_col is not None and start_col in df.columns:
            df = df.with_columns(
                pl.col(start_col).dt.convert_time_zone(CAISO.default_timezone),
                pl.col(end_col).dt.convert_time_zone(CAISO.default_timezone),
            ).rename(
                {
                    start_col: "Interval Start",
                    end_col: "Interval End",
                },
            )
            df = df.with_columns(pl.col("Interval Start").alias("Time"))
            other_cols = [
                c
                for c in df.columns
                if c not in {"Time", "Interval Start", "Interval End"}
            ]
            df = df.select(["Time", "Interval Start", "Interval End", *other_cols])

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
    ) -> pl.DataFrame:
        """Get fuel mix in 5 minute intervals for a provided day.

        Args:
            date: "latest", "today", or an object that can be parsed as a
                datetime for the day to return data.
            start: Start of date range to return. Alias for ``date`` parameter.
                Only specify one of ``date`` or ``start``.
            end: "today" or an object that can be parsed as a datetime for the
                day to return data. Only used if requesting a range of dates.
            verbose: Print verbose output. Defaults to False.

        Returns:
            A DataFrame with columns for Time and each fuel type.
        """
        if date == "latest":
            mix = self.get_fuel_mix("today", verbose=verbose)
            return mix.tail(1)

        return self._get_historical_fuel_mix(date, verbose=verbose)

    def _get_historical_fuel_mix(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pl.DataFrame:
        df = _get_historical("fuelsource", date, column="Solar", verbose=verbose)

        # rename some inconsistent columns names to standardize across dates
        df = df.rename(
            {
                "Small hydro": "Small Hydro",
                "Natural gas": "Natural Gas",
                "Large hydro": "Large Hydro",
            },
        )

        # when day light savings time switches, there are na rows
        # maybe better way to do this in case there are other cases
        # where there are all na rows
        # ignore Time, Interval Start, Interval End columns
        subset = [
            c for c in df.columns if c not in {"Time", "Interval Start", "Interval End"}
        ]
        df = df.filter(
            ~pl.all_horizontal(pl.col(c).is_null() for c in subset),
        )

        return df

    @support_date_range(frequency="DAY_START")
    def get_load(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return load at a previous date in 5 minute intervals"""

        if date == "latest":
            return self.get_load("today", verbose=verbose)

        return self._get_historical_load(date, verbose=verbose)

    def _get_historical_load(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pl.DataFrame:
        df = _get_historical("demand", date, column="Current demand", verbose=verbose)

        df = df.select(["Time", "Interval Start", "Interval End", "Current demand"])
        df = df.rename({"Current demand": "Load"})
        df = df.filter(pl.col("Load").is_not_null())
        return df

    @support_date_range(frequency="DAY_START")
    def get_seven_day_resource_adequacy_outlook(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Seven-day resource adequacy outlook in 5-minute intervals.

        Source: ``/outlook/history/{{yyyymmdd}}/rtm_forecast_7day.csv`` (historical)
        or current outlook for today.

        The CSV ``Time`` column marks interval end; ``Interval Start`` is five minutes
        prior. ``Publish Time`` is midnight Pacific on the publication date encoded in
        the URL path.
        """
        if date == "latest":
            return self.get_seven_day_resource_adequacy_outlook(
                "today",
                end=end,
                verbose=verbose,
            )

        publish_day = utils._handle_date(date, self.default_timezone).normalize()
        raw = self._fetch_rtm_forecast_7day_csv(publish_day, verbose=verbose)
        df = self._parse_rtm_forecast_7day_csv(raw, publish_day)
        if df.is_empty():
            raise NoDataFoundException(
                f"No seven-day resource adequacy outlook data found for {publish_day.date()}",
            )
        return df

    def _fetch_rtm_forecast_7day_csv(
        self,
        date: pd.Timestamp,
        verbose: bool = False,
    ) -> pl.DataFrame:
        tz = self.default_timezone
        file_stem = "rtm_forecast_7day"
        cache_buster = int(pd.Timestamp.now(tz=tz).timestamp())
        if utils.is_today(date, tz):
            url = f"{CURRENT_BASE}/{file_stem}.csv?_={cache_buster}"
        else:
            date_str = date.strftime("%Y%m%d")
            url = f"{HISTORY_BASE}/{date_str}/{file_stem}.csv?_={cache_buster}"
        logger.info(f"Fetching URL: {url}")

        try:
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            return pl.from_pandas(pd.read_csv(io.StringIO(r.text)))

        except ValueError as e:
            raise NoDataFoundException(
                f"No seven-day resource adequacy outlook data found for {date.date()}: {e}",
            ) from e

    def _parse_rtm_forecast_7day_csv(
        self,
        df: pl.DataFrame,
        publish_day_pt: pd.Timestamp,
    ) -> pl.DataFrame:
        df = df.filter(pl.col("Time").is_not_null())
        value_cols = [c for c in df.columns if c != "Time"]
        df = df.filter(
            ~pl.all_horizontal(pl.col(c).is_null() for c in value_cols),
        )
        df = df.with_columns(
            pl.col("Time")
            .str.to_datetime(format="%m/%d/%Y %H:%M")
            .alias("_interval_end_naive"),
        )
        df = utils.localize_shift_forward_polars(
            df,
            "_interval_end_naive",
            self.default_timezone,
        )
        df = df.filter(pl.col("_interval_end_naive").is_not_null())
        publish_ts = publish_day_pt.tz_convert(self.default_timezone).normalize()
        df = df.with_columns(
            pl.col("_interval_end_naive").alias("Interval End"),
            (pl.col("_interval_end_naive") - pl.duration(minutes=5)).alias(
                "Interval Start",
            ),
            pl.lit(publish_ts).alias("Publish Time"),
        )
        df = df.rename(
            {
                "Day-ahead demand forecast": "Day Ahead Demand Forecast",
                "Day-ahead net demand forecast": "Day Ahead Net Demand Forecast",
                "Resource adequacy capacity forecast": "Resource Adequacy Capacity Forecast",
                "Net resource adequacy capacity forecast": "Net Resource Adequacy Capacity Forecast",
                "Reserve requirement": "Reserve Requirement",
                "Reserve requirement forecast": "Reserve Requirement Forecast",
                "Resource adequacy credits": "Resource Adequacy Credits",
            },
        )
        numeric_cols = [
            "Demand",
            "Net Demand",
            "Day Ahead Demand Forecast",
            "Day Ahead Net Demand Forecast",
            "Resource Adequacy Capacity Forecast",
            "Net Resource Adequacy Capacity Forecast",
            "Reserve Requirement",
            "Reserve Requirement Forecast",
            "Resource Adequacy Credits",
        ]
        df = df.with_columns(
            [pl.col(col).cast(pl.Float64, strict=False) for col in numeric_cols],
        )
        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Demand",
                "Net Demand",
                "Day Ahead Demand Forecast",
                "Day Ahead Net Demand Forecast",
                "Resource Adequacy Capacity Forecast",
                "Net Resource Adequacy Capacity Forecast",
                "Reserve Requirement",
                "Reserve Requirement Forecast",
                "Resource Adequacy Credits",
            ],
        ).sort(["Interval Start", "Publish Time"])

    # Deprecated in favor of the vintage-based functions, e.g. get_load_forecast_5_min
    @support_date_range(frequency="31D")
    def get_load_forecast(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        if date == "today" or date == "latest":
            date = pd.Timestamp.now(tz=self.default_timezone).normalize()
        df = self.get_load_forecast_day_ahead(date, end=end)
        df = df.with_columns(pl.col("Interval Start").alias("Time"))
        df = df.filter(pl.col("TAC Area Name") == "CA ISO-TAC")
        return df.drop("TAC Area Name")

    @support_date_range(frequency="31D")
    def get_load_forecast_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns 5-minute load forecast from the Real-Time Market

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pl.DataFrame: DataFrame with load forecast data
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
            {"MW": "Load Forecast", "TAC_AREA_NAME": "TAC Area Name"},
        )

        df = df.with_columns(
            (pl.col("Interval Start") - pl.duration(minutes=2, seconds=30)).alias(
                "Publish Time",
            ),
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "TAC Area Name",
                "Load Forecast",
            ],
        ).sort("Interval Start")

    @support_date_range(frequency="31D")
    def get_load_forecast_15_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns 15-minute load forecast from the Real-Time Pre-Dispatch Market

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pl.DataFrame: DataFrame with load forecast data
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
            {"MW": "Load Forecast", "TAC_AREA_NAME": "TAC Area Name"},
        )

        df = df.with_columns(
            (pl.col("Interval Start") - pl.duration(minutes=22, seconds=30)).alias(
                "Publish Time",
            ),
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "TAC Area Name",
                "Load Forecast",
            ],
        ).sort("Interval Start")

    @support_date_range(frequency="31D")
    def get_load_forecast_day_ahead(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns hourly day-ahead load forecast

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return data.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pl.DataFrame: DataFrame with load forecast data
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
            {"MW": "Load Forecast", "TAC_AREA_NAME": "TAC Area Name"},
        )
        df = self._add_load_forecast_publish_time(df, day_offset=1)

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "TAC Area Name",
                "Load Forecast",
            ],
        ).sort("Interval Start")

    @support_date_range(frequency="31D")
    def get_load_forecast_two_day_ahead(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns hourly two-day-ahead load forecast

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return data.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pl.DataFrame: DataFrame with load forecast data
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
            {"MW": "Load Forecast", "TAC_AREA_NAME": "TAC Area Name"},
        )

        df = self._add_load_forecast_publish_time(df, day_offset=2)

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "TAC Area Name",
                "Load Forecast",
            ],
        ).sort("Interval Start")

    @support_date_range(frequency="31D")
    def get_load_forecast_seven_day_ahead(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns hourly seven-day-ahead load forecast

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return data.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pl.DataFrame: DataFrame with load forecast data
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
            {"MW": "Load Forecast", "TAC_AREA_NAME": "TAC Area Name"},
        )

        df = self._add_load_forecast_publish_time(df, day_offset=7)

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "TAC Area Name",
                "Load Forecast",
            ],
        ).sort("Interval Start")

    def _add_load_forecast_publish_time(
        self,
        df: pl.DataFrame,
        day_offset: int,
    ) -> pl.DataFrame:
        """Adds a publish time to the load forecast data

        Args:
            df (pl.DataFrame): load forecast data
            day_offset (int): number of days before the forecast date that it was published

        Returns:
            pl.DataFrame: load forecast data with publish time
        """
        df = df.with_columns(pl.col("Interval Start").dt.date().alias("date"))
        unique_dates = sorted(df["date"].unique().to_list())

        # All daily forecasts are published at the same time each day, 9:10 AM PT
        # http://oasis.caiso.com/mrioasis/logon.do > Atlas Reference > Publications > OASIS Publications Schedule
        publish_time_map = {
            forecast_date: (
                pd.Timestamp(forecast_date, tz=self.default_timezone)
                - pd.Timedelta(days=day_offset)
            ).replace(
                hour=9,
                minute=10,
            )
            for forecast_date in unique_dates
        }
        df = df.with_columns(
            pl.col("date").replace_strict(publish_time_map).alias("Publish Time"),
        )
        return df.drop("date")

    @support_date_range(frequency="31D")
    def get_load_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns actual load values

        Arguments:
            date (str | pd.Timestamp): day to return
            end (str | pd.Timestamp, optional): end of date range to return.
                If None, returns only date. Defaults to None.
            sleep (int): seconds to sleep before returning to avoid rate limit. Defaults to 4.
            verbose (bool): print verbose output. Defaults to False.

        Returns:
            pl.DataFrame: DataFrame with actual load data
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
            {"MW": "Load", "TAC_AREA_NAME": "TAC Area Name"},
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "TAC Area Name",
                "Load",
            ],
        ).sort("Interval Start")

    def get_renewables_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get wind and solar hourly actuals from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of wind and solar hourly actuals
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
    ) -> pl.DataFrame:
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
        data: pl.DataFrame,
        current_time: pd.Timestamp | None = None,
        publish_time_offset_from_day_start: pd.Timedelta | None = None,
    ) -> pl.DataFrame:
        df = data.select(
            [
                "Interval Start",
                "Interval End",
                "TRADING_HUB",
                "RENEWABLE_TYPE",
                "MW",
            ],
        )

        # Totals across all trading hubs for each renewable type at each interval
        totals = (
            df.group_by(["RENEWABLE_TYPE", "Interval Start", "Interval End"])
            .agg(pl.col("MW").sum())
            .with_columns(pl.lit("CAISO").alias("TRADING_HUB"))
            .select(df.columns)
        )

        df = pl.concat([df, totals])

        df = df.pivot(
            index=["Interval Start", "Interval End", "TRADING_HUB"],
            on="RENEWABLE_TYPE",
            values="MW",
        )

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
                    {
                        "TRADING_HUB": "Location",
                        "Solar": "Solar MW",
                        "Wind": "Wind MW",
                    },
                ),
                ["Interval Start", "Interval End", "Publish Time", "Location"],
            )

            return df.sort(["Interval Start", "Publish Time", "Location"])

        df = utils.move_cols_to_front(
            df.rename(
                {
                    "TRADING_HUB": "Location",
                },
            ),
            ["Interval Start", "Interval End", "Location"],
        )

        return df.sort(["Interval Start", "Location"])

    def _add_forecast_publish_time(
        self,
        data: pl.DataFrame,
        current_time: pd.Timestamp,
        publish_time_offset_from_day_start: pd.Timedelta | None = None,
    ) -> pl.DataFrame:
        """Labels forecasts with a publish time.

        The logic is:

        - If tomorrow or further in the future, the publish time is today's
          publish time (if current time is after it) or yesterday's publish time
          (if current time is before it).
        - If today or earlier, the publish time is the previous day's publish time.

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
        def _past_publish_time(interval_start: datetime | pd.Timestamp) -> pd.Timestamp:
            ts = pd.Timestamp(interval_start)
            return ts.floor("D").replace(
                hour=hour_offset,
                minute=minute_offset,
            ) - pd.Timedelta(days=1)

        return data.with_columns(
            pl.when(
                pl.col("Interval Start").dt.date()
                > future_forecasts_publish_time.date(),
            )
            .then(pl.lit(future_forecasts_publish_time))
            .otherwise(
                pl.col("Interval Start").map_elements(
                    _past_publish_time,
                    return_dtype=pl.Datetime(time_zone=self.default_timezone),
                ),
            )
            .alias("Publish Time"),
        )

    def _handle_renewables_forecast(
        self,
        df: pl.DataFrame,
        publish_time_offset: pd.Timedelta,
    ) -> pl.DataFrame:
        df = df.rename(
            {
                "TRADING_HUB": "Location",
                "RENEWABLE_TYPE": "Renewable Type",
            },
        )

        df = df.pivot(
            index=[
                "Interval Start",
                "Interval End",
                "Location",
            ],
            on="Renewable Type",
            values="MW",
            aggregate_function="first",
        )

        offset_secs = int(publish_time_offset.total_seconds())
        return df.with_columns(
            (pl.col("Interval Start") - pl.duration(seconds=offset_secs)).alias(
                "Publish Time",
            ),
        ).select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Location",
                "Solar",
                "Wind",
            ],
        )

    def get_renewables_forecast_hasp(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get solar and wind generation HASP hourly data from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of solar and wind generation HASP hourly data
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
    ) -> pl.DataFrame:
        """Get RTD renewable forecast from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of RTD renewable forecast
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
    ) -> pl.DataFrame:
        """Get RTPD renewable forecast from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of RTPD renewable forecast
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

    @support_date_range(frequency="DAY_START")
    def get_edam_wind_solar_forecast(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Day-ahead, hourly, BAA-level wind and solar forecasts for balancing
        areas participating in the extended day-ahead market (EDAM).

        Data at: http://oasis.caiso.com/mrioasis/logon.do at Energy >
        EDAM > Wind and Solar Forecast.

        Per the OASIS Publications Schedule, the report is published every
        30 minutes between 6:00 and 10:00 AM Pacific. The Publish Time on
        each row is the actual publish timestamp pulled from the
        ``MessageHeader.TimeDate`` of the corresponding XML report, not a
        derived offset.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: hourly EDAM wind and solar forecasts by BAA
        """
        rows = self._fetch_edam_wind_solar_forecast_xml(
            date=date,
            end=end,
            verbose=verbose,
        )
        df = pl.DataFrame(rows)
        df = df.with_columns(
            pl.col("Solar").cast(pl.Float64, strict=False),
            pl.col("Wind").cast(pl.Float64, strict=False),
            pl.col("Interval Start").dt.convert_time_zone(self.default_timezone),
            pl.col("Interval End").dt.convert_time_zone(self.default_timezone),
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "BAA",
                "Solar",
                "Wind",
            ],
        ).sort(["Interval Start", "BAA"])

    def _fetch_edam_wind_solar_forecast_xml(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> list[dict]:
        """Fetch the OASIS XML version of the EDAM Wind and Solar Forecast
        and return a list of row dicts that include the publish timestamp
        from each daily file's ``MessageHeader.TimeDate``.
        """
        start = utils._handle_date(date, self.default_timezone)
        if end is not None:
            end = utils._handle_date(end, self.default_timezone)
        start_str, end_str = _caiso_handle_start_end(start, end)

        url = (
            "http://oasis.caiso.com/oasisapi/GroupZip"
            f"?resultformat=5&version=1&groupid=EDAM_WND_SLR_FORECAST_GRP"
            f"&startdatetime={start_str}&enddatetime={end_str}"
        )
        logger.info(f"Fetching URL: {url}")

        # NOTE: OASIS rate-limits ~1 request per 5s; pace daily chunks to stay under it.
        # Matches the _get_oasis retry-on-429 pattern
        time.sleep(5)
        r = requests.get(url, verify=True)
        r.raise_for_status()

        rows: list[dict] = []
        ns = "{http://www.caiso.com/soa/EDAMWindAndSolarForecast_v1.xsd}"
        with ZipFile(io.BytesIO(r.content)) as z:
            for fname in z.namelist():
                with z.open(fname) as fh:
                    tree = ElementTree.parse(fh)
                root = tree.getroot()
                publish_time = pd.Timestamp(
                    root.find(f"./{ns}MessageHeader/{ns}TimeDate").text,
                )
                for record in root.iter(f"{ns}REPORT_DATA"):
                    fields = {child.tag.replace(ns, ""): child.text for child in record}
                    rows.append(
                        {
                            "Interval Start": pd.to_datetime(
                                fields["INTERVAL_START_GMT"],
                                utc=True,
                            ),
                            "Interval End": pd.to_datetime(
                                fields["INTERVAL_END_GMT"],
                                utc=True,
                            ),
                            "Publish Time": publish_time,
                            "BAA": fields["BAA_GRP_ID"],
                            "Solar": fields["SOLAR"],
                            "Wind": fields["WIND"],
                        },
                    )

        return rows

    def get_pnodes(self, verbose: bool = False) -> pl.DataFrame:
        start = utils._handle_date("today")

        df = self.get_oasis_dataset(
            dataset="pnode_map",
            start=start,
            end=start + pd.Timedelta(days=1),
            verbose=verbose,
        )

        df = df.rename(
            {
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
    def _get_lmp(
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
            pl.DataFrame: A DataFrame of pricing data
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

        if df.is_empty():
            raise NoDataFoundException(
                f"No data found for start date: {date} and end date: {end}",
            )

        df = df.pivot(
            index=["Time", "Interval Start", "Interval End", "NODE"],
            on="LMP_TYPE",
            values=PRICE_COL,
            aggregate_function="first",
        )

        df = df.rename(
            {
                "NODE": "Location",
                "LMP": "LMP",
                "MCE": "Energy",
                "MCC": "Congestion",
                "MCL": "Loss",
                "MGHG": "GHG",
            },
        )

        df = df.with_columns(
            pl.lit(market.value).alias("Market"),
            pl.lit("Node").alias("Location Type"),
        )

        # if -APND in location then "APND" Location Type

        df = df.with_columns(
            pl.when(pl.col("Location").str.ends_with("-APND"))
            .then(pl.lit("AP Node"))
            .when(pl.col("Location").is_in(self.trading_hub_locations))
            .then(pl.lit("Trading Hub"))
            .when(pl.col("Location").str.starts_with("DLAP_"))
            .then(pl.lit("DLAP"))
            .otherwise(pl.col("Location Type"))
            .alias("Location Type"),
        )

        return df.select(
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
            ],
        )

    @lmp_config(
        supports={
            Markets.DAY_AHEAD_HOURLY: ["latest", "today", "historical"],
            Markets.REAL_TIME_15_MIN: ["latest", "today", "historical"],
            Markets.REAL_TIME_5_MIN: ["latest", "today", "historical"],
        },
    )
    def get_lmp(
        self,
        date: str | pd.Timestamp,
        market: str,
        locations: list = None,
        sleep: int = 5,
        end: str | pd.Timestamp = None,
        verbose: bool = False,
    ):
        """Deprecated. Use the per-dataset methods instead:
        :meth:`get_lmp_real_time_5_min`, :meth:`get_lmp_real_time_15_min`,
        :meth:`get_lmp_day_ahead_hourly`.
        """
        warnings.warn(
            "CAISO.get_lmp is deprecated; use the per-dataset methods "
            "get_lmp_real_time_5_min, get_lmp_real_time_15_min, or "
            "get_lmp_day_ahead_hourly instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._get_lmp(
            date,
            market=market,
            locations=locations,
            sleep=sleep,
            end=end,
            verbose=verbose,
        )

    def get_lmp_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        sleep: int = 5,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get real-time 5-minute LMPs for all nodes."""
        return self._get_lmp(
            date,
            market=Markets.REAL_TIME_5_MIN,
            locations="ALL",
            sleep=sleep,
            end=end,
            verbose=verbose,
        )

    def get_lmp_real_time_15_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        sleep: int = 5,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get real-time 15-minute LMPs for all nodes."""
        return self._get_lmp(
            date,
            market=Markets.REAL_TIME_15_MIN,
            locations="ALL",
            sleep=sleep,
            end=end,
            verbose=verbose,
        )

    def get_lmp_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        sleep: int = 5,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get day-ahead hourly LMPs for all nodes."""
        return self._get_lmp(
            date,
            market=Markets.DAY_AHEAD_HOURLY,
            locations="ALL",
            sleep=sleep,
            end=end,
            verbose=verbose,
        )

    def get_price_corrections(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return CAISO price corrections (OASIS ``PRC_CORR_GRP`` summary).

        CAISO reprices an operating day when it detects an error in published
        prices and posts a structured summary of every correction to the
        ``PRC_CORR_GRP`` report group. Each row describes a single corrected
        interval: the trade date (operating day) and market that were
        corrected, the hour ending and interval, the correction method and
        reason, and the time the correction was generated.

        The report is keyed by trade date and serves one day per request, so a
        range is fetched one day at a time. A correction is typically generated
        several days to two weeks after the trade date, so ``Report Generated``
        is the column to use when selecting recently issued corrections.

        Arguments:
            date (datetime.date, str): start of the trade-date range.

            end (datetime.date, str): end of the trade-date range. If None,
                returns only ``date``. Defaults to None.

            sleep (int): seconds to sleep between requests to avoid the OASIS
                rate limit. Defaults to 4.

            verbose (bool, optional): print out url being fetched. Defaults to
                False.

        Returns:
            pl.DataFrame: one row per corrected interval with columns
            ``Trade Date`` (the corrected operating day), ``Hour Ending``,
            ``Interval``, ``Market`` (e.g. ``DAM``, ``RTD``, ``RTPD``),
            ``Affected Area``, ``Correction Method``, ``Correction Count``,
            ``Energy Type``, ``Correction Reason`` and ``Report Generated``
            (when the correction was issued). ``Trade Date`` and
            ``Report Generated`` are Pacific-localized timestamps.

        Raises:
            NoDataFoundException: if no corrections were issued for the range.
        """
        raw = self._get_price_corrections_raw(
            date=date,
            end=end,
            sleep=sleep,
            verbose=verbose,
        )

        result = self._parse_price_corrections(raw)

        if result.is_empty():
            raise NoDataFoundException(
                f"No CAISO price corrections for trade dates between {date} and {end}",
            )

        return result

    @support_date_range(frequency="DAY_START")
    def _get_price_corrections_raw(
        self,
        date: pd.Timestamp,
        end: pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Fetch the ``PRC_CORR_GRP`` summary for one trade date; the decorator
        iterates and concatenates a range. A "no data" response (the group is
        not generated for the date yet, or a transient rate limit) is retried,
        then returns an empty frame.
        """
        start, _ = _caiso_handle_start_end(date)
        url = (
            "http://oasis.caiso.com/oasisapi/GroupZip?resultformat=6"
            f"&groupid=PRC_CORR_GRP&version=1&startdatetime={start}"
        )
        if verbose:
            print(f"Fetching URL: {url}")
        logger.info(f"Fetching URL: {url}")

        for _ in range(4):
            response = requests.get(url, verify=True)
            if response.status_code == 200:
                archive = ZipFile(io.BytesIO(response.content))
                contents = archive.read(archive.namelist()[0])
                # A data response is a CSV; "no data" is an XML error document.
                if b"ERR_CODE" not in contents[:600]:
                    return pl.from_pandas(pd.read_csv(io.BytesIO(contents)))
            time.sleep(sleep)

        return pl.DataFrame()

    def _parse_price_corrections(self, df: pl.DataFrame) -> pl.DataFrame:
        """Rename and type the ``PRC_CORR_GRP`` columns, dropping the placeholder
        rows that mark market/method combinations with no correction for the day.
        """
        if df.is_empty():
            return df

        df = df.filter(
            pl.col("CORRECTION_REASON") != PRICE_CORRECTION_NO_RECORDS_REASON,
        )
        if df.is_empty():
            return df

        df = df.with_columns(
            pl.col("TRADE_DATE").str.to_datetime().alias("_trade_date_naive"),
            pl.col("REPORT_GENERATED")
            .str.to_datetime()
            .alias("_report_generated_naive"),
        )
        df = utils.localize_shift_forward_polars(
            df,
            "_trade_date_naive",
            self.default_timezone,
        )
        df = utils.localize_shift_forward_polars(
            df,
            "_report_generated_naive",
            self.default_timezone,
        )
        return df.select(
            pl.col("_trade_date_naive").alias("Trade Date"),
            pl.col("HOUR_ENDING").cast(pl.Int64).alias("Hour Ending"),
            pl.col("INTERVAL").cast(pl.Int64).alias("Interval"),
            pl.col("MARKET").alias("Market"),
            pl.col("AFFECTED_AREA").alias("Affected Area"),
            pl.col("CORRECTION_METHOD").alias("Correction Method"),
            pl.col("CORRECTION_COUNT").cast(pl.Int64).alias("Correction Count"),
            pl.col("ENERGY_TYPECODE").alias("Energy Type"),
            pl.col("CORRECTION_REASON").alias("Correction Reason"),
            pl.col("_report_generated_naive").alias("Report Generated"),
        ).sort(["Trade Date", "Market", "Hour Ending", "Interval"])

    @support_date_range(frequency="DAY_START")
    def get_storage(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
        df = df.with_columns(
            [pl.col(col).cast(pl.Int64) for col in rename.keys()],
        )

        df = df.rename(rename)
        return df.select(
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Supply",
                "Stand-alone Batteries",
                "Hybrid Batteries",
            ],
        )

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
            pl.DataFrame: A DataFrame of gas prices
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
            {
                "FUEL_REGION_ID": "Fuel Region Id",
                "PRC": "Price",
            },
        )
        return df.select(
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Fuel Region Id",
                "Price",
            ],
        ).sort(["Fuel Region Id", "Time"])

    def get_fuel_regions(self, verbose: bool = False) -> pl.DataFrame:
        """Retrieves the (mostly static) list of fuel regions with associated data.
        This file can be joined to the gas prices on Fuel Region Id"""
        url = (
            "https://www.caiso.com/documents/fuelregion_electricregiondefinitions.xlsx"  # noqa
        )

        logger.info(f"Fetching {url}")

        response = requests.get(url)
        response.raise_for_status()

        # Only want the "GPI_Fuel_Region" sheet
        return utils.read_excel_via_pandas(
            io.BytesIO(response.content),
            sheet_name="GPI_Fuel_Region",
        ).rename(
            {
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
            {
                "GHG_PRC_IDX": "GHG Allowance Price",
            },
        )

        return df.select(
            [
                "Time",
                "Interval Start",
                "Interval End",
                "GHG Allowance Price",
            ],
        )

    def get_raw_interconnection_queue(self, verbose: bool = False) -> io.BytesIO:
        url = "http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx"

        logger.info(f"Downloading interconnection queue from {url}")
        response = requests.get(url)
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose: bool = False) -> pl.DataFrame:
        raw_data = self.get_raw_interconnection_queue(verbose)

        sheets = utils.read_excel_via_pandas(
            raw_data,
            skiprows=3,
            sheet_name=None,
            dtype=str,
        )

        # remove legend at the bottom
        queued_projects = sheets["Grid GenerationQueue"].head(
            sheets["Grid GenerationQueue"].height - 8,
        )
        completed_projects = sheets["Completed Generation Projects"].head(
            sheets["Completed Generation Projects"].height - 2,
        )
        withdrawn_projects = (
            sheets["Withdrawn Generation Projects"]
            .head(
                sheets["Withdrawn Generation Projects"].height - 2,
            )
            .rename({"Project Name - Confidential": "Project Name"})
        )

        queue = pl.concat(
            [queued_projects, completed_projects, withdrawn_projects],
            how="diagonal_relaxed",
        )

        queue = queue.rename(
            {
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

        def _join_generation_types(row: dict) -> str:
            parts = [
                str(row[col])
                for col in type_columns
                if row.get(col) is not None and str(row[col]) != "nan"
            ]
            return " + ".join(parts)

        queue = queue.with_columns(
            pl.struct(type_columns)
            .map_elements(_join_generation_types, return_dtype=pl.Utf8)
            .alias("Generation Type"),
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
    ) -> pl.DataFrame:
        """Return curtailment data for a given date.

        Note:
            Data available from June 30, 2016 to May 31, 2025. For current data,
            please use ``get_curtailment``.

        Args:
            date: Date to return data.
            verbose: Print out url being fetched. Defaults to False.

        Returns:
            A DataFrame of curtailment data.
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

        tables: list[pl.DataFrame] = []
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
                        tables.append(
                            pl.from_pandas(pd.DataFrame(table[1:], columns=header)),
                        )
                    # Subsequent pages - use saved header
                    elif header is not None:
                        tables.append(
                            pl.from_pandas(pd.DataFrame(table, columns=header)),
                        )

        if len(tables) == 0:
            raise ValueError("No tables found")

        df = pl.concat(tables, how="diagonal")

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

        df = df.rename({k: v for k, v in rename.items() if k in df.columns})

        # convert from hour ending to hour beginning
        def _hour_to_time(hour: int) -> pd.Timestamp:
            return date + pd.Timedelta(hours=int(hour))

        df = df.with_columns(
            pl.col("Hour").cast(pl.Int64).sub(1).alias("Hour"),
        )
        df = df.with_columns(
            pl.col("Hour")
            .map_elements(
                _hour_to_time,
                return_dtype=pl.Datetime(time_zone=self.default_timezone),
            )
            .alias("Time"),
        )
        df = df.with_columns(
            pl.col("Time").alias("Interval Start"),
            (pl.col("Time") + pl.duration(hours=1)).alias("Interval End"),
        )
        df = df.drop("Date", "Hour")
        df = df.with_columns(
            pl.col("Fuel Type").replace({"SOLR": "Solar", "WIND": "Wind"}),
        )
        df = df.with_columns(
            [
                pl.when(pl.col(col) == "").then(None).otherwise(pl.col(col)).alias(col)
                for col in ["Curtailment (MWh)", "Curtailment (MW)"]
            ],
        )
        return df.select(
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Curtailment Type",
                "Curtailment Reason",
                "Fuel Type",
                "Curtailment (MWh)",
                "Curtailment (MW)",
            ],
        )

    def _pivot_aggregated_generation_outages(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        df = df.with_columns(
            pl.col("Fuel Category").replace({"Not Avail": "Not Available"}),
        )

        df = df.pivot(
            index=[
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Trading Hub",
            ],
            on="Fuel Category",
            values="MW",
            aggregate_function="first",
        )

        for column in [
            "Aggregated",
            "Hydro",
            "Not Available",
            "Renewable",
            "Thermal",
        ]:
            if column not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(column))

        component_sum = pl.sum_horizontal(
            "Hydro",
            "Not Available",
            "Renewable",
            "Thermal",
        )
        df = df.with_columns(
            pl.coalesce("Aggregated", component_sum).alias("Aggregated"),
        )

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Trading Hub",
                "Aggregated",
                "Hydro",
                "Not Available",
                "Renewable",
                "Thermal",
            ],
        )

    @support_date_range(frequency="DAY_START")
    def get_aggregated_generation_outages(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return hourly aggregated generator outages by trading hub.

        Outage MW is reported with an ``Aggregated`` hub-total column plus
        fuel-category breakdown columns where CAISO provides them. Some hubs
        (e.g. ZP26) publish only the aggregate; others publish Thermal,
        Renewable, Hydro, and sometimes Not Available. ``Aggregated`` uses the
        published value when present, otherwise the sum of the breakdown
        columns.

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            sleep (int, optional): seconds to sleep between requests.
                Defaults to 4.

            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame with one row per
            (Interval Start, Publish Time, Trading Hub), with outage MW by
            fuel category in separate columns.
        """
        df = self.get_oasis_dataset(
            dataset="aggregated_generation_outages",
            date=date,
            end=end,
            raw_data=False,
            sleep=sleep,
            verbose=verbose,
        )

        if df.is_empty():
            return df

        df = df.with_columns(
            pl.col("REPORT_DATE_GMT")
            .dt.convert_time_zone(self.default_timezone)
            .alias("Publish Time"),
        )

        df = df.rename(
            {
                "FUEL_CATEGORY": "Fuel Category",
                "TRADING_HUB": "Trading Hub",
            },
        )

        df = df.with_columns(pl.col("MW").cast(pl.Float64, strict=False))

        return self._pivot_aggregated_generation_outages(df).sort(
            ["Interval Start", "Trading Hub"],
        )

    @support_date_range(frequency="DAY_START")
    def get_as_prices(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        market: str = "DAM",
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return AS prices for a given date for each region

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            market (str): DAM or HASP. Defaults to DAM.

            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of AS prices
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

        if df.is_empty():
            return df

        df = df.rename(
            {
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
        df = df.with_columns(pl.col("ANC_TYPE").replace(as_type_map))

        df = df.pivot(
            index=[
                "Time",
                "Interval Start",
                "Interval End",
                "Region",
                "Market",
            ],
            on="ANC_TYPE",
            values="MW",
        )

        value_cols = sorted(
            c
            for c in df.columns
            if c not in {"Time", "Interval Start", "Interval End", "Region", "Market"}
        )
        return df.with_columns(
            [pl.col(c).fill_null(0) for c in value_cols],
        ).select(
            ["Time", "Interval Start", "Interval End", "Region", "Market", *value_cols],
        )

    @support_date_range(frequency="DAY_START")
    def get_ir_rc_prices(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return day-ahead nodal Imbalance Reserve and Reliability Capacity prices.

        The Marginal Clearing Price for Reliability Capacity (RCU/RCD) is comprised
        of Capacity, Congestion, and Loss components, while Imbalance Reserves
        (IRU/IRD) only have Capacity and Congestion components (Loss is NaN).

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of IR/RC prices with one row per
            (Interval Start, Location, Product). Earliest available date is
            May 1, 2026.
        """
        df = self.get_oasis_dataset(
            dataset="ir_rc_prices_day_ahead_hourly",
            start=date,
            end=end,
            sleep=sleep,
            verbose=verbose,
            raw_data=False,
        )

        columns = [
            "Interval Start",
            "Interval End",
            "Location",
            "Product",
            "MCP",
            "Capacity",
            "Congestion",
            "Loss",
        ]

        df = df.rename(
            {
                "NODE": "Location",
                "PRODUCT": "Product",
                "MARGINAL_CLEARING_PRICE": "MCP",
                "CAPACITY_PRICE": "Capacity",
                "CONGESTION_PRICE": "Congestion",
                "LOSS_PRICE": "Loss",
            },
        )

        return df.select(columns).sort(["Interval Start", "Location", "Product"])

    def _parse_ir_rc_requirements_awards(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.rename(
            {
                "BAA_GRP_ID": "BAA",
                "PRODUCT_TYPE": "Product",
            },
        )

        df = df.unpivot(
            index=[
                "Interval Start",
                "Interval End",
                "BAA",
                "Product",
            ],
            on=["REQ_MW", "AGG_AWD_MW"],
            variable_name="Type",
            value_name="MW",
        )

        df = df.with_columns(
            pl.col("Type").replace(
                {
                    "REQ_MW": "Requirement",
                    "AGG_AWD_MW": "Award",
                },
            ),
            pl.col("MW").cast(pl.Float64, strict=False),
        )
        df = df.filter(pl.col("MW").is_not_null())

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "BAA",
                "Product",
                "Type",
                "MW",
            ],
        ).sort(["Interval Start", "BAA", "Product", "Type"])

    @support_date_range(frequency="DAY_START")
    def get_ir_rc_requirements_awards_dam(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return day-ahead hourly Imbalance Reserve requirements and
        Imbalance Reserve and Reliability Capacity awards by BAA.

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame with one row per
            (Interval Start, BAA, Product, Type). Earliest available date is
            May 1, 2026.
        """
        df = self.get_oasis_dataset(
            dataset="ir_rc_requirements_awards",
            start=date,
            end=end,
            params={"groupid": "DAM_CAP_REQ_AWRD_GRP"},
            sleep=sleep,
            verbose=verbose,
            raw_data=False,
        )

        return self._parse_ir_rc_requirements_awards(df)

    @support_date_range(frequency="DAY_START")
    def get_ir_rc_requirements_awards_2da(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return two-day-ahead hourly Imbalance Reserve requirements by BAA.

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame with one row per
            (Interval Start, BAA, Product, Type). Earliest available date is
            May 1, 2026.
        """
        df = self.get_oasis_dataset(
            dataset="ir_rc_requirements_awards",
            start=date,
            end=end,
            params={"groupid": "2DA_CAP_REQ_AWRD_GRP"},
            sleep=sleep,
            verbose=verbose,
            raw_data=False,
        )

        return self._parse_ir_rc_requirements_awards(df)

    @support_date_range(frequency="DAY_START")
    def get_ir_rc_requirements_awards_3da(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return three-day-ahead hourly Imbalance Reserve requirements by BAA.

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame with one row per
            (Interval Start, BAA, Product, Type). Earliest available date is
            May 1, 2026.
        """
        df = self.get_oasis_dataset(
            dataset="ir_rc_requirements_awards",
            start=date,
            end=end,
            params={"groupid": "3DA_CAP_REQ_AWRD_GRP"},
            sleep=sleep,
            verbose=verbose,
            raw_data=False,
        )

        return self._parse_ir_rc_requirements_awards(df)

    @support_date_range(frequency="DAY_START")
    def get_curtailed_non_operational_generator_report(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return curtailed non-operational generator report for a given date.
           Earliest available date is June 17, 2021.

        Arguments:
            date (str, pd.Timestamp): date to return data
            end (str, pd.Timestamp, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of curtailed non-operational generator report

        Notes:
            Column glossary:
            http://www.caiso.com/market/Pages/OutageManagement/Curtailed-OperationalGeneratorReportGlossary.aspx

            If requesting multiple days, you may want to run the following
            to remove outages that get reported across multiple days::

                df.drop_duplicates(
                    subset=["OUTAGE MRID", "CURTAILMENT START DATE TIME"],
                    keep="last",
                )
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
            dtype=str,
        )
        first_col = test_parse.columns[0]
        outage_mrid_index = (
            test_parse.index[test_parse[first_col] == "OUTAGE MRID"].tolist()[0] + 1
        )

        content_io.seek(0)

        # load again, but skip rows up to outage mrid
        pdf = pd.read_excel(
            content_io,
            usecols="B:M",
            skiprows=outage_mrid_index,
            sheet_name="PREV_DAY_OUTAGES",
            engine="openpyxl",
        )
        pdf = pdf.dropna(axis=1, how="all")
        df = pl.from_pandas(pdf)

        # drop columns where the name is nan
        # artifact of the excel file
        non_null_cols = [
            c for c in df.columns if not (isinstance(c, float) and np.isnan(c))
        ]
        df = df.select(non_null_cols)

        # published day after
        publish_time = date.normalize() + pd.DateOffset(days=1)
        df = df.with_columns(pl.lit(publish_time).alias("Publish Time"))
        df = utils.move_cols_to_front(df, ["Publish Time"])

        df = df.with_columns(
            pl.col("CURTAILMENT START DATE TIME").alias("_curtailment_start_naive"),
            pl.col("CURTAILMENT END DATE TIME").alias("_curtailment_end_naive"),
        )
        df = utils.localize_ambiguous_infer_polars(
            df,
            "_curtailment_start_naive",
            self.default_timezone,
        )
        df = utils.localize_ambiguous_infer_polars(
            df,
            "_curtailment_end_naive",
            self.default_timezone,
        )

        # only some dates have this
        if "OUTAGE STATUS" in df.columns:
            df = df.drop("OUTAGE STATUS")

        df = df.drop(["CURTAILMENT START DATE TIME", "CURTAILMENT END DATE TIME"])
        df = df.rename(
            {
                "OUTAGE MRID": "Outage MRID",
                "RESOURCE NAME": "Resource Name",
                "RESOURCE ID": "Resource ID",
                "OUTAGE TYPE": "Outage Type",
                "NATURE OF WORK": "Nature of Work",
                "_curtailment_start_naive": "Curtailment Start Time",
                "_curtailment_end_naive": "Curtailment End Time",
                "CURTAILMENT MW": "Curtailment MW",
                "RESOURCE PMAX MW": "Resource PMAX MW",
                "NET QUALIFYING CAPACITY MW": "Net Qualifying Capacity MW",
            },
        )

        # if there are duplicates, set trce
        if df.select(
            pl.struct(["Outage MRID", "Curtailment Start Time"]).is_duplicated().any(),
        ).item():
            # drop where start and end are the same and end time isnt null
            # this appears to fix
            df = df.filter(
                ~(
                    (pl.col("Curtailment Start Time") == pl.col("Curtailment End Time"))
                    & pl.col("Curtailment End Time").is_not_null()
                ),
            )

            assert not df.select(
                pl.struct(["Outage MRID", "Curtailment Start Time"])
                .is_duplicated()
                .any(),
            ).item(), "There are still duplicates"

        return df.select(
            [
                "Publish Time",
                "Outage MRID",
                "Resource Name",
                "Resource ID",
                "Outage Type",
                "Nature of Work",
                "Curtailment Start Time",
                "Curtailment End Time",
                "Curtailment MW",
                "Resource PMAX MW",
                "Net Qualifying Capacity MW",
            ],
        )

    @support_date_range(frequency="DAY_START")
    def get_tie_flows_real_time(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return real time tie flow data.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of real time tie flow data
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
    ) -> pl.DataFrame:
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

    def _process_tie_flows_data(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.drop(
            "Time",
            "DATA_ITEM",
            "OPR_DT",
            "OPR_HR",
            "OPR_INTERVAL",
            "OASIS_REC_STAT",
            "UPD_DATE",
            "UPD_BY",
            "GROUP",
            "BAA_GRP_ID",
        ).rename({"MARKET_TYPE": "MARKET", "VALUE": "MW"})

        # Multiply imports by -1 to match convention of imports being negative
        df = df.with_columns(
            pl.when(pl.col("DIRECTION") == "I")
            .then(pl.col("MW") * -1)
            .otherwise(pl.col("MW"))
            .alias("MW"),
        )

        # Sum MW by Interval Start, TIE_NAME, FROM_BAA, TO_BAA so we can remove
        # the direction column
        df = df.group_by(
            [
                "Interval Start",
                "Interval End",
                "TIE_NAME",
                "FROM_BAA",
                "TO_BAA",
                "MARKET",
            ],
        ).agg(pl.col("MW").sum())

        rename_map = {
            "Interval Start": "Interval Start",
            "Interval End": "Interval End",
            "TIE_NAME": "Tie Name",
            "FROM_BAA": "From BAA",
            "TO_BAA": "To BAA",
            "MARKET": "Market",
            "MW": "MW",
        }
        df = df.rename(rename_map)

        # Create an identifier column (separated by hyphens because some of the tie
        # names have underscores in them) to use for indexing
        df = df.with_columns(
            (
                pl.col("Tie Name")
                + pl.lit("-")
                + pl.col("From BAA")
                + pl.lit("-")
                + pl.col("To BAA")
            ).alias("Interface ID"),
        )

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

        return df.sort(["Interval Start", "Interface ID"])

    @support_date_range(frequency="31D")
    def get_as_procurement(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        market: str = "DAM",
        sleep: int = 4,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get ancillary services procurement data from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            market (str, optional): DAM or RTM. Defaults to "DAM".
            sleep (int, optional): number of seconds to sleep between requests. Defaults to 4.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of ancillary services data
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

        if df.is_empty():
            return df

        df = df.rename(
            {
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
        result_type_map = {
            "AS_BUY_MW": "Procured (MW)",
            "AS_SELF_MW": "Self-Provided (MW)",
            "AS_MW": "Total (MW)",
            "AS_COST": "Total Cost",
        }
        df = df.with_columns(
            pl.col("ANC_TYPE").replace(as_type_map).alias("ANC_TYPE"),
            pl.col("RESULT_TYPE").replace(result_type_map).alias("RESULT_TYPE"),
        )
        df = df.with_columns(
            (pl.col("ANC_TYPE") + pl.lit(" ") + pl.col("RESULT_TYPE")).alias("column"),
        )

        df = df.pivot(
            index=[
                "Time",
                "Interval Start",
                "Interval End",
                "Region",
                "Market",
            ],
            on="column",
            values="MW",
        )

        index_cols = ["Time", "Interval Start", "Interval End", "Region", "Market"]
        value_cols = sorted(c for c in df.columns if c not in index_cols)
        return df.select([*index_cols, *value_cols])

    def get_lmp_scheduling_point_tie_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get LMP scheduling point tie combination 5-min data from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of LMP scheduling point tie combination 5-min data
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
        return self._handle_lmp_scheduling_point_tie_combination(
            df,
            "Real Time 5 Min",
            date,
            end,
        )

    def get_lmp_scheduling_point_tie_real_time_15_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
        return self._handle_lmp_scheduling_point_tie_combination(
            df,
            "Real Time 15 Min",
            date,
            end,
        )

    def get_lmp_scheduling_point_tie_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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

        return self._handle_lmp_scheduling_point_tie_combination(
            df,
            "Day Ahead Hourly",
            date,
            end,
        )

    def _handle_lmp_scheduling_point_tie_combination(
        self,
        df: pl.DataFrame,
        dataset_name: Literal[
            "Day Ahead Hourly",
            "Real Time 15 Min",
            "Real Time 5 Min",
        ],
        date: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pl.DataFrame:
        if df.is_empty():
            raise NoDataFoundException(
                f"No data found for LMP Scheduling Point Tie Combination {dataset_name} for start date: {date} and end date: {end}",
            )
        df = df.rename(
            {
                "NODE": "Node",
                "TIE": "Tie",
                "MARKET_RUN_ID": "Market",
            },
        )

        df = df.pivot(
            index=[
                "Interval Start",
                "Interval End",
                "Node",
                "Tie",
                "Market",
            ],
            on="LMP_TYPE",
            values="PRC",
            aggregate_function="first",
        )

        df = df.rename(
            {
                "MCE": "Energy",
                "MCC": "Congestion",
                "MCL": "Loss",
                "MGHG": "GHG",
            },
        )

        return df.with_columns(
            (pl.col("Node") + pl.lit(" ") + pl.col("Tie")).alias("Location"),
        ).select(
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
            ],
        )

    def get_lmp_hasp_15_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get LMP HASP 15-min data from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of LMP HASP 15-min data
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
        if df.is_empty():
            raise NoDataFoundException(
                f"No data found for LMP HASP 15-min for start date: {date} and end date: {end}",
            )

        return self._handle_lmp_hasp_15_min(df)

    def _handle_lmp_hasp_15_min(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.rename({"NODE": "Location"})
        df = df.pivot(
            index=[
                "Interval Start",
                "Interval End",
                "Location",
            ],
            on="LMP_TYPE",
            values="MW",
            aggregate_function="first",
        )
        return df.rename(
            {
                "MCE": "Energy",
                "MCC": "Congestion",
                "MCL": "Loss",
                "MGHG": "GHG",
            },
        ).select(
            [
                "Interval Start",
                "Interval End",
                "Location",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
                "GHG",
            ],
        )

    @support_date_range(frequency="31D")
    def get_nomogram_branch_shadow_prices_day_ahead_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns hourly day-ahead nomogram/branch shadow price forecast.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched.

        Returns:
            pl.DataFrame: A DataFrame with the shadow price forecast
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
            {
                "NOMOGRAM_ID": "Location",
                "PRC": "Price",
                "NOMOGRAM_ID_XML": "Nomogram ID XML",
                "CONSTRAINT_CAUSE": "Constraint Cause",
                "MARKET_RUN_ID": "Market Run ID",
                "GROUP": "Group",
            },
        )

        group_cols = [
            "Interval Start",
            "Interval End",
            "Location",
            "Nomogram ID XML",
            "Market Run ID",
            "Constraint Cause",
            "Price",
        ]
        df = _collapse_group_to_array(df, group_cols)

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Nomogram ID XML",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ],
        )

    def get_nomogram_branch_shadow_prices_hasp_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns nomogram/branch shadow price HASP hourly data from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched.

        Returns:
            pl.DataFrame: A DataFrame with the shadow price HASP data
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
            {
                "NOMOGRAM_ID": "Location",
                "PRC": "Price",
                "NOMOGRAM_ID_XML": "Nomogram ID XML",
                "CONSTRAINT_CAUSE": "Constraint Cause",
                "MARKET_RUN_ID": "Market Run ID",
                "GROUP": "Group",
            },
        )

        group_cols = [
            "Interval Start",
            "Interval End",
            "Location",
            "Nomogram ID XML",
            "Market Run ID",
            "Constraint Cause",
            "Price",
        ]
        df = _collapse_group_to_array(df, group_cols)

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Nomogram ID XML",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ],
        )

    def get_nomogram_branch_shadow_price_forecast_15_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns 15-minute nomogram/branch shadow price forecast from the Real-Time Pre-Dispatch Market.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched.

        Returns:
            pl.DataFrame: A DataFrame with the shadow price forecast
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
            {
                "NOMOGRAM_ID": "Location",
                "PRC": "Price",
                "NOMOGRAM_ID_XML": "Nomogram ID XML",
                "CONSTRAINT_CAUSE": "Constraint Cause",
                "MARKET_RUN_ID": "Market Run ID",
                "GROUP": "Group",
            },
        )

        group_cols = [
            "Interval Start",
            "Interval End",
            "Location",
            "Nomogram ID XML",
            "Market Run ID",
            "Constraint Cause",
            "Price",
        ]
        df = _collapse_group_to_array(df, group_cols)

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Nomogram ID XML",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ],
        )

    def get_interval_nomogram_branch_shadow_prices_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get 5-min nomogram/branch shadow prices from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched.

        Returns:
            pl.DataFrame: A DataFrame with the shadow prices
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
            {
                "NOMOGRAM_ID": "Location",
                "PRC": "Price",
                "MARKET_RUN_ID": "Market Run ID",
                "CONSTRAINT_CAUSE": "Constraint Cause",
                "GROUP": "Group",
            },
        )

        group_cols = [
            "Interval Start",
            "Interval End",
            "Location",
            "Market Run ID",
            "Constraint Cause",
            "Price",
        ]
        df = _collapse_group_to_array(df, group_cols)

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "Location",
                "Market Run ID",
                "Constraint Cause",
                "Price",
                "Groups",
            ],
        )

    def get_intertie_constraint_shadow_prices_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get 5-min intertie constraint shadow prices from CAISO.

        Args:
            date (str | pd.Timestamp): date to return data
            end (str | pd.Timestamp | None, optional): last date of range to return data.
                If None, returns only date. Defaults to None.
            verbose (bool, optional): print out url being fetched.

        Returns:
            pl.DataFrame: A DataFrame with the intertie constraint shadow prices
        """
        if date == "latest":
            return self.get_intertie_constraint_shadow_prices_real_time_5_min(
                pd.Timestamp.now(tz=self.default_timezone),
            )

        df = self.get_oasis_dataset(
            dataset="interval_intertie_constraint_shadow_prices",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )

        df = df.rename(
            {
                "TI_ID": "TI ID",
                "TI_DIRECTION": "TI Direction",
                "MARKET_RUN_ID": "Market Run ID",
                "CONSTRAINT_CAUSE": "Constraint Cause",
                "PRC": "Shadow Price",
                "GROUP": "Group",
            },
        )

        group_cols = [
            "Interval Start",
            "Interval End",
            "TI ID",
            "TI Direction",
            "Market Run ID",
            "Constraint Cause",
            "Shadow Price",
        ]
        df = _collapse_group_to_array(df, group_cols)

        return df.select(
            [
                "Interval Start",
                "Interval End",
                "TI ID",
                "TI Direction",
                "Market Run ID",
                "Constraint Cause",
                "Shadow Price",
                "Groups",
            ],
        )

    @support_date_range(frequency="DAY_START")
    def get_curtailment(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Return curtailment data for a given date

        Arguments:
            date (datetime.date, str): date to return data

            end (datetime.date, str): last date of range to return data.
                If None, returns only date. Defaults to None.

            verbose: print out url being fetched. Defaults to False.

        Returns:
            pl.DataFrame: A DataFrame of curtailment data
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

        melted_dfs: dict[str, pl.DataFrame] = {}
        for df_key, fuel_type, unit in fuel_configs:
            # Melt from a wide format to a long format
            value_cols = [
                c
                for c in dataframes[df_key].columns
                if c not in {"Interval Start", "Interval End"}
            ]
            df = dataframes[df_key].unpivot(
                index=["Interval Start", "Interval End"],
                on=value_cols,
                variable_name="Curtailment Type",
                value_name=f"Curtailment {unit}",
            )

            # Curtailment Type format is "Curtailment Type Curtailment Reason MW/MWH"
            # Split it into two columns: Curtailment Type and Curtailment Reason
            df = df.with_columns(
                pl.col("Curtailment Type").str.split(" ").alias("_parts"),
            )
            df = df.with_columns(
                pl.col("_parts").list.get(0).alias("Curtailment Type"),
                pl.col("_parts").list.get(1).alias("Curtailment Reason"),
                pl.lit(fuel_type).alias("Fuel Type"),
            ).drop("_parts")

            melted_dfs[f"{fuel_type} {unit}"] = df

        # Merge MWH and MW data for each fuel type
        merge_cols = [
            "Interval Start",
            "Interval End",
            "Curtailment Type",
            "Curtailment Reason",
            "Fuel Type",
        ]

        solar_df = melted_dfs["Solar MWH"].join(
            melted_dfs["Solar MW"],
            on=merge_cols,
            how="full",
            coalesce=True,
        )
        wind_df = melted_dfs["Wind MWH"].join(
            melted_dfs["Wind MW"],
            on=merge_cols,
            how="full",
            coalesce=True,
        )

        return (
            pl.concat([solar_df, wind_df], how="diagonal")
            .select(
                merge_cols + ["Curtailment MWH", "Curtailment MW"],
            )
            .sort(merge_cols)
        )

    def get_caiso_renewables_report(
        self,
        date: pd.Timestamp,
    ) -> dict[str, pl.DataFrame]:
        """
        Fetches the CAISO daily renewable report for a given date and extracts data from
        all the charts into wide dataframes.
        """
        slug = date.strftime("%b-%d-%Y").lower()
        primary_url = (
            f"https://www.caiso.com/documents/daily-renewable-report-{slug}.html"
        )
        response = requests.get(primary_url)
        if response.status_code != 200:
            corrected_url = f"https://www.caiso.com/documents/daily-renewable-report-{slug}-corrected.html"
            corrected_response = requests.get(corrected_url)
            if corrected_response.status_code == 200:
                response = corrected_response
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

        dataframes = {}
        for df_name, timestamps, duration, unit, column_mapping in dataframe_configs:
            if unit == "minute":
                interval_end_timedelta: pd.Timedelta | pd.DateOffset = pd.Timedelta(
                    minutes=duration,
                )
            elif unit == "hour":
                interval_end_timedelta = pd.Timedelta(hours=duration)
            elif unit == "day":
                interval_end_timedelta = pd.Timedelta(days=duration)
            else:
                interval_end_timedelta = pd.DateOffset(**{f"{unit}s": duration})

            target_length = len(timestamps)
            data = {
                "Interval Start": timestamps,
                "Interval End": timestamps + interval_end_timedelta,
            }

            for col_name, var_name in column_mapping.items():
                values = extract_array(html_content, var_name)
                if len(values) > target_length:
                    values = values[-target_length:]
                elif len(values) < target_length:
                    raise ValueError(
                        f"Renewables report column {var_name} returned {len(values)} values for {date.strftime('%Y-%m-%d')}, expected {target_length}",
                    )
                data[col_name] = values

            dataframes[df_name] = pl.DataFrame(data)

        return dataframes

    def _load_daily_energy_storage_report(
        self,
        date: str | pd.Timestamp,
        verbose: bool = False,
    ) -> tuple[str, pd.Timestamp]:
        if date == "latest":
            raise NotSupported()
        return daily_energy_storage.load_daily_energy_storage_report(
            date=date,
            tz=self.default_timezone,
            verbose=verbose,
        )

    @support_date_range(frequency="DAY_START")
    def get_storage_awards_fmm(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Energy and ancillary services awards for storage in the FMM (15-minute)."""
        html, rs = self._load_daily_energy_storage_report(date=date, verbose=verbose)
        return daily_energy_storage.build_storage_awards_fmm(html, rs)

    @support_date_range(frequency="DAY_START")
    def get_storage_awards_ifm(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Energy and AS awards for storage in the IFM (energy at 5-minute, AS hourly)."""
        html, rs = self._load_daily_energy_storage_report(date=date, verbose=verbose)
        return daily_energy_storage.build_storage_awards_ifm(html, rs)

    @support_date_range(frequency="DAY_START")
    def get_storage_awards_rtd(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Energy awards for storage in RTD (5-minute)."""
        html, rs = self._load_daily_energy_storage_report(date=date, verbose=verbose)
        return daily_energy_storage.build_storage_awards_rtd(html, rs)

    @support_date_range(frequency="DAY_START")
    def get_storage_energy_awards_ruc(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """RUC energy awards to storage (5-minute)."""
        html, rs = self._load_daily_energy_storage_report(date=date, verbose=verbose)
        return daily_energy_storage.build_storage_energy_awards_ruc(html, rs)

    @support_date_range(frequency="DAY_START")
    def get_storage_energy_bids_fmm(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """FMM energy bid-in capacity by price bin (15-minute)."""
        html, rs = self._load_daily_energy_storage_report(date=date, verbose=verbose)
        return daily_energy_storage.build_storage_energy_bids_fmm(html, rs)

    @support_date_range(frequency="DAY_START")
    def get_storage_energy_bids_ifm(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """IFM energy bid-in capacity by price bin (hourly)."""
        html, rs = self._load_daily_energy_storage_report(date=date, verbose=verbose)
        return daily_energy_storage.build_storage_energy_bids_ifm(html, rs)

    @support_date_range(frequency="DAY_START")
    def get_storage_soc_fmm(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """State of charge for storage in the FMM (15-minute, standalone resources)."""
        html, rs = self._load_daily_energy_storage_report(date=date, verbose=verbose)
        return daily_energy_storage.build_storage_soc_fmm(html, rs)

    @support_date_range(frequency="DAY_START")
    def get_storage_soc_hourly(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Hourly IFM and RUC state of charge (see ``build_storage_soc_hourly``)."""
        html, rs = self._load_daily_energy_storage_report(date=date, verbose=verbose)
        return daily_energy_storage.build_storage_soc_hourly(html, rs)

    @support_date_range(frequency="DAY_START")
    def get_storage_soc_rtd(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """State of charge for storage in RTD (5-minute, standalone resources)."""
        html, rs = self._load_daily_energy_storage_report(date=date, verbose=verbose)
        return daily_energy_storage.build_storage_soc_rtd(html, rs)

    def get_system_load_and_resource_schedules_day_ahead(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get CAISO System Load and Resource Schedules Day-Ahead data from CAISO."""
        if date == "latest":
            # DAM data should be available 1 day in the future after 13:00 PT
            try:
                return self.get_system_load_and_resource_schedules_day_ahead(
                    self.local_now().normalize() + pd.DateOffset(days=1),
                )
            except KeyError:
                # Fallback to today
                return self.get_system_load_and_resource_schedules_day_ahead(
                    self.local_now().normalize(),
                )

        return self._get_system_load_and_resource_schedules_for_market(
            date,
            end,
            verbose,
            market="day_ahead",
        )

    def get_system_load_and_resource_schedules_hasp(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get CAISO System Load and Resource Schedules HASP data from CAISO."""
        if date == "latest":
            return self.get_system_load_and_resource_schedules_hasp(
                "today",
            )

        return self._get_system_load_and_resource_schedules_for_market(
            date,
            end,
            verbose,
            market="hasp",
        )

    def get_system_load_and_resource_schedules_real_time_5_min(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get CAISO System Load and Resource Schedules Real Time data from CAISO."""
        if date == "latest":
            return self.get_system_load_and_resource_schedules_real_time_5_min(
                "today",
            )

        return self._get_system_load_and_resource_schedules_for_market(
            date,
            end,
            verbose,
            market="real_time_5_min",
        )

    def get_system_load_and_resource_schedules_ruc(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get CAISO System Load and Resource Schedules RUC data from CAISO."""
        if date == "latest":
            return self.get_system_load_and_resource_schedules_ruc(
                "today",
            )

        return self._get_system_load_and_resource_schedules_for_market(
            date,
            end,
            verbose,
            market="ruc",
        )

    def _get_system_load_and_resource_schedules_for_market(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
        market: Literal["day_ahead", "hasp", "real_time_5_min", "ruc"] = "day_ahead",
    ):
        df = self.get_oasis_dataset(
            dataset=f"system_load_and_resource_schedules_{market}",
            date=date,
            end=end,
            verbose=verbose,
            raw_data=False,
        )

        if df.is_empty():
            return df

        df = df.pivot(
            index=["Interval Start", "Interval End", "TAC_ZONE_NAME"],
            on="SCHEDULE",
            values="MW",
        )

        df = df.rename({"TAC_ZONE_NAME": "TAC Name"})

        schedule_cols = sorted(
            c
            for c in df.columns
            if c not in {"Interval Start", "Interval End", "TAC Name"}
        )
        return df.select(
            ["Interval Start", "Interval End", "TAC Name", *schedule_cols],
        ).sort(
            ["Interval Start", "TAC Name"],
        )

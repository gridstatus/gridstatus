import re
import urllib
from enum import StrEnum
from typing import BinaryIO, Callable

import pandas as pd
import polars as pl
import pytz
import requests
import tqdm

from gridstatus import utils
from gridstatus.base import (
    InterconnectionQueueStatus,
    ISOBase,
    Markets,
    NoDataFoundException,
    NotSupported,
)
from gridstatus.decorators import FiveMinOffset, support_date_range
from gridstatus.gs_logging import logger

# Endpoints
RTBM_LMP_BY_BUS = "rtbm-lmp-by-bus"
FS_RTBM_LMP_BY_LOCATION = "rtbm-lmp-by-location"
FS_DAM_LMP_BY_LOCATION = "da-lmp-by-settlement-location"
FS_DAM_LMP_BY_BUS = "da-lmp-by-bus"
LMP_BY_SETTLEMENT_LOCATION_WEIS = "lmp-by-settlement-location-weis"
OPERATING_RESERVES = "operating-reserves"
RTBM_MCP = "rtbm-mcp"
DA_BINDING_CONSTRAINTS = "da-binding-constraints"
RTBM_BINDING_CONSTRAINTS = "rtbm-binding-constraints"

HOURLY_LOAD_WIDE_FORMAT_END_DATE = pd.Timestamp("2026-03-24", tz="US/Central")
# NOTE: Typically SWPW is ~2000-3000MW and SPP is ~20000-30000MW, so we can tell if there
# is a load value with null BAA value, we can tell which BAA it is.
BAA_LOAD_THRESHOLD_MW = 5000

MARKETPLACE_BASE_URL = "https://portal.spp.org"
FILE_BROWSER_API_URL = "https://portal.spp.org/file-browser-api/"
FILE_BROWSER_DOWNLOAD_URL = "https://portal.spp.org/file-browser-api/download"

BASE_SOLAR_AND_WIND_SHORT_TERM_URL = (
    f"{FILE_BROWSER_DOWNLOAD_URL}/shortterm-resource-forecast?path="
)
BASE_SOLAR_AND_WIND_MID_TERM_URL = (
    f"{FILE_BROWSER_DOWNLOAD_URL}/midterm-resource-forecast?path="
)

BASE_RESERVE_ZONE_FORECAST_URL = (
    f"{FILE_BROWSER_DOWNLOAD_URL}/resource-forecast-by-reserve-zone?path="
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

LMP_HUBS_AND_INTERFACES = {
    "AECI": "Interface",
    "ALTW": "Interface",
    "AMRN": "Interface",
    "BLKW": "Interface",
    "CLEC": "Interface",
    "DPC": "Interface",
    "EDDY": "Interface",
    "EES": "Interface",
    "ERCOTE": "Interface",
    "ERCOTN": "Interface",
    "GRE": "Interface",
    "LAM345": "Interface",
    "MCWEST": "Interface",
    "MDU": "Interface",
    "MEC": "Interface",
    "MISO": "Interface",
    "NSP": "Interface",
    "OTP": "Interface",
    "RCEAST": "Interface",
    "SCSE": "Interface",
    "SGE": "Interface",
    "SPA": "Interface",
    "SPC": "Interface",
    "SPPNORTH_HUB": "Hub",
    "SPPSOUTH_HUB": "Hub",
}


class BAAEnum(StrEnum):
    SPP = "SPP"
    SWPW = "SWPW"


def _timedelta_duration(td: pd.Timedelta) -> pl.Expr:
    return pl.duration(microseconds=int(td / pd.Timedelta(microseconds=1)))


def _parse_utc_to_local(df: pl.DataFrame, column: str, tz: str) -> pl.Series:
    parsed = pd.to_datetime(
        df.get_column(column).to_pandas(),
        utc=True,
    ).dt.tz_convert(tz)
    return pl.Series(column, parsed)


def _drop_cols(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    existing = [c for c in cols if c in df.columns]
    return df.drop(existing) if existing else df


def _select_cols(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    return df.select([c for c in cols if c in df.columns])


def _rename_cols(df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    existing = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(existing) if existing else df


def _strip_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({c: c.strip() for c in df.columns})


def _ensure_columns(
    df: pl.DataFrame,
    cols: list[str],
    defaults: dict[str, object] | None = None,
) -> pl.DataFrame:
    defaults = defaults or {}
    for col in cols:
        if col not in df.columns:
            default = defaults.get(col, None)
            df = df.with_columns(pl.lit(default).alias(col))
    return df.select(cols)


def _sum_across_cols(df: pl.DataFrame, cols: list[str], out_col: str) -> pl.DataFrame:
    exprs = [
        pl.col(c).cast(pl.Float64, strict=False).fill_null(0)
        for c in cols
        if c in df.columns
    ]
    if not exprs:
        return df.with_columns(pl.lit(None).cast(pl.Float64).alias(out_col))
    return df.with_columns(pl.sum_horizontal(*exprs).alias(out_col))


def fill_baa_column(df: pl.DataFrame, load_col: str) -> pl.DataFrame:
    """Fill missing BAA values based on load magnitude.

    If the BAA column doesn't exist, creates it. If it exists but has NaN values,
    fills only the missing entries. Uses BAA_LOAD_THRESHOLD_MW to distinguish
    between SWPW (small loads) and SPP (large loads).

    Args:
        df: DataFrame with a load column to use for BAA inference.
        load_col: Name of the column containing load values.

    Returns:
        The DataFrame with BAA column filled in-place.
    """
    if load_col not in df.columns:
        raise KeyError(load_col)
    inferred_baa = (
        pl.when(
            pl.col(load_col).is_not_null() & (pl.col(load_col) < BAA_LOAD_THRESHOLD_MW),
        )
        .then(pl.lit(BAAEnum.SWPW.value))
        .otherwise(pl.lit(BAAEnum.SPP.value))
    )
    if "BAA" not in df.columns:
        return df.with_columns(inferred_baa.alias("BAA"))
    return df.with_columns(
        pl.when(pl.col("BAA").is_null())
        .then(inferred_baa)
        .otherwise(pl.col("BAA"))
        .alias("BAA"),
    )


def _binding_constraints_real_time_5_min_frequency(args_dict: dict) -> str:
    """support_date_range frequency for SPP real-time binding constraints.

    Splits at 5-min boundaries only for same-day recent windows so the
    caller's explicit end is preserved through decorator chunking. Otherwise
    splits per day, matching the source's daily-file granularity.
    """
    iso = args_dict["self"]
    end = args_dict.get("end")
    if end is None:
        return "DAY_START"

    start = utils._handle_date(args_dict["date"], iso.default_timezone)
    end = utils._handle_date(end, iso.default_timezone)
    if start.normalize() != end.normalize():
        return "DAY_START"
    if not utils.is_within_last_days(start, 1, iso.default_timezone):
        return "DAY_START"
    return "5_MIN"


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

    @support_date_range(frequency=None)
    def get_fuel_mix(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get combined fuel mix summed across SPP and SWPW BAAs

        Args:
            date: "latest", "today", a timestamp, or a date range tuple
            end: optional end date for range queries

        Returns:
            pl.DataFrame: fuel mix summed across both BAAs
        """
        return self._get_combined_fuel_mix(
            date=date,
            end=end,
            detailed=False,
            verbose=verbose,
            by_baa=False,
        )

    @support_date_range(frequency=None)
    def get_fuel_mix_detailed(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get combined detailed fuel mix summed across SPP and SWPW BAAs

        Breaks out self scheduled and market scheduled generation.

        Args:
            date: "latest", "today", a timestamp, or a date range tuple
            end: optional end date for range queries

        Returns:
            pl.DataFrame: detailed fuel mix summed across both BAAs
        """
        return self._get_combined_fuel_mix(
            date=date,
            end=end,
            detailed=True,
            verbose=verbose,
            by_baa=False,
        )

    @support_date_range(frequency=None)
    def get_fuel_mix_by_baa(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get fuel mix for both SPP and SWPW BAAs with a BAA column

        Args:
            date: "latest", "today", a timestamp, or a date range tuple
            end: optional end date for range queries

        Returns:
            pl.DataFrame: fuel mix with BAA column differentiating SPP and SWPW
        """
        return self._get_combined_fuel_mix(
            date=date,
            end=end,
            detailed=False,
            verbose=verbose,
            by_baa=True,
        )

    @support_date_range(frequency=None)
    def get_fuel_mix_by_baa_detailed(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get detailed fuel mix for both SPP and SWPW BAAs with a BAA column

        Breaks out self scheduled and market scheduled generation.

        Args:
            date: "latest", "today", a timestamp, or a date range tuple
            end: optional end date for range queries

        Returns:
            pl.DataFrame: detailed fuel mix with BAA column
        """
        return self._get_combined_fuel_mix(
            date=date,
            end=end,
            detailed=True,
            verbose=verbose,
            by_baa=True,
        )

    def _get_combined_fuel_mix(
        self,
        date: str | pd.Timestamp,
        end: pd.Timestamp | None = None,
        detailed: bool = False,
        verbose: bool = False,
        by_baa: bool = False,
    ) -> pl.DataFrame:
        """Fetch fuel mix for both SPP and SWPW BAAs and combine them.

        Args:
            date: date to fetch
            end: optional end date
            detailed: if True, breaks out self scheduled and market scheduled
            verbose: if True, log debug info
            by_baa: if True, keep BAA column; if False, sum across BAAs

        Returns:
            pl.DataFrame: combined fuel mix
        """
        spp_df = self._get_fuel_mix(
            date=date,
            end=end,
            detailed=detailed,
            verbose=verbose,
            baa=BAAEnum.SPP,
        )
        swpw_df = self._get_fuel_mix(
            date=date,
            end=end,
            detailed=detailed,
            verbose=verbose,
            baa=BAAEnum.SWPW,
        )

        if by_baa:
            return pl.concat([spp_df, swpw_df], how="diagonal").sort(
                ["Interval Start", "BAA"],
            )

        time_cols = ["Interval Start", "Interval End"]
        fuel_cols = [c for c in spp_df.columns if c not in time_cols + ["BAA"]]

        spp_df = _drop_cols(spp_df, ["BAA"])
        swpw_df = _drop_cols(swpw_df, ["BAA"])

        spp_renamed = spp_df.rename({c: f"{c}_spp" for c in fuel_cols})
        swpw_renamed = swpw_df.rename({c: f"{c}_swpw" for c in fuel_cols})
        df = spp_renamed.join(swpw_renamed, on=time_cols, how="full", coalesce=True)

        for col in fuel_cols:
            spp_col = f"{col}_spp"
            swpw_col = f"{col}_swpw"
            df = df.with_columns(
                (
                    pl.col(spp_col).cast(pl.Float64, strict=False).fill_null(0)
                    + pl.col(swpw_col).cast(pl.Float64, strict=False).fill_null(0)
                ).alias(col),
            ).drop([spp_col, swpw_col])

        result_cols = time_cols + fuel_cols
        return df.select(result_cols).sort("Interval Start")

    def _get_fuel_mix(
        self,
        date: str | pd.Timestamp,
        end: pd.Timestamp | None = None,
        detailed: bool = False,
        verbose: bool = False,
        baa: BAAEnum = BAAEnum.SPP,
    ) -> pl.DataFrame:
        now = pd.Timestamp.now(tz=self.default_timezone)
        two_hours_ago = now - pd.Timedelta(hours=2)
        one_year_ago = now - pd.Timedelta(days=365)

        if date == "latest":
            file_type = "GenMix2Hour"
        elif isinstance(date, pd.Timestamp):
            start = date
            if start < one_year_ago:
                raise NotSupported(
                    f"{baa} fuel mix data is only available for the last 365 days",
                )
            if start >= two_hours_ago:
                file_type = "GenMix2Hour"
            else:
                file_type = "GenMix365"
        else:
            raise ValueError(f"Unexpected date type: {type(date)}")

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/generation-mix-historical?path=/{baa}/{file_type}_{baa}.csv"  # noqa

        if verbose:
            logger.info(f"Downloading fuel mix from {url}")

        df_raw = pl.read_csv(url, infer_schema_length=None)
        df = process_gen_mix(df_raw, detailed=detailed)

        df = _drop_cols(
            df,
            ["Short Term Load Forecast", "Average Actual Load", "Time"],
        )

        if date != "latest" and isinstance(date, pd.Timestamp):
            if end is None:
                end = date.normalize() + pd.Timedelta(days=1)
            df = df.filter(
                (pl.col("Interval Start") >= pl.lit(date))
                & (pl.col("Interval Start") < pl.lit(end)),
            )

        return df

    def get_load(
        self,
        date: str | pd.Timestamp,
        end: str | pd.Timestamp | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns total RTO load in 5 minute intervals from STLF data."""
        baa_df = self.get_load_by_baa(date=date, end=end, verbose=verbose)

        if baa_df.is_empty():
            raise NoDataFoundException(f"No load data found for date {date}")

        return (
            baa_df.group_by(["Interval Start", "Interval End"])
            .agg(pl.col("Load").cast(pl.Float64, strict=False).sum())
            .sort("Interval Start")
        )

    def get_load_forecast(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns total RTO load forecast in hourly intervals from MTLF data."""
        baa_df = self.get_load_forecast_by_baa(date=date, end=end, verbose=verbose)

        if baa_df.is_empty():
            raise NoDataFoundException(
                f"No load forecast by BAA data found for date {date}",
            )

        return (
            baa_df.group_by(["Interval Start", "Interval End", "Publish Time"])
            .agg(pl.col("Load Forecast").cast(pl.Float64, strict=False).sum())
            .sort(["Interval Start", "Publish Time"])
        )

    @support_date_range("5_MIN")
    def get_load_forecast_short_term(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        drop_null_forecast_rows: bool = True,
    ) -> pl.DataFrame | None:
        """
        5-minute load forecast data for the SPP footprint (system-wide) for +/- 10
        minutes. Also includes actual load.

        Data from https://portal.spp.org/pages/stlf-vs-actual

        Arguments:
            date (pd.Timestamp|str): date to get data for. Supports "latest" and "today"
            verbose (bool): print info
            end (pd.Timestamp|str): end date
            drop_null_forecast_rows (bool): if True, drop rows with null forecast values

        Returns:
            pl.DataFrame: forecast as dataframe.
        """
        result = self._get_short_term_forecast_data(
            date,
            base_url=BASE_LOAD_FORECAST_SHORT_TERM_URL,
            file_prefix="OP-STLF",
            buffer_minutes=2,
        )

        if result is None:
            return None

        df, url = result

        # According to the docs, the end time col should be GMTIntervalEnd, but it's
        # only GMTInterval in the data
        df = self._post_process_load_forecast(
            df,
            url,
            forecast_type="SHORT_TERM",
            forecast_col="STLF",
            end_time_col="GMTInterval",
            interval_duration=pd.Timedelta(minutes=5),
            drop_null_forecast_rows=drop_null_forecast_rows,
        )

        df = fill_baa_column(df, "STLF")

        return df

    @support_date_range("HOUR_START")
    def get_load_forecast_mid_term(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame | None:
        """
        Returns load forecast for +7 days in hourly intervals. Includes actual load
        for the past 24 hours. Data from https://portal.spp.org/pages/mtlf-vs-actual

        Arguments:
            date (pd.Timestamp|str): date to get data for. Supports "latest" and "today"
            verbose (bool): print info

        Returns:
            pl.DataFrame: forecast as dataframe.
        """
        result = self._get_mid_term_forecast_data(
            date,
            base_url=BASE_LOAD_FORECAST_MID_TERM_URL,
            file_prefix="OP-MTLF",
            buffer_minutes=10,
        )

        if result is None:
            return None

        df, url = result

        df = self._post_process_load_forecast(
            df,
            url,
            forecast_type="MID_TERM",
            forecast_col="MTLF",
            end_time_col="GMTIntervalEnd",
            interval_duration=pd.Timedelta(hours=1),
        )

        return df

    def get_load_forecast_by_baa(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns hourly load forecast by BAA from MTLF data."""
        df = self._get_load_forecast_by_baa_raw(date=date, end=end, verbose=verbose)

        if df is None or df.is_empty():
            raise NoDataFoundException(
                f"No load forecast by BAA data found for {date}",
            )

        return (
            df.filter(pl.col("Load Forecast").is_not_null())
            .unique(
                subset=["Interval Start", "Interval End", "Publish Time", "BAA"],
                keep="last",
            )
            .sort(["Interval Start", "Publish Time"])
        )

    @support_date_range("HOUR_START")
    def _get_load_forecast_by_baa_raw(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame | None:
        result = self._get_mid_term_forecast_data(
            date=date,
            base_url=BASE_LOAD_FORECAST_MID_TERM_URL,
            file_prefix="OP-MTLF",
            buffer_minutes=10,
        )

        if result is None:
            return None

        df, url = result

        df = self._post_process_load_forecast(
            df,
            url,
            forecast_type="MID_TERM",
            forecast_col="MTLF",
            end_time_col="GMTIntervalEnd",
            interval_duration=pd.Timedelta(hours=1),
        )

        df = fill_baa_column(df, "MTLF")

        return _rename_cols(
            df.select(
                ["Interval Start", "Interval End", "Publish Time", "BAA", "MTLF"],
            ),
            {"MTLF": "Load Forecast"},
        )

    def _handle_dst_floor_date(
        self,
        date: pd.Timestamp,
        freq: str = "5min",
    ) -> pd.Timestamp:
        """Handle DST transition when flooring a date.

        Args:
            date: The date to floor
            freq: The frequency to floor to (e.g., "5min", "h")

        Returns:
            Timestamp floored to the specified frequency
        """
        try:
            floored_date = date.floor(freq)
        except pytz.AmbiguousTimeError:
            floored_date = self.safe_for_dst_transition_floor(date, freq)

        return floored_date

    def _get_short_term_forecast_data(
        self,
        date: str | pd.Timestamp,
        base_url: str,
        file_prefix: str,
        buffer_minutes: int = 2,
    ) -> tuple[pl.DataFrame, str] | None:
        """Get short-term forecast data with common DST handling logic.

        Args:
            date: Date to get data for. Supports "latest" and "today"
            base_url: Base URL for downloads
            file_prefix: Prefix for the file name (e.g., "OP-STLF", "OP-STRF")
            buffer_minutes: Buffer minutes for "latest" date

        Returns:
            tuple: (dataframe, url) or None if date is in the future
        """
        if date == "latest":
            date = self.now() - pd.Timedelta(minutes=buffer_minutes)

        # Files do not exist in the future
        if date > self.now():
            return None

        floored_date = self._handle_dst_floor_date(date, "5min")

        hour = floored_date.hour
        padded_hour = str(hour).zfill(2)
        padded_hour_plus_one = str((hour + 1) % 24).zfill(2)

        # NOTE: this needs to be updated for DST every year
        # 0105d through 0155d have a "d" on 2025-11-02
        add_d = (
            floored_date.year == 2025
            and floored_date.month == 11
            and floored_date.day == 2
            and floored_date.hour == 1
            and floored_date.minute != 0
        )

        # The first hour in the URL is 1 after the hour in the filename.
        url = base_url + floored_date.strftime(
            f"/%Y/%m/%d/{padded_hour_plus_one}/{file_prefix}-%Y%m%d{padded_hour}%M{'d' if add_d else ''}.csv",
        )

        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)

        return df, url

    def _get_mid_term_forecast_data(
        self,
        date: str | pd.Timestamp,
        base_url: str,
        file_prefix: str,
        buffer_minutes: int = 10,
    ) -> tuple[pl.DataFrame, str] | None:
        """Get mid-term forecast data with common DST handling logic.

        Args:
            date: Date to get data for. Supports "latest" and "today"
            base_url: Base URL for downloads
            file_prefix: Prefix for the file name (e.g., "OP-MTLF", "OP-MTRF")
            buffer_minutes: Buffer minutes for "latest" date

        Returns:
            tuple: (dataframe, url) or None if date is in the future
        """
        if date == "latest":
            date = self.now() - pd.Timedelta(minutes=buffer_minutes)

        # Files do not exist in the future
        if date > self.now():
            return None

        floored_date = self._handle_dst_floor_date(date, "h")

        # For mid-term hourly forecasts during 2025 DST end there is a 0200d file.
        # Special case for DST end on 2025-11-02
        add_d = (
            floored_date.year == 2025
            and floored_date.month == 11
            and floored_date.day == 2
            and floored_date.hour == 2
        )

        # Explicitly set the minutes to 00 in the URL
        url = base_url + floored_date.strftime(
            f"/%Y/%m/%d/{file_prefix}-%Y%m%d{str(floored_date.hour).zfill(2)}00{'d' if add_d else ''}.csv",
        )

        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)

        return df, url

    def _post_process_load_forecast(
        self,
        df: pl.DataFrame,
        url: str,
        forecast_type: str,
        forecast_col: str,
        end_time_col: str,
        interval_duration: pd.Timedelta,
        drop_null_forecast_rows: bool = True,
    ) -> pl.DataFrame:
        df = self._handle_market_end_to_interval(df, end_time_col, interval_duration)

        publish_time = pd.Timestamp(
            re.search(r"[0-9]{12}", url).group(0),
        ).tz_localize(
            tz=self.default_timezone,
            ambiguous=not url.endswith("d.csv"),
        )

        df = _strip_columns(df).with_columns(
            pl.lit(publish_time).alias("Publish Time"),
            pl.lit(forecast_type).alias("Forecast Type"),
        )

        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End", "Publish Time", "Forecast Type"],
        )
        df = _drop_cols(df, ["Time", "Interval"]).sort(
            ["Interval Start", "Publish Time"],
        )

        if drop_null_forecast_rows:
            df = df.filter(pl.col(forecast_col).is_not_null())

        return df

    @support_date_range("5_MIN")
    def get_solar_and_wind_forecast_short_term(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame | None:
        """
        Returns solar and wind generation forecast for +4 hours in 5 minute intervals.
        Include actuals for past day in 5 minute intervals.

        Data from https://portal.spp.org/pages/shortterm-resource-forecast

        Arguments:
            date (pd.Timestamp|str): date to get data for. Supports "latest" and "today"
            verbose (bool): print info

        Returns:
            pl.DataFrame: forecast as dataframe.
        """
        result = self._get_short_term_forecast_data(
            date,
            base_url=BASE_SOLAR_AND_WIND_SHORT_TERM_URL,
            file_prefix="OP-STRF",
            buffer_minutes=2,
        )

        if result is None:
            return None

        df, url = result

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
    def get_solar_and_wind_forecast_mid_term(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame | None:
        """
        Returns solar and wind generation forecast for +7 days in hourly intervals.

        Data from https://portal.spp.org/pages/midterm-resource-forecast.

        Arguments:
            date (pd.Timestamp|str): date to get data for. Supports "latest" and "today"
            verbose (bool): print info

        Returns:
            pl.DataFrame: forecast as dataframe.
        """
        result = self._get_mid_term_forecast_data(
            date,
            base_url=BASE_SOLAR_AND_WIND_MID_TERM_URL,
            file_prefix="OP-MTRF",
            buffer_minutes=10,
        )

        if result is None:
            return None

        df, url = result

        df = self._post_process_solar_and_wind_forecast(
            df,
            url,
            forecast_type="MID_TERM",
            end_time_col="GMTIntervalEnd",
            interval_duration=pd.Timedelta(hours=1),
        )

        return df

    @support_date_range("HOUR_START")
    def get_solar_and_wind_forecast_by_reserve_zone(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame | None:
        """Returns hourly solar and wind forecasts and actuals by reserve zone.

        Data from https://portal.spp.org/pages/resource-forecast-by-reserve-zone.
        The ``date`` parameter is the publish hour of the source file.
        """
        result = self._get_mid_term_forecast_data(
            date,
            base_url=BASE_RESERVE_ZONE_FORECAST_URL,
            file_prefix="RF_RESERVE_ZONE",
            buffer_minutes=10,
        )

        if result is None:
            return None

        df, url = result

        return self._post_process_solar_and_wind_forecast_by_reserve_zone(
            df,
            url,
            end_time_col="GMTIntervalEnd",
            interval_duration=pd.Timedelta(hours=1),
        )

    def get_load_by_baa(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Returns actual load by BAA from short-term load forecast data."""
        df = self._get_load_by_baa_raw(date=date, end=end, verbose=verbose)

        if df is None or df.is_empty():
            return pl.DataFrame(
                schema={
                    "Interval Start": pl.Datetime("us", SPP.default_timezone),
                    "Interval End": pl.Datetime("us", SPP.default_timezone),
                    "BAA": pl.Utf8,
                    "Load": pl.Float64,
                },
            )

        return (
            df.filter(pl.col("Load").is_not_null())
            .unique(
                subset=["Interval Start", "Interval End", "BAA"],
                keep="last",
            )
            .sort("Interval Start")
        )

    @support_date_range(frequency="5_MIN")
    def _get_load_by_baa_raw(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame | None:
        result = self._get_short_term_forecast_data(
            date=date,
            base_url=BASE_LOAD_FORECAST_SHORT_TERM_URL,
            file_prefix="OP-STLF",
            buffer_minutes=2,
        )

        if result is None:
            return None

        df, url = result

        df = self._post_process_load_forecast(
            df,
            url,
            forecast_type="SHORT_TERM",
            end_time_col="GMTInterval",
            interval_duration=pd.Timedelta(minutes=5),
            forecast_col="STLF",
            drop_null_forecast_rows=False,
        )

        df = fill_baa_column(df, "Actual")

        return _rename_cols(
            df.select(["Interval Start", "Interval End", "BAA", "Actual"]),
            {"Actual": "Load"},
        )

    @support_date_range("DAY_START")
    def get_load_by_baa_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame | None:
        """Returns hourly actual load by BAA from mid-term load forecast data."""

        if date == "latest":
            fetch_date: str | pd.Timestamp = date
        else:
            date_ts = utils._handle_date(date, self.default_timezone)
            now = self.now()
            if date_ts.normalize() == now.normalize():
                fetch_date = now - pd.Timedelta(minutes=10)
            else:
                fetch_date = date_ts.normalize() + pd.Timedelta(hours=23)

        result = self._get_mid_term_forecast_data(
            date=fetch_date,
            base_url=BASE_LOAD_FORECAST_MID_TERM_URL,
            file_prefix="OP-MTLF",
            buffer_minutes=10,
        )

        if result is None:
            return pl.DataFrame(
                schema={
                    "Interval Start": pl.Datetime("us", SPP.default_timezone),
                    "Interval End": pl.Datetime("us", SPP.default_timezone),
                    "BAA": pl.Utf8,
                    "Load": pl.Float64,
                },
            )

        df, url = result

        df = self._post_process_load_forecast(
            df,
            url,
            forecast_type="MID_TERM",
            end_time_col="GMTIntervalEnd",
            interval_duration=pd.Timedelta(hours=1),
            forecast_col="MTLF",
        )

        df = fill_baa_column(df, "Averaged Actual")

        result_df = _rename_cols(
            df.select(["Interval Start", "Interval End", "BAA", "Averaged Actual"]),
            {"Averaged Actual": "Load"},
        )

        if date != "latest":
            date_ts = utils._handle_date(date, self.default_timezone)
            day_start = date_ts.normalize()
            day_end = day_start + pd.Timedelta(days=1)
            result_df = result_df.filter(
                (pl.col("Interval Start") >= pl.lit(day_start))
                & (pl.col("Interval Start") < pl.lit(day_end)),
            )

        return result_df.filter(pl.col("Load").is_not_null()).sort("Interval Start")

    def _post_process_solar_and_wind_forecast(
        self,
        df: pl.DataFrame,
        url: str,
        forecast_type: str,
        end_time_col: str,
        interval_duration: pd.Timedelta,
    ) -> pl.DataFrame:
        df = self._handle_market_end_to_interval(df, end_time_col, interval_duration)

        publish_time = pd.Timestamp(
            re.search(r"[0-9]{12}", url).group(0),
        ).tz_localize(
            tz=self.default_timezone,
            ambiguous=not url.endswith("d.csv"),
        )

        df = _strip_columns(df).with_columns(
            pl.lit(publish_time).alias("Publish Time"),
            pl.lit(forecast_type).alias("Forecast Type"),
        )

        df = utils.move_cols_to_front(
            df,
            ["Interval Start", "Interval End", "Publish Time", "Forecast Type"],
        )
        return (
            _drop_cols(df, ["Time", "Interval"])
            .sort(["Interval Start", "Publish Time"])
            .filter(
                pl.col("Wind Forecast MW").is_not_null()
                & pl.col("Solar Forecast MW").is_not_null(),
            )
        )

    def _post_process_solar_and_wind_forecast_by_reserve_zone(
        self,
        df: pl.DataFrame,
        url: str,
        end_time_col: str,
        interval_duration: pd.Timedelta,
    ) -> pl.DataFrame:
        df = _strip_columns(df)
        df = self._handle_market_end_to_interval(df, end_time_col, interval_duration)

        publish_time = pd.Timestamp(
            re.search(r"[0-9]{12}", url).group(0),
        ).tz_localize(
            tz=self.default_timezone,
            ambiguous=not url.endswith("d.csv"),
        )

        df = _strip_columns(df).with_columns(
            pl.lit(publish_time).alias("Publish Time"),
        )

        df = _rename_cols(
            df,
            {
                "ReserveZone": "Reserve Zone",
                "WindForecastMW": "Wind Forecast",
                "WindActualMW": "Wind Actual",
                "SolarForecastMW": "Solar Forecast",
                "SolarActualMW": "Solar Actual",
            },
        )

        df = utils.move_cols_to_front(
            df,
            [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "BAA",
                "Reserve Zone",
            ],
        )
        df = _drop_cols(df, ["Time", "IntervalEnd", "Interval"]).sort(
            ["Interval Start", "Publish Time", "Reserve Zone"],
        )

        value_cols = [
            "Wind Forecast",
            "Wind Actual",
            "Solar Forecast",
            "Solar Actual",
        ]
        return df.filter(
            pl.any_horizontal([pl.col(c).is_not_null() for c in value_cols]),
        )

    def _mid_term_solar_and_wind_url(self, date: pd.Timestamp) -> str:
        # Explicitly set the minutes to 00.
        return BASE_SOLAR_AND_WIND_MID_TERM_URL + date.strftime(
            "/%Y/%m/%d/OP-MTRF-%Y%m%d%H00.csv",
        )

    def _mid_term_load_forecast_url(self, date: pd.Timestamp) -> str:
        # Explicitly set the minutes to 00.
        return BASE_LOAD_FORECAST_MID_TERM_URL + date.strftime(
            "/%Y/%m/%d/OP-MTLF-%Y%m%d%H00.csv",
        )

    def _handle_market_end_to_interval(
        self,
        df: pl.DataFrame,
        column: str,
        interval_duration: pd.Timedelta,
        format: str | None = None,
    ) -> pl.DataFrame:
        """Converts market end time to interval end time"""

        df = _rename_cols(df, {column: "Interval End"})
        duration = _timedelta_duration(interval_duration)

        interval_end = pl.Series(
            "Interval End",
            pd.to_datetime(
                df.get_column("Interval End").to_pandas(),
                utc=True,
                format=format,
            ).dt.tz_convert(self.default_timezone),
        )
        df = df.with_columns(interval_end)
        df = df.with_columns(
            (pl.col("Interval End") - duration).alias("Interval Start"),
        )
        df = df.with_columns(pl.col("Interval Start").alias("Time"))

        return utils.move_cols_to_front(df, ["Time", "Interval Start", "Interval End"])

    _ver_curtailment_numerical_cols = [
        "Wind Redispatch Curtailments",
        "Wind Manual Curtailments",
        "Wind Curtailed For Energy",
        "Solar Redispatch Curtailments",
        "Solar Manual Curtailments",
        "Solar Curtailed For Energy",
    ]

    def _process_ver_curtailments(self, df: pl.DataFrame) -> pl.DataFrame:
        df = _rename_cols(
            df,
            {
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
            "Interval Start",
            "Interval End",
            "Wind Redispatch Curtailments",
            "Wind Manual Curtailments",
            "Wind Curtailed For Energy",
            "Solar Redispatch Curtailments",
            "Solar Manual Curtailments",
            "Solar Curtailed For Energy",
            "BAA",
        ]

        defaults = {col: BAAEnum.SPP for col in cols if col == "BAA"}
        df = _ensure_columns(df, cols, defaults=defaults)

        df = df.filter(
            pl.any_horizontal(
                [pl.col(c).is_not_null() for c in self._ver_curtailment_numerical_cols],
            ),
        )
        df = df.filter(pl.col("Interval Start").is_not_null())
        return df.sort("Interval Start")

    def _aggregate_ver_curtailments(self, df: pl.DataFrame) -> pl.DataFrame:
        """Sum VER curtailment numerical columns across BAAs."""
        return (
            df.group_by(["Interval Start", "Interval End"])
            .agg(
                [
                    pl.col(c).cast(pl.Float64, strict=False).sum()
                    for c in self._ver_curtailment_numerical_cols
                ],
            )
            .sort("Interval Start")
        )

    @support_date_range("DAY_START")
    def get_capacity_of_generation_on_outage(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get Capacity of Generation on Outage.

        Published daily at 8am CT for next 7 days

        Args:
            date: start date
            end: end date


        """
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/capacity-of-generation-on-outage?path=/{date.strftime('%Y')}/{date.strftime('%m')}/Capacity-Gen-Outage-{date.strftime('%Y%m%d')}.csv"  # noqa

        logger.info(f"Downloading {url}")

        df = pl.read_csv(url, infer_schema_length=None)

        return self._process_capacity_of_generation_on_outage(df, publish_time=date)

    def get_capacity_of_generation_on_outage_annual(
        self,
        year: int,
        verbose: bool = True,
    ) -> pl.DataFrame:
        """Get VER Curtailments for a year. Starting 2014.
        Recent data use get_capacity_of_generation_on_outage

        Args:
            year: year to get data for
            verbose: print url

        Returns:
            pl.DataFrame: VER Curtailments
        """
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/capacity-of-generation-on-outage?path=/{year}/{year}.zip"  # noqa

        def process_csv(df: pl.DataFrame, file_name: str) -> pl.DataFrame:
            publish_time_str = file_name.split(".")[0].split("-")[-1]
            publish_time = pd.to_datetime(publish_time_str).tz_localize(
                self.default_timezone,
            )

            return self._process_capacity_of_generation_on_outage(df, publish_time)

        df = utils.download_csvs_from_zip_url(
            url,
            process_csv=process_csv,
            verbose=verbose,
        )

        return df.sort("Interval Start")

    def _process_capacity_of_generation_on_outage(
        self,
        df: pl.DataFrame,
        publish_time: pd.Timestamp,
    ) -> pl.DataFrame:
        df = _strip_columns(df)

        df = self._handle_market_end_to_interval(
            df,
            column="Market Hour",
            interval_duration=pd.Timedelta(minutes=60),
        )

        df = _rename_cols(df, {"Outaged MW": "Total Outaged MW"})

        publish_time = pd.to_datetime(publish_time.normalize())

        df = df.with_columns(pl.lit(publish_time).alias("Publish Time"))

        return utils.move_cols_to_front(
            _drop_cols(df, ["Time"]),
            ["Publish Time"],
        )

    def _fetch_ver_curtailments_daily(self, date: pd.Timestamp) -> pl.DataFrame:
        """Fetch and process a single day's VER curtailments CSV."""
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/ver-curtailments?path=/{date.strftime('%Y')}/{date.strftime('%m')}/VER-Curtailments-{date.strftime('%Y%m%d')}.csv"  # noqa
        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)
        return self._process_ver_curtailments(df)

    def _fetch_ver_curtailments_annual(
        self,
        year: int,
        verbose: bool = True,
    ) -> pl.DataFrame:
        """Fetch and process a full year's VER curtailments zip."""
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/ver-curtailments?path=/{year}/{year}.zip"  # noqa
        df = utils.download_csvs_from_zip_url(url, verbose=verbose)
        return self._process_ver_curtailments(df)

    @support_date_range("DAY_START")
    def get_ver_curtailments(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get VER Curtailments summed across BAAs.

        Supports recent data. For historical annual data use
        get_ver_curtailments_annual. For data broken down by BAA use
        get_ver_curtailments_by_baa.

        Args:
            date: start date
            end: end date
        """
        df = self._fetch_ver_curtailments_daily(date)
        return self._aggregate_ver_curtailments(df)

    def get_ver_curtailments_annual(
        self,
        year: int,
        verbose: bool = True,
    ) -> pl.DataFrame:
        """Get VER Curtailments summed across BAAs for a year. Starting 2014.

        Recent data use get_ver_curtailments. For data broken down by BAA use
        get_ver_curtailments_by_baa_annual.

        Args:
            year: year to get data for
            verbose: print url

        Returns:
            pl.DataFrame: VER Curtailments
        """
        df = self._fetch_ver_curtailments_annual(year, verbose=verbose)
        return self._aggregate_ver_curtailments(df)

    @support_date_range("DAY_START")
    def get_ver_curtailments_by_baa(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get VER Curtailments broken down by BAA.

        Supports recent data. For historical annual data use
        get_ver_curtailments_by_baa_annual.

        Args:
            date: start date
            end: end date
        """
        return self._fetch_ver_curtailments_daily(date)

    def get_ver_curtailments_by_baa_annual(
        self,
        year: int,
        verbose: bool = True,
    ) -> pl.DataFrame:
        """Get VER Curtailments broken down by BAA for a year. Starting 2014.

        Recent data use get_ver_curtailments_by_baa.

        Args:
            year: year to get data for
            verbose: print url

        Returns:
            pl.DataFrame: VER Curtailments
        """
        return self._fetch_ver_curtailments_annual(year, verbose=verbose)

    def _get_load_and_forecast(self, verbose: bool = False) -> pl.DataFrame:
        url = f"{MARKETPLACE_BASE_URL}/chart-api/load-forecast/asChart"

        logger.info(f"Getting load and forecast from {url}")

        r = self._get_json(url)["response"]

        data = {"Time": r["labels"]}
        for d in r["datasets"][:3]:
            if d["label"] == "Actual Load":
                data["Actual Load"] = d["data"]
            elif d["label"] == "Mid-Term Load Forecast":
                data["Mid-Term Forecast"] = d["data"]
            elif d["label"] == "Short-Term Load Forecast":
                data["Short-Term Forecast"] = d["data"]

        df = pl.DataFrame(data)

        df = df.with_columns(
            _parse_utc_to_local(df, "Time", self.default_timezone).alias("Time"),
        )

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

    def get_raw_interconnection_queue(self, verbose: bool = False) -> BinaryIO:
        url = "https://opsportal.spp.org/Studies/GenerateSummaryCSV"
        logger.info(f"Getting interconnection queue from {url}")
        response = requests.get(url)
        return utils.get_response_blob(response)

    def get_interconnection_queue(self, verbose: bool = False) -> pl.DataFrame:
        """Get interconnection queue

        Returns:
            pandas.DataFrame: Interconnection queue
        """
        raw_data = self.get_raw_interconnection_queue(verbose)
        queue = pl.from_pandas(pd.read_csv(raw_data, skiprows=1))

        completed_val = InterconnectionQueueStatus.COMPLETED.value
        active_val = InterconnectionQueueStatus.ACTIVE.value
        withdrawn_val = InterconnectionQueueStatus.WITHDRAWN.value
        queue = queue.with_columns(
            pl.col("Status").alias("Status (Original)"),
            pl.col("Status").replace(
                {
                    "IA FULLY EXECUTED/COMMERCIAL OPERATION": completed_val,
                    "IA FULLY EXECUTED/ON SCHEDULE": completed_val,
                    "IA FULLY EXECUTED/ON SUSPENSION": completed_val,
                    "IA PENDING": active_val,
                    "DISIS STAGE": active_val,
                    "None": active_val,
                    "WITHDRAWN": withdrawn_val,
                },
            ),
            pl.concat_str(
                [pl.col("Generation Type"), pl.col("Fuel Type")],
                separator=" - ",
                ignore_nulls=True,
            ).alias("Generation Type"),
            pl.col("Commercial Operation Date").alias("Proposed Completion Date"),
        )

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
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        location_type: str = LOCATION_TYPE_ALL,
        verbose: bool = False,
        use_daily_files: bool = False,
    ) -> pl.DataFrame:
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
            use_daily_files: if True, use daily files instead of 5 minute files.
        """
        if use_daily_files:
            df = self._get_real_time_5_min_data_from_daily_files(
                date,
                end=end,
                location_type=location_type,
                verbose=verbose,
            )
            df = _strip_columns(df)
            df = _rename_cols(
                df,
                {
                    "GMT Interval": "GMTIntervalEnd",
                    "Settlement Location Name": "Settlement Location",
                    "PNODE Name": "PNode",
                },
            )
        else:
            df = self._get_real_time_5_min_data(
                date,
                end=end,
                location_type=location_type,
                verbose=verbose,
            )

        return self._finalize_spp_df(
            df,
            market=Markets.REAL_TIME_5_MIN,
            location_type=location_type,
            verbose=verbose,
        )

    def get_lmp_real_time_5_min_by_bus(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        location_type: str = LOCATION_TYPE_ALL,
        verbose: bool = False,
    ) -> pl.DataFrame:
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

        logger.info(f"Getting data for {date} from {url}")

        df = pl.read_csv(url, infer_schema_length=None)

        return df

    @support_date_range(frequency="DAY_START")
    def _get_real_time_5_min_data_from_daily_files(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        location_type: str = LOCATION_TYPE_ALL,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """
        Internal function that consolidates logic for getting LMP data for the
        Real-Time 5 Minute Market
        """
        if location_type not in self.location_types:
            raise NotSupported(f"Location type {location_type} not supported")

        if location_type == LOCATION_TYPE_BUS:
            endpoint = RTBM_LMP_BY_BUS
            file_prefix = "RTBM-LMP-DAILY-B"
        else:
            endpoint = FS_RTBM_LMP_BY_LOCATION
            file_prefix = "RTBM-LMP-DAILY-SL"

        url = self._format_daily_url(date, end, endpoint, file_prefix)

        logger.info(f"Getting data for {date} from {url} (daily file)")

        df = pl.read_csv(url, infer_schema_length=None)

        return df

    @support_date_range(frequency="DAY_START")
    def get_lmp_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        location_type: str = LOCATION_TYPE_ALL,
        verbose: bool = False,
    ) -> pl.DataFrame:
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

        df = self._get_dam_lmp(date, location_type=location_type, verbose=verbose)

        return self._finalize_spp_df(
            df,
            market=Markets.DAY_AHEAD_HOURLY,
            location_type=location_type,
            verbose=verbose,
            include_baa=True,
        )

    @support_date_range(frequency="DAY_START")
    def get_lmp_day_ahead_hourly_by_bus(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get day ahead hourly LMP data by bus (pnode-level).

        Args:
            date: date to get data for
            end: end date
            verbose: print url

        NOTE: does not take a location_type argument because it always returns
        LOCATION_TYPE_BUS.
        """
        if date == "latest":
            raise NotSupported("Latest not supported for day ahead hourly by bus")

        df = self._get_dam_lmp(date, location_type=LOCATION_TYPE_BUS, verbose=verbose)

        return self._finalize_spp_df(
            df,
            market=Markets.DAY_AHEAD_HOURLY,
            location_type=LOCATION_TYPE_BUS,
            verbose=verbose,
            include_baa=True,
        )

    def _get_feature_data(self, base_url: str, verbose: bool = False) -> pl.DataFrame:
        """Fetches data from ArcGIS Map Service with Feature Data

        Returns:
            pl.DataFrame of features
        """
        args = {
            "f": "json",
            "where": "OBJECTID IS NOT NULL",
            "returnGeometry": "false",
            "outFields": "*",
        }
        doc = self._get_json(base_url, verbose=verbose, params=args)
        return pl.DataFrame([feature["attributes"] for feature in doc["features"]])

    def _get_dam_lmp(
        self,
        date: pd.Timestamp,
        location_type: str = LOCATION_TYPE_ALL,
        verbose: bool = False,
    ) -> pl.DataFrame:
        if location_type == LOCATION_TYPE_BUS:
            endpoint = FS_DAM_LMP_BY_BUS
            file_prefix = "DA-LMP-B"
        else:
            endpoint = FS_DAM_LMP_BY_LOCATION
            file_prefix = "DA-LMP-SL"

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/{endpoint}?path=/{date.strftime('%Y')}/{date.strftime('%m')}/By_Day/{file_prefix}-{date.strftime('%Y%m%d')}0100.csv"  # noqa
        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)
        return df

    def _finalize_spp_df(
        self,
        df: pl.DataFrame,
        market: Markets,
        location_type: str,
        verbose: bool = False,
        include_baa: bool = False,
    ) -> pl.DataFrame:
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
            include_baa (bool, optional): Include BAA column. If BAA is not present and
             this is True, it will be added with the default value of "SPP"
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

        df = df.with_columns(pl.lit(market.value).alias("Market"))

        if location_type != LOCATION_TYPE_BUS:
            df = df.with_columns(
                pl.col("Settlement Location").alias("Location"),
                pl.col("Settlement Location")
                .replace_strict(
                    LMP_HUBS_AND_INTERFACES,
                    default=LOCATION_TYPE_SETTLEMENT_LOCATION,
                    return_dtype=pl.Utf8,
                )
                .alias("Location Type"),
            )
            df = utils.filter_lmp_locations(df, location_type=location_type)
        else:
            df = df.with_columns(
                pl.col("Pnode").alias("Location"),
                pl.lit(LOCATION_TYPE_BUS).alias("Location Type"),
            )

        df = _rename_cols(
            df,
            {
                "Pnode": "PNode",
                "LMP": "LMP",
                "MLC": "Loss",
                "MCC": "Congestion",
                "MEC": "Energy",
            },
        )

        if include_baa and "BAA" not in df.columns:
            df = df.with_columns(pl.lit("SPP").alias("BAA"))

        cols = [
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

        if "BAA" in df.columns:
            cols.insert(cols.index("Location"), "BAA")

        df = df.select(cols)

        if location_type == LOCATION_TYPE_BUS:
            df = _drop_cols(df, ["PNode"])

        return df.sort(["Time", "Location"])

    @support_date_range("5_MIN")
    def get_operating_reserves(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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

        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)
        return self._process_operating_reserves(df)

    def _process_operating_reserves(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=pd.Timedelta(minutes=5),
        )

        df = _drop_cols(df, ["Interval"])

        return _rename_cols(
            df,
            {
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

    def get_as_prices_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
        use_daily_files: bool = False,
    ) -> pl.DataFrame:
        """
        Provides Marginal Clearing Price information by Reserve Zone for each
        Real-Time 5-minute Market solution.

        Args:
            date: date to get data for. Supports "latest" for most recent interval.
            end: end date
            verbose: print url
            use_daily_files: if True, use daily files instead of 5 minute files.

        Returns:
            pl.DataFrame: Real-Time 5-minute Marginal Clearing Prices
        """
        if use_daily_files:
            return self._get_as_prices_real_time_5_min_from_daily_files(
                date,
                end=end,
                verbose=verbose,
            )
        else:
            return self._get_as_prices_real_time_5_min_from_intervals(
                date,
                end=end,
                verbose=verbose,
            )

    @support_date_range("5_MIN")
    def _get_as_prices_real_time_5_min_from_intervals(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get AS prices from 5-minute interval files."""
        if date == "latest":
            url = f"{FILE_BROWSER_DOWNLOAD_URL}/rtbm-mcp?path=/RTBM-MCP-latestInterval.csv"
        else:
            url = self._format_5_min_url(
                date,
                end,
                endpoint=RTBM_MCP,
                file_prefix="RTBM-MCP",
                include_interval=True,
            )

        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)
        return self._process_as_prices_real_time(df)

    @support_date_range("DAY_START")
    def _get_as_prices_real_time_5_min_from_daily_files(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get AS prices from daily files."""
        if date == "latest":
            raise ValueError("Latest not supported with daily files")

        url = self._format_daily_mcp_url(date, end)
        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)

        return self._process_as_prices_real_time(_strip_columns(df))

    def _process_as_prices_real_time(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=pd.Timedelta(minutes=5),
        )

        df = _strip_columns(df)

        column_mapping = {
            "RegUPService": "Reg Up Service",
            "RegDNService": "Reg DN Service",
            "RegUpMile": "Reg Up Mile",
            "RegDNMile": "Reg DN Mile",
            "RampUP": "Ramp Up",
            "RampDN": "Ramp DN",
            "Spin": "Spin",
            "Supp": "Supp",
            "UncUP": "Unc Up",
        }

        df = _rename_cols(df, column_mapping)

        cols_to_keep = [
            "Interval Start",
            "Interval End",
            "Reserve Zone",
        ] + list(column_mapping.values())

        return df.sort(["Interval Start", "Reserve Zone"]).select(
            [c for c in cols_to_keep if c in df.columns],
        )

    def _format_daily_mcp_url(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp | None,
    ) -> str:
        """
        Formats the URL for daily MCP data files.

        Args:
            start (pd.Timestamp): Start date of the data.
            end (pd.Timestamp): End date of the data.

        Returns:
            str: The formatted URL.
        """
        folder_year = start.strftime("%Y")
        folder_month = start.strftime("%m")

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/{RTBM_MCP}?path=/{folder_year}/{folder_month}/By_Day/RTBM-MCP-DAILY-{start.strftime('%Y%m%d')}.csv"

        return url

    @support_date_range("DAY_START")
    def get_day_ahead_operating_reserve_prices(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Provides Marginal Clearing Price information by Reserve Zone for each
        Day-Ahead Market solution for each Operating Day.
        Posting is updated each day after the DA Market results are posted.
        Available at https://portal.spp.org/pages/da-mcp#

        Args:
            date: date to get data for
            end: end date
            verbose: print url

        Returns:
            pl.DataFrame: Day Ahead Marginal Clearing Prices
        """
        if date == "latest":
            raise ValueError(
                "Latest not supported for Day Ahead Marginal Clearing Prices",
            )

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/da-mcp?path=/{date.strftime('%Y')}/{date.strftime('%m')}/DA-MCP-{date.strftime('%Y%m%d')}0100.csv"  # noqa

        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)

        return self._process_day_ahead_operating_reserve_prices(df)

    def _process_day_ahead_operating_reserve_prices(
        self,
        df: pl.DataFrame,
    ) -> pl.DataFrame:
        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=pd.Timedelta(hours=1),
        ).with_columns(pl.lit("DAM").alias("Market"))

        column_mapping = {
            "RegUP": "Reg_Up",
            "RegDN": "Reg_Dn",
            "RampUP": "Ramp_Up",
            "RampDN": "Ramp_Dn",
            "Spin": "Spin",
            "Supp": "Supp",
            "UncUP": "Unc_Up",
        }

        df = _rename_cols(df, column_mapping)

        cols_to_keep = [
            "Interval Start",
            "Interval End",
            "Market",
            "Reserve Zone",
        ] + list(column_mapping.values())

        return df.select([c for c in cols_to_keep if c in df.columns])

    @support_date_range("5_MIN")
    def get_lmp_real_time_weis(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
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
        logger.info(f"Downloading {url}")

        try:
            df = pl.read_csv(url, infer_schema_length=None)
        except ConnectionResetError as e:
            logger.error(f"Error downloading {url}: {e}")
            return pl.DataFrame()

        return self._process_lmp_real_time_weis(df)

    def _process_lmp_real_time_weis(self, df: pl.DataFrame) -> pl.DataFrame:
        df = _strip_columns(df)

        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=pd.Timedelta(minutes=5),
        )

        df = df.with_columns(
            pl.lit(LOCATION_TYPE_SETTLEMENT_LOCATION).alias("Location Type"),
            pl.lit("REAL_TIME_WEIS").alias("Market"),
        )

        df = _rename_cols(
            df,
            {
                "Settlement Location": "Location",
                "Pnode": "PNode",
                "LMP": "LMP",
                "MLC": "Loss",
                "MCC": "Congestion",
                "MEC": "Energy",
            },
        )

        return df.select(
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
            ],
        )

    def _format_5_min_url(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp | None,
        endpoint: str,
        file_prefix: str,
        include_interval: bool = True,
    ) -> str:
        # Folder path is based on start date. File name is based on end date.
        # As an example, the file with the name 202407010000 representing the interval
        # 2024-06-30 23:55:00 to 2024-07-01 00:00:00 is in the folder
        # 2024/06/By_Interval/30/

        if end is None:
            end = start + FiveMinOffset()
        else:
            # To deal with DST, convert to UTC before ceil
            end = end.tz_convert("UTC").ceil("5min").tz_convert(self.default_timezone)

        folder_year = start.strftime("%Y")
        folder_month = start.strftime("%m")
        folder_day = start.strftime("%d")

        interval_str = "/By_Interval" if include_interval else ""

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/{endpoint}?path=/{folder_year}/{folder_month}{interval_str}/{folder_day}/{file_prefix}-{end.strftime('%Y%m%d%H%M')}.csv"  # noqa

        # Intervals that occur after DST end during the repeated hour have a "d" suffix
        # Identify these intervals by the offset of the end time. Since CDT is UTC-5 and
        # CST is UTC-6, if the UTC offset is larger than the offset of the hour before,
        # it's a repeated hour in CST.
        if abs(end.utcoffset()) > abs((end - pd.Timedelta(hours=1)).utcoffset()):
            url = url.split(".csv")[0] + "d.csv"

        status_code = requests.head(url).status_code

        if status_code == 200:
            return url
        else:
            raise NoDataFoundException(f"No data found for {url}")

    def _format_daily_url(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp | None,
        endpoint: str,
        file_prefix: str,
    ) -> str:
        """
        Formats the URL for daily data files.

        Args:
            start (pd.Timestamp): Start date of the data.
            end (pd.Timestamp): End date of the data.
            endpoint (str): The API endpoint to use.
            file_prefix (str): The prefix for the file name.

        Returns:
            str: The formatted URL.
        """
        folder_year = start.strftime("%Y")
        folder_month = start.strftime("%m")

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/{endpoint}?path=/{folder_year}/{folder_month}/By_Day/{file_prefix}-{start.strftime('%Y%m%d')}.csv"  # noqa: E501

        return url

    def _get_location_list(
        self,
        location_type: str,
        verbose: bool = False,
    ) -> list[str]:
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

    def _fetch_and_concat_csvs(
        self,
        urls: list[str],
        verbose: bool = False,
    ) -> pl.DataFrame:
        all_dfs = []
        for url in tqdm.tqdm(urls):
            logger.info(f"Fetching {url}")
            df = pl.read_csv(url, infer_schema_length=None)
            all_dfs.append(df)
        return pl.concat(all_dfs, how="diagonal")

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
        needles: list[str],
        haystacks: list[str],
        needle_norm_fn: Callable[[str], str] = lambda x: x.lower(),
        haystack_norm_fn: Callable[[str], str] = lambda x: x.lower(),
    ) -> list[str]:
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
    def get_hourly_load_historical(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get Hourly Load in the legacy wide format (before 2026-03-24).

        Deprecated: SPP changed the hourly load data format on 2026-03-24.
        Use get_hourly_load for data on or after 2026-03-24.

        Args:
            date: start date (must be before 2026-03-24)
            end: end date

        Returns:
            pl.DataFrame: Hourly Load in wide format
        """
        if date >= HOURLY_LOAD_WIDE_FORMAT_END_DATE:
            raise NotSupported(
                "SPP changed the hourly load data format on 2026-03-24. "
                "Use get_hourly_load for data on or after 2026-03-24.",
            )

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/hourly-load?path=/{date.strftime('%Y')}/DAILY_HOURLY_LOAD-{date.strftime('%Y%m%d')}.csv"  # noqa
        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)

        return self._process_hourly_load(df)

    @support_date_range("DAY_START")
    def get_hourly_load(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get Hourly Load in the long format (on or after 2026-03-24).

        Args:
            date: start date (must be on or after 2026-03-24)
            end: end date

        Returns:
            pl.DataFrame: Hourly Load with columns Time, Interval Start,
                Interval End, Balancing Area Name, Control Zone Name,
                Forecast Area Type, Load
        """
        if date in ["today", "latest"] or utils.is_today(
            date,
            tz=self.default_timezone,
        ):
            raise NoDataFoundException("Data is on at least a one day delay")

        if date < HOURLY_LOAD_WIDE_FORMAT_END_DATE:
            raise NoDataFoundException(
                "Data before 2026-03-24 uses the legacy wide format. "
                "Use get_hourly_load_historical for data before 2026-03-24.",
            )

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/hourly-load?path=/{date.strftime('%Y')}/DAILY_HOURLY_LOAD-{date.strftime('%Y%m%d')}.csv"  # noqa
        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)

        return self._process_hourly_load_long(df)

    def get_hourly_load_annual(self, year: int, verbose: bool = True) -> pl.DataFrame:
        """Get Hourly Load for a year. Starting 2011.
        For recent data use `get_hourly_load`

        Args:
            year: year to get data for
            verbose: print url

        Returns:
            pl.DataFrame: Hourly Load
        """
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/hourly-load?path=/{year}/{year}.zip"  # noqa
        df = utils.download_csvs_from_zip_url(
            url=url,
            verbose=verbose,
            strip_whitespace_from_cols=True,
        )

        return self._process_hourly_load(df)

    def _process_hourly_load(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process hourly load data in the legacy wide format.

        Deprecated: This method handles the wide format used before 2026-03-24.
        For data on or after 2026-03-24, use _process_hourly_load_long instead.
        """
        if "Market Hour" in df.columns:
            raise NotSupported(
                "SPP changed the hourly load data format on 2026-03-24 from wide "
                "to long. This method only supports the wide format used before "
                "2026-03-24. Use get_hourly_load with dates on or after 2026-03-24 "
                "for the new long format.",
            )

        df = _strip_columns(df)
        df = df.filter(~pl.all_horizontal(pl.all().is_null()))

        def clean_market_hour(val: str | None) -> str | None:
            if val is None:
                return val
            if val.endswith(":00"):
                return val
            return val + " 00:00"

        df = df.with_columns(
            pl.col("MarketHour")
            .map_elements(clean_market_hour, return_dtype=pl.Utf8)
            .alias("MarketHour"),
        )

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

        df = _ensure_columns(df, all_cols)
        df = df.filter(pl.col("Interval Start").is_not_null()).unique()
        df = _sum_across_cols(df, load_cols, "System Total")
        return df.sort("Time")

    def _process_hourly_load_long(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process hourly load data in the new long format (starting 2026-03-24).

        The new format has columns: Market Hour, Balancing Area Name,
        Control Zone Name, Forecast Area Type, Load MW.
        """
        df = _strip_columns(df)
        df = df.filter(~pl.all_horizontal(pl.all().is_null()))

        df = self._handle_market_end_to_interval(
            df,
            column="Market Hour",
            interval_duration=pd.Timedelta(minutes=60),
            format="mixed",
        )

        df = df.filter(pl.col("Interval Start").is_not_null()).unique()

        df = _rename_cols(df, {"Load MW": "Load"})

        col_order = [
            "Interval Start",
            "Interval End",
            "Balancing Area Name",
            "Control Zone Name",
            "Forecast Area Type",
            "Load",
        ]

        return df.select(col_order).sort(
            [
                "Interval Start",
                "Balancing Area Name",
                "Control Zone Name",
                "Forecast Area Type",
            ],
        )

    @support_date_range("DAY_START")
    def get_market_clearing_real_time(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get Market Clearing Real Time

        Args:
            date: start date
            end: end date

        Returns:
            pl.DataFrame: Market Clearing Real Time
        """
        if date == "latest":
            try:
                return self.get_market_clearing_real_time("today")
            except urllib.error.HTTPError:
                logger.info("Data not available for today, trying yesterday")
                return self.get_market_clearing_real_time(
                    self.local_now().normalize() - pd.DateOffset(days=1),
                    verbose=verbose,
                )

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/market-clearing-rtbm?path=/{date.strftime('%Y')}/{date.strftime('%m')}/RTBM-MC-{date.strftime('%Y%m%d')}.csv"  # noqa

        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)

        return self._process_market_clearing(df, 5)

    @support_date_range("DAY_START")
    def get_market_clearing_day_ahead(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get Market Clearing Day Ahead

        Args:
            date: start date
            end: end date

        Returns:
            pl.DataFrame: Market Clearing Day Ahead
        """
        if date == "latest":
            date = self.local_now().normalize() + pd.DateOffset(days=1)
            try:
                return self.get_market_clearing_day_ahead(date, verbose=verbose)
            except urllib.error.HTTPError:
                logger.info(
                    f"Data not available for {date.strftime('%Y-%m-%d')}, trying today",
                )
                return self.get_market_clearing_day_ahead("today", verbose=verbose)

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/market-clearing?path=/{date.strftime('%Y')}/{date.strftime('%m')}/DA-MC-{date.strftime('%Y%m%d')}0100.csv"  # noqa

        logger.info(f"Downloading {url}")
        df = pl.read_csv(url, infer_schema_length=None)

        return self._process_market_clearing(df, 60)

    def _process_market_clearing(
        self,
        df: pl.DataFrame,
        interval_minutes: int,
    ) -> pl.DataFrame:
        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=pd.Timedelta(minutes=interval_minutes),
        )

        df = _strip_columns(df)
        df = _rename_cols(
            df,
            {
                "RegUP": "Reg Up",
                "RegDN": "Reg Dn",
                "RampUP": "Ramp Up",
                "RampDN": "Ramp Dn",
                "UncUP": "Unc Up",
            },
        )
        return _drop_cols(df, ["Time", "Interval"]).sort("Interval Start")

    @support_date_range("DAY_START")
    def get_binding_constraints_day_ahead_hourly(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get Day-Ahead Binding Constraints

        Args:
            date: date to get data for. Supports "latest" for most recently available data.
            end: end date
            verbose: print url

        Returns:
            pl.DataFrame: Day-Ahead Binding Constraints
        """
        if date == "latest":
            tomorrow = pd.Timestamp.now(
                tz=self.default_timezone,
            ).normalize() + pd.Timedelta(days=1)
            try:
                return self.get_binding_constraints_day_ahead_hourly(
                    date=tomorrow,
                    end=end,
                    verbose=verbose,
                )
            except NoDataFoundException:
                return self.get_binding_constraints_day_ahead_hourly(date="today")

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/{DA_BINDING_CONSTRAINTS}?path=/{date.strftime('%Y')}/{date.strftime('%m')}/By_Day/DA-BC-{date.strftime('%Y%m%d')}0100.csv"  # noqa
        return self._process_binding_constraints_day_ahead_hourly(url)

    def _process_binding_constraints_day_ahead_hourly(self, url: str) -> pl.DataFrame:
        logger.info(f"Downloading {url}...")
        try:
            df = pl.read_csv(url, infer_schema_length=None)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise NoDataFoundException(f"No data found for {url}")
            raise

        df = _strip_columns(df)

        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=pd.Timedelta(hours=1),
        )

        df = _rename_cols(df, {"NERCID": "NERC ID"})
        df = df.with_columns(
            pl.col("NERC ID")
            .cast(pl.Float64, strict=False)
            .cast(pl.Int64, strict=False)
            .alias("NERC ID"),
        )

        cols_to_keep = [
            "Interval Start",
            "Interval End",
            "Constraint Name",
            "Constraint Type",
            "NERC ID",
            "State",
            "Shadow Price",
            "Monitored Facility",
            "Contingent Facility",
            "Contingency Name",
        ]

        return df.select(cols_to_keep).sort(["Interval Start", "Constraint Name"])

    # NB: SPP RTBM publishes per-interval CSVs and daily aggregates. Both are
    # aligned to local midnight. Interval files live under
    # By_Interval/<DD>/RTBM-BC-<YYYYMMDDHHMM>.csv where DD is the interval's
    # start-day and HHMM is the interval's end-time; the midnight-crossing
    # interval (start = prev day 23:55, end = next day 00:00) sits in the
    # previous day's folder with the next day's filename. Daily files are
    # faster (one request per day) but lag publication, so we use interval
    # files for today/yesterday and daily files for older history. Daily files
    # always return the whole local day; this method slices the result to the
    # caller's [date, end) window.
    def get_binding_constraints_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get Real-Time Binding Constraints

        Args:
            date: date to get data for. Supports "latest" for most recent interval.
            end: end date
            verbose: print url

        Returns:
            pl.DataFrame: Real-Time Binding Constraints
        """
        if date == "latest":
            return self._get_binding_constraints_real_time_5_min_from_intervals(
                date,
                end=end,
                verbose=verbose,
            )

        start_ts = utils._handle_date(date, self.default_timezone)
        end_ts = (
            utils._handle_date(end, self.default_timezone) if end is not None else None
        )

        df = self._fetch_binding_constraints_real_time_5_min(
            date=start_ts,
            end=end_ts,
            verbose=verbose,
        )

        if end_ts is None:
            return df

        return df.filter(
            (pl.col("Interval Start") >= pl.lit(start_ts))
            & (pl.col("Interval Start") < pl.lit(end_ts)),
        )

    @support_date_range(_binding_constraints_real_time_5_min_frequency)
    def _fetch_binding_constraints_real_time_5_min(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Per-chunk fetch dispatching to interval or daily-file source."""
        start = utils._handle_date(date, self.default_timezone)
        if not utils.is_within_last_days(start, 1, self.default_timezone):
            return self._get_binding_constraints_real_time_5_min_from_daily_files(
                start,
                end=end,
                verbose=verbose,
            )

        interval_start, interval_end = self._resolve_recent_interval_window(
            start,
            end,
        )
        return self._get_binding_constraints_real_time_5_min_from_intervals(
            interval_start,
            end=interval_end,
            verbose=verbose,
        )

    def _resolve_recent_interval_window(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp | None,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Window for today/yesterday interval fetches.

        When end is omitted (DAY_START chunk), fetch through end-of-local-day,
        capping today at the most recent 5-min boundary so we don't request
        future intervals.
        """
        if end is None:
            end = start.normalize() + pd.Timedelta(days=1)
            if utils.is_today(start, self.default_timezone):
                end = min(
                    end,
                    pd.Timestamp.now(tz=self.default_timezone).ceil("5min"),
                )

        if end <= start:
            end = start + pd.Timedelta(minutes=5)

        return start, end

    def _get_binding_constraints_real_time_5_min_from_daily_files(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get Real-Time Binding Constraints from daily files."""

        folder_year = date.strftime("%Y")
        folder_month = date.strftime("%m")

        url = f"{FILE_BROWSER_DOWNLOAD_URL}/{RTBM_BINDING_CONSTRAINTS}?path=/{folder_year}/{folder_month}/By_Day/RTBM-DAILY-BC-{date.strftime('%Y%m%d')}.csv"

        logger.info(f"Downloading {url} (daily file)...")
        df = pl.read_csv(url, infer_schema_length=None)
        return self._process_binding_constraints_real_time(_strip_columns(df))

    @support_date_range("5_MIN")
    def _get_binding_constraints_real_time_5_min_from_intervals(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get Real-Time Binding Constraints from 5-minute interval files."""
        if date == "latest":
            url = f"{FILE_BROWSER_DOWNLOAD_URL}/{RTBM_BINDING_CONSTRAINTS}?path=/RTBM-BC-latestInterval.csv"  # noqa
        else:
            url = self._format_5_min_url(
                date,
                end,
                RTBM_BINDING_CONSTRAINTS,
                "RTBM-BC",
            )

        logger.info(f"Downloading {url}...")
        df = pl.read_csv(url, infer_schema_length=None)
        return self._process_binding_constraints_real_time(df)

    def _process_binding_constraints_real_time(self, df: pl.DataFrame) -> pl.DataFrame:
        df = _strip_columns(df)
        df = df.rename(
            {c: c.strip().title().replace("_", " ") for c in df.columns},
        )
        df = _rename_cols(
            df,
            {
                "Gmtintervalend": "GMTIntervalEnd",
                "Nercid": "NERC ID",
                "Tlr Level": "TLR Level",
            },
        )

        df = self._handle_market_end_to_interval(
            df,
            column="GMTIntervalEnd",
            interval_duration=pd.Timedelta(minutes=5),
        )

        df = df.with_columns(
            pl.col("NERC ID")
            .cast(pl.Float64, strict=False)
            .cast(pl.Int64, strict=False)
            .alias("NERC ID"),
        )

        cols_to_keep = [
            "Interval Start",
            "Interval End",
            "Constraint Name",
            "Constraint Type",
            "NERC ID",
            "TLR Level",
            "State",
            "Shadow Price",
            "Monitored Facility",
            "Contingent Facility",
        ]

        return df.select(cols_to_keep).sort(["Interval Start", "Constraint Name"])

    @support_date_range("MONTH_START")
    def get_interchange_real_time(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get real-time interchange (tie flow) data.

        For "latest" and "today", returns ~2 days of 1-minute interchange data
        from the real-time endpoint.

        For historical dates, downloads monthly CSV files from the historical
        tie flow archive.

        Data from:
        - Real-time: https://portal.spp.org/pages/integrated-marketplace-interchange-trend
        - Historical: https://portal.spp.org/pages/historical-tie-flow

        Args:
            date: supports "latest", "today", or a historical date/date range
            end: end date for historical range queries
            verbose: print info

        Returns:
            pl.DataFrame: interchange data
        """
        if date == "latest":
            return self.get_interchange_real_time(
                "today",
                verbose=verbose,
            )

        if isinstance(date, tuple):
            start = date[0]
        else:
            start = utils._handle_date(date, tz=self.default_timezone)

        if utils.is_within_last_days(start, days=2, tz=self.default_timezone):
            url = f"{MARKETPLACE_BASE_URL}/chart-api/interchange-trend/asFile"
            logger.info(f"Downloading {url}")
            df = pl.read_csv(url, infer_schema_length=None)
            return self._process_interchange_real_time(df)

        # Historical data: download monthly CSV
        month_str = start.strftime("%b%Y")  # e.g., "Apr2015"
        # Starting March 2026, files use "TieFlows_SPP_" prefix
        if start >= pd.Timestamp("2026-03-01"):
            filename = f"TieFlows_SPP_{month_str}.csv"
        else:
            filename = f"TieFlows_{month_str}.csv"
        url = f"{FILE_BROWSER_DOWNLOAD_URL}/historical-tie-flow?path=/{filename}"

        logger.info(f"Downloading {url}")
        try:
            df = pl.read_csv(url, infer_schema_length=None)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise NoDataFoundException(
                    f"No historical tie flow data found for {month_str}. "
                    f"Historical data is available starting Mar2014.",
                )
            raise

        # Normalize historical column names to match real-time format
        df = _rename_cols(
            df,
            {
                "GMTTIME": "GMTTime",
                "SPP_NSI": "SPP NSI",
                "SPP_NAI": "SPP NAI",
            },
        )

        return self._process_interchange_real_time(df)

    @support_date_range("MONTH_START")
    def get_west_interchange_real_time(
        self,
        date: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp],
        end: str | pd.Timestamp | tuple[pd.Timestamp, pd.Timestamp] | None = None,
        verbose: bool = False,
    ) -> pl.DataFrame:
        """Get real-time interchange (tie flow) data for SPP West (SWPW).

        For "latest" and "today", returns ~2 days of 1-minute interchange data
        from the real-time endpoint.

        For historical dates, downloads monthly CSV files from the historical
        tie flow archive.

        Data from:
        - Real-time: https://portal.spp.org/pages/integrated-marketplace-interchange-trend
        - Historical: https://portal.spp.org/pages/historical-tie-flow

        Args:
            date: supports "latest", "today", or a historical date/date range
            end: end date for historical range queries
            verbose: print info

        Returns:
            pl.DataFrame: interchange data
        """
        if date == "latest":
            return self.get_west_interchange_real_time(
                "today",
                verbose=verbose,
            )

        if isinstance(date, tuple):
            start = date[0]
        else:
            start = utils._handle_date(date, tz=self.default_timezone)

        if utils.is_within_last_days(start, days=2, tz=self.default_timezone):
            url = f"{MARKETPLACE_BASE_URL}/chart-api/interchange-trend-swpw/asFile"
            logger.info(f"Downloading {url}")
            df = pl.read_csv(url, infer_schema_length=None)
            return self._process_interchange_real_time(df)

        # Historical data: download monthly CSV
        month_str = start.strftime("%b%Y")  # e.g., "Apr2026"
        url = (
            f"{FILE_BROWSER_DOWNLOAD_URL}/historical-tie-flow"
            f"?path=/TieFlows_SWPW_{month_str}.csv"
        )

        logger.info(f"Downloading {url}")
        try:
            df = pl.read_csv(url, infer_schema_length=None)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise NoDataFoundException(
                    f"No historical SWPW tie flow data found for {month_str}. "
                    f"Historical data is available starting Mar2026.",
                )
            raise

        # Normalize historical column names to match real-time format
        df = _rename_cols(
            df,
            {
                "GMTTIME": "GMTTime",
                "SPP_NSI": "SWPW NSI",
                "SPP_NAI": "SWPW NAI",
            },
        )

        return self._process_interchange_real_time(df)

    def _process_interchange_real_time(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            _parse_utc_to_local(df, "GMTTime", self.default_timezone).alias("Time"),
        )
        df = _drop_cols(df, ["GMTTime"])
        df = df.filter(pl.col("Time").is_not_null())

        data_cols = [c for c in df.columns if c != "Time"]
        df = df.filter(
            pl.any_horizontal([pl.col(c).is_not_null() for c in data_cols]),
        )

        df = df.unpivot(
            index=["Time"],
            on=data_cols,
            variable_name="Region",
            value_name="Interchange",
        )

        return df.filter(pl.col("Interchange").is_not_null()).sort(["Time", "Region"])


def process_gen_mix(df: pl.DataFrame, detailed: bool = False) -> pl.DataFrame:
    """Parse SPP generation mix data from
    https://marketplace.spp.org/pages/generation-mix-historical

    Args:
        df (pl.DataFrame): raw data
        detailed (bool): whether to combine market and self columns

    Returns:
        pl.DataFrame: processed data
    """
    new_df = _strip_columns(df)

    new_df = _rename_cols(
        new_df,
        {
            "GMTTime": "Time",
            "GMT MKT Interval": "Time",
            "Gas Self": "Natural Gas Self",
            "Load": "Short Term Load Forecast",
        },
    )

    new_df = new_df.with_columns(
        _parse_utc_to_local(new_df, "Time", SPP.default_timezone).alias("Time"),
    )

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

            new_df = new_df.with_columns(
                (
                    pl.col(market_col).cast(pl.Float64, strict=False).fill_null(0)
                    + pl.col(self_col).cast(pl.Float64, strict=False).fill_null(0)
                ).alias(col),
            ).drop([market_col, self_col])

    return add_interval(new_df, 5)


def add_interval(df: pl.DataFrame, interval_min: int) -> pl.DataFrame:
    """Adds Interval Start and Interval End columns to df"""
    duration = pl.duration(minutes=interval_min)
    df = df.with_columns(
        pl.col("Time").alias("Interval Start"),
        (pl.col("Time") + duration).alias("Interval End"),
    )

    return utils.move_cols_to_front(df, ["Time", "Interval Start", "Interval End"])

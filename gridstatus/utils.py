import glob
import io
import os
from typing import Callable
from zipfile import ZipFile

import pandas as pd
import polars as pl
import requests
import tqdm

import gridstatus
from gridstatus.base import ISOBase, NotSupported, _interconnection_columns
from gridstatus.caiso import CAISO
from gridstatus.ercot import Ercot
from gridstatus.gs_logging import log
from gridstatus.ieso import IESO
from gridstatus.isone import ISONE
from gridstatus.lmp_config import lmp_config
from gridstatus.miso import MISO
from gridstatus.nyiso import NYISO
from gridstatus.pjm import PJM
from gridstatus.spp import SPP

GREEN_CHECKMARK_HTML_ENTITY: str = "&#x2705;"

RED_X_HTML_ENTITY: str = "&#10060;"
all_isos: list[ISOBase] = [MISO, CAISO, PJM, Ercot, SPP, NYISO, ISONE, IESO]


def is_polars(obj: object) -> bool:
    """Return whether ``obj`` is a polars DataFrame."""
    return isinstance(obj, pl.DataFrame)


def read_html_via_pandas(
    io: object,
    process: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    **kwargs,
) -> list[pl.DataFrame]:
    """Parse HTML tables with ``pandas.read_html`` and return polars DataFrames.

    pandas is retained only for IO parse edges that have no polars equivalent
    (HTML tables, Excel workbooks, exotic CSVs); the parsed frames are converted
    to polars immediately at the boundary. ``process`` runs on each pandas frame
    before conversion, for pandas-specific fixups like flattening MultiIndex
    columns.
    """
    tables = pd.read_html(io, **kwargs)
    if process is not None:
        tables = [process(t) for t in tables]
    return [pl.from_pandas(t) for t in tables]


def read_excel_via_pandas(
    io: object,
    process: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    **kwargs,
) -> pl.DataFrame | dict[str, pl.DataFrame]:
    """Parse an Excel workbook with ``pandas.read_excel`` and return polars.

    One of the sanctioned pandas IO edges (see ``read_html_via_pandas``).
    Returns a dict of frames when ``sheet_name=None`` or a list of sheets is
    requested, mirroring pandas. ``process`` runs on each pandas frame before
    conversion, for pandas-specific fixups like flattening MultiIndex columns.
    """
    result = pd.read_excel(io, **kwargs)
    if isinstance(result, dict):
        if process is not None:
            result = {k: process(v) for k, v in result.items()}
        return {k: pl.from_pandas(v) for k, v in result.items()}
    if process is not None:
        result = process(result)
    return pl.from_pandas(result)


def read_csv_exotic_via_pandas(
    io: object,
    process: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    **kwargs,
) -> pl.DataFrame:
    """Parse a CSV with ``pandas.read_csv`` and return a polars DataFrame.

    One of the sanctioned pandas IO edges (see ``read_html_via_pandas``), for
    CSVs that polars cannot read directly: ``skipfooter``, ``engine="python"``,
    multi-row headers, etc. Simple CSVs should use ``pl.read_csv`` instead.
    ``process`` runs on the pandas frame before conversion.
    """
    df = pd.read_csv(io, **kwargs)
    if process is not None:
        df = process(df)
    return pl.from_pandas(df)


def concat_dataframes(dfs: list) -> object:
    """Concatenate a list of frames, dispatching on pandas vs polars.

    Used by ``support_date_range`` to combine per-chunk results regardless of
    whether the decorated method produced pandas or polars frames.
    """
    if not dfs:
        return pl.DataFrame()

    if is_polars(dfs[0]):
        return pl.concat(dfs, how="diagonal_relaxed")

    return pd.concat(dfs).reset_index(drop=True)


def list_isos() -> pl.DataFrame:
    """List available ISOs"""

    isos = [[i.name, i.iso_id, i.__name__] for i in all_isos]

    return pl.DataFrame(
        isos,
        schema=["Name", "Id", "Class"],
        orient="row",
    )


def get_iso(iso_id: str) -> ISOBase:
    """Get an ISO by its id"""
    for i in all_isos:
        if i.iso_id == iso_id:
            return i

    raise KeyError


def _df_to_markdown(df: pl.DataFrame) -> str:
    """Render a polars DataFrame as a GitHub-flavored markdown table."""
    headers = df.columns
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
    ]
    for row in df.iter_rows():
        lines.append(
            "| " + " | ".join("" if v is None else str(v) for v in row) + " |",
        )
    return "\n".join(lines) + "\n"


def make_availability_df() -> dict[str, pl.DataFrame]:
    methods = [
        "get_status",
        "get_fuel_mix",
        "get_load",
        "get_load_forecast",
        "get_storage",
    ]

    availability = {}
    for i in tqdm.tqdm(gridstatus.all_isos):
        # TODO: Skipping AESO as it's missing some methods
        if i.__name__ == "AESO":
            continue
        availability[i.__name__] = {}
        for method in methods:
            availability[i.__name__][method] = {}
            for date in ["latest", "today", "historical"]:
                test = date
                if date == "historical":
                    test = pd.Timestamp.now(
                        tz=i.default_timezone,
                    ).date() - pd.Timedelta(days=3)

                if method == "get_load_forecast" and date == "latest":
                    is_defined = RED_X_HTML_ENTITY

                else:
                    try:
                        getattr(i(), method)(test)
                        is_defined = GREEN_CHECKMARK_HTML_ENTITY
                    except NotSupported:
                        is_defined = RED_X_HTML_ENTITY
                    except NotImplementedError:
                        is_defined = RED_X_HTML_ENTITY

                availability[i.__name__][method][date] = is_defined

    dates = ["latest", "today", "historical"]
    availability_dfs = {}
    for i in all_isos:
        if i.__name__ not in availability:
            continue
        data: dict[str, list[str]] = {"": dates}
        for method in methods:
            data[method] = [availability[i.__name__][method][d] for d in dates]
        availability_dfs[i.__name__] = pl.DataFrame(data)

    return availability_dfs


def make_availability_table() -> str:
    dfs = make_availability_df()

    markdown = ""
    for method, df in sorted(dfs.items()):
        markdown += "## " + method + "\n"
        # df.index = ["`" + v + "`" for v in df.index.values]
        markdown += _df_to_markdown(df) + "\n"

    return markdown


def _handle_date(
    date: str | pd.Timestamp | None,
    tz: str | None = None,
) -> pd.Timestamp | None:
    if date is None:
        return date

    if date == "today":
        date = pd.Timestamp.now(tz=tz).normalize()

    if not isinstance(date, pd.Timestamp):
        date = pd.to_datetime(date)

    if tz:
        if date.tzinfo is None:
            date = date.tz_localize(tz)
        else:
            # todo see if this triggers in tests
            date = date.tz_convert(tz)

    return date


LMP_METHOD_NAMES: list[str] = ["get_lmp", "get_spp"]


def make_lmp_availability_df() -> pl.DataFrame:
    availability = {}
    DOES_NOT_EXIST_SENTINEL = "dne"
    for iso in tqdm.tqdm(gridstatus.all_isos):
        availability[iso.__name__] = {"Method": "-"}
        matching_method_name = None
        for method_name in LMP_METHOD_NAMES:
            if (
                getattr(iso(), method_name, DOES_NOT_EXIST_SENTINEL)
                != DOES_NOT_EXIST_SENTINEL
            ):
                matching_method_name = method_name
                break
        if matching_method_name is None:
            continue
        availability[iso.__name__]["Method"] = f"`{matching_method_name}`"
        matching_method = getattr(iso(), matching_method_name)
        config = lmp_config.get_support(matching_method)
        for market, supported_dates in config.items():
            availability[iso.__name__][market.name] = ", ".join(
                supported_dates,
            )

    market_cols: list[str] = []
    for iso_availability in availability.values():
        for col in iso_availability:
            if col != "Method" and col not in market_cols:
                market_cols.append(col)

    columns = ["Method", *market_cols]
    rows = [
        {"": iso_name, **{c: iso_availability.get(c, "-") for c in columns}}
        for iso_name, iso_availability in availability.items()
    ]
    return pl.DataFrame(rows)


def convert_bool_to_emoji(value: bool) -> str:
    """If value is boolean, convert to Green Checkmark or Red X. Otherwise, leave be."""
    if isinstance(value, bool):
        if value:
            return GREEN_CHECKMARK_HTML_ENTITY
        else:
            return RED_X_HTML_ENTITY
    else:
        return value


def make_lmp_availability_table() -> str:
    df = make_lmp_availability_df().sort("")
    df = df.with_columns(
        pl.col(c).map_elements(convert_bool_to_emoji, return_dtype=pl.String)
        for c in df.columns
        if c != ""
    )
    return _df_to_markdown(df)


# todo require locations and location_type arguments


def filter_lmp_locations(
    df: pd.DataFrame,
    locations: list[str] | None = None,
    location_type: str | None = None,
) -> pd.DataFrame:
    """
    Filters DataFrame by locations, which can be a list, "ALL" or None

    Arguments:
        df (pandas.DataFrame): DataFrame to filter
        locations: "ALL" or list of locations to filter "Location" column by
    """
    if is_polars(df):
        if location_type != "ALL" and location_type is not None:
            if isinstance(location_type, str):
                location_type = [location_type]
            df = df.filter(pl.col("Location Type").is_in(location_type))

        if locations != "ALL" and locations is not None:
            df = df.filter(pl.col("Location").is_in(locations))

        return df

    if location_type != "ALL" and location_type is not None:
        if isinstance(location_type, str):
            location_type = [location_type]

        df = df[df["Location Type"].isin(location_type)]

    if locations != "ALL" and locations is not None:
        df = df[df["Location"].isin(locations)]

    return df


def get_zip_file(url: str, verbose: bool = False) -> ZipFile:
    z = get_zip_folder(url, verbose=verbose)
    return z.open(z.namelist()[0])


def get_zip_folder(url: str, verbose: bool = False, **kwargs) -> ZipFile:
    msg = f"Requesting {url}"
    log(msg, verbose)
    r = requests.get(url, **kwargs)
    z = ZipFile(io.BytesIO(r.content))
    return z


def get_response_blob(resp: requests.Response) -> io.BytesIO:
    if resp.status_code != 200:
        raise RuntimeError(f"{resp.request.method} {resp.request.url} failed: {resp}")
    return io.BytesIO(resp.content)


def download_csvs_from_zip_url(
    url: str,
    process_csv: Callable[[pl.DataFrame, str], pl.DataFrame] | None = None,
    verbose: bool = False,
    strip_whitespace_from_cols: bool = False,
) -> pl.DataFrame:
    z = get_zip_folder(url, verbose=verbose)

    all_dfs = []

    for f in z.filelist:
        if f.filename.endswith(".csv"):
            df = pl.read_csv(z.open(f.filename).read(), infer_schema_length=None)
            if process_csv:
                df = process_csv(df, f.filename)

            if strip_whitespace_from_cols:
                # Some data files have leading whitespace in header - remove it
                df = df.rename({c: c.strip() for c in df.columns})

            all_dfs.append(df)

    df = pl.concat(all_dfs, how="diagonal")

    return df


def is_today(date: str | pd.Timestamp, tz: str) -> bool:
    return _handle_date(date, tz=tz).date() == pd.Timestamp.now(tz=tz).date()


def is_yesterday(date: pd.Timestamp, tz: str) -> bool:
    return _handle_date(date, tz=tz).date() == (
        pd.Timestamp.now(tz=tz).date() - pd.Timedelta(days=1)
    )


def is_within_last_days(date: pd.Timestamp, days: int, tz: str) -> bool:
    """Returns whether date is within N days"""
    now = pd.Timestamp.now(tz=tz).date()
    date_value = _handle_date(date, tz=tz).date()
    period_start = (now - pd.DateOffset(days=days)).date()
    return date_value <= now and date_value >= period_start


def format_interconnection_df(
    queue: pd.DataFrame,
    rename: dict[str, str],
    extra: list[str] | None = None,
    missing: list[str] | None = None,
) -> pd.DataFrame:
    """Format interconnection queue data"""
    assert set(rename.keys()).issubset(queue.columns), set(
        rename.keys(),
    ) - set(queue.columns)

    if is_polars(queue):
        queue = queue.rename(rename)
        columns = _interconnection_columns.copy()

        if extra:
            for e in extra:
                assert e in queue.columns, f"Extra column {e} does not exist"
            columns += extra

        if missing:
            for m in missing:
                assert m not in queue.columns, "Missing column already exists"
                queue = queue.with_columns(pl.lit(None).alias(m))

        return queue.select(columns)

    queue = queue.rename(columns=rename)
    columns = _interconnection_columns.copy()

    if extra:
        for e in extra:
            assert e in queue.columns, f"Extra column {e} does not exist"

        columns += extra

    if missing:
        for m in missing:
            assert m not in queue.columns, "Missing column already exists"
            queue[m] = None

    return queue[columns].reset_index(drop=True)


def is_dst_end(date: pd.Timestamp) -> bool:
    return (date.dst() - (date + pd.DateOffset(1)).dst()).seconds == 3600


def load_folder(
    path: str,
    time_zone: str | None = None,
    verbose: bool = True,
) -> pl.DataFrame:
    """Load a single DataFrame for same schema csv files in a folder

    Arguments:
        path (str): path to folder
        time_zone (str): time zone to localize to timestamps.
            By default returns as UTC
        verbose (bool, optional): print verbose output. Defaults to True.

    Returns:
        polars.DataFrame: A DataFrame of all files
    """
    all_files = glob.glob(os.path.join(path, "*.csv"))
    all_files = sorted(all_files)

    dfs = []
    for f in tqdm.tqdm(all_files, disable=not verbose):
        df = pl.read_csv(f, infer_schema_length=None)
        dfs.append(df)

    data = pl.concat(dfs, how="diagonal")

    for time_col in ["Time", "Interval Start", "Interval End"]:
        if time_col in data.columns:
            data = data.with_columns(
                pl.col(time_col)
                .str.to_datetime(time_zone="UTC")
                .dt.convert_time_zone(time_zone or "UTC"),
            )

    # todo make sure dates get parsed
    # todo make sure rows are sorted by time

    return data


def get_interconnection_queues() -> pl.DataFrame:
    """Get interconnection queue data for all ISOs"""
    all_queues = []
    for iso in tqdm.tqdm(all_isos):
        iso = iso()
        # only shared columns
        # add error handling for IESO

        try:
            queue = iso.get_interconnection_queue().select(_interconnection_columns)
        except NotImplementedError:
            queue = pl.DataFrame()

        queue = queue.with_columns(pl.lit(iso.name).alias("ISO"))
        queue = move_cols_to_front(queue, ["ISO"])
        all_queues.append(queue)

    all_queues = pl.concat(all_queues, how="diagonal_relaxed")
    return all_queues


def move_cols_to_front(df: pd.DataFrame, cols_to_move: list[str]) -> pd.DataFrame:
    """Move columns to front of DataFrame"""
    if is_polars(df):
        rest = [c for c in df.columns if c not in cols_to_move]
        return df.select(cols_to_move + rest)

    cols = list(df.columns)
    for c in cols_to_move:
        cols.remove(c)
    return df[cols_to_move + cols]


def localize_ambiguous_infer_polars(
    df: object,
    time_col: str,
    tz: str,
    group_cols: list[str] | None = None,
) -> object:
    """Localize a naive polars datetime column, mimicking pandas ``ambiguous="infer"``.

    pandas infers ambiguous (fall-back DST) timestamps by assuming the values
    are monotonic increasing within each group: the first occurrence of a
    repeated wall-clock time is the pre-transition (earliest) offset and later
    occurrences are post-transition (latest). polars has no "infer" option, so
    we reproduce it by ranking duplicate naive timestamps within each group and
    passing per-row "earliest"/"latest" to ``replace_time_zone``. The value is
    ignored for non-ambiguous rows.
    """
    sort_cols = [*group_cols, time_col] if group_cols else [time_col]
    over_cols = [*group_cols, time_col] if group_cols else [time_col]

    df = df.sort(sort_cols)
    df = df.with_columns(
        pl.int_range(pl.len()).over(over_cols).alias("_dup_rank"),
    )
    df = df.with_columns(
        pl.when(pl.col("_dup_rank") > 0)
        .then(pl.lit("latest"))
        .otherwise(pl.lit("earliest"))
        .alias("_ambiguous"),
    )
    df = df.with_columns(
        pl.col(time_col).dt.replace_time_zone(tz, ambiguous=pl.col("_ambiguous")),
    )
    return df.drop(["_dup_rank", "_ambiguous"])


def localize_shift_forward_polars(
    df: pl.DataFrame,
    time_col: str,
    tz: str,
    ambiguous: str = "earliest",
) -> pl.DataFrame:
    """Localize a naive polars datetime column, mimicking pandas
    ``nonexistent="shift_forward"``.

    polars has no shift_forward option for spring-forward gaps, so nonexistent
    wall-clock times are localized to null and replaced by the same wall time
    plus one hour (the first valid instant after the gap), matching pandas.
    Used by the ERCOT/PJM patterns that combine ``ambiguous`` handling with
    ``nonexistent="shift_forward"``.

    Note: pandas ``merge_asof`` equivalents should use ``pl.DataFrame.join_asof``
    (AESO pattern).
    """
    localized = pl.col(time_col).dt.replace_time_zone(
        tz,
        ambiguous=ambiguous,
        non_existent="null",
    )
    shifted = (pl.col(time_col) + pl.duration(hours=1)).dt.replace_time_zone(
        tz,
        ambiguous=ambiguous,
        non_existent="null",
    )
    return df.with_columns(pl.coalesce(localized, shifted).alias(time_col))


_ISONE_DATE_FORMAT = "%m/%d/%Y"


def create_interval_start_from_hour_start_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Build naive Interval Start from ISONE Date and Hour Ending columns."""
    return df.with_columns(
        pl.col("Hour Ending")
        .cast(pl.Utf8)
        .str.replace("02X", "02")
        .cast(pl.Int64)
        .sub(1)
        .alias("Hour Start"),
    ).with_columns(
        (
            pl.col("Date").str.to_datetime(format=_ISONE_DATE_FORMAT, strict=False)
            + pl.duration(hours=pl.col("Hour Start"))
        ).alias("Interval Start"),
    )


def localize_interval_start_polars(
    df: pl.DataFrame,
    time_col: str,
    tz: str,
    group_cols: list[str] | None = None,
    date_col: str | None = None,
    hour_start_col: str = "Hour Start",
) -> pl.DataFrame:
    """Localize naive interval starts, handling ISONE DST fall-back and spring-forward."""
    sort_cols = [*group_cols, time_col] if group_cols else [time_col]
    over_cols = [*group_cols, time_col] if group_cols else [time_col]

    df = df.sort(sort_cols)
    df = df.with_columns(
        pl.int_range(pl.len()).over(over_cols).alias("_dup_rank"),
    )
    df = df.with_columns(
        pl.when(pl.col("_dup_rank") > 0)
        .then(pl.lit("latest"))
        .otherwise(pl.lit("earliest"))
        .alias("_ambiguous"),
    )
    df = df.with_columns(
        pl.col(time_col).dt.replace_time_zone(
            tz,
            ambiguous=pl.col("_ambiguous"),
            non_existent="null",
        ),
    )
    df = df.drop(["_dup_rank", "_ambiguous"])

    if date_col is None or df.filter(pl.col(time_col).is_null()).height == 0:
        return df

    fixed = (
        df.filter(pl.col(time_col).is_null())
        .with_columns(
            (
                pl.col(date_col).str.to_datetime(
                    format=_ISONE_DATE_FORMAT,
                    strict=False,
                )
                + pl.duration(hours=pl.col(hour_start_col) - 1)
            ).alias(time_col),
        )
        .with_columns(
            pl.col(time_col).dt.replace_time_zone(tz),
        )
    )

    good = df.filter(pl.col(time_col).is_not_null())
    return pl.concat([good, fixed], how="diagonal")

import glob
import io
import os
from zipfile import ZipFile

import pandas as pd
import requests
import tqdm

import gridstatus
from gridstatus.base import Markets, NotSupported, _interconnection_columns
from gridstatus.caiso import CAISO
from gridstatus.ercot import Ercot
from gridstatus.isone import ISONE
from gridstatus.miso import MISO
from gridstatus.nyiso import NYISO
from gridstatus.pjm import PJM
from gridstatus.spp import SPP

GREEN_CHECKMARK_HTML_ENTITY = "&#x2705;"

RED_X_HTML_ENTITY = "&#10060;"

all_isos = [MISO, CAISO, PJM, Ercot, SPP, NYISO, ISONE]


def list_isos():
    """List available ISOs"""

    isos = [[i.name, i.iso_id, i.__name__] for i in all_isos]

    return pd.DataFrame(isos, columns=["Name", "Id", "Class"])


def get_iso(iso_id):
    """Get an ISO by its id"""
    for i in all_isos:
        if i.iso_id == iso_id:
            return i

    raise KeyError


def make_availability_df():
    methods = [
        "get_status",
        "get_fuel_mix",
        "get_load",
        "get_load_forecast",
        "get_storage",
    ]

    availability = {}
    for i in tqdm.tqdm(gridstatus.all_isos):
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

    availability_dfs = {}
    for i in all_isos:
        availability_dfs[i.__name__] = pd.DataFrame(availability[i.__name__])

    return availability_dfs


def make_availability_table():
    dfs = make_availability_df()

    markdown = ""
    for method, df in sorted(dfs.items()):
        markdown += "## " + method + "\n"
        # df.index = ["`" + v + "`" for v in df.index.values]
        markdown += df.to_markdown() + "\n"

    return markdown


def _handle_date(date, tz=None):
    if date == "today":
        date = pd.Timestamp.now(tz=tz)

    if not isinstance(date, pd.Timestamp):
        date = pd.to_datetime(date)

    if tz and date.tzinfo is None:
        date = date.tz_localize(tz)

    return date


LMP_METHODS = ["get_lmp", "get_spp"]


def make_lmp_availability_df():
    markets = [
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    ]
    availability = {}
    DOES_NOT_EXIST_SENTINEL = "dne"
    for iso in tqdm.tqdm(gridstatus.all_isos):
        availability[iso.__name__] = {"Method": "-"}
        for method in LMP_METHODS:
            if (
                getattr(iso(), method, DOES_NOT_EXIST_SENTINEL)
                != DOES_NOT_EXIST_SENTINEL
            ):
                availability[iso.__name__]["Method"] = f"`{method}`"
                break
        for market in markets:
            iso_markets = getattr(iso, "markets")
            availability[iso.__name__][market] = market in iso_markets

    return pd.DataFrame(availability)


def convert_bool_to_emoji(value):
    """If value is boolean, convert to Green Checkmark or Red X. Otherwise, leave be."""
    if isinstance(value, bool):
        if value:
            return GREEN_CHECKMARK_HTML_ENTITY
        else:
            return RED_X_HTML_ENTITY
    else:
        return value


def make_lmp_availability_table():
    transposed = make_lmp_availability_df().transpose()
    transposed = transposed.rename(
        columns={
            Markets.REAL_TIME_5_MIN: "REAL_TIME_5_MIN",
            Markets.REAL_TIME_15_MIN: "REAL_TIME_15_MIN",
            Markets.REAL_TIME_HOURLY: "REAL_TIME_HOURLY",
            Markets.DAY_AHEAD_HOURLY: "DAY_AHEAD_HOURLY",
        },
    )

    transposed = transposed.sort_index().applymap(convert_bool_to_emoji)

    return transposed.to_markdown() + "\n"


def filter_lmp_locations(df, locations):
    """
    Filters dataframe by locations, which can be a list, "ALL" or None

    Parameters:
        df: pd.DataFrame
        locations: "ALL" or list of locations to filter "Location" column by
    """
    if locations == "ALL" or locations is None:
        return df

    return df[df["Location"].isin(locations)]


def get_zip_file(url):
    # todo add retry logic
    # todo does this need to be a with statement?
    r = requests.get(url)
    z = ZipFile(io.BytesIO(r.content))
    return z.open(z.namelist()[0])


def is_today(date, tz=None):
    return _handle_date(date, tz=tz).date() == pd.Timestamp.now(tz=tz).date()


def is_within_last_days(date, days, tz=None):
    """Returns whether date is within N days"""
    now = pd.Timestamp.now(tz=tz).date()
    date_value = _handle_date(date, tz=tz).date()
    period_start = (now - pd.DateOffset(days=days)).date()
    return date_value <= now and date_value >= period_start


def format_interconnection_df(queue, rename, extra=None, missing=None):
    """Format interconnection queue data"""
    assert set(rename.keys()).issubset(queue.columns), set(
        rename.keys(),
    ) - set(queue.columns)
    queue = queue.rename(columns=rename)
    columns = _interconnection_columns.copy()

    if extra:
        columns += extra

    if missing:
        for m in missing:
            assert m not in queue.columns, "Missing column already exists"
            queue[m] = None

    return queue[columns].reset_index(drop=True)


def get_interconnection_queues():
    """Get interconnection queue data for all ISOs"""
    all_queues = []
    for iso in tqdm.tqdm(all_isos):
        iso = iso()
        # only shared columns
        queue = iso.get_interconnection_queue()[_interconnection_columns]
        queue.insert(0, "ISO", iso.name)
        all_queues.append(queue)
        pd.concat(all_queues)

    all_queues = pd.concat(all_queues).reset_index(drop=True)
    return all_queues


def is_dst_end(date):
    return (date.dst() - (date + pd.DateOffset(1)).dst()).seconds == 3600


def load_folder(path, time_zone=None, verbose=True):
    """Load a single dataframe for same schema csv files in a folder

    Arguments:
        path {str} -- path to folder
        time_zone {str} -- time zone to localize to timestamps. By default returns as UTC

    Returns:
        pd.DataFrame -- dataframe of all files
    """
    all_files = glob.glob(os.path.join(path, "*.csv"))
    all_files = sorted(all_files)

    dfs = []
    for f in tqdm.tqdm(all_files, disable=not verbose):
        df = pd.read_csv(f, parse_dates=True)
        dfs.append(df)

    data = pd.concat(dfs).reset_index(drop=True)

    if "Time" in data.columns:
        data["Time"] = pd.to_datetime(data["Time"], utc=True)
        if time_zone:
            data["Time"] = data["Time"].dt.tz_convert(time_zone)

    # todo make sure dates get parsed
    # todo make sure rows are sorted by time

    return data

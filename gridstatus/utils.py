import glob
import io
import os
from zipfile import ZipFile

import pandas as pd
import requests
import tqdm

import gridstatus
from gridstatus.base import ISOBase, Markets, NotSupported, _interconnection_columns
from gridstatus.caiso import CAISO
from gridstatus.ercot import Ercot
from gridstatus.isone import ISONE
from gridstatus.miso import MISO
from gridstatus.nyiso import NYISO
from gridstatus.pjm import PJM
from gridstatus.spp import SPP

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
                    is_defined = "&#10060;"  # red x

                else:
                    try:
                        getattr(i(), method)(test)
                        is_defined = "&#x2705;"  # green checkmark
                    except NotSupported:
                        is_defined = "&#10060;"  # red x
                    except NotImplementedError:
                        is_defined = "&#10060;"  # red x

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
    if not isinstance(date, pd.Timestamp):
        date = pd.to_datetime(date)

    if tz and date.tzinfo is None:
        date = date.tz_localize(tz)

    return date


def make_lmp_availability():
    lmp_availability = {}
    for i in all_isos:
        lmp_availability[i.name] = i.markets

    return lmp_availability


def make_lmp_availability_table():
    a = make_lmp_availability()
    for iso in a:
        a[iso] = ["`" + v.value + "`" for v in a[iso]]
        a[iso] = ", ".join(a[iso])

    s = pd.Series(a, name="Markets")
    return s.to_markdown()


def filter_lmp_locations(data, locations: list):
    if locations == "ALL" or locations is None:
        return data

    return data[data["Location"].isin(locations)]


def get_zip_file(url):
    # todo add retry logic
    # todo does this need to be a with statement?
    r = requests.get(url)
    z = ZipFile(io.BytesIO(r.content))
    return z.open(z.namelist()[0])


def is_today(date, tz=None):
    return _handle_date(date, tz=tz).date() == pd.Timestamp.now(tz=tz).date()


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

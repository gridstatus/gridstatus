import io
from zipfile import ZipFile

import pandas as pd
import requests

import gridstatus
from gridstatus.base import ISOBase, Markets
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
    methods = {
        "Status": ["get_latest_status", "get_historical_status"],
        "Fuel Mix": [
            "get_latest_fuel_mix",
            "get_fuel_mix_today",
            "get_historical_fuel_mix",
        ],
        "Load": [
            "get_latest_load",
            "get_load_today",
            "get_historical_load",
        ],
        "Supply": [
            "get_latest_supply",
            "get_supply_today",
            "get_historical_supply",
        ],
        "Load Forecast": [
            "get_forecast_today",
            "get_historical_forecast",
        ],
        "Storage": [
            "get_storage_today",
            "get_historical_storage",
        ],
    }

    availability = {}
    for method_type in methods:
        availability[method_type] = {}
        for i in gridstatus.all_isos:
            availability[method_type][i.__name__] = {}
            for method in methods[method_type]:
                is_defined = "&#10060;"  # red x
                if getattr(i, method) != getattr(ISOBase, method):
                    is_defined = "&#x2705;"  # green checkmark
                availability[method_type][i.__name__][method] = is_defined

    availability_dfs = {}
    for method_type in methods:
        availability_dfs[method_type] = pd.DataFrame(availability[method_type])
    return availability_dfs


def make_availability_table():
    dfs = make_availability_df()

    markdown = ""
    for method_type in dfs:
        markdown += "## " + method_type + "\n"
        # df.index = ["`" + v + "`" for v in df.index.values]
        markdown += dfs[method_type].to_markdown() + "\n"

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

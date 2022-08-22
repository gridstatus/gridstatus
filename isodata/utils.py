from datetime import datetime

import pandas as pd

import isodata
from isodata.base import ISOBase, Markets
from isodata.caiso import CAISO
from isodata.ercot import Ercot
from isodata.isone import ISONE
from isodata.miso import MISO
from isodata.nyiso import NYISO
from isodata.pjm import PJM
from isodata.spp import SPP

all_isos = [MISO, CAISO, PJM, Ercot, SPP, NYISO, ISONE]


def list_isos():

    isos = [[i.name, i.iso_id] for i in all_isos]

    return pd.DataFrame(isos, columns=["Name", "Id"])


def get_iso(iso_id):
    for i in all_isos:
        if i.iso_id == iso_id:
            return i

    raise KeyError


def make_availability_df():
    methods = [
        "get_latest_status",
        "get_latest_fuel_mix",
        "get_latest_demand",
        "get_latest_supply",
        "get_fuel_mix_today",
        "get_demand_today",
        "get_forecast_today",
        "get_supply_today",
        "get_battery_today",
        "get_historical_fuel_mix",
        "get_historical_demand",
        "get_historical_forecast",
        "get_historical_supply",
        "get_historical_battery",
    ]

    availability = {}
    for i in isodata.all_isos:
        availability[i.name] = {}
        for m in methods:
            is_defined = "&#10060;"  # red x
            if getattr(i, m) != getattr(ISOBase, m):
                is_defined = "&#x2705;"  # green checkmark
            availability[i.name][m] = is_defined

    availability_df = pd.DataFrame(availability)

    return availability_df


def make_availability_table():
    df = make_availability_df()
    df.index = ["`" + v + "`" for v in df.index.values]
    return df.to_markdown()


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
    if locations == "ALL":
        return data

    return data[data["Location"].isin(locations)]

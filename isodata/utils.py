import isodata
from isodata.base import ISOBase
import pandas as pd
from isodata.nyiso import NYISO
from isodata.caiso import CAISO
from isodata.ercot import Ercot
from isodata.isone import ISONE
from isodata.miso import MISO
from isodata.spp import SPP
from isodata.pjm import PJM

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
        'get_latest_status',
        'get_latest_fuel_mix',
        'get_fuel_mix_today',
        'get_fuel_mix_yesterday',
        'get_historical_fuel_mix',
        'get_latest_demand',
        'get_demand_today',
        'get_demand_yesterday',
        'get_historical_demand',
        'get_latest_supply',
        'get_supply_today',
        'get_supply_yesterday',
        'get_historical_supply'
    ]

    availability = {}
    for i in isodata.all_isos:
        availability[i.name] = {}
        for m in methods:
            is_defined = '&#10060;'  # red x
            if getattr(i, m) != getattr(ISOBase, m):
                is_defined = '&#x2705;'  # green checkmark
            availability[i.name][m] = is_defined

    availability_df = pd.DataFrame(availability)

    return availability_df


def make_availability_table():
    return make_availability_df().to_markdown()


def _handle_date(date):
    if isinstance(date, str):
        date = pd.to_datetime(date)

    return date

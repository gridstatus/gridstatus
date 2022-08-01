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

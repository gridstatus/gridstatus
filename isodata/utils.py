import pandas as pd
from isodata.nyiso import NYISO
from isodata.caiso import CAISO
from isodata.ercot import Ercot
from isodata.isone import ISONE
from isodata.miso import MISO
from isodata.spp import SPP
from isodata.pjm import PJM


def list_isos():
    isos = [
        ["California ISO", "caiso"],
        ["Electric Reliability Council of Texas", "ercot"],
        ["New York ISO", "nyiso"],
        ["Southwest Power Pool", "spp"],
        ["PJM", "pjm"],
        ["Midcontinent ISO", "miso"],
        ["ISO New England", "isone"]
    ]

    return pd.DataFrame(isos, columns=["Name", "Id"])


def get_iso(iso_id):
    mapping = {
        "nyiso": NYISO,
        "caiso": CAISO,
        "ercot": Ercot,
        "isone": ISONE,
        "miso": MISO,
        "spp": SPP,
        "pjm": PJM
    }

    return mapping[iso_id]

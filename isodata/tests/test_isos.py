from isodata import *
import isodata
from isodata.base import FuelMix, ISOBase
import pandas as pd
import pytest

all_isos = [MISO(), CAISO(), PJM(), Ercot(), SPP(), NYISO(), ISONE()]


@pytest.mark.parametrize('iso', all_isos)
def test_all_isos(iso):
    print(iso)
    mix = iso.get_fuel_mix()
    assert isinstance(mix, FuelMix)
    assert isinstance(mix.mix, pd.DataFrame)
    assert isinstance(repr(mix), str)


def test_list_isos():
    assert len(isodata.list_isos()) == 7


def test_get_iso():
    for iso in isodata.list_isos()["Id"].values:
        assert issubclass(isodata.get_iso(iso), ISOBase)

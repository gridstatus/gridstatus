from isodata import *
from isodata.base import FuelMix
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

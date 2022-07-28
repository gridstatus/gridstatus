from isodata import *
from isodata.base import FuelMix
import pandas as pd

isos = [MISO(), CAISO(), PJM(), Ercot(), SPP(), NYISO(), ISONE()]
for iso in isos:
    print(iso)
    mix = iso.get_fuel_mix()
    assert isinstance(mix, FuelMix)
    assert isinstance(mix.mix, pd.DataFrame)
    print(mix)

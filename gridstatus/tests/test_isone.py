import gridstatus
from gridstatus.base import Markets


def test_isone_fuel_mix():
    iso = gridstatus.ISONE()
    data = iso.get_fuel_mix(date="Nov 7, 2022")
    # make sure no nan values are returned
    # nov 7 is a known data where nan values are returned
    assert not data.isna().any().any()


def test_isone_dst_shift_back():
    date = "Nov 6, 2022"
    iso = gridstatus.ISONE()
    data = iso.get_fuel_mix(date=date)

import gridstatus
from gridstatus.base import Markets


def test_isone_fuel_mix():
    iso = gridstatus.ISONE()
    data = iso.get_fuel_mix(date="Nov 7, 2022")
    # make sure no nan values are returned
    # nov 7 is a known data where nan values are returned
    assert not data.isna().any().any()


def run_all(date):
    iso = gridstatus.ISONE()

    data = iso.get_fuel_mix(date=date)
    data = iso.get_load(date=date)
    data = iso.get_load_forecast(date=date)
    data = iso.get_lmp(date=date, market=Markets.DAY_AHEAD_HOURLY)
    data = iso.get_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
    data = iso.get_lmp(date=date, market=Markets.REAL_TIME_HOURLY)


def test_isone_dst_end():
    date = "Nov 6, 2022"
    run_all(date)


def test_isone_dst_start():
    date = "Mar 13, 2022"
    run_all(date)

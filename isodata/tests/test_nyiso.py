import isodata
from isodata.base import Markets


def test_nyiso_edt_to_est():
    iso = isodata.NYISO()

    date = "Nov 7, 2021"

    df = iso.get_historical_status(date=date)
    assert df.shape[0] >= 0

    df = iso.get_historical_fuel_mix(date=date)
    assert df.shape[0] >= 0

    df = iso.get_historical_forecast(date=date)
    assert df.shape[0] >= 0

    df = iso.get_historical_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
    assert df.shape[0] >= 0

    df = iso.get_historical_lmp(date=date, market=Markets.DAY_AHEAD_5_MIN)
    assert df.shape[0] >= 0

    df = iso.get_historical_demand(date=date)
    assert df.shape[0] >= 0


def test_nyiso_est_to_edt():
    iso = isodata.NYISO()

    date = "March 14, 2021"

    df = iso.get_historical_status(date=date)
    assert df.shape[0] >= 0

    df = iso.get_historical_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
    assert df.shape[0] >= 0

    df = iso.get_historical_lmp(date=date, market=Markets.DAY_AHEAD_5_MIN)
    assert df.shape[0] >= 0

    df = iso.get_historical_forecast(date=date)
    assert df.shape[0] >= 0

    df = iso.get_historical_fuel_mix(date=date)
    assert df.shape[0] >= 0

    df = iso.get_historical_demand(date=date)
    assert df.shape[0] >= 0

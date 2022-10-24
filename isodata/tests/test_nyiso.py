import isodata
from isodata.base import Markets


def test_nyiso_edt_to_est():
    # number of rows hardcoded based on when this test was written. should stay same
    iso = isodata.NYISO()

    date = "Nov 7, 2021"

    df = iso.get_historical_status(date=date)
    assert df.shape[0] >= 1

    df = iso.get_historical_fuel_mix(date=date)
    assert df.shape[0] >= 307

    df = iso.get_historical_forecast(date=date)
    assert df.shape[0] >= 145
    df = iso.get_historical_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
    assert df.shape[0] >= 4605
    df = iso.get_historical_lmp(date=date, market=Markets.DAY_AHEAD_HOURLY)
    assert df.shape[0] >= 375

    df = iso.get_historical_demand(date=date)
    assert df.shape[0] >= 307


def test_nyiso_est_to_edt():
    # number of rows hardcoded based on when this test was written. should stay same
    iso = isodata.NYISO()

    date = "March 14, 2021"

    df = iso.get_historical_status(date=date)
    assert df.shape[0] >= 5

    df = iso.get_historical_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
    assert df.shape[0] >= 4215

    df = iso.get_historical_lmp(date=date, market=Markets.DAY_AHEAD_HOURLY)
    assert df.shape[0] >= 345

    df = iso.get_historical_forecast(date=date)
    assert df.shape[0] >= 143

    df = iso.get_historical_fuel_mix(date=date)
    assert df.shape[0] >= 281

    df = iso.get_historical_demand(date=date)
    assert df.shape[0] >= 281

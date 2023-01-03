import pytest

import gridstatus
from gridstatus.base import Markets

# toggle for debugging
VERBOSE = False

DST_BOUNDARIES = [
    "Mar 13, 2022",
    "Nov 6, 2022",
]


def test_isone_fuel_mix():
    iso = gridstatus.ISONE()
    data = iso.get_fuel_mix(date="Nov 7, 2022")
    # make sure no nan values are returned
    # nov 7 is a known data where nan values are returned
    assert not data.isna().any().any()


@pytest.mark.parametrize("date", DST_BOUNDARIES)
def test_get_fuel_mix(date):
    iso = gridstatus.ISONE()
    iso.get_fuel_mix(date=date, verbose=VERBOSE)


@pytest.mark.parametrize("date", DST_BOUNDARIES)
def test_get_load(date):
    iso = gridstatus.ISONE()
    iso.get_load(date=date, verbose=VERBOSE)


@pytest.mark.parametrize("date", DST_BOUNDARIES)
def test_get_load_forecast(date):
    iso = gridstatus.ISONE()
    iso.get_load_forecast(date=date, verbose=VERBOSE)


@pytest.mark.parametrize("date", DST_BOUNDARIES)
@pytest.mark.parametrize("market", gridstatus.ISONE().markets)
def test_get_lmp(date, market):
    iso = gridstatus.ISONE()
    iso.get_lmp(
        date=date,
        market=Markets.DAY_AHEAD_HOURLY,
        verbose=VERBOSE,
    )

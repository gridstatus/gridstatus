import pytest

from gridstatus import ISONE
from gridstatus.base import Markets
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets

# toggle for debugging
VERBOSE = False

DST_BOUNDARIES = [
    "Mar 13, 2022",
    "Nov 6, 2022",
]


class TestISONE(BaseTestISO):
    iso = ISONE()

    """get_fuel_mix"""

    def test_get_fuel_mix_nov_7_2022(self):
        data = self.iso.get_fuel_mix(date="Nov 7, 2022")
        # make sure no nan values are returned
        # nov 7 is a known data where nan values are returned
        assert not data.isna().any().any()

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    def test_get_fuel_mix(self, date):
        self.iso.get_fuel_mix(date=date, verbose=VERBOSE)

    """get_lmp"""

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

    @with_markets(
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_HOURLY,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_today(self, market):
        super().test_get_lmp_today(market=market)

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    @pytest.mark.parametrize(
        "market",
        [
            Markets.REAL_TIME_5_MIN,
            Markets.REAL_TIME_HOURLY,
            Markets.DAY_AHEAD_HOURLY,
        ],
    )
    def test_get_lmp_dst_boundaries(self, date, market):
        self.iso.get_lmp(
            date=date,
            market=market,
            verbose=VERBOSE,
        )

    """get_load"""

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    def test_get_load(self, date):
        self.iso.get_load(date=date, verbose=VERBOSE)

    """get_load_forecast"""

    @pytest.mark.parametrize("date", DST_BOUNDARIES)
    def test_get_load_forecast(self, date):
        self.iso.get_load_forecast(date=date, verbose=VERBOSE)

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

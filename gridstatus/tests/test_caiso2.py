from gridstatus import CAISO, Markets
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestCAISO(BaseTestISO):
    iso = CAISO()

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_HOURLY,
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_HOURLY,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_15_MIN,
        Markets.REAL_TIME_HOURLY,
    )
    def test_get_lmp_today(self, market):
        super().test_get_lmp_today(market=market)

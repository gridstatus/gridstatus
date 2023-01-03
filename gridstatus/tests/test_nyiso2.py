import pytest

from gridstatus import NYISO, Markets
from gridstatus.tests.base_test_iso import BaseTestISO


class TestNYISO(BaseTestISO):
    iso = NYISO()

    @pytest.mark.parametrize(
        "market",
        [
            Markets.DAY_AHEAD_HOURLY,
            Markets.REAL_TIME_5_MIN,
        ],
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

    @pytest.mark.parametrize(
        "market",
        [
            Markets.DAY_AHEAD_HOURLY,
            Markets.REAL_TIME_5_MIN,
        ],
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market=market)

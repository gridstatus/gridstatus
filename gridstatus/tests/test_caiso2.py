import pytest

from gridstatus import CAISO, Markets
from gridstatus.tests.base_test_iso import BaseTestISO


class TestCAISO(BaseTestISO):
    iso = CAISO()

    @pytest.mark.parametrize(
        "market",
        [
            Markets.DAY_AHEAD_HOURLY,
            Markets.REAL_TIME_15_MIN,
            Markets.REAL_TIME_HOURLY,
        ],
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

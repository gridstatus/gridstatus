import pytest

from gridstatus import PJM, Markets
from gridstatus.tests.base_test_iso import BaseTestISO


class TestPJM(BaseTestISO):
    iso = PJM()

    @pytest.mark.parametrize(
        "market",
        [
            # Markets.REAL_TIME_5_MIN, # TODO reenable, but too slow
            Markets.REAL_TIME_HOURLY,
            Markets.DAY_AHEAD_HOURLY,
        ],
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

    def test_get_status_latest(self):
        with pytest.raises(NotImplementedError):
            super().test_get_status_latest()

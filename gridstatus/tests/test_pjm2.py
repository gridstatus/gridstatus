import pytest

from gridstatus import PJM, Markets, NotSupported
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestPJM(BaseTestISO):
    iso = PJM()

    """get_lmp"""

    @with_markets(
        # Markets.REAL_TIME_5_MIN, # TODO reenable, but too slow
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_get_lmp_today(self, market):
        super().test_get_lmp_today(market=market)

    """get_load_forecast"""

    def test_get_load_forecast_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_forecast_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    def test_get_status_latest(self):
        with pytest.raises(NotImplementedError):
            super().test_get_status_latest()

    """get_load_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

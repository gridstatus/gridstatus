import pytest

from gridstatus import MISO, Markets, NotSupported
from gridstatus.tests.base_test_iso import BaseTestISO


class TestMISO(BaseTestISO):
    iso = MISO()

    def test_get_fuel_mix_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_historical()

    def test_get_fuel_mix_historical_with_date_range(self):
        pass

    def test_get_fuel_mix_today(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_today()

    @pytest.mark.parametrize(
        "market",
        [
            Markets.REAL_TIME_5_MIN,
            Markets.DAY_AHEAD_HOURLY,
        ],
    )
    def test_get_lmp_historical(self, market):
        with pytest.raises(NotSupported):
            super().test_get_lmp_historical(market)

    @pytest.mark.parametrize(
        "market",
        [
            Markets.REAL_TIME_5_MIN,
            Markets.DAY_AHEAD_HOURLY,
        ],
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market)

    def test_get_load_forecast_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_forecast_historical()

    def test_get_load_forecast_historical_with_date_range(self):
        pass

    def test_get_load_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_historical()

    def test_get_load_historical_with_date_range(self):
        pass

    def test_get_status_latest(self):
        with pytest.raises(NotImplementedError):
            super().test_get_status_latest()

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

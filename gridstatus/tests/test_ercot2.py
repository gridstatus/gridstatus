import pytest

from gridstatus import Ercot, NotSupported
from gridstatus.tests.base_test_iso import BaseTestISO


class TestErcot(BaseTestISO):
    iso = Ercot()

    """get_fuel_mix"""

    @pytest.mark.skip
    def test_get_fuel_mix_date_or_start(self):
        pass

    def test_get_fuel_mix_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_historical()

    @pytest.mark.skip
    def test_get_fuel_mix_historical_with_date_range(self):
        pass

    """get_lmp"""

    @pytest.mark.skip
    def test_get_lmp_historical(self, markets=None):
        pass

    """get_load_forecast"""

    def test_get_load_forecast_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_forecast_historical()

    @pytest.mark.skip
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    """get_load_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

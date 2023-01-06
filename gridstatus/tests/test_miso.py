import pytest

from gridstatus import MISO, NotSupported
from gridstatus.base import Markets
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestMISO(BaseTestISO):
    iso = MISO()

    """get_fuel_mix"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_date_or_start(self):
        pass

    def test_get_fuel_mix_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_historical_with_date_range(self):
        pass

    def test_get_fuel_mix_today(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_today()

    """get_lmp"""

    @with_markets(
        Markets.REAL_TIME_5_MIN,
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_get_lmp_historical(self, market):
        with pytest.raises(NotSupported):
            super().test_get_lmp_historical(market)

    @with_markets(
        Markets.REAL_TIME_5_MIN,
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market)

    def test_get_lmp_locations(self):
        data = self.iso.get_lmp(
            date="latest",
            market=Markets.REAL_TIME_5_MIN,
            locations=self.iso.hubs,
        )
        assert set(data["Location"].unique()) == set(self.iso.hubs)

    """get_load"""

    def test_get_load_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_historical_with_date_range(self):
        pass

    """get_load_forecast"""

    def test_get_load_forecast_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_forecast_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    """get_status"""

    def test_get_status_latest(self):
        with pytest.raises(NotImplementedError):
            super().test_get_status_latest()

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

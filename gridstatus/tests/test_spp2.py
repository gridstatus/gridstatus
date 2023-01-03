import pytest

from gridstatus import SPP, NotSupported
from gridstatus.tests.base_test_iso import BaseTestISO


class TestSPP(BaseTestISO):
    iso = SPP()

    def test_get_fuel_mix_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_historical()

    def test_get_fuel_mix_today(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_today()

    # TODO: https://github.com/kmax12/gridstatus/issues/109
    def test_get_status_latest(self):
        with pytest.raises(ValueError):
            super().test_get_status_latest()

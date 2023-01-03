import pytest

from gridstatus import MISO, NotSupported
from gridstatus.tests.base_test_iso import BaseTestISO


class TestMISO(BaseTestISO):
    iso = MISO()

    def test_get_fuel_mix_today(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_today()

    def test_get_status_latest(self):
        with pytest.raises(NotImplementedError):
            super().test_get_status_latest()

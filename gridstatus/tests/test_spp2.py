import pytest

from gridstatus import SPP, NotSupported
from gridstatus.tests.base_test_iso import BaseTestISO


class TestSPP(BaseTestISO):
    iso = SPP()

    def test_get_fuel_mix_today(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_today()

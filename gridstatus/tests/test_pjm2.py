import pytest

from gridstatus import PJM
from gridstatus.tests.base_test_iso import BaseTestISO


class TestPJM(BaseTestISO):
    iso = PJM()

    def test_get_status_latest(self):
        with pytest.raises(NotImplementedError):
            super().test_get_status_latest()

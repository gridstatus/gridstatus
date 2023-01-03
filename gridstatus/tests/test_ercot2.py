from gridstatus import Ercot
from gridstatus.tests.base_test_iso import BaseTestISO


class TestErcot(BaseTestISO):
    iso = Ercot()

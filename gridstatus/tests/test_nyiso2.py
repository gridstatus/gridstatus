from gridstatus import NYISO
from gridstatus.tests.base_test_iso import BaseTestISO


class TestNYISO(BaseTestISO):
    iso = NYISO()

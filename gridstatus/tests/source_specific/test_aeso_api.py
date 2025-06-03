import os

import pandas as pd

from gridstatus.aeso_api.aeso_api import AESO
from gridstatus.aeso_api.aeso_api_constants import (
    ASSET_LIST_COLUMN_MAPPING,
    FUEL_MIX_COLUMN_MAPPING,
    INTERCHANGE_COLUMN_MAPPING,
    RESERVES_COLUMN_MAPPING,
    SUPPLY_DEMAND_COLUMN_MAPPING,
)
from gridstatus.tests.base_test_iso import TestHelperMixin
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="aeso_api",
    record_mode=RECORD_MODE,
)


class TestAESO(TestHelperMixin):
    @classmethod
    def setup_class(cls):
        cls.iso = AESO(api_key=os.getenv("AESO_API_KEY"))

    def _check_supply_and_demand(self, df: pd.DataFrame) -> None:
        expected_columns = list(SUPPLY_DEMAND_COLUMN_MAPPING.values())
        for col in expected_columns:
            assert col in df.columns, f"Expected column {col} not found in DataFrame"

        assert df.dtypes["Time"] == f"datetime64[ns, {self.iso.default_timezone}]"

    def test_get_supply_and_demand(self):
        with api_vcr.use_cassette("test_get_supply_and_demand.yaml"):
            df = self.iso.get_supply_and_demand()
            self._check_supply_and_demand(df)

    def _check_fuel_mix(self, df: pd.DataFrame) -> None:
        expected_columns = list(FUEL_MIX_COLUMN_MAPPING.values())
        assert df.columns.tolist() == expected_columns

        assert df.dtypes["Time"] == f"datetime64[ns, {self.iso.default_timezone}]"

    def test_get_fuel_mix(self):
        with api_vcr.use_cassette("test_get_fuel_mix.yaml"):
            df = self.iso.get_fuel_mix()
            self._check_fuel_mix(df)

    def _check_interchange(self, df: pd.DataFrame) -> None:
        expected_columns = list(INTERCHANGE_COLUMN_MAPPING.values())
        assert df.columns.tolist() == expected_columns

        assert df.dtypes["Time"] == f"datetime64[ns, {self.iso.default_timezone}]"

    def test_get_interchange(self):
        with api_vcr.use_cassette("test_get_interchange.yaml"):
            df = self.iso.get_interchange()
            self._check_interchange(df)

    def _check_reserves(self, df: pd.DataFrame) -> None:
        expected_columns = list(RESERVES_COLUMN_MAPPING.values())
        assert df.columns.tolist() == expected_columns

        assert df.dtypes["Time"] == f"datetime64[ns, {self.iso.default_timezone}]"

    def test_get_reserves(self):
        with api_vcr.use_cassette("test_get_reserves.yaml"):
            df = self.iso.get_reserves()
            self._check_reserves(df)

    def _check_asset_list(self, df: pd.DataFrame) -> None:
        expected_columns = list(ASSET_LIST_COLUMN_MAPPING.values())
        for col in expected_columns:
            assert col in df.columns, f"Expected column {col} not found in DataFrame"

        assert df["Asset ID"].dtype == "object"
        assert df["Asset Name"].dtype == "object"
        assert df["Asset Type"].dtype == "object"
        assert df["Operating Status"].dtype == "object"
        assert df["Pool Participant ID"].dtype == "object"
        assert df["Pool Participant Name"].dtype == "object"
        assert df["Net To Grid Asset Flag"].dtype == "object"
        assert df["Asset Include Storage Flag"].dtype == "object"

    def test_get_asset_list(self):
        with api_vcr.use_cassette("test_get_asset_list.yaml"):
            df = self.iso.get_asset_list()
            self._check_asset_list(df)

    def test_get_asset_list_empty(self):
        with api_vcr.use_cassette("test_get_asset_list_empty.yaml"):
            df = self.iso.get_asset_list(asset_id="NONEXISTENT")
            self._check_asset_list(df)
            assert len(df) == 0

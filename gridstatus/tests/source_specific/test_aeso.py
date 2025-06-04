import os

import pandas as pd
import pytest

from gridstatus.aeso.aeso import AESO
from gridstatus.aeso.aeso_constants import (
    ASSET_LIST_COLUMN_MAPPING,
    FUEL_MIX_COLUMN_MAPPING,
    INTERCHANGE_COLUMN_MAPPING,
    RESERVES_COLUMN_MAPPING,
    SUPPLY_DEMAND_COLUMN_MAPPING,
)
from gridstatus.tests.base_test_iso import TestHelperMixin
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="aeso",
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

        numeric_cols = df.columns.drop("Time")
        for col in numeric_cols:
            assert pd.api.types.is_numeric_dtype(df[col]), (
                f"Column {col} should be numeric"
            )

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

    def _check_pool_price(self, df: pd.DataFrame) -> None:
        """Check pool price DataFrame structure and types."""
        expected_columns = ["Interval Start", "Interval End", "Pool Price"]
        assert df.columns.tolist() == expected_columns
        assert (
            df.dtypes["Interval Start"]
            == f"datetime64[ns, {self.iso.default_timezone}]"
        )
        assert (
            df.dtypes["Interval End"] == f"datetime64[ns, {self.iso.default_timezone}]"
        )
        assert pd.api.types.is_numeric_dtype(df["Pool Price"])
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

    def _check_forecast_pool_price(self, df: pd.DataFrame) -> None:
        """Check forecast pool price DataFrame structure and types."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Forecast Pool Price",
        ]
        assert df.columns.tolist() == expected_columns
        assert (
            df.dtypes["Interval Start"]
            == f"datetime64[ns, {self.iso.default_timezone}]"
        )
        assert (
            df.dtypes["Interval End"] == f"datetime64[ns, {self.iso.default_timezone}]"
        )
        assert (
            df.dtypes["Publish Time"] == f"datetime64[ns, {self.iso.default_timezone}]"
        )
        assert pd.api.types.is_numeric_dtype(df["Forecast Pool Price"])
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()
        assert (
            df["Interval Start"] - df["Publish Time"] == pd.Timedelta(hours=4)
        ).all()

    def test_get_pool_price_latest(self):
        """Test getting latest pool price data."""
        with api_vcr.use_cassette("test_get_pool_price_latest.yaml"):
            df = self.iso.get_pool_price(date="latest")
            self._check_pool_price(df)
            assert len(df) > 0

    @pytest.mark.parametrize(
        "start_date,end_date,expected_hours",
        [
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-04"),
                96,
            ),
        ],
    )
    def test_get_pool_price_historical_range(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        expected_hours: int,
    ) -> None:
        """Test getting historical pool price data."""
        with api_vcr.use_cassette(
            f"test_get_pool_price_historical_range_{start_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_pool_price(
                date=start_date,
                end=end_date,
            )
            self._check_pool_price(df)
            assert len(df) == expected_hours

    def test_get_forecast_pool_price_latest(self):
        """Test getting latest forecast pool price data."""
        with api_vcr.use_cassette("test_get_forecast_pool_price_latest.yaml"):
            df = self.iso.get_forecast_pool_price(date="latest")
            self._check_forecast_pool_price(df)
            assert len(df) > 0

    @pytest.mark.parametrize(
        "start_date,end_date,expected_hours",
        [
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-04"),
                96,
            ),
        ],
    )
    def test_get_forecast_pool_price_historical_range(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        expected_hours: int,
    ) -> None:
        """Test getting historical forecast pool price data."""
        with api_vcr.use_cassette(
            f"test_get_forecast_pool_price_historical_range_{start_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_forecast_pool_price(
                date=start_date,
                end=end_date,
            )
            self._check_forecast_pool_price(df)
            assert len(df) == expected_hours

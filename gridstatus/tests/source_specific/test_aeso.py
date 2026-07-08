import os

import pandas as pd
import polars as pl
import pytest

from gridstatus.aeso.aeso import AESO
from gridstatus.aeso.aeso_constants import (
    ASSET_LIST_COLUMN_MAPPING,
    FUEL_MIX_COLUMN_MAPPING,
    RESERVES_COLUMN_MAPPING,
    SUPPLY_DEMAND_COLUMN_MAPPING,
)
from gridstatus.base import NotSupported
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

    def _check_tz_datetime(self, df: pl.DataFrame, col: str) -> None:
        assert isinstance(df.schema[col], pl.Datetime)
        assert df.schema[col].time_zone == self.iso.default_timezone

    def _check_supply_and_demand(self, df: pl.DataFrame) -> None:
        expected_columns = list(SUPPLY_DEMAND_COLUMN_MAPPING.values())
        for col in expected_columns:
            assert col in df.columns, f"Expected column {col} not found in DataFrame"

        self._check_tz_datetime(df, "Time")

    def test_get_supply_and_demand(self):
        with api_vcr.use_cassette("test_get_supply_and_demand.yaml"):
            df = self.iso.get_supply_and_demand()
            self._check_supply_and_demand(df)

    def _check_fuel_mix(self, df: pl.DataFrame) -> None:
        expected_columns = list(FUEL_MIX_COLUMN_MAPPING.values())
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Time")

        numeric_cols = [col for col in df.columns if col != "Time"]
        for col in numeric_cols:
            assert df.schema[col].is_numeric(), f"Column {col} should be numeric"

    def test_get_fuel_mix(self):
        with api_vcr.use_cassette("test_get_fuel_mix.yaml"):
            df = self.iso.get_fuel_mix()
            self._check_fuel_mix(df)

    def _check_interchange(self, df: pl.DataFrame) -> None:
        expected_columns = [
            "Time",
            "Net Interchange Flow",
            "British Columbia Flow",
            "Montana Flow",
            "Saskatchewan Flow",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Time")

        flow_columns = [col for col in df.columns if col != "Time"]
        for col in flow_columns:
            assert df.schema[col].is_numeric(), f"Column {col} should be numeric"

        individual_flows = [
            col for col in flow_columns if col != "Net Interchange Flow"
        ]
        assert (
            df.select(
                pl.col("Net Interchange Flow")
                == pl.sum_horizontal([pl.col(col) for col in individual_flows]),
            )
            .to_series()
            .all()
        ), "Net Interchange Flow should be the sum of individual flows"

    def test_get_interchange(self):
        with api_vcr.use_cassette("test_get_interchange.yaml"):
            df = self.iso.get_interchange()
            self._check_interchange(df)

    def _check_reserves(self, df: pl.DataFrame) -> None:
        expected_columns = list(RESERVES_COLUMN_MAPPING.values())
        assert df.columns == expected_columns

        self._check_tz_datetime(df, "Time")

    def test_get_reserves(self):
        with api_vcr.use_cassette("test_get_reserves.yaml"):
            df = self.iso.get_reserves()
            self._check_reserves(df)

    def _check_asset_list(self, df: pl.DataFrame) -> None:
        expected_columns = list(ASSET_LIST_COLUMN_MAPPING.values())
        for col in expected_columns:
            assert col in df.columns, f"Expected column {col} not found in DataFrame"

        string_columns = list(ASSET_LIST_COLUMN_MAPPING.values())
        for col in string_columns:
            assert df.schema[col] == pl.String

    def test_get_asset_list(self):
        with api_vcr.use_cassette("test_get_asset_list.yaml"):
            df = self.iso.get_asset_list()
            self._check_asset_list(df)

    def test_get_asset_list_empty(self):
        with api_vcr.use_cassette("test_get_asset_list_empty.yaml"):
            df = self.iso.get_asset_list(asset_id="NONEXISTENT")
            self._check_asset_list(df)
            assert df.height == 0

    def _check_pool_price(self, df: pl.DataFrame) -> None:
        """Check pool price DataFrame structure and types."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Pool Price",
            "Rolling 30 Day Average Pool Price",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Interval Start")
        self._check_tz_datetime(df, "Interval End")
        assert df.schema["Pool Price"].is_numeric()
        assert (
            df.select(
                pl.col("Interval End") - pl.col("Interval Start")
                == pl.duration(hours=1),
            )
            .to_series()
            .all()
        )

    def _check_forecast_pool_price(self, df: pl.DataFrame) -> None:
        """Check forecast pool price DataFrame structure and types."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Forecast Pool Price",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Interval Start")
        self._check_tz_datetime(df, "Interval End")
        self._check_tz_datetime(df, "Publish Time")
        assert df.schema["Forecast Pool Price"].is_numeric()
        assert (
            df.select(
                pl.col("Interval End") - pl.col("Interval Start")
                == pl.duration(hours=1),
            )
            .to_series()
            .all()
        )

        request_time = pd.Timestamp.now(tz=self.iso.default_timezone)
        for row in df.iter_rows(named=True):
            interval_start = pd.Timestamp(row["Interval Start"])
            if interval_start > request_time:
                assert row["Publish Time"] == request_time.floor("5min")
            else:
                assert row["Publish Time"] == interval_start - pd.Timedelta(
                    minutes=5,
                )

    def test_get_pool_price_latest(self):
        """Test getting latest pool price data."""
        with api_vcr.use_cassette("test_get_pool_price_latest.yaml"):
            df = self.iso.get_pool_price(date="latest")
            self._check_pool_price(df)
            assert df.height > 0

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
            assert df.height == expected_hours

    def test_get_forecast_pool_price_latest(self):
        """Test getting latest forecast pool price data."""
        with api_vcr.use_cassette("test_get_forecast_pool_price_latest.yaml"):
            df = self.iso.get_forecast_pool_price(date="latest")
            self._check_forecast_pool_price(df)
            assert df.height > 0

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
            f"test_get_forecast_pool_price_historical_range_{start_date.date()}_{end_date.date()}.yaml",
        ):
            df = self.iso.get_forecast_pool_price(
                date=start_date,
                end=end_date,
            )
            self._check_forecast_pool_price(df)
            assert df.height == expected_hours

    def _check_system_marginal_price(self, df: pl.DataFrame) -> None:
        """Check system marginal price DataFrame structure and types."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "System Marginal Price",
            "Volume",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Interval Start")
        self._check_tz_datetime(df, "Interval End")
        assert df.schema["System Marginal Price"].is_numeric()
        assert df.schema["Volume"].is_numeric()

        assert (
            df.select(
                pl.col("Interval End") - pl.col("Interval Start")
                == pl.duration(minutes=1),
            )
            .to_series()
            .all()
        )
        assert df["Interval Start"].is_sorted()
        assert df["Interval End"].is_sorted()
        assert not df["System Marginal Price"].is_null().any()
        assert not df["Volume"].is_null().any()

    def test_get_system_marginal_price_latest(self):
        """Test getting latest system marginal price data."""
        with api_vcr.use_cassette("test_get_system_marginal_price_latest.yaml"):
            df = self.iso.get_system_marginal_price(date="latest")
            self._check_system_marginal_price(df)
            assert df.height > 0

            current_time = pd.Timestamp.now(tz=self.iso.default_timezone)
            assert df["Interval End"].max() >= current_time.floor("min")

    @pytest.mark.parametrize(
        "start_date,end_date,expected_minutes",
        [
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01 01:00"),
                60,
            ),
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-03"),
                2880,
            ),
        ],
    )
    def test_get_system_marginal_price_historical_range(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        expected_minutes: int,
    ) -> None:
        """Test getting historical system marginal price data."""
        with api_vcr.use_cassette(
            f"test_get_system_marginal_price_historical_range_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            start_date = start_date.tz_localize(self.iso.default_timezone)
            end_date = end_date.tz_localize(self.iso.default_timezone)
            df = self.iso.get_system_marginal_price(
                date=start_date,
                end=end_date,
            )
            self._check_system_marginal_price(df)
            assert df.height == expected_minutes
            assert df["Interval Start"].min() == start_date
            assert df["Interval End"].max() == end_date

    def _check_load(self, df: pl.DataFrame) -> None:
        """Check load DataFrame structure and types."""
        expected_columns = ["Interval Start", "Interval End", "Load"]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Interval Start")
        self._check_tz_datetime(df, "Interval End")
        assert df.schema["Load"].is_numeric()
        assert (
            df.select(
                pl.col("Interval End") - pl.col("Interval Start")
                == pl.duration(hours=1),
            )
            .to_series()
            .all()
        )

    def _check_load_forecast(self, df: pl.DataFrame) -> None:
        """Check load forecast DataFrame structure and types."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Load",
            "Load Forecast",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Interval Start")
        self._check_tz_datetime(df, "Interval End")
        self._check_tz_datetime(df, "Publish Time")
        assert df.schema["Load"].is_numeric()
        assert df.schema["Load Forecast"].is_numeric()
        assert (
            df.select(
                pl.col("Interval End") - pl.col("Interval Start")
                == pl.duration(hours=1),
            )
            .to_series()
            .all()
        )

        current_time = pd.Timestamp.now(tz=self.iso.default_timezone)
        today_7am = current_time.floor("D") + pd.Timedelta(hours=7)

        for row in df.iter_rows(named=True):
            interval_start = pd.Timestamp(row["Interval Start"])
            if interval_start > current_time:
                expected_publish = (
                    today_7am
                    if current_time >= today_7am
                    else today_7am - pd.Timedelta(days=1)
                )
                assert row["Publish Time"] == expected_publish
            else:
                interval_day_7am = interval_start.floor("D") + pd.Timedelta(
                    hours=7,
                )
                expected_publish = (
                    interval_day_7am
                    if interval_start >= interval_day_7am
                    else interval_day_7am - pd.Timedelta(days=1)
                )
                assert row["Publish Time"] == expected_publish

    def test_get_load_latest(self):
        """Test getting latest load data."""
        with api_vcr.use_cassette("test_get_load_latest.yaml"):
            df = self.iso.get_load(date="latest")
            self._check_load(df)
            assert df.height > 0
            assert not df["Load"].is_null().any()

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
    def test_get_load_historical_range(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        expected_hours: int,
    ) -> None:
        """Test getting historical load data."""
        with api_vcr.use_cassette(
            f"test_get_load_historical_range_{start_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load(
                date=start_date,
                end=end_date,
            )
            self._check_load(df)
            assert df.height == expected_hours

    def test_get_load_forecast_latest(self):
        """Test getting latest load forecast data."""
        with api_vcr.use_cassette("test_get_load_forecast_latest.yaml"):
            df = self.iso.get_load_forecast(date="latest")
            self._check_load_forecast(df)
            assert df.height > 0

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
    def test_get_load_forecast_historical_range(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        expected_hours: int,
    ) -> None:
        """Test getting historical load forecast data."""
        with api_vcr.use_cassette(
            f"test_get_load_forecast_historical_range_{start_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load_forecast(
                date=start_date,
                end=end_date,
            )
            self._check_load_forecast(df)
            assert df.height == expected_hours

    def test_get_load_forecast_future_publish_times(self):
        """Test that future intervals have correct publish times."""
        with api_vcr.use_cassette("test_get_load_forecast_future_publish_times.yaml"):
            future_date = pd.Timestamp.now(tz=self.iso.default_timezone) + pd.Timedelta(
                days=1,
            )
            df = self.iso.get_load_forecast(date=future_date)

            current_time = pd.Timestamp.now(tz=self.iso.default_timezone)
            today_7am = current_time.floor("D") + pd.Timedelta(hours=7)
            expected_publish = (
                today_7am
                if current_time >= today_7am
                else today_7am - pd.Timedelta(days=1)
            )

            assert (df["Publish Time"] == expected_publish).all()

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-04"),
            ),
        ],
    )
    def test_get_load_forecast_historical_publish_times(
        self,
        date: pd.Timestamp,
        end: pd.Timestamp,
    ):
        """Test that historical intervals have correct publish times."""
        with api_vcr.use_cassette(
            "test_get_load_forecast_historical_publish_times.yaml",
        ):
            df = self.iso.get_load_forecast(date=date, end=end)

            for row in df.iter_rows(named=True):
                interval_start = pd.Timestamp(row["Interval Start"])
                interval_day_7am = interval_start.floor("D") + pd.Timedelta(
                    hours=7,
                )
                expected_publish = (
                    interval_day_7am
                    if interval_start >= interval_day_7am
                    else interval_day_7am - pd.Timedelta(days=1)
                )
                assert row["Publish Time"] == expected_publish

    def _check_unit_status(self, df: pl.DataFrame) -> None:
        """Check unit status DataFrame structure and types."""
        expected_columns = [
            "Time",
            "Asset",
            "Fuel Type",
            "Sub Fuel Type",
            "Maximum Capability",
            "Net Generation",
            "Dispatched Contingency Reserve",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Time")
        assert df.schema["Asset"] == pl.String
        assert df.schema["Fuel Type"] == pl.String
        assert df.schema["Sub Fuel Type"] == pl.String

        numeric_columns = [
            "Maximum Capability",
            "Net Generation",
            "Dispatched Contingency Reserve",
        ]
        for col in numeric_columns:
            assert df.schema[col].is_numeric(), f"Column {col} should be numeric"

    def test_get_unit_status(self):
        """Test getting current unit status data."""
        with api_vcr.use_cassette("test_get_unit_status.yaml"):
            df = self.iso.get_unit_status(date="latest")
            self._check_unit_status(df)
            assert df.height > 0

    def _check_generator_outages_hourly(self, df: pl.DataFrame) -> None:
        """Check generator outages DataFrame structure and types."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Total Outage",
            "Simple Cycle",
            "Combined Cycle",
            "Cogeneration",
            "Gas Fired Steam",
            "Coal",
            "Hydro",
            "Wind",
            "Solar",
            "Energy Storage",
            "Biomass and Other",
            "Mothball Outage",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Interval Start")
        self._check_tz_datetime(df, "Interval End")
        self._check_tz_datetime(df, "Publish Time")
        assert (
            df.select(
                pl.col("Interval End") - pl.col("Interval Start")
                == pl.duration(hours=1),
            )
            .to_series()
            .all()
        )

        numeric_columns = [
            col
            for col in df.columns
            if col not in ["Interval Start", "Interval End", "Publish Time"]
        ]
        for col in numeric_columns:
            assert df.schema[col].is_numeric(), f"Column {col} should be numeric"

        outage_columns = [
            col
            for col in numeric_columns
            if col not in ["Mothball Outage", "Total Outage"]
        ]
        assert (
            df.select(
                pl.col("Total Outage")
                == pl.sum_horizontal([pl.col(col) for col in outage_columns]),
            )
            .to_series()
            .all()
        )

    def test_get_generator_outages_hourly_latest(self):
        """Test getting latest generator outages data."""
        with api_vcr.use_cassette("test_get_generator_outages_hourly_latest.yaml"):
            df = self.iso.get_generator_outages_hourly(date="latest")
            self._check_generator_outages_hourly(df)
            assert df.height > 0

    @pytest.mark.parametrize(
        "start_date,end_date",
        [
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-04"),
            ),
        ],
    )
    def test_get_generator_outages_hourly_historical_range(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
    ) -> None:
        """Test getting historical generator outages data."""
        with api_vcr.use_cassette(
            f"test_get_generator_outages_hourly_historical_range_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_generator_outages_hourly(
                date=start_date,
                end=end_date,
            )
            self._check_generator_outages_hourly(df)
            assert df["Interval Start"].min() >= start_date.tz_localize(
                self.iso.default_timezone,
            )
            assert df["Interval Start"].max() <= end_date.tz_localize(
                self.iso.default_timezone,
            )

    def _check_transmission_outages(self, df: pl.DataFrame) -> None:
        """Check transmission outages DataFrame structure and types."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Transmission Owner",
            "Type",
            "Element",
            "Scheduled Activity",
            "Date Time Comments",
            "Interconnection",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Interval Start")
        self._check_tz_datetime(df, "Interval End")
        self._check_tz_datetime(df, "Publish Time")
        string_columns = [
            "Transmission Owner",
            "Type",
            "Element",
            "Scheduled Activity",
            "Date Time Comments",
            "Interconnection",
        ]
        for col in string_columns:
            assert df.schema[col] == pl.String, (
                f"Column {col} should be object/string type"
            )

        assert (
            df.select(
                pl.col("Interval End") >= pl.col("Interval Start"),
            )
            .to_series()
            .all()
        )

    def test_get_transmission_outages_latest(self):
        """Test getting latest transmission outages data."""
        with api_vcr.use_cassette("test_get_transmission_outages_latest.yaml"):
            df = self.iso.get_transmission_outages(date="latest")
            self._check_transmission_outages(df)
            assert df.height > 0
            assert df["Publish Time"].n_unique() == 1

    @pytest.mark.parametrize(
        "start_date,end_date",
        [
            (
                pd.Timestamp("2025-05-01"),
                pd.Timestamp("2025-06-15"),
            ),
        ],
    )
    def test_get_transmission_outages_historical_range(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
    ) -> None:
        """Test getting historical transmission outages data."""
        with api_vcr.use_cassette(
            f"test_get_transmission_outages_historical_range_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_transmission_outages(
                date=start_date,
                end=end_date,
            )
            self._check_transmission_outages(df)
            assert df["Publish Time"].n_unique() > 1
            assert df["Publish Time"].min().date() >= start_date.date()
            assert df["Publish Time"].max().date() <= end_date.date()

    @pytest.mark.parametrize(
        "target_date",
        [
            pd.Timestamp("2025-01-17"),
        ],
    )
    def test_get_transmission_outages_single_date(
        self,
        target_date: pd.Timestamp,
    ) -> None:
        """Test getting transmission outages for a single date (most recent file before target date)."""
        with api_vcr.use_cassette(
            f"test_get_transmission_outages_single_date_{target_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_transmission_outages(date=target_date)
            self._check_transmission_outages(df)
            assert df.height > 0
            assert df["Publish Time"].n_unique() == 1
            publish_time = df[0, "Publish Time"]
            assert publish_time.date() < target_date.date(), (
                f"Publish time {publish_time.date()} should be before target date {target_date.date()}"
            )

    def _check_wind_solar_forecast(self, df: pl.DataFrame, forecast_type: str) -> None:
        """Check wind/solar forecast DataFrame structure and types."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Minimum Generation Forecast",
            "Most Likely Generation Forecast",
            "Maximum Generation Forecast",
            f"Total {forecast_type.capitalize()} Capacity",
            "Minimum Generation Percentage",
            "Most Likely Generation Percentage",
            "Maximum Generation Percentage",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Interval Start")
        self._check_tz_datetime(df, "Interval End")
        self._check_tz_datetime(df, "Publish Time")

        numeric_columns = [
            "Minimum Generation Forecast",
            "Most Likely Generation Forecast",
            "Maximum Generation Forecast",
            f"Total {forecast_type.capitalize()} Capacity",
            "Minimum Generation Percentage",
            "Most Likely Generation Percentage",
            "Maximum Generation Percentage",
        ]
        for col in numeric_columns:
            assert df.schema[col].is_numeric(), f"Column {col} should be numeric"

        assert df["Interval Start"].is_sorted()
        assert (
            df.select(pl.col("Interval End") > pl.col("Interval Start"))
            .to_series()
            .all()
        )

    def test_get_wind_forecast_12_hour_latest(self):
        """Test getting latest 12-hour wind forecast data."""
        with api_vcr.use_cassette("test_get_wind_forecast_12_hour_latest.yaml"):
            df = self.iso.get_wind_forecast_12_hour(date="latest")
            self._check_wind_solar_forecast(df, "wind")
            assert df.height > 0

    def test_get_wind_forecast_7_day_latest(self):
        """Test getting latest 7-day wind forecast data."""
        with api_vcr.use_cassette("test_get_wind_forecast_7_day_latest.yaml"):
            df = self.iso.get_wind_forecast_7_day(date="latest")
            self._check_wind_solar_forecast(df, "wind")
            assert df.height > 0

    def test_get_solar_forecast_12_hour_latest(self):
        """Test getting latest 12-hour solar forecast data."""
        with api_vcr.use_cassette("test_get_solar_forecast_12_hour_latest.yaml"):
            df = self.iso.get_solar_forecast_12_hour(date="latest")
            self._check_wind_solar_forecast(df, "solar")
            assert df.height > 0

    def test_get_solar_forecast_7_day_latest(self):
        """Test getting latest 7-day solar forecast data."""
        with api_vcr.use_cassette("test_get_solar_forecast_7_day_latest.yaml"):
            df = self.iso.get_solar_forecast_7_day(date="latest")
            self._check_wind_solar_forecast(df, "solar")
            assert df.height > 0

    def test_get_wind_forecast_12_hour_historical(self):
        """Test that historical 12-hour wind forecast raises NotSupported."""
        from gridstatus.base import NotSupported

        with pytest.raises(
            NotSupported,
            match="Historical data is not supported for 12-hour wind forecasts",
        ):
            self.iso.get_wind_forecast_12_hour(date="2024-01-01")

    @pytest.mark.parametrize(
        "start_date,end_date",
        [
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-03"),
            ),
        ],
    )
    def test_get_wind_forecast_7_day_historical(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
    ) -> None:
        """Test getting historical 7-day wind forecast data."""
        with api_vcr.use_cassette(
            f"test_get_wind_forecast_7_day_historical_{start_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_wind_forecast_7_day(
                date=start_date,
                end=end_date,
            )
            self._check_wind_solar_forecast(df, "wind")
            assert df.height > 0
            assert df["Interval Start"].min().date() >= start_date.date()
            assert df["Interval Start"].max().date() <= end_date.date()

    def test_get_wind_forecast_7_day_out_of_range(self):
        """Test that out-of-range dates for 7-day wind forecast raise NotSupported."""

        with pytest.raises(
            NotSupported,
            match="Historical wind forecast data is only available from 2023-03-01 to 2025-04-01",
        ):
            self.iso.get_wind_forecast_7_day(date="2022-01-01")

    def test_get_solar_forecast_12_hour_historical(self):
        """Test that historical 12-hour solar forecast raises NotSupported."""
        from gridstatus.base import NotSupported

        with pytest.raises(
            NotSupported,
            match="Historical data is not supported for 12-hour solar forecasts",
        ):
            self.iso.get_solar_forecast_12_hour(date="2024-01-01")

    @pytest.mark.parametrize(
        "start_date,end_date",
        [
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-03"),
            ),
        ],
    )
    def test_get_solar_forecast_7_day_historical(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
    ) -> None:
        """Test getting historical 7-day solar forecast data."""
        with api_vcr.use_cassette(
            f"test_get_solar_forecast_7_day_historical_{start_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_solar_forecast_7_day(
                date=start_date,
                end=end_date,
            )
            self._check_wind_solar_forecast(df, "solar")
            assert df.height > 0
            assert df["Interval Start"].min().date() >= start_date.date()
            assert df["Interval Start"].max().date() <= end_date.date()

    def test_get_solar_forecast_7_day_out_of_range(self):
        """Test that out-of-range dates for 7-day solar forecast raise NotSupported."""

        with pytest.raises(
            NotSupported,
            match="Historical solar forecast data is only available from 2023-03-01 to 2025-04-01",
        ):
            self.iso.get_solar_forecast_7_day(date="2022-01-01")

    def _check_daily_average_pool_price(self, df: pl.DataFrame) -> None:
        """Check daily average pool price DataFrame structure and types."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Daily Average",
            "Daily On Peak Average",
            "Daily Off Peak Average",
            "30 Day Average",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Interval Start")
        self._check_tz_datetime(df, "Interval End")

        assert (
            df.select(
                pl.col("Interval End") - pl.col("Interval Start")
                == pl.duration(days=1),
            )
            .to_series()
            .all()
        )

        price_columns = [
            "Daily Average",
            "Daily On Peak Average",
            "Daily Off Peak Average",
            "30 Day Average",
        ]
        for col in price_columns:
            assert df.schema[col].is_numeric(), f"Column {col} should be numeric"

    def test_get_daily_average_pool_price_latest(self):
        """Test getting latest daily average pool price data."""
        with api_vcr.use_cassette("test_get_daily_average_pool_price_latest.yaml"):
            df = self.iso.get_daily_average_pool_price(date="latest")
            self._check_daily_average_pool_price(df)
            assert df.height > 0

    @pytest.mark.parametrize(
        "start_date,end_date,expected_days",
        [
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-04"),
                4,
            ),
        ],
    )
    def test_get_daily_average_pool_price_historical_range(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        expected_days: int,
    ) -> None:
        """Test getting historical daily average pool price data."""
        with api_vcr.use_cassette(
            f"test_get_daily_average_pool_price_historical_range_{start_date.date()}_{end_date.date()}.yaml",
        ):
            df = self.iso.get_daily_average_pool_price(date=start_date, end=end_date)
            self._check_daily_average_pool_price(df)
            assert df.height == expected_days

    def _check_wind_solar(self, df: pl.DataFrame, generation_type: str) -> None:
        """Check wind/solar actual generation DataFrame structure and types."""
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Actual Generation",
            f"Total {generation_type.capitalize()} Capacity",
        ]
        assert df.columns == expected_columns
        self._check_tz_datetime(df, "Interval Start")
        self._check_tz_datetime(df, "Interval End")

        numeric_columns = [
            "Actual Generation",
            f"Total {generation_type.capitalize()} Capacity",
        ]
        for col in numeric_columns:
            assert df.schema[col].is_numeric(), f"Column {col} should be numeric"

        assert df["Interval Start"].is_sorted()
        assert (
            df.select(pl.col("Interval End") > pl.col("Interval Start"))
            .to_series()
            .all()
        )

    def test_get_wind_10_min_latest(self):
        """Test getting latest wind generation data."""
        with api_vcr.use_cassette("test_get_wind_10_min_latest.yaml"):
            df = self.iso.get_wind_10_min(date="latest")
            self._check_wind_solar(df, "wind")
            assert df.height > 0

    def test_get_solar_10_min_latest(self):
        """Test getting latest solar generation data."""
        with api_vcr.use_cassette("test_get_solar_10_min_latest.yaml"):
            df = self.iso.get_solar_10_min(date="latest")
            self._check_wind_solar(df, "solar")
            assert df.height > 0

    @pytest.mark.parametrize(
        "start_date,end_date",
        [
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-03"),
            ),
        ],
    )
    def test_get_wind_hourly_historical(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
    ) -> None:
        """Test getting historical wind generation data."""
        with api_vcr.use_cassette(
            f"test_get_wind_hourly_historical_{start_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_wind_hourly(
                date=start_date,
                end=end_date,
            )
            self._check_wind_solar(df, "wind")
            assert df.height > 0
            assert df["Interval Start"].min().date() >= start_date.date()
            assert df["Interval Start"].max().date() <= end_date.date()

    @pytest.mark.parametrize(
        "start_date,end_date",
        [
            (
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-03"),
            ),
        ],
    )
    def test_get_solar_hourly_historical(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
    ) -> None:
        """Test getting historical solar generation data."""
        with api_vcr.use_cassette(
            f"test_get_solar_hourly_historical_{start_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_solar_hourly(
                date=start_date,
                end=end_date,
            )
            self._check_wind_solar(df, "solar")
            assert df.height > 0
            assert df["Interval Start"].min().date() >= start_date.date()
            assert df["Interval Start"].max().date() <= end_date.date()

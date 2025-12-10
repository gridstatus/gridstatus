from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from gridstatus.base import Markets, NotSupported
from gridstatus.miso_api import MISOAPI
from gridstatus.tests.base_test_iso import TestHelperMixin
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="miso_api",
    record_mode=RECORD_MODE,
)


LMP_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Market",
    "Location",
    "Location Type",
    "LMP",
    "Energy",
    "Congestion",
    "Loss",
]


class TestMISOAPI(TestHelperMixin):
    @classmethod
    def setup_class(cls):
        # https://docs.pytest.org/en/stable/how-to/xunit_setup.html
        # Runs before all tests in this class
        cls.iso = MISOAPI()

    def _check_lmp(self, df, market_value):
        assert df.columns.tolist() == LMP_COLUMNS
        assert list(df["Market"].unique()) == [market_value]
        assert df["Location Type"].notna().all()

    """get_lmp_day_ahead_hourly_ex_ante"""

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_lmp_day_ahead_hourly_ex_ante")
    def test_get_lmp_day_ahead_hourly_ex_ante_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=2)).floor("h")
        end = start + pd.Timedelta(hours=3)
        df = self.iso.get_lmp_day_ahead_hourly_ex_ante(start, end)

        self._check_lmp(df, market_value=Markets.DAY_AHEAD_HOURLY_EX_ANTE.value)

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(hours=1)

    """get_lmp_day_ahead_hourly_ex_post"""

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_lmp_day_ahead_hourly_ex_post")
    def test_get_lmp_day_ahead_hourly_ex_post_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=2)).floor("h")
        end = start + pd.Timedelta(hours=3)
        df = self.iso.get_lmp_day_ahead_hourly_ex_post(start, end)

        self._check_lmp(df, market_value=Markets.DAY_AHEAD_HOURLY_EX_POST.value)

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(hours=1)

    """get_lmp_real_time_hourly_ex_post_prelim"""

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_lmp_real_time_hourly_ex_post_prelim")
    def test_get_lmp_real_time_hourly_ex_post_prelim_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=2)).floor("h")
        end = start + pd.Timedelta(hours=3)
        df = self.iso.get_lmp_real_time_hourly_ex_post_prelim(start, end)

        self._check_lmp(df, market_value=Markets.REAL_TIME_HOURLY_EX_POST_PRELIM.value)

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(hours=1)

    """get_lmp_real_time_hourly_ex_post_final"""

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_lmp_real_time_hourly_ex_post_final")
    def test_get_lmp_real_time_hourly_ex_post_final_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=7)).floor("h")
        end = start + pd.Timedelta(hours=3)
        df = self.iso.get_lmp_real_time_hourly_ex_post_final(start, end)

        self._check_lmp(df, market_value=Markets.REAL_TIME_HOURLY_EX_POST_FINAL.value)

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(hours=1)

    """get_lmp_real_time_5_min_ex_ante"""

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_lmp_real_time_5_min_ex_ante")
    def test_get_lmp_real_time_5_min_ex_ante_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=2)).floor("5min")
        end = start + pd.Timedelta(hours=1)
        df = self.iso.get_lmp_real_time_5_min_ex_ante(start, end)

        self._check_lmp(df, market_value=Markets.REAL_TIME_5_MIN_EX_ANTE.value)

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(minutes=5)

    """get_lmp_real_time_5_min_ex_post_prelim"""

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_lmp_real_time_5_min_ex_post_prelim")
    def test_get_lmp_real_time_5_min_ex_post_prelim_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=2)).floor("5min")
        end = start + pd.Timedelta(hours=1)
        df = self.iso.get_lmp_real_time_5_min_ex_post_prelim(start, end)

        self._check_lmp(df, market_value=Markets.REAL_TIME_5_MIN_EX_POST_PRELIM.value)

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(minutes=5)

    """get_lmp_real_time_5_min_ex_post_final"""

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_lmp_real_time_5_min_ex_post_final")
    def test_get_lmp_real_time_5_min_ex_post_final_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=7)).floor("5min")
        end = start + pd.Timedelta(hours=1)
        df = self.iso.get_lmp_real_time_5_min_ex_post_final(start, end)

        self._check_lmp(df, market_value=Markets.REAL_TIME_5_MIN_EX_POST_FINAL.value)

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(minutes=5)

    """get_interchange_hourly"""

    def _check_interchange_hourly(self, data: pd.DataFrame):
        assert data.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Net Scheduled Interchange Forward",
            "Net Scheduled Interchange Real Time",
            "Net Scheduled Interchange Delta",
            "MHEB Scheduled",
            "MHEB Actual",
            "ONT Scheduled",
            "ONT Actual",
            "SWPP Scheduled",
            "SWPP Actual",
            "TVA Scheduled",
            "TVA Actual",
            "AECI Scheduled",
            "AECI Actual",
            "SOCO Scheduled",
            "SOCO Actual",
            "LGEE Scheduled",
            "LGEE Actual",
            "PJM Scheduled",
            "PJM Actual",
            "OTHER Scheduled",
            "SPA Actual",
        ]

        assert data["Interval Start"].dtype == "datetime64[ns, EST]"
        assert data["Interval End"].dtype == "datetime64[ns, EST]"

        for col in data.columns:
            if col not in ["Interval Start", "Interval End"]:
                assert data[col].dtype == "float64"

    def test_get_interchange_hourly_today(self):
        with api_vcr.use_cassette("test_get_interchange_hourly_today"):
            df = self.iso.get_interchange_hourly("today")

        self._check_interchange_hourly(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        # Scheduled data extends to the end of the day
        assert df["Interval Start"].max() == self.local_start_of_today() + pd.Timedelta(
            hours=23,
        )

    def test_get_interchange_hourly_date_range(self):
        start = (
            self.local_start_of_today()
            - pd.DateOffset(days=20)
            + pd.DateOffset(hours=10)
        )
        end = start + pd.DateOffset(days=2, hours=4)

        with api_vcr.use_cassette(
            f"test_get_interchange_hourly_{start:%Y-%m-%dT%H-%M-%S}_{end:%Y-%m-%dT%H-%M-%S}",
        ):
            df = self.iso.get_interchange_hourly(start, end)

        self._check_interchange_hourly(df)

        assert df["Interval Start"].min() == start
        # Data extends to the end of the day because of the way the support_date_range
        # decorator works.
        assert df["Interval Start"].max() == self.local_start_of_day(
            end.date(),
        ) + pd.Timedelta(hours=23)

    """MCP Tests"""

    def _check_mcp_columns(self, df, expected_products):
        core_columns = ["Interval Start", "Interval End", "Zone"]
        expected_columns = core_columns + expected_products

        assert set(df.columns.tolist()) == set(expected_columns)
        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"
        assert isinstance(df["Zone"].iloc[0], str)

        for product in expected_products:
            assert df[product].dtype in ["float64", "Float64"]

    @pytest.mark.integration
    def test_get_as_mcp_day_ahead_ex_ante_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=2)).floor("h")
        end = start + pd.Timedelta(hours=1)

        with api_vcr.use_cassette(
            f"test_get_as_mcp_day_ahead_ex_ante_{start.date()}_{end.date()}",
        ):
            df = self.iso.get_as_mcp_day_ahead_ex_ante(start, end)

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(hours=1)

    @pytest.mark.integration
    def test_get_as_mcp_day_ahead_ex_post_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=2)).floor("h")
        end = start + pd.Timedelta(hours=1)

        with api_vcr.use_cassette(
            f"test_get_as_mcp_day_ahead_ex_post_{start.date()}_{end.date()}",
        ):
            df = self.iso.get_as_mcp_day_ahead_ex_post(start, end)

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(hours=1)

    @pytest.mark.integration
    def test_get_as_mcp_real_time_5_min_ex_ante_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=2)).floor("5min")
        end = start + pd.Timedelta(minutes=30)

        with api_vcr.use_cassette(
            f"test_get_as_mcp_real_time_5_min_ex_ante_{start.date()}_{end.date()}",
        ):
            df = self.iso.get_as_mcp_real_time_5_min_ex_ante(start, end)

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(minutes=5)

    @pytest.mark.integration
    def test_get_as_mcp_real_time_5_min_ex_post_prelim_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=2)).floor("5min")
        end = start + pd.Timedelta(minutes=30)

        with api_vcr.use_cassette(
            f"test_get_as_mcp_real_time_5_min_ex_post_prelim_{start.date()}_{end.date()}",
        ):
            df = self.iso.get_as_mcp_real_time_5_min_ex_post_prelim(start, end)

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(minutes=5)

    @pytest.mark.integration
    def test_get_as_mcp_real_time_hourly_ex_post_prelim_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=3)).floor("h")
        end = start + pd.Timedelta(hours=1)

        with api_vcr.use_cassette(
            f"test_get_as_mcp_real_time_hourly_ex_post_prelim_{start.date()}_{end.date()}",
        ):
            df = self.iso.get_as_mcp_real_time_hourly_ex_post_prelim(start, end)

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(hours=1)

    @pytest.mark.integration
    def test_get_as_mcp_real_time_5_min_ex_post_final_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=10)).floor("5min")
        end = start + pd.Timedelta(minutes=30)

        with api_vcr.use_cassette(
            f"test_get_as_mcp_real_time_5_min_ex_post_final_{start.date()}_{end.date()}",
        ):
            df = self.iso.get_as_mcp_real_time_5_min_ex_post_final(start, end)

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(minutes=5)

    @pytest.mark.integration
    def test_get_as_mcp_real_time_hourly_ex_post_final_date_range(self):
        start = (self.local_now() - pd.DateOffset(days=10)).floor("h")
        end = start + pd.Timedelta(hours=1)

        with api_vcr.use_cassette(
            f"test_get_as_mcp_real_time_hourly_ex_post_final_{start.date()}_{end.date()}",
        ):
            df = self.iso.get_as_mcp_real_time_hourly_ex_post_final(start, end)

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == start
        assert df["Interval Start"].max() == end - pd.Timedelta(hours=1)

    """MCP Tests with use_daily_requests=True"""

    @pytest.mark.integration
    def test_get_as_mcp_use_daily_requests_day_ahead_ex_ante(self):
        date = (self.local_now() - pd.DateOffset(days=2)).floor("d")

        with api_vcr.use_cassette(
            f"test_get_as_mcp_use_daily_requests_day_ahead_ex_ante_{date.date()}",
        ):
            df = self.iso.get_as_mcp_day_ahead_ex_ante(date, use_daily_requests=True)

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(hours=23)

    @pytest.mark.integration
    def test_get_as_mcp_use_daily_requests_day_ahead_ex_post(self):
        date = (self.local_now() - pd.DateOffset(days=2)).floor("d")

        with api_vcr.use_cassette(
            f"test_get_as_mcp_use_daily_requests_day_ahead_ex_post_{date.date()}",
        ):
            df = self.iso.get_as_mcp_day_ahead_ex_post(date, use_daily_requests=True)

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(hours=23)

    @pytest.mark.integration
    def test_get_as_mcp_use_daily_requests_real_time_5_min_ex_ante(self):
        date = (self.local_now() - pd.DateOffset(days=2)).floor("d")

        with api_vcr.use_cassette(
            f"test_get_as_mcp_use_daily_requests_real_time_5_min_ex_ante_{date.date()}",
        ):
            df = self.iso.get_as_mcp_real_time_5_min_ex_ante(
                date,
                use_daily_requests=True,
            )

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(hours=23, minutes=55)

    @pytest.mark.integration
    def test_get_as_mcp_use_daily_requests_real_time_5_min_ex_post_prelim(self):
        date = (self.local_now() - pd.DateOffset(days=2)).floor("d")

        with api_vcr.use_cassette(
            f"test_get_as_mcp_use_daily_requests_real_time_5_min_ex_post_prelim_{date.date()}",
        ):
            df = self.iso.get_as_mcp_real_time_5_min_ex_post_prelim(
                date,
                use_daily_requests=True,
            )

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(hours=23, minutes=55)

    @pytest.mark.integration
    def test_get_as_mcp_use_daily_requests_real_time_hourly_ex_post_prelim(self):
        date = (self.local_now() - pd.DateOffset(days=3)).floor("d")

        with api_vcr.use_cassette(
            f"test_get_as_mcp_use_daily_requests_real_time_hourly_ex_post_prelim_{date.date()}",
        ):
            df = self.iso.get_as_mcp_real_time_hourly_ex_post_prelim(
                date,
                use_daily_requests=True,
            )

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(hours=23)

    @pytest.mark.integration
    def test_get_as_mcp_use_daily_requests_real_time_5_min_ex_post_final(self):
        date = (self.local_now() - pd.DateOffset(days=10)).floor("d")

        with api_vcr.use_cassette(
            f"test_get_as_mcp_use_daily_requests_real_time_5_min_ex_post_final_{date.date()}",
        ):
            df = self.iso.get_as_mcp_real_time_5_min_ex_post_final(
                date,
                use_daily_requests=True,
            )

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(hours=23, minutes=55)

    @pytest.mark.integration
    def test_get_as_mcp_use_daily_requests_real_time_hourly_ex_post_final(self):
        date = (self.local_now() - pd.DateOffset(days=10)).floor("d")

        with api_vcr.use_cassette(
            f"test_get_as_mcp_use_daily_requests_real_time_hourly_ex_post_final_{date.date()}",
        ):
            df = self.iso.get_as_mcp_real_time_hourly_ex_post_final(
                date,
                use_daily_requests=True,
            )

        self._check_mcp_columns(
            df,
            ["Ramp Down", "Ramp Up", "Regulation", "STR", "Spin", "Supplemental"],
        )

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(hours=23)

    def _check_test_get_day_ahead_cleared_demand(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Region",
            "Fixed Bids Cleared",
            "Price Sensitive Bids Cleared",
            "Virtual Bids Cleared",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                assert df[col].dtype == "float64"

    @pytest.mark.integration
    def test_get_day_ahead_cleared_demand_daily(self):
        date = self.local_start_of_today()

        with api_vcr.use_cassette(f"get_day_ahead_cleared_demand_daily_{date.date()}"):
            df = self.iso.get_day_ahead_cleared_demand_daily(date)

        self._check_test_get_day_ahead_cleared_demand(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    @pytest.mark.integration
    def test_get_day_ahead_cleared_demand_hourly(self):
        date = self.local_start_of_today()

        with api_vcr.use_cassette(f"get_day_ahead_cleared_demand_hourly_{date.date()}"):
            df = self.iso.get_day_ahead_cleared_demand_hourly(date)

        self._check_test_get_day_ahead_cleared_demand(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )

    def _check_day_ahead_cleared_generation_hourly(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Region",
            "Supply Cleared",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                assert df[col].dtype == "float64"

    @pytest.mark.integration
    def test_get_day_ahead_cleared_generation_physical_hourly(self):
        date = self.local_start_of_today()

        with api_vcr.use_cassette(
            f"get_day_ahead_cleared_generation_physical_hourly_{date.date()}",
        ):
            df = self.iso.get_day_ahead_cleared_generation_physical_hourly(date)

        self._check_day_ahead_cleared_generation_hourly(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )

    @pytest.mark.integration
    def test_get_day_ahead_cleared_generation_virtual_hourly(self):
        date = self.local_start_of_today()

        with api_vcr.use_cassette(
            f"get_day_ahead_cleared_generation_virtual_hourly_{date.date()}",
        ):
            df = self.iso.get_day_ahead_cleared_generation_virtual_hourly(date)

        self._check_day_ahead_cleared_generation_hourly(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )

    @pytest.mark.integration
    def test_get_day_ahead_net_scheduled_interchange_hourly(self):
        date = self.local_start_of_today()

        with api_vcr.use_cassette(
            f"get_day_ahead_net_scheduled_interchange_hourly_{date.date()}",
        ):
            df = self.iso.get_day_ahead_net_scheduled_interchange_hourly(date)

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Region",
            "Net Scheduled Interchange",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                assert df[col].dtype == "float64"

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )

    def _check_day_ahead_offered_generation_hourly(self, df, expected_columns):
        assert df.columns.tolist() == expected_columns

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                assert df[col].dtype == "float64"

    @pytest.mark.integration
    def test_get_day_ahead_offered_generation_ecomax_hourly(self):
        date = self.local_start_of_today()

        with api_vcr.use_cassette(
            f"get_day_ahead_offered_generation_ecomax_hourly_{date.date()}",
        ):
            df = self.iso.get_day_ahead_offered_generation_ecomax_hourly(date)

        self._check_day_ahead_offered_generation_hourly(
            df,
            [
                "Interval Start",
                "Interval End",
                "Region",
                "Must Run",
                "Economic",
                "Emergency",
            ],
        )

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )

    @pytest.mark.integration
    def test_get_day_ahead_offered_generation_ecomin_hourly(self):
        date = self.local_start_of_today()

        with api_vcr.use_cassette(
            f"get_day_ahead_offered_generation_ecomin_hourly_{date.date()}",
        ):
            df = self.iso.get_day_ahead_offered_generation_ecomin_hourly(date)

        self._check_day_ahead_offered_generation_hourly(
            df,
            [
                "Interval Start",
                "Interval End",
                "Region",
                "Must Run",
                "Economic",
                "Emergency",
            ],
        )

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )

    @pytest.mark.integration
    def test_get_day_ahead_generation_fuel_type_hourly(self):
        date = self.local_start_of_today()

        with api_vcr.use_cassette(
            f"get_day_ahead_generation_fuel_type_hourly_{date.date()}",
        ):
            df = self.iso.get_day_ahead_generation_fuel_type_hourly(date)

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Region",
            "Total",
            "Coal",
            "Gas",
            "Nuclear",
            "Water",
            "Wind",
            "Solar",
            "Other",
            "Storage",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                assert df[col].dtype == "float64"

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    def _check_test_get_real_time_cleared_demand(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Cleared Demand",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End"]:
                assert df[col].dtype == "float64"

    @pytest.mark.integration
    def test_get_real_time_cleared_demand_daily(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(f"get_real_time_cleared_demand_daily_{date.date()}"):
            df = self.iso.get_real_time_cleared_demand_daily(date)

        self._check_test_get_real_time_cleared_demand(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    @pytest.mark.integration
    def test_get_real_time_cleared_demand_hourly(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(f"get_real_time_cleared_demand_hourly_{date.date()}"):
            df = self.iso.get_real_time_cleared_demand_hourly(date)

        self._check_test_get_real_time_cleared_demand(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(days=1)

    def _check_real_time_cleared_generation(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Generation Cleared",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                assert df[col].dtype == "float64"

    # @pytest.mark.integration
    @pytest.mark.skip(reason="MISO returns wrong data for a specific date")
    def test_get_real_time_cleared_generation_hourly(self):
        date = self.local_start_of_today() - pd.Timedelta(days=2)

        with api_vcr.use_cassette(
            f"get_real_time_cleared_generation_hourly_{date.date()}",
        ):
            df = self.iso.get_real_time_cleared_generation_hourly(date)

        self._check_real_time_cleared_generation(df)

        # Unfortunately MISO returns wrong data for a specific date. 🤯
        # assert df["Interval Start"].min() == start
        # assert df["Interval Start"].max() == start + pd.Timedelta(
        #     hours=23,
        # )

    @pytest.mark.integration
    def test_get_real_time_offered_generation_ecomax_hourly(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"get_real_time_offered_generation_ecomax_hourly_{date.date()}",
        ):
            df = self.iso.get_real_time_offered_generation_ecomax_hourly(date)

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Offered FRAC Economic Max",
            "Offered Real Time Economic Max",
            "Offered Economic Max Delta",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End"]:
                assert df[col].dtype == "float64"

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    @pytest.mark.integration
    def test_get_real_time_committed_generation_ecomax_hourly(self):
        date = self.local_start_of_today() - pd.Timedelta(days=2)

        with api_vcr.use_cassette(
            f"get_real_time_committed_generation_ecomax_hourly_{date.date()}",
        ):
            df = self.iso.get_real_time_committed_generation_ecomax_hourly(date)

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Committed FRAC Economic Max",
            "Committed Real Time Economic Max",
            "Committed Economic Max Delta",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End"]:
                assert df[col].dtype == "float64"

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    @pytest.mark.integration
    def test_get_real_time_generation_fuel_type_hourly(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"get_real_time_generation_fuel_type_hourly_{date.date()}",
        ):
            df = self.iso.get_real_time_generation_fuel_type_hourly(date)

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Region",
            "Total",
            "Coal",
            "Gas",
            "Nuclear",
            "Water",
            "Wind",
            "Solar",
            "Other",
            "Storage",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                assert df[col].dtype == "float64"

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    def _check_test_actual_load(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Region",
            "Load",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in ["Interval Start", "Interval End", "Region"]:
                assert df[col].dtype == "float64"

    @pytest.mark.integration
    def test_get_actual_load_hourly(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(f"get_actual_load_hourly_{date.date()}"):
            df = self.iso.get_actual_load_hourly(date)

        self._check_test_actual_load(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    @pytest.mark.integration
    def test_get_actual_load_daily(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(f"get_actual_load_daily_{date.date()}"):
            df = self.iso.get_actual_load_daily(date)

        self._check_test_actual_load(df)

        assert df["Interval Start"].min() == date
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    def _check_test_actual_load_local_resource_zone(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Local Resource Zone",
            "Load",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in [
                "Interval Start",
                "Interval End",
                "Local Resource Zone",
            ]:
                assert df[col].dtype == "float64"

    def test_get_actual_load_hourly_local_resource_zone(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"get_actual_load_hourly_local_resource_zone_{date.date()}",
        ):
            df = self.iso.get_actual_load_hourly(
                date,
                geo_resolution="localResourceZone",
            )

        self._check_test_actual_load_local_resource_zone(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    def test_get_actual_load_daily_local_resource_zone(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"get_actual_load_daily_local_resource_zone_{date.date()}",
        ):
            df = self.iso.get_actual_load_daily(
                date,
                geo_resolution="localResourceZone",
            )

        self._check_test_actual_load_local_resource_zone(df)

        assert df["Interval Start"].min() == date
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    def _check_test_medium_term_load_forecast(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Region",
            "Local Resource Zone",
            "Load Forecast",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Region",
                "Local Resource Zone",
            ]:
                assert df[col].dtype == "float64"

    @pytest.mark.integration
    def test_get_medium_term_load_forecast_hourly(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"get_medium_term_load_forecast_hourly_{date.date()}",
        ):
            df = self.iso.get_medium_term_load_forecast_hourly(date)

        self._check_test_medium_term_load_forecast(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    @pytest.mark.integration
    def test_get_medium_term_load_forecast_hourly_with_publish_time(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)
        publish_time = date - pd.Timedelta(days=2)

        with api_vcr.use_cassette(
            f"test_get_medium_term_load_forecast_hourly_with_publish_time_{date.date()}",
        ):
            df = self.iso.get_medium_term_load_forecast_hourly(
                date,
                publish_time=publish_time,
            )

        self._check_test_medium_term_load_forecast(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )
        assert df["Publish Time"].min() == publish_time

    @pytest.mark.integration
    def test_get_medium_term_load_forecast_daily(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(f"get_medium_term_load_forecast_daily_{date.date()}"):
            df = self.iso.get_medium_term_load_forecast_daily(date)

        self._check_test_medium_term_load_forecast(df)

        assert df["Interval Start"].min() == date
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    def _check_test_actual_load_hourly_pivoted(self, df):
        expected_columns = [
            "Interval Start",
            "Interval End",
            "LRZ1",
            "LRZ2 7",
            "LRZ3 5",
            "LRZ4",
            "LRZ6",
            "LRZ8 9 10",
            "MISO",
        ]
        assert df.columns.tolist() == expected_columns

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        # All load columns should be float
        for col in expected_columns[2:]:
            assert df[col].dtype == "float64"

    def test_get_actual_load_hourly_pivoted(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"get_actual_load_hourly_pivoted_{date.date()}",
        ):
            df = self.iso.get_actual_load_hourly_pivoted(date)

        self._check_test_actual_load_hourly_pivoted(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

        # Verify MISO total is sum of all LRZ zones
        calculated_miso = df[
            ["LRZ1", "LRZ2 7", "LRZ3 5", "LRZ4", "LRZ6", "LRZ8 9 10"]
        ].sum(axis=1)
        pd.testing.assert_series_equal(
            df["MISO"],
            calculated_miso,
            check_names=False,
        )

    def _check_test_medium_term_load_forecast_hourly_aggregated(self, df):
        expected_columns = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "LRZ1 MTLF",
            "LRZ2_7 MTLF",
            "LRZ3_5 MTLF",
            "LRZ4 MTLF",
            "LRZ6 MTLF",
            "LRZ8_9_10 MTLF",
            "MISO MTLF",
        ]
        assert df.columns.tolist() == expected_columns

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"
        assert df["Publish Time"].dtype == "datetime64[ns, EST]"

        # All load columns should be float
        for col in expected_columns[3:]:
            assert df[col].dtype == "float64"

    def test_get_medium_term_load_forecast_hourly_aggregated(self):
        date = self.local_start_of_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"get_medium_term_load_forecast_hourly_aggregated_{date.date()}",
        ):
            df = self.iso.get_medium_term_load_forecast_hourly_aggregated(date)

        self._check_test_medium_term_load_forecast_hourly_aggregated(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

        # Verify MISO total is sum of all LRZ zones
        calculated_miso = df[
            [
                "LRZ1 MTLF",
                "LRZ2_7 MTLF",
                "LRZ3_5 MTLF",
                "LRZ4 MTLF",
                "LRZ6 MTLF",
                "LRZ8_9_10 MTLF",
            ]
        ].sum(axis=1)
        pd.testing.assert_series_equal(
            df["MISO MTLF"],
            calculated_miso,
            check_names=False,
        )

    def _check_test_outage_forecast(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Region",
            "Outage Forecast",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in df.columns:
            if col not in [
                "Interval Start",
                "Interval End",
                "Region",
            ]:
                assert df[col].dtype == "float64"

    def test_get_outage_forecast(self):
        # Outage forecast is for future dates - use tomorrow
        date = self.local_start_of_today() + pd.Timedelta(days=1)

        with api_vcr.use_cassette(f"get_outage_forecast_{date.date()}"):
            df = self.iso.get_outage_forecast(date)

        self._check_test_outage_forecast(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    def test_get_outage_forecast_past_date_raises_error(self):
        # Try to get outage forecast for yesterday - should raise NotSupported
        yesterday = self.local_start_of_today() - pd.Timedelta(days=1)

        with pytest.raises(NotSupported, match="only available for future dates"):
            self.iso.get_outage_forecast(yesterday)

    def _check_test_look_ahead_hourly(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Region",
            "MTLF",
            "Outage",
        ]

        assert df["Interval Start"].dtype == "datetime64[ns, EST]"
        assert df["Interval End"].dtype == "datetime64[ns, EST]"

        for col in ["MTLF", "Outage"]:
            assert df[col].dtype == "float64"

    def test_get_look_ahead_hourly(self):
        # Look ahead is for today and future dates
        date = self.local_start_of_today()

        with api_vcr.use_cassette(f"get_look_ahead_hourly_{date.date()}"):
            df = self.iso.get_look_ahead_hourly(date)

        self._check_test_look_ahead_hourly(df)

        assert df["Interval Start"].min() == date
        assert df["Interval Start"].max() == date + pd.Timedelta(
            hours=23,
        )
        assert df["Interval End"].max() == date + pd.Timedelta(
            days=1,
        )

    def test_get_look_ahead_hourly_past_date_raises_error(self):
        # Try to get look ahead for yesterday - should raise NotSupported
        yesterday = self.local_start_of_today() - pd.Timedelta(days=1)

        with pytest.raises(NotSupported, match="only available for future dates"):
            self.iso.get_look_ahead_hourly(yesterday)

    def test_get_pricing_nodes(self):
        today = pd.Timestamp(self.local_today())

        with api_vcr.use_cassette(f"get_pricing_nodes_{today.strftime('%Y-%m-%d')}"):
            df = self.iso.get_pricing_nodes()

        assert df.shape[0] > 0

        assert df.columns.tolist() == ["Node", "Location Type"]

        for col in df.columns:
            if col not in ["Node", "Location Type"]:
                assert df[col].dtype == "str"

    def test_get_pricing_nodes_by_date(self):
        today = pd.Timestamp(self.local_today())
        date = "latest"

        with api_vcr.use_cassette(
            f"get_pricing_nodes_by_date_{today.strftime('%Y-%m-%d')}",
        ):
            df = self.iso.get_pricing_nodes(date)

        assert df.shape[0] > 0

        assert df.columns.tolist() == ["Node", "Location Type"]

        for col in df.columns:
            if col not in ["Node", "Location Type"]:
                assert df[col].dtype == "str"

    def test_get_pricing_nodes_by_date_range(self):
        today = pd.Timestamp(self.local_today())
        date = today - pd.Timedelta(days=100)
        end = today

        with api_vcr.use_cassette(
            f"get_pricing_nodes_by_date_range_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}",
        ):
            df = self.iso.get_pricing_nodes(date, end)

        assert df.shape[0] > 0

        assert df.columns.tolist() == ["Node", "Location Type"]

        for col in df.columns:
            if col not in ["Node", "Location Type"]:
                assert df[col].dtype == "str"


class TestMISOAPIRetryMechanism:
    """Tests for the MISOAPI retry mechanism with exponential backoff."""

    def test_make_request_with_retry_success_on_first_attempt(self):
        """Test successful request on first attempt (200 OK)."""
        api = MISOAPI(max_retries=3)

        mock_response = Mock()
        mock_response.ok = True
        mock_response.status_code = 200

        with patch("requests.get", return_value=mock_response) as mock_get:
            response = api._make_request_with_retry(
                url="https://example.com/test",
                headers={"Authorization": "test"},
            )

        assert response == mock_response
        assert mock_get.call_count == 1

    @patch("time.sleep")
    def test_make_request_with_retry_5xx_then_success(self, mock_sleep):
        """Test retry on 5xx server errors with exponential backoff."""
        api = MISOAPI(max_retries=3)

        # Create mock responses: 500 error, 502 error, then success
        error_response_1 = Mock()
        error_response_1.ok = False
        error_response_1.status_code = 500
        error_response_1.reason = "Internal Server Error"

        error_response_2 = Mock()
        error_response_2.ok = False
        error_response_2.status_code = 502
        error_response_2.reason = "Bad Gateway"

        success_response = Mock()
        success_response.ok = True
        success_response.status_code = 200

        with patch(
            "requests.get",
            side_effect=[error_response_1, error_response_2, success_response],
        ) as mock_get:
            response = api._make_request_with_retry(
                url="https://example.com/test",
                headers={"Authorization": "test"},
            )

        assert response == success_response
        assert mock_get.call_count == 3
        # Verify exponential backoff: 2^1=2 seconds, then 2^2=4 seconds
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(2)  # First retry: 2^(0+1) = 2 seconds
        mock_sleep.assert_any_call(4)  # Second retry: 2^(1+1) = 4 seconds

    @patch("time.sleep")
    def test_make_request_with_retry_429_then_success(self, mock_sleep):
        """Test retry on 429 Too Many Requests (rate limiting)."""
        api = MISOAPI(max_retries=2)

        error_response = Mock()
        error_response.ok = False
        error_response.status_code = 429
        error_response.reason = "Too Many Requests"

        success_response = Mock()
        success_response.ok = True
        success_response.status_code = 200

        with patch(
            "requests.get",
            side_effect=[error_response, success_response],
        ) as mock_get:
            response = api._make_request_with_retry(
                url="https://example.com/test",
                headers={"Authorization": "test"},
            )

        assert response == success_response
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(2)  # 2^(0+1) = 2 seconds

    @patch("time.sleep")
    def test_make_request_with_retry_custom_exponential_base(self, mock_sleep):
        """Test that custom exponential_base affects backoff timing."""
        api = MISOAPI(max_retries=2, exponential_base=3)

        error_response = Mock()
        error_response.ok = False
        error_response.status_code = 503
        error_response.reason = "Service Unavailable"

        success_response = Mock()
        success_response.ok = True
        success_response.status_code = 200

        with patch(
            "requests.get",
            side_effect=[error_response, success_response],
        ) as mock_get:
            response = api._make_request_with_retry(
                url="https://example.com/test",
                headers={"Authorization": "test"},
            )

        assert response == success_response
        assert mock_get.call_count == 2
        # With exponential_base=3: 3^(0+1) = 3 seconds
        mock_sleep.assert_called_once_with(3)

    @patch("time.sleep")
    def test_make_request_with_retry_exhausted_on_5xx(self, mock_sleep):
        """Test that exception is raised when all retries are exhausted on 5xx."""
        api = MISOAPI(max_retries=2)

        error_response = Mock()
        error_response.ok = False
        error_response.status_code = 500
        error_response.reason = "Internal Server Error"

        with patch("requests.get", return_value=error_response) as mock_get:
            with pytest.raises(requests.HTTPError, match="500"):
                api._make_request_with_retry(
                    url="https://example.com/test",
                    headers={"Authorization": "test"},
                )

        # Initial attempt + 2 retries = 3 total attempts
        assert mock_get.call_count == 3
        # Sleep called between retries (not after the last failed attempt)
        assert mock_sleep.call_count == 2

    def test_make_request_with_retry_no_retry_on_404(self):
        """Test that 404 Not Found is raised immediately without retry."""
        api = MISOAPI(max_retries=3)

        error_response = Mock()
        error_response.ok = False
        error_response.status_code = 404
        error_response.reason = "Not Found"
        error_response.raise_for_status.side_effect = requests.HTTPError(
            "404 Not Found",
        )

        with patch("requests.get", return_value=error_response) as mock_get:
            with pytest.raises(requests.HTTPError, match="404"):
                api._make_request_with_retry(
                    url="https://example.com/test",
                    headers={"Authorization": "test"},
                )

        # Should only try once - no retries for 404
        assert mock_get.call_count == 1

    def test_make_request_with_retry_no_retry_on_401(self):
        """Test that 401 Unauthorized is raised immediately without retry."""
        api = MISOAPI(max_retries=3)

        error_response = Mock()
        error_response.ok = False
        error_response.status_code = 401
        error_response.reason = "Unauthorized"
        error_response.raise_for_status.side_effect = requests.HTTPError(
            "401 Unauthorized",
        )

        with patch("requests.get", return_value=error_response) as mock_get:
            with pytest.raises(requests.HTTPError, match="401"):
                api._make_request_with_retry(
                    url="https://example.com/test",
                    headers={"Authorization": "test"},
                )

        # Should only try once - no retries for 401
        assert mock_get.call_count == 1

    @patch("time.sleep")
    def test_make_request_with_retry_zero_retries(self, mock_sleep):
        """Test with max_retries=0 (only initial attempt, no retries)."""
        api = MISOAPI(max_retries=0)

        error_response = Mock()
        error_response.ok = False
        error_response.status_code = 503
        error_response.reason = "Service Unavailable"

        with patch("requests.get", return_value=error_response) as mock_get:
            with pytest.raises(requests.HTTPError):
                api._make_request_with_retry(
                    url="https://example.com/test",
                    headers={"Authorization": "test"},
                )

        # Only 1 attempt with max_retries=0
        assert mock_get.call_count == 1
        # No sleep called since no retries
        assert mock_sleep.call_count == 0

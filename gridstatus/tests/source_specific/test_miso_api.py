import pandas as pd
import pytest

from gridstatus.base import Markets
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
        start = (self.local_now() - pd.DateOffset(days=6)).floor("h")
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
        start = (self.local_now() - pd.DateOffset(days=5)).floor("5min")
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
            f"test_get_interchange_hourly_{start}_{end}",
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

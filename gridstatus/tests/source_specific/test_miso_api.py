import pandas as pd

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
        assert df["Market"].unique() == [market_value]

    """get_lmp_day_ahead_hourly_ex_ante"""

    @api_vcr.use_cassette("test_get_lmp_day_ahead_hourly_ex_ante")
    def test_get_lmp_day_ahead_hourly_ex_ante_date_range(self):
        start = self.local_now() - pd.DateOffset(days=2)
        end = start + pd.Timedelta(hours=12)
        df = self.iso.get_lmp_day_ahead_hourly_ex_ante(start, end)

        self._check_lmp(df, market_value=Markets.DAY_AHEAD_HOURLY_EX_ANTE.value)

        assert df["Interval Start"].min() == start.floor("H")
        assert df["Interval Start"].max() == end.floor("H")

    """get_lmp_day_ahead_hourly_ex_post"""

    @api_vcr.use_cassette("test_get_lmp_day_ahead_hourly_ex_post")
    def test_get_lmp_day_ahead_hourly_ex_post_date_range(self):
        start = self.local_now() - pd.DateOffset(days=2)
        end = start + pd.Timedelta(hours=12)
        df = self.iso.get_lmp_day_ahead_hourly_ex_post(start, end)

        self._check_lmp(df, market_value=Markets.DAY_AHEAD_HOURLY_EX_POST.value)

        assert df["Interval Start"].min() == start.floor("H")
        assert df["Interval Start"].max() == end.floor("H")

    """get_lmp_real_time_hourly_ex_post_prelim"""

    @api_vcr.use_cassette("test_get_lmp_real_time_hourly_ex_post_prelim")
    def test_get_lmp_real_time_hourly_ex_post_prelim_date_range(self):
        start = self.local_now() - pd.DateOffset(days=2)
        end = start + pd.Timedelta(hours=12)
        df = self.iso.get_lmp_real_time_hourly_ex_post_prelim(start, end)

        self._check_lmp(df, market_value=Markets.REAL_TIME_HOURLY_EX_POST_PRELIM.value)

        assert df["Interval Start"].min() == start.floor("H")
        assert df["Interval Start"].max() == end.floor("H")

    """get_lmp_real_time_hourly_ex_post_final"""

    @api_vcr.use_cassette("test_get_lmp_real_time_hourly_ex_post_final")
    def test_get_lmp_real_time_hourly_ex_post_final_date_range(self):
        start = self.local_now() - pd.DateOffset(days=2)
        end = start + pd.Timedelta(hours=12)
        df = self.iso.get_lmp_real_time_hourly_ex_post_final(start, end)

        self._check_lmp(df, market_value=Markets.REAL_TIME_HOURLY_EX_POST_FINAL.value)

        assert df["Interval Start"].min() == start.floor("H")
        assert df["Interval Start"].max() == end.floor("H")

    """get_lmp_real_time_5_min_ex_ante"""

    @api_vcr.use_cassette("test_get_lmp_real_time_5_min_ex_ante")
    def test_get_lmp_real_time_5_min_ex_ante_date_range(self):
        start = self.local_now() - pd.DateOffset(days=2)
        end = start + pd.Timedelta(hours=2)
        df = self.iso.get_lmp_real_time_5_min_ex_ante(start, end)

        self._check_lmp(df, market_value=Markets.REAL_TIME_5_MIN_EX_ANTE.value)

        assert df["Interval Start"].min() == start.floor("5T")
        assert df["Interval Start"].max() == end.floor("5T")

    """get_lmp_real_time_5_min_ex_post_prelim"""

    @api_vcr.use_cassette("test_get_lmp_real_time_5_min_ex_post_prelim")
    def test_get_lmp_real_time_5_min_ex_post_prelim_date_range(self):
        start = self.local_now() - pd.DateOffset(days=2)
        end = start + pd.Timedelta(hours=2)
        df = self.iso.get_lmp_real_time_5_min_ex_post_prelim(start, end)

        self._check_lmp(df, market_value=Markets.REAL_TIME_5_MIN_EX_POST_PRELIM.value)

        assert df["Interval Start"].min() == start.floor("5T")
        assert df["Interval Start"].max() == end.floor("5T")

    """get_lmp_real_time_5_min_ex_post_final"""

    @api_vcr.use_cassette("test_get_lmp_real_time_5_min_ex_post_final")
    def test_get_lmp_real_time_5_min_ex_post_final_date_range(self):
        start = self.local_now() - pd.DateOffset(days=2)
        end = start + pd.Timedelta(hours=2)
        df = self.iso.get_lmp_real_time_5_min_ex_post_final(start, end)

        self._check_lmp(df, market_value=Markets.REAL_TIME_5_MIN_EX_POST_FINAL.value)

        assert df["Interval Start"].min() == start.floor("5T")
        assert df["Interval Start"].max() == end.floor("5T")

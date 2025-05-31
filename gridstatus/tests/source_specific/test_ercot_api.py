import datetime

import pandas as pd
import pytest

from gridstatus.base import Markets
from gridstatus.ercot import ELECTRICAL_BUS_LOCATION_TYPE
from gridstatus.ercot_60d_utils import DAM_RESOURCE_AS_OFFERS_COLUMNS
from gridstatus.ercot_api.api_parser import VALID_VALUE_TYPES
from gridstatus.ercot_api.ercot_api import (
    HISTORICAL_DAYS_THRESHOLD,
    ErcotAPI,
)
from gridstatus.ercot_constants import (
    SOLAR_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS,
    SOLAR_ACTUAL_AND_FORECAST_COLUMNS,
    WIND_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS,
    WIND_ACTUAL_AND_FORECAST_COLUMNS,
)
from gridstatus.tests.base_test_iso import TestHelperMixin
from gridstatus.tests.source_specific.test_ercot import (
    check_60_day_dam_disclosure,
    check_60_day_sced_disclosure,
)
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="ercot_api",
    record_mode=RECORD_MODE,
)


class TestErcotAPI(TestHelperMixin):
    @classmethod
    def setup_class(cls):
        # https://docs.pytest.org/en/stable/how-to/xunit_setup.html
        # Runs before all tests in this class
        cls.iso = ErcotAPI(sleep_seconds=3, max_retries=5)

    """utils"""

    def test_handle_end_date(self):
        dst_start = self.local_start_of_day(datetime.date(2021, 3, 14))

        assert self.iso._handle_end_date(
            date=dst_start,
            end=None,
            days_to_add_if_no_end=1,
        ) == self.local_start_of_day(datetime.date(2021, 3, 15))

        assert self.iso._handle_end_date(
            date=dst_start,
            end=None,
            days_to_add_if_no_end=-1,
        ) == self.local_start_of_day(datetime.date(2021, 3, 13))

        assert self.iso._handle_end_date(
            date=dst_start,
            end=dst_start,
            days_to_add_if_no_end=1,
        ) == self.local_start_of_day(datetime.date(2021, 3, 14))

        dst_end = self.local_start_of_day(datetime.date(2021, 11, 7))

        assert self.iso._handle_end_date(
            date=dst_end,
            end=None,
            days_to_add_if_no_end=1,
        ) == self.local_start_of_day(datetime.date(2021, 11, 8))

        assert self.iso._handle_end_date(
            date=dst_end,
            end=None,
            days_to_add_if_no_end=-12,
        ) == self.local_start_of_day(datetime.date(2021, 10, 26))

        assert self.iso._handle_end_date(
            date=dst_end,
            end=dst_end,
            days_to_add_if_no_end=1,
        ) == self.local_start_of_day(datetime.date(2021, 11, 7))

    """get_wind_actual_and_forecast_hourly"""

    def _check_wind_actual_and_forecast_hourly(self, df):
        assert df.columns.tolist() == WIND_ACTUAL_AND_FORECAST_COLUMNS
        assert (df["Interval End"] - df["Interval Start"]).eq(pd.Timedelta("1h")).all()

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_wind_actual_and_forecast_hourly_today.yaml")
    def test_get_wind_actual_and_forecast_hourly_today(self):
        df = self.iso.get_wind_actual_and_forecast_hourly("today")

        # The data should start at the beginning of two days ago
        assert df[
            "Interval Start"
        ].min() == self.local_start_of_today() - pd.DateOffset(days=2)

        self._check_wind_actual_and_forecast_hourly(df)

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_wind_actual_and_forecast_hourly_latest.yaml")
    def test_get_wind_actual_and_forecast_hourly_latest(self):
        df = self.iso.get_wind_actual_and_forecast_hourly("latest")

        assert df["Publish Time"].nunique() == 1
        self._check_wind_actual_and_forecast_hourly(df)

    @pytest.mark.integration
    @api_vcr.use_cassette(
        "test_get_wind_actual_and_forecast_hourly_date_range.yaml",
    )
    def test_get_wind_actual_and_forecast_hourly_date_range(self):
        date = self.local_today() - pd.DateOffset(days=HISTORICAL_DAYS_THRESHOLD * 3)
        end = date + pd.Timedelta(hours=2)

        df = self.iso.get_wind_actual_and_forecast_hourly(date, end, verbose=True)

        self._check_wind_actual_and_forecast_hourly(df)

        assert df["Publish Time"].nunique() == 2

        assert df["Interval Start"].min() == self.local_start_of_day(
            date.date(),
        ) - pd.DateOffset(days=2)
        assert df["Interval End"].max() >= self.local_start_of_day(
            date.date(),
        ) + pd.DateOffset(days=7)

    """get_wind_actual_and_forecast_by_geographical_region_hourly"""

    def _check_wind_actual_and_forecast_by_geographical_region_hourly(self, df):
        assert (
            df.columns.tolist()
            == WIND_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS
        )

        assert (df["Interval End"] - df["Interval Start"]).eq(pd.Timedelta("1h")).all()

    @pytest.mark.integration
    @api_vcr.use_cassette(
        "test_get_wind_actual_and_forecast_by_geographical_region_hourly_today.yaml",
    )
    def test_get_wind_actual_and_forecast_by_geographical_region_hourly_today(self):
        df = self.iso.get_wind_actual_and_forecast_by_geographical_region_hourly(
            "today",
        )

        # The data should start at the beginning of two days ago
        assert df[
            "Interval Start"
        ].min() == self.local_start_of_today() - pd.DateOffset(days=2)

        self._check_wind_actual_and_forecast_by_geographical_region_hourly(df)

    @pytest.mark.integration
    @api_vcr.use_cassette(
        "test_get_wind_actual_and_forecast_by_geographical_region_hourly_latest.yaml",
    )
    def test_get_wind_actual_and_forecast_by_geographical_region_hourly_latest(self):
        df = self.iso.get_wind_actual_and_forecast_by_geographical_region_hourly(
            "latest",
        )

        assert df["Publish Time"].nunique() == 1
        self._check_wind_actual_and_forecast_by_geographical_region_hourly(df)

    @pytest.mark.integration
    @api_vcr.use_cassette(
        "test_get_wind_actual_and_forecast_by_geographical_region_hourly_date_range.yaml",  # noqa: E501
    )
    def test_get_wind_actual_and_forecast_by_geographical_region_hourly_date_range(
        self,
    ):
        date = self.local_today() - pd.DateOffset(days=HISTORICAL_DAYS_THRESHOLD * 3)
        end = date + pd.Timedelta(hours=2)

        df = self.iso.get_wind_actual_and_forecast_by_geographical_region_hourly(
            date,
            end,
            verbose=True,
        )

        self._check_wind_actual_and_forecast_by_geographical_region_hourly(df)

        assert df["Publish Time"].nunique() == 2

        assert df["Interval Start"].min() == self.local_start_of_day(
            date.date(),
        ) - pd.DateOffset(days=2)
        assert df["Interval End"].max() >= self.local_start_of_day(
            date.date(),
        ) + pd.DateOffset(days=7)

    """get_solar_actual_and_forecast_hourly"""

    def _check_solar_actual_and_forecast_hourly(self, df):
        assert df.columns.tolist() == SOLAR_ACTUAL_AND_FORECAST_COLUMNS

        assert (df["Interval End"] - df["Interval Start"]).eq(pd.Timedelta("1h")).all()

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_solar_actual_and_forecast_hourly_today.yaml")
    def test_get_solar_actual_and_forecast_hourly_today(self):
        df = self.iso.get_solar_actual_and_forecast_hourly("today")

        # We don't know the exact number of publish times
        # The data should start at the beginning of two days ago
        assert df[
            "Interval Start"
        ].min() == self.local_start_of_today() - pd.DateOffset(days=2)

        self._check_solar_actual_and_forecast_hourly(df)

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_solar_actual_and_forecast_hourly_latest.yaml")
    def test_get_solar_actual_and_forecast_hourly_latest(self):
        df = self.iso.get_solar_actual_and_forecast_hourly("latest")

        assert df["Publish Time"].nunique() == 1
        self._check_solar_actual_and_forecast_hourly(df)

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_solar_actual_and_forecast_hourly_date_range.yaml")
    def test_get_solar_actual_and_forecast_hourly_date_range(self):
        date = self.local_today() - pd.DateOffset(days=HISTORICAL_DAYS_THRESHOLD * 3)
        end = date + pd.Timedelta(hours=2)

        df = self.iso.get_solar_actual_and_forecast_hourly(date, end, verbose=True)

        self._check_solar_actual_and_forecast_hourly(df)

        assert df["Publish Time"].nunique() == 2

        assert df["Interval Start"].min() == self.local_start_of_day(
            date.date(),
        ) - pd.DateOffset(days=2)
        assert df["Interval End"].max() >= self.local_start_of_day(
            date.date(),
        ) + pd.DateOffset(days=7)

    """get_solar_actual_and_forecast_by_geographical_region_hourly"""

    def _check_solar_actual_and_forecast_by_geographical_region_hourly(self, df):
        assert (
            df.columns.tolist()
            == SOLAR_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS
        )
        assert (df["Interval End"] - df["Interval Start"]).eq(pd.Timedelta("1h")).all()

    @pytest.mark.integration
    @api_vcr.use_cassette(
        "test_get_solar_actual_and_forecast_by_geographical_region_hourly_today.yaml",
    )
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_today(self):
        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            "today",
        )

        # We don't know the exact number of publish times
        # The data should start at the beginning of two days ago
        assert df[
            "Interval Start"
        ].min() == self.local_start_of_today() - pd.DateOffset(days=2)

        self._check_solar_actual_and_forecast_by_geographical_region_hourly(df)

    @pytest.mark.integration
    @api_vcr.use_cassette(
        "test_get_solar_actual_and_forecast_by_geographical_region_hourly_latest.yaml",
    )
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_latest(self):
        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            "latest",
        )

        assert df["Publish Time"].nunique() == 1
        self._check_solar_actual_and_forecast_by_geographical_region_hourly(df)

    @pytest.mark.integration
    @api_vcr.use_cassette(
        "test_get_solar_actual_and_forecast_by_geographical_region_hourly_date_range.yaml",  # noqa: E501
    )
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_date_range(
        self,
    ):
        date = self.local_today() - pd.DateOffset(days=HISTORICAL_DAYS_THRESHOLD * 3)
        end = date + pd.Timedelta(hours=2)

        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            date,
            end,
            verbose=True,
        )

        self._check_solar_actual_and_forecast_by_geographical_region_hourly(df)

        assert df["Publish Time"].nunique() == 2

        assert df["Interval Start"].min() == self.local_start_of_day(
            date.date(),
        ) - pd.DateOffset(days=2)
        assert df["Interval End"].max() >= self.local_start_of_day(
            date.date(),
        ) + pd.DateOffset(days=7)

    """get_as_prices"""

    def _check_as_prices(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Market",
            "Non-Spinning Reserves",
            "Regulation Down",
            "Regulation Up",
            "Responsive Reserves",
            "ERCOT Contingency Reserve Service",
        ]

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

        assert (df["Market"] == "DAM").all()
        assert ((df["Interval End"] - df["Interval Start"]) == pd.Timedelta("1h")).all()

    @pytest.mark.integration
    def test_get_as_prices_today_or_latest(self):
        df = self.iso.get_as_prices("today")

        self._check_as_prices(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        # Depending on time of day, the end date will be today or tomorrow
        assert df["Interval End"].max() in [
            self.local_start_of_today() + pd.DateOffset(days=1),
            self.local_start_of_today() + pd.DateOffset(days=2),
        ]

        assert self.iso.get_as_prices("latest").equals(df)

    @pytest.mark.integration
    def test_get_as_prices_historical_date(self):
        historical_date = datetime.date(2021, 3, 12)
        df = self.iso.get_as_prices(historical_date, verbose=True)

        self._check_as_prices(df)

        assert df["Interval Start"].min() == self.local_start_of_day(historical_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            historical_date,
        ) + pd.DateOffset(
            days=1,
        )

    @pytest.mark.integration
    def test_get_as_prices_historical_date_range(self):
        start_date = datetime.date(2021, 3, 8)
        end_date = datetime.date(2021, 3, 10)
        df = self.iso.get_as_prices(start_date, end_date, verbose=True)

        self._check_as_prices(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        # Not inclusive of end date
        assert df["Interval End"].max() == self.local_start_of_day(end_date)

    """get_as_reports"""

    def _check_as_reports(self, df, before_full_columns=False):
        # Earlier datasets only have these limited columns
        if before_full_columns:
            columns = [
                "Interval Start",
                "Interval End",
                "Total Cleared AS - RegUp",
                "Total Cleared AS - RegDown",
                "Total Cleared AS - NonSpin",
                "Total Self-Arranged AS - RegUp",
                "Total Self-Arranged AS - RegDown",
                "Total Self-Arranged AS - NonSpin",
                "Bid Curve - REGUP",
                "Bid Curve - REGDN",
                "Bid Curve - ONNS",
                "Bid Curve - OFFNS",
            ]
        else:
            columns = [
                "Interval Start",
                "Interval End",
                "Total Cleared AS - RRSPFR",
                "Total Cleared AS - RRSUFR",
                "Total Cleared AS - RRSFFR",
                "Total Cleared AS - ECRSM",
                "Total Cleared AS - ECRSS",
                "Total Cleared AS - RegUp",
                "Total Cleared AS - RegDown",
                "Total Cleared AS - NonSpin",
                "Total Self-Arranged AS - RRSPFR",
                "Total Self-Arranged AS - RRSUFR",
                "Total Self-Arranged AS - RRSFFR",
                "Total Self-Arranged AS - ECRSM",
                "Total Self-Arranged AS - ECRSS",
                "Total Self-Arranged AS - RegUp",
                "Total Self-Arranged AS - RegDown",
                "Total Self-Arranged AS - NonSpin",
                "Total Self-Arranged AS - NSPNM",
                "Bid Curve - RRSPFR",
                "Bid Curve - RRSUFR",
                "Bid Curve - RRSFFR",
                "Bid Curve - ECRSM",
                "Bid Curve - ECRSS",
                "Bid Curve - REGUP",
                "Bid Curve - REGDN",
                "Bid Curve - ONNS",
                "Bid Curve - OFFNS",
            ]

        assert df.columns.tolist() == columns

        bid_curve_columns = [
            "Bid Curve - RRSPFR",
            "Bid Curve - RRSUFR",
            "Bid Curve - RRSFFR",
            "Bid Curve - ECRSM",
            "Bid Curve - ECRSS",
            "Bid Curve - REGUP",
            "Bid Curve - REGDN",
            "Bid Curve - ONNS",
            "Bid Curve - OFFNS",
        ]

        for column in bid_curve_columns:
            if column in df.columns:
                # Column should be a list of lists
                first_non_null_value = df[column].dropna().iloc[0]
                assert isinstance(first_non_null_value, list)
                assert all(isinstance(x, list) for x in first_non_null_value)

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    @pytest.mark.integration
    def test_get_as_reports_today_or_latest_raises_error(self):
        with pytest.raises(ValueError) as error:
            self.iso.get_as_reports("today")
            assert str(error.value) == "Cannot get AS reports for 'latest' or 'today'"

        with pytest.raises(ValueError) as error:
            self.iso.get_as_reports("latest")
            assert str(error.value) == "Cannot get AS reports for 'latest' or 'today'"

    @pytest.mark.integration
    def test_get_as_reports_historical_date(self):
        historical_date = datetime.date(2022, 1, 1)
        df = self.iso.get_as_reports(historical_date, verbose=True)

        self._check_as_reports(df, before_full_columns=True)

        assert df["Interval Start"].min() == self.local_start_of_day(historical_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            historical_date,
        ) + pd.DateOffset(
            days=1,
        )

    @pytest.mark.integration
    def test_get_as_reports_historical_date_range(self):
        start_date = datetime.date(2021, 1, 1)
        end_date = datetime.date(2021, 1, 3)
        df = self.iso.get_as_reports(start_date, end_date, verbose=True)

        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        # Not inclusive of the end date
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
        )

        self._check_as_reports(df, before_full_columns=True)

    @api_vcr.use_cassette("test_get_as_reports_full_columns_21_days_ago.yaml")
    def test_get_as_reports_full_columns(self):
        df = self.iso.get_as_reports(
            self.local_start_of_today() - pd.DateOffset(days=21),
        )

        self._check_as_reports(df)

    @api_vcr.use_cassette("test_get_as_reports_dst_end_2024_11_03.yaml")
    def test_get_as_reports_dst_end(self):
        df = self.iso.get_as_reports("2024-11-03")

        self._check_as_reports(df)

        # Check for the repeated hour
        assert {"2024-11-03 01:00:00-05:00", "2024-11-03 01:00:00-06:00"}.issubset(
            set(df["Interval Start"].astype(str).unique()),
        )

    """get_as_plan"""

    def _check_as_plan(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "NSPIN",
            "REGDN",
            "REGUP",
            "RRS",
            "ECRS",
        ]

    @pytest.mark.integration
    def test_get_as_plan_today_or_latest(self):
        df = self.iso.get_as_plan("today")

        self._check_as_plan(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

        assert df["Publish Time"].dt.date.unique().tolist() == [self.local_today()]

        assert self.iso.get_as_plan("latest").equals(df)

    @pytest.mark.integration
    def test_get_as_plan_historical_date(self):
        date = self.local_today() - pd.Timedelta(days=30)

        df = self.iso.get_as_plan(date)

        self._check_as_plan(df)

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=7)

        assert df["Publish Time"].dt.date.unique().tolist() == [date]

        assert df["ECRS"].notna().any()

    @pytest.mark.integration
    def test_get_as_plan_historical_date_range(self):
        start_date = self.local_today() - pd.Timedelta(days=30)
        end_date = start_date + pd.Timedelta(days=2)

        df = self.iso.get_as_plan(start_date, end_date)

        self._check_as_plan(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
            # Not inclusive of end date
        ) + pd.DateOffset(days=6)

        assert df["Publish Time"].dt.date.unique().tolist() == [
            start_date,
            (start_date + pd.DateOffset(days=1)).date(),
        ]

    @pytest.mark.integration
    def test_get_as_plan_before_ecrs(self):
        # Check that we add an ECRS column of nulls if it's not present
        date = "2012-05-01"

        df = self.iso.get_as_plan(date)

        self._check_as_plan(df)

        assert df["ECRS"].isna().all()

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
            # Earlier files only contain two days of data
        ) + pd.DateOffset(days=2)

        # First date of data
        date = "2010-12-29"
        df = self.iso.get_as_plan(date)
        self._check_as_plan(df)

    """get_lmp_by_settlement_point"""

    def _check_lmp_by_settlement_point(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "SCED Timestamp",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

        assert (df["Market"] == Markets.REAL_TIME_SCED.name).all()
        assert sorted(df["Location Type"].unique().tolist()) == [
            "Load Zone",
            "Load Zone DC Tie",
            "Resource Node",
            "Trading Hub",
        ]

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta("5min")
        ).all()

    @pytest.mark.integration
    def test_get_lmp_by_settlement_point_today_or_latest(self):
        df = self.iso.get_lmp_by_settlement_point("today")

        self._check_lmp_by_settlement_point(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() <= self.local_now()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_lmp_by_settlement_point_historical_date(self):
        historical_date = datetime.date(2021, 11, 6)
        df = self.iso.get_lmp_by_settlement_point(historical_date, verbose=True)

        self._check_lmp_by_settlement_point(df)

        assert df["Interval Start"].min() == self.local_start_of_day(historical_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            historical_date,
        ) + pd.DateOffset(
            days=1,
        )

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_lmp_by_settlement_point_historical_date_range(self):
        start_date = datetime.date(2021, 11, 12)
        end_date = datetime.date(2021, 11, 14)
        df = self.iso.get_lmp_by_settlement_point(start_date, end_date, verbose=True)

        self._check_lmp_by_settlement_point(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        assert df["Interval End"].max() == self.local_start_of_day(end_date)

    """get_hourly_resource_outage_capacity"""

    def _check_hourly_resource_outage_capacity(self, df):
        # New files
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Total Resource MW Zone South",
            "Total Resource MW Zone North",
            "Total Resource MW Zone West",
            "Total Resource MW Zone Houston",
            "Total Resource MW",
            "Total IRR MW Zone South",
            "Total IRR MW Zone North",
            "Total IRR MW Zone West",
            "Total IRR MW Zone Houston",
            "Total IRR MW",
            "Total New Equip Resource MW Zone South",
            "Total New Equip Resource MW Zone North",
            "Total New Equip Resource MW Zone West",
            "Total New Equip Resource MW Zone Houston",
            "Total New Equip Resource MW",
        ] or df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Total Resource MW",
            "Total IRR MW",
            "Total New Equip Resource MW",
        ]
        # Old files

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    @pytest.mark.integration
    def test_get_hourly_resource_outage_capacity_today_or_latest(self):
        df = self.iso.get_hourly_resource_outage_capacity("today")

        self._check_hourly_resource_outage_capacity(df)

        assert (df["Publish Time"].dt.date == self.local_today()).all()

        assert df["Interval Start"].min() <= self.local_start_of_today()
        assert df["Interval End"].max() >= self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

        assert self.iso.get_hourly_resource_outage_capacity("latest").equals(df)

    @pytest.mark.integration
    def test_get_hourly_resource_outage_capacity_historical_date(self):
        historical_date = datetime.date(2021, 3, 1)
        df = self.iso.get_hourly_resource_outage_capacity(historical_date, verbose=True)

        self._check_hourly_resource_outage_capacity(df)

        assert (df["Publish Time"].dt.date == historical_date).all()
        assert df["Publish Time"].nunique() == 24

        assert df["Interval Start"].min() == self.local_start_of_day(historical_date)
        assert df["Interval End"].max() >= self.local_start_of_day(
            historical_date,
        ) + pd.DateOffset(days=7)

    @pytest.mark.integration
    def test_get_hourly_resource_outage_capacity_historical_date_range(self):
        start_date = datetime.date(2021, 3, 15)
        end_date = datetime.date(2021, 3, 17)

        df = self.iso.get_hourly_resource_outage_capacity(
            start_date,
            end_date,
            verbose=True,
        )

        self._check_hourly_resource_outage_capacity(df)

        # Not inclusive of end date
        assert df["Publish Time"].dt.date.unique().tolist() == [
            start_date,
            (start_date + pd.DateOffset(days=1)).date(),
        ]
        assert df["Publish Time"].nunique() == 2 * 24

        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        assert df["Interval End"].max() >= self.local_start_of_day(
            end_date,
        ) + pd.DateOffset(days=6)

    """lmp_by_bus"""

    def _check_lmp_by_bus(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "SCED Timestamp",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]

        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"

        assert (df["Market"] == Markets.REAL_TIME_SCED.name).all()
        assert (df["Location Type"] == ELECTRICAL_BUS_LOCATION_TYPE).all()

        assert df.dtypes["LMP"] == "float64"

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=5)
        ).all()

    @pytest.mark.integration
    def test_get_lmp_by_bus_today(self):
        df = self.iso.get_lmp_by_bus("today", verbose=True)

        self._check_lmp_by_bus(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() <= self.local_now()

    @pytest.mark.integration
    def test_get_lmp_by_bus_latest(self):
        df = self.iso.get_lmp_by_bus("latest")

        self._check_lmp_by_bus(df)

        assert df["Interval Start"].min() >= self.local_now() - pd.Timedelta(minutes=30)
        assert df["Interval End"].max() <= self.local_now()

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_lmp_by_bus_historical_date(self):
        date = self.local_today() - pd.DateOffset(days=HISTORICAL_DAYS_THRESHOLD * 2)

        df = self.iso.get_lmp_by_bus(date, verbose=True)

        self._check_lmp_by_bus(df)

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=1)

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_lmp_by_bus_historical_date_range(self):
        start_date = self.local_today() - pd.DateOffset(
            days=HISTORICAL_DAYS_THRESHOLD * 3,
        )
        end_date = start_date + pd.DateOffset(days=2)

        df = self.iso.get_lmp_by_bus(start_date, end_date, verbose=True)

        self._check_lmp_by_bus(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        # Not inclusive of end date
        assert df["Interval End"].max() == self.local_start_of_day(end_date)

    """lmp_by_bus_dam"""

    def _check_lmp_by_bus_dam(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]

        assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
        assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"

        assert (df["Market"] == Markets.DAY_AHEAD_HOURLY.name).all()

        assert df.dtypes["Location"] == "object"
        assert (df["Location Type"] == ELECTRICAL_BUS_LOCATION_TYPE).all()

        assert df.dtypes["LMP"] == "float64"

        assert ((df["Interval End"] - df["Interval Start"]) == pd.Timedelta("1h")).all()

    @pytest.mark.integration
    def test_get_lmp_by_bus_dam_today_and_latest(self):
        df = self.iso.get_lmp_by_bus_dam("today")

        self._check_lmp_by_bus_dam(df)

        assert df["Interval Start"].min() == self.local_start_of_today()

        # The end date will depend on when this runs, so check if it's today or tomorrow
        assert df["Interval End"].max() in [
            (self.local_start_of_today() + pd.DateOffset(days=d)) for d in [1, 2]
        ]

        assert self.iso.get_lmp_by_bus_dam("latest").equals(df)

    @pytest.mark.integration
    def test_get_lmp_by_bus_dam_historical(self):
        past_date = self.local_start_of_today() - pd.DateOffset(
            days=HISTORICAL_DAYS_THRESHOLD * 2,
        )

        df = self.iso.get_lmp_by_bus_dam(past_date, verbose=True)

        self._check_lmp_by_bus_dam(df)

        assert df["Interval Start"].min() == past_date.normalize()
        assert df["Interval End"].max() == past_date.normalize() + pd.DateOffset(
            days=1,
        )

    @pytest.mark.integration
    def test_get_lmp_by_bus_dam_historical_range(self):
        past_date = self.local_start_of_today() - pd.DateOffset(
            days=HISTORICAL_DAYS_THRESHOLD * 3,
        )
        past_end_date = past_date + pd.DateOffset(days=2)

        df = self.iso.get_lmp_by_bus_dam(past_date, past_end_date, verbose=True)

        self._check_lmp_by_bus_dam(df)

        assert df["Interval Start"].min() == past_date.normalize()
        assert df["Interval End"].max() == past_end_date.normalize()

    @pytest.mark.integration
    def test_get_lmp_by_bus_dam_dst_end(self):
        date = "2024-11-03"

        df = self.iso.get_lmp_by_bus_dam(date)

        assert not df[["Interval Start", "Location"]].duplicated().any()

        # Check that 01:00 local time is duplicated
        unique_interval_strings = df["Interval Start"].astype(str).unique()
        assert len(unique_interval_strings) == 25

        assert "2024-11-03 01:00:00-05:00" in unique_interval_strings
        assert "2024-11-03 01:00:00-06:00" in unique_interval_strings

    @pytest.mark.integration
    def test_get_lmp_by_bus_dam_dst_start(self):
        date = "2024-03-10"

        df = self.iso.get_lmp_by_bus_dam(date)

        assert not df[["Interval Start", "Location"]].duplicated().any()

        # Check that there is a gap at 02:00 local time
        unique_interval_strings = df["Interval Start"].astype(str).unique()

        assert len(unique_interval_strings) == 23

        assert "2024-03-10 01:00:00-06:00" in unique_interval_strings
        # This hour does not exist
        assert "2024-03-10 02:00:00-06:00" not in unique_interval_strings
        assert "2024-03-10 03:00:00-05:00" in unique_interval_strings

    """shadow_prices_dam"""

    expected_shadow_prices_dam_columns = [
        "Interval Start",
        "Interval End",
        "Constraint ID",
        "Constraint Name",
        "Contingency Name",
        "Limiting Facility",
        "Constraint Limit",
        "Constraint Value",
        "Violation Amount",
        "Shadow Price",
        "From Station",
        "To Station",
        "From Station kV",
        "To Station kV",
    ]

    def _check_shadow_prices_dam(self, df):
        assert df.columns.tolist() == self.expected_shadow_prices_dam_columns

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

        assert (
            df.loc[df["Contingency Name"] == "BASE CASE", "Limiting Facility"]
            .isna()
            .all()
        )

    @pytest.mark.integration
    def test_get_shadow_prices_dam_today_and_latest(self):
        df = self.iso.get_shadow_prices_dam("today", verbose=True)

        self._check_shadow_prices_dam(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        # The end date will depend on when this runs, so check if it's end of today
        # or end of tomorrow
        assert df["Interval End"].max() in [
            self.local_start_of_today()
            + pd.DateOffset(
                days=d,
            )
            for d in [1, 2]
        ]

        assert self.iso.get_shadow_prices_dam("latest").equals(df)

    @pytest.mark.integration
    def test_get_shadow_prices_dam_historical(self):
        past_date = self.local_start_of_today() - pd.DateOffset(
            days=HISTORICAL_DAYS_THRESHOLD * 3,
        )
        df = self.iso.get_shadow_prices_dam(past_date, verbose=True)

        self._check_shadow_prices_dam(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date.date())
        assert df["Interval Start"].max() == self.local_start_of_day(
            past_date.date(),
        ) + pd.Timedelta(hours=23)

    @pytest.mark.integration
    def test_get_shadow_prices_dam_historical_range(self):
        past_date = self.local_start_of_today() - pd.DateOffset(
            days=HISTORICAL_DAYS_THRESHOLD * 4,
        )
        past_end_date = past_date + pd.DateOffset(days=2)

        df = self.iso.get_shadow_prices_dam(
            date=past_date,
            end=past_end_date,
            verbose=True,
        )

        self._check_shadow_prices_dam(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date.date())
        # The data ends at the end of the day before the end date
        assert df["Interval Start"].max() == self.local_start_of_day(
            past_end_date.date(),
        ) - pd.Timedelta(hours=1)

    """shadow_prices_sced"""

    # This dataset has interval timestamps even though it's a SCED dataset
    # because we add approximate intervals to SCED datasets.
    expected_shadow_prices_sced_columns = [
        "Interval Start",
        "Interval End",
        "SCED Timestamp",
        "Constraint ID",
        "Constraint Name",
        "Contingency Name",
        "Limiting Facility",
        "Shadow Price",
        "Max Shadow Price",
        "Limit",
        "Value",
        "Violated MW",
        "From Station",
        "To Station",
        "From Station kV",
        "To Station kV",
        "CCT Status",
    ]

    def _check_shadow_prices_sced(self, df):
        assert df.columns.tolist() == self.expected_shadow_prices_sced_columns

        time_cols = ["Interval Start", "Interval End", "SCED Timestamp"]

        for col in time_cols:
            assert isinstance(df.loc[0][col], pd.Timestamp)
            assert df.loc[0][col].tz is not None

        ordered_by_col = "SCED Timestamp"
        self._check_ordered_by_time(df, ordered_by_col)

    @pytest.mark.integration
    def test_get_shadow_prices_sced_today_and_latest(self):
        df = self.iso.get_shadow_prices_sced("today", verbose=True)

        self._check_shadow_prices_sced(df)

        # We don't know the exact SCED Timestamps
        assert df["SCED Timestamp"].min() < self.local_start_of_today() + pd.Timedelta(
            minutes=5,
        )
        assert df["SCED Timestamp"].max() < self.local_now()

        assert self.iso.get_shadow_prices_sced("latest").equals(df)

    @pytest.mark.integration
    def test_get_shadow_prices_sced_historical(self):
        past_date = self.local_start_of_today() - pd.DateOffset(
            days=HISTORICAL_DAYS_THRESHOLD * 3,
        )
        df = self.iso.get_shadow_prices_sced(past_date, verbose=True)

        self._check_shadow_prices_sced(df)

        start_of_past_date = self.local_start_of_day(past_date.date())

        assert df["SCED Timestamp"].min() < start_of_past_date

        max_timestamp = df["SCED Timestamp"].max()

        assert (
            start_of_past_date + pd.Timedelta(hours=22)
            < max_timestamp
            < start_of_past_date + pd.Timedelta(hours=24)
        )

    @pytest.mark.integration
    def test_get_shadow_prices_sced_historical_range(self):
        past_date = self.local_start_of_today() - pd.DateOffset(
            days=HISTORICAL_DAYS_THRESHOLD * 2,
        )
        past_end_date = past_date + pd.DateOffset(days=2)

        df = self.iso.get_shadow_prices_sced(
            date=past_date,
            end=past_end_date,
            verbose=True,
        )

        self._check_shadow_prices_sced(df)

        assert df["SCED Timestamp"].min() < self.local_start_of_day(past_date.date())

        max_timestamp = df["SCED Timestamp"].max()

        assert (
            self.local_start_of_day(past_end_date.date())
            - pd.DateOffset(days=1)
            + pd.Timedelta(hours=22)
            < max_timestamp
            < self.local_start_of_day(past_end_date.date())
        )

    """get_spp_real_time_15_min"""

    def _check_spp_real_time_15_min(self, df):
        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Location",
            "Location Type",
            "Market",
            "SPP",
        ]

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=15)
        ).all()

        assert sorted(df["Location Type"].unique().tolist()) == [
            "Load Zone",
            "Load Zone DC Tie",
            "Load Zone DC Tie Energy Weighted",
            "Load Zone Energy Weighted",
            "Resource Node",
            "Trading Hub",
        ]

        assert df["Market"].unique().tolist() == ["REAL_TIME_15_MIN"]

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_spp_real_time_15_min_historical_date_range(self):
        start_date = self.local_today() - pd.DateOffset(days=100)

        end_date = start_date + pd.DateOffset(days=2)

        df = ErcotAPI(sleep_seconds=3.0, max_retries=5).get_spp_real_time_15_min(
            date=start_date,
            end=end_date,
            verbose=True,
        )

        self._check_spp_real_time_15_min(df)

        assert df["Interval Start"].nunique() == 96 * 2

        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        assert df["Interval End"].max() == self.local_start_of_day(end_date)

    """get_spp_day_ahead_hourly"""

    def _check_spp_day_ahead_hourly(self, df):
        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Location",
            "Location Type",
            "Market",
            "SPP",
        ]

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=60)
        ).all()

        assert sorted(df["Location Type"].unique().tolist()) == [
            "Load Zone",
            "Load Zone DC Tie",
            "Resource Node",
            "Trading Hub",
        ]

        assert df["Market"].unique().tolist() == ["DAY_AHEAD_HOURLY"]

    @pytest.mark.integration
    def test_get_spp_day_ahead_hourly_historical_date_range(self):
        start_date = self.local_today() - pd.DateOffset(days=100)

        end_date = start_date + pd.DateOffset(days=2)

        df = ErcotAPI().get_spp_day_ahead_hourly(
            date=start_date,
            end=end_date,
            verbose=True,
        )

        self._check_spp_day_ahead_hourly(df)

        assert df["Interval Start"].nunique() == 24 * 2

        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        assert df["Interval End"].max() == self.local_start_of_day(end_date)

    """get_60_day_dam_disclosure"""

    @pytest.mark.integration
    def test_get_60_day_dam_disclosure_historical(self):
        start_date = self.local_start_of_today() - pd.DateOffset(days=3000)
        end_date = start_date + pd.DateOffset(days=2)

        df_dict = ErcotAPI().get_60_day_dam_disclosure(
            start_date,
            end_date,
        )

        check_60_day_dam_disclosure(df_dict)

        for df in df_dict.values():
            assert df["Interval Start"].min() == start_date
            assert df["Interval End"].max() == end_date

    @pytest.mark.integration
    def test_get_60_day_dam_disclosure_repeated_offers(self):
        """Tests a problematic date where one resource has repeated offers for a
        single service on a single interval"""
        # This is the resource. We expect to still have the data for this resource
        resource_name = "CANYONRO_LD1"
        date_with_issue = pd.Timestamp("2024-09-04", tz="US/Central")

        df_dict = ErcotAPI().get_60_day_dam_disclosure(
            date_with_issue,
        )

        check_60_day_dam_disclosure(df_dict)

        df_load = df_dict["dam_load_resource_as_offers"]
        df_gen = df_dict["dam_gen_resource_as_offers"]

        # The resource only occurs in the load data
        assert df_load[df_load["Resource Name"] == resource_name].shape[0] == 24

        for df in [df_load, df_gen]:
            assert df.columns.tolist() == DAM_RESOURCE_AS_OFFERS_COLUMNS

            assert df["Interval Start"].min() == pd.Timestamp(date_with_issue)
            assert df["Interval End"].max() == pd.Timestamp(
                date_with_issue,
            ) + pd.DateOffset(
                days=1,
            )

            assert df.groupby(["Interval Start", "Resource Name"]).size().max() == 1

    """get_60_day_sced_disclosure"""

    def test_get_60_day_sced_disclosure_historical(self):
        start_date = self.local_start_of_today() - pd.DateOffset(days=1000)
        end_date = start_date + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            f"test_get_60_day_sced_disclosure_historical_{start_date.date()}_{end_date.date()}.yaml",
        ):
            df_dict = ErcotAPI().get_60_day_sced_disclosure(
                start_date,
                end_date,
            )

        check_60_day_sced_disclosure(df_dict)

        for df in df_dict.values():
            assert df["Interval Start"].min() == start_date
            assert df["Interval End"].max() == end_date

    """get_historical_data"""

    @pytest.mark.integration
    def test_get_historical_data(self):
        start_date = datetime.date(2023, 1, 1)
        end_date = datetime.date(2023, 1, 3)

        data = self.iso.get_historical_data(
            "/np4-745-cd/spp_hrly_actual_fcast_geo",
            start_date=start_date,
            end_date=end_date,
        )

        assert data.columns.tolist() == [
            "DELIVERY_DATE",
            "HOUR_ENDING",
            "GEN_SYSTEM_WIDE",
            "COP_HSL_SYSTEM_WIDE",
            "STPPF_SYSTEM_WIDE",
            "PVGRPP_SYSTEM_WIDE",
            "GEN_CenterWest",
            "COP_HSL_CenterWest",
            "STPPF_CenterWest",
            "PVGRPP_CenterWest",
            "GEN_NorthWest",
            "COP_HSL_NorthWest",
            "STPPF_NorthWest",
            "PVGRPP_NorthWest",
            "GEN_FarWest",
            "COP_HSL_FarWest",
            "STPPF_FarWest",
            "PVGRPP_FarWest",
            "GEN_FarEast",
            "COP_HSL_FarEast",
            "STPPF_FarEast",
            "PVGRPP_FarEast",
            "GEN_SouthEast",
            "COP_HSL_SouthEast",
            "STPPF_SouthEast",
            "PVGRPP_SouthEast",
            "GEN_CenterEast",
            "COP_HSL_CenterEast",
            "STPPF_CenterEast",
            "PVGRPP_CenterEast",
            "DSTFlag",
        ]

        data["DELIVERY_DATE"] = pd.to_datetime(data["DELIVERY_DATE"], format="%m/%d/%Y")

        assert data["DELIVERY_DATE"].min().date() == datetime.date(2022, 12, 30)
        # This a forecast
        assert data["DELIVERY_DATE"].max().date() == datetime.date(2023, 1, 9)
        # Any change in the shape would be a regression since this is historical data
        assert data.shape == (10368, 31)

        start_date = datetime.date(2020, 12, 1)
        end_date = datetime.date(2020, 12, 2)

        data = self.iso.get_historical_data(
            "/np4-732-cd/wpp_hrly_avrg_actl_fcast",
            start_date=start_date,
            end_date=end_date,
        )

        assert data.columns.tolist() == [
            "DELIVERY_DATE",
            "HOUR_ENDING",
            "ACTUAL_SYSTEM_WIDE",
            "COP_HSL_SYSTEM_WIDE",
            "STWPF_SYSTEM_WIDE",
            "WGRPP_SYSTEM_WIDE",
            "ACTUAL_LZ_SOUTH_HOUSTON",
            "COP_HSL_LZ_SOUTH_HOUSTON",
            "STWPF_LZ_SOUTH_HOUSTON",
            "WGRPP_LZ_SOUTH_HOUSTON",
            "ACTUAL_LZ_WEST",
            "COP_HSL_LZ_WEST",
            "STWPF_LZ_WEST",
            "WGRPP_LZ_WEST",
            "ACTUAL_LZ_NORTH",
            "COP_HSL_LZ_NORTH",
            "STWPF_LZ_NORTH",
            "WGRPP_LZ_NORTH",
            "DSTFlag",
        ]

        data["DELIVERY_DATE"] = pd.to_datetime(data["DELIVERY_DATE"], format="%m/%d/%Y")
        assert data["DELIVERY_DATE"].min().date() == datetime.date(2020, 11, 29)
        assert data["DELIVERY_DATE"].max().date() == datetime.date(2020, 12, 8)

        # Since this is historical data, we do not except the shape to change. A change
        # would be a regression.
        assert data.shape == (5184, 19)

    """hit_ercot_api"""

    @pytest.mark.integration
    def test_hit_ercot_api(self):
        """
        First we test that entering a bad endpoint results in a keyerror
        """
        with pytest.raises(KeyError) as _:
            self.iso.hit_ercot_api("just a real bad endpoint right here")

        """
        Now a happy path test, using "actual system load by weather zone" endpoint.
        Starting from two days ago should result in 48 hourly values (or 24, depending
            on when the data is released and when the test is run), and there are
            12 columns in the resulting dataframe.
        We are also testing here that datetime objects are correctly parsed into
            the desired date string format that the operatingDayFrom parameter expects.
        """
        two_days_ago = pd.Timestamp.utcnow() - pd.DateOffset(days=2)
        actual_by_wzn_endpoint = "/np6-345-cd/act_sys_load_by_wzn"
        two_days_actual_by_wzn = self.iso.hit_ercot_api(
            actual_by_wzn_endpoint,
            operatingDayFrom=two_days_ago,
        )
        result_rows, result_cols = two_days_actual_by_wzn.shape
        assert result_rows in {24, 48}
        assert result_cols == 12

        """
        Now let's apply a value filter and test it.
        We start by taking the midpoint value between min and max of total load over
            the last two days, then query with a filter of only values above that,
            using the totalFrom parameter. There should be fewer than 48 rows, and all
            values for total load should be greater than the threshold we put in.
        """
        min_load = two_days_actual_by_wzn["Total"].min()
        max_load = two_days_actual_by_wzn["Total"].max()
        in_between_load = (max_load + min_load) / 2
        higher_loads_result = self.iso.hit_ercot_api(
            actual_by_wzn_endpoint,
            operatingDayFrom=two_days_ago,
            totalFrom=in_between_load,
        )
        assert len(higher_loads_result["Total"]) < result_rows
        assert all(higher_loads_result["Total"] > in_between_load)

        """
        Now we test the page_size and max_pages arguments. We know that our two days
            query returns 24 or 48 results, so if we lower page_size to 10 and max_pages
            to 2, we should only get 20 rows total. We can also use this opportunity to
            test that invalid parameter names are silently ignored.
        """
        small_pages_result = self.iso.hit_ercot_api(
            actual_by_wzn_endpoint,
            page_size=10,
            max_pages=2,
            operatingDayFrom=two_days_ago,
            wowWhatAFakeParameter=True,
            thisOneIsAlsoFake=42,
        )
        assert small_pages_result.shape == (20, 12)

    """endpoints_map"""

    @pytest.mark.integration
    def test_get_public_endpoints_map(self):
        endpoints_map = self.iso._get_public_endpoints_map()

        # update this count as needed, if ercot api evolves to add/remove endpoints
        assert len(endpoints_map) == 106

        # detailed check of all endpoints, fields, and values
        issues = []
        for endpoint, endpoint_dict in endpoints_map.items():
            for issue in self._endpoints_map_check(endpoint_dict):
                issues.append([f"{endpoint} - {issue}"])
        assert len(issues) == 0

    @pytest.mark.integration
    def test_get_esr_endpoints_map(self):
        endpoints_map = self.iso._get_esr_endpoints_map()

        # update this count as needed, if ercot api evolves to add/remove endpoints
        assert len(endpoints_map) == 1

        # detailed check of all endpoints, fields, and values
        issues = []
        for endpoint, endpoint_dict in endpoints_map.items():
            for issue in self._endpoints_map_check(endpoint_dict):
                issues.append([f"{endpoint} - {issue}"])
        assert len(issues) == 0

    def _endpoints_map_check(self, endpoint_dict: dict) -> list[str]:
        """Applies unit test checks to a single endpoint in the endpoints map.

        Ensures that top-level fields are present, and each parameter has a valid
        "payload" of value_type and parser_method

        Returns empty list if the given endpoint passes the check,
        otherwise returns a list of everything that's wrong, for ease of debugging
        """
        issues = []

        if "summary" not in endpoint_dict:
            issues.append("missing summary")

        parameters = endpoint_dict.get("parameters")
        if parameters is None:
            issues.append("missing parameters")
        else:
            for param, param_dict in parameters.items():
                value_type = param_dict.get("value_type")
                if value_type is None:
                    issues.append(f"{param} is missing value_type")
                elif value_type not in VALID_VALUE_TYPES:
                    issues.append(f"{param} has invalid value_type {value_type}")
                parser_method = param_dict.get("parser_method")
                if parser_method is None:
                    issues.append(f"{param} is missing parser_method")
                elif not callable(parser_method):
                    issues.append(f"{param} has an invalid parser_method")
        return issues

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2024-02-15 00:00:00", "2024-02-15 01:00:00"),
            ("2024-03-10 00:00:00", "2024-03-10 04:00:00"),
            ("2024-11-03 00:00:00", "2024-11-03 02:00:00"),
        ],
    )
    def test_get_indicative_lmp_by_settlement_point(self, date, end):
        with api_vcr.use_cassette(
            f"test_get_indicative_lmp_historical_{date}_{end}.yaml",
        ):
            df = self.iso.get_indicative_lmp_by_settlement_point(date, end)

            assert df.columns.tolist() == [
                "RTD Timestamp",
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
            ]

            assert df.dtypes["Interval Start"] == "datetime64[ns, US/Central]"
            assert df.dtypes["Interval End"] == "datetime64[ns, US/Central]"
            assert df.dtypes["LMP"] == "float64"
            assert (
                (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=5)
            ).all()
            assert df["Interval Start"].min() == pd.Timestamp(date).tz_localize(
                self.iso.default_timezone,
            )
            assert df["Interval End"].max() == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            ) + pd.Timedelta(minutes=50)

    """get_cop_adjustment_period_snapshot_60_day"""

    def _check_cop_adjustment_period_snapshot_60_day(self, df: pd.DataFrame) -> None:
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Resource Name",
            "QSE",
            "Status",
            "High Sustained Limit",
            "Low Sustained Limit",
            "High Emergency Limit",
            "Low Emergency Limit",
            "Reg Up",
            "Reg Down",
            "RRS",
            "RRSPFR",
            "RRSFFR",
            "RRSUFR",
            "NSPIN",
            "ECRS",
            "Minimum SOC",
            "Maximum SOC",
            "Hour Beginning Planned SOC",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

        assert df["Resource Name"].dtype == object
        assert df["QSE"].dtype == object

    def test_get_cop_adjustment_period_snapshot_60_day_date(self):
        # Check the most recent date that data is available
        date = self.local_today() - pd.DateOffset(days=61)

        with api_vcr.use_cassette(
            f"test_get_cop_adjustment_period_snapshot_60_day_date_{date}.yaml",
        ):
            df = self.iso.get_cop_adjustment_period_snapshot_60_day(date)

        self._check_cop_adjustment_period_snapshot_60_day(df)
        assert df["Interval Start"].min() == self.local_start_of_day(date)

        assert df["Interval Start"].max() == self.local_start_of_day(
            date,
        ) + pd.Timedelta(hours=23)

        assert df["RRS"].isnull().all()

        for col in [
            "RRSPFR",
            "RRSFFR",
            "RRSUFR",
            "ECRS",
            "Minimum SOC",
            "Maximum SOC",
            "Hour Beginning Planned SOC",
        ]:
            assert df[col].notnull().all()

    def test_get_cop_adjustment_period_snapshot_60_day_historical_date_range(self):
        start_date = self.local_start_of_today() - pd.DateOffset(days=500)
        end_date = start_date + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            f"test_get_cop_adjustment_period_snapshot_60_day_historical_date_range_{start_date.date()}_{end_date.date()}.yaml",
        ):
            df = self.iso.get_cop_adjustment_period_snapshot_60_day(
                start_date,
                end_date,
            )

        self._check_cop_adjustment_period_snapshot_60_day(df)

        assert df["Interval Start"].min() == start_date
        assert df["Interval Start"].max() == end_date - pd.Timedelta(hours=1)

        # Column only present in older data. We add it as null to keep columns same
        assert df["RRS"].isnull().all()

    def test_get_cop_adjustment_period_snapshot_60_day_missing_columns_are_null(self):
        # This is an early date when many columns were not present
        date = "2012-01-01"

        with api_vcr.use_cassette(
            f"test_get_cop_adjustment_period_snapshot_60_day_missing_columns_are_null_{date}.yaml",
        ):
            df = self.iso.get_cop_adjustment_period_snapshot_60_day(date)

        self._check_cop_adjustment_period_snapshot_60_day(df)

        # Column not present in older data. We add it as null to keep columns same
        for col in [
            "RRSPFR",
            "RRSFFR",
            "RRSUFR",
            "ECRS",
            "Minimum SOC",
            "Maximum SOC",
            "Hour Beginning Planned SOC",
        ]:
            assert df[col].isnull().all()

        assert df["RRS"].notna().all()

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval Start"].max() == self.local_start_of_day(
            date,
        ) + pd.Timedelta(hours=23)

    """get_system_load_charging_4_seconds"""

    def _check_system_load_charging_4_seconds(self, df: pd.DataFrame) -> None:
        assert df.columns.tolist() == [
            "Time",
            "System Demand",
            "ESR Charging MW",
        ]

        assert df.dtypes["Time"] == "datetime64[ns, US/Central]"
        assert df.dtypes["System Demand"] == "float64"
        assert df.dtypes["ESR Charging MW"] == "float64"

    def test_get_system_load_charging_4_seconds_today(self):
        with api_vcr.use_cassette(
            "test_get_system_load_charging_4_seconds_today.yaml",
        ):
            df = self.iso.get_system_load_charging_4_seconds("today", verbose=True)

        self._check_system_load_charging_4_seconds(df)

        assert df["Time"].min() == self.local_start_of_today()
        assert df["Time"].max() <= self.local_now()

    def test_get_system_load_charging_4_seconds_date_range(self):
        # This dataset doesn't have historical data yet, so use recent data
        start_date = self.local_today() - pd.DateOffset(days=1)
        end_date = start_date + pd.DateOffset(days=1)

        df = self.iso.get_system_load_charging_4_seconds(
            date=start_date,
            end=end_date,
            verbose=True,
        )

        self._check_system_load_charging_4_seconds(df)

        assert df["Time"].min() >= self.local_start_of_day(start_date)

        # Not inclusive of end date
        assert df["Time"].max() <= pd.Timestamp(
            end_date,
            tz=ErcotAPI().default_timezone,
        )

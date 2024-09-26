import datetime

import pandas as pd
import pytest

from gridstatus.base import Markets
from gridstatus.ercot import ELECTRICAL_BUS_LOCATION_TYPE
from gridstatus.ercot_api.api_parser import VALID_VALUE_TYPES
from gridstatus.ercot_api.ercot_api import HISTORICAL_DAYS_THRESHOLD, ErcotAPI
from gridstatus.tests.base_test_iso import TestHelperMixin


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

    """get_hourly_wind_report"""

    def _check_hourly_wind_report(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "ACTUAL SYSTEM WIDE",
            "COP HSL SYSTEM WIDE",
            "STWPF SYSTEM WIDE",
            "WGRPP SYSTEM WIDE",
            "ACTUAL LZ SOUTH HOUSTON",
            "COP HSL LZ SOUTH HOUSTON",
            "STWPF LZ SOUTH HOUSTON",
            "WGRPP LZ SOUTH HOUSTON",
            "ACTUAL LZ WEST",
            "COP HSL LZ WEST",
            "STWPF LZ WEST",
            "WGRPP LZ WEST",
            "ACTUAL LZ NORTH",
            "COP HSL LZ NORTH",
            "STWPF LZ NORTH",
            "WGRPP LZ NORTH",
        ]

        assert (df["Interval End"] - df["Interval Start"]).eq(pd.Timedelta("1h")).all()

    def test_get_hourly_wind_report_today(self):
        df = self.iso.get_hourly_wind_report("today")

        # The data should start at the beginning of two days ago
        assert df[
            "Interval Start"
        ].min() == self.local_start_of_today() - pd.DateOffset(days=2)

        self._check_hourly_wind_report(df)

    def test_get_hourly_wind_report_latest(self):
        df = self.iso.get_hourly_wind_report("latest")

        assert df["Publish Time"].nunique() == 1
        self._check_hourly_wind_report(df)

    def test_get_hourly_wind_report_historical_date_range(self):
        date = self.local_today() - pd.DateOffset(days=HISTORICAL_DAYS_THRESHOLD * 3)
        end = date + pd.Timedelta(hours=2)

        df = self.iso.get_hourly_wind_report(date, end, verbose=True)

        self._check_hourly_wind_report(df)

        assert df["Publish Time"].nunique() == 2

        assert df["Interval Start"].min() == self.local_start_of_day(
            date.date(),
        ) - pd.DateOffset(days=2)
        assert df["Interval End"].max() >= self.local_start_of_day(
            date.date(),
        ) + pd.DateOffset(days=7)

    """get_hourly_solar_report"""

    def _check_hourly_solar_report(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "GEN SYSTEM WIDE",
            "COP HSL SYSTEM WIDE",
            "STPPF SYSTEM WIDE",
            "PVGRPP SYSTEM WIDE",
            "GEN CenterWest",
            "COP HSL CenterWest",
            "STPPF CenterWest",
            "PVGRPP CenterWest",
            "GEN NorthWest",
            "COP HSL NorthWest",
            "STPPF NorthWest",
            "PVGRPP NorthWest",
            "GEN FarWest",
            "COP HSL FarWest",
            "STPPF FarWest",
            "PVGRPP FarWest",
            "GEN FarEast",
            "COP HSL FarEast",
            "STPPF FarEast",
            "PVGRPP FarEast",
            "GEN SouthEast",
            "COP HSL SouthEast",
            "STPPF SouthEast",
            "PVGRPP SouthEast",
            "GEN CenterEast",
            "COP HSL CenterEast",
            "STPPF CenterEast",
            "PVGRPP CenterEast",
        ]

        assert (df["Interval End"] - df["Interval Start"]).eq(pd.Timedelta("1h")).all()

    def test_get_hourly_solar_report_today(self):
        df = self.iso.get_hourly_solar_report("today")

        # We don't know the exact number of publish times
        # The data should start at the beginning of two days ago
        assert df[
            "Interval Start"
        ].min() == self.local_start_of_today() - pd.DateOffset(days=2)

        self._check_hourly_solar_report(df)

    def test_get_hourly_solar_report_latest(self):
        df = self.iso.get_hourly_solar_report("latest")

        assert df["Publish Time"].nunique() == 1
        self._check_hourly_solar_report(df)

    def test_get_hourly_solar_report_historical_date_range(self):
        date = self.local_today() - pd.DateOffset(days=HISTORICAL_DAYS_THRESHOLD * 3)
        end = date + pd.Timedelta(hours=2)

        df = self.iso.get_hourly_solar_report(date, end, verbose=True)

        self._check_hourly_solar_report(df)

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

    def test_get_as_prices_historical_date_range(self):
        start_date = datetime.date(2021, 3, 8)
        end_date = datetime.date(2021, 3, 10)
        df = self.iso.get_as_prices(start_date, end_date, verbose=True)

        self._check_as_prices(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        # Not inclusive of end date
        assert df["Interval End"].max() == self.local_start_of_day(end_date)

    """get_as_reports"""

    def test_get_as_reports_today_or_latest_raises_error(self):
        with pytest.raises(ValueError) as error:
            self.iso.get_as_reports("today")
            assert str(error.value) == "Cannot get AS reports for 'latest' or 'today'"

        with pytest.raises(ValueError) as error:
            self.iso.get_as_reports("latest")
            assert str(error.value) == "Cannot get AS reports for 'latest' or 'today'"

    def test_get_as_reports_historical_date(self):
        historical_date = datetime.date(2021, 1, 1)
        df = self.iso.get_as_reports(historical_date, verbose=True)

        assert df.columns.tolist() == [
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

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

        assert df["Interval Start"].min() == self.local_start_of_day(historical_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            historical_date,
        ) + pd.DateOffset(
            days=1,
        )

    def test_get_as_reports_historical_date_range(self):
        start_date = datetime.date(2021, 1, 1)
        end_date = datetime.date(2021, 1, 3)
        df = self.iso.get_as_reports(start_date, end_date, verbose=True)

        assert df.columns.tolist() == [
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

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        # Not inclusive of the end date
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
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

    def test_get_as_plan_today_or_latest(self):
        df = self.iso.get_as_plan("today")

        self._check_as_plan(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

        assert df["Publish Time"].dt.date.unique().tolist() == [self.local_today()]

        assert self.iso.get_as_plan("latest").equals(df)

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

    def test_get_lmp_by_settlement_point_today_or_latest(self):
        df = self.iso.get_lmp_by_settlement_point("today")

        self._check_lmp_by_settlement_point(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() <= self.local_now()

    @pytest.mark.slow
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

    def test_get_hourly_resource_outage_capacity_today_or_latest(self):
        df = self.iso.get_hourly_resource_outage_capacity("today")

        self._check_hourly_resource_outage_capacity(df)

        assert (df["Publish Time"].dt.date == self.local_today()).all()

        assert df["Interval Start"].min() <= self.local_start_of_today()
        assert df["Interval End"].max() >= self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

        assert self.iso.get_hourly_resource_outage_capacity("latest").equals(df)

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

    def test_get_lmp_by_bus_today(self):
        df = self.iso.get_lmp_by_bus("today", verbose=True)

        self._check_lmp_by_bus(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() <= self.local_now()

    def test_get_lmp_by_bus_latest(self):
        df = self.iso.get_lmp_by_bus("latest")

        self._check_lmp_by_bus(df)

        assert df["Interval Start"].min() >= self.local_now() - pd.Timedelta(minutes=30)
        assert df["Interval End"].max() <= self.local_now()

    @pytest.mark.slow
    def test_get_lmp_by_bus_historical_date(self):
        date = self.local_today() - pd.DateOffset(days=HISTORICAL_DAYS_THRESHOLD * 2)

        df = self.iso.get_lmp_by_bus(date, verbose=True)

        self._check_lmp_by_bus(df)

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=1)

    @pytest.mark.slow
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

    def test_get_lmp_by_bus_dam_today_and_latest(self):
        df = self.iso.get_lmp_by_bus_dam("today")

        self._check_lmp_by_bus_dam(df)

        assert df["Interval Start"].min() == self.local_start_of_today()

        # The end date will depend on when this runs, so check if it's today or tomorrow
        assert df["Interval End"].max() in [
            (self.local_start_of_today() + pd.DateOffset(days=d)) for d in [1, 2]
        ]

        assert self.iso.get_lmp_by_bus_dam("latest").equals(df)

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

    def test_get_lmp_by_bus_dam_historical_range(self):
        past_date = self.local_start_of_today() - pd.DateOffset(
            days=HISTORICAL_DAYS_THRESHOLD * 3,
        )
        past_end_date = past_date + pd.DateOffset(days=2)

        df = self.iso.get_lmp_by_bus_dam(past_date, past_end_date, verbose=True)

        self._check_lmp_by_bus_dam(df)

        assert df["Interval Start"].min() == past_date.normalize()
        assert df["Interval End"].max() == past_end_date.normalize()

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

    def test_get_shadow_prices_sced_today_and_latest(self):
        df = self.iso.get_shadow_prices_sced("today", verbose=True)

        self._check_shadow_prices_sced(df)

        # We don't know the exact SCED Timestamps
        assert df["SCED Timestamp"].min() < self.local_start_of_today() + pd.Timedelta(
            minutes=5,
        )
        assert df["SCED Timestamp"].max() < self.local_now()

        assert self.iso.get_shadow_prices_sced("latest").equals(df)

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

    """get_historical_data"""

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

    def test_get_endpoints_map(self):
        endpoints_map = self.iso._get_endpoints_map()

        # update this count as needed, if ercot api evolves to add/remove endpoints
        assert len(endpoints_map) == 102

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

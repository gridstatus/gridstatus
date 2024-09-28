from io import StringIO

import pandas as pd
import pytest

import gridstatus
from gridstatus import Markets, NotSupported
from gridstatus.ercot import (
    ELECTRICAL_BUS_LOCATION_TYPE,
    Ercot,
    ERCOTSevenDayLoadForecastReport,
    parse_timestamp_from_friendly_name,
)
from gridstatus.tests.base_test_iso import BaseTestISO

INTERVALS_PER_HOUR_AT_FIVE_MINUTE_RESOLUTION = 12


class TestErcot(BaseTestISO):
    iso = Ercot()

    # These are the weather zones in ERCOT in the order we want them.
    weather_zone_columns = [
        "Coast",
        "East",
        "Far West",
        "North",
        "North Central",
        "South Central",
        "Southern",
        "West",
    ]

    """dam_system_lambda"""

    def test_get_dam_system_lambda_latest(self):
        df = self.iso.get_dam_system_lambda("latest", verbose=True)

        self._check_dam_system_lambda(df)
        # We don't know the exact publish date because it could be yesterday
        # or today depending on when this test is run
        assert df["Publish Time"].dt.date.nunique() == 1

    def test_get_dam_system_lambda_today(self):
        df = self.iso.get_dam_system_lambda("today", verbose=True)

        self._check_dam_system_lambda(df)

        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()

        # Published yesterday
        assert df["Publish Time"].dt.date.unique() == [today - pd.Timedelta(days=1)]
        assert df["Interval Start"].dt.date.unique() == [today]

    def test_get_dam_system_lambda_historical(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=2,
        )

        df = self.iso.get_dam_system_lambda(two_days_ago)

        self._check_dam_system_lambda(df)

        assert list(df["Publish Time"].dt.date.unique()) == [
            two_days_ago - pd.Timedelta(days=1),
        ]

    def test_get_dam_system_lambda_historical_range(self):
        three_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=3,
        )

        two_days_ago = three_days_ago + pd.Timedelta(
            days=1,
        )

        df = self.iso.get_dam_system_lambda(
            start=three_days_ago,
            end=two_days_ago + pd.Timedelta(days=1),
            verbose=True,
        )

        self._check_dam_system_lambda(df)

        assert list(df["Publish Time"].dt.date.unique()) == [
            three_days_ago - pd.Timedelta(days=1),
            two_days_ago - pd.Timedelta(days=1),
        ]

    """sced_system_lambda"""

    def test_get_sced_system_lambda(self):
        for i in ["latest", "today"]:
            df = self.iso.get_sced_system_lambda(i, verbose=True)
            assert df.shape[0] >= 0
            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "SCED Timestamp",
                "System Lambda",
            ]
            today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
            assert df["SCED Timestamp"].unique()[0].date() == today
            assert isinstance(df["System Lambda"].unique()[0], float)

    """as_prices"""

    def test_get_as_prices(self):
        as_cols = [
            "Time",
            "Interval Start",
            "Interval End",
            "Market",
            "Non-Spinning Reserves",
            "Regulation Down",
            "Regulation Up",
            "Responsive Reserves",
            "ERCOT Contingency Reserve Service",
        ]

        # today
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()

        df = self.iso.get_as_prices(today)
        assert df.shape[0] >= 0
        assert df.columns.tolist() == as_cols
        assert df["Time"].unique()[0].date() == today

        date = today - pd.Timedelta(days=3)
        df = self.iso.get_as_prices(date)
        assert df.shape[0] >= 0
        assert df.columns.tolist() == as_cols
        assert df["Time"].unique()[0].date() == date

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

    def test_get_as_monitor(self):
        df = self.iso.get_as_monitor()

        # asset length is 1, 49 columns
        assert df.shape == (1, 49)
        # assert every colunn but the first is int dtype
        assert df.iloc[:, 1:].dtypes.unique() == "int64"
        assert df.columns[0] == "Time"

    def test_get_real_time_system_conditions(self):
        df = self.iso.get_real_time_system_conditions()
        assert df.shape == (1, 15)
        assert df.columns[0] == "Time"

    def test_get_energy_storage_resources(self):
        df = self.iso.get_energy_storage_resources()

        assert df.columns.tolist() == [
            "Time",
            "Total Charging",
            "Total Discharging",
            "Net Output",
        ]

    """get_fuel_mix"""

    def test_get_fuel_mix(self):
        # today
        cols = [
            "Time",
            "Coal and Lignite",
            "Hydro",
            "Nuclear",
            "Power Storage",
            "Solar",
            "Wind",
            "Natural Gas",
            "Other",
        ]
        df = self.iso.get_fuel_mix("today")
        self._check_fuel_mix(df)
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        # latest
        df = self.iso.get_fuel_mix("latest")
        self._check_fuel_mix(df)
        # returns two days of data
        assert df["Time"].dt.date.nunique() == 2
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_date_or_start(self):
        pass

    def test_get_fuel_mix_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_historical_with_date_range(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_range_two_days_with_day_start_endpoint(self):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_fuel_mix_start_end_same_day(self):
        pass

    """get_lmp"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_lmp_date_range(self, markets=None):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_historical(self, markets=None):
        pass

    def test_get_load_3_days_ago(self):
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        three_days_ago = today - pd.Timedelta(days=3)
        df = self.iso.get_load(three_days_ago)
        self._check_load(df)
        assert df["Time"].unique()[0].date() == three_days_ago

    def test_get_load_by_weather_zone(self):
        df = self.iso.get_load_by_weather_zone("today")
        self._check_time_columns(df, instant_or_interval="interval")
        cols = (
            [
                "Time",
                "Interval Start",
                "Interval End",
            ]
            + self.weather_zone_columns
            + ["System Total"]
        )

        assert df.columns.tolist() == cols

        # test 5 days ago
        five_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=5)
        df = self.iso.get_load_by_weather_zone(five_days_ago)
        self._check_time_columns(df, instant_or_interval="interval")
        assert df["Time"].unique()[0].date() == five_days_ago

        assert df.columns.tolist() == cols

    def test_get_load_by_forecast_zone_today(self):
        df = self.iso.get_load_by_forecast_zone("today")
        self._check_time_columns(df, instant_or_interval="interval")
        columns = [
            "Time",
            "Interval Start",
            "Interval End",
            "NORTH",
            "SOUTH",
            "WEST",
            "HOUSTON",
            "TOTAL",
        ]
        assert df.columns.tolist() == columns

        five_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=5)
        df = self.iso.get_load_by_forecast_zone(five_days_ago)
        self._check_time_columns(df, instant_or_interval="interval")
        assert df["Time"].unique()[0].date() == five_days_ago

    """get_load_forecast"""

    def test_get_load_forecast_range(self):
        end = pd.Timestamp.now(tz=self.iso.default_timezone)
        start = end - pd.Timedelta(hours=3)
        df = self.iso.get_load_forecast(start=start, end=end)

        unique_load_forecast_time = df["Publish Time"].unique()
        # make sure each is between start and end
        assert (unique_load_forecast_time >= start).all()
        assert (unique_load_forecast_time <= end).all()

    expected_load_forecast_columns = [
        "Time",
        "Interval Start",
        "Interval End",
        "Publish Time",
        "North",
        "South",
        "West",
        "Houston",
        "System Total",
    ]

    def test_get_load_forecast_historical(self):
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=2)).date()
        forecast = self.iso.get_load_forecast(date=test_date)
        self._check_forecast(
            forecast,
            expected_columns=self.expected_load_forecast_columns,
        )

    def test_get_load_forecast_today(self):
        forecast = self.iso.get_load_forecast("today")
        self._check_forecast(
            forecast,
            expected_columns=self.expected_load_forecast_columns,
        )

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    def test_get_load_forecast_by_weather_zone(self):
        df = self.iso.get_load_forecast(
            "today",
            forecast_type=ERCOTSevenDayLoadForecastReport.BY_WEATHER_ZONE,
        )

        cols = (
            [
                "Time",
                "Interval Start",
                "Interval End",
                "Publish Time",
            ]
            + self.weather_zone_columns
            + ["System Total"]
        )

        self._check_forecast(df, expected_columns=cols)

        five_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=5)
        df = self.iso.get_load_forecast(
            five_days_ago,
            forecast_type=ERCOTSevenDayLoadForecastReport.BY_WEATHER_ZONE,
        )

        self._check_forecast(df, expected_columns=cols)

    """get_capacity_committed"""

    def test_get_capacity_committed(self):
        df = self.iso.get_capacity_committed("latest")

        assert df.columns.tolist() == ["Interval Start", "Interval End", "Capacity"]

        assert df["Interval Start"].min() == self.local_start_of_today()
        # The end time is approximately now
        assert (
            self.local_now() - pd.Timedelta(minutes=5)
            < df["Interval End"].max()
            < self.local_now() + pd.Timedelta(minutes=5)
        )

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            minutes=5,
        )

    """get_capacity_forecast"""

    def test_get_capacity_forecast(self):
        df = self.iso.get_capacity_forecast("latest")

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Committed Capacity",
            "Available Capacity",
        ]

        # The start time is approximately now
        assert (
            self.local_now() - pd.Timedelta(minutes=5)
            < df["Interval Start"].min()
            < self.local_now() + pd.Timedelta(minutes=5)
        )

        assert df["Interval End"].max() >= self.local_start_of_day(
            self.local_today() + pd.Timedelta(days=1),
        )

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            minutes=5,
        )

    """get_available_seasonal_capacity_forecast"""

    def test_get_available_seasonal_capacity_forecast(self):
        df = self.iso.get_available_seasonal_capacity_forecast("latest")

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Available Capacity",
            "Load Forecast",
        ]

        assert df["Interval Start"].min() == self.local_start_of_today() + pd.Timedelta(
            days=1,
        )
        assert df["Interval End"].max() == self.local_start_of_today() + pd.Timedelta(
            days=7,
        )

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            hours=1,
        )

    """get_spp"""

    def test_get_spp_dam_today_day_ahead_hourly_hub(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="Trading Hub",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Trading Hub")

    def test_get_spp_dam_today_day_ahead_hourly_node(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="Resource Node",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Resource Node")

    def test_get_spp_dam_today_day_ahead_hourly_zone(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="Load Zone",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Load Zone")

    def test_get_spp_dam_range(self):
        today = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize()

        two_days_ago = today - pd.Timedelta(
            days=2,
        )

        df = self.iso.get_spp(
            start=two_days_ago,
            end=today,
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="Load Zone",
        )

        # two unique days
        # should be today and yesterday since published one day ahead
        assert set(df["Interval Start"].dt.date.unique()) == {
            today.date(),
            today.date() - pd.Timedelta(days=1),
        }
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Load Zone")

    def test_get_spp_real_time_range(self):
        today = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize()

        one_hour_earlier = today - pd.Timedelta(
            hours=1,
        )

        df = self.iso.get_spp(
            start=one_hour_earlier,
            end=today,
            market=Markets.REAL_TIME_15_MIN,
            location_type="Load Zone",
        )

        # should be 4 intervals in last hour
        assert (df.groupby("Location")["Interval Start"].count() == 4).all()
        assert df["Interval End"].min() > one_hour_earlier
        assert df["Interval End"].max() <= today

        self._check_ercot_spp(df, Markets.REAL_TIME_15_MIN, "Load Zone")

    def test_get_spp_real_time_yesterday(self):
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        yesterday = today - pd.Timedelta(days=1)

        df = self.iso.get_spp(
            date=yesterday,
            market=Markets.REAL_TIME_15_MIN,
            location_type="Trading Hub",
            verbose=True,
        )

        # assert Interval End max is today
        assert df["Interval End"].max().date() == today
        assert df["Interval Start"].min().date() == yesterday

    def test_get_spp_real_time_handles_all_location_types(self):
        df = self.iso.get_spp(
            date="latest",
            market=Markets.REAL_TIME_15_MIN,
            verbose=True,
        )

        assert set(df["Location Type"].unique()) == {
            "Resource Node",
            "Load Zone DC Tie",
            "Load Zone DC Tie Energy Weighted",
            "Trading Hub",
            "Load Zone Energy Weighted",
            "Load Zone",
        }

    def test_get_spp_day_ahead_handles_all_location_types(self):
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        yesterday = today - pd.Timedelta(days=1)
        df = self.iso.get_spp(
            date=yesterday,
            market=Markets.DAY_AHEAD_HOURLY,
            verbose=True,
        )

        assert set(df["Location Type"].unique()) == {
            "Resource Node",
            "Load Zone DC Tie",
            "Trading Hub",
            "Load Zone",
        }

    @pytest.mark.skip(reason="takes too long to run")
    def test_get_spp_rtm_historical(self):
        rtm = gridstatus.Ercot().get_rtm_spp(2020)
        assert isinstance(rtm, pd.DataFrame)
        assert len(rtm) > 0

    @pytest.mark.slow
    def test_get_spp_today_real_time_15_minutes_zone(self):
        df = self.iso.get_spp(
            date="today",
            market=Markets.REAL_TIME_15_MIN,
            location_type="Load Zone",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.REAL_TIME_15_MIN, "Load Zone")

    def test_get_spp_two_days_ago_day_ahead_hourly_zone(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=2,
        )
        df = self.iso.get_spp(
            date=two_days_ago,
            market=Markets.DAY_AHEAD_HOURLY,
            location_type="Load Zone",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.DAY_AHEAD_HOURLY, "Load Zone")

    @pytest.mark.slow
    def test_get_spp_two_days_ago_real_time_15_minutes_zone(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=2,
        )
        df = self.iso.get_spp(
            date=two_days_ago,
            market=Markets.REAL_TIME_15_MIN,
            location_type="Load Zone",
        )
        # minimum interval start is beginning of day
        assert df["Interval Start"].min().hour == 0
        assert df["Interval Start"].min().minute == 0
        self._check_ercot_spp(df, Markets.REAL_TIME_15_MIN, "Load Zone")

    """get_60_day_sced_disclosure"""

    def test_get_60_day_sced_disclosure_historical(self):
        days_ago_65 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=65,
        )

        df_dict = self.iso.get_60_day_sced_disclosure(date=days_ago_65, process=True)

        load_resource = df_dict["sced_load_resource"]
        gen_resource = df_dict["sced_gen_resource"]
        smne = df_dict["sced_smne"]

        assert load_resource["SCED Time Stamp"].dt.date.unique()[0] == days_ago_65
        assert gen_resource["SCED Time Stamp"].dt.date.unique()[0] == days_ago_65
        assert smne["Interval Time"].dt.date.unique()[0] == days_ago_65

        assert load_resource.shape[1] == 22
        assert gen_resource.shape[1] == 29
        assert smne.shape[1] == 6

    def test_get_60_day_sced_disclosure_range(self):
        days_ago_65 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=65,
        )

        days_ago_66 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=66,
        )

        df_dict = self.iso.get_60_day_sced_disclosure(
            start=days_ago_66,
            end=days_ago_65
            + pd.Timedelta(days=1),  # add one day to end date since exclusive
            verbose=True,
        )

        load_resource = df_dict["sced_load_resource"]
        gen_resource = df_dict["sced_gen_resource"]
        smne = df_dict["sced_smne"]

        assert load_resource["SCED Time Stamp"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

        assert gen_resource["SCED Time Stamp"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

        assert smne["Interval Time"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

    """get_60_day_dam_disclosure"""

    def test_get_60_day_dam_disclosure_historical(self):
        days_ago_65 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=65,
        )

        df_dict = self.iso.get_60_day_dam_disclosure(date=days_ago_65, process=True)
        assert df_dict is not None

        dam_gen_resource = df_dict["dam_gen_resource"]
        dam_gen_resource_as_offers = df_dict["dam_gen_resource_as_offers"]
        dam_load_resource = df_dict["dam_load_resource"]
        dam_load_resource_as_offers = df_dict["dam_load_resource_as_offers"]
        dam_energy_bids = df_dict["dam_energy_bids"]
        dam_energy_bid_awards = df_dict["dam_energy_bid_awards"]

        assert dam_gen_resource.shape[1] == 29
        assert dam_gen_resource_as_offers.shape[1] == 62
        assert dam_load_resource.shape[1] == 19
        assert dam_load_resource_as_offers.shape[1] == 63
        assert dam_energy_bids.shape[1] == 28
        assert dam_energy_bid_awards.shape[1] == 8

    def test_get_sara(self):
        columns = [
            "Unit Name",
            "Generation Interconnection Project Code",
            "Unit Code",
            "County",
            "Fuel",
            "Zone",
            "In Service Year",
            "Installed Capacity Rating",
            "Summer Capacity (MW)",
            "New Planned Project Additions to Report",
        ]
        df = self.iso.get_sara(verbose=True)
        assert df.shape[0] > 0
        assert df.columns.tolist() == columns

    def test_spp_real_time_parse_retry_file_name(self):
        assert parse_timestamp_from_friendly_name(
            "SPPHLZNP6905_retry_20230608_1545_csv",
        ) == pd.Timestamp("2023-06-08 15:45:00-0500", tz="US/Central")

        assert parse_timestamp_from_friendly_name(
            "SPPHLZNP6905_20230608_1545_csv",
        ) == pd.Timestamp("2023-06-08 15:45:00-0500", tz="US/Central")

    """get_unplanned_resource_outages"""

    def _check_unplanned_resource_outages(self, df):
        assert df.shape[0] >= 0

        assert df.columns.tolist() == [
            "Current As Of",
            "Publish Time",
            "Actual Outage Start",
            "Planned End Date",
            "Actual End Date",
            "Resource Name",
            "Resource Unit Code",
            "Fuel Type",
            "Outage Type",
            "Nature Of Work",
            "Available MW Maximum",
            "Available MW During Outage",
            "Effective MW Reduction Due to Outage",
        ]

        time_cols = [
            "Current As Of",
            "Publish Time",
            "Actual Outage Start",
            "Planned End Date",
            "Actual End Date",
        ]

        for col in time_cols:
            assert df[col].dt.tz.zone == self.iso.default_timezone

    def test_get_unplanned_resource_outages_historical_date(self):
        five_days_ago = self.local_start_of_today() - pd.DateOffset(days=5)
        df = self.iso.get_unplanned_resource_outages(date=five_days_ago)

        self._check_unplanned_resource_outages(df)

        assert df["Current As Of"].dt.date.unique() == [
            (five_days_ago - pd.DateOffset(days=3)).date(),
        ]
        assert df["Publish Time"].dt.date.unique() == [five_days_ago.date()]

    def test_get_unplanned_resource_outages_historical_range(self):
        start = self.local_start_of_today() - pd.DateOffset(6)

        df_2_days = self.iso.get_unplanned_resource_outages(
            start=start,
            end=start + pd.DateOffset(2),
        )

        self._check_unplanned_resource_outages(df_2_days)

        assert df_2_days["Current As Of"].dt.date.nunique() == 2
        assert (
            df_2_days["Current As Of"].min().date()
            == (start - pd.DateOffset(days=3)).date()
        )
        assert (
            df_2_days["Current As Of"].max().date()
            == (start - pd.DateOffset(days=2)).date()
        )

        assert df_2_days["Publish Time"].dt.date.nunique() == 2
        assert df_2_days["Publish Time"].min().date() == start.date()
        assert (
            df_2_days["Publish Time"].max().date() == (start + pd.DateOffset(1)).date()
        )

    """test get_highest_price_as_offer_selected"""

    def test_get_highest_price_as_offer_selected(self):
        four_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=4,
        )

        five_days_ago = four_days_ago - pd.Timedelta(
            days=1,
        )

        df = self.iso.get_highest_price_as_offer_selected(
            start=five_days_ago,
            end=four_days_ago
            + pd.Timedelta(
                days=1,
            ),
        )

        assert (
            df["Interval Start"].dt.date.unique()
            == [five_days_ago.date(), four_days_ago.date()]
        ).all()

        cols = [
            "Time",
            "Interval Start",
            "Interval End",
            "Market",
            "QSE",
            "DME",
            "Resource Name",
            "AS Type",
            "Block Indicator",
            "Offered Price",
            "Total Offered Quantity",
            "Offered Quantities",
        ]

        assert df.columns.tolist() == cols

    """test get_as_reports"""

    def test_get_as_reports(self):
        four_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).normalize() - pd.Timedelta(
            days=4,
        )

        five_days_ago = four_days_ago - pd.Timedelta(
            days=1,
        )

        df = self.iso.get_as_reports(
            start=five_days_ago,
            end=four_days_ago
            + pd.Timedelta(
                days=1,
            ),
        )

        assert (
            df["Interval Start"].dt.date.unique()
            == [five_days_ago.date(), four_days_ago.date()]
        ).all()

        cols = [
            "Time",
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

        assert df.columns.tolist() == cols

    """get_reported_outages"""

    def test_get_reported_outages(self):
        df = self.iso.get_reported_outages()

        assert df.columns.tolist() == [
            "Time",
            "Combined Unplanned",
            "Combined Planned",
            "Combined Total",
            "Dispatchable Unplanned",
            "Dispatchable Planned",
            "Dispatchable Total",
            "Renewable Unplanned",
            "Renewable Planned",
            "Renewable Total",
        ]

        assert df["Time"].min() <= self.local_start_of_today() - pd.Timedelta(
            # Add the minutes because the times do not line up exactly on the hour
            days=6,
            minutes=-5,
        )

        assert df["Time"].max() >= self.local_start_of_today()

        assert (
            df["Combined Total"] == (df["Combined Unplanned"] + df["Combined Planned"])
        ).all()

        assert (
            df["Dispatchable Total"]
            == (df["Dispatchable Unplanned"] + df["Dispatchable Planned"])
        ).all()

        assert (
            df["Renewable Total"]
            == (df["Renewable Unplanned"] + df["Renewable Planned"])
        ).all()

    """get_hourly_resource_outage_capacity"""

    def test_get_hourly_resource_outage_capacity(self):
        cols = [
            "Publish Time",
            "Time",
            "Interval Start",
            "Interval End",
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
        ]

        # test specific hour
        date = pd.Timestamp.now(tz=self.iso.default_timezone) - pd.Timedelta(
            days=1,
        )
        df = self.iso.get_hourly_resource_outage_capacity(date)

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        # test latest and confirm published in last 2 hours
        df = self.iso.get_hourly_resource_outage_capacity("latest")
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        assert df["Publish Time"].min() >= pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ) - pd.Timedelta(hours=2)

        # test date range
        end = date.floor("h")
        start = end - pd.Timedelta(
            hours=3,
        )
        df = self.iso.get_hourly_resource_outage_capacity(
            start=start,
            end=end,
            verbose=True,
        )

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols
        assert df["Publish Time"].nunique() == 3

    """get_hourly_wind_report"""

    def _check_hourly_wind_report(self, df):
        cols = [
            "Publish Time",
            "Time",
            "Interval Start",
            "Interval End",
            "GEN SYSTEM WIDE",
            "COP HSL SYSTEM WIDE",
            "STWPF SYSTEM WIDE",
            "WGRPP SYSTEM WIDE",
            "GEN LZ SOUTH HOUSTON",
            "COP HSL LZ SOUTH HOUSTON",
            "STWPF LZ SOUTH HOUSTON",
            "WGRPP LZ SOUTH HOUSTON",
            "GEN LZ WEST",
            "COP HSL LZ WEST",
            "STWPF LZ WEST",
            "WGRPP LZ WEST",
            "GEN LZ NORTH",
            "COP HSL LZ NORTH",
            "STWPF LZ NORTH",
            "WGRPP LZ NORTH",
            "HSL SYSTEM WIDE",
        ]

        assert df.columns.tolist() == cols
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    def test_get_hourly_wind_report_today(self):
        df = self.iso.get_hourly_wind_report("today", verbose=True)

        self._check_hourly_wind_report(df)

        hours_since_local_midnight = (
            self.local_now() - self.local_start_of_today()
        ) // pd.Timedelta(hours=1)

        assert df["Publish Time"].nunique() == hours_since_local_midnight

    def test_get_hourly_wind_report_latest(self):
        df = self.iso.get_hourly_wind_report("latest", verbose=True)

        self._check_hourly_wind_report(df)

        assert df["Publish Time"].nunique() == 1

    def test_get_hourly_wind_report_historical_date(self):
        date = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_hourly_wind_report(date, verbose=True)

        self._check_hourly_wind_report(df)

        assert df["Publish Time"].nunique() == 24  # One for each hour
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    def test_get_hourly_wind_report_historical_date_range(self):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_hourly_wind_report(start, end, verbose=True)

        self._check_hourly_wind_report(df)

        assert df["Publish Time"].nunique() == 48
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    """get_hourly_solar_report"""

    def _check_hourly_solar_report(self, df):
        cols = [
            "Publish Time",
            "Time",
            "Interval Start",
            "Interval End",
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
            "HSL SYSTEM WIDE",
        ]

        assert df.columns.tolist() == cols
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    def test_get_hourly_solar_report_today(self):
        df = self.iso.get_hourly_solar_report("today", verbose=True)

        self._check_hourly_solar_report(df)

        hours_since_local_midnight = (
            self.local_now() - self.local_start_of_today()
        ) // pd.Timedelta(hours=1)

        assert df["Publish Time"].nunique() == hours_since_local_midnight

    def test_get_hourly_solar_report_latest(self):
        df = self.iso.get_hourly_solar_report("latest", verbose=True)

        self._check_hourly_solar_report(df)

        assert df["Publish Time"].nunique() == 1

    def test_get_hourly_solar_report_historical_date(self):
        date = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_hourly_solar_report(date, verbose=True)

        self._check_hourly_solar_report(df)

        assert df["Publish Time"].nunique() == 24  # One for each hour
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    def test_get_hourly_solar_report_historical_date_range(self):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_hourly_solar_report(start, end, verbose=True)

        self._check_hourly_solar_report(df)

        assert df["Publish Time"].nunique() == 48
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

    """get_price_corrections"""

    def test_get_rtm_price_corrections(self):
        df = self.iso.get_rtm_price_corrections(rtm_type="RTM_SPP")

        cols = [
            "Price Correction Time",
            "Interval Start",
            "Interval End",
            "Location",
            "Location Type",
            "SPP Original",
            "SPP Corrected",
        ]

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

    # TODO: this url has no DocumentList
    # https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=13044
    @pytest.mark.skip(reason="Failing")
    def test_get_dam_price_corrections(self):
        df = self.iso.get_dam_price_corrections(dam_type="DAM_SPP")

        cols = [
            "Price Correction Time",
            "Interval Start",
            "Interval End",
            "Location",
            "Location Type",
            "SPP Original",
            "SPP Corrected",
        ]

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

    """get_system_wide_actuals"""

    def test_get_system_wide_actual_load_for_date(self):
        yesterday = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=1,
        )
        df = self.iso.get_system_wide_actual_load(yesterday)

        # 1 Hour of data
        assert df.shape[0] == 4
        assert df["Interval Start"].min() == pd.Timestamp(
            yesterday,
            tz=self.iso.default_timezone,
        )

        cols = ["Time", "Interval Start", "Interval End", "Demand"]
        assert df.columns.tolist() == cols

    def test_get_system_wide_actual_load_date_range(self):
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        two_days_ago = today - pd.Timedelta(days=2)

        df = self.iso.get_system_wide_actual_load(
            start=two_days_ago,
            end=today,
            verbose=True,
        )

        cols = ["Time", "Interval Start", "Interval End", "Demand"]

        assert df["Interval Start"].min() == pd.Timestamp(
            two_days_ago,
            tz=self.iso.default_timezone,
        )
        assert df["Interval Start"].max() == pd.Timestamp(
            today,
            tz=self.iso.default_timezone,
        ) - pd.Timedelta(minutes=15)
        assert df.columns.tolist() == cols

    def test_get_system_wide_actual_load_today(self):
        df = self.iso.get_system_wide_actual_load("today")

        cols = ["Time", "Interval Start", "Interval End", "Demand"]

        assert df["Interval Start"].min() == pd.Timestamp(
            pd.Timestamp.now(tz=self.iso.default_timezone).date(),
            tz=self.iso.default_timezone,
        )
        # 1 Hour of data
        assert df.shape[0] == 4
        assert df.columns.tolist() == cols

    def test_get_system_wide_actual_load_latest(self):
        df = self.iso.get_system_wide_actual_load("latest")

        cols = ["Time", "Interval Start", "Interval End", "Demand"]

        assert df["Interval Start"].min() == pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).floor("h") - pd.Timedelta(hours=1)

        # 1 Hour of data
        assert df.shape[0] == 4
        assert df.columns.tolist() == cols

    """get_short_term_system_adequacy"""

    def _check_short_term_system_adequacy(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Capacity Generation Resource South",
            "Capacity Generation Resource North",
            "Capacity Generation Resource West",
            "Capacity Generation Resource Houston",
            "Capacity Load Resource South",
            "Capacity Load Resource North",
            "Capacity Load Resource West",
            "Capacity Load Resource Houston",
            "Offline Available MW South",
            "Offline Available MW North",
            "Offline Available MW West",
            "Offline Available MW Houston",
            "Available Capacity Generation",
            "Available Capacity Reserve",
            "Capacity Generation Resource Total",
            "Capacity Load Resource Total",
            "Offline Available MW Total",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

    def test_get_short_term_system_adequacy_today(self):
        df = self.iso.get_short_term_system_adequacy("today")

        self._check_short_term_system_adequacy(df)

        # At least one published per hour
        assert (
            df["Publish Time"].nunique()
            >= (self.local_now() - self.local_start_of_today()).total_seconds() // 3600
        )
        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

    def test_get_short_term_system_adequacy_latest(self):
        df = self.iso.get_short_term_system_adequacy("latest")

        self._check_short_term_system_adequacy(df)

        assert df["Publish Time"].nunique() == 1

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

    def test_get_short_term_system_adequacy_historical_date(self):
        date = self.local_today() - pd.DateOffset(days=15)
        df = self.iso.get_short_term_system_adequacy(date)

        assert df["Publish Time"].nunique() >= 24

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=7)

        self._check_short_term_system_adequacy(df)

    def test_get_short_term_system_adequacy_historical_range(self):
        start = self.local_today() - pd.DateOffset(days=15)
        end = self.local_today() - pd.DateOffset(days=14)
        df = self.iso.get_short_term_system_adequacy(
            start=start,
            end=end,
        )

        assert df["Publish Time"].nunique() >= 24
        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval End"].max() == self.local_start_of_day(end) + pd.DateOffset(
            days=6,
        )

        self._check_short_term_system_adequacy(df)

    """get_real_time_adders_and_reserves"""

    def _check_real_time_adders_and_reserves(self, df):
        assert df.columns.tolist() == [
            "SCED Timestamp",
            "Interval Start",
            "Interval End",
            "BatchID",
            "System Lambda",
            "PRC",
            "RTORPA",
            "RTOFFPA",
            "RTOLCAP",
            "RTOFFCAP",
            "RTOLHSL",
            "RTBP",
            "RTCLRCAP",
            "RTCLRREG",
            "RTCLRBP",
            "RTCLRLSL",
            "RTCLRNS",
            "RTNCLRRRS",
            "RTOLNSRS",
            "RTCST30HSL",
            "RTOFFNSHSL",
            "RTRUCCST30HSL",
            "RTORDPA",
            "RTRRUC",
            "RTRRMR",
            "RTDNCLR",
            "RTDERS",
            "RTDCTIEIMPORT",
            "RTDCTIEEXPORT",
            "RTBLTIMPORT",
            "RTBLTEXPORT",
            "RTOLLASL",
            "RTOLHASL",
            "RTNCLRNSCAP",
            "RTNCLRECRS",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

    def test_get_real_time_adders_and_reserves_today(self):
        df = self.iso.get_real_time_adders_and_reserves("today")

        self._check_real_time_adders_and_reserves(df)

        hours_since_start_of_day = (
            self.local_now()
            - self.local_start_of_today()
            # Integer division
        ) // pd.Timedelta(hours=1)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert (
            len(df)
            >= hours_since_start_of_day * INTERVALS_PER_HOUR_AT_FIVE_MINUTE_RESOLUTION
        )

    def test_get_real_time_adders_and_reserves_latest(self):
        df = self.iso.get_real_time_adders_and_reserves("latest")

        self._check_real_time_adders_and_reserves(df)

        assert len(df) == 1

    def test_get_real_time_adders_and_reserves_historical(self):
        date = self.local_today() - pd.DateOffset(days=3)
        df = self.iso.get_real_time_adders_and_reserves(date)

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=1)

        self._check_real_time_adders_and_reserves(df)

        assert len(df) >= 24 * INTERVALS_PER_HOUR_AT_FIVE_MINUTE_RESOLUTION

    def test_get_real_time_adders_and_reserves_historical_range(self):
        start = self.local_today() - pd.DateOffset(days=4)
        end = self.local_today() - pd.DateOffset(days=2)
        df = self.iso.get_real_time_adders_and_reserves(
            start=start,
            end=end,
        )

        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval End"].max() == self.local_start_of_day(end)

        self._check_real_time_adders_and_reserves(df)

        assert len(df) >= 24 * INTERVALS_PER_HOUR_AT_FIVE_MINUTE_RESOLUTION * 2

    """get_temperature_forecast_by_weather_zone"""

    def _check_temperature_forecast_by_weather_zone(self, df):
        assert (
            df.columns.tolist()
            == [
                "Interval Start",
                "Interval End",
                "Publish Time",
            ]
            + self.weather_zone_columns
        )

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

    # For some reason, the datasets (confirmed in the raw data) start with
    # HourEnding 02:00 and end with HourEnding 01:00
    temperature_forecast_start_offset = -pd.DateOffset(days=3, hours=-1)
    temperature_forecast_end_offset = pd.DateOffset(days=9, hours=1)

    def test_get_temperature_forecast_by_weather_zone_today_and_latest(self):
        df = self.iso.get_temperature_forecast_by_weather_zone("today")
        self._check_temperature_forecast_by_weather_zone(df)

        # One publish time
        assert df["Publish Time"].nunique() == 1

        # Data goes into the past 3 days.
        assert (
            df["Interval Start"].min()
            == self.local_start_of_today() + self.temperature_forecast_start_offset
        )

        assert (
            df["Interval End"].max()
            == self.local_start_of_today() + self.temperature_forecast_end_offset
        )

        assert self.iso.get_temperature_forecast_by_weather_zone("latest").equals(df)

    def test_get_temperature_forecast_by_weather_zone_historical_date(self):
        date = self.local_today() - pd.DateOffset(days=22)
        df = self.iso.get_temperature_forecast_by_weather_zone(date)

        assert df["Publish Time"].nunique() == 1

        assert (
            df["Interval Start"].min()
            == self.local_start_of_day(date) + self.temperature_forecast_start_offset
        )

        assert (
            df["Interval End"].max()
            == self.local_start_of_day(
                date,
            )
            + self.temperature_forecast_end_offset
        )

        self._check_temperature_forecast_by_weather_zone(df)

    def test_get_temperature_forecast_by_weather_zone_historical_range(self):
        start = self.local_today() - pd.DateOffset(days=24)
        end = self.local_today() - pd.DateOffset(days=21)

        df = self.iso.get_temperature_forecast_by_weather_zone(
            start=start,
            end=end,
        )

        assert df["Publish Time"].nunique() == 3
        assert (
            df["Interval Start"].min()
            == self.local_start_of_day(start) + self.temperature_forecast_start_offset
        )

        assert df["Interval End"].max() == self.local_start_of_day(
            end,
            # Non-inclusive end date
        ) + self.temperature_forecast_end_offset - pd.DateOffset(days=1)

        self._check_temperature_forecast_by_weather_zone(df)

    """parse_doc"""

    def test_parse_doc_works_on_dst_data(self):
        data_string = """DeliveryDate,TimeEnding,Demand,DSTFlag
        03/13/2016,01:15,26362.1563,N
        03/13/2016,01:30,26123.679,N
        03/13/2016,01:45,25879.7454,N
        03/13/2016,03:00,25668.166,N
        """
        # Read the data into a DataFrame
        df = pd.read_csv(StringIO(data_string))

        df = self.iso.parse_doc(df)

        assert df["Interval Start"].min() == pd.Timestamp(
            "2016-03-13 01:00:00-0600",
            tz="US/Central",
        )
        assert df["Interval Start"].max() == pd.Timestamp(
            "2016-03-13 01:45:00-0600",
            tz="US/Central",
        )

        assert df["Interval End"].min() == pd.Timestamp(
            "2016-03-13 01:15:00-0600",
            tz="US/Central",
        )
        # Note the hour jump due to DST
        assert df["Interval End"].max() == pd.Timestamp(
            "2016-03-13 03:00:00-0500",
            tz="US/Central",
        )

    def test_parse_doc_works_on_dst_end(self):
        data_string = """DeliveryDate,TimeEnding,Demand,DSTFlag
        11/06/2016,01:15,28907.1315,N
        11/06/2016,01:30,28595.5918,N
        11/06/2016,01:45,28266.6354,N
        11/06/2016,01:00,28057.502,N
        11/06/2016,01:15,27707.4798,Y
        11/06/2016,01:30,27396.1973,Y
        11/06/2016,01:45,27157.3464,Y
        11/06/2016,02:00,26981.778,Y
        """

        df = pd.read_csv(StringIO(data_string))

        df = self.iso.parse_doc(df)

        assert df["Interval Start"].min() == pd.Timestamp(
            "2016-11-06 00:45:00-0500",
            tz="US/Central",
        )

        assert df["Interval Start"].max() == pd.Timestamp(
            "2016-11-06 01:45:00-0600",
            tz="US/Central",
        )

        assert df["Interval End"].min() == pd.Timestamp(
            "2016-11-06 01:00:00-0500",
            tz="US/Central",
        )

        assert df["Interval End"].max() == pd.Timestamp(
            "2016-11-06 02:00:00-0600",
            tz="US/Central",
        )

    """get_lmp"""

    def test_get_lmp_electrical_bus(self):
        cols = [
            "Interval Start",
            "Interval End",
            "SCED Timestamp",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]

        df = self.iso.get_lmp(
            date="latest",
            location_type=ELECTRICAL_BUS_LOCATION_TYPE,
        )

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        now = pd.Timestamp.now(tz=self.iso.default_timezone)
        start = now - pd.Timedelta(hours=1)
        df = self.iso.get_lmp(
            location_type=ELECTRICAL_BUS_LOCATION_TYPE,
            start=start,
            end=now,
            verbose=True,
        )

        # There should be at least 12 intervals in the last hour
        # sometimes there are more if sced is run more frequently
        # subtracting 1 to allow for some flexibility
        assert df["SCED Timestamp"].nunique() >= 12 - 1

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols
        assert df["SCED Timestamp"].min() >= start
        assert df["SCED Timestamp"].max() <= now

    def test_get_lmp_settlement_point(self):
        df = self.iso.get_lmp(
            date="latest",
            location_type="Settlement Point",
        )

        cols = [
            "Interval Start",
            "Interval End",
            "SCED Timestamp",
            "Market",
            "Location",
            "Location Type",
            "LMP",
        ]

        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        assert (df["Interval Start"] == df["SCED Timestamp"].dt.floor("5min")).all()
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()

    def test_read_docs_return_empty_df(self):
        df = self.iso.read_docs(docs=[], empty_df=pd.DataFrame(columns=["test"]))

        assert df.shape[0] == 0
        assert df.columns.tolist() == ["test"]

    @staticmethod
    def _check_ercot_spp(df, market, location_type):
        """Common checks for SPP data:
        - Columns
        - One Market
        - One Location Type
        """
        cols = [
            "Time",
            "Interval Start",
            "Interval End",
            "Location",
            "Location Type",
            "Market",
            "SPP",
        ]
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols
        markets = df["Market"].unique()
        assert len(markets) == 1
        assert markets[0] == market.value

        location_types = df["Location Type"].unique()
        assert len(location_types) == 1
        assert location_types[0] == location_type

    def _check_dam_system_lambda(self, df):
        cols = [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Market",
            "System Lambda",
        ]
        assert df.shape[0] >= 0
        assert df.columns.tolist() == cols

        assert df["System Lambda"].dtype == float

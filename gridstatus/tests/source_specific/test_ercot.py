from io import StringIO
from typing import Dict

import pandas as pd
import pytest

from gridstatus import Markets, NoDataFoundException, NotSupported
from gridstatus.ercot import (
    ELECTRICAL_BUS_LOCATION_TYPE,
    Ercot,
    ERCOTSevenDayLoadForecastReport,
    parse_timestamp_from_friendly_name,
)
from gridstatus.ercot_60d_utils import (
    DAM_ENERGY_BID_AWARDS_COLUMNS,
    DAM_ENERGY_BID_AWARDS_KEY,
    DAM_ENERGY_BIDS_COLUMNS,
    DAM_ENERGY_BIDS_KEY,
    DAM_ENERGY_ONLY_OFFER_AWARDS_COLUMNS,
    DAM_ENERGY_ONLY_OFFER_AWARDS_KEY,
    DAM_ENERGY_ONLY_OFFERS_COLUMNS,
    DAM_ENERGY_ONLY_OFFERS_KEY,
    DAM_GEN_RESOURCE_AS_OFFERS_KEY,
    DAM_GEN_RESOURCE_COLUMNS,
    DAM_GEN_RESOURCE_KEY,
    DAM_LOAD_RESOURCE_AS_OFFERS_KEY,
    DAM_LOAD_RESOURCE_COLUMNS,
    DAM_LOAD_RESOURCE_KEY,
    DAM_PTP_OBLIGATION_BID_AWARDS_COLUMNS,
    DAM_PTP_OBLIGATION_BID_AWARDS_KEY,
    DAM_PTP_OBLIGATION_BIDS_COLUMNS,
    DAM_PTP_OBLIGATION_BIDS_KEY,
    DAM_PTP_OBLIGATION_OPTION_AWARDS_COLUMNS,
    DAM_PTP_OBLIGATION_OPTION_AWARDS_KEY,
    DAM_PTP_OBLIGATION_OPTION_COLUMNS,
    DAM_PTP_OBLIGATION_OPTION_KEY,
    DAM_RESOURCE_AS_OFFERS_COLUMNS,
    SCED_GEN_RESOURCE_COLUMNS,
    SCED_GEN_RESOURCE_KEY,
    SCED_LOAD_RESOURCE_COLUMNS,
    SCED_LOAD_RESOURCE_KEY,
    SCED_SMNE_COLUMNS,
    SCED_SMNE_KEY,
)
from gridstatus.ercot_constants import (
    SOLAR_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS,
    SOLAR_ACTUAL_AND_FORECAST_COLUMNS,
    WIND_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS,
    WIND_ACTUAL_AND_FORECAST_COLUMNS,
)
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="ercot",
    record_mode=RECORD_MODE,
)

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

    @pytest.mark.integration
    def test_get_dam_system_lambda_latest(self):
        df = self.iso.get_dam_system_lambda("latest", verbose=True)
        self._check_dam_system_lambda(df)
        # We don't know the exact publish date because it could be yesterday
        # or today depending on when this test is run
        assert df["Publish Time"].dt.date.nunique() == 1

    @pytest.mark.integration
    def test_get_dam_system_lambda_today(self):
        df = self.iso.get_dam_system_lambda("today", verbose=True)
        self._check_dam_system_lambda(df)
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        # Published yesterday
        assert df["Publish Time"].dt.date.unique() == [today - pd.Timedelta(days=1)]
        assert df["Interval Start"].dt.date.unique() == [today]

    @pytest.mark.integration
    def test_get_dam_system_lambda_historical(self):
        two_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=2)
        df = self.iso.get_dam_system_lambda(two_days_ago)
        self._check_dam_system_lambda(df)
        assert list(df["Publish Time"].dt.date.unique()) == [
            two_days_ago - pd.Timedelta(days=1),
        ]

    @pytest.mark.integration
    def test_get_dam_system_lambda_historical_range(self):
        three_days_ago = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(days=3)
        two_days_ago = three_days_ago + pd.Timedelta(days=1)
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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
    def test_get_as_monitor(self):
        df = self.iso.get_as_monitor()
        # asset length is 1, 49 columns
        assert df.shape == (1, 49)
        # assert every colunn but the first is int dtype
        assert df.iloc[:, 1:].dtypes.unique() == "int64"
        assert df.columns[0] == "Time"

    @pytest.mark.integration
    def test_get_real_time_system_conditions(self):
        df = self.iso.get_real_time_system_conditions()
        assert df.shape == (1, 15)
        assert df.columns[0] == "Time"

    @pytest.mark.integration
    def test_get_energy_storage_resources(self):
        df = self.iso.get_energy_storage_resources()
        assert df.columns.tolist() == [
            "Time",
            "Total Charging",
            "Total Discharging",
            "Net Output",
        ]

    """get_fuel_mix"""

    fuel_mix_cols = [
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

    def test_get_fuel_mix_today(self):
        with api_vcr.use_cassette("test_get_fuel_mix_today.yaml"):
            df = self.iso.get_fuel_mix("today")
        self._check_fuel_mix(df)
        assert df.shape[0] >= 0
        assert df.columns.tolist() == self.fuel_mix_cols

    def test_get_fuel_mix_latest(self):
        with api_vcr.use_cassette("test_get_fuel_mix_latest.yaml"):
            df = self.iso.get_fuel_mix("latest")
        self._check_fuel_mix(df)
        # returns two days of data
        assert df["Time"].dt.date.nunique() == 2
        assert df.shape[0] >= 0
        assert df.columns.tolist() == self.fuel_mix_cols

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

    """get_fuel_mix_detailed"""
    fuel_mix_detailed_columns = [
        "Time",
        "Coal and Lignite Gen",
        "Coal and Lignite HSL",
        "Coal and Lignite Seasonal Capacity",
        "Hydro Gen",
        "Hydro HSL",
        "Hydro Seasonal Capacity",
        "Nuclear Gen",
        "Nuclear HSL",
        "Nuclear Seasonal Capacity",
        "Power Storage Gen",
        "Power Storage HSL",
        "Power Storage Seasonal Capacity",
        "Solar Gen",
        "Solar HSL",
        "Solar Seasonal Capacity",
        "Wind Gen",
        "Wind HSL",
        "Wind Seasonal Capacity",
        "Natural Gas Gen",
        "Natural Gas HSL",
        "Natural Gas Seasonal Capacity",
        "Other Gen",
        "Other HSL",
        "Other Seasonal Capacity",
    ]

    def test_get_fuel_mix_detailed_latest(self):
        with api_vcr.use_cassette("test_get_fuel_mix_detailed_latest.yaml"):
            df = self.iso.get_fuel_mix_detailed("latest")
        assert df.columns.tolist() == self.fuel_mix_detailed_columns
        assert df["Time"].dt.date.nunique() == 2

    def test_get_fuel_mix_detailed_today(self):
        with api_vcr.use_cassette("test_get_fuel_mix_detailed_today.yaml"):
            df = self.iso.get_fuel_mix_detailed("today")
        assert df.columns.tolist() == self.fuel_mix_detailed_columns
        assert df["Time"].dt.date.nunique() == 1

    """get_lmp"""

    @pytest.mark.skip(reason="Not Applicable")
    def test_lmp_date_range(self, markets=None):
        pass

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_lmp_historical(self, markets=None):
        pass

    @pytest.mark.integration
    def test_get_load_3_days_ago(self):
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()
        three_days_ago = today - pd.Timedelta(days=3)
        df = self.iso.get_load(three_days_ago)
        self._check_load(df)
        assert df["Time"].unique()[0].date() == three_days_ago

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
    def test_get_load_forecast_historical(self):
        test_date = (pd.Timestamp.now() - pd.Timedelta(days=2)).date()
        forecast = self.iso.get_load_forecast(date=test_date)
        self._check_forecast(
            forecast,
            expected_columns=self.expected_load_forecast_columns,
        )

    @pytest.mark.integration
    def test_get_load_forecast_today(self):
        forecast = self.iso.get_load_forecast("today")
        self._check_forecast(
            forecast,
            expected_columns=self.expected_load_forecast_columns,
        )

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
    def test_get_available_seasonal_capacity_forecast(self):
        df = self.iso.get_available_seasonal_capacity_forecast("latest")

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Available Capacity",
            "Load Forecast",
        ]

        # Use DateOffset for comparisons because it takes into account DST
        assert df[
            "Interval Start"
        ].min() == self.local_start_of_today() + pd.DateOffset(
            days=1,
        )
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

        # This can use a timedelta because it doesn't span a day
        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            hours=1,
        )

    """get_spp"""

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.slow
    @pytest.mark.integration
    def test_get_spp_rtm_historical(self):
        rtm = self.iso.get_rtm_spp(2020)
        assert isinstance(rtm, pd.DataFrame)
        assert len(rtm) > 0

    @pytest.mark.slow
    @pytest.mark.integration
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

    @pytest.mark.integration
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
    @pytest.mark.integration
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

        with api_vcr.use_cassette(
            f"test_get_60_day_sced_disclosure_historical_{days_ago_65}",
        ):
            df_dict = self.iso.get_60_day_sced_disclosure(
                date=days_ago_65,
                process=True,
            )

        load_resource = df_dict[SCED_LOAD_RESOURCE_KEY]
        gen_resource = df_dict[SCED_GEN_RESOURCE_KEY]
        smne = df_dict[SCED_SMNE_KEY]

        assert load_resource["SCED Timestamp"].dt.date.unique()[0] == days_ago_65
        assert gen_resource["SCED Timestamp"].dt.date.unique()[0] == days_ago_65
        assert smne["Interval Time"].dt.date.unique()[0] == days_ago_65

        check_60_day_sced_disclosure(df_dict)

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

        with api_vcr.use_cassette(
            f"test_get_60_day_sced_disclosure_range_{days_ago_66}_{days_ago_65}",
        ):
            df_dict = self.iso.get_60_day_sced_disclosure(
                start=days_ago_66,
                end=days_ago_65
                + pd.Timedelta(days=1),  # add one day to end date since exclusive
                process=True,
                verbose=True,
            )

        load_resource = df_dict[SCED_LOAD_RESOURCE_KEY]
        gen_resource = df_dict[SCED_GEN_RESOURCE_KEY]
        smne = df_dict[SCED_SMNE_KEY]

        self._check_60_day_sced_disclosure(df_dict)

        assert load_resource["SCED Timestamp"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

        assert gen_resource["SCED Timestamp"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

        assert smne["Interval Time"].dt.date.unique().tolist() == [
            days_ago_66,
            days_ago_65,
        ]

    """get_60_day_dam_disclosure"""

    @pytest.mark.integration
    def test_get_60_day_dam_disclosure_historical(self):
        days_ago_65 = pd.Timestamp.now(
            tz=self.iso.default_timezone,
        ).date() - pd.Timedelta(
            days=65,
        )

        df_dict = self.iso.get_60_day_dam_disclosure(date=days_ago_65, process=True)

        check_60_day_dam_disclosure(df_dict)

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
    def test_get_unplanned_resource_outages_historical_date(self):
        five_days_ago = self.local_start_of_today() - pd.DateOffset(days=5)
        df = self.iso.get_unplanned_resource_outages(date=five_days_ago)

        self._check_unplanned_resource_outages(df)

        assert df["Current As Of"].dt.date.unique() == [
            (five_days_ago - pd.DateOffset(days=3)).date(),
        ]
        assert df["Publish Time"].dt.date.unique() == [five_days_ago.date()]

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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
        ] + bid_curve_columns

        assert df.columns.tolist() == cols

        for col in bid_curve_columns:
            # Check that the first non-null value is a list of lists
            first_non_null_value = df[col].dropna().iloc[0]
            assert isinstance(first_non_null_value, list)
            assert all(isinstance(x, list) for x in first_non_null_value)

    """get_reported_outages"""

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    """get_wind_actual_and_forecast_hourly"""

    def _check_hourly_wind_report(self, df, geographic_data=False):
        assert (
            df.columns.tolist() == WIND_ACTUAL_AND_FORECAST_COLUMNS
            if not geographic_data
            else WIND_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS
        )

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    @pytest.mark.integration
    def test_get_wind_actual_and_forecast_hourly_today(self):
        df = self.iso.get_wind_actual_and_forecast_hourly("today", verbose=True)

        self._check_hourly_wind_report(df)

        hours_since_local_midnight = (
            self.local_now() - self.local_start_of_today()
        ) // pd.Timedelta(hours=1)

        assert df["Publish Time"].nunique() == hours_since_local_midnight

    @pytest.mark.integration
    def test_get_wind_actual_and_forecast_hourly_latest(self):
        df = self.iso.get_wind_actual_and_forecast_hourly("latest", verbose=True)

        self._check_hourly_wind_report(df)

        assert df["Publish Time"].nunique() == 1

    @pytest.mark.integration
    def test_get_wind_actual_and_forecast_hourly_historical_date(self):
        date = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_wind_actual_and_forecast_hourly(date, verbose=True)

        self._check_hourly_wind_report(df)

        assert df["Publish Time"].nunique() == 24  # One for each hour
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    @pytest.mark.integration
    def test_get_wind_actual_and_forecast_hourly_historical_date_range(self):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_wind_actual_and_forecast_hourly(start, end, verbose=True)

        self._check_hourly_wind_report(df)

        assert df["Publish Time"].nunique() == 48
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    """get_wind_actual_and_forecast_by_geographical_region_hourly"""

    def test_get_wind_actual_and_forecast_by_geographical_region_hourly_today(self):
        with api_vcr.use_cassette(
            "test_get_wind_actual_and_forecast_by_geographical_region_hourly_today.yaml",
        ):
            df = self.iso.get_wind_actual_and_forecast_by_geographical_region_hourly(
                "today",
                verbose=True,
            )

        self._check_hourly_wind_report(df, geographic_data=True)

        hours_since_local_midnight = (
            self.local_now() - self.local_start_of_today()
        ) // pd.Timedelta(hours=1)

        assert df["Publish Time"].nunique() == hours_since_local_midnight

    def test_get_wind_actual_and_forecast_by_geographical_region_hourly_historical_date_range(  # noqa: E501
        self,
    ):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"test_get_wind_actual_and_forecast_by_geographical_region_hourly_historical_date_range_{start}_{end}.yaml",  # noqa: E501
        ):
            df = self.iso.get_wind_actual_and_forecast_by_geographical_region_hourly(
                start,
                end,
                verbose=True,
            )

        self._check_hourly_wind_report(df, geographic_data=True)

        assert df["Publish Time"].nunique() == 48
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    """get_solar_actual_and_forecast_hourly"""

    def test_get_solar_actual_and_forecast_hourly_today(self):
        with api_vcr.use_cassette(
            "test_get_solar_actual_and_forecast_hourly_today.yaml",
        ):
            df = self.iso.get_solar_actual_and_forecast_hourly("today", verbose=True)

        self._check_hourly_solar_report(df)

        hours_since_local_midnight = (
            self.local_now() - self.local_start_of_today()
        ) // pd.Timedelta(hours=1)

        assert df["Publish Time"].nunique() == hours_since_local_midnight

    def test_get_solar_actual_and_forecast_hourly_historical_date_range(self):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=1)

        with api_vcr.use_cassette(
            f"test_get_solar_actual_and_forecast_hourly_historical_date_range_{start}_{end}.yaml",  # noqa: E501
        ):
            df = self.iso.get_solar_actual_and_forecast_hourly(start, end, verbose=True)

        self._check_hourly_solar_report(df)

        assert df["Publish Time"].nunique() == 48
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    """get_solar_actual_and_forecast_by_geographical_region_hourly"""

    def _check_hourly_solar_report(self, df, geographic_data=False):
        assert (
            df.columns.tolist() == SOLAR_ACTUAL_AND_FORECAST_COLUMNS
            if not geographic_data
            else SOLAR_ACTUAL_AND_FORECAST_BY_GEOGRAPHICAL_REGION_COLUMNS
        )
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    @pytest.mark.integration
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_today(self):
        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            "today",
            verbose=True,
        )

        self._check_hourly_solar_report(df, geographic_data=True)

        hours_since_local_midnight = (
            self.local_now() - self.local_start_of_today()
        ) // pd.Timedelta(hours=1)

        assert df["Publish Time"].nunique() == hours_since_local_midnight

    @pytest.mark.integration
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_latest(self):
        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            "latest",
            verbose=True,
        )

        self._check_hourly_solar_report(df, geographic_data=True)

        assert df["Publish Time"].nunique() == 1

    @pytest.mark.integration
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_historical_date(  # noqa: E501
        self,
    ):
        date = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            date,
            verbose=True,
        )

        self._check_hourly_solar_report(df, geographic_data=True)

        assert df["Publish Time"].nunique() == 24  # One for each hour
        assert df["Publish Time"].min().hour == 0
        assert df["Publish Time"].max().hour == 23

    @pytest.mark.integration
    def test_get_solar_actual_and_forecast_by_geographical_region_hourly_historical_date_range(
        self,
    ):
        start = self.local_today() - pd.Timedelta(days=3)
        end = self.local_today() - pd.Timedelta(days=1)
        df = self.iso.get_solar_actual_and_forecast_by_geographical_region_hourly(
            start,
            end,
            verbose=True,
        )

        self._check_hourly_solar_report(df, geographic_data=True)

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

    @pytest.mark.integration
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
    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
    def test_get_short_term_system_adequacy_latest(self):
        df = self.iso.get_short_term_system_adequacy("latest")

        self._check_short_term_system_adequacy(df)

        assert df["Publish Time"].nunique() == 1

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

    @pytest.mark.integration
    def test_get_short_term_system_adequacy_historical_date(self):
        date = self.local_today() - pd.DateOffset(days=15)
        df = self.iso.get_short_term_system_adequacy(date)

        assert df["Publish Time"].nunique() >= 24

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=7)

        self._check_short_term_system_adequacy(df)

    @pytest.mark.integration
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

    @pytest.mark.integration
    def test_get_real_time_adders_and_reserves_today(self):
        df = self.iso.get_real_time_adders_and_reserves("today")

        self._check_real_time_adders_and_reserves(df)

        hours_since_start_of_day = (
            self.local_now() - self.local_start_of_today()
            # Integer division
        ) // pd.Timedelta(hours=1)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert (
            len(df)
            >= hours_since_start_of_day * INTERVALS_PER_HOUR_AT_FIVE_MINUTE_RESOLUTION
        )

    @pytest.mark.integration
    def test_get_real_time_adders_and_reserves_latest(self):
        df = self.iso.get_real_time_adders_and_reserves("latest")

        self._check_real_time_adders_and_reserves(df)

        assert len(df) == 1

    @pytest.mark.integration
    def test_get_real_time_adders_and_reserves_historical(self):
        date = self.local_today() - pd.DateOffset(days=3)
        df = self.iso.get_real_time_adders_and_reserves(date)

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=1)

        self._check_real_time_adders_and_reserves(df)

        assert len(df) >= 24 * INTERVALS_PER_HOUR_AT_FIVE_MINUTE_RESOLUTION

    @pytest.mark.integration
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

    temperature_forecast_start_offset = -pd.DateOffset(days=3)
    temperature_forecast_end_offset = pd.DateOffset(days=9)

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    @pytest.mark.integration
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

    def test_get_documents_raises_exception_when_no_docs(self):
        with pytest.raises(NoDataFoundException):
            self.iso.get_load_forecast("2010-01-01")

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp.now().normalize() - pd.Timedelta(hours=1),
                pd.Timestamp.now().normalize(),
            ),
        ],
    )
    def test_get_indicative_lmp_by_settlement_point(self, date, end):
        with api_vcr.use_cassette(
            f"test_get_indicative_lmp_historical_{date}_{end}.yaml",
            record_mode="all",  # NOTE(kladar) Relative parameters and fixtures don't play nicely together yet,
            # so always record new interactions
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
            assert df["Interval Start"].min() == date.tz_localize(
                self.iso.default_timezone,
            )
            assert df["Interval End"].max() == end.tz_localize(
                self.iso.default_timezone,
            ) + pd.Timedelta(minutes=50)

    """get_dam_total_energy_purchased"""

    def _check_dam_total_energy_purchased(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Location",
            "Total",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()
        assert df["Total"].dtype == float
        assert df["Location"].dtype == object

    def test_get_dam_total_energy_purchased_today(self):
        with api_vcr.use_cassette(
            "test_get_dam_total_energy_purchased_today.yaml",
        ):
            df = self.iso.get_dam_total_energy_purchased("today")

        self._check_dam_total_energy_purchased(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df[
            "Interval Start"
        ].max() == self.local_start_of_today() + pd.DateOffset(days=1, hours=-1)

    def test_get_dam_total_energy_purchased_historical_date_range(self):
        start = self.local_today() - pd.DateOffset(days=8)
        end = start + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            f"test_get_dam_total_energy_purchased_historical_date_range_{start}_{end}.yaml",
        ):
            df = self.iso.get_dam_total_energy_purchased(start, end)

        self._check_dam_total_energy_purchased(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval Start"].max() == self.local_start_of_day(
            end,
        ) - pd.DateOffset(
            hours=1,
        )

    """get_dam_total_energy_sold"""

    def _check_dam_total_energy_sold(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Location",
            "Total",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()
        assert df["Total"].dtype == float
        assert df["Location"].dtype == object

    def test_get_dam_total_energy_sold_today(self):
        with api_vcr.use_cassette(
            "test_get_dam_total_energy_sold_today.yaml",
        ):
            df = self.iso.get_dam_total_energy_sold("today")

        self._check_dam_total_energy_sold(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df[
            "Interval Start"
        ].max() == self.local_start_of_today() + pd.DateOffset(days=1, hours=-1)

    def test_get_dam_total_energy_sold_historical_date_range(self):
        start = self.local_today() - pd.DateOffset(days=15)
        end = start + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            f"test_get_dam_total_energy_sold_historical_date_range_{start}_{end}.yaml",
        ):
            df = self.iso.get_dam_total_energy_sold(start, end)

        self._check_dam_total_energy_sold(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval Start"].max() == self.local_start_of_day(
            end,
        ) - pd.DateOffset(hours=1)

    """get_cop_adjustment_period_snapshot_60_day"""

    def _check_cop_adjustment_period_snapshot_60_day(self, df):
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

        # Column not in newer data so it's added as null
        assert df["RRS"].isnull().all()

        # Columns not in older data but should be present in newer data
        for col in [
            "High Sustained Limit",
            "Low Sustained Limit",
            "High Emergency Limit",
            "Low Emergency Limit",
            "Reg Up",
            "Reg Down",
            "RRSPFR",
            "RRSFFR",
            "RRSUFR",
            "NSPIN",
            "ECRS",
        ]:
            assert df[col].notnull().all()

    def test_get_cop_adjustment_period_snapshot_60_day_raises_error(self):
        with pytest.raises(ValueError):
            self.iso.get_cop_adjustment_period_snapshot_60_day(
                start=self.local_today() - pd.DateOffset(days=59),
                end=self.local_today(),
            )

    def test_get_cop_adjustment_period_snapshot_60_day_historical_date_range(self):
        # Must be at least 60 days in the past
        start = self.local_today() - pd.DateOffset(days=63)
        end = start + pd.DateOffset(days=2)

        with api_vcr.use_cassette(
            f"test_get_cop_adjustment_period_snapshot_60_day_historical_date_range_{start}_{end}.yaml",
        ):
            df = self.iso.get_cop_adjustment_period_snapshot_60_day(start, end)

        self._check_cop_adjustment_period_snapshot_60_day(df)

        assert df["Interval Start"].min() == self.local_start_of_day(start)
        assert df["Interval Start"].max() == self.local_start_of_day(
            end,
        ) - pd.DateOffset(hours=1)


def check_60_day_sced_disclosure(df_dict: Dict[str, pd.DataFrame]) -> None:
    load_resource = df_dict[SCED_LOAD_RESOURCE_KEY]
    gen_resource = df_dict[SCED_GEN_RESOURCE_KEY]
    smne = df_dict[SCED_SMNE_KEY]

    assert load_resource.columns.tolist() == SCED_LOAD_RESOURCE_COLUMNS
    assert gen_resource.columns.tolist() == SCED_GEN_RESOURCE_COLUMNS
    assert smne.columns.tolist() == SCED_SMNE_COLUMNS


def check_60_day_dam_disclosure(df_dict):
    assert df_dict is not None

    dam_gen_resource = df_dict[DAM_GEN_RESOURCE_KEY]
    dam_gen_resource_as_offers = df_dict[DAM_GEN_RESOURCE_AS_OFFERS_KEY]
    dam_load_resource = df_dict[DAM_LOAD_RESOURCE_KEY]
    dam_load_resource_as_offers = df_dict[DAM_LOAD_RESOURCE_AS_OFFERS_KEY]
    dam_energy_only_offer_awards = df_dict[DAM_ENERGY_ONLY_OFFER_AWARDS_KEY]
    dam_energy_only_offers = df_dict[DAM_ENERGY_ONLY_OFFERS_KEY]
    dam_ptp_obligation_bid_awards = df_dict[DAM_PTP_OBLIGATION_BID_AWARDS_KEY]
    dam_ptp_obligation_bids = df_dict[DAM_PTP_OBLIGATION_BIDS_KEY]
    dam_energy_bid_awards = df_dict[DAM_ENERGY_BID_AWARDS_KEY]
    dam_energy_bids = df_dict[DAM_ENERGY_BIDS_KEY]
    dam_ptp_obligation_option = df_dict[DAM_PTP_OBLIGATION_OPTION_KEY]
    dam_ptp_obligation_option_awards = df_dict[DAM_PTP_OBLIGATION_OPTION_AWARDS_KEY]

    assert dam_gen_resource.columns.tolist() == DAM_GEN_RESOURCE_COLUMNS
    assert dam_gen_resource_as_offers.columns.tolist() == DAM_RESOURCE_AS_OFFERS_COLUMNS
    assert dam_load_resource.columns.tolist() == DAM_LOAD_RESOURCE_COLUMNS

    assert (
        dam_load_resource_as_offers.columns.tolist() == DAM_RESOURCE_AS_OFFERS_COLUMNS
    )

    assert (
        dam_energy_only_offer_awards.columns.tolist()
        == DAM_ENERGY_ONLY_OFFER_AWARDS_COLUMNS
    )

    assert dam_energy_only_offers.columns.tolist() == DAM_ENERGY_ONLY_OFFERS_COLUMNS

    assert (
        dam_ptp_obligation_bid_awards.columns.tolist()
        == DAM_PTP_OBLIGATION_BID_AWARDS_COLUMNS
    )

    assert dam_ptp_obligation_bids.columns.tolist() == DAM_PTP_OBLIGATION_BIDS_COLUMNS

    assert dam_energy_bid_awards.columns.tolist() == DAM_ENERGY_BID_AWARDS_COLUMNS
    assert dam_energy_bids.columns.tolist() == DAM_ENERGY_BIDS_COLUMNS

    assert (
        dam_ptp_obligation_option.columns.tolist() == DAM_PTP_OBLIGATION_OPTION_COLUMNS
    )

    assert (
        dam_ptp_obligation_option_awards.columns.tolist()
        == DAM_PTP_OBLIGATION_OPTION_AWARDS_COLUMNS
    )

    assert not dam_gen_resource_as_offers.duplicated(
        subset=["Interval Start", "Interval End", "QSE", "DME", "Resource Name"],
    ).any()

    assert not dam_load_resource_as_offers.duplicated(
        subset=["Interval Start", "Interval End", "QSE", "DME", "Resource Name"],
    ).any()

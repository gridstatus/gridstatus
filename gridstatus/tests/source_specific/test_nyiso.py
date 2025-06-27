from typing import Literal

import pandas as pd
import pytest

import gridstatus
from gridstatus import NYISO, Markets
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

nyiso_vcr = setup_vcr(
    source="nyiso",
    record_mode=RECORD_MODE,
)


class TestNYISO(BaseTestISO):
    iso = NYISO()

    """"get_capacity_prices"""

    @pytest.mark.parametrize(
        "date",
        [
            "Dec 1, 2022",
            "Jan 1, 2023",
            "Dec 1, 2023",
            "Jan 1, 2024",
            "Jan 1, 2025",
            "today",
        ],
    )
    def test_get_capacity_prices(self, date):
        with nyiso_vcr.use_cassette(
            f"test_get_capacity_prices_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_capacity_prices(date=date, verbose=True)
            assert not df.empty, "DataFrame came back empty"
            # TODO: missing report: https://github.com/gridstatus/gridstatus/issues/309

    """get_fuel_mix"""

    @pytest.mark.parametrize(
        "date,end",
        [
            ("Aug 1, 2022", "Oct 22, 2022"),
        ],
    )
    def test_get_fuel_mix_date_range(self, date, end):
        with nyiso_vcr.use_cassette(
            f"test_get_fuel_mix_date_range_{pd.Timestamp(date).strftime('%Y-%m-%d')}_{pd.Timestamp(end).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_fuel_mix(start=date, end=end)
            assert df.shape[0] >= 0

    def test_range_two_days_across_month(self):
        today = gridstatus.utils._handle_date("today", self.iso.default_timezone)
        first_day_of_month = today.replace(day=1, hour=5, minute=0, second=0)
        last_day_of_prev_month = first_day_of_month - pd.Timedelta(days=1)
        with nyiso_vcr.use_cassette(
            f"test_get_fuel_mix_range_two_days_across_month_{last_day_of_prev_month.strftime('%Y-%m-%d')}_{first_day_of_month.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_fuel_mix(
                start=last_day_of_prev_month,
                end=first_day_of_month,
            )

        # Midnight of the end date
        assert df["Time"].max() == first_day_of_month.normalize() + pd.Timedelta(days=1)
        # First 5 minute interval of the start date
        assert df["Time"].min() == last_day_of_prev_month.normalize() + pd.Timedelta(
            minutes=5,
        )

        assert df["Time"].dt.date.nunique() == 3  # 2 days in range + 1 day for midnight
        self._check_fuel_mix(df)

    @pytest.mark.parametrize(
        "date,end",
        [
            ("2022-01-01T06:00:00Z", "2022-03-01T06:00:00Z"),
        ],
    )
    def test_month_start_multiple_months(self, date, end):
        date = pd.Timestamp(date, tz=self.iso.default_timezone)
        end = pd.Timestamp(end, tz=self.iso.default_timezone)
        with nyiso_vcr.use_cassette(
            f"test_month_start_multiple_months_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_fuel_mix(start=date, end=end)

            # Midnight of the end date
            assert df["Time"].max() == end.replace(
                minute=0,
                hour=0,
            ) + pd.Timedelta(
                days=1,
            )
            # First 5 minute interval of the start date
            assert df["Time"].min() == date.replace(minute=5, hour=0)
            assert (df["Time"].dt.month.unique() == [1, 2, 3]).all()
            self._check_fuel_mix(df)

    """get_generators"""

    def test_get_generators(self):
        with nyiso_vcr.use_cassette(
            "test_get_generators.yaml",
        ):
            df = self.iso.get_generators()
            columns = [
                "Generator Name",
                "PTID",
                "Subzone",
                "Zone",
                "Latitude",
                "Longitude",
            ]
            assert set(df.columns).issuperset(set(columns))
            assert df.shape[0] >= 0

    """get_load"""

    @pytest.mark.parametrize(
        "date",
        ["today", "latest"],
    )
    def test_get_load_contains_zones(self, date):
        with nyiso_vcr.use_cassette(
            f"test_get_load_contains_zones_{date}.yaml",
        ):
            df = self.iso.get_load(date=date)
            nyiso_load_cols = [
                "Time",
                "Load",
                "CAPITL",
                "CENTRL",
                "DUNWOD",
                "GENESE",
                "HUD VL",
                "LONGIL",
                "MHK VL",
                "MILLWD",
                "N.Y.C.",
                "NORTH",
                "WEST",
            ]
            assert df.columns.tolist() == nyiso_load_cols

    @pytest.mark.parametrize(
        "date,end",
        [
            ("2023-04-01", "2023-05-16"),
        ],
    )
    def test_get_load_month_range(self, date, end):
        with nyiso_vcr.use_cassette(
            f"test_get_load_month_range_{pd.Timestamp(date).strftime('%Y-%m-%d')}_{pd.Timestamp(end).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load(start=date, end=end)
            assert df.shape[0] >= 0

    @pytest.mark.parametrize(
        "lookback_days",
        [8],
    )
    def test_get_load_historical(self, lookback_days):
        # TODO: why does this not work more than 8 days in the past
        super().test_get_load_historical(lookback_days=lookback_days)

    """get_lmp"""

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_lmp_date_range(self, market):
        with nyiso_vcr.use_cassette(
            f"test_lmp_date_range_{market}.yaml",
        ):
            super().test_lmp_date_range(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
        # Markets.REAL_TIME_15_MIN, # Not supported
    )
    def test_get_lmp_historical(self, market):
        with nyiso_vcr.use_cassette(
            f"test_get_lmp_historical_{market}.yaml",
        ):
            super().test_get_lmp_historical(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_15_MIN,
    )
    def test_get_lmp_today(self, market):
        with nyiso_vcr.use_cassette(
            f"test_get_lmp_today_{market}.yaml",
        ):
            super().test_get_lmp_today(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
        Markets.REAL_TIME_15_MIN,
    )
    def test_get_lmp_latest(self, market):
        with nyiso_vcr.use_cassette(
            f"test_get_lmp_latest_{market}.yaml",
        ):
            super().test_get_lmp_latest(market=market)

    @pytest.mark.parametrize(
        "market, interval_duration_minutes",
        [(Markets.REAL_TIME_5_MIN, 5), (Markets.REAL_TIME_15_MIN, 15)],
    )
    def test_get_lmp_real_time_today(self, market, interval_duration_minutes):
        with nyiso_vcr.use_cassette(
            f"test_get_lmp_real_time_{interval_duration_minutes}_min_today.yaml",
        ):
            df = self.iso.get_lmp("today", market=market)

            assert (
                df["Interval End"] - df["Interval Start"]
                == pd.Timedelta(minutes=interval_duration_minutes)
            ).all()

            diffs = df["Interval End"].diff()
            assert diffs[diffs > pd.Timedelta(minutes=0)].min() <= pd.Timedelta(
                minutes=interval_duration_minutes,
            )
            assert diffs.max() == pd.Timedelta(minutes=interval_duration_minutes)

    @pytest.mark.parametrize(
        "market, interval_duration_minutes",
        [(Markets.REAL_TIME_5_MIN, 5), (Markets.REAL_TIME_15_MIN, 15)],
    )
    def test_get_lmp_real_time_5_and_15_min_latest(
        self,
        market,
        interval_duration_minutes,
    ):
        with nyiso_vcr.use_cassette(
            f"test_get_lmp_real_time_{interval_duration_minutes}_min_latest.yaml",
        ):
            df = self.iso.get_lmp("latest", market=market)

            assert (
                df["Interval End"] - df["Interval Start"]
                == pd.Timedelta(minutes=interval_duration_minutes)
            ).all()

            diffs = df["Interval End"].diff().dropna()
            # There is only one interval, so the diff is 0
            assert (diffs == pd.Timedelta(minutes=0)).all()

    @pytest.mark.parametrize(
        "start,end",
        [
            ("2021-12-01", "2022-02-02"),
        ],
    )
    def test_get_lmp_historical_with_range(self, start, end):
        with nyiso_vcr.use_cassette(
            f"test_get_lmp_historical_with_range_{start}_{end}.yaml",
        ):
            df = self.iso.get_lmp(
                start=start,
                end=end,
                market=Markets.REAL_TIME_5_MIN,
            )
            assert df.shape[0] >= 0

    @pytest.mark.parametrize(
        "date",
        ["2022-06-09"],
    )
    def test_get_lmp_location_type_zone_historical_date(self, date):
        with nyiso_vcr.use_cassette(
            f"test_get_lmp_location_type_zone_historical_date_{date}.yaml",
        ):
            df_zone = self.iso.get_lmp(
                date=date,
                market=Markets.DAY_AHEAD_HOURLY,
                location_type="zone",
            )
            assert (df_zone["Location Type"] == "Zone").all()

    def test_get_lmp_location_type_zone_today(self):
        with nyiso_vcr.use_cassette(
            "test_get_lmp_location_type_zone_today.yaml",
        ):
            df_zone = self.iso.get_lmp(
                date="today",
                market=Markets.DAY_AHEAD_HOURLY,
                location_type="zone",
            )
            assert (df_zone["Location Type"] == "Zone").all()

    def test_get_lmp_location_type_zone_latest(self):
        with nyiso_vcr.use_cassette(
            "test_get_lmp_location_type_zone_latest.yaml",
        ):
            df_zone = self.iso.get_lmp(
                date="latest",
                market=Markets.DAY_AHEAD_HOURLY,
                location_type="zone",
            )
            assert (df_zone["Location Type"] == "Zone").all()

    @pytest.mark.parametrize(
        "date",
        ["2022-06-09"],
    )
    def test_get_lmp_location_type_generator_historical_date(self, date):
        with nyiso_vcr.use_cassette(
            f"test_get_lmp_location_type_parameter_{date}.yaml",
        ):
            df_gen = self.iso.get_lmp(
                date=date,
                market=Markets.DAY_AHEAD_HOURLY,
                location_type="generator",
            )
            assert (df_gen["Location Type"] == "Generator").all()

    def test_get_lmp_location_type_generator_today(self):
        with nyiso_vcr.use_cassette(
            "test_get_lmp_location_type_generator_today.yaml",
        ):
            df_gen = self.iso.get_lmp(
                date="today",
                market=Markets.DAY_AHEAD_HOURLY,
                location_type="generator",
            )
            assert (df_gen["Location Type"] == "Generator").all()

    def test_get_lmp_location_type_generator_latest(self):
        with nyiso_vcr.use_cassette(
            "test_get_lmp_location_type_generator_latest.yaml",
        ):
            df_gen = self.iso.get_lmp(
                date="latest",
                market=Markets.DAY_AHEAD_HOURLY,
                location_type="generator",
            )
            assert (df_gen["Location Type"] == "Generator").all()

    @pytest.mark.parametrize(
        "date,market,location_type",
        [
            ("latest", Markets.DAY_AHEAD_HOURLY, "dummy"),
        ],
    )
    def test_get_lmp_location_type_dummy(self, date, market, location_type):
        with pytest.raises(ValueError):
            self.iso.get_lmp(
                date=date,
                market=market,
                location_type=location_type,
            )

    """get_interconnection_queue"""

    # This test is in addition to the base_test_iso test
    def test_get_interconnection_queue_handles_new_file(self):
        with nyiso_vcr.use_cassette(
            "test_get_interconnection_queue_handles_new_file.yaml",
        ):
            df = self.iso.get_interconnection_queue()
            # There are a few missing values, but a small percentage
            assert df["Interconnection Location"].isna().sum() < 0.01 * df.shape[0]

    """get_loads"""

    def test_get_loads(self):
        with nyiso_vcr.use_cassette(
            "test_get_loads.yaml",
        ):
            df = self.iso.get_loads()
            columns = [
                "Load Name",
                "PTID",
                "Subzone",
                "Zone",
            ]
            assert set(df.columns) == set(columns)
            assert df.shape[0] >= 0

    """get_status"""

    @pytest.mark.parametrize(
        "date",
        ["20220609"],
    )
    def test_get_status_historical_status(self, date):
        with nyiso_vcr.use_cassette(
            f"test_get_status_historical_status_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            status = self.iso.get_status(date)
            self._check_status(status)

    @pytest.mark.parametrize(
        "start,end",
        [
            ("2022-05-01", "2022-10-02"),
        ],
    )
    def test_get_status_historical_status_range(self, start, end):
        with nyiso_vcr.use_cassette(
            f"test_get_status_historical_status_range_{start}_{end}.yaml",
        ):
            status = self.iso.get_status(start=start, end=end)
            self._check_status(status)

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

    @pytest.mark.parametrize(
        "date",
        ["Nov 7, 2021"],
    )
    def test_status_edt_to_est(self, date):
        # number of rows hardcoded based on when this test was written. should stay same
        with nyiso_vcr.use_cassette(
            f"test_status_edt_to_est_{date}.yaml",
        ):
            df = self.iso.get_status(date=date)
            assert df.shape[0] >= 1

    @pytest.mark.parametrize(
        "date",
        ["Nov 7, 2021"],
    )
    def test_fuel_mix_edt_to_est(self, date):
        with nyiso_vcr.use_cassette(
            f"test_fuel_mix_edt_to_est_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_fuel_mix(date=date)
            assert df.shape[0] >= 307

    @pytest.mark.parametrize(
        "date",
        ["Nov 7, 2021"],
    )
    def test_load_forecast_edt_to_est(self, date):
        with nyiso_vcr.use_cassette(
            f"test_load_forecast_edt_to_est_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load_forecast(date=date)
            assert df.shape[0] >= 145

    @pytest.mark.parametrize(
        "date",
        ["Nov 7, 2021"],
    )
    def test_lmp_rt_5_min_edt_to_est(self, date):
        with nyiso_vcr.use_cassette(
            f"test_lmp_rt_5_min_edt_to_est_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
            assert df.shape[0] >= 4605

    @pytest.mark.parametrize(
        "date",
        ["Nov 7, 2021"],
    )
    def test_lmp_da_edt_to_est(self, date):
        with nyiso_vcr.use_cassette(
            f"test_lmp_da_edt_to_est_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp(date=date, market=Markets.DAY_AHEAD_HOURLY)
            assert df.shape[0] >= 375

    @pytest.mark.parametrize(
        "date",
        ["Nov 7, 2021"],
    )
    def test_load_edt_to_est(self, date):
        with nyiso_vcr.use_cassette(
            f"test_load_edt_to_est_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load(date=date)
            assert df.shape[0] >= 307

    @pytest.mark.parametrize(
        "date",
        ["March 14, 2021"],
    )
    def test_status_est_to_edt(self, date):
        # number of rows hardcoded based on when this test was written. should stay same
        with nyiso_vcr.use_cassette(
            f"test_status_est_to_edt_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_status(date=date)
            assert df.shape[0] >= 5

    @pytest.mark.parametrize(
        "date",
        ["March 14, 2021"],
    )
    def test_lmp_rt_5_min_est_to_edt(self, date):
        with nyiso_vcr.use_cassette(
            f"test_lmp_rt_5_min_est_to_edt_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp(date=date, market=Markets.REAL_TIME_5_MIN)
            assert df.shape[0] >= 4215

    @pytest.mark.parametrize(
        "date",
        ["March 14, 2021"],
    )
    def test_lmp_da_est_to_edt(self, date):
        with nyiso_vcr.use_cassette(
            f"test_lmp_da_est_to_edt_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp(date=date, market=Markets.DAY_AHEAD_HOURLY)
            assert df.shape[0] >= 345

    @pytest.mark.parametrize(
        "date",
        ["March 14, 2021"],
    )
    def test_load_forecast_est_to_edt(self, date):
        with nyiso_vcr.use_cassette(
            f"test_load_forecast_est_to_edt_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load_forecast(date=date)
            assert df.shape[0] >= 143

    @pytest.mark.parametrize(
        "date",
        ["March 14, 2021"],
    )
    def test_fuel_mix_est_to_edt(self, date):
        with nyiso_vcr.use_cassette(
            f"test_fuel_mix_est_to_edt_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_fuel_mix(date=date)
            assert df.shape[0] >= 281

    @pytest.mark.parametrize(
        "date",
        ["March 14, 2021"],
    )
    def test_load_est_to_edt(self, date):
        with nyiso_vcr.use_cassette(
            f"test_load_est_to_edt_{pd.Timestamp(date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load(date=date)
            assert df.shape[0] >= 281

    """get_btm_solar"""

    def test_get_btm_solar(self):
        # published ~8 hours after finish of previous day
        two_days_ago = pd.Timestamp.now(tz="US/Eastern").date() - pd.Timedelta(days=2)

        with nyiso_vcr.use_cassette(
            f"test_get_btm_solar_{two_days_ago}.yaml",
        ):
            df = self.iso.get_btm_solar(
                date=two_days_ago,
                verbose=True,
            )

            columns = [
                "Time",
                "Interval Start",
                "Interval End",
                "SYSTEM",
                "CAPITL",
                "CENTRL",
                "DUNWOD",
                "GENESE",
                "HUD VL",
                "LONGIL",
                "MHK VL",
                "MILLWD",
                "N.Y.C.",
                "NORTH",
                "WEST",
            ]

            assert df.columns.tolist() == columns
            assert df.shape[0] >= 0

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2023-04-30", "2023-05-02"),
        ],
    )
    def test_get_btm_solar_historical_date_range(self, date, end):
        with nyiso_vcr.use_cassette(
            f"test_get_btm_solar_historical_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_btm_solar(
                start=date,
                end=end,
                verbose=True,
            )
            assert df["Time"].dt.date.nunique() == 3

    """get_btm_solar_forecast"""

    def _check_btm_solar_forecast(self, df: pd.DataFrame):
        expected_columns = [
            "Time",
            "Interval Start",
            "Interval End",
            "Publish Time",
            "SYSTEM",
            "CAPITL",
            "CENTRL",
            "DUNWOD",
            "GENESE",
            "HUD VL",
            "LONGIL",
            "MHK VL",
            "MILLWD",
            "N.Y.C.",
            "NORTH",
            "WEST",
        ]
        assert df.columns.tolist() == expected_columns
        assert df.shape[0] >= 0

        assert (
            df["Publish Time"]
            == df["Interval Start"].dt.floor("D")
            - pd.DateOffset(days=1)
            + pd.Timedelta(hours=7, minutes=55)
        ).all()

    def test_get_btm_solar_forecast_today(self):
        with nyiso_vcr.use_cassette(
            "test_get_btm_solar_forecast_today.yaml",
        ):
            df = self.iso.get_btm_solar_forecast(
                date="today",
                verbose=True,
            )

        self._check_btm_solar_forecast(df)

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2023-04-30", "2023-05-02"),
        ],
    )
    def test_get_btm_solar_forecast_historical_date_range(self, date, end):
        with nyiso_vcr.use_cassette(
            f"test_get_btm_solar_forecast_historical_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_btm_solar_forecast(
                start=date,
                end=end,
                verbose=True,
            )
            assert df["Time"].dt.date.nunique() == 3

        self._check_btm_solar_forecast(df)

    """get_load_forecast"""

    def test_load_forecast_today(self):
        with nyiso_vcr.use_cassette(
            "test_load_forecast_today.yaml",
        ):
            forecast = self.iso.get_load_forecast("today")
            self._check_forecast(
                forecast,
                expected_columns=[
                    "Time",
                    "Interval Start",
                    "Interval End",
                    "Forecast Time",
                    "Load Forecast",
                ],
            )

    def test_load_forecast_historical_date_range(self):
        end = pd.Timestamp.now().normalize() - pd.Timedelta(days=14)
        date = (end - pd.Timedelta(days=7)).date()
        with nyiso_vcr.use_cassette(
            f"test_load_forecast_historical_date_range_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            forecast = self.iso.get_load_forecast(
                start=date,
                end=end,
            )

            self._check_forecast(
                forecast,
                expected_columns=[
                    "Time",
                    "Interval Start",
                    "Interval End",
                    "Forecast Time",
                    "Load Forecast",
                ],
            )

    """get_zonal_load_forecast"""

    def test_zonal_load_forecast_today(self):
        with nyiso_vcr.use_cassette(
            "test_zonal_load_forecast_today.yaml",
        ):
            df = self.iso.get_zonal_load_forecast("today")

            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "NYISO",
                "Capitl",
                "Centrl",
                "Dunwod",
                "Genese",
                "Hud Vl",
                "Longil",
                "Mhk Vl",
                "Millwd",
                "N.Y.C.",
                "North",
                "West",
            ]

            assert df["Publish Time"].nunique() == 1
            assert df["Interval Start"].min() == self.local_start_of_today()
            assert (
                (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=60)
            ).all()

    def test_zonal_load_forecast_historical_date_range(self):
        end = self.local_start_of_today() - pd.Timedelta(days=14)
        date = end - pd.Timedelta(days=7)

        with nyiso_vcr.use_cassette(
            f"test_zonal_load_forecast_historical_date_range_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_zonal_load_forecast(
                start=date,
                end=end,
            )

            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "NYISO",
                "Capitl",
                "Centrl",
                "Dunwod",
                "Genese",
                "Hud Vl",
                "Longil",
                "Mhk Vl",
                "Millwd",
                "N.Y.C.",
                "North",
                "West",
            ]

            assert df["Publish Time"].nunique() == 8
            assert df["Interval Start"].min() == self.local_start_of_day(date.date())
            assert (
                (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=60)
            ).all()

    """get_interface_limits_and_flows_5_min"""

    def test_get_interface_limits_and_flows_5_min_historical_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=10)
        end = start + pd.Timedelta(days=1)

        with nyiso_vcr.use_cassette(
            f"test_get_interface_limits_and_flows_5_min_historical_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",  # noqa: E501
        ):
            df = self.iso.get_interface_limits_and_flows_5_min(
                start=start,
                end=end,
            )

            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Interface Name",
                "Point ID",
                "Flow MW",
                "Positive Limit MW",
                "Negative Limit MW",
            ]

            assert df["Interval Start"].min() == start
            # NYISO is inclusive of the end date
            assert df["Interval End"].max() == end + pd.DateOffset(days=1)

    def test_get_interface_limits_and_flows_dst_end(self):
        start = self.local_start_of_day("2024-11-03")
        end = start + pd.DateOffset(days=1)

        with nyiso_vcr.use_cassette(
            f"test_get_interface_limits_and_flows_dst_end_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",  # noqa: E501
        ):
            df = self.iso.get_interface_limits_and_flows_5_min(
                start=start,
                end=end,
            )

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Interface Name",
            "Point ID",
            "Flow MW",
            "Positive Limit MW",
            "Negative Limit MW",
        ]

        assert df["Interval Start"].min() == start
        # NYISO is inclusive of the end date
        assert df["Interval End"].max() == end + pd.DateOffset(days=1)

    def test_get_interface_limits_and_flows_dst_start(self):
        start = self.local_start_of_day("2024-03-10")
        end = start + pd.DateOffset(days=1)

        with nyiso_vcr.use_cassette(
            f"test_get_interface_limits_and_flows_dst_start_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",  # noqa: E501
        ):
            df = self.iso.get_interface_limits_and_flows_5_min(
                start=start,
                end=end,
            )

        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Interface Name",
            "Point ID",
            "Flow MW",
            "Positive Limit MW",
            "Negative Limit MW",
        ]

        assert df["Interval Start"].min() == start
        # NYISO is inclusive of the end date
        assert df["Interval End"].max() == end + pd.DateOffset(days=1)

    """get_lake_erie_circulation_real_time"""

    def test_get_lake_erie_circulation_real_time_historical_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=30)
        end = start + pd.DateOffset(days=2)

        with nyiso_vcr.use_cassette(
            f"test_get_lake_erie_circulation_real_time_historical_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",  # noqa: E501
        ):
            df = self.iso.get_lake_erie_circulation_real_time(
                start=start,
                end=end,
            )
        assert df.columns.tolist() == ["Time", "MW"]
        assert df["Time"].min() == start
        # NYISO is inclusive of the end date
        assert df["Time"].max() == self.local_start_of_day(
            end.date(),
        ) + pd.DateOffset(days=1, minutes=-5)

    """get_lake_erie_circulation_day_ahead"""

    def test_get_lake_erie_circulation_day_ahead_historical_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=60)
        end = start + pd.DateOffset(days=2)

        with nyiso_vcr.use_cassette(
            f"test_get_lake_erie_circulation_day_ahead_historical_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",  # noqa: E501
        ):
            df = self.iso.get_lake_erie_circulation_day_ahead(
                start=start,
                end=end,
            )

        assert df.columns.tolist() == ["Time", "MW"]
        assert df["Time"].min() == start
        # NYISO is inclusive of the end date
        assert df["Time"].max() == self.local_start_of_day(
            end.date(),
        ) + pd.DateOffset(days=1, minutes=-60)

    @staticmethod
    def _check_status(df):
        assert set(df.columns) == set(
            ["Time", "Status", "Notes"],
        )

    def _check_as_prices(
        self,
        df: pd.DataFrame,
        rt_or_dam: Literal["rt", "dam"],
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Zone",
            "10 Min Spin Reserves",
            "10 Min Non-Spin Reserves",
            "30 Min Reserves",
            "Regulation Capacity",
        ]

        assert df.shape[0] >= 0
        assert (
            df["Interval End"] - df["Interval Start"]
            == pd.Timedelta(minutes=60 if rt_or_dam == "dam" else 5)
        ).all()

        if start is not None:
            assert df["Interval Start"].min().round("5min") >= pd.Timestamp(
                start,
                tz=self.iso.default_timezone,
            )

        if end is not None:
            assert df["Interval End"].max() <= pd.Timestamp(
                end,
                tz=self.iso.default_timezone,
            ) + pd.Timedelta(days=1)

    """get_as_prices_day_ahead_hourly"""

    def test_get_as_prices_day_ahead_hourly_latest(self):
        with nyiso_vcr.use_cassette(
            "test_get_as_prices_day_ahead_hourly_latest.yaml",
        ):
            df = self.iso.get_as_prices_day_ahead_hourly(date="latest")
            self._check_as_prices(df, rt_or_dam="dam")

    @pytest.mark.parametrize(
        "start,end",
        [
            ("2023-04-01", "2023-04-03"),
        ],
    )
    def test_get_as_prices_day_ahead_hourly_historical_range(self, start, end):
        with nyiso_vcr.use_cassette(
            f"test_get_as_prices_day_ahead_hourly_historical_range_{start}_{end}.yaml",
        ):
            df = self.iso.get_as_prices_day_ahead_hourly(start=start, end=end)
            self._check_as_prices(
                df,
                rt_or_dam="dam",
                start=start,
                end=end,
            )

    """get_as_prices_real_time_5_min"""

    def test_get_as_prices_real_time_5_min_latest(self):
        with nyiso_vcr.use_cassette(
            "test_get_as_prices_real_time_5_min_latest.yaml",
        ):
            df = self.iso.get_as_prices_real_time_5_min(date="latest")
            self._check_as_prices(df, rt_or_dam="rt")

    @pytest.mark.parametrize(
        "start,end",
        [
            ("2023-04-01", "2023-04-02"),
        ],
    )
    def test_get_as_prices_real_time_5_min_historical_range(self, start, end):
        with nyiso_vcr.use_cassette(
            f"test_get_as_prices_real_time_5_min_historical_range_{start}_{end}.yaml",
        ):
            df = self.iso.get_as_prices_real_time_5_min(start=start, end=end)
            self._check_as_prices(df, rt_or_dam="rt", start=start, end=end)

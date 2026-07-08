import pandas as pd
import polars as pl
import pytest

from gridstatus import MISO, NotSupported
from gridstatus.base import Markets, NoDataFoundException
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

miso_vcr = setup_vcr(
    source="miso",
    record_mode=RECORD_MODE,
)


class TestMISO(BaseTestISO):
    iso = MISO()

    test_dates = [
        ("2023-11-05", "2023-11-07"),
        ("2024-02-15", "2024-02-17"),
        ("2024-03-01", "2024-03-04"),
    ]

    # NOTE(kladar): Some constraint datasets are sparse, so make sure we have data for these tests
    constraint_dates = [
        ("2024-11-08", "2024-11-10"),
    ]

    """get_fuel_mix"""

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

    def _check_fuel_mix(self, df: pl.DataFrame):
        assert df.columns == [
            "Time",
            "Interval Start",
            "Interval End",
            "Battery Storage",
            "Coal",
            "Imports",
            "Natural Gas",
            "Nuclear",
            "Other",
            "Solar",
            "Wind",
        ]

        for col in [
            "Coal",
            "Imports",
            "Natural Gas",
            "Nuclear",
            "Other",
            "Solar",
            "Wind",
        ]:
            assert df.schema[col] == pl.Int64

    def test_get_fuel_mix_today(self):
        with miso_vcr.use_cassette("test_get_fuel_mix_today.yaml"):
            df = self.iso.get_fuel_mix("today")

        self._check_fuel_mix(df)

    def test_get_fuel_mix_latest(self):
        with miso_vcr.use_cassette("test_get_fuel_mix_latest.yaml"):
            df = self.iso.get_fuel_mix("latest")

        self._check_fuel_mix(df)
        assert df.height == 1

    def test_get_fuel_mix_yesterdays_date(self):
        date = self.local_start_of_today() - pd.DateOffset(days=1)
        with miso_vcr.use_cassette(
            f"test_get_fuel_mix_{date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_fuel_mix(date)

        self._check_fuel_mix(df)
        assert df.height == 288

    def test_get_interconnection_queue(self):
        with miso_vcr.use_cassette("test_get_interconnection_queue.yaml"):
            df = self.iso.get_interconnection_queue()
            assert df.columns == [
                "Queue ID",
                "Project Name",
                "Interconnecting Entity",
                "County",
                "State",
                "Interconnection Location",
                "Transmission Owner",
                "Generation Type",
                "Capacity (MW)",
                "Summer Capacity (MW)",
                "Winter Capacity (MW)",
                "Queue Date",
                "Status",
                "Proposed Completion Date",
                "Withdrawn Date",
                "Withdrawal Comment",
                "Actual Completion Date",
                "facilityType",
                "Post Generator Interconnection Agreement Status",
                "Interconnection Approval Date",
                "inService",
                "giaToExec",
                "studyCycle",
                "studyGroup",
                "studyPhase",
                "svcType",
                "dp1ErisMw",
                "dp1NrisMw",
                "dp2ErisMw",
                "dp2NrisMw",
                "sisPhase1",
            ]
            assert not df.is_empty()

    """get_lmp_real_time_5_min_final"""

    def _check_lmp_real_time_5_min_final(self, df):
        assert df.columns == [
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

        assert (df["Interval End"] - df["Interval Start"]).unique().to_list()[
            0
        ].total_seconds() == pd.Timedelta("5min").total_seconds()

        assert df["Market"].unique().to_list() == [Markets.REAL_TIME_5_MIN_FINAL.value]

    def test_get_lmp_real_time_5_min_final_today_or_latest_raises(self):
        with pytest.raises(NotSupported):
            self.iso.get_lmp_real_time_5_min_final("today")

    def test_get_lmp_real_time_5_min_final_historical_date_range(self):
        start = self.local_today() - pd.Timedelta(days=100)
        # Set start to a Wednesday to check logic
        start = start - pd.DateOffset(days=start.weekday() - 2)
        assert start.weekday() == 2

        # Make sure to span a week
        end = start + pd.Timedelta(days=7)

        cassette_name = f"test_get_lmp_real_time_5_min_final_historical_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_lmp_real_time_5_min_final(start, end)

            most_recent_monday = self.local_start_of_day(start) - pd.DateOffset(
                days=self.local_start_of_day(start).weekday(),
            )

            assert df["Interval Start"].min() == most_recent_monday
            assert df["Interval End"].max() == most_recent_monday + pd.Timedelta(
                days=14,
            )

            self._check_lmp_real_time_5_min_final(df)

    def test_get_lmp_real_time_5_min_final_raises_error_if_no_data(self):
        date = self.local_today() - pd.DateOffset(days=5)
        cassette_name = f"test_get_lmp_real_time_5_min_final_raises_error_if_no_data_{date.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            with pytest.raises(NoDataFoundException):
                self.iso.get_lmp_real_time_5_min_final(date)

    """get_lmp"""

    @with_markets(Markets.REAL_TIME_HOURLY_FINAL, Markets.REAL_TIME_HOURLY_PRELIM)
    def test_lmp_date_range(self, market):
        with miso_vcr.use_cassette(f"test_lmp_date_range_{market.value}.yaml"):
            offset_from_today = 5 if market == Markets.REAL_TIME_HOURLY_FINAL else 1
            super().test_lmp_date_range(market, offset_from_today)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_HOURLY_FINAL,
        Markets.REAL_TIME_HOURLY_PRELIM,
    )
    def test_get_lmp_historical(self, market):
        # Prelim data only goes back 4 days
        if market == Markets.REAL_TIME_HOURLY_PRELIM:
            date = self.local_today() - pd.Timedelta(days=2)
        else:
            date = self.local_today() - pd.Timedelta(days=100)

        date_str = date.strftime("%Y-%m-%d")
        cassette_name = f"test_get_lmp_historical_{market.value}_{date_str}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            super().test_get_lmp_historical(market, date_str=date_str)

    @with_markets(
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_latest(self, market):
        cassette_name = f"test_get_lmp_latest_{market.value}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            super().test_get_lmp_latest(market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_today(self, market):
        cassette_name = f"test_get_lmp_today_{market.value}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            super().test_get_lmp_today(market=market)

    def test_get_lmp_real_time_5_min_yesterday(self):
        date = self.local_today() - pd.DateOffset(days=1)
        cassette_name = (
            f"test_get_lmp_real_time_5_min_yesterday_{date.strftime('%Y-%m-%d')}.yaml"
        )
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_lmp(
                date=date,
                market=Markets.REAL_TIME_5_MIN,
            )
            assert df["Interval Start"].min() == self.local_start_of_day(date)
            assert df["Interval End"].max() == self.local_start_of_day(
                date,
            ) + pd.DateOffset(days=1)
            assert sorted(df["Location Type"].unique().to_list()) == [
                "Gennode",
                "Hub",
                "Interface",
                "Loadzone",
            ]

    def test_get_lmp_locations(self):
        cassette_name = "test_get_lmp_locations.yaml"
        with miso_vcr.use_cassette(cassette_name):
            data = self.iso.get_lmp(
                date="latest",
                market=Markets.REAL_TIME_5_MIN,
                locations=self.iso.hubs,
            )
            assert set(data["Location"].unique().to_list()) == set(self.iso.hubs)

    """get_load"""

    def test_get_load_historical(self):
        cassette_name = "test_get_load_historical.yaml"
        with miso_vcr.use_cassette(cassette_name):
            with pytest.raises(NotSupported):
                super().test_get_load_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_historical_with_date_range(self):
        pass

    """get_load_forecast"""

    load_forecast_cols = [
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

    def test_get_load_forecast_today(self):
        cassette_name = "test_get_load_forecast_today.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_load_forecast("today")

        assert df.columns == self.load_forecast_cols

        assert (df["Publish Time"] == self.local_start_of_today()).all()
        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.Timedelta(
            days=6,
        )

    def test_get_load_forecast_latest(self):
        cassette_name = "test_get_load_forecast_latest.yaml"
        with miso_vcr.use_cassette(cassette_name):
            assert self.iso.get_load_forecast("latest").equals(
                self.iso.get_load_forecast("today"),
            )

    def test_get_load_forecast_historical(self):
        past_date = self.local_today() - pd.Timedelta(days=30)
        cassette_name = (
            f"test_get_load_forecast_historical_{past_date.strftime('%Y-%m-%d')}.yaml"
        )
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_load_forecast(past_date)
            assert df.columns == self.load_forecast_cols

            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(days=6)

            assert df["Publish Time"].dt.date().unique().to_list() == [
                pd.to_datetime(past_date).date(),
            ]

    def test_get_load_forecast_historical_with_date_range(self):
        past_date = self.local_today() - pd.Timedelta(days=250)
        end_date = past_date + pd.Timedelta(days=3)
        cassette_name = f"test_get_load_forecast_historical_with_date_range_{past_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_load_forecast(
                start=past_date,
                end=end_date,
            )

            assert df.columns == self.load_forecast_cols
            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                end_date,
            ) + pd.Timedelta(days=5)

    # MISO changed the _df_al.xls layout starting with the file published on
    # 2026-04-27 (extra blank rows/columns introduced by merged-cell formatting).
    # 2026-04-25 covers the legacy layout, 2026-04-27 covers the new layout.
    @pytest.mark.parametrize("past_date", ["2026-04-25", "2026-04-27"])
    def test_get_load_forecast_2026_04_27_layout_change(self, past_date):
        past_date = pd.Timestamp(past_date)
        cassette_name = f"test_get_load_forecast_2026_04_27_layout_change_{past_date.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_load_forecast(past_date)

            assert df.columns == self.load_forecast_cols
            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(days=6)
            assert df["Publish Time"].dt.date().unique().to_list() == [
                pd.to_datetime(past_date).date(),
            ]

    # 2026-04-25 → 2026-04-27 spans the layout change: op day 2026-04-25 reads
    # the legacy-format file and op day 2026-04-26 reads the new-format file in
    # a single call.
    def test_get_zonal_load_hourly_2026_04_27_layout_change(self):
        cassette_name = "test_get_zonal_load_hourly_2026_04_27_layout_change.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_zonal_load_hourly(
                start="2026-04-25",
                end="2026-04-27",
            )

            self._check_zonal_load_hourly(df)
            assert df["Interval Start"].min() == self.local_start_of_day("2026-04-25")
            assert df["Interval End"].max() == self.local_start_of_day("2026-04-27")

    def test_get_load_forecast_dst_spring_forward(self):
        dst_start = pd.Timestamp("2025-03-09")
        cassette_name = f"test_get_load_forecast_dst_spring_forward_{dst_start.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_load_forecast(dst_start)

            assert df.columns == self.load_forecast_cols
            assert df["Interval Start"].min() == self.local_start_of_day(dst_start)

    def test_get_load_forecast_dst_fall_back(self):
        dst_end = pd.Timestamp("2025-11-02")
        cassette_name = (
            f"test_get_load_forecast_dst_fall_back_{dst_end.strftime('%Y-%m-%d')}.yaml"
        )
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_load_forecast(dst_end)

            assert df.columns == self.load_forecast_cols
            assert df["Interval Start"].min() == self.local_start_of_day(dst_end)

    solar_and_wind_forecast_cols = [
        "Interval Start",
        "Interval End",
        "Publish Time",
        "North",
        "Central",
        "South",
        "MISO",
    ]

    """get_solar_forecast"""

    def _check_solar_and_wind_forecast(self, df):
        assert df.columns == self.solar_and_wind_forecast_cols
        assert (df["Interval End"] - df["Interval Start"]).unique().to_list()[
            0
        ].total_seconds() == pd.Timedelta("1h").total_seconds()

    def test_get_solar_forecast_historical(self):
        past_date = self.local_today() - pd.Timedelta(days=30)
        cassette_name = (
            f"test_get_solar_forecast_historical_{past_date.strftime('%Y-%m-%d')}.yaml"
        )
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_solar_forecast(past_date)

            self._check_solar_and_wind_forecast(df)

            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(days=7)

            assert df["Publish Time"].dt.date().unique().to_list() == [
                pd.to_datetime(past_date).date(),
            ]

    def test_get_solar_forecast_historical_date_range(self):
        past_date = self.local_today() - pd.Timedelta(days=100)
        end_date = past_date + pd.Timedelta(days=3)
        cassette_name = f"test_get_solar_forecast_historical_date_range_{past_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_solar_forecast(
                start=past_date,
                end=end_date,
            )

            self._check_solar_and_wind_forecast(df)

            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                end_date,
            ) + pd.Timedelta(days=6)

            assert df["Publish Time"].dt.date().unique().to_list() == [
                past_date,
                past_date + pd.Timedelta(days=1),
                past_date + pd.Timedelta(days=2),
            ]

    def test_get_solar_forecast_historical_before_schema_change(self):
        # Data schema changed on 2022-06-13
        date = pd.Timestamp("2022-05-12").date()
        cassette_name = f"test_get_solar_forecast_historical_before_schema_change_{date.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_solar_forecast(date)
            self._check_solar_and_wind_forecast(df)

    """get_wind_forecast"""

    def test_get_wind_forecast_historical(self):
        past_date = self.local_today() - pd.Timedelta(days=30)
        cassette_name = (
            f"test_get_wind_forecast_historical_{past_date.strftime('%Y-%m-%d')}.yaml"
        )
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_wind_forecast(past_date)

            self._check_solar_and_wind_forecast(df)

            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(days=7)
            assert df["Publish Time"].dt.date().unique().to_list() == [
                pd.to_datetime(past_date).date(),
            ]

    def test_get_wind_forecast_historical_date_range(self):
        past_date = self.local_today() - pd.Timedelta(days=100)
        end_date = past_date + pd.Timedelta(days=3)
        cassette_name = f"test_get_wind_forecast_historical_date_range_{past_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_wind_forecast(
                start=past_date,
                end=end_date,
            )

            self._check_solar_and_wind_forecast(df)

            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                end_date,
            ) + pd.Timedelta(days=6)

            assert df["Publish Time"].dt.date().unique().to_list() == [
                past_date,
                past_date + pd.Timedelta(days=1),
                past_date + pd.Timedelta(days=2),
            ]

    def test_get_wind_forecast_historical_before_schema_change(self):
        # Data schema changed on 2022-06-13
        # No south data for 2022-05-12 for wind
        date = pd.Timestamp("2022-05-12").date()
        cassette_name = f"test_get_wind_forecast_historical_before_schema_change_{date.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_wind_forecast(date)
            self._check_solar_and_wind_forecast(df)
            assert df["South"].is_null().all()

    """get_status"""

    def test_get_status_latest(self):
        cassette_name = "test_get_status_latest.yaml"
        with miso_vcr.use_cassette(cassette_name):
            with pytest.raises(NotImplementedError):
                super().test_get_status_latest()

    """get_storage"""

    def test_get_storage_historical(self):
        cassette_name = "test_get_storage_historical.yaml"
        with miso_vcr.use_cassette(cassette_name):
            with pytest.raises(NotImplementedError):
                super().test_get_storage_historical()

    def test_get_storage_today(self):
        cassette_name = "test_get_storage_today.yaml"
        with miso_vcr.use_cassette(cassette_name):
            with pytest.raises(NotImplementedError):
                super().test_get_storage_today()

    """get_generation_outages_forecast"""

    def _check_generation_outages(self, df):
        assert df.columns == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Region",
            "Derated Outages MW",
            "Forced Outages MW",
            "Planned Outages MW",
            "Unplanned Outages MW",
        ]

        assert (df["Interval End"] - df["Interval Start"]).unique().to_list()[
            0
        ].total_seconds() == pd.Timedelta("1d").total_seconds()

        assert sorted(df["Region"].unique().to_list()) == sorted(
            ["Central", "MISO", "North", "South"],
        )

    def test_get_generation_outages_forecast_latest(self):
        cassette_name = "test_get_generation_outages_forecast_latest.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_generation_outages_forecast("latest")

            self._check_generation_outages(df)

            # Latest fetches the file published yesterday with the first forecast day today
            expected_start_date = self.local_start_of_today()

            assert df["Publish Time"].unique().to_list() == [
                expected_start_date - pd.DateOffset(days=1),
            ]
            assert df["Interval Start"].min() == expected_start_date
            assert df["Interval End"].max() == expected_start_date + pd.DateOffset(
                days=7,
            )

    def test_get_generation_outages_forecast_historical_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=100)
        end = start + pd.DateOffset(days=3)
        cassette_name = f"test_get_generation_outages_forecast_historical_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_generation_outages_forecast(start, end)
            self._check_generation_outages(df)

            assert df["Interval Start"].min() == start + pd.DateOffset(days=1)
            assert df["Interval End"].max() == end + pd.DateOffset(days=7)
            assert df["Publish Time"].min() == start
            assert df["Publish Time"].n_unique() == 3

    """get_generation_outages_estimated"""

    def test_get_generation_outages_estimated_latest(self):
        cassette_name = "test_get_generation_outages_estimated_latest.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_generation_outages_estimated("latest")
            self._check_generation_outages(df)

            # Latest fetches the file published yesterday
            expected_start_date = self.local_start_of_today() - pd.DateOffset(days=30)

            assert df["Publish Time"].unique().to_list() == [
                self.local_start_of_today() - pd.DateOffset(days=1),
            ]

            assert df["Interval Start"].min() == expected_start_date
            assert df["Interval End"].max() == self.local_start_of_today()

    def test_get_generation_outages_estimated_historical_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=100)
        end = start + pd.DateOffset(days=3)
        cassette_name = f"test_get_generation_outages_estimated_historical_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_generation_outages_estimated(start, end)
            self._check_generation_outages(df)
            assert df["Interval Start"].min() == start - pd.DateOffset(days=29)
            assert df["Interval End"].max() == end
            assert df["Publish Time"].min() == start
            assert df["Publish Time"].n_unique() == 3

    @pytest.mark.parametrize(
        "date,end",
        constraint_dates,
    )
    def test_get_binding_constraints_supplemental(self, date, end):
        cassette_name = f"test_get_binding_constraints_supplemental_{date}_{end}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_supplemental(
                date=date,
                end=end,
            )

            assert isinstance(df, pl.DataFrame)
            assert list(df.columns) == [
                "Date",
                "Constraint ID",
                "Constraint Name",
                "Contingency Name",
                "Constraint Type",
                "Flowgate Name",
                "Device Type",
                "Key1",
                "Key2",
                "Key3",
                "Direction",
                "From Area",
                "To Area",
                "From Station",
                "To Station",
                "From KV",
                "To KV",
            ]

            assert pd.Timestamp(df["Date"].min()).date() == pd.to_datetime(date).date()
            assert pd.Timestamp(df["Date"].max()).date() <= pd.Timestamp(end).date()
            assert df.schema["Constraint ID"] == pl.Int64
            assert df.schema["Constraint Name"] in [pl.Utf8, pl.Null]
            assert df.schema["Contingency Name"] in [pl.Utf8, pl.Null]
            assert df.schema["Constraint Type"] in [pl.Utf8, pl.Null]
            assert df.schema["Flowgate Name"] in [pl.Utf8, pl.Null]
            assert df.schema["Device Type"] in [pl.Utf8, pl.Null]
            assert df.schema["Key1"] in [pl.Utf8, pl.Null]
            assert df.schema["Key2"] in [pl.Utf8, pl.Null]
            assert df.schema["Key3"] in [pl.Utf8, pl.Null]
            assert df.schema["Direction"] == pl.Int64
            assert df.schema["From Area"] in [pl.Utf8, pl.Null]
            assert df.schema["To Area"] in [pl.Utf8, pl.Null]
            assert df.schema["From Station"] in [pl.Utf8, pl.Null]
            assert df.schema["To Station"] in [pl.Utf8, pl.Null]
            assert df.schema["From KV"] in [pl.Int64, pl.Float64]
            assert df.schema["To KV"] in [pl.Int64, pl.Float64]

    @pytest.mark.parametrize(
        "date,end",
        constraint_dates,
    )
    def test_get_binding_constraints_day_ahead_hourly(self, date, end):
        cassette_name = (
            f"test_get_binding_constraints_day_ahead_hourly_{date}_{end}.yaml"
        )
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_day_ahead_hourly(
                date=date,
                end=end,
            )

            assert isinstance(df, pl.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "Flowgate NERC ID",
                "Constraint ID",
                "Constraint Name",
                "Branch Name",
                "Contingency Description",
                "Shadow Price",
                "Constraint Description",
                "Override",
                "Curve Type",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
                "Reason",
            ]

            assert df["Interval Start"].min().date() == pd.Timestamp(date).date()
            assert (
                pd.Timestamp(df["Interval End"].max()).date()
                <= pd.Timestamp(end).date()
            )
            assert df.schema["Constraint ID"] == pl.Int64
            assert df.schema["Constraint Name"] in [pl.Utf8, pl.Null]
            assert df.schema["Branch Name"] in [pl.Utf8, pl.Null]
            assert df.schema["Contingency Description"] in [pl.Utf8, pl.Null]
            assert df.schema["Shadow Price"] in [pl.Float64, pl.Int64]
            assert df.schema["Constraint Description"] in [pl.Utf8, pl.Null]
            assert df.schema["Override"] == pl.Int64
            assert df.schema["Curve Type"] in [pl.Utf8, pl.Null]
            assert df.schema["BP1"] in [pl.Float64, pl.Int64]
            assert df.schema["PC1"] in [pl.Float64, pl.Int64]
            assert df.schema["BP2"] in [pl.Float64, pl.Int64]
            assert df.schema["PC2"] in [pl.Float64, pl.Int64]
            assert df.schema["Reason"] in [pl.Utf8, pl.Null]

    @pytest.mark.parametrize(
        "date,end",
        constraint_dates,
    )
    def test_get_subregional_power_balance_constraints_day_ahead_hourly(
        self,
        date,
        end,
    ):
        cassette_name = f"test_get_subregional_power_balance_constraints_day_ahead_hourly_{date}_{end}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_subregional_power_balance_constraints_day_ahead_hourly(
                date=date,
                end=end,
            )

            assert isinstance(df, pl.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "CONSTRAINT_NAME",
                "PRELIMINARY_SHADOW_PRICE",
                "CURVETYPE",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
                "BP3",
                "PC3",
                "BP4",
                "PC4",
                "OVERRIDE",
                "REASON",
            ]

            if not df.is_empty():
                assert df["Interval Start"].min().date() <= pd.Timestamp(date).date()
                assert (
                    pd.Timestamp(df["Interval End"].max()).date()
                    <= pd.Timestamp(end).date()
                )
            else:
                pytest.skip(
                    "No data available for this date range, so skipping data-comparison assertions",
                )

    @pytest.mark.parametrize(
        "date,end",
        constraint_dates,
    )
    def test_get_reserve_product_binding_constraints_day_ahead_hourly(
        self,
        date,
        end,
    ):
        cassette_name = f"test_get_reserve_product_binding_constraints_day_ahead_hourly_{date}_{end}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_reserve_product_binding_constraints_day_ahead_hourly(
                date=date,
                end=end,
            )

            assert isinstance(df, pl.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Shadow Price",
                "Constraint Description",
            ]

            if not df.is_empty():
                assert df["Interval Start"].min().date() == pd.to_datetime(date).date()
                assert (
                    pd.Timestamp(df["Interval End"].max()).date()
                    <= pd.Timestamp(end).date()
                )
            else:
                pytest.skip(
                    "No data available for this date range, so skipping data-comparison assertions",
                )

    @pytest.mark.parametrize(
        "date,end",
        constraint_dates,
    )
    def test_get_binding_constraints_real_time_5_min(self, date, end):
        cassette_name = (
            f"test_get_binding_constraints_real_time_5_min_{date}_{end}.yaml"
        )
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_real_time_5_min(
                date=date,
                end=end,
            )

            assert isinstance(df, pl.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "Flowgate NERC ID",
                "Constraint ID",
                "Constraint Name",
                "Branch Name",
                "Contingency Description",
                "Preliminary Shadow Price",
                "Constraint Description",
                "Override",
                "Curve Type",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
            ]

            if not df.is_empty():
                assert df["Interval Start"].min().date() <= pd.Timestamp(date).date()
                assert (
                    pd.Timestamp(df["Interval End"].max()).date()
                    <= pd.Timestamp(end).date()
                )
            else:
                pytest.skip(
                    "No data available for this date range, so skipping data-comparison assertions",
                )

    def test_get_binding_constraints_real_time_yearly_historical(self):
        year = 2023
        cassette_name = (
            f"test_get_binding_constraints_real_time_yearly_historical_{year}.yaml"
        )
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_real_time_yearly_historical(
                year=year,
            )

            assert isinstance(df, pl.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "Flowgate NERC ID",
                "Constraint ID",
                "Constraint Name",
                "Branch Name",
                "Contingency Description",
                "Preliminary Shadow Price",
                "Constraint Description",
                "Override",
                "Curve Type",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
            ]

            if not df.is_empty():
                assert pd.Timestamp(df["Interval End"].min()).year == year
                assert pd.Timestamp(df["Interval End"].max()).year == year
            else:
                pytest.skip(
                    "No data available for this date range, so skipping data-comparison assertions",
                )

    @pytest.mark.parametrize(
        "date,end",
        constraint_dates,
    )
    def test_get_binding_constraint_overrides_real_time_5_min(self, date, end):
        cassette_name = (
            f"test_get_binding_constraint_overrides_real_time_5_min_{date}_{end}.yaml"
        )
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraint_overrides_real_time_5_min(
                date=date,
                end=end,
            )

            assert isinstance(df, pl.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "Flowgate NERC ID",
                "Constraint Name",
                "Branch Name",
                "Contingency Description",
                "Preliminary Shadow Price",
                "Constraint Description",
                "Override",
                "Curve Type",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
                "Reason",
            ]

            if not df.is_empty():
                assert df["Interval Start"].min().date() == pd.to_datetime(date).date()
                assert (
                    pd.Timestamp(df["Interval End"].max()).date()
                    <= pd.Timestamp(end).date()
                )
            else:
                pytest.skip(
                    "No data available for this date range, so skipping data-comparison assertions",
                )

    @pytest.mark.parametrize(
        "date,end",
        constraint_dates,
    )
    def test_get_subregional_power_balance_constraints_real_time_5_min(
        self,
        date,
        end,
    ):
        cassette_name = f"test_get_subregional_power_balance_constraints_real_time_5_min_{date}_{end}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_subregional_power_balance_constraints_real_time_5_min(
                date=date,
                end=end,
            )

            assert isinstance(df, pl.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "CONSTRAINT_NAME",
                "PRELIMINARY_SHADOW_PRICE",
                "CURVETYPE",
                "BP1",
                "PC1",
                "BP2",
                "PC2",
                "BP3",
                "PC3",
                "BP4",
                "PC4",
                "OVERRIDE",
                "REASON",
            ]

            if not df.is_empty():
                assert df["Interval Start"].min().date() == pd.to_datetime(date).date()
                assert (
                    pd.Timestamp(df["Interval End"].max()).date()
                    <= pd.Timestamp(end).date()
                )
            else:
                pytest.skip(
                    "No data available for this date range, so skipping data-comparison assertions",
                )

    @pytest.mark.parametrize(
        "date,end",
        constraint_dates,
    )
    def test_get_reserve_product_binding_constraints_real_time_5_min(
        self,
        date,
        end,
    ):
        cassette_name = f"test_get_reserve_product_binding_constraints_real_time_5_min_{date}_{end}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_reserve_product_binding_constraints_real_time_5_min(
                date=date,
                end=end,
            )

            assert isinstance(df, pl.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Shadow Price",
                "Constraint Description",
            ]

            if not df.is_empty():
                assert df["Interval Start"].min().date() == pd.to_datetime(date).date()
                assert (
                    pd.Timestamp(df["Interval End"].max()).date()
                    <= pd.Timestamp(end).date()
                )
            else:
                pytest.skip(
                    "No data available for this date range, so skipping data-comparison assertions",
                )

    """get_look_ahead_hourly"""

    @pytest.mark.parametrize(
        "date,end",
        test_dates,
    )
    def test_get_look_ahead_hourly(self, date, end):
        with miso_vcr.use_cassette(f"test_get_look_ahead_hourly_{date}_{end}.yaml"):
            df = self.iso.get_look_ahead_hourly(
                date=date,
                end=end,
            )

            assert isinstance(df, pl.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Region",
                "MTLF",
                "Outage",
            ]

            # Each day has 24 hours, 7 days of forecast, and 4 regions
            days = (pd.Timestamp(end) - pd.Timestamp(date)).days
            expected_rows = 24 * 7 * 4 * days
            assert df.height == expected_rows
            expected_regions = ["Central", "MISO", "North", "South"]
            assert sorted(df["Region"].unique().to_list()) == sorted(expected_regions)

            assert df["Interval Start"].dtype.time_zone == self.iso.default_timezone
            assert df["Interval End"].dtype.time_zone == self.iso.default_timezone
            assert df["Publish Time"].dtype.time_zone == self.iso.default_timezone
            assert (df["Interval End"] - df["Interval Start"]).unique().to_list()[
                0
            ].total_seconds() == pd.Timedelta(hours=1).total_seconds()
            assert df["Interval Start"].min().date() == pd.Timestamp(date).date()

    """get_zonal_load_hourly"""

    def _check_zonal_load_hourly(self, df):
        assert df.columns == [
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

        assert (df["Interval End"] - df["Interval Start"]).unique().to_list()[
            0
        ].total_seconds() == pd.Timedelta("1h").total_seconds()
        assert df.schema["LRZ1"] == pl.Float64
        assert df.schema["LRZ2 7"] == pl.Float64
        assert df.schema["LRZ3 5"] == pl.Float64
        assert df.schema["LRZ4"] == pl.Float64
        assert df.schema["LRZ6"] == pl.Float64
        assert df.schema["LRZ8 9 10"] == pl.Float64
        assert df.schema["MISO"] == pl.Float64

    def test_get_zonal_load_hourly_latest(self):
        cassette_name = "test_get_zonal_load_hourly_latest.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_zonal_load_hourly("latest")
            self._check_zonal_load_hourly(df)

            expected_start_date = self.local_start_of_today() - pd.DateOffset(days=1)
            assert df["Interval Start"].min() == expected_start_date
            assert df["Interval End"].max() == expected_start_date + pd.DateOffset(
                days=1,
            )

    @pytest.mark.parametrize(
        "date,end",
        [
            ("2022-01-01", "2022-01-03"),
            *test_dates,
        ],
    )
    def test_get_zonal_load_hourly_historical_date_range(self, date, end):
        cassette_name = f"test_get_zonal_load_hourly_historical_date_range_{pd.Timestamp(date).strftime('%Y-%m-%d')}_{pd.Timestamp(end).strftime('%Y-%m-%d')}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_zonal_load_hourly(
                start=date,
                end=end,
            )

            self._check_zonal_load_hourly(df)

            assert df["Interval Start"].min() == self.local_start_of_day(date)
            assert df["Interval End"].max() == self.local_start_of_day(end)

    """get_interchange_5_min"""

    def _check_get_interchange_5_min(self, df):
        assert df.columns == [
            "Interval Start",
            "Interval End",
            "Net Scheduled Interchange",
            "Net Actual Interchange",
            "AECI",
            "LGEE",
            "MHEB",
            "ONT",
            "PJM",
            "SOCO",
            "SPA",
            "SWPP",
            "TVA",
        ]

        assert (df["Interval End"] - df["Interval Start"]).unique().to_list()[
            0
        ].total_seconds() == pd.Timedelta("5min").total_seconds()

        for col in df.columns:
            if col not in ["Interval Start", "Interval End"]:
                assert df.schema[col] == pl.Int64

        # The first values in Net Actual Interchange should be null because the actual
        # data does not go back as far as the scheduled data
        assert df["Net Actual Interchange"].is_null()[0]

        # The last value in Net Scheduled Interchange should be null because the
        # scheduled data is one interval behind the actual data
        assert df["Net Scheduled Interchange"].is_null()[-1]

    def test_get_interchange_5_min_latest(self):
        with miso_vcr.use_cassette("test_get_interchange_5_min_latest.yaml"):
            df = self.iso.get_interchange_5_min("latest")
            self._check_get_interchange_5_min(df)

            assert pd.Timestamp(
                df["Interval Start"].min(),
            ) <= self.local_now() - pd.DateOffset(
                days=1,
            )

            # Data should be near-real-time
            assert pd.Timestamp(
                df["Interval End"].max(),
            ) >= self.local_now() - pd.DateOffset(
                minutes=5,
            )

    @pytest.mark.parametrize("date", ["2025-01-01", "today"])
    def test_get_interchange_5_min_raises_error_if_not_latest(self, date):
        cassette_name = (
            f"test_get_interchange_5_min_raises_error_if_not_latest_{date}.yaml"
        )
        with miso_vcr.use_cassette(cassette_name):
            with pytest.raises(NotSupported):
                self.iso.get_interchange_5_min(date)

    """get_binding_constraints_real_time_intraday"""

    def _check_binding_constraints_real_time_intraday(self, df):
        assert df.columns == [
            "Interval Start",
            "Interval End",
            "Constraint Name",
            "Shadow Price",
            "Override",
            "Curve Type",
            "BP1",
            "PC1",
            "BP2",
            "PC2",
        ]

        assert (df["Interval End"] - df["Interval Start"]).unique().to_list()[
            0
        ].total_seconds() == pd.Timedelta("5min").total_seconds()

        assert df.schema["Constraint Name"] in [pl.Utf8, pl.Null]
        assert df.schema["Shadow Price"] == pl.Float64
        assert df.schema["Override"] == pl.Int64
        assert df.schema["BP1"] == pl.Int64
        assert df.schema["PC1"] == pl.Int64
        assert df.schema["BP2"] == pl.Int64
        assert df.schema["PC2"] == pl.Int64

        assert (df["Constraint Name"] != "None").all()

    def test_get_binding_constraints_real_time_intraday_latest(self):
        with miso_vcr.use_cassette(
            "test_get_binding_constraints_real_time_intraday_latest.yaml",
        ):
            df = self.iso.get_binding_constraints_real_time_intraday("latest")

        self._check_binding_constraints_real_time_intraday(df)

    @pytest.mark.parametrize("date", ["2025-01-01", "today"])
    def test_get_binding_constraints_real_time_intraday_raises_error_if_not_latest(
        self,
        date,
    ):
        cassette_name = f"test_get_binding_constraints_real_time_intraday_raises_error_if_not_latest_{date}.yaml"
        with miso_vcr.use_cassette(cassette_name):
            with pytest.raises(NotSupported):
                self.iso.get_binding_constraints_real_time_intraday(date)

    """get_multiday_operating_margin"""

    @pytest.mark.parametrize(
        "days_ago",
        [730, 2],  # ~2 years ago and ~2 days ago
    )
    def test_get_multiday_operating_margin(self, days_ago):
        date = pd.Timestamp.now(tz="EST").normalize() - pd.Timedelta(days=days_ago)
        cassette_name = f"test_get_multiday_operating_margin_{days_ago}d.yaml"

        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_multiday_operating_margin(date=date)

        assert df.height > 0

        # Check data types
        assert df.schema["Publish Date"] == pl.Date
        assert isinstance(df.schema["Peak Hour"], pl.Datetime)
        assert df.schema["Region"] == pl.Utf8
        assert df.schema["Resource Committed"] == pl.Float64
        assert df.schema["Committed Additional Emergency Headroom"] == pl.Float64
        assert df.schema["Resource Uncommitted"] == pl.Float64
        assert df.schema["Uncommitted Greater than 16 Hours"] == pl.Float64
        assert df.schema["Uncommitted 12 to 16 Hours"] == pl.Float64
        assert df.schema["Uncommitted 8 to 12 Hours"] == pl.Float64
        assert df.schema["Uncommitted 4 to 8 Hours"] == pl.Float64
        assert df.schema["Uncommitted Less than 4 Hours"] == pl.Float64
        assert df.schema["Uncommitted Additional Emergency Headroom"] == pl.Float64
        assert df.schema["Emergency Resources Additional Headroom"] == pl.Float64
        assert df.schema["Renewable Forecast"] == pl.Float64
        assert df.schema["Wind Forecast"] == pl.Float64
        assert df.schema["Solar Forecast"] == pl.Float64
        assert df.schema["MISO Resources Available"] == pl.Float64
        assert df.schema["NSI"] == pl.Float64
        assert df.schema["Total Resources Available"] == pl.Float64
        assert df.schema["Projected Load"] == pl.Float64
        assert df.schema["Operating Reserve Requirement"] == pl.Float64
        assert df.schema["Obligation"] == pl.Float64
        assert df.schema["Resource Operating Margin"] == pl.Float64

        # Check region values
        assert (df["Region"] == "MISO").all()

        # Check Publish Date matches input date
        assert (df["Publish Date"] == date.date()).all()

        # Check Peak Hour is on or after the publish date
        assert (df["Peak Hour"] >= date).all()

        # Check data is sorted
        assert df["Peak Hour"].is_sorted()

        # Check reasonable value ranges
        assert (df["Projected Load"] > 0).all()
        assert (df["MISO Resources Available"] > 0).all()

    """get_multiday_operating_margin_regional"""

    @pytest.mark.parametrize(
        "days_ago",
        [730, 2],  # ~2 years ago and ~2 days ago
    )
    def test_get_multiday_operating_margin_regional(self, days_ago):
        date = pd.Timestamp.now(tz="EST").normalize() - pd.Timedelta(days=days_ago)
        cassette_name = f"test_get_multiday_operating_margin_regional_{days_ago}d.yaml"

        with miso_vcr.use_cassette(cassette_name):
            df = self.iso.get_multiday_operating_margin_regional(date=date)

        assert df.height > 0

        # Check data types
        assert df.schema["Publish Date"] == pl.Date
        assert isinstance(df.schema["Peak Hour"], pl.Datetime)
        assert df.schema["Region"] == pl.Utf8
        assert df.schema["Resource Committed"] == pl.Float64
        assert df.schema["Committed Additional Emergency Headroom"] == pl.Float64
        assert df.schema["Resource Uncommitted"] == pl.Float64
        assert df.schema["Uncommitted Greater than 16 Hours"] == pl.Float64
        assert df.schema["Uncommitted 12 to 16 Hours"] == pl.Float64
        assert df.schema["Uncommitted 8 to 12 Hours"] == pl.Float64
        assert df.schema["Uncommitted 4 to 8 Hours"] == pl.Float64
        assert df.schema["Uncommitted Less than 4 Hours"] == pl.Float64
        assert df.schema["Uncommitted Additional Emergency Headroom"] == pl.Float64
        assert df.schema["Emergency Resources Additional Headroom"] == pl.Float64
        assert df.schema["Renewable Forecast"] == pl.Float64
        assert df.schema["Wind Forecast"] == pl.Float64
        assert df.schema["Solar Forecast"] == pl.Float64
        assert df.schema["MISO Resources Available"] == pl.Float64
        assert df.schema["NSI"] == pl.Float64
        assert df.schema["Total Resources Available"] == pl.Float64
        assert df.schema["Projected Load"] == pl.Float64
        assert df.schema["Region Resources Above Load"] == pl.Float64
        assert df.schema["Max Possible RDT"] == pl.Float64

        # Check all 4 regions are present
        expected_regions = {"North", "Central", "North and Central", "South"}
        assert set(df["Region"].unique().to_list()) == expected_regions

        # Check Publish Date matches input date
        assert (df["Publish Date"] == date.date()).all()

        # Check Peak Hour is on or after the publish date
        assert (df["Peak Hour"] >= date).all()

        # Check each region has data
        for region in expected_regions:
            region_df = df.filter(pl.col("Region") == region)
            assert region_df.height > 0

        # Check reasonable value ranges
        assert (df["Projected Load"] > 0).all()
        assert (df["MISO Resources Available"] > 0).all()

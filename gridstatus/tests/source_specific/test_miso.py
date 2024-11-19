import numpy as np
import pandas as pd
import pytest

from gridstatus import MISO, NotSupported
from gridstatus.base import Markets, NoDataFoundException
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
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

    def test_get_fuel_mix_today(self):
        with pytest.raises(NotSupported):
            super().test_get_fuel_mix_today()

    """get_lmp_weekly"""

    def _check_lmp_weekly(self, df):
        assert df.columns.tolist() == [
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

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            "5min",
        )

        assert df["Market"].unique().tolist() == [Markets.REAL_TIME_5_MIN_WEEKLY.value]

    def test_get_lmp_weekly_today_or_latest_raises(self):
        with pytest.raises(NotSupported):
            self.iso.get_lmp_weekly("today")

    def test_get_lmp_weekly_historical_date(self):
        date = self.local_today() - pd.Timedelta(days=300)
        df = self.iso.get_lmp_weekly(date)

        most_recent_monday = self.local_start_of_day(date) - pd.DateOffset(
            days=self.local_start_of_day(date).weekday(),
        )

        assert df["Interval Start"].min() == most_recent_monday
        assert df["Interval End"].max() == most_recent_monday + pd.Timedelta(days=7)

        self._check_lmp_weekly(df)

    def test_get_lmp_weekly_historical_date_range(self):
        start = self.local_today() - pd.Timedelta(days=100)
        # Make sure to span a week
        end = start + pd.Timedelta(days=12)
        df = self.iso.get_lmp_weekly(start, end)

        most_recent_monday = self.local_start_of_day(start) - pd.DateOffset(
            days=self.local_start_of_day(start).weekday(),
        )

        assert df["Interval Start"].min() == most_recent_monday
        assert df["Interval End"].max() == most_recent_monday + pd.Timedelta(days=14)

        self._check_lmp_weekly(df)

    def test_get_lmp_weekly_raises_error_if_no_data(self):
        date = self.local_today() - pd.DateOffset(days=5)

        with pytest.raises(NoDataFoundException):
            self.iso.get_lmp_weekly(date)

    """get_lmp"""

    @with_markets(Markets.REAL_TIME_HOURLY_FINAL, Markets.REAL_TIME_HOURLY_PRELIM)
    def test_lmp_date_range(self, market):
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

        super().test_get_lmp_historical(market, date_str=date_str)

    @with_markets(
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_latest(self, market):
        super().test_get_lmp_latest(market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_today(self, market):
        super().test_get_lmp_today(market=market)

    def test_get_lmp_real_time_5_min_yesterday(self):
        date = self.local_today() - pd.DateOffset(days=1)

        df = self.iso.get_lmp(
            date=date,
            market=Markets.REAL_TIME_5_MIN,
        )

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(days=1)

        assert sorted(list(df["Location Type"].unique())) == [
            "Gennode",
            "Hub",
            "Interface",
            "Loadzone",
        ]

    def test_get_lmp_locations(self):
        data = self.iso.get_lmp(
            date="latest",
            market=Markets.REAL_TIME_5_MIN,
            locations=self.iso.hubs,
        )
        assert set(data["Location"].unique()) == set(self.iso.hubs)

    """get_load"""

    def test_get_load_historical(self):
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
        df = self.iso.get_load_forecast("today")

        assert df.columns.tolist() == self.load_forecast_cols

        assert (df["Publish Time"] == self.local_start_of_today()).all()
        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.Timedelta(
            days=6,
        )

    def test_get_load_forecast_latest(self):
        assert self.iso.get_load_forecast("latest").equals(
            self.iso.get_load_forecast("today"),
        )

    def test_get_load_forecast_historical(self):
        past_date = self.local_today() - pd.Timedelta(days=30)
        df = self.iso.get_load_forecast(past_date)

        assert df.columns.tolist() == self.load_forecast_cols

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=6)

        assert df["Publish Time"].dt.date.unique() == pd.to_datetime(past_date).date()

    def test_get_load_forecast_historical_with_date_range(self):
        past_date = self.local_today() - pd.Timedelta(days=250)
        end_date = past_date + pd.Timedelta(days=3)

        df = self.iso.get_load_forecast(
            start=past_date,
            end=end_date,
        )

        assert df.columns.tolist() == self.load_forecast_cols
        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
        ) + pd.Timedelta(days=5)

    def test_get_load_forecast_dst_start_and_end(self):
        dst_start = pd.Timestamp("2022-03-13")
        df = self.iso.get_load_forecast(dst_start)

        assert df.columns.tolist() == self.load_forecast_cols
        assert df["Interval Start"].min() == self.local_start_of_day(dst_start)

        dst_end = pd.Timestamp("2022-11-06")
        df = self.iso.get_load_forecast(dst_end)

        assert df.columns.tolist() == self.load_forecast_cols
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
        assert df.columns.tolist() == self.solar_and_wind_forecast_cols
        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            "1h",
        )

    def test_get_solar_forecast_historical(self):
        past_date = self.local_today() - pd.Timedelta(days=30)
        df = self.iso.get_solar_forecast(past_date)

        self._check_solar_and_wind_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=7)

        assert df["Publish Time"].dt.date.unique() == pd.to_datetime(past_date).date()

    def test_get_solar_forecast_historical_date_range(self):
        past_date = self.local_today() - pd.Timedelta(days=100)
        end_date = past_date + pd.Timedelta(days=3)

        df = self.iso.get_solar_forecast(
            start=past_date,
            end=end_date,
        )

        self._check_solar_and_wind_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
        ) + pd.Timedelta(days=6)

        assert df["Publish Time"].dt.date.unique().tolist() == [
            past_date,
            past_date + pd.Timedelta(days=1),
            past_date + pd.Timedelta(days=2),
        ]

    def test_get_solar_forecast_historical_before_schema_change(self):
        # Data schema changed on 2022-06-13
        date = pd.Timestamp("2022-05-12").date()

        df = self.iso.get_solar_forecast(date)

        self._check_solar_and_wind_forecast(df)

    """get_wind_forecast"""

    def test_get_wind_forecast_historical(self):
        past_date = self.local_today() - pd.Timedelta(days=30)
        df = self.iso.get_wind_forecast(past_date)

        self._check_solar_and_wind_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=7)

        assert df["Publish Time"].dt.date.unique() == pd.to_datetime(past_date).date()

    def test_get_wind_forecast_historical_date_range(self):
        past_date = self.local_today() - pd.Timedelta(days=100)
        end_date = past_date + pd.Timedelta(days=3)

        df = self.iso.get_wind_forecast(
            start=past_date,
            end=end_date,
        )

        self._check_solar_and_wind_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
        ) + pd.Timedelta(days=6)

        assert df["Publish Time"].dt.date.unique().tolist() == [
            past_date,
            past_date + pd.Timedelta(days=1),
            past_date + pd.Timedelta(days=2),
        ]

    def test_get_wind_forecast_historical_before_schema_change(self):
        # Data schema changed on 2022-06-13
        # No south data for 2022-05-12 for wind
        date = pd.Timestamp("2022-05-12").date()

        df = self.iso.get_wind_forecast(date)

        self._check_solar_and_wind_forecast(df)

        assert df["South"].isnull().all()

    """get_status"""

    def test_get_status_latest(self):
        with pytest.raises(NotImplementedError):
            super().test_get_status_latest()

    """get_storage"""

    def test_get_storage_historical(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_historical()

    def test_get_storage_today(self):
        with pytest.raises(NotImplementedError):
            super().test_get_storage_today()

    """get_generation_outages_forecast"""

    def _check_generation_outages(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Region",
            "Derated Outages MW",
            "Forced Outages MW",
            "Planned Outages MW",
            "Unplanned Outages MW",
        ]

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            "1d",
        )

        assert (df["Region"].unique() == ["Central", "MISO", "North", "South"]).all()

    def test_get_generation_outages_forecast_latest(self):
        df = self.iso.get_generation_outages_forecast("latest")

        self._check_generation_outages(df)

        # Latest fetches the file published yesterday with the first forecast day today
        expected_start_date = self.local_start_of_today()

        assert df["Publish Time"].unique() == expected_start_date - pd.DateOffset(
            days=1,
        )
        assert df["Interval Start"].min() == expected_start_date
        assert df["Interval End"].max() == expected_start_date + pd.DateOffset(days=7)

    def test_get_generation_outages_forecast_historical_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=100)
        end = start + pd.DateOffset(days=3)

        df = self.iso.get_generation_outages_forecast(start, end)

        self._check_generation_outages(df)

        assert df["Interval Start"].min() == start + pd.DateOffset(days=1)

        assert df["Interval End"].max() == end + pd.DateOffset(days=7)

        assert df["Publish Time"].min() == start
        assert df["Publish Time"].nunique() == 3

    """get_generation_outages_estimated"""

    def test_get_generation_outages_estimated_latest(self):
        df = self.iso.get_generation_outages_estimated("latest")

        self._check_generation_outages(df)

        # Latest fetches the file published yesterday
        expected_start_date = self.local_start_of_today() - pd.DateOffset(days=30)

        assert df[
            "Publish Time"
        ].unique() == self.local_start_of_today() - pd.DateOffset(days=1)

        assert df["Interval Start"].min() == expected_start_date
        assert df["Interval End"].max() == self.local_start_of_today()

    def test_get_generation_outages_estimated_historical_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=100)
        end = start + pd.DateOffset(days=3)

        df = self.iso.get_generation_outages_estimated(start, end)

        self._check_generation_outages(df)

        assert df["Interval Start"].min() == start - pd.DateOffset(days=29)
        assert df["Interval End"].max() == end

        assert df["Publish Time"].min() == start
        assert df["Publish Time"].nunique() == 3

    @pytest.mark.parametrize(
        "date,end",
        constraint_dates,
    )
    def test_get_binding_constraints_supplemental(self, date, end):
        cassette_name = f"test_get_binding_constraints_supplemental_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_supplemental(
                date=date,
                end=end,
            )

            assert isinstance(df, pd.DataFrame)
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

            assert min(df["Date"]).date() == pd.to_datetime(date).date()
            assert max(df["Date"]).date() <= pd.Timestamp(end).date()
            assert df["Constraint ID"].dtype == np.int64
            assert df["Constraint Name"].dtype == object
            assert df["Contingency Name"].dtype == object
            assert df["Constraint Type"].dtype == object
            assert df["Flowgate Name"].dtype == object
            assert df["Device Type"].dtype == object
            assert df["Key1"].dtype == object
            assert df["Key2"].dtype == object
            assert df["Key3"].dtype == object
            assert df["Direction"].dtype == np.int64
            assert df["From Area"].dtype == object
            assert df["To Area"].dtype == object
            assert df["From Station"].dtype == object
            assert df["To Station"].dtype == object
            assert df["From KV"].dtype in [np.int64, np.float64]
            assert df["To KV"].dtype in [np.int64, np.float64]

    @pytest.mark.parametrize(
        "date,end",
        constraint_dates,
    )
    def test_get_binding_constraints_day_ahead_hourly(self, date, end):
        cassette_name = (
            f"test_get_binding_constraints_day_ahead_hourly_{date}_{end}.yaml"
        )
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_day_ahead_hourly(
                date=date,
                end=end,
            )

            assert isinstance(df, pd.DataFrame)
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

            assert min(df["Interval Start"]).date() == pd.Timestamp(date).date()
            assert max(df["Interval End"]).date() <= pd.Timestamp(end).date()
            assert df["Constraint ID"].dtype == np.int64
            assert df["Constraint Name"].dtype == object
            assert df["Branch Name"].dtype == object
            assert df["Contingency Description"].dtype == object
            assert df["Shadow Price"].dtype in [np.float64, np.int64]
            assert df["Constraint Description"].dtype == object
            assert df["Override"].dtype == np.int64
            assert df["Curve Type"].dtype == object
            assert df["BP1"].dtype in [np.float64, np.int64]
            assert df["PC1"].dtype in [np.float64, np.int64]
            assert df["BP2"].dtype in [np.float64, np.int64]
            assert df["PC2"].dtype in [np.float64, np.int64]
            assert df["Reason"].dtype == object

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
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_subregional_power_balance_constraints_day_ahead_hourly(
                date=date,
                end=end,
            )

            assert isinstance(df, pd.DataFrame)
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

            if not df.empty:
                assert min(df["Interval Start"]).date() <= pd.Timestamp(date).date()
                assert max(df["Interval End"]).date() <= pd.Timestamp(end).date()
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
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_reserve_product_binding_constraints_day_ahead_hourly(
                date=date,
                end=end,
            )

            assert isinstance(df, pd.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Shadow Price",
                "Constraint Description",
            ]

            if not df.empty:
                assert min(df["Interval Start"]).date() == pd.to_datetime(date).date()
                assert max(df["Interval End"]).date() <= pd.Timestamp(end).date()
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
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_real_time_5_min(
                date=date,
                end=end,
            )

            assert isinstance(df, pd.DataFrame)
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

            if not df.empty:
                assert min(df["Interval Start"]).date() <= pd.Timestamp(date).date()
                assert max(df["Interval End"]).date() <= pd.Timestamp(end).date()
            else:
                pytest.skip(
                    "No data available for this date range, so skipping data-comparison assertions",
                )

    def test_get_binding_constraints_real_time_yearly_historical(self):
        year = 2023
        cassette_name = (
            f"test_get_binding_constraints_real_time_yearly_historical_{year}.yaml"
        )
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_real_time_yearly_historical(
                year=year,
            )

            assert isinstance(df, pd.DataFrame)
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

            if not df.empty:
                assert min(df["Interval End"]).year == year
                assert max(df["Interval End"]).year == year
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
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraint_overrides_real_time_5_min(
                date=date,
                end=end,
            )

            assert isinstance(df, pd.DataFrame)
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

            if not df.empty:
                assert min(df["Interval Start"]).date() == pd.to_datetime(date).date()
                assert max(df["Interval End"]).date() <= pd.Timestamp(end).date()
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
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_subregional_power_balance_constraints_real_time_5_min(
                date=date,
                end=end,
            )

            assert isinstance(df, pd.DataFrame)
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

            if not df.empty:
                assert min(df["Interval Start"]).date() == pd.to_datetime(date).date()
                assert max(df["Interval End"]).date() <= pd.Timestamp(end).date()
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
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_reserve_product_binding_constraints_real_time_5_min(
                date=date,
                end=end,
            )

            assert isinstance(df, pd.DataFrame)
            assert list(df.columns) == [
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Shadow Price",
                "Constraint Description",
            ]

            if not df.empty:
                assert min(df["Interval Start"]).date() == pd.to_datetime(date).date()
                assert max(df["Interval End"]).date() <= pd.Timestamp(end).date()
            else:
                pytest.skip(
                    "No data available for this date range, so skipping data-comparison assertions",
                )

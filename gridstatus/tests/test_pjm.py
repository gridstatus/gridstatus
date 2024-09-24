import os
from datetime import datetime
from unittest import mock

import pandas as pd
import pytest

import gridstatus
from gridstatus import PJM, NotSupported
from gridstatus.base import Markets, NoDataFoundException
from gridstatus.decorators import _get_pjm_archive_date
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestPJM(BaseTestISO):
    iso = PJM()

    @mock.patch.dict(os.environ, {"PJM_API_KEY": "test_env"})
    def test_api_key_from_env(self):
        # test that api key is set from env var
        pjm = PJM()
        assert pjm.api_key == "test_env"

    def test_api_key_from_arg(self):
        # test that api key is set from arg
        pjm = PJM(api_key="test")
        assert pjm.api_key == "test"

    @mock.patch.dict(os.environ, {"PJM_API_KEY": ""})
    def test_api_key_raises_if_missing(self):
        with pytest.raises(ValueError):
            _ = PJM(api_key=None)

    """get_fuel_mix"""

    def test_get_fuel_mix_no_data(self):
        date = "2000-01-14"
        with pytest.raises(NoDataFoundException):
            self.iso.get_fuel_mix(start=date)

    def test_get_fuel_mix_dst_shift_back(self):
        date = "2021-11-07"
        df = self.iso.get_fuel_mix(start=date)

        assert (
            len(df["Interval Start"]) == 25
        )  # 25 hours due to shift backwards in time
        assert (df["Interval Start"].dt.strftime("%Y-%m-%d") == date).all()

    def test_get_fuel_mix_dst_shift_forward(self):
        date = "2021-03-14"
        df = self.iso.get_fuel_mix(start=date)

        assert len(df["Interval Start"]) == 23  # 23 hours due to shift forwards in time
        assert (df["Interval Start"].dt.strftime("%Y-%m-%d") == date).all()

    """get_lmp"""

    # override for base test case
    lmp_cols = [
        "Time",
        "Interval Start",
        "Interval End",
        "Market",
        "Location Id",
        "Location Name",
        "Location Short Name",
        "Location Type",
        "LMP",
        "Energy",
        "Congestion",
        "Loss",
    ]

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_lmp_date_range(self, market):
        super().test_lmp_date_range(market=market)

    @with_markets(
        # Markets.REAL_TIME_5_MIN, # TODO reenable, but too slow
        Markets.REAL_TIME_HOURLY,
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_get_lmp_historical(self, market):
        super().test_get_lmp_historical(market=market)

    @with_markets(
        Markets.DAY_AHEAD_HOURLY,
        Markets.REAL_TIME_HOURLY,
        Markets.REAL_TIME_5_MIN,
    )
    def test_get_lmp_latest(self, market):
        if market in [Markets.DAY_AHEAD_HOURLY, Markets.REAL_TIME_HOURLY]:
            with pytest.raises(NotSupported):
                super().test_get_lmp_latest(market=market)
        else:
            super().test_get_lmp_latest(market=market)

    @with_markets(
        Markets.REAL_TIME_HOURLY,
        Markets.REAL_TIME_5_MIN,
        Markets.DAY_AHEAD_HOURLY,
    )
    def test_get_lmp_today(self, market):
        if market in [Markets.REAL_TIME_HOURLY]:
            with pytest.raises(
                NoDataFoundException,
                match="No data found for rt_hrl_lmps",
            ):  # noqa
                super().test_get_lmp_today(market=market)
        else:
            super().test_get_lmp_today(market=market)

    def test_get_lmp_no_data(self):
        # raise no error since date in future
        future_date = pd.Timestamp.now().normalize() + pd.DateOffset(days=10)
        with pytest.raises(NoDataFoundException):
            self.iso.get_lmp(
                date=future_date,
                market="REAL_TIME_5_MIN",
            )

    def test_get_lmp_hourly(self):
        markets = [
            Markets.REAL_TIME_HOURLY,
            Markets.DAY_AHEAD_HOURLY,
        ]

        for m in markets:
            print(self.iso.iso_id, m)
            self._lmp_tests(m)

    def test_get_lmp_returns_latest(self):
        # this interval has two LMP versions
        # make sure only one is returned
        # for each location
        df = self.iso.get_lmp(
            start="04-06-2023 17:45",
            end="04-06-2023 17:50",
            market="REAL_TIME_5_MIN",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert df.duplicated(["Interval Start", "Location Id"]).sum() == 0

    @pytest.mark.slow
    def test_get_lmp_5_min(self):
        self._lmp_tests(Markets.REAL_TIME_5_MIN)

    def test_get_lmp_query_by_location_type(self):
        df = self.iso.get_lmp(
            date="Oct 20, 2022",
            market="DAY_AHEAD_HOURLY",
            location_type="ZONE",
            verbose=True,
        )
        df

    @pytest.mark.slow
    def test_get_lmp_all_pnodes(self):
        df = self.iso.get_lmp(
            date="Jan 1, 2022",
            market="REAL_TIME_HOURLY",
            locations="ALL",
        )

        assert len(df) > 0

    """ get_load """

    def test_get_load_today(self):
        df = self.iso.get_load("today")
        self._check_load(df)
        today = pd.Timestamp.now(tz=self.iso.default_timezone).date()

        # okay as long as one of these columns is only today
        assert (
            (df["Time"].dt.date == today).all()
            or (df["Interval Start"].dt.date == today).all()
            or (df["Interval End"].dt.date == today).all()
        )

        assert df.columns.tolist() == [
            "Time",
            "Interval Start",
            "Interval End",
            "Load",
            "AE",
            "AEP",
            "APS",
            "ATSI",
            "BC",
            "COMED",  # noqa
            "DAYTON",
            "DEOK",
            "DOM",
            "DPL",
            "DUQ",
            "EKPC",
            "JC",
            "ME",
            "PE",
            "PEP",  # noqa
            "PJM MID ATLANTIC REGION",
            "PJM RTO",
            "PJM SOUTHERN REGION",  # noqa
            "PJM WESTERN REGION",
            "PL",
            "PN",
            "PS",
            "RECO",
            "UG",
        ]  # noqa

    """get_load_forecast"""

    load_forecast_columns = [
        "Interval Start",
        "Interval End",
        "Publish Time",
        "Load Forecast",
        "AE/MIDATL",
        "AEP",
        "AP",
        "ATSI",
        "BG&E/MIDATL",
        "COMED",
        "DAYTON",
        "DEOK",
        "DOMINION",
        "DP&L/MIDATL",
        "DUQUESNE",
        "EKPC",
        "JCP&L/MIDATL",
        "METED/MIDATL",
        "MID_ATLANTIC_REGION",
        "PECO/MIDATL",
        "PENELEC/MIDATL",
        "PEPCO/MIDATL",
        "PPL/MIDATL",
        "PSE&G/MIDATL",
        "RECO/MIDATL",
        "RTO_COMBINED",
        "SOUTHERN_REGION",
        "UGI/MIDATL",
        "WESTERN_REGION",
    ]

    def test_get_load_forecast_today(self):
        df = self.iso.get_load_forecast("today")
        assert df.columns.tolist() == self.load_forecast_columns
        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=7,
        )

        assert df["Publish Time"].nunique() == 1

        assert self.iso.get_load_forecast("latest").equals(df)

    def test_get_load_forecast_in_past_raises_error(self):
        start_date = self.local_today() - pd.Timedelta(days=1)
        with pytest.raises(NotSupported):
            self.iso.get_load_forecast(start_date)

    """get_load_forecast_historical"""

    load_forecast_columns_historical = [
        "Interval Start",
        "Interval End",
        "Publish Time",
        "Load Forecast",
        "AEP",
        "APS",
        "ATSI",
        "COMED",
        "DAY",
        "DEOK",
        "DOM",
        "DUQ",
        "EKPC",
        "MIDATL",
        "RTO",
    ]

    def test_get_load_forecast_historical(self):
        start_date = "2023-05-01"
        df = self.iso.get_load_forecast_historical(start_date)

        assert df.columns.tolist() == self.load_forecast_columns_historical
        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            start_date,
            # End is inclusive in this case
        ) + pd.DateOffset(days=1, hours=1)

        assert df["Interval Start"].value_counts().max() == 10
        assert df["Publish Time"].nunique() == 5 * 2

    def test_get_load_forecast_historical_with_date_range(self):
        start_date = "2022-10-17"
        end_date = "2022-10-20"

        df = self.iso.get_load_forecast_historical(start_date, end_date)

        assert df.columns.tolist() == self.load_forecast_columns_historical
        assert df["Interval Start"].min() == self.local_start_of_day(start_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            end_date,
        ) + pd.DateOffset(days=1, hours=1)

        assert df["Interval Start"].value_counts().max() == 10
        assert df["Publish Time"].nunique() == 5 * 5

    """get_pnode_ids"""

    def test_get_pnode_ids(self):
        df = self.iso.get_pnode_ids()
        assert len(df) > 0

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

    """pjm_update_dates"""

    def test_pjm_update_dates(self):
        args_dict = {
            "self": gridstatus.PJM(),
            "market": Markets.REAL_TIME_5_MIN,
        }

        # cross year
        dates = [
            pd.Timestamp("2018-12-31 00:00:00-0500", tz="US/Eastern"),
            pd.Timestamp("2019-01-01 00:00:00-0500", tz="US/Eastern"),
        ]
        new_dates = gridstatus.pjm.pjm_update_dates(dates, args_dict)
        assert new_dates == [
            pd.Timestamp("2018-12-31 00:00:00-0500", tz="US/Eastern"),
            pd.Timestamp("2018-12-31 23:59:00-0500", tz="US/Eastern"),
        ]

        # cross year and then more dates
        dates = [
            pd.Timestamp("2018-12-01 00:00:00-0500", tz="US/Eastern"),
            pd.Timestamp("2019-01-01 00:00:00-0500", tz="US/Eastern"),
            pd.Timestamp("2019-02-01 00:00:00-0500", tz="US/Eastern"),
        ]
        new_dates = gridstatus.pjm.pjm_update_dates(dates, args_dict)
        assert new_dates == [
            pd.Timestamp("2018-12-01 00:00:00-0500", tz="US/Eastern"),
            pd.Timestamp(
                "2018-12-31 23:59:00-0500",
                tz="US/Eastern",
            ),
            None,
            pd.Timestamp(
                "2019-01-01 00:00:00-0500",
                tz="US/Eastern",
            ),
            pd.Timestamp("2019-02-01 00:00:00-0500", tz="US/Eastern"),
        ]

        # cross multiple years
        dates = [
            pd.Timestamp("2017-12-01 00:00:00-0500", tz="US/Eastern"),
            pd.Timestamp("2020-02-01 00:00:00-0500", tz="US/Eastern"),
        ]
        new_dates = gridstatus.pjm.pjm_update_dates(dates, args_dict)
        assert new_dates == [
            pd.Timestamp("2017-12-01 00:00:00-0500", tz="US/Eastern"),
            pd.Timestamp(
                "2017-12-31 23:59:00-0500",
                tz="US/Eastern",
            ),
            None,
            pd.Timestamp(
                "2018-01-01 00:00:00-0500",
                tz="US/Eastern",
            ),
            pd.Timestamp(
                "2018-12-31 23:59:00-0500",
                tz="US/Eastern",
            ),
            None,
            pd.Timestamp(
                "2019-01-01 00:00:00-0500",
                tz="US/Eastern",
            ),
            pd.Timestamp(
                "2019-12-31 23:59:00-0500",
                tz="US/Eastern",
            ),
            None,
            pd.Timestamp(
                "2020-01-01 00:00:00-0500",
                tz="US/Eastern",
            ),
            pd.Timestamp(
                "2020-02-01 00:00:00-0500",
                tz="US/Eastern",
            ),
        ]

        # cross archive date
        archive_date = _get_pjm_archive_date(args_dict["market"])
        start = archive_date - pd.DateOffset(days=1)
        end = archive_date + pd.DateOffset(days=1)
        new_dates = gridstatus.pjm.pjm_update_dates([start, end], args_dict)
        day_before_archive = archive_date - pd.DateOffset(days=1)
        before_archive = pd.Timestamp(
            year=day_before_archive.year,
            month=day_before_archive.month,
            day=day_before_archive.day,
            hour=23,
            minute=59,
            tz=args_dict["self"].default_timezone,
        )
        assert new_dates == [
            start,
            before_archive,
            None,
            archive_date,
            end,
        ]

    """get_solar_forecast"""

    def _check_solar_forecast(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Solar Forecast BTM",
            "Solar Forecast",
        ]

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    def test_get_solar_forecast_today_or_latest(self):
        df = self.iso.get_solar_forecast("today")

        self._check_solar_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() >= self.local_start_of_today() + pd.Timedelta(
            days=2,
        )

        assert (
            df["Publish Time"].dt.tz_convert(self.iso.default_timezone).dt.date
            == self.local_today()
        ).all()

        assert self.iso.get_solar_forecast("latest").equals(df)

    def test_get_solar_forecast_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)

        df = self.iso.get_solar_forecast(past_date)

        self._check_solar_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() >= self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=2)

        assert df["Publish Time"].min() == self.local_start_of_day(past_date)
        # When end date is generated this data
        # doesn't include forecast on the next day
        assert df["Publish Time"].max() < self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=1)

    def test_get_solar_forecast_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=12)
        past_end_date = past_date + pd.Timedelta(days=3)

        df = self.iso.get_solar_forecast(past_date, past_end_date)

        self._check_solar_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() >= self.local_start_of_day(
            past_end_date,
        ) + pd.Timedelta(days=2)

        assert df["Publish Time"].min() == self.local_start_of_day(past_date)
        # This data also includes one forecast time on the next day
        assert df["Publish Time"].max() == self.local_start_of_day(past_end_date)

    """get_wind_forecast"""

    def _check_wind_forecast(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Wind Forecast",
        ]

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    def test_get_wind_forecast_today_or_latest(self):
        df = self.iso.get_wind_forecast("today")

        self._check_wind_forecast(df)

        # For some reason, the start of the forecast is 5 hours after the day start
        assert df["Interval Start"].min() == self.local_start_of_today() + pd.Timedelta(
            hours=5,
        )
        assert df["Interval End"].max() >= self.local_start_of_today() + pd.Timedelta(
            days=2,
            hours=5,
        )

        assert (
            df["Publish Time"].dt.tz_convert(self.iso.default_timezone).dt.date
            == self.local_today()
        ).all()

        assert self.iso.get_wind_forecast("latest").equals(df)

    def test_get_wind_forecast_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)

        df = self.iso.get_wind_forecast(past_date)

        self._check_wind_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(hours=5)
        assert df["Interval End"].max() >= self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=2, hours=5)

        assert df["Publish Time"].min() == self.local_start_of_day(past_date)
        # When end date is generated this data
        # doesn't include forecast on the next day
        assert df["Publish Time"].max() < self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(days=1)

    def test_get_wind_forecast_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=12)
        past_end_date = past_date + pd.Timedelta(days=3)

        df = self.iso.get_wind_forecast(past_date, past_end_date)

        self._check_wind_forecast(df)

        assert df["Interval Start"].min() == self.local_start_of_day(
            past_date,
        ) + pd.Timedelta(hours=5)

        assert df["Interval End"].max() >= self.local_start_of_day(
            past_end_date,
        ) + pd.Timedelta(days=2)

        assert df["Publish Time"].min() == self.local_start_of_day(past_date)
        # This data also includes one forecast time on the next day
        assert df["Publish Time"].max() == self.local_start_of_day(past_end_date)

    """_lmp_tests"""

    def _lmp_tests(self, m):
        # uses location_type hub because it has the fewest results, so runs faster

        # test span archive date and year
        archive_date = _get_pjm_archive_date(m)
        start = archive_date - pd.DateOffset(days=366)
        end = archive_date + pd.DateOffset(days=1)
        hist = self.iso.get_lmp(
            start=start,
            end=end,
            location_type="hub",
            market=m,
        )
        assert isinstance(hist, pd.DataFrame)
        self._check_lmp_columns(hist, m)
        # has every hour in the range

        # check that every day has 23, 24, or 25 hrs
        unique_hours_per_day = (
            hist["Interval Start"]
            .drop_duplicates()
            .dt.strftime("%Y-%m-%d")
            .value_counts()
            .unique()
        )
        assert set(unique_hours_per_day).issubset([25, 24, 23])

        # test span archive date
        archive_date = _get_pjm_archive_date(m)
        start = archive_date - pd.DateOffset(days=1)
        end = archive_date + pd.DateOffset(days=1)
        hist = self.iso.get_lmp(
            start=start,
            end=end,
            location_type="hub",
            market=m,
        )
        assert isinstance(hist, pd.DataFrame)
        self._check_lmp_columns(hist, m)
        # 2 days worth of data for each location
        assert (
            hist.groupby("Location Id")["Interval Start"].agg(
                lambda x: x.dt.day.nunique(),
            )
            == 2
        ).all()

        # span calendar year
        hist = self.iso.get_lmp(
            start="2018-12-31",
            end="2019-01-02",
            location_type="hub",
            market=m,
        )
        assert isinstance(hist, pd.DataFrame)
        self._check_lmp_columns(hist, m)
        # 2 days worth of data for each location
        assert (hist.groupby("Location Id")["Interval Start"].count() == 48).all()

        # all archive
        hist = self.iso.get_lmp(
            start="2019-07-15",
            end="2019-07-16",
            location_type="hub",
            market=m,
        )
        assert isinstance(hist, pd.DataFrame)
        self._check_lmp_columns(hist, m)

        # all standard
        # move a few days back to avoid late published data
        end = pd.Timestamp.now().normalize() - pd.DateOffset(days=4)
        start = end - pd.DateOffset(days=1)

        hist = self.iso.get_lmp(
            start=start,
            end=end,
            location_type="hub",
            market=m,
        )
        assert isinstance(hist, pd.DataFrame)
        self._check_lmp_columns(hist, m)

    def test_get_gen_outages_by_type_with_latest(self):
        start_date_local = self.local_today()
        df = self.iso.get_gen_outages_by_type("latest")
        self._check_gen_outages_by_type(df)

        expected_date = self.to_local_datetime(start_date_local)
        assert (df["Publish Time"] == expected_date).all()
        assert (
            df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
        ).all()

    def test_get_gen_outages_by_type_with_past_date(self):
        start_date_local = self.local_today() - pd.DateOffset(days=3)
        start_date_time_local = self.local_start_of_day(start_date_local)
        df = self.iso.get_gen_outages_by_type(start_date_time_local)
        self._check_gen_outages_by_type(df)

        expected_date = self.to_local_datetime(start_date_local)
        assert (df["Publish Time"] == expected_date).all()
        assert (
            df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
        ).all()

    def test_get_gen_outages_by_type_with_multi_day_range(self):
        # start example: 2024-04-30 00:00:00-04:00
        start_date_local = self.local_today() - pd.DateOffset(days=3)
        start_date_time_local = self.local_start_of_day(start_date_local)
        # end example: 2024-05-01 23:59:59-04:00
        end_date_local = start_date_time_local + pd.DateOffset(days=2)
        end_date_time_local = end_date_local - pd.DateOffset(seconds=1)

        # expect only 2024-04-30 00:00:00-04:00 and 2024-05-01 00:00:00-04:00 in results
        expected_date_1 = self.to_local_datetime(start_date_local)
        expected_date_2 = self.to_local_datetime(
            (start_date_local + pd.DateOffset(days=1)),
        )
        expected_dates = {expected_date_1, expected_date_2}

        df = self.iso.get_gen_outages_by_type(
            start_date_time_local,
            end_date_time_local,
        )
        self._check_gen_outages_by_type(df)
        assert (df["Publish Time"].isin(expected_dates)).all()
        assert (
            df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
        ).all()

    def to_local_datetime(self, date_local):
        return pd.to_datetime(date_local).tz_localize(
            self.iso.default_timezone,
        )

    def _check_gen_outages_by_type(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Region",
            "Planned Outages MW",
            "Maintenance Outages MW",
            "Forced Outages MW",
            "Total Outages MW",
        ]

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    """get_projected_rto_statistics_at_peak"""

    def _check_projected_rto_statistics_at_peak(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Projected Peak Time",
            "Area",
            "Internal Scheduled Capacity",
            "Scheduled Tie Flow Total",
            "Capacity Adjustments",
            "Total Scheduled Capacity",
            "Load Forecast",
            "Operating Reserve",
            "Unscheduled Steam Capacity",
        ]

        assert (
            df["Total Scheduled Capacity"]
            == df["Load Forecast"] + df["Operating Reserve"]
        ).all()

    def test_projected_rto_statistics_at_peak_today_or_latest(self):
        df = self.iso.get_projected_rto_statistics_at_peak("today")

        self._check_projected_rto_statistics_at_peak(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() >= self.local_start_of_today() + pd.DateOffset(
            days=1,
        )

        assert self.iso.get_projected_rto_statistics_at_peak("latest").equals(df)

    def test_projected_rto_statistics_at_peak_historical_date(self):
        past_date = self.local_today() - pd.DateOffset(days=10)

        df = self.iso.get_projected_rto_statistics_at_peak(past_date)

        self._check_projected_rto_statistics_at_peak(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            past_date,
        ) + pd.DateOffset(days=1)

    def test_projected_rto_statistics_at_peak_historical_date_range(self):
        past_date = self.local_today() - pd.DateOffset(days=1000)
        past_end_date = past_date + pd.Timedelta(days=800)

        df = self.iso.get_projected_rto_statistics_at_peak(past_date, past_end_date)

        self._check_projected_rto_statistics_at_peak(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(past_end_date)

        assert df.shape[0] == 800
        assert df["Publish Time"].nunique() == 800

    """get_projected_area_statistics_at_peak"""

    def _check_projected_area_statistics_at_peak(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Projected Peak Time",
            "Area",
            "Internal Scheduled Capacity",
            "PJM Load Forecast",
            "Unscheduled Steam Capacity",
        ]

        assert set(df["Area"].unique().tolist()) == {
            "DAYTON",
            "AEP",
            "ATSI",
            "COMED",
            "MIDATL",
            "EKPC",
            "DOM",
            "DEOK",
            "DUQ",
            "OVEC",
            "AP",
        }

    def test_projected_area_statistics_at_peak_today_or_latest(self):
        df = self.iso.get_projected_area_statistics_at_peak("today")

        self._check_projected_area_statistics_at_peak(df)

        assert df["Interval Start"].min() == self.local_start_of_today()
        assert df["Interval End"].max() == self.local_start_of_today() + pd.DateOffset(
            days=1,
        )

        assert self.iso.get_projected_area_statistics_at_peak("latest").equals(df)

    def test_projected_area_statistics_at_peak_historical_date(self):
        past_date = self.local_today() - pd.DateOffset(days=2000)

        df = self.iso.get_projected_area_statistics_at_peak(past_date)

        self._check_projected_area_statistics_at_peak(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(
            past_date,
        ) + pd.DateOffset(days=1)

    def test_projected_area_statistics_at_peak_historical_date_range(self):
        past_date = self.local_today() - pd.DateOffset(days=2000)
        past_end_date = past_date + pd.Timedelta(days=800)

        df = self.iso.get_projected_area_statistics_at_peak(past_date, past_end_date)

        self._check_projected_area_statistics_at_peak(df)

        assert df["Interval Start"].min() == self.local_start_of_day(past_date)
        assert df["Interval End"].max() == self.local_start_of_day(past_end_date)

        unique_area_count = df["Area"].nunique()

        assert df.shape[0] == 800 * unique_area_count
        assert df["Publish Time"].nunique() == 800

    """get_solar_generation_5_min"""

    def _check_pjm_response(self, df, expected_cols, start, end):
        assert df.columns.tolist() == expected_cols

        is_single_day = start.date == end.date
        if is_single_day:
            # Make sure all the intervals start on the specified day
            assert (df["Interval Start"].dt.day == start.day).all()

        # There could be missing data at the start or end, so we
        # can only assert that all of the values are between the specified
        # start and stop times.
        assert df["Interval Start"].min() >= start
        assert df["Interval Start"].max() < end

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    expected_five_min_solar_gen_cols = [
        "Interval Start",
        "Interval End",
        "Solar Generation",
    ]

    def test_get_solar_generation_5_min_today_or_latest(self):
        df = self.iso.get_solar_generation_5_min("today")
        range_start = self.local_start_of_today()
        range_end = self.local_start_of_today() + pd.Timedelta(days=1)
        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_five_min_solar_gen_cols,
            start=range_start,
            end=range_end,
        )

        assert self.iso.get_solar_generation_5_min("latest").equals(df)

    def test_get_solar_generation_5_min_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        df = self.iso.get_solar_generation_5_min(past_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_five_min_solar_gen_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_solar_generation_5_min_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=12)
        past_end_date = past_date + pd.Timedelta(days=3)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_end_date)

        df = self.iso.get_solar_generation_5_min(past_date, past_end_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_five_min_solar_gen_cols,
            start=range_start,
            end=range_end,
        )

    """get_wind_generation_instantaneous"""

    expected_wind_gen_cols = [
        "Interval Start",
        "Interval End",
        "Wind Generation",
    ]

    def test_get_wind_generation_instantaneous_today_or_latest(self):
        df = self.iso.get_wind_generation_instantaneous("today")
        range_start = self.local_start_of_today()
        range_end = self.local_start_of_today() + pd.Timedelta(days=1)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_wind_gen_cols,
            start=range_start,
            end=range_end,
        )

        assert self.iso.get_wind_generation_instantaneous("latest").equals(df)

    def test_get_wind_generation_instantaneous_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        df = self.iso.get_wind_generation_instantaneous(past_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_wind_gen_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_wind_generation_instantaneous_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=12)
        past_end_date = past_date + pd.Timedelta(days=3)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_end_date)

        df = self.iso.get_wind_generation_instantaneous(past_date, past_end_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_wind_gen_cols,
            start=range_start,
            end=range_end,
        )

    """get_operational_reserves"""

    expected_operational_reserves_cols = [
        "Interval Start",
        "Interval End",
        "Reserve Name",
        "Reserve",
    ]

    def test_get_operational_reserves_today_or_latest(self):
        df = self.iso.get_operational_reserves("today")
        range_start = self.local_start_of_today()
        range_end = self.local_start_of_today() + pd.Timedelta(days=1)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_operational_reserves_cols,
            start=range_start,
            end=range_end,
        )

        assert self.iso.get_operational_reserves("latest").equals(df)

    def test_get_operational_reserves_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        df = self.iso.get_operational_reserves(past_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_operational_reserves_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_operational_reserves_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_end_date)

        df = self.iso.get_operational_reserves(past_date, past_end_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_operational_reserves_cols,
            start=range_start,
            end=range_end,
        )

    """get_transfer_interface_information_5_min"""

    expected_transfer_interface_info_cols = [
        "Interval Start",
        "Interval End",
        "Interface Name",
        "Actual Flow",
        "Warning Level",
        "Transfer Limit",
    ]

    def test_get_transfer_interface_information_5_min_today_or_latest(self):
        df = self.iso.get_transfer_interface_information_5_min("today")
        range_start = self.local_start_of_today()
        range_end = self.local_start_of_today() + pd.Timedelta(days=1)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_transfer_interface_info_cols,
            start=range_start,
            end=range_end,
        )

        assert self.iso.get_transfer_interface_information_5_min("latest").equals(df)

    def test_get_transfer_interface_information_5_min_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        df = self.iso.get_transfer_interface_information_5_min(past_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_transfer_interface_info_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_transfer_interface_information_5_min_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_end_date)

        df = self.iso.get_transfer_interface_information_5_min(past_date, past_end_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_transfer_interface_info_cols,
            start=range_start,
            end=range_end,
        )

    """get_transfer_interface_information_5_min"""

    expected_transmission_limits_cols = [
        "Interval Start",
        "Interval End",
        "Constraint Name",
        "Constraint Type",
        "Contingency",
        "Shadow Price",
    ]

    def test_get_transmission_limits_today_or_latest(self):
        df = self.iso.get_transmission_limits("today")
        range_start = self.local_start_of_today()
        range_end = self.local_start_of_today() + pd.Timedelta(days=1)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_transmission_limits_cols,
            start=range_start,
            end=range_end,
        )

        assert self.iso.get_transmission_limits("latest").equals(df)

    def test_get_transmission_limits_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        df = self.iso.get_transmission_limits(past_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_transmission_limits_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_transmission_limits_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_end_date)

        df = self.iso.get_transmission_limits(past_date, past_end_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_transmission_limits_cols,
            start=range_start,
            end=range_end,
        )

    """get_solar_generation_by_area"""

    expected_solar_wind_gen_by_area_cols = [
        "Interval Start",
        "Interval End",
        "MIDATL",
        "OTHER",
        "RFC",
        "RTO",
        "SOUTH",
        "WEST",
    ]

    def test_get_solar_generation_by_area_today_or_latest(self):
        df = self.iso.get_solar_generation_by_area("today")
        range_start = self.local_start_of_today()
        range_end = self.local_start_of_today() + pd.Timedelta(days=1)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_solar_wind_gen_by_area_cols,
            start=range_start,
            end=range_end,
        )

        assert self.iso.get_solar_generation_by_area("latest").equals(df)

    def test_get_solar_generation_by_area_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        df = self.iso.get_solar_generation_by_area(past_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_solar_wind_gen_by_area_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_solar_generation_by_area_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_end_date)

        df = self.iso.get_solar_generation_by_area(past_date, past_end_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_solar_wind_gen_by_area_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_wind_generation_by_area_today_or_latest(self):
        df = self.iso.get_wind_generation_by_area("today")
        range_start = self.local_start_of_today()
        range_end = self.local_start_of_today() + pd.Timedelta(days=1)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_solar_wind_gen_by_area_cols,
            start=range_start,
            end=range_end,
        )

        assert self.iso.get_wind_generation_by_area("latest").equals(df)

    def test_get_wind_generation_by_area_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        df = self.iso.get_wind_generation_by_area(past_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_solar_wind_gen_by_area_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_wind_generation_by_area_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_end_date)

        df = self.iso.get_wind_generation_by_area(past_date, past_end_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_solar_wind_gen_by_area_cols,
            start=range_start,
            end=range_end,
        )

    expected_dam_as_market_results_cols = [
        "Interval Start",
        "Interval End",
        "Ancillary Service",
        "Locale",
        "Service Type",
        "Market Clearing Price",
        "Market Clearing Price Capped",
        "Ancillary Service Required",
        "Total MW",
        "Assigned MW",
        "Self-Scheduled MW",
        "Interface Reserve Capability MW",
        "Demand Response MW Assigned",
        "Non-Synchronized Reserve MW Assigned",
    ]

    def test_get_dam_as_market_results_today_or_latest(self):
        df = self.iso.get_dam_as_market_results("today")
        range_start = self.local_start_of_today()
        range_end = self.local_start_of_today() + pd.Timedelta(days=1)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_dam_as_market_results_cols,
            start=range_start,
            end=range_end,
        )

        assert self.iso.get_dam_as_market_results("latest").equals(df)

    def test_get_dam_as_market_results_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        df = self.iso.get_dam_as_market_results(past_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_dam_as_market_results_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_dam_as_market_results_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_end_date)

        df = self.iso.get_dam_as_market_results(past_date, past_end_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_dam_as_market_results_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_dam_as_market_results_parsing(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)

        df = self.iso.get_dam_as_market_results(past_date, past_end_date)

        # Should have full values for locale and service type without abbreviations
        assert df["Locale"].isin(self.iso.locale_abbreviated_to_full.values()).all()
        assert (
            df["Service Type"]
            .isin(self.iso.service_type_abbreviated_to_full.values())
            .all()
        )

        # Should contain new Ancillary Service that is concatenation of
        # abbreviated locale and full service values
        assert "Ancillary Service" in df.columns

        # Should contain new Ancillary Service that is concatenation of
        # abbreviated locale and full service values
        assert "Ancillary Service" in df.columns
        for row in df.iterrows():
            prefix, suffix = row[1]["Ancillary Service"].split("-")
            assert prefix in self.iso.locale_abbreviated_to_full.keys()
            assert suffix in self.iso.service_type_abbreviated_to_full.values()

    expected_real_time_as_market_results_cols = [
        "Interval Start",
        "Interval End",
        "Ancillary Service",
        "Locale",
        "Service Type",
        "Market Clearing Price",
        "Market Clearing Price Capped",
        "Regulation Capability Clearing Price",
        "Regulation Performance Clearing Price",
        "Ancillary Service Required",
        "Total MW",
        "Assigned MW",
        "Self-Scheduled MW",
        "Tier 1 MW",
        "Interface Reserve Capability MW",
        "Demand Response MW Assigned",
        "Non-Synchronized Reserve MW Assigned",
        "REGD MW",
    ]

    def test_get_real_time_as_market_results_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        df = self.iso.get_real_time_as_market_results(past_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_real_time_as_market_results_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_real_time_as_market_results_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_end_date)

        df = self.iso.get_real_time_as_market_results(past_date, past_end_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_real_time_as_market_results_cols,
            start=range_start,
            end=range_end,
        )

    def test_get_real_time_as_market_results_parsing(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)

        df = self.iso.get_real_time_as_market_results(past_date, past_end_date)

        # Should replace abbreviations with full values
        assert df["Locale"].isin(self.iso.locale_abbreviated_to_full.values()).all()
        assert (
            df["Service Type"]
            .isin(self.iso.service_type_abbreviated_to_full.values())
            .all()
        )

        # Should contain new Ancillary Service that is concatenation of
        # abbreviated locale and full service values
        assert "Ancillary Service" in df.columns
        for row in df.iterrows():
            prefix, suffix = row[1]["Ancillary Service"].split("-")
            assert prefix in self.iso.locale_abbreviated_to_full.keys()
            assert suffix in self.iso.service_type_abbreviated_to_full.values()

    def test_get_real_time_as_market_results_valid_dates(self):
        cutoff_date = datetime(2022, 9, 1)

        # If both dates are before the cutoff, this is valid
        # Data interval should be one hour
        start = cutoff_date - pd.Timedelta(days=5)
        end = cutoff_date - pd.Timedelta(days=3)
        df = self.iso.get_real_time_as_market_results(date=start, end=end)
        interval_start = df.iloc[0, :]["Interval Start"]
        interval_end = df.iloc[0, :]["Interval End"]
        assert interval_end - interval_start == pd.Timedelta(hours=1)

        # If both dates are after the cutoff, this is valid
        # Data interval should be five minutes
        start = cutoff_date + pd.Timedelta(days=3)
        end = cutoff_date + pd.Timedelta(days=5)
        df = self.iso.get_real_time_as_market_results(date=start, end=end)
        interval_start = df.iloc[0, :]["Interval Start"]
        interval_end = df.iloc[0, :]["Interval End"]
        assert interval_end - interval_start == pd.Timedelta(minutes=5)

        # If the start is before the cutoff and the end is after,
        # this is invalid, and an error should be raised.
        start = cutoff_date - pd.Timedelta(days=5)
        end = cutoff_date + pd.Timedelta(days=3)
        with pytest.raises(ValueError, match="Both start and end dates must be before"):
            self.iso.get_real_time_as_market_results(date=start, end=end, error="raise")

    def test_get_interconnection_queue(self):
        from gridstatus.base import _interconnection_columns

        queue = self.iso.get_interconnection_queue()
        # todo make sure datetime columns are right type
        assert isinstance(queue, pd.DataFrame)
        assert queue.shape[0] > 0
        assert set(_interconnection_columns).issubset(queue.columns)

    """get_load_metered_hourly"""

    def _check_load_metered_hourly(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "NERC Region",
            "Mkt Region",
            "Zone",
            "Load Area",
            "MW",
            "Is Verified",
        ]

        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(hours=1)
        ).all()

        assert set(df["Mkt Region"]) == {
            "WEST",
            "SOUTH",
            "MIDATL",
            "RTO",
        }

        assert set(df["NERC Region"]) == {"RFC", "SERC", "RTO"}

    def test_get_load_metered_hourly_historical_date(self):
        date = self.local_today() - pd.Timedelta(days=10)

        df = self.iso.get_load_metered_hourly(date)

        self._check_load_metered_hourly(df)

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(
            date,
        ) + pd.DateOffset(
            days=1,
        )

    def test_get_load_metered_hourly_historical_date_range(self):
        date = self.local_today() - pd.Timedelta(days=12)
        end_date = date + pd.Timedelta(days=3)

        df = self.iso.get_load_metered_hourly(date, end_date)

        self._check_load_metered_hourly(df)

        assert df["Interval Start"].min() == self.local_start_of_day(date)
        assert df["Interval End"].max() == self.local_start_of_day(end_date)

    """get_forecasted_generation_outages"""

    def _check_forecasted_gen_outages(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "RTO MW",
            "West MW",
            "Other MW",
        ]

        self._check_time_columns(
            df,
            instant_or_interval="interval",
            skip_column_named_time=True,
        )

    def test_get_forecasted_generation_outages_today_or_latest(self):
        df = self.iso.get_forecasted_generation_outages("today")
        self._check_forecasted_gen_outages(df)
        start_date_local = self.local_today()
        expected_date = self.to_local_datetime(start_date_local)

        assert (df["Publish Time"] == expected_date).all()
        assert (
            df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
        ).all()

        assert self.iso.get_forecasted_generation_outages("latest").equals(df)

    def test_get_forecasted_generation_outages_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        df = self.iso.get_forecasted_generation_outages(past_date)
        self._check_forecasted_gen_outages(df)
        expected_date = self.to_local_datetime(past_date)

        assert (df["Publish Time"] == expected_date).all()
        assert (
            df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
        ).all()

    def test_get_forecasted_generation_outages_historical_range(self):
        # start example: 2024-04-30 00:00:00-04:00
        start_date_local = self.local_today() - pd.DateOffset(days=3)
        start_date_time_local = self.local_start_of_day(start_date_local)
        # end example: 2024-05-01 23:59:59-04:00
        end_date_local = start_date_time_local + pd.DateOffset(days=2)
        end_date_time_local = end_date_local - pd.DateOffset(seconds=1)

        # expect only 2024-04-30 00:00:00-04:00 and 2024-05-01 00:00:00-04:00 in results
        expected_date_1 = self.to_local_datetime(start_date_local)
        expected_date_2 = self.to_local_datetime(
            (start_date_local + pd.DateOffset(days=1)),
        )
        expected_dates = {expected_date_1, expected_date_2}

        df = self.iso.get_forecasted_generation_outages(
            start_date_time_local,
            end_date_time_local,
        )
        self._check_forecasted_gen_outages(df)
        assert (df["Publish Time"].isin(expected_dates)).all()
        assert (
            df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
        ).all()

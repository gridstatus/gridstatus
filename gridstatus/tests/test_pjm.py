import os
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

    def test_get_load_forecast_historical(self):
        with pytest.raises(NotSupported):
            super().test_get_load_forecast_historical()

    @pytest.mark.skip(reason="Not Applicable")
    def test_get_load_forecast_historical_with_date_range(self):
        pass

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
        assert (df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)).all()

    def test_get_gen_outages_by_type_with_past_date(self):
        start_date_local = self.local_today() - pd.DateOffset(days=3)
        start_date_time_local = self.local_start_of_day(start_date_local)
        df = self.iso.get_gen_outages_by_type(start_date_time_local)
        self._check_gen_outages_by_type(df)

        expected_date = self.to_local_datetime(start_date_local)
        assert (df["Publish Time"] == expected_date).all()
        assert (df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)).all()

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
            (start_date_local + pd.DateOffset(days=1))
        )
        expected_dates = {expected_date_1, expected_date_2}

        df = self.iso.get_gen_outages_by_type(
            start_date_time_local, end_date_time_local
        )
        self._check_gen_outages_by_type(df)
        assert (df["Publish Time"].isin(expected_dates)).all()
        assert (df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)).all()

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

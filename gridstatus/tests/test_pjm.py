import pandas as pd
import pytest

import gridstatus
from gridstatus import PJM, NotSupported
from gridstatus.base import Markets
from gridstatus.decorators import _get_pjm_archive_date
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.decorators import with_markets


class TestPJM(BaseTestISO):
    iso = PJM()

    """get_fuel_mix"""

    def test_get_fuel_mix_no_data(self):
        date = "2000-01-14"
        with pytest.raises(RuntimeError):
            self.iso.get_fuel_mix(start=date)

    def test_get_fuel_mix_dst_shift_back(self):
        date = "2021-11-07"
        df = self.iso.get_fuel_mix(start=date)

        assert len(df["Time"]) == 25  # 25 hours due to shift backwards in time
        assert (df["Time"].dt.strftime("%Y-%m-%d") == date).all()

    def test_get_fuel_mix_dst_shift_forward(self):
        date = "2021-03-14"
        df = self.iso.get_fuel_mix(start=date)

        assert len(df["Time"]) == 23  # 23 hours due to shift forwards in time
        assert (df["Time"].dt.strftime("%Y-%m-%d") == date).all()

    """get_lmp"""

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
            with pytest.raises(RuntimeError, match="No data found for query"):
                super().test_get_lmp_today(market=market)
        else:
            super().test_get_lmp_today(market=market)

    def test_get_lmp_no_data(self):
        # raise no error since date in future
        future_date = pd.Timestamp.now().normalize() + pd.DateOffset(days=10)
        with pytest.raises(RuntimeError):
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
        df = super().test_get_load_today()

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
            hist["Time"]
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
            hist.groupby("Location")["Time"].agg(
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
        assert (hist.groupby("Location")["Time"].count() == 48).all()

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
        end = pd.Timestamp.now() - pd.DateOffset(days=4)
        start = end - pd.DateOffset(days=1)

        hist = self.iso.get_lmp(
            start=start,
            end=end,
            location_type="hub",
            market=m,
        )
        assert isinstance(hist, pd.DataFrame)
        self._check_lmp_columns(hist, m)

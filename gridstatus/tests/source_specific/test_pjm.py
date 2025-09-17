import json
import os
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest

import gridstatus
from gridstatus import PJM, NotSupported
from gridstatus.base import Markets, NoDataFoundException
from gridstatus.decorators import _get_pjm_archive_date
from gridstatus.tests.base_test_iso import BaseTestISO
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

pjm_vcr = setup_vcr(
    source="pjm",
    record_mode=RECORD_MODE,
)


class TestPJM(BaseTestISO):
    iso = PJM()

    test_dates = [
        ("2023-11-05", "2023-11-07"),
        ("2024-09-02", "2024-09-04"),
    ]

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

    @pytest.mark.parametrize("date", ["2000-01-14"])
    def test_get_fuel_mix_no_data(self, date):
        with pjm_vcr.use_cassette(f"test_get_fuel_mix_no_data_{date}.yaml"):
            with pytest.raises(NoDataFoundException):
                self.iso.get_fuel_mix(start=date)

    @pytest.mark.parametrize("date", ["2021-11-07"])
    def test_get_fuel_mix_dst_shift_back(self, date):
        with pjm_vcr.use_cassette(f"test_get_fuel_mix_dst_shift_back_{date}.yaml"):
            df = self.iso.get_fuel_mix(start=date)

        assert (
            len(df["Interval Start"]) == 25
        )  # 25 hours due to shift backwards in time
        assert (df["Interval Start"].dt.strftime("%Y-%m-%d") == date).all()

    @pytest.mark.parametrize("date", ["2021-03-14"])
    def test_get_fuel_mix_dst_shift_forward(self, date):
        with pjm_vcr.use_cassette(f"test_get_fuel_mix_dst_shift_forward_{date}.yaml"):
            df = self.iso.get_fuel_mix(start=date)

            assert (
                len(df["Interval Start"]) == 23
            )  # 23 hours due to shift forwards in time
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

    @pytest.mark.parametrize("market", [Markets.DAY_AHEAD_HOURLY])
    def test_lmp_date_range(self, market: Markets):
        with pjm_vcr.use_cassette(f"test_lmp_date_range_{market}.yaml"):
            super().test_lmp_date_range(market=market)

    @pytest.mark.parametrize(
        "market",
        [
            Markets.REAL_TIME_5_MIN,
            Markets.REAL_TIME_HOURLY,
            Markets.DAY_AHEAD_HOURLY,
        ],
    )
    def test_get_lmp_historical(self, market: Markets):
        with pjm_vcr.use_cassette(f"test_get_lmp_historical_{market}.yaml"):
            super().test_get_lmp_historical(market=market)

    @pytest.mark.parametrize(
        "market",
        [
            Markets.DAY_AHEAD_HOURLY,
            Markets.REAL_TIME_HOURLY,
            Markets.REAL_TIME_5_MIN,
        ],
    )
    def test_get_lmp_latest(self, market: Markets):
        with pjm_vcr.use_cassette(f"test_get_lmp_latest_{market}.yaml"):
            if market in [Markets.DAY_AHEAD_HOURLY, Markets.REAL_TIME_HOURLY]:
                with pytest.raises(NotSupported):
                    super().test_get_lmp_latest(market=market)
            else:
                super().test_get_lmp_latest(market=market)

    @pytest.mark.parametrize(
        "market",
        [
            Markets.REAL_TIME_HOURLY,
            Markets.REAL_TIME_5_MIN,
            Markets.DAY_AHEAD_HOURLY,
        ],
    )
    def test_get_lmp_today(self, market: Markets):
        with pjm_vcr.use_cassette(f"test_get_lmp_today_{market}.yaml"):
            if market in [Markets.REAL_TIME_HOURLY]:
                with pytest.raises(
                    NoDataFoundException,
                    match="No data found for rt_hrl_lmps",
                ):  # noqa
                    super().test_get_lmp_today(market=market)
            else:
                super().test_get_lmp_today(market=market)

    @pytest.mark.parametrize(
        "date",
        [
            pd.Timestamp.now().normalize() + pd.DateOffset(days=10),
        ],
    )
    def test_get_lmp_no_data(self, date: pd.Timestamp):
        with pytest.raises(NoDataFoundException):
            self.iso.get_lmp(
                date=date,
                market="REAL_TIME_5_MIN",
            )

    @pytest.mark.parametrize(
        "market",
        [
            Markets.REAL_TIME_HOURLY,
            Markets.DAY_AHEAD_HOURLY,
        ],
    )
    def test_get_lmp_hourly(self, market: Markets):
        with pjm_vcr.use_cassette(f"test_get_lmp_hourly_{market}.yaml"):
            self._lmp_tests(market)

    @pytest.mark.parametrize(
        "date, end",
        [
            ("04-06-2023 17:45", "04-06-2023 17:50"),
        ],
    )
    def test_get_lmp_returns_latest(self, date: str, end: str):
        # this interval has two LMP versions
        # make sure only one is returned
        # for each location
        with pjm_vcr.use_cassette(f"test_get_lmp_returns_latest_{date}_{end}.yaml"):
            df = self.iso.get_lmp(
                start=date,
                end=end,
                market="REAL_TIME_5_MIN",
            )
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert df.duplicated(["Interval Start", "Location Id"]).sum() == 0

    @pytest.mark.parametrize(
        "date",
        [
            "Oct 20, 2022",
        ],
    )
    def test_get_lmp_query_by_location_type(self, date: str):
        with pjm_vcr.use_cassette(f"test_get_lmp_query_by_location_type_{date}.yaml"):
            df = self.iso.get_lmp(
                date=date,
                market="DAY_AHEAD_HOURLY",
                location_type="ZONE",
                verbose=True,
            )
            assert isinstance(df, pd.DataFrame)

    @pytest.mark.parametrize(
        "date",
        [
            "Jan 1, 2022",
        ],
    )
    def test_get_lmp_all_pnodes(self, date: str):
        with pjm_vcr.use_cassette(f"test_get_lmp_all_pnodes_{date}.yaml"):
            df = self.iso.get_lmp(
                date=date,
                market="REAL_TIME_HOURLY",
                locations="ALL",
            )

            assert len(df) > 0

    """get_it_sced_lmp_5_min"""

    def _check_it_sced_lmp_5_min(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Case Approval Time",
            "Location Id",
            "Location Name",
            "Location Short Name",
            "LMP",
            "Energy",
            "Congestion",
            "Loss",
        ]

        assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
            minutes=5,
        )

        assert np.allclose(
            df["LMP"],
            df["Energy"] + df["Congestion"] + df["Loss"],
        )

    @pytest.mark.parametrize(
        "date",
        [
            "today",
            "latest",
        ],
    )
    def test_get_it_sced_lmp_5_min_today(self, date: str):
        with pjm_vcr.use_cassette(f"test_get_it_sced_lmp_5_min_{date}.yaml"):
            df = self.iso.get_it_sced_lmp_5_min(date)

            self._check_it_sced_lmp_5_min(df)
            assert df["Interval Start"].min() == self.local_start_of_today()
            assert (
                df["Case Approval Time"].dt.date.unique()
                == [(self.local_today() - pd.Timedelta(days=1)), self.local_today()]
            ).all()

            # Compare to latest if testing today
            if date == "today":
                with pjm_vcr.use_cassette("test_get_it_sced_lmp_5_min_latest.yaml"):
                    df_latest = self.iso.get_it_sced_lmp_5_min("latest")
                    pd.testing.assert_frame_equal(df, df_latest)

    def test_get_it_sced_lmp_5_min_historical_date_range(self):
        start_date = self.local_today() - pd.Timedelta(days=10)
        end_date = start_date + pd.Timedelta(days=3)
        with pjm_vcr.use_cassette(
            f"test_get_it_sced_lmp_5_min_historical_date_range_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_it_sced_lmp_5_min(start_date, end_date)
            self._check_it_sced_lmp_5_min(df)

            assert df["Interval Start"].min() == self.local_start_of_day(start_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                end_date,
            ) + pd.DateOffset(minutes=-10)

            assert df["Case Approval Time"].dt.date.min() == start_date - pd.Timedelta(
                days=1,
            )
            assert df["Case Approval Time"].dt.date.max() == end_date - pd.Timedelta(
                days=1,
            )

    """ get_load """

    @pytest.mark.parametrize(
        "date",
        [
            "today",
        ],
    )
    def test_get_load_today(self, date: str):
        with pjm_vcr.use_cassette("test_get_load_today.yaml"):
            df = self.iso.get_load(date)
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
        "AE_MIDATL",
        "AEP",
        "AP",
        "ATSI",
        "BGE_MIDATL",
        "COMED",
        "DAYTON",
        "DEOK",
        "DOMINION",
        "DPL_MIDATL",
        "DUQUESNE",
        "EKPC",
        "JCPL_MIDATL",
        "METED_MIDATL",
        "MID_ATLANTIC_REGION",
        "PECO_MIDATL",
        "PENELEC_MIDATL",
        "PEPCO_MIDATL",
        "PPL_MIDATL",
        "PSEG_MIDATL",
        "RECO_MIDATL",
        "RTO_COMBINED",
        "SOUTHERN_REGION",
        "UGI_MIDATL",
        "WESTERN_REGION",
    ]

    def test_get_load_forecast_today(self):
        with pjm_vcr.use_cassette("test_get_load_forecast_today.yaml"):
            df = self.iso.get_load_forecast("today")
            assert df.columns.tolist() == self.load_forecast_columns
            assert df["Interval Start"].min() == self.local_start_of_today()
            assert df[
                "Interval End"
            ].max() == self.local_start_of_today() + pd.DateOffset(
                days=7,
            )

            assert df["Publish Time"].nunique() == 1
            assert self.iso.get_load_forecast("latest").equals(df)

    def test_get_load_forecast_in_past_raises_error(self):
        start_date = self.local_today() - pd.Timedelta(days=1)
        with pjm_vcr.use_cassette("test_get_load_forecast_in_past_raises_error.yaml"):
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

    @pytest.mark.parametrize(
        "date",
        [
            "2023-05-01",
        ],
    )
    def test_get_load_forecast_historical(self, date: str):
        with pjm_vcr.use_cassette(f"test_get_load_forecast_historical_{date}.yaml"):
            df = self.iso.get_load_forecast_historical(date)

            assert df.columns.tolist() == self.load_forecast_columns_historical
            assert df["Interval Start"].min() == self.local_start_of_day(date)
            assert df["Interval End"].max() == self.local_start_of_day(
                date,
                # End is inclusive in this case
            ) + pd.DateOffset(days=1, hours=1)

            assert df["Interval Start"].value_counts().max() == 10
            assert df["Publish Time"].nunique() == 5 * 2

    @pytest.mark.parametrize(
        "date, end",
        [
            ("2022-10-17", "2022-10-20"),
        ],
    )
    def test_get_load_forecast_historical_with_date_range(self, date: str, end: str):
        with pjm_vcr.use_cassette(
            f"test_get_load_forecast_historical_with_date_range_{date}_{end}.yaml",
        ):
            df = self.iso.get_load_forecast_historical(date, end)

            assert df.columns.tolist() == self.load_forecast_columns_historical
            assert df["Interval Start"].min() == self.local_start_of_day(date)
            assert df["Interval End"].max() == self.local_start_of_day(
                end,
            ) + pd.DateOffset(days=1, hours=1)

            assert df["Interval Start"].value_counts().max() == 10
            assert df["Publish Time"].nunique() == 5 * 5

    """get_pnode_ids"""

    def test_get_pnode_ids(self):
        with pjm_vcr.use_cassette("test_get_pnode_ids.yaml"):
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

    @pytest.mark.parametrize(
        "dates",  # across the year change
        [
            [
                pd.Timestamp("2018-12-31 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2019-01-01 00:00:00-0500", tz="US/Eastern"),
            ],
        ],
    )
    def test_pjm_update_dates(self, dates: list[pd.Timestamp]):
        args_dict = {
            "self": gridstatus.PJM(),
            "market": Markets.REAL_TIME_5_MIN,
        }

        with pjm_vcr.use_cassette(
            f"test_pjm_update_dates_{dates[0].strftime('%Y-%m-%d')}.yaml",
        ):
            new_dates = gridstatus.pjm.pjm_update_dates(dates, args_dict)
            assert new_dates == [
                pd.Timestamp("2018-12-31 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2018-12-31 23:59:00-0500", tz="US/Eastern"),
            ]

    @pytest.mark.parametrize(
        "dates",  # across the year change
        [
            [
                pd.Timestamp("2018-12-01 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2019-01-01 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2019-02-01 00:00:00-0500", tz="US/Eastern"),
            ],
        ],
    )
    def test_pjm_update_dates_cross_year_with_multiple_dates(
        self,
        dates: list[pd.Timestamp],
    ):
        args_dict = {
            "self": gridstatus.PJM(),
            "market": Markets.REAL_TIME_5_MIN,
        }

        with pjm_vcr.use_cassette(
            f"test_pjm_update_dates_cross_year_with_multiple_dates_{dates[0].strftime('%Y-%m-%d')}.yaml",
        ):
            new_dates = gridstatus.pjm.pjm_update_dates(dates, args_dict)
            assert new_dates == [
                pd.Timestamp("2018-12-01 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2018-12-31 23:59:00-0500", tz="US/Eastern"),
                None,
                pd.Timestamp("2019-01-01 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2019-02-01 00:00:00-0500", tz="US/Eastern"),
            ]

    @pytest.mark.parametrize(
        "dates",  # across the year change
        [
            [
                pd.Timestamp("2017-12-01 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2020-02-01 00:00:00-0500", tz="US/Eastern"),
            ],
        ],
    )
    def test_pjm_update_dates_cross_multiple_years(self, dates: list[pd.Timestamp]):
        args_dict = {
            "self": gridstatus.PJM(),
            "market": Markets.REAL_TIME_5_MIN,
        }

        with pjm_vcr.use_cassette(
            f"test_pjm_update_dates_cross_multiple_years_{dates[0].strftime('%Y-%m-%d')}.yaml",
        ):
            new_dates = gridstatus.pjm.pjm_update_dates(dates, args_dict)
            assert new_dates == [
                pd.Timestamp("2017-12-01 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2017-12-31 23:59:00-0500", tz="US/Eastern"),
                None,
                pd.Timestamp("2018-01-01 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2018-12-31 23:59:00-0500", tz="US/Eastern"),
                None,
                pd.Timestamp("2019-01-01 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2019-12-31 23:59:00-0500", tz="US/Eastern"),
                None,
                pd.Timestamp("2020-01-01 00:00:00-0500", tz="US/Eastern"),
                pd.Timestamp("2020-02-01 00:00:00-0500", tz="US/Eastern"),
            ]

    def test_pjm_update_dates_cross_archive_date(self):
        args_dict = {
            "self": gridstatus.PJM(),
            "market": Markets.REAL_TIME_5_MIN,
        }
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
        with pjm_vcr.use_cassette(
            f"test_pjm_update_dates_cross_archive_date_{start.strftime('%Y-%m-%d')}.yaml",
        ):
            assert new_dates == [
                start,
                before_archive,
                None,
                archive_date,
                end,
            ]

    def sample_forecast_data(self, request):
        with pjm_vcr.use_cassette(f"test_sample_forecast_data_{request.param}.yaml"):
            filename = request.param
            current_dir = Path(__file__).parent.parent
            file_path = os.path.join(current_dir, "fixtures", "pjm", filename)
            with open(file_path, "r") as f:
                return json.load(f)

    @pytest.mark.parametrize(
        "endpoint",
        [("five_min_solar_power_forecast")],
    )
    def test_get_pjm_json(self, endpoint):
        start = self.local_start_of_today() - pd.Timedelta(days=1)
        end = start + pd.Timedelta(days=1)
        with pjm_vcr.use_cassette(
            f"test_get_pjm_json_{endpoint}_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            result = self.iso._get_pjm_json(
                endpoint=endpoint,
                start=start,
                params={
                    "fields": "datetime_beginning_ept,datetime_beginning_utc,datetime_ending_ept,datetime_ending_utc,evaluated_at_ept,evaluated_at_utc,solar_forecast_btm_mwh,solar_forecast_mwh",
                },
                end=end,
                filter_timestamp_name="evaluated_at",
                interval_duration_min=5,
                verbose=False,
            )

            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            actual_columns = set(result.columns)
            expected_dt_columns = ["Interval Start", "Interval End", "Publish Time"]
            for col in set(expected_dt_columns) & set(actual_columns):
                assert isinstance(
                    result[col].dtype,
                    pd.DatetimeTZDtype,
                ), f"{col} is not a timezone-aware datetime column"
                assert str(result[col].dt.tz) == str(
                    self.iso.default_timezone,
                ), f"{col} timezone doesn't match the default timezone"

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

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_solar_forecast_hourly_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_solar_forecast_hourly_{date}.yaml"):
            df = self.iso.get_solar_forecast_hourly(date)

            self._check_solar_forecast(df)
            assert df["Interval Start"].min() == self.local_start_of_today()
            assert df[
                "Interval End"
            ].max() >= self.local_start_of_today() + pd.Timedelta(
                days=2,
            )
            assert (
                df["Publish Time"].dt.tz_convert(self.iso.default_timezone).dt.date
                == self.local_today()
            ).all()

    def test_get_solar_forecast_hourly_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=12)
        past_end_date = past_date + pd.Timedelta(days=3)
        with pjm_vcr.use_cassette(
            f"test_get_solar_forecast_hourly_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_solar_forecast_hourly(past_date, past_end_date)
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            self._check_solar_forecast(df)

    def test_get_solar_forecast_hourly_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        with pjm_vcr.use_cassette(
            f"test_get_solar_forecast_hourly_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_solar_forecast_hourly(past_date)
            self._check_solar_forecast(df)

            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() >= self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(days=2)

            assert df["Publish Time"].min() == self.local_start_of_day(past_date)
            # NB: When end date is generated this data
            # doesn't include forecast on the next day
            assert df["Publish Time"].max() < self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(days=1)

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_solar_forecast_5_min_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_solar_forecast_5_min_{date}.yaml"):
            df = self.iso.get_solar_forecast_5_min(date)
            self._check_solar_forecast(df)
            assert df["Interval Start"].min() == self.local_start_of_today()
            assert df["Interval End"].max() >= pd.Timestamp.now(
                tz=self.iso.default_timezone,
            ) + pd.Timedelta(hours=2)

    def test_get_solar_forecast_5_min_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        with pjm_vcr.use_cassette(
            f"test_get_solar_forecast_5_min_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_solar_forecast_5_min(past_date)

            self._check_solar_forecast(df)

            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() >= self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(hours=3)

            assert df["Publish Time"].min() == self.local_start_of_day(past_date)
            # NB: When end date is generated this data
            # doesn't include forecast on the next day
            assert df["Publish Time"].max() < self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(days=1)

    def test_get_solar_forecast_5_min_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=12)
        past_end_date = past_date + pd.Timedelta(days=3)
        with pjm_vcr.use_cassette(
            f"test_get_solar_forecast_5_min_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_solar_forecast_5_min(past_date, past_end_date)

            self._check_solar_forecast(df)
            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() >= self.local_start_of_day(
                past_end_date,
            ) + pd.Timedelta(hours=3)

            assert df["Publish Time"].min() == self.local_start_of_day(past_date)
            # NB: This data also includes one forecast time on the next day
            assert df["Publish Time"].max() == self.local_start_of_day(past_end_date)

    """get_wind_forecast tests"""

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

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_wind_forecast_hourly_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_wind_forecast_hourly_{date}.yaml"):
            df = self.iso.get_wind_forecast_hourly(date)

            self._check_wind_forecast(df)
            # NB: For some reason, the start of the forecast is 5 hours after the day start
            assert df[
                "Interval Start"
            ].min() == self.local_start_of_today() + pd.Timedelta(
                hours=5,
            )
            assert df[
                "Interval End"
            ].max() >= self.local_start_of_today() + pd.Timedelta(
                days=2,
                hours=5,
            )

            assert (
                df["Publish Time"].dt.tz_convert(self.iso.default_timezone).dt.date
                == self.local_today()
            ).all()

            assert self.iso.get_wind_forecast_hourly("latest").equals(df)

    def test_get_wind_forecast_hourly_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=12)
        past_end_date = past_date + pd.Timedelta(days=3)
        with pjm_vcr.use_cassette(
            f"test_get_wind_forecast_hourly_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_wind_forecast_hourly(past_date, past_end_date)
            self._check_wind_forecast(df)

            assert df["Interval Start"].min() == self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(hours=5)
            assert df["Interval End"].max() >= self.local_start_of_day(
                past_end_date,
            ) + pd.Timedelta(days=2)

            assert df["Publish Time"].min() == self.local_start_of_day(past_date)
            # NB: This data also includes one forecast time on the next day
            assert df["Publish Time"].max() == self.local_start_of_day(past_end_date)

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_wind_forecast_5_min_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_wind_forecast_5_min_{date}.yaml"):
            df = self.iso.get_wind_forecast_5_min(date)
            self._check_wind_forecast(df)
            assert df["Interval Start"].min() == self.local_start_of_today()
            assert df[
                "Interval End"
            ].max() >= self.local_start_of_today() + pd.Timedelta(
                hours=6,
            )

            assert (
                df["Publish Time"].dt.tz_convert(self.iso.default_timezone).dt.date
                == self.local_today()
            ).all()

            assert self.iso.get_wind_forecast_5_min("latest").equals(df)

    def test_get_wind_forecast_5_min_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        with pjm_vcr.use_cassette(
            f"test_get_wind_forecast_5_min_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_wind_forecast_5_min(past_date)
            self._check_wind_forecast(df)

            assert df["Interval Start"].min() == self.local_start_of_day(
                past_date,
            )
            assert df["Interval End"].max() >= self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(hours=6)

            assert df["Publish Time"].min() == self.local_start_of_day(past_date)
            # NB: When end date is generated this data
            # doesn't include forecast on the next day
            assert df["Publish Time"].max() < self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(days=1)

    def test_get_wind_forecast_5_min_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=12)
        past_end_date = past_date + pd.Timedelta(days=3)
        with pjm_vcr.use_cassette(
            f"test_get_wind_forecast_5_min_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_wind_forecast_5_min(past_date, past_end_date)
            self._check_wind_forecast(df)

            assert df["Interval Start"].min() == self.local_start_of_day(
                past_date,
            )

            assert df["Interval End"].max() >= self.local_start_of_day(
                past_end_date,
            ) + pd.Timedelta(hours=4)

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

        # Test spanning the archive date
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

    @pytest.mark.parametrize("date", ["latest", "today"])
    def test_get_gen_outages_by_type_with_latest(self, date):
        start_date_local = self.local_today()
        with pjm_vcr.use_cassette(f"test_get_gen_outages_by_type_{date}.yaml"):
            df = self.iso.get_gen_outages_by_type(date)
            self._check_gen_outages_by_type(df)

            expected_date = self.to_local_datetime(start_date_local)
            assert (df["Publish Time"] == expected_date).all()
            assert (
                df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
            ).all()

    def test_get_gen_outages_by_type_with_past_date(self):
        start_date_local = self.local_today() - pd.DateOffset(days=3)
        start_date_time_local = self.local_start_of_day(start_date_local)
        with pjm_vcr.use_cassette(
            f"test_get_gen_outages_by_type_{start_date_time_local.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_gen_outages_by_type(start_date_time_local)
            self._check_gen_outages_by_type(df)

            expected_date = self.to_local_datetime(start_date_local)
            assert (df["Publish Time"] == expected_date).all()
            assert (
                df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
            ).all()

    @pytest.mark.parametrize(
        "date, end",
        [
            (
                pd.Timestamp("2024-04-30 00:00:00-04:00"),
                pd.Timestamp("2024-05-01 23:59:59-04:00"),
            ),
        ],
    )
    def test_get_gen_outages_by_type_with_multi_day_range(self, date, end):
        expected_date_1 = "2024-04-30 00:00:00-04:00"
        expected_date_2 = "2024-05-01 00:00:00-04:00"
        expected_dates = {expected_date_1, expected_date_2}

        with pjm_vcr.use_cassette(
            f"test_get_gen_outages_by_type_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_gen_outages_by_type(
                date,
                end,
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

    @pytest.mark.parametrize("date", ["today"])
    def test_projected_rto_statistics_at_peak_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_projected_rto_statistics_at_peak_{date}.yaml"):
            df = self.iso.get_projected_rto_statistics_at_peak(date)

            self._check_projected_rto_statistics_at_peak(df)

            assert df["Interval Start"].min() == self.local_start_of_today()
            assert df[
                "Interval End"
            ].max() >= self.local_start_of_today() + pd.DateOffset(
                days=1,
            )

    def test_projected_rto_statistics_at_peak_historical_date(self):
        past_date = self.local_today() - pd.DateOffset(days=10)

        with pjm_vcr.use_cassette(
            f"test_projected_rto_statistics_at_peak_{pd.Timestamp(past_date).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_projected_rto_statistics_at_peak(past_date)

            self._check_projected_rto_statistics_at_peak(df)
            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                past_date,
            ) + pd.DateOffset(days=1)

    @pytest.mark.parametrize("date", test_dates)
    def test_projected_rto_statistics_at_peak_historical_date_range(self, date):
        with pjm_vcr.use_cassette(
            f"test_projected_rto_statistics_at_peak_{pd.Timestamp(date[0]).strftime('%Y-%m-%d')}_{pd.Timestamp(date[1]).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_projected_rto_statistics_at_peak(date)

            self._check_projected_rto_statistics_at_peak(df)
            assert df["Interval Start"].min() == self.local_start_of_day(date[0])
            assert df["Interval End"].max() == self.local_start_of_day(date[1])

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

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_projected_area_statistics_at_peak_today_or_latest(self, date):
        with pjm_vcr.use_cassette(
            f"test_projected_area_statistics_at_peak_{date}.yaml",
        ):
            df = self.iso.get_projected_area_statistics_at_peak(date)

            self._check_projected_area_statistics_at_peak(df)

            assert df["Interval Start"].min() == self.local_start_of_today()
            assert df[
                "Interval End"
            ].max() == self.local_start_of_today() + pd.DateOffset(
                days=1,
            )

    @pytest.mark.parametrize("date", test_dates)
    def test_projected_area_statistics_at_peak_historical_date(self, date):
        with pjm_vcr.use_cassette(
            f"test_projected_area_statistics_at_peak_{pd.Timestamp(date[0]).strftime('%Y-%m-%d')}_{pd.Timestamp(date[1]).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_projected_area_statistics_at_peak(date)

            self._check_projected_area_statistics_at_peak(df)
            assert df["Interval Start"].min() == self.local_start_of_day(date[0])
            assert df["Interval End"].max() == self.local_start_of_day(
                date[1],
            )

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

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_solar_generation_5_min_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_solar_generation_5_min_{date}.yaml"):
            df = self.iso.get_solar_generation_5_min(date)

            range_start = self.local_start_of_today()
            range_end = self.local_start_of_today() + pd.Timedelta(days=1)
            self._check_pjm_response(
                df=df,
                expected_cols=self.expected_five_min_solar_gen_cols,
                start=range_start,
                end=range_end,
            )

    def test_get_solar_generation_5_min_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)
        with pjm_vcr.use_cassette(
            f"test_get_solar_generation_5_min_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
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
        with pjm_vcr.use_cassette(
            f"test_get_solar_generation_5_min_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_wind_generation_instantaneous_today_or_latest(self, date):
        range_start = self.local_start_of_today()
        range_end = self.local_start_of_today() + pd.Timedelta(days=1)
        with pjm_vcr.use_cassette(
            f"test_get_wind_generation_instantaneous_{date}.yaml",
        ):
            df = self.iso.get_wind_generation_instantaneous(date)

            self._check_pjm_response(
                df=df,
                expected_cols=self.expected_wind_gen_cols,
                start=range_start,
                end=range_end,
            )

    def test_get_wind_generation_instantaneous_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)
        with pjm_vcr.use_cassette(
            f"test_get_wind_generation_instantaneous_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
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
        with pjm_vcr.use_cassette(
            f"test_get_wind_generation_instantaneous_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_operational_reserves_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_operational_reserves_{date}.yaml"):
            df = self.iso.get_operational_reserves(date)
            range_start = self.local_start_of_today()
            range_end = self.local_start_of_today() + pd.Timedelta(days=1)

            self._check_pjm_response(
                df=df,
                expected_cols=self.expected_operational_reserves_cols,
                start=range_start,
                end=range_end,
            )

    def test_get_operational_reserves_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        with pjm_vcr.use_cassette(
            f"test_get_operational_reserves_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

        with pjm_vcr.use_cassette(
            f"test_get_operational_reserves_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_transfer_interface_information_5_min_today_or_latest(self, date):
        with pjm_vcr.use_cassette(
            f"test_get_transfer_interface_information_5_min_{date}.yaml",
        ):
            df = self.iso.get_transfer_interface_information_5_min(date)
            range_start = self.local_start_of_today()
            range_end = self.local_start_of_today() + pd.Timedelta(days=1)

            self._check_pjm_response(
                df=df,
                expected_cols=self.expected_transfer_interface_info_cols,
                start=range_start,
                end=range_end,
            )

    def test_get_transfer_interface_information_5_min_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        with pjm_vcr.use_cassette(
            f"test_get_transfer_interface_information_5_min_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

        with pjm_vcr.use_cassette(
            f"test_get_transfer_interface_information_5_min_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_transfer_interface_information_5_min(
                past_date,
                past_end_date,
            )

            self._check_pjm_response(
                df=df,
                expected_cols=self.expected_transfer_interface_info_cols,
                start=range_start,
                end=range_end,
            )

    """get_transmission_limits"""

    expected_transmission_limits_cols = [
        "Interval Start",
        "Interval End",
        "Constraint Name",
        "Constraint Type",
        "Contingency",
        "Shadow Price",
    ]

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_transmission_limits_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_transmission_limits_{date}.yaml"):
            df = self.iso.get_transmission_limits(date)
            range_start = self.local_start_of_today()
            range_end = self.local_start_of_today() + pd.Timedelta(days=1)

            self._check_pjm_response(
                df=df,
                expected_cols=self.expected_transmission_limits_cols,
                start=range_start,
                end=range_end,
            )

    def test_get_transmission_limits_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        with pjm_vcr.use_cassette(
            f"test_get_transmission_limits_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

        with pjm_vcr.use_cassette(
            f"test_get_transmission_limits_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_solar_generation_by_area_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_solar_generation_by_area_{date}.yaml"):
            df = self.iso.get_solar_generation_by_area(date)
            range_start = self.local_start_of_today()
            range_end = self.local_start_of_today() + pd.Timedelta(days=1)

            self._check_pjm_response(
                df=df,
                expected_cols=self.expected_solar_wind_gen_by_area_cols,
                start=range_start,
                end=range_end,
            )

    def test_get_solar_generation_by_area_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        with pjm_vcr.use_cassette(
            f"test_get_solar_generation_by_area_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

        with pjm_vcr.use_cassette(
            f"test_get_solar_generation_by_area_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_solar_generation_by_area(past_date, past_end_date)

        self._check_pjm_response(
            df=df,
            expected_cols=self.expected_solar_wind_gen_by_area_cols,
            start=range_start,
            end=range_end,
        )

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_wind_generation_by_area_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_wind_generation_by_area_{date}.yaml"):
            df = self.iso.get_wind_generation_by_area(date)
            range_start = self.local_start_of_today()
            range_end = self.local_start_of_today() + pd.Timedelta(days=1)

            self._check_pjm_response(
                df=df,
                expected_cols=self.expected_solar_wind_gen_by_area_cols,
                start=range_start,
                end=range_end,
            )

    def test_get_wind_generation_by_area_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)

        with pjm_vcr.use_cassette(
            f"test_get_wind_generation_by_area_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

        with pjm_vcr.use_cassette(
            f"test_get_wind_generation_by_area_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_dam_as_market_results_today_or_latest(self, date):
        range_start = self.local_start_of_today()
        range_end = self.local_start_of_today() + pd.Timedelta(days=1)
        with pjm_vcr.use_cassette(f"test_get_dam_as_market_results_{date}.yaml"):
            df = self.iso.get_dam_as_market_results(date)

            self._check_pjm_response(
                df=df,
                expected_cols=self.expected_dam_as_market_results_cols,
                start=range_start,
                end=range_end,
            )

    def test_get_dam_as_market_results_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        range_start = self.local_start_of_day(past_date)
        range_end = self.local_start_of_day(past_date) + pd.Timedelta(days=1)
        with pjm_vcr.use_cassette(
            f"test_get_dam_as_market_results_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

        with pjm_vcr.use_cassette(
            f"test_get_dam_as_market_results_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

        with pjm_vcr.use_cassette(
            f"test_get_dam_as_market_results_parsing_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

        with pjm_vcr.use_cassette(
            f"test_get_real_time_as_market_results_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

        with pjm_vcr.use_cassette(
            f"test_get_real_time_as_market_results_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

        with pjm_vcr.use_cassette(
            f"test_get_real_time_as_market_results_parsing_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

    @pytest.mark.parametrize(
        "start, end",
        [
            (
                pd.Timestamp("2022-09-01") - pd.Timedelta(days=5),
                pd.Timestamp("2022-09-01") - pd.Timedelta(days=3),
            ),
        ],
    )
    def test_get_real_time_as_market_results_valid_dates_before_cutoff(
        self,
        start,
        end,
    ):
        with pjm_vcr.use_cassette(
            f"test_get_real_time_as_market_results_valid_dates_before_cutoff_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_real_time_as_market_results(date=start, end=end)
            interval_start = df.iloc[0, :]["Interval Start"]
            interval_end = df.iloc[0, :]["Interval End"]
            assert interval_end - interval_start == pd.Timedelta(hours=1)

    @pytest.mark.parametrize(
        "start, end",
        [
            (
                pd.Timestamp("2022-09-01") + pd.Timedelta(days=3),
                pd.Timestamp("2022-09-01") + pd.Timedelta(days=5),
            ),
        ],
    )
    def test_get_real_time_as_market_results_valid_dates_after_cutoff(self, start, end):
        with pjm_vcr.use_cassette(
            f"test_get_real_time_as_market_results_valid_dates_after_cutoff_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_real_time_as_market_results(date=start, end=end)
            interval_start = df.iloc[0, :]["Interval Start"]
            interval_end = df.iloc[0, :]["Interval End"]
            assert interval_end - interval_start == pd.Timedelta(minutes=5)

    @pytest.mark.parametrize(
        "start, end",
        [
            (
                pd.Timestamp("2022-09-01") - pd.Timedelta(days=5),
                pd.Timestamp("2022-09-01") + pd.Timedelta(days=3),
            ),
        ],
    )
    def test_get_real_time_as_market_results_invalid_dates(self, start, end):
        with pjm_vcr.use_cassette(
            f"test_get_real_time_as_market_results_invalid_dates_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            with pytest.raises(
                ValueError,
                match="Both start and end dates must be before",
            ):
                self.iso.get_real_time_as_market_results(
                    date=start,
                    end=end,
                    error="raise",
                )

    def test_get_interconnection_queue(self):
        from gridstatus.base import _interconnection_columns

        with pjm_vcr.use_cassette("test_get_interconnection_queue.yaml"):
            queue = self.iso.get_interconnection_queue()
            # TODO: make sure datetime columns are right type
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
        with pjm_vcr.use_cassette(
            f"test_get_load_metered_hourly_historical_date_{date.strftime('%Y-%m-%d')}.yaml",
        ):
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
        with pjm_vcr.use_cassette(
            f"test_get_load_metered_hourly_historical_date_range_{date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.yaml",
        ):
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

    @pytest.mark.parametrize("date", ["today", "latest"])
    def test_get_forecasted_generation_outages_today_or_latest(self, date):
        start_date_local = self.local_today()
        expected_date = self.to_local_datetime(start_date_local)

        with pjm_vcr.use_cassette(
            f"test_get_forecasted_generation_outages_{date}.yaml",
        ):
            df = self.iso.get_forecasted_generation_outages(date)
            self._check_forecasted_gen_outages(df)
            assert (df["Publish Time"] == expected_date).all()
            assert (
                df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
            ).all()

            assert self.iso.get_forecasted_generation_outages("latest").equals(df)

    def test_get_forecasted_generation_outages_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        with pjm_vcr.use_cassette(
            f"test_get_forecasted_generation_outages_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_forecasted_generation_outages(past_date)
            self._check_forecasted_gen_outages(df)
            expected_date = self.to_local_datetime(past_date)

            assert (df["Publish Time"] == expected_date).all()
            assert (
                df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
            ).all()

    @pytest.mark.parametrize(
        "date, end",
        [("2024-04-30 00:00:00-04:00", "2024-05-01 23:59:59-04:00")],
    )
    def test_get_forecasted_generation_outages_historical_range(self, date, end):
        expected_dates = {
            pd.Timestamp(date),
            pd.Timestamp(date) + pd.DateOffset(days=1),
        }

        with pjm_vcr.use_cassette(
            f"test_get_forecasted_generation_outages_historical_range_{pd.Timestamp(date).strftime('%Y-%m-%d')}_{pd.Timestamp(end).strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_forecasted_generation_outages(
                date,
                end,
            )
            self._check_forecasted_gen_outages(df)
            assert (df["Publish Time"].isin(expected_dates)).all()
            assert (
                df["Interval End"] == df["Interval Start"] + pd.DateOffset(days=1)
            ).all()

    @pytest.mark.parametrize(
        "date,end",
        test_dates,
    )
    def test_get_marginal_value_real_time_5_min(self, date, end):
        cassette_name = f"test_get_marginal_value_real_time_5_min_{date}_{end}.yaml"
        with pjm_vcr.use_cassette(cassette_name):
            result = self.iso.get_marginal_value_real_time_5_min(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Monitored Facility",
                "Contingency Facility",
                "Transmission Constraint Penalty Factor",
                "Limit Control Percentage",
                "Shadow Price",
            ]

            assert min(result["Interval Start"]).date() == pd.Timestamp(date).date()
            assert max(result["Interval End"]).date() <= pd.Timestamp(end).date()
            assert result["Monitored Facility"].dtype == object
            assert result["Contingency Facility"].dtype == object
            assert result["Shadow Price"].dtype in [np.int64, np.float64]
            assert result["Transmission Constraint Penalty Factor"].dtype in [
                np.int64,
                np.float64,
            ]
            assert result["Limit Control Percentage"].dtype in [np.int64, np.float64]

    @pytest.mark.parametrize(
        "date,end",
        test_dates,
    )
    def test_get_marginal_value_day_ahead_hourly(self, date, end):
        cassette_name = f"test_get_marginal_value_day_ahead_hourly_{date}_{end}.yaml"
        with pjm_vcr.use_cassette(cassette_name):
            result = self.iso.get_marginal_value_day_ahead_hourly(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Monitored Facility",
                "Contingency Facility",
                "Shadow Price",
            ]

            assert min(result["Interval Start"]).date() == pd.Timestamp(date).date()
            assert max(result["Interval End"]).date() <= pd.Timestamp(end).date()
            assert result["Monitored Facility"].dtype == object
            assert result["Contingency Facility"].dtype == object
            assert result["Shadow Price"].dtype in [np.int64, np.float64]

    @pytest.mark.parametrize(
        "date,end",
        test_dates,
    )
    def test_get_transmission_constraints_day_ahead_hourly(self, date, end):
        cassette_name = (
            f"test_get_transmission_constraints_day_ahead_hourly_{date}_{end}.yaml"
        )
        with pjm_vcr.use_cassette(cassette_name):
            result = self.iso.get_transmission_constraints_day_ahead_hourly(
                date=date,
                end=end,
            )

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Duration",
                "Day Ahead Congestion Event",
                "Monitored Facility",
                "Contingency Facility",
            ]

            assert min(result["Interval Start"]).date() == pd.Timestamp(date).date()
            assert max(result["Interval End"]).date() <= pd.Timestamp(end).date()
            assert result["Day Ahead Congestion Event"].dtype == object
            assert result["Monitored Facility"].dtype == object
            assert result["Contingency Facility"].dtype == object
            assert result["Duration"].dtype in [np.int64, np.float64]

    def test_get_settlements_verified_lmp_5_min_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=30)
        end = start + pd.Timedelta(hours=4)

        with pjm_vcr.use_cassette(
            f"test_get_settlements_verified_lmp_5_min_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_settlements_verified_lmp_5_min(start=start, end=end)

            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location Id",
                "Location Name",
                "Location Type",
                "Voltage",
                "Equipment",
                "Zone",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end
            assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
                minutes=5,
            )

    def test_get_settlements_verified_lmp_hourly_date_range(self):
        # Data is only available more than 30 days in the past
        start = self.local_start_of_today() - pd.DateOffset(days=32)
        end = start + pd.Timedelta(hours=4)
        with pjm_vcr.use_cassette(
            f"test_get_settlements_verified_lmp_hourly_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_settlements_verified_lmp_hourly(start=start, end=end)

            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location Id",
                "Location Name",
                "Location Type",
                "Voltage",
                "Equipment",
                "Zone",
                "LMP RT",
                "Energy RT",
                "Congestion RT",
                "Loss RT",
                "LMP DA",
                "Energy DA",
                "Congestion DA",
                "Loss DA",
            ]

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end
            assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
                hours=1,
            )

    def test_get_day_ahead_demand_bids_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=30)
        end = start + pd.Timedelta(hours=4)

        with pjm_vcr.use_cassette(
            f"test_get_day_ahead_demand_bids_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_day_ahead_demand_bids(start=start, end=end)

            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Area",
                "Demand Bid",
            ]

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end
            assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
                hours=1,
            )

    def test_get_area_control_error_date_range(self):
        date = self.local_start_of_today() - pd.Timedelta(days=20)
        end = date + pd.Timedelta(days=2)
        """Test getting area control error data for a date range"""
        with pjm_vcr.use_cassette(
            f"test_get_area_control_error_{date.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_area_control_error(date=date, end=end)

            assert isinstance(df, pd.DataFrame)
            assert df.columns.tolist() == [
                "Time",
                "Area Control Error",
            ]

            assert df["Area Control Error"].dtype in [np.float64, np.int64]
            assert df["Time"].min().date() == pd.Timestamp(date).date()

    @pytest.mark.parametrize(
        "date",
        [
            "today",
            "latest",
        ],
    )
    def test_get_area_control_error_today_or_latest(self, date: str):
        """Test getting area control error data for today and latest"""
        with pjm_vcr.use_cassette(f"test_get_area_control_error_{date}.yaml"):
            df = self.iso.get_area_control_error(date)

            assert isinstance(df, pd.DataFrame)
            assert df.columns.tolist() == [
                "Time",
                "Area Control Error",
            ]

            assert df["Area Control Error"].dtype in [np.float64, np.int64]
            assert df["Time"].min() <= self.local_start_of_today() + pd.Timedelta(
                seconds=15,
            )
            assert df["Time"].max() <= self.local_start_of_today() + pd.Timedelta(
                days=1,
            )

    """get_dispatched_reserves_prelim"""

    expected_dispatched_reserves_prelim_cols = [
        "Interval Start",
        "Interval End",
        "Ancillary Service",
        "Area",
        "Reserve Type",
        "Reserve Quantity",
        "Reserve Requirement",
        "Reliability Requirement",
        "Extended Requirement",
        "MW Adjustment",
        "Market Clearing Price",
        "Shortage Indicator",
    ]

    expected_reserve_areas = [
        "PJM RTO Reserve Zone",
        "Mid-Atlantic/Dominion Reserve Subzone",
    ]
    expected_ancillary_services = [
        "MAD-Primary Reserve",
        "MAD-Synchronized Reserve",
        "PJM_RTO-Primary Reserve",
        "PJM_RTO-Synchronized Reserve",
        "PJM_RTO-Thirty-Minute Reserve",
    ]

    @pytest.mark.parametrize(
        "date",
        [
            "today",
            "latest",
        ],
    )
    def test_get_dispatched_reserves_prelim_today_or_latest(self, date):
        with pjm_vcr.use_cassette(
            f"test_get_dispatched_reserves_prelim_today_or_latest_{date}.yaml",
        ):
            df = self.iso.get_dispatched_reserves_prelim(date)

            assert isinstance(df, pd.DataFrame)
            assert df.columns.tolist() == self.expected_dispatched_reserves_prelim_cols

            assert df["Interval Start"].min() >= self.local_start_of_today()
            assert df[
                "Interval End"
            ].max() <= self.local_start_of_today() + pd.Timedelta(days=1)

            assert df["Ancillary Service"].dtype == object
            assert df["Area"].dtype == object
            assert (
                df["Area"].unique().tolist().sort()
                == self.expected_reserve_areas.sort()
            )
            assert (
                df["Ancillary Service"].unique().tolist().sort()
                == self.expected_ancillary_services.sort()
            )
            assert df["Reserve Type"].dtype == object
            assert df["Reserve Quantity"].dtype in [np.float64, np.int64]
            assert df["Reserve Requirement"].dtype in [np.float64, np.int64]
            assert df["Reliability Requirement"].dtype in [np.float64, np.int64]
            assert df["Extended Requirement"].dtype in [np.float64, np.int64]
            assert df["MW Adjustment"].dtype in [np.float64, np.int64]
            assert df["Market Clearing Price"].dtype in [np.float64, np.int64]
            assert df["Shortage Indicator"].dtype in [bool]

    def test_get_dispatched_reserves_prelim_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)
        with pjm_vcr.use_cassette(
            f"test_get_dispatched_reserves_prelim_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_dispatched_reserves_prelim(past_date, past_end_date)

            assert isinstance(df, pd.DataFrame)
            assert df.columns.tolist() == self.expected_dispatched_reserves_prelim_cols

            assert df["Interval Start"].min() >= self.local_start_of_day(past_date)
            assert df["Interval End"].max() <= self.local_start_of_day(
                past_end_date,
            ) + pd.Timedelta(days=1)

            assert df["Ancillary Service"].dtype == object
            assert df["Area"].dtype == object
            assert (
                df["Area"].unique().tolist().sort()
                == self.expected_reserve_areas.sort()
            )
            assert (
                df["Ancillary Service"].unique().tolist().sort()
                == self.expected_ancillary_services.sort()
            )
            assert df["Reserve Type"].dtype == object
            assert df["Reserve Quantity"].dtype in [np.float64, np.int64]
            assert df["Reserve Requirement"].dtype in [np.float64, np.int64]
            assert df["Reliability Requirement"].dtype in [np.float64, np.int64]
            assert df["Extended Requirement"].dtype in [np.float64, np.int64]
            assert df["MW Adjustment"].dtype in [np.float64, np.int64]
            assert df["Market Clearing Price"].dtype in [np.float64, np.int64]
            assert df["Shortage Indicator"].dtype in [bool]

    """get_dispatched_reserves_verified"""

    expected_dispatched_reserves_verified_cols = [
        "Interval Start",
        "Interval End",
        "Ancillary Service",
        "Area",
        "Reserve Type",
        "Total Reserve",
        "Reserve Requirement",
        "Reliability Requirement",
        "Extended Requirement",
        "Additional Extended Requirement",
        "Deficit",
    ]

    @pytest.mark.parametrize(
        "date",
        [
            "latest",
        ],
    )
    def test_get_dispatched_reserves_verified_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_dispatched_reserves_verified_{date}.yaml"):
            df = self.iso.get_dispatched_reserves_verified(date)

            assert isinstance(df, pd.DataFrame)
            assert (
                df.columns.tolist() == self.expected_dispatched_reserves_verified_cols
            )

            assert df[
                "Interval Start"
            ].min() >= self.local_start_of_today() - pd.Timedelta(days=1)
            assert df["Interval End"].max() <= self.local_start_of_today()

            assert df["Ancillary Service"].dtype == object
            assert df["Area"].dtype == object
            assert (
                df["Area"].unique().tolist().sort()
                == self.expected_reserve_areas.sort()
            )
            assert (
                df["Ancillary Service"].unique().tolist().sort()
                == self.expected_ancillary_services.sort()
            )
            assert df["Reserve Type"].dtype == object
            assert df["Total Reserve"].dtype in [np.float64, np.int64]
            assert df["Reserve Requirement"].dtype in [np.float64, np.int64]
            assert df["Reliability Requirement"].dtype in [np.float64, np.int64]
            assert df["Extended Requirement"].dtype in [np.float64, np.int64]
            assert df["Additional Extended Requirement"].dtype in [np.float64, np.int64]
            assert df["Deficit"].dtype in [np.float64, np.int64]

    def test_get_dispatched_reserves_verified_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)
        with pjm_vcr.use_cassette(
            f"test_get_dispatched_reserves_verified_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_dispatched_reserves_verified(past_date, past_end_date)

            assert isinstance(df, pd.DataFrame)
            assert (
                df.columns.tolist() == self.expected_dispatched_reserves_verified_cols
            )

            assert df["Interval Start"].min() >= self.local_start_of_day(past_date)
            assert df["Interval End"].max() <= self.local_start_of_day(
                past_end_date,
            ) + pd.Timedelta(days=1)

            assert df["Ancillary Service"].dtype == object
            assert df["Area"].dtype == object
            assert (
                df["Area"].unique().tolist().sort()
                == self.expected_reserve_areas.sort()
            )
            assert (
                df["Ancillary Service"].unique().tolist().sort()
                == self.expected_ancillary_services.sort()
            )
            assert df["Reserve Type"].dtype == object
            assert df["Total Reserve"].dtype in [np.float64, np.int64]
            assert df["Reserve Requirement"].dtype in [np.float64, np.int64]
            assert df["Reliability Requirement"].dtype in [np.float64, np.int64]
            assert df["Extended Requirement"].dtype in [np.float64, np.int64]
            assert df["Additional Extended Requirement"].dtype in [np.float64, np.int64]
            assert df["Deficit"].dtype in [np.float64, np.int64]

    expected_regulation_market_monthly_cols = [
        "Interval Start",
        "Interval End",
        "Requirement",
        "RegD SSMW",
        "RegA SSMW",
        "RegD Procure",
        "RegA Procure",
        "Total MW",
        "Deficiency",
        "RTO Perfscore",
        "RegA Mileage",
        "RegD Mileage",
        "RegA Hourly",
        "RegD Hourly",
        "Is Approved",
        "Modified Datetime UTC",
    ]

    def test_get_regulation_market_monthly_latest(self):
        with pjm_vcr.use_cassette("test_get_regulation_market_monthly_latest.yaml"):
            df = self.iso.get_regulation_market_monthly("latest")

            assert isinstance(df, pd.DataFrame)
            assert df.columns.tolist() == self.expected_regulation_market_monthly_cols

            numeric_cols = [
                "Requirement",
                "RegD SSMW",
                "RegA SSMW",
                "RegD Procure",
                "RegA Procure",
                "Total MW",
                "Deficiency",
                "RTO Perfscore",
                "RegA Mileage",
                "RegD Mileage",
                "RegA Hourly",
                "RegD Hourly",
                "Is Approved",
            ]
            for col in numeric_cols:
                assert df[col].dtype in [np.float64, np.int64]

    def test_get_regulation_market_monthly_historical_range(self):
        past_date = self.local_today() - pd.DateOffset(months=4)
        past_end_date = past_date + pd.DateOffset(months=2)

        with pjm_vcr.use_cassette(
            f"test_get_regulation_market_monthly_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_regulation_market_monthly(past_date, past_end_date)

            assert isinstance(df, pd.DataFrame)
            assert df.columns.tolist() == self.expected_regulation_market_monthly_cols

            assert df["Interval Start"].min() >= self.local_start_of_day(past_date)
            assert df["Interval End"].max() <= self.local_start_of_day(past_end_date)

            numeric_cols = [
                "Requirement",
                "RegD SSMW",
                "RegA SSMW",
                "RegD Procure",
                "RegA Procure",
                "Total MW",
                "Deficiency",
                "RTO Perfscore",
                "RegA Mileage",
                "RegD Mileage",
                "RegA Hourly",
                "RegD Hourly",
                "Is Approved",
            ]
            for col in numeric_cols:
                assert df[col].dtype in [np.float64, np.int64]

    expected_lmp_real_time_unverified_hourly_cols = [
        "Interval Start",
        "Interval End",
        "Location",
        "Location Type",
        "LMP",
        "Energy",
        "Congestion",
        "Loss",
    ]

    def _check_lmp_real_time_unverified_hourly(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == self.expected_lmp_real_time_unverified_hourly_cols
        numeric_cols = ["LMP", "Energy", "Congestion", "Loss"]
        for col in numeric_cols:
            assert df[col].dtype in [np.float64, np.int64]

    @pytest.mark.parametrize("date", ["latest", "today"])
    def test_get_lmp_real_time_unverified_hourly_latest(self, date):
        with pjm_vcr.use_cassette(
            f"test_get_lmp_real_time_unverified_hourly_{date}.yaml",
        ):
            df = self.iso.get_lmp_real_time_unverified_hourly(date)

            self._check_lmp_real_time_unverified_hourly(df)
            assert df["Interval Start"].min() == self.local_start_of_day("today")
            assert df["Interval End"].max() <= self.local_start_of_day(
                "today",
            ) + pd.Timedelta(hours=24)

    def test_get_lmp_real_time_unverified_hourly_historical_range(self):
        past_date = self.local_today() - pd.DateOffset(days=4)
        past_end_date = past_date + pd.DateOffset(days=2)

        with pjm_vcr.use_cassette(
            f"test_get_lmp_real_time_unverified_hourly_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_lmp_real_time_unverified_hourly(past_date, past_end_date)

            self._check_lmp_real_time_unverified_hourly(df)
            assert df["Interval Start"].min() >= self.local_start_of_day(past_date)
            assert df["Interval End"].max() <= self.local_start_of_day(past_end_date)

    """get_load_forecast_5_min"""

    def test_get_load_forecast_5_min_latest(self):
        with pjm_vcr.use_cassette("test_get_load_forecast_5_min_latest.yaml"):
            df = self.iso.get_load_forecast_5_min("latest")
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert df.columns.tolist() == self.load_forecast_columns
            assert df["Interval Start"].min() == self.local_start_of_day("today")

    def test_get_load_forecast_5_min_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=29)
        past_end_date = past_date + pd.Timedelta(days=2)
        with pjm_vcr.use_cassette(
            f"test_get_load_forecast_5_min_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_load_forecast_5_min(past_date, past_end_date)
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert df.columns.tolist() == self.load_forecast_columns
            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                past_end_date,
            ) + pd.Timedelta(minutes=175)

    """get_tie_flows_5_min"""

    expected_tie_flows_5_min_cols = [
        "Interval Start",
        "Interval End",
        "Tie Flow Name",
        "Actual",
        "Scheduled",
    ]

    def _check_tie_flows_5_min(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == self.expected_tie_flows_5_min_cols
        assert (
            df["Interval End"] - df["Interval Start"] == pd.Timedelta(minutes=5)
        ).all()
        assert df["Tie Flow Name"].dtype == object
        assert df["Actual"].dtype in [np.float64, np.int64]
        assert df["Scheduled"].dtype in [np.float64, np.int64]

    @pytest.mark.parametrize("date", ["latest", "today"])
    def test_get_tie_flows_5_min_today_or_latest(self, date):
        with pjm_vcr.use_cassette(f"test_get_tie_flows_5_min_{date}.yaml"):
            df = self.iso.get_tie_flows_5_min(date)
            self._check_tie_flows_5_min(df)
            assert df["Interval Start"].min() == self.local_start_of_today()
            assert df[
                "Interval End"
            ].max() <= self.local_start_of_today() + pd.Timedelta(days=1)

    def test_get_tie_flows_5_min_historical_date(self):
        past_date = self.local_today() - pd.Timedelta(days=10)
        with pjm_vcr.use_cassette(
            f"test_get_tie_flows_5_min_historical_date_{past_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_tie_flows_5_min(past_date)
            self._check_tie_flows_5_min(df)
            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(
                past_date,
            ) + pd.Timedelta(days=1)

    def test_get_tie_flows_5_min_historical_range(self):
        past_date = self.local_today() - pd.Timedelta(days=5)
        past_end_date = past_date + pd.Timedelta(days=3)
        with pjm_vcr.use_cassette(
            f"test_get_tie_flows_5_min_historical_range_{past_date.strftime('%Y-%m-%d')}_{past_end_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_tie_flows_5_min(past_date, past_end_date)
            self._check_tie_flows_5_min(df)
            assert df["Interval Start"].min() == self.local_start_of_day(past_date)
            assert df["Interval End"].max() == self.local_start_of_day(past_end_date)

    """get_instantaneous_dispatch_rates"""

    def _check_instantaneous_dispatch_rates(self, df):
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Zone",
            "Instantaneous Dispatch Rate",
        ]

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(seconds=15)
        ).all()
        assert df["Zone"].dtype == object
        assert df["Instantaneous Dispatch Rate"].dtype == np.float64

    def test_get_instantaneous_dispatch_rates_today(self):
        with pjm_vcr.use_cassette("test_get_instantaneous_dispatch_rates_today.yaml"):
            df = self.iso.get_instantaneous_dispatch_rates("today")
            self._check_instantaneous_dispatch_rates(df)

            # The minimum interval start should be within 15 seconds of the local start
            # of today
            assert (
                self.local_start_of_today()
                <= df["Interval Start"].min()
                <= self.local_start_of_today() + pd.Timedelta(seconds=15)
            )

            # The maximum interval start should be within 30 seconds of the current time
            assert (
                self.local_now()
                - pd.Timedelta(
                    seconds=30,
                )
                <= df["Interval Start"].max()
                <= self.local_now()
            )

    def test_get_instantaneous_dispatch_rates_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=2)
        end = start + pd.Timedelta(hours=4)

        with pjm_vcr.use_cassette(
            f"test_get_instantaneous_dispatch_rates_date_range_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.yaml",  # noqa: E501
        ):
            df = self.iso.get_instantaneous_dispatch_rates(start, end)
            self._check_instantaneous_dispatch_rates(df)

            # Minimum interval start should be within 15 seconds of the start date
            assert (
                start <= df["Interval Start"].min() <= start + pd.Timedelta(seconds=15)
            )

            # Maximum interval start should be within 15 seconds of the end date
            assert end - pd.Timedelta(seconds=15) <= df["Interval Start"].max() <= end

    def _check_hourly_net_exports_by_state(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "State",
            "Net Interchange",
        ]
        assert not df.empty
        assert df["Interval Start"].is_monotonic_increasing

    def test_get_hourly_net_exports_by_state_latest(self):
        with pjm_vcr.use_cassette("test_get_hourly_net_exports_by_state_latest.yaml"):
            df = self.iso.get_hourly_net_exports_by_state("latest")
            self._check_hourly_net_exports_by_state(df)

    @pytest.mark.parametrize("date, end", test_dates)
    def test_get_hourly_net_exports_by_state_historical_date_range(self, date, end):
        with pjm_vcr.use_cassette(
            f"test_get_hourly_net_exports_by_state_{date}_{end}.yaml",
        ):
            df = self.iso.get_hourly_net_exports_by_state(date, end)
            self._check_hourly_net_exports_by_state(df)
            assert df["Interval Start"].min().date() == pd.Timestamp(date).date()
            assert df["Interval End"].max().date() <= pd.Timestamp(
                end,
            ).date() + pd.Timedelta(days=1)

    def _check_hourly_transfer_limits_and_flows(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Transfer Limit Area",
            "Average Transfers",
            "Average Transfer Limit",
        ]
        assert not df.empty

    def test_get_hourly_transfer_limits_and_flows_latest(self):
        with pjm_vcr.use_cassette(
            "test_get_hourly_transfer_limits_and_flows_latest.yaml",
        ):
            df = self.iso.get_hourly_transfer_limits_and_flows("latest")
            self._check_hourly_transfer_limits_and_flows(df)

    @pytest.mark.parametrize("date, end", test_dates)
    def test_get_hourly_transfer_limits_and_flows_historical_date_range(
        self,
        date,
        end,
    ):
        with pjm_vcr.use_cassette(
            f"test_get_hourly_transfer_limits_and_flows_{date}_{end}.yaml",
        ):
            df = self.iso.get_hourly_transfer_limits_and_flows(date, end)
            self._check_hourly_transfer_limits_and_flows(df)
            assert df["Interval Start"].min().date() == pd.Timestamp(date).date()
            assert df["Interval End"].max().date() <= pd.Timestamp(end).date()

    def _check_actual_and_scheduled_interchange_summary(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Tie Line",
            "Actual Flow",
            "Scheduled Flow",
            "Inadvertent Flow",
        ]
        assert not df.empty

    def test_get_actual_and_scheduled_interchange_summary_latest(self):
        with pjm_vcr.use_cassette(
            "test_get_actual_and_scheduled_interchange_summary_latest.yaml",
        ):
            df = self.iso.get_actual_and_scheduled_interchange_summary("latest")
            self._check_actual_and_scheduled_interchange_summary(df)

    @pytest.mark.parametrize("date, end", test_dates)
    def test_get_actual_and_scheduled_interchange_summary_historical_date_range(
        self,
        date,
        end,
    ):
        with pjm_vcr.use_cassette(
            f"test_get_actual_and_scheduled_interchange_summary_{date}_{end}.yaml",
        ):
            df = self.iso.get_actual_and_scheduled_interchange_summary(date, end)
            self._check_actual_and_scheduled_interchange_summary(df)
            assert df["Interval Start"].min().date() == pd.Timestamp(date).date()
            assert df["Interval End"].max().date() <= pd.Timestamp(
                end,
            ).date() + pd.Timedelta(days=1)

    def _check_scheduled_interchange_real_time(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Tie Line",
            "Hourly Net Tie Schedule",
        ]
        assert not df.empty
        assert df["Interval Start"].is_monotonic_increasing

    def test_get_scheduled_interchange_real_time_latest(self):
        with pjm_vcr.use_cassette(
            "test_get_scheduled_interchange_real_time_latest.yaml",
        ):
            df = self.iso.get_scheduled_interchange_real_time("latest")
            self._check_scheduled_interchange_real_time(df)

    @pytest.mark.parametrize("date, end", test_dates)
    def test_get_scheduled_interchange_real_time_historical_date_range(self, date, end):
        with pjm_vcr.use_cassette(
            f"test_get_scheduled_interchange_real_time_{date}_{end}.yaml",
        ):
            df = self.iso.get_scheduled_interchange_real_time(date, end)
            self._check_scheduled_interchange_real_time(df)
            assert df["Interval Start"].min().date() == pd.Timestamp(date).date()
            assert df["Interval End"].max().date() <= pd.Timestamp(
                end,
            ).date() + pd.Timedelta(days=1)

    def _check_interface_flows_and_limits_day_ahead(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Interface Limit Name",
            "Flow",
            "Limit",
        ]
        assert not df.empty

    def test_get_interface_flows_and_limits_day_ahead_latest(self):
        with pjm_vcr.use_cassette(
            "test_get_interface_flows_and_limits_day_ahead_latest.yaml",
        ):
            df = self.iso.get_interface_flows_and_limits_day_ahead("latest")
            self._check_interface_flows_and_limits_day_ahead(df)

    @pytest.mark.parametrize("date, end", test_dates)
    def test_get_interface_flows_and_limits_day_ahead_historical_date_range(
        self,
        date,
        end,
    ):
        with pjm_vcr.use_cassette(
            f"test_get_interface_flows_and_limits_day_ahead_{date}_{end}.yaml",
        ):
            df = self.iso.get_interface_flows_and_limits_day_ahead(date, end)
            self._check_interface_flows_and_limits_day_ahead(df)
            assert df["Interval Start"].min().date() == pd.Timestamp(date).date()
            assert df["Interval End"].max().date() <= pd.Timestamp(
                end,
            ).date() + pd.Timedelta(days=1)

    def _check_projected_peak_tie_flow(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Projected Peak Time",
            "Interface",
            "Scheduled Tie Flow",
        ]
        assert not df.empty

    def test_get_projected_peak_tie_flow_latest(self):
        with pjm_vcr.use_cassette("test_get_projected_peak_tie_flow_latest.yaml"):
            df = self.iso.get_projected_peak_tie_flow("latest")
            self._check_projected_peak_tie_flow(df)

    @pytest.mark.parametrize("date, end", test_dates)
    def test_get_projected_peak_tie_flow_historical_date_range(self, date, end):
        with pjm_vcr.use_cassette(
            f"test_get_projected_peak_tie_flow_{date}_{end}.yaml",
        ):
            df = self.iso.get_projected_peak_tie_flow(date, end)
            self._check_projected_peak_tie_flow(df)

    """get_actual_operational_statistics"""

    def _check_actual_operational_statistics(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Area",
            "Area Load Forecast",
            "Actual Load",
            "Dispatch Rate",
        ]
        assert not df.empty
        assert df["Area"].dtype == object
        assert df["Area Load Forecast"].dtype in [np.float64, np.int64]
        assert df["Actual Load"].dtype in [np.float64, np.int64]
        assert df["Dispatch Rate"].dtype in [np.float64, np.int64]

    def test_get_actual_operational_statistics_latest(self):
        with pjm_vcr.use_cassette("test_get_actual_operational_statistics_latest.yaml"):
            df = self.iso.get_actual_operational_statistics("latest")
            self._check_actual_operational_statistics(df)
            min_start = df["Interval Start"].min().date()
            today = self.local_start_of_today().date()
            yesterday = today - pd.Timedelta(days=1)
            # The implementation can return either today's or yesterday's data depending on time
            assert min_start in [today, yesterday]

    @pytest.mark.parametrize("date, end", test_dates)
    def test_get_actual_operational_statistics_historical_date_range(self, date, end):
        with pjm_vcr.use_cassette(
            f"test_get_actual_operational_statistics_{date}_{end}.yaml",
        ):
            df = self.iso.get_actual_operational_statistics(date, end)
            self._check_actual_operational_statistics(df)
            expected_start_date = pd.Timestamp(date).date() - pd.Timedelta(days=1)
            assert df["Interval Start"].min().date() == expected_start_date
            assert df["Interval End"].max().date() <= pd.Timestamp(
                end,
            ).date() - pd.Timedelta(days=1)

    """get_pricing_nodes"""

    def _check_pricing_nodes(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Pricing Node ID",
            "Pricing Node Name",
            "Pricing Node Type",
            "Pricing Node SubType",
            "Zone",
            "Voltage Level",
            "Effective Date",
            "Termination Date",
        ]
        assert not df.empty
        assert df["Pricing Node ID"].dtype in [np.int64, np.float64]
        assert df["Pricing Node Name"].dtype == object
        assert df["Pricing Node Type"].dtype == object
        assert df["Zone"].dtype == object

    @pytest.mark.parametrize("as_of", ["now", None])
    def test_get_pricing_nodes_as_of(self, as_of):
        with pjm_vcr.use_cassette(f"test_get_pricing_nodes_as_of_{as_of}.yaml"):
            df = self.iso.get_pricing_nodes(as_of=as_of)
            self._check_pricing_nodes(df)
            if as_of == "now":
                # Should filter out terminated records
                assert (
                    df["Termination Date"].isna()
                    | (
                        df["Termination Date"]
                        > pd.Timestamp.now(tz=self.iso.default_timezone)
                    )
                ).all()

    def test_get_pricing_nodes_with_specific_date(self):
        specific_date = pd.Timestamp("2024-01-01", tz=self.iso.default_timezone)
        with pjm_vcr.use_cassette(
            f"test_get_pricing_nodes_specific_date_{specific_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_pricing_nodes(as_of=specific_date)
            self._check_pricing_nodes(df)
            # Should filter out records terminated before the specific date
            assert (
                df["Termination Date"].isna() | (df["Termination Date"] > specific_date)
            ).all()

    """get_reserve_subzone_resources"""

    def _check_reserve_subzone_resources(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Resource ID",
            "Resource Name",
            "Resource Type",
            "Zone",
            "Subzone",
            "Effective Date",
            "Termination Date",
        ]
        assert not df.empty
        assert df["Resource ID"].dtype in [object, np.int64, np.float64]
        assert df["Resource Name"].dtype == object
        assert df["Resource Type"].dtype == object
        assert df["Subzone"].dtype == object
        assert df["Zone"].dtype == object

    @pytest.mark.parametrize("as_of", ["now", None])
    def test_get_reserve_subzone_resources_as_of(self, as_of):
        with pjm_vcr.use_cassette(
            f"test_get_reserve_subzone_resources_as_of_{as_of}.yaml",
        ):
            df = self.iso.get_reserve_subzone_resources(as_of=as_of)
            self._check_reserve_subzone_resources(df)
            if as_of == "now":
                # Should filter out terminated records
                assert (
                    df["Termination Date"].isna()
                    | (
                        df["Termination Date"]
                        > pd.Timestamp.now(tz=self.iso.default_timezone)
                    )
                ).all()

    def test_get_reserve_subzone_resources_with_specific_date(self):
        specific_date = pd.Timestamp("2024-01-01", tz=self.iso.default_timezone)
        with pjm_vcr.use_cassette(
            f"test_get_reserve_subzone_resources_specific_date_{specific_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_reserve_subzone_resources(as_of=specific_date)
            self._check_reserve_subzone_resources(df)
            # Should filter out records terminated before the specific date
            assert (
                df["Termination Date"].isna() | (df["Termination Date"] > specific_date)
            ).all()

    """get_reserve_subzone_buses"""

    def _check_reserve_subzone_buses(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Pricing Node ID",
            "Pricing Node Name",
            "Pricing Node Type",
            "Subzone",
            "Effective Date",
            "Termination Date",
        ]
        assert not df.empty
        assert df["Pricing Node ID"].dtype in [np.int64, np.float64]
        assert df["Pricing Node Name"].dtype == object
        assert df["Pricing Node Type"].dtype == object
        assert df["Subzone"].dtype == object

    @pytest.mark.parametrize("as_of", ["now", None])
    def test_get_reserve_subzone_buses_as_of(self, as_of):
        with pjm_vcr.use_cassette(f"test_get_reserve_subzone_buses_as_of_{as_of}.yaml"):
            df = self.iso.get_reserve_subzone_buses(as_of=as_of)
            self._check_reserve_subzone_buses(df)
            if as_of == "now":
                # Should filter out terminated records
                assert (
                    df["Termination Date"].isna()
                    | (
                        df["Termination Date"]
                        > pd.Timestamp.now(tz=self.iso.default_timezone)
                    )
                ).all()

    def test_get_reserve_subzone_buses_with_specific_date(self):
        specific_date = pd.Timestamp("2024-01-01", tz=self.iso.default_timezone)
        with pjm_vcr.use_cassette(
            f"test_get_reserve_subzone_buses_specific_date_{specific_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_reserve_subzone_buses(as_of=specific_date)
            self._check_reserve_subzone_buses(df)
            # Should filter out records terminated before the specific date
            assert (
                df["Termination Date"].isna() | (df["Termination Date"] > specific_date)
            ).all()

    """get_weight_average_aggregation_definition"""

    def _check_weight_average_aggregation_definition(self, df):
        assert isinstance(df, pd.DataFrame)
        assert df.columns.tolist() == [
            "Aggregate Node ID",
            "Aggregate Node Name",
            "Bus Node ID",
            "Bus Node Name",
            "Bus Node Factor",
            "Effective Date",
            "Termination Date",
        ]
        assert not df.empty
        assert df["Aggregate Node ID"].dtype in [np.int64, np.float64]
        assert df["Aggregate Node Name"].dtype == object
        assert df["Bus Node ID"].dtype in [np.int64, np.float64]
        assert df["Bus Node Name"].dtype == object
        assert df["Bus Node Factor"].dtype in [np.float64, np.int64]

    @pytest.mark.parametrize("as_of", ["now", None])
    def test_get_weight_average_aggregation_definition_as_of(self, as_of):
        with pjm_vcr.use_cassette(
            f"test_get_weight_average_aggregation_definition_as_of_{as_of}.yaml",
        ):
            df = self.iso.get_weight_average_aggregation_definition(as_of=as_of)
            self._check_weight_average_aggregation_definition(df)
            if as_of == "now":
                # Should filter out terminated records
                assert (
                    df["Termination Date"].isna()
                    | (
                        df["Termination Date"]
                        > pd.Timestamp.now(tz=self.iso.default_timezone)
                    )
                ).all()

    def test_get_weight_average_aggregation_definition_with_specific_date(self):
        specific_date = pd.Timestamp("2024-01-01", tz=self.iso.default_timezone)
        with pjm_vcr.use_cassette(
            f"test_get_weight_average_aggregation_definition_specific_date_{specific_date.strftime('%Y-%m-%d')}.yaml",
        ):
            df = self.iso.get_weight_average_aggregation_definition(as_of=specific_date)
            self._check_weight_average_aggregation_definition(df)
            # Should filter out records terminated before the specific date
            assert (
                df["Termination Date"].isna() | (df["Termination Date"] > specific_date)
            ).all()

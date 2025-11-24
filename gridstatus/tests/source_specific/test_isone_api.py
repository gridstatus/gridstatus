import numpy as np
import pandas as pd
import pytest

from gridstatus.base import NoDataFoundException
from gridstatus.isone_api.isone_api import ISONEAPI, ZONE_LOCATIONID_MAP
from gridstatus.isone_api.isone_api_constants import (
    ISONE_CAPACITY_FORECAST_7_DAY_COLUMNS,
    ISONE_FCM_RECONFIGURATION_COLUMNS,
    ISONE_RESERVE_ZONE_ALL_COLUMNS,
)
from gridstatus.tests.base_test_iso import TestHelperMixin
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="isone_api",
    record_mode=RECORD_MODE,
)

DST_CHANGE_TEST_DATES = [
    ("2024-11-02", "2024-11-04"),
    ("2024-03-09", "2024-03-11"),
]

TEST_MULTIPLE_LOCATIONS = [
    (".Z.MAINE", ".Z.NEWHAMPSHIRE"),
]

TEST_SINGLE_LOCATIONS = [loc for pair in TEST_MULTIPLE_LOCATIONS for loc in pair]

# Shared column definitions for tests
LMP_COLUMNS = [
    "Interval Start",
    "Interval End",
    "Location",
    "Location Type",
    "LMP",
    "Energy",
    "Congestion",
    "Loss",
]


class TestISONEAPI(TestHelperMixin):
    def setup_class(cls):
        cls.iso = ISONEAPI(sleep_seconds=0.1, max_retries=2)

    def test_class_init(self):
        assert self.iso.sleep_seconds == 0.1
        assert self.iso.max_retries == 2
        assert self.iso.username is not None
        assert self.iso.password is not None

    def test_zone_locationid_map(self):
        for zone, location_id in ZONE_LOCATIONID_MAP.items():
            assert ZONE_LOCATIONID_MAP[zone] == location_id

    def test_get_locations(self):
        with api_vcr.use_cassette("test_get_locations.yaml"):
            result = self.iso.get_locations()
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 20
            assert list(result.columns) == [
                "LocationID",
                "LocationType",
                "LocationName",
                "AreaType",
            ]

    @pytest.mark.parametrize(
        "location",
        TEST_SINGLE_LOCATIONS,
    )
    def test_get_realtime_hourly_demand_latest(self, location: str):
        with api_vcr.use_cassette(
            f"test_get_realtime_hourly_demand_latest_{location}.yaml",
        ):
            result = self.iso.get_realtime_hourly_demand(
                date="latest",
                locations=[location],
            )

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
            ]
            assert result["Location"].iloc[0] == location
            assert result["Location Id"].iloc[0] == ZONE_LOCATIONID_MAP[location]
            assert isinstance(result["Load"].iloc[0], (int, float))

    def test_get_dayahead_hourly_demand_latest(self):
        with api_vcr.use_cassette("test_get_dayahead_hourly_demand_latest.yaml"):
            result = self.iso.get_dayahead_hourly_demand(
                date="latest",
                locations=["NEPOOL AREA"],
            )
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
            ]
            assert result["Location"].iloc[0] == "NEPOOL AREA"
            assert result["Location Id"].iloc[0] == 32
            assert isinstance(result["Load"].iloc[0], np.number)

    # NOTE(kladar): These two are not super useful as tests go, but starting to think about API failure modes and
    # how to catch them.
    def test_get_dayahead_hourly_demand_invalid_location(self):
        with pytest.raises(ValueError):
            self.iso.get_dayahead_hourly_demand(locations=["INVALID_LOCATION"])

    @pytest.mark.parametrize(
        "locations",
        TEST_MULTIPLE_LOCATIONS,
    )
    def test_get_realtime_hourly_demand_multiple_locations(
        self,
        locations: tuple[str, str],
    ):
        with api_vcr.use_cassette(
            f"test_get_realtime_hourly_demand_multiple_locations_{locations}.yaml",
        ):
            result = self.iso.get_realtime_hourly_demand(
                date="latest",
                locations=list(locations),
            )

            assert isinstance(result, pd.DataFrame)
            assert len(result) == len(locations)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
            ]
            assert set(result["Location"]) == set(locations)
            assert set(result["Location Id"]) == {
                ZONE_LOCATIONID_MAP[loc] for loc in locations
            }
            assert all(isinstance(load, (int, float)) for load in result["Load"])

    @pytest.mark.parametrize(
        "date,end,locations",
        [(date, end, TEST_SINGLE_LOCATIONS) for date, end in DST_CHANGE_TEST_DATES],
    )
    def test_get_realtime_hourly_demand_date_range(
        self,
        date: str,
        end: str,
        locations: list[str],
    ):
        with api_vcr.use_cassette(
            f"test_get_realtime_hourly_demand_date_range_{date}_{end}_{locations}.yaml",
        ):
            result = self.iso.get_realtime_hourly_demand(
                date=date,
                end=end,
                locations=locations,
            )

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
            ]
            assert set(result["Location"]) == set(locations)
            assert set(result["Location Id"]) == {
                ZONE_LOCATIONID_MAP[loc] for loc in locations
            }
            assert all(isinstance(load, (int, float)) for load in result["Load"])
            assert min(result["Interval Start"]).date() == pd.Timestamp(date).date()
            assert max(result["Interval End"]).date() == pd.Timestamp(end).date()

    @pytest.mark.parametrize(
        "date,end,locations",
        [(date, end, TEST_SINGLE_LOCATIONS) for date, end in DST_CHANGE_TEST_DATES],
    )
    def test_get_dayahead_hourly_demand_date_range(
        self,
        date: str,
        end: str,
        locations: list[str],
    ):
        with api_vcr.use_cassette(
            f"test_get_dayahead_hourly_demand_date_range_{date}_{end}_{locations}.yaml",
        ):
            result = self.iso.get_dayahead_hourly_demand(
                date=date,
                end=end,
                locations=locations,
            )

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
            ]
            assert set(result["Location"]) == set(locations)
            assert set(result["Location Id"]) == {
                ZONE_LOCATIONID_MAP[loc] for loc in locations
            }
            assert all(isinstance(load, (int, float)) for load in result["Load"])
            assert min(result["Interval Start"]).date() == pd.Timestamp(date).date()
            assert max(result["Interval End"]).date() == pd.Timestamp(end).date()

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_load_forecast_hourly(self, date, end):
        cassette_name = f"test_get_load_forecast_hourly_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_load_forecast_hourly(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Load Forecast",
                "Net Load Forecast",
            ]
            assert (
                min(result["Interval Start"]).date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert result["Load Forecast"].dtype in [np.int64, np.float64]
            assert result["Net Load Forecast"].dtype in [np.int64, np.float64]
            assert (result["Load Forecast"] > 0).all()
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(hours=1)
            ).all()

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_reliability_region_load_forecast(self, date, end):
        cassette_name = f"test_get_reliability_region_load_forecast_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_reliability_region_load_forecast(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Location",
                "Load Forecast",
                "Regional Percentage",
            ]
            assert (
                min(result["Interval Start"]).date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(hours=1)
            ).all()
            assert set(result["Location"].unique()) == {
                ".Z.CONNECTICUT",
                ".Z.MAINE",
                ".Z.NEWHAMPSHIRE",
                ".Z.RHODEISLAND",
                ".Z.VERMONT",
                ".Z.SEMASS",
                ".Z.WCMASS",
                ".Z.NEMASSBOST",
            }
            assert result["Load Forecast"].dtype in [np.int64, np.float64]
            assert (result["Load Forecast"] > 0).all()
            assert result["Regional Percentage"].dtype == np.float64
            assert (
                (result["Regional Percentage"] >= 0)
                & (result["Regional Percentage"] <= 100)
            ).all()
            grouped = result.groupby(["Interval Start", "Publish Time"])
            assert (grouped["Regional Percentage"].sum().between(99.9, 100.1)).all()

    def test_get_fuel_mix_latest(self):
        with api_vcr.use_cassette("test_get_fuel_mix_latest.yaml"):
            result = self.iso.get_fuel_mix(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "Time" in result.columns

            assert isinstance(result["Time"].iloc[0], pd.Timestamp)
            numeric_cols = [col for col in result.columns if col != "Time"]
            for col in numeric_cols:
                assert result[col].dtype in [np.int64, np.float64]
                assert (result[col] >= 0).all()

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_fuel_mix_date_range(self, date, end):
        cassette_name = f"test_get_fuel_mix_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_fuel_mix(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert "Time" in result.columns

            assert min(result["Time"]).date() == pd.Timestamp(date).date()
            assert max(result["Time"]).date() == pd.Timestamp(
                end,
            ).date() - pd.Timedelta(
                days=1,
            )

            assert all(isinstance(t, pd.Timestamp) for t in result["Time"])
            numeric_cols = [col for col in result.columns if col != "Time"]
            for col in numeric_cols:
                assert result[col].dtype in [np.int64, np.float64]
                assert (result[numeric_cols].sum(axis=1) > 0).all()

    def test_get_load_hourly_latest(self):
        with api_vcr.use_cassette("test_get_load_hourly_latest.yaml"):
            result = self.iso.get_load_hourly(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
                "Native Load",
                "ARD Demand",
            ]
            assert result["Location"].iloc[0] == "NEPOOL AREA"
            assert result["Location Id"].iloc[0] == 32
            assert isinstance(result["Load"].iloc[0], (int, float))
            assert isinstance(result["Native Load"].iloc[0], (int, float))
            assert isinstance(result["ARD Demand"].iloc[0], (int, float))

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_load_hourly_date_range(
        self,
        date,
        end,
    ):
        cassette_name = f"test_get_load_hourly_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_load_hourly(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
                "Native Load",
                "ARD Demand",
            ]
            assert all(result["Location"] == "NEPOOL AREA")
            assert all(result["Location Id"] == 32)
            assert all(isinstance(load, (int, float)) for load in result["Load"])
            assert all(isinstance(load, (int, float)) for load in result["Native Load"])
            assert all(isinstance(load, (int, float)) for load in result["ARD Demand"])
            assert min(result["Interval Start"]).date() == pd.Timestamp(date).date()
            assert max(result["Interval End"]).date() == pd.Timestamp(end).date()
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(hours=1)
            ).all()

    """get_interchange_hourly"""

    @pytest.mark.integration
    @api_vcr.use_cassette("test_get_interchange_hourly_date_range.yaml")
    def test_get_interchange_hourly_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=10)
        end = start + pd.DateOffset(days=1)
        with api_vcr.use_cassette(
            f"test_get_interchange_hourly_date_range_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_interchange_hourly(start, end)

            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Actual Interchange",
                "Purchase",
                "Sale",
            ]

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max()

            assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
                hours=1,
            )

            assert sorted(df["Location"].unique()) == [
                ".I.HQHIGATE120 2",
                ".I.HQ_P1_P2345 5",
                ".I.NRTHPORT138 5",
                ".I.ROSETON 345 1",
                ".I.SALBRYNB345 1",
                ".I.SHOREHAM138 99",
            ]

    def test_get_interchange_hourly_dst_end(self):
        start = self.local_start_of_day(DST_CHANGE_TEST_DATES[0][0])
        end = self.local_start_of_day(DST_CHANGE_TEST_DATES[0][1])
        with api_vcr.use_cassette(
            f"test_get_interchange_hourly_dst_end_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_interchange_hourly(start, end)

            # ISONE does publish data for the repeated hour so there is one extra data point
            # for each location
            # 24 hours * 2 days * 6 locations + 6 locations
            assert len(df) == 24 * 2 * 6 + 6
            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end

    def test_get_interchange_hourly_dst_start(self):
        start = self.local_start_of_day(DST_CHANGE_TEST_DATES[1][0])
        end = self.local_start_of_day(DST_CHANGE_TEST_DATES[1][1])
        with api_vcr.use_cassette(
            f"test_get_interchange_hourly_dst_start_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_interchange_hourly(start, end)

            # 24 hours * 2 days * 6 locations - 6 locations
            assert len(df) == 24 * 2 * 6 - 6

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end

    """get_interchange_15_min"""

    def test_get_interchange_15_min_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=10)
        end = start + pd.DateOffset(days=1)
        with api_vcr.use_cassette(
            f"test_get_interchange_15_min_date_range_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_interchange_15_min(start, end)

            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Actual Interchange",
                "Purchase",
                "Sale",
            ]

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end

            assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
                minutes=15,
            )

            assert sorted(df["Location"].unique()) == [".I.ROSETON 345 1"]

    def test_get_interchange_15_min_dst_end(self):
        start = self.local_start_of_day(DST_CHANGE_TEST_DATES[0][0])
        end = self.local_start_of_day(DST_CHANGE_TEST_DATES[0][1])
        with api_vcr.use_cassette(
            f"test_get_interchange_15_min_dst_end_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_interchange_15_min(start, end)

            # ISONE does not publish data for the repeated hour so there are no extra
            # data points. 24 hours * 4 intervals per hour * 2 days
            assert len(df) == 24 * 4 * 2

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end

    def test_get_interchange_15_min_dst_start(self):
        start = self.local_start_of_day(DST_CHANGE_TEST_DATES[1][0])
        end = self.local_start_of_day(DST_CHANGE_TEST_DATES[1][1])
        with api_vcr.use_cassette(
            f"test_get_interchange_15_min_dst_start_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_interchange_15_min(start, end)

            # 24 hours * 4 intervals per hour * 2 days - (1 hour * 4 intervals per hour)
            assert len(df) == 24 * 4 * 2 - 4

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end

    """get_external_flows_5_min"""

    def test_get_external_flows_5_min_date_range(self):
        start = self.local_start_of_today() - pd.DateOffset(days=10)
        end = start + pd.DateOffset(days=1)
        with api_vcr.use_cassette(
            f"test_get_external_flows_5_min_date_range_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_external_flows_5_min(start, end)

            assert df.columns.tolist() == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Actual Flow",
                "Import Limit",
                "Export Limit",
                "Current Schedule",
                "Purchase",
                "Sale",
                "Total Exports",
                "Total Imports",
            ]

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end

            assert (df["Interval End"] - df["Interval Start"]).unique() == pd.Timedelta(
                minutes=5,
            )

            assert sorted(df["Location"].unique()) == [
                ".I.HQHIGATE120 2",
                ".I.HQ_P1_P2345 5",
                ".I.NRTHPORT138 5",
                ".I.ROSETON 345 1",
                ".I.SALBRYNB345 1",
                ".I.SHOREHAM138 99",
            ]

    def test_get_external_flows_5_min_dst_end(self):
        start = self.local_start_of_day(DST_CHANGE_TEST_DATES[0][0])
        end = self.local_start_of_day(DST_CHANGE_TEST_DATES[0][1])
        with api_vcr.use_cassette(
            f"test_get_external_flows_5_min_dst_end_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_external_flows_5_min(start, end)

            # 12 intervals per hour * 24 hours * 2 days * 6 locations + (6 locations * 12
            # intervals per hour * 1 extra hour)
            assert len(df) == 12 * 24 * 2 * 6 + 6 * 12

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end

    def test_get_external_flows_5_min_dst_start(self):
        start = self.local_start_of_day(DST_CHANGE_TEST_DATES[1][0])
        end = self.local_start_of_day(DST_CHANGE_TEST_DATES[1][1])
        with api_vcr.use_cassette(
            f"test_get_external_flows_5_min_dst_start_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_external_flows_5_min(start, end)

            # 12 intervals per hour * 24 hours * 2 days * 6 locations - (6 locations * 12
            # intervals per hour)
            assert len(df) == 12 * 24 * 2 * 6 - 6 * 12

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end

    def test_get_lmp_real_time_hourly_prelim_latest(self):
        with api_vcr.use_cassette("test_get_lmp_real_time_hourly_prelim_latest.yaml"):
            result = self.iso.get_lmp_real_time_hourly_prelim(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            self._check_lmp_columns(result)

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_hourly_prelim_date_range(self, date, end):
        cassette_name = f"test_get_lmp_real_time_hourly_prelim_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_hourly_prelim(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            self._check_lmp_columns(result)
            assert (
                min(result["Interval Start"]).date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(hours=1)
            ).all()

    def test_get_lmp_real_time_hourly_final_latest(self):
        with api_vcr.use_cassette("test_get_lmp_real_time_hourly_final_latest.yaml"):
            result = self.iso.get_lmp_real_time_hourly_final(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            self._check_lmp_columns(result)

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_hourly_final_date_range(self, date, end):
        cassette_name = f"test_get_lmp_real_time_hourly_final_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_hourly_final(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            self._check_lmp_columns(result)
            assert (
                min(result["Interval Start"]).date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(hours=1)
            ).all()

    def test_get_lmp_real_time_5_min_prelim_latest(self):
        with api_vcr.use_cassette("test_get_lmp_real_time_5_min_prelim_latest.yaml"):
            result = self.iso.get_lmp_real_time_5_min_prelim(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            self._check_lmp_columns(result)
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(minutes=5)
            ).all()

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_5_min_prelim_date_range(self, date: str, end: str):
        cassette_name = f"test_get_lmp_real_time_5_min_prelim_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_5_min_prelim(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            self._check_lmp_columns(result)
            assert (
                min(result["Interval Start"]).date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(minutes=5)
            ).all()

    def test_get_lmp_real_time_5_min_final_latest(self):
        with api_vcr.use_cassette("test_get_lmp_real_time_5_min_final_latest.yaml"):
            result = self.iso.get_lmp_real_time_5_min_final(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            self._check_lmp_columns(result)
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(minutes=5)
            ).all()

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_5_min_final_date_range(self, date: str, end: str):
        cassette_name = f"test_get_lmp_real_time_5_min_final_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_5_min_final(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            self._check_lmp_columns(result)
            assert (
                min(result["Interval Start"]).date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(minutes=5)
            ).all()

    """get_capacity_forecast_7_day"""

    def _check_capacity_forecast_7_day_columns(self, result: pd.DataFrame) -> None:
        """Validate the DataFrame columns against the Pydantic model fields."""
        assert list(result.columns) == ISONE_CAPACITY_FORECAST_7_DAY_COLUMNS
        assert result["Interval Start"].dtype == "datetime64[ns, US/Eastern]"
        assert result["Interval End"].dtype == "datetime64[ns, US/Eastern]"
        assert result["Publish Time"].dtype == "datetime64[ns, US/Eastern]"
        assert result["High Temperature Boston"].dtype in [np.int64, np.float64]
        assert result["Dew Point Boston"].dtype in [np.int64, np.float64]
        assert result["High Temperature Hartford"].dtype in [np.int64, np.float64]
        assert result["Dew Point Hartford"].dtype in [np.int64, np.float64]
        assert result["Total Capacity Supply Obligation"].dtype in [
            np.int64,
            np.float64,
        ]
        assert result["Anticipated Cold Weather Outages"].dtype in [
            np.int64,
            np.float64,
        ]
        assert result["Other Generation Outages"].dtype in [np.int64, np.float64]
        assert result["Anticipated Delist MW Offered"].dtype in [np.int64, np.float64]
        assert result["Total Generation Available"].dtype in [np.int64, np.float64]
        assert result["Import at Time of Peak"].dtype in [np.int64, np.float64]
        assert result["Total Available Generation and Imports"].dtype in [
            np.int64,
            np.float64,
        ]
        assert result["Projected Peak Load"].dtype in [np.int64, np.float64]
        assert result["Replacement Reserve Requirement"].dtype in [np.int64, np.float64]
        assert result["Required Reserve"].dtype in [np.int64, np.float64]
        assert result["Required Reserve Including Replacement"].dtype in [
            np.int64,
            np.float64,
        ]
        assert result["Total Load Plus Required Reserve"].dtype in [
            np.int64,
            np.float64,
        ]
        assert result["Projected Surplus or Deficiency"].dtype in [np.int64, np.float64]
        assert result["Available Demand Response Resources"].dtype in [
            np.int64,
            np.float64,
        ]
        assert result["Load Relief Actions Anticipated"].dtype == object
        assert result["Power Watch"].dtype == object
        assert result["Power Warning"].dtype == object
        assert result["Cold Weather Watch"].dtype == object
        assert result["Cold Weather Warning"].dtype == object
        assert result["Cold Weather Event"].dtype == object

    def test_get_capacity_forecast_7_day_latest(self):
        with api_vcr.use_cassette("test_get_capacity_forecast_7_day_latest.yaml"):
            result = self.iso.get_capacity_forecast_7_day(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            self._check_capacity_forecast_7_day_columns(result)

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_capacity_forecast_7_day_date_range(self, date: str, end: str):
        cassette_name = f"test_get_capacity_forecast_7_day_{pd.Timestamp(date).strftime('%Y%m%d')}_{pd.Timestamp(end).strftime('%Y%m%d')}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_capacity_forecast_7_day(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            self._check_capacity_forecast_7_day_columns(result)

    """get_regulation_clearing_prices_real_time_5_min"""

    def _check_regulation_clearing_prices_real_time_5_min(self, df: pd.DataFrame):
        assert list(df.columns) == [
            "Interval Start",
            "Interval End",
            "Reg Service Clearing Price",
            "Reg Capacity Clearing Price",
        ]
        assert df["Reg Service Clearing Price"].dtype == np.float64
        assert df["Reg Capacity Clearing Price"].dtype == np.float64

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=5)
        ).all()

    def test_get_regulation_clearing_prices_real_time_5_min_latest(self):
        with api_vcr.use_cassette(
            "test_get_regulation_clearing_prices_real_time_5_min_latest.yaml",
        ):
            result = self.iso.get_regulation_clearing_prices_real_time_5_min(
                date="latest",
            )

        self._check_regulation_clearing_prices_real_time_5_min(result)

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_regulation_clearing_prices_real_time_5_min_date_range(
        self,
        date: str,
        end: str,
    ):
        cassette_name = (
            f"test_get_regulation_clearing_prices_real_time_5_min_{date}_{end}.yaml"
        )
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_regulation_clearing_prices_real_time_5_min(
                date=date,
                end=end,
            )

        self._check_regulation_clearing_prices_real_time_5_min(result)

        assert result["Interval Start"].min() == pd.Timestamp(date).tz_localize(
            self.iso.default_timezone,
        )
        assert result["Interval Start"].max() == pd.Timestamp(end).tz_localize(
            self.iso.default_timezone,
        ) - pd.Timedelta(minutes=5)

    """get_reserve_requirements_prices_forecast_day_ahead"""

    def _check_reserve_requirements_prices_forecast_day_ahead(
        self,
        df: pd.DataFrame,
    ):
        assert list(df.columns) == [
            "Interval Start",
            "Interval End",
            "EIR Designation MW",
            "FER Clearing Price",
            "Forecasted Energy Req MW",
            "Ten Min Spin Req MW",
            "TMNSR Clearing Price",
            "TMNSR Designation MW",
            "TMOR Clearing Price",
            "TMOR Designation MW",
            "TMSR Clearing Price",
            "TMSR Designation MW",
            "Total Ten Min Req MW",
            "Total Thirty Min Req MW",
        ]
        assert df["EIR Designation MW"].dtype == np.float64
        assert df["FER Clearing Price"].dtype == np.float64
        assert df["Forecasted Energy Req MW"].dtype == np.float64
        assert df["Ten Min Spin Req MW"].dtype == np.float64
        assert df["TMNSR Clearing Price"].dtype == np.float64
        assert df["TMNSR Designation MW"].dtype == np.float64
        assert df["TMOR Clearing Price"].dtype == np.float64
        assert df["TMOR Designation MW"].dtype == np.float64
        assert df["TMSR Clearing Price"].dtype == np.float64
        assert df["TMSR Designation MW"].dtype == np.float64
        assert df["Total Ten Min Req MW"].dtype == np.float64
        assert df["Total Thirty Min Req MW"].dtype == np.float64

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    def test_get_reserve_requirements_prices_forecast_day_ahead_latest(self):
        with api_vcr.use_cassette(
            "test_get_reserve_requirements_prices_forecast_day_ahead_latest.yaml",
        ):
            result = self.iso.get_reserve_requirements_prices_forecast_day_ahead(
                date="latest",
            )

        self._check_reserve_requirements_prices_forecast_day_ahead(result)

    # Dataset doesn't have data for 2024
    @pytest.mark.parametrize("date,end", [("2025-03-08", "2025-03-10")])
    def test_get_reserve_requirements_prices_forecast_day_ahead_date_range(
        self,
        date: str,
        end: str,
    ):
        cassette_name = (
            f"test_get_reserve_requirements_prices_forecast_day_ahead_{date}_{end}.yaml"
        )
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_reserve_requirements_prices_forecast_day_ahead(
                date=date,
                end=end,
            )

        self._check_reserve_requirements_prices_forecast_day_ahead(result)

        assert result["Interval Start"].min() == pd.Timestamp(date).tz_localize(
            self.iso.default_timezone,
        )
        assert result["Interval Start"].max() == pd.Timestamp(end).tz_localize(
            self.iso.default_timezone,
        ) - pd.Timedelta(hours=1)

    def _check_lmp_columns(self, df: pd.DataFrame):
        """Shared helper to validate LMP column structure and dtypes."""
        assert list(df.columns) == LMP_COLUMNS
        assert df["LMP"].dtype in [np.int64, np.float64]
        assert df["Energy"].dtype in [np.int64, np.float64]
        assert df["Congestion"].dtype in [np.int64, np.float64]
        assert df["Loss"].dtype in [np.int64, np.float64]

    """get_reserve_zone_prices_designations_real_time_5_min"""

    def _check_reserve_zone_prices_designations(
        self,
        df: pd.DataFrame,
        interval: pd.Timedelta,
    ):
        """Shared helper to validate reserve zone price data across different intervals."""
        assert list(df.columns) == ISONE_RESERVE_ZONE_ALL_COLUMNS

        assert df["Reserve Zone Id"].dtype == np.int64
        assert df["Reserve Zone Name"].dtype == object
        assert df["Ten Min Spin Requirement"].dtype == np.float64
        assert df["TMNSR Clearing Price"].dtype == np.float64
        assert df["TMNSR Designated MW"].dtype == np.float64
        assert df["TMOR Clearing Price"].dtype == np.float64
        assert df["TMOR Designated MW"].dtype == np.float64
        assert df["TMSR Clearing Price"].dtype == np.float64
        assert df["TMSR Designated MW"].dtype == np.float64
        assert df["Total 10 Min Requirement"].dtype == np.float64
        assert df["Total 30 Min Requirement"].dtype == np.float64

        assert list(df["Reserve Zone Id"].unique()) == [7000, 7001, 7002, 7003]

        assert ((df["Interval End"] - df["Interval Start"]) == interval).all()

    def test_get_reserve_zone_prices_designations_real_time_5_min_latest(self):
        with api_vcr.use_cassette(
            "test_get_reserve_zone_prices_designations_real_time_5_min_latest.yaml",
        ):
            result = self.iso.get_reserve_zone_prices_designations_real_time_5_min(
                date="latest",
            )

        self._check_reserve_zone_prices_designations(result, pd.Timedelta(minutes=5))

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_reserve_zone_prices_designations_real_time_5_min_date_range(
        self,
        date: str,
        end: str,
    ):
        cassette_name = f"test_get_reserve_zone_prices_designations_real_time_5_min_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_reserve_zone_prices_designations_real_time_5_min(
                date=date,
                end=end,
            )

        self._check_reserve_zone_prices_designations(result, pd.Timedelta(minutes=5))

        assert result["Interval Start"].min() == pd.Timestamp(date).tz_localize(
            self.iso.default_timezone,
        )
        assert result["Interval Start"].max() == pd.Timestamp(end).tz_localize(
            self.iso.default_timezone,
        ) - pd.Timedelta(minutes=5)

    """get_reserve_zone_prices_designations_real_time_hourly_final"""

    def test_get_reserve_zone_prices_designations_real_time_hourly_final_latest(self):
        with api_vcr.use_cassette(
            "test_get_reserve_zone_prices_designations_real_time_hourly_final_latest.yaml",
        ):
            result = (
                self.iso.get_reserve_zone_prices_designations_real_time_hourly_final(
                    date="latest",
                )
            )

        self._check_reserve_zone_prices_designations(result, pd.Timedelta(hours=1))

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_reserve_zone_prices_designations_real_time_hourly_final_date_range(
        self,
        date: str,
        end: str,
    ):
        cassette_name = f"test_get_reserve_zone_prices_designations_real_time_hourly_final_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = (
                self.iso.get_reserve_zone_prices_designations_real_time_hourly_final(
                    date=date,
                    end=end,
                )
            )

        self._check_reserve_zone_prices_designations(result, pd.Timedelta(hours=1))

        assert result["Interval Start"].min() == pd.Timestamp(date).tz_localize(
            self.iso.default_timezone,
        )
        assert result["Interval Start"].max() == pd.Timestamp(end).tz_localize(
            self.iso.default_timezone,
        ) - pd.Timedelta(hours=1)

    """get_reserve_zone_prices_designations_real_time_hourly_prelim"""

    def test_get_reserve_zone_prices_designations_real_time_hourly_prelim_latest(
        self,
    ):
        with api_vcr.use_cassette(
            "test_get_reserve_zone_prices_designations_real_time_hourly_prelim_latest.yaml",
        ):
            result = (
                self.iso.get_reserve_zone_prices_designations_real_time_hourly_prelim(
                    date="latest",
                )
            )

        self._check_reserve_zone_prices_designations(result, pd.Timedelta(hours=1))

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_reserve_zone_prices_designations_real_time_hourly_prelim_date_range(
        self,
        date: str,
        end: str,
    ):
        cassette_name = f"test_get_reserve_zone_prices_designations_real_time_hourly_prelim_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = (
                self.iso.get_reserve_zone_prices_designations_real_time_hourly_prelim(
                    date=date,
                    end=end,
                )
            )

        self._check_reserve_zone_prices_designations(result, pd.Timedelta(hours=1))

        assert result["Interval Start"].min() == pd.Timestamp(date).tz_localize(
            self.iso.default_timezone,
        )
        assert result["Interval Start"].max() == pd.Timestamp(end).tz_localize(
            self.iso.default_timezone,
        ) - pd.Timedelta(hours=1)

    """get_ancillary_services_strike_prices_day_ahead"""

    def _check_strike_prices_day_ahead(
        self,
        df: pd.DataFrame,
    ):
        assert list(df.columns) == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Expected Closeout Charge",
            "Expected Closeout Charge Override",
            "Expected RT Hub LMP",
            "Percentile 10 RT Hub LMP",
            "Percentile 25 RT Hub LMP",
            "Percentile 75 RT Hub LMP",
            "Percentile 90 RT Hub LMP",
            "SPC Load Forecast MW",
            "Strike Price",
        ]
        assert df["Expected Closeout Charge"].dtype == np.float64
        assert df["Expected Closeout Charge Override"].dtype == np.float64
        assert df["Expected RT Hub LMP"].dtype == np.float64
        assert df["Percentile 10 RT Hub LMP"].dtype == np.float64
        assert df["Percentile 25 RT Hub LMP"].dtype == np.float64
        assert df["Percentile 75 RT Hub LMP"].dtype == np.float64
        assert df["Percentile 90 RT Hub LMP"].dtype == np.float64
        assert df["SPC Load Forecast MW"].dtype == np.float64
        assert df["Strike Price"].dtype == np.float64

        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    def test_get_ancillary_services_strike_prices_day_ahead_latest(self):
        with api_vcr.use_cassette(
            "test_get_ancillary_services_strike_prices_day_ahead_latest.yaml",
        ):
            result = self.iso.get_ancillary_services_strike_prices_day_ahead(
                date="latest",
            )

        self._check_strike_prices_day_ahead(result)

    @pytest.mark.parametrize(
        "date,end",
        [("2025-03-08", "2025-03-10")],  # Dataset doesn't have data for 2024
    )
    def test_get_ancillary_services_strike_prices_day_ahead_date_range(
        self,
        date: str,
        end: str,
    ):
        cassette_name = (
            f"test_get_ancillary_services_strike_prices_day_ahead_{date}_{end}.yaml"
        )
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_ancillary_services_strike_prices_day_ahead(
                date=date,
                end=end,
            )

        self._check_strike_prices_day_ahead(result)

        assert result["Interval Start"].min() == pd.Timestamp(date).tz_localize(
            self.iso.default_timezone,
        )
        assert result["Interval Start"].max() == pd.Timestamp(end).tz_localize(
            self.iso.default_timezone,
        ) - pd.Timedelta(hours=1)

    def _check_constraints(
        self,
        df: pd.DataFrame,
        expected_columns: list[str],
        expected_interval: pd.Timedelta,
    ) -> None:
        assert list(df.columns) == expected_columns
        assert df["Interval Start"].dtype == "datetime64[ns, US/Eastern]"
        assert df["Interval End"].dtype == "datetime64[ns, US/Eastern]"
        assert ((df["Interval End"] - df["Interval Start"]) == expected_interval).all()
        assert df["Marginal Value"].dtype in [np.int64, np.float64]

    def test_get_binding_constraints_day_ahead_hourly_latest(self) -> None:
        with api_vcr.use_cassette(
            "test_get_binding_constraints_day_ahead_hourly_latest.yaml",
        ):
            df = self.iso.get_binding_constraints_day_ahead_hourly(date="latest")

        self._check_constraints(
            df,
            expected_columns=[
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Contingency Name",
                "Interface Flag",
                "Marginal Value",
            ],
            expected_interval=pd.Timedelta(hours=1),
        )

    @pytest.mark.parametrize(
        "date,end",
        [
            (
                pd.Timestamp("2025-11-01").tz_localize("US/Eastern"),
                pd.Timestamp("2025-11-03").tz_localize("US/Eastern"),
            ),
        ],
    )
    def test_get_binding_constraints_day_ahead_hourly_date_range(
        self,
        date: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        cassette_name = f"test_get_binding_constraints_day_ahead_hourly_{date.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml"
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_day_ahead_hourly(date=date, end=end)

        self._check_constraints(
            df,
            expected_columns=[
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Contingency Name",
                "Interface Flag",
                "Marginal Value",
            ],
            expected_interval=pd.Timedelta(hours=1),
        )

    def test_get_binding_constraints_preliminary_real_time_15_min_latest(self) -> None:
        with api_vcr.use_cassette(
            "test_get_binding_constraints_preliminary_real_time_15_min_latest.yaml",
        ):
            try:
                df = self.iso.get_binding_constraints_preliminary_real_time_15_min(
                    date="latest",
                )
                self._check_constraints(
                    df,
                    expected_columns=[
                        "Interval Start",
                        "Interval End",
                        "Constraint Name",
                        "Contingency Name",
                        "Marginal Value",
                    ],
                    expected_interval=pd.Timedelta(minutes=15),
                )
            except NoDataFoundException:
                pytest.skip(
                    "No data found for preliminary real-time 15-minute binding constraints",
                )

    def test_get_binding_constraints_final_real_time_15_min_latest(self) -> None:
        with api_vcr.use_cassette(
            "test_get_binding_constraints_final_real_time_15_min_latest.yaml",
        ):
            df = self.iso.get_binding_constraints_final_real_time_15_min(date="latest")

        self._check_constraints(
            df,
            expected_columns=[
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Marginal Value",
            ],
            expected_interval=pd.Timedelta(minutes=15),
        )

    @pytest.mark.parametrize(
        "date,end",
        [
            (
                pd.Timestamp("2025-11-01").tz_localize("US/Eastern"),
                pd.Timestamp("2025-11-03").tz_localize("US/Eastern"),
            ),
        ],
    )
    def test_get_binding_constraints_preliminary_real_time_15_min_date_range(
        self,
        date: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        cassette_name = f"test_get_binding_constraints_preliminary_real_time_15_min_{date.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml"
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_preliminary_real_time_15_min(
                date=date,
                end=end,
            )

        self._check_constraints(
            df,
            expected_columns=[
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Marginal Value",
            ],
            expected_interval=pd.Timedelta(minutes=15),
        )

    @pytest.mark.parametrize(
        "date,end",
        [
            (
                pd.Timestamp("2025-11-01").tz_localize("US/Eastern"),
                pd.Timestamp("2025-11-03").tz_localize("US/Eastern"),
            ),
        ],
    )
    def test_get_binding_constraints_final_real_time_15_min_date_range(
        self,
        date: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        cassette_name = f"test_get_binding_constraints_final_real_time_15_min_{date.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml"
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_final_real_time_15_min(
                date=date,
                end=end,
            )

        self._check_constraints(
            df,
            expected_columns=[
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Marginal Value",
            ],
            expected_interval=pd.Timedelta(minutes=15),
        )

    def test_get_binding_constraints_preliminary_real_time_5_min_latest(self) -> None:
        with api_vcr.use_cassette(
            "test_get_binding_constraints_preliminary_real_time_5_min_latest.yaml",
        ):
            df = self.iso.get_binding_constraints_preliminary_real_time_5_min(
                date="latest",
            )

        self._check_constraints(
            df,
            expected_columns=[
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Contingency Name",
                "Marginal Value",
            ],
            expected_interval=pd.Timedelta(minutes=5),
        )

    def test_get_binding_constraints_final_real_time_5_min_latest(self) -> None:
        with api_vcr.use_cassette(
            "test_get_binding_constraints_final_real_time_5_min_latest.yaml",
        ):
            df = self.iso.get_binding_constraints_final_real_time_5_min(date="latest")

        self._check_constraints(
            df,
            expected_columns=[
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Marginal Value",
            ],
            expected_interval=pd.Timedelta(minutes=5),
        )

    @pytest.mark.parametrize(
        "date,end",
        [
            (
                pd.Timestamp("2025-11-01").tz_localize("US/Eastern"),
                pd.Timestamp("2025-11-03").tz_localize("US/Eastern"),
            ),
        ],
    )
    def test_get_binding_constraints_preliminary_real_time_5_min_date_range(
        self,
        date: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        cassette_name = f"test_get_binding_constraints_preliminary_real_time_5_min_{date.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml"
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_preliminary_real_time_5_min(
                date=date,
                end=end,
            )

        self._check_constraints(
            df,
            expected_columns=[
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Contingency Name",
                "Marginal Value",
            ],
            expected_interval=pd.Timedelta(minutes=5),
        )

    @pytest.mark.parametrize(
        "date,end",
        [
            (
                pd.Timestamp("2025-11-01").tz_localize("US/Eastern"),
                pd.Timestamp("2025-11-03").tz_localize("US/Eastern"),
            ),
        ],
    )
    def test_get_binding_constraints_final_real_time_5_min_date_range(
        self,
        date: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        cassette_name = f"test_get_binding_constraints_final_real_time_5_min_{date.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml"
        with api_vcr.use_cassette(cassette_name):
            df = self.iso.get_binding_constraints_final_real_time_5_min(
                date=date,
                end=end,
            )

        self._check_constraints(
            df,
            expected_columns=[
                "Interval Start",
                "Interval End",
                "Constraint Name",
                "Marginal Value",
            ],
            expected_interval=pd.Timedelta(minutes=5),
        )

    def _check_fcm_reconfiguration(self, df: pd.DataFrame) -> None:
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert list(df.columns) == ISONE_FCM_RECONFIGURATION_COLUMNS
        assert df["Location Type"].isin(["Capacity Zone", "External Interface"]).all()
        assert df["Location ID"].dtype in [np.int64, np.float64]
        assert df["Location Name"].dtype == "object"
        assert df["Capacity Zone Type"].notna().any()
        numeric_cols = [
            "Total Supply Offers Submitted",
            "Total Demand Bids Submitted",
            "Total Supply Offers Cleared",
            "Total Demand Bids Cleared",
            "Net Capacity Cleared",
            "Clearing Price",
        ]
        for col in numeric_cols:
            assert df[col].dtype in [np.int64, np.float64]

    def test_get_fcm_reconfiguration_monthly_latest(self):
        with api_vcr.use_cassette("test_get_fcm_reconfiguration_monthly_latest.yaml"):
            result = self.iso.get_fcm_reconfiguration_monthly(date="latest")

            self._check_fcm_reconfiguration(result)

    @pytest.mark.parametrize(
        "date",
        [
            pd.Timestamp("2025-07-01").tz_localize("US/Eastern"),
            pd.Timestamp("2024-01-01").tz_localize("US/Eastern"),
        ],
    )
    def test_get_fcm_reconfiguration_monthly_date_range(self, date: pd.Timestamp):
        with api_vcr.use_cassette(
            f"test_get_fcm_reconfiguration_monthly_{date.strftime('%Y%m%d')}.yaml",
        ):
            result = self.iso.get_fcm_reconfiguration_monthly(
                date=date,
            )

            self._check_fcm_reconfiguration(result)
            assert result["Interval Start"].min().date() == date.date()

    def test_get_fcm_reconfiguration_annual_latest(self):
        with api_vcr.use_cassette("test_get_fcm_reconfiguration_annual_latest.yaml"):
            result = self.iso.get_fcm_reconfiguration_annual(date="latest")

            self._check_fcm_reconfiguration(result)

    @pytest.mark.parametrize(
        "date",
        [
            pd.Timestamp("2024-01-01").tz_localize("US/Eastern"),
        ],
    )
    def test_get_fcm_reconfiguration_annual_date_range(self, date: pd.Timestamp):
        with api_vcr.use_cassette(
            f"test_get_fcm_reconfiguration_annual_{date.strftime('%Y%m%d')}.yaml",
        ):
            result = self.iso.get_fcm_reconfiguration_annual(
                date=date,
            )

            self._check_fcm_reconfiguration(result)
            cp_start_year = date.year if date.month >= 6 else date.year - 1
            expected_cp_start = pd.Timestamp(
                year=cp_start_year,
                month=6,
                day=1,
                tz=date.tz,
            )
            assert result["Interval Start"].min().date() == expected_cp_start.date()

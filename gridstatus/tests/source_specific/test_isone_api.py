import numpy as np
import pandas as pd
import pytest

from gridstatus.isone_api.isone_api import ISONEAPI, ZONE_LOCATIONID_MAP
from gridstatus.isone_api.isone_api_constants import (
    ISONE_CAPACITY_FORECAST_7_DAY_COLUMNS,
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
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
            assert result["LMP"].dtype in [np.int64, np.float64]
            assert result["Energy"].dtype in [np.int64, np.float64]
            assert result["Congestion"].dtype in [np.int64, np.float64]
            assert result["Loss"].dtype in [np.int64, np.float64]

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_hourly_prelim_date_range(self, date, end):
        cassette_name = f"test_get_lmp_real_time_hourly_prelim_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_hourly_prelim(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
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
            assert result["LMP"].dtype in [np.int64, np.float64]
            assert result["Energy"].dtype in [np.int64, np.float64]
            assert result["Congestion"].dtype in [np.int64, np.float64]
            assert result["Loss"].dtype in [np.int64, np.float64]

    def test_get_lmp_real_time_hourly_final_latest(self):
        with api_vcr.use_cassette("test_get_lmp_real_time_hourly_final_latest.yaml"):
            result = self.iso.get_lmp_real_time_hourly_final(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
            assert result["LMP"].dtype in [np.int64, np.float64]
            assert result["Energy"].dtype in [np.int64, np.float64]
            assert result["Congestion"].dtype in [np.int64, np.float64]
            assert result["Loss"].dtype in [np.int64, np.float64]

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_hourly_final_date_range(self, date, end):
        cassette_name = f"test_get_lmp_real_time_hourly_final_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_hourly_final(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
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
            assert result["LMP"].dtype in [np.int64, np.float64]
            assert result["Energy"].dtype in [np.int64, np.float64]
            assert result["Congestion"].dtype in [np.int64, np.float64]
            assert result["Loss"].dtype in [np.int64, np.float64]

    def test_get_lmp_real_time_5_min_prelim_latest(self):
        with api_vcr.use_cassette("test_get_lmp_real_time_5_min_prelim_latest.yaml"):
            result = self.iso.get_lmp_real_time_5_min_prelim(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
            assert result["LMP"].dtype in [np.int64, np.float64]
            assert result["Energy"].dtype in [np.int64, np.float64]
            assert result["Congestion"].dtype in [np.int64, np.float64]
            assert result["Loss"].dtype in [np.int64, np.float64]
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(minutes=5)
            ).all()

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_5_min_prelim_date_range(self, date: str, end: str):
        cassette_name = f"test_get_lmp_real_time_5_min_prelim_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_5_min_prelim(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
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
                == pd.Timedelta(minutes=5)
            ).all()
            assert result["LMP"].dtype in [np.int64, np.float64]
            assert result["Energy"].dtype in [np.int64, np.float64]
            assert result["Congestion"].dtype in [np.int64, np.float64]
            assert result["Loss"].dtype in [np.int64, np.float64]

    def test_get_lmp_real_time_5_min_final_latest(self):
        with api_vcr.use_cassette("test_get_lmp_real_time_5_min_final_latest.yaml"):
            result = self.iso.get_lmp_real_time_5_min_final(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
            ]
            assert result["LMP"].dtype in [np.int64, np.float64]
            assert result["Energy"].dtype in [np.int64, np.float64]
            assert result["Congestion"].dtype in [np.int64, np.float64]
            assert result["Loss"].dtype in [np.int64, np.float64]
            assert (
                (result["Interval End"] - result["Interval Start"])
                == pd.Timedelta(minutes=5)
            ).all()

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_5_min_final_date_range(self, date: str, end: str):
        cassette_name = f"test_get_lmp_real_time_5_min_final_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_5_min_final(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Type",
                "LMP",
                "Energy",
                "Congestion",
                "Loss",
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
                == pd.Timedelta(minutes=5)
            ).all()
            assert result["LMP"].dtype in [np.int64, np.float64]
            assert result["Energy"].dtype in [np.int64, np.float64]
            assert result["Congestion"].dtype in [np.int64, np.float64]
            assert result["Loss"].dtype in [np.int64, np.float64]

    """get_capacity_forecast_7_day"""

    def _check_capacity_forecast_7_day_columns(self, result: pd.DataFrame) -> None:
        """Validate the DataFrame columns against the Pydantic model fields."""
        assert list(result.columns) == ISONE_CAPACITY_FORECAST_7_DAY_COLUMNS
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

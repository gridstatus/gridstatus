from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from gridstatus.base import NoDataFoundException
from gridstatus.isone_api.isone_api import ISONEAPI, ZONE_LOCATIONID_MAP
from gridstatus.isone_api.isone_api_constants import (
    ISONE_CAPACITY_FORECAST_7_DAY_COLUMNS,
    ISONE_FCM_RECONFIGURATION_COLUMNS,
    ISONE_FIVE_MIN_ESTIMATED_ZONAL_LOAD_COLUMNS,
    ISONE_FIVE_MIN_ZONAL_LOAD_FORECAST_COLUMNS,
    ISONE_MORNING_REPORT_COLUMNS,
    ISONE_RESERVE_ZONE_ALL_COLUMNS,
    ISONE_TOTAL_DEMAND_COLUMNS,
)
from gridstatus.isone_constants import (
    ISONE_ANCILLARY_SERVICES_ARD_OFFERS_COLUMNS,
    ISONE_ANCILLARY_SERVICES_DRR_OFFERS_COLUMNS,
    ISONE_ANCILLARY_SERVICES_ENERGY_OFFERS_COLUMNS,
    ISONE_ARD_BIDS_COLUMNS,
    ISONE_DRR_OFFERS_COLUMNS,
    ISONE_IMPORT_OFFERS_EXPORT_BIDS_COLUMNS,
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
        # `location_id == location_id` asserted nothing. The map's contract is that
        # every zone name resolves to an integer location id.
        for location_id in ZONE_LOCATIONID_MAP.values():
            assert isinstance(location_id, int)

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

    def test_get_marginal_fuel_type_latest(self):
        with api_vcr.use_cassette("test_get_marginal_fuel_type_latest.yaml"):
            result = self.iso.get_marginal_fuel_type(date="latest")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "Time" in result.columns
            assert isinstance(result["Time"].iloc[0], pd.Timestamp)

            fuel_cols = [col for col in result.columns if col != "Time"]
            assert len(fuel_cols) > 0
            for col in fuel_cols:
                assert result[col].dtype == bool

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_marginal_fuel_type_date_range(self, date, end):
        cassette_name = f"test_get_marginal_fuel_type_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_marginal_fuel_type(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert "Time" in result.columns

            assert min(result["Time"]).date() == pd.Timestamp(date).date()
            assert max(result["Time"]).date() == pd.Timestamp(
                end,
            ).date() - pd.Timedelta(days=1)

            assert all(isinstance(t, pd.Timestamp) for t in result["Time"])

            fuel_cols = [col for col in result.columns if col != "Time"]
            assert len(fuel_cols) > 0
            for col in fuel_cols:
                # Cross-day concat can upgrade bool->object when a fuel
                # category is absent on some days (e.g. Coal in newer data).
                assert result[col].dropna().isin([True, False]).all()
            assert (result[fuel_cols] == True).any(axis=1).sum() > 0  # noqa: E712

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

    """get_zonal_load_estimated_5_min"""

    def _check_zonal_load_estimated_5_min(self, df: pd.DataFrame) -> None:
        assert list(df.columns) == ISONE_FIVE_MIN_ESTIMATED_ZONAL_LOAD_COLUMNS
        assert df["Load Zone ID"].dtype in [np.int64, np.float64]
        assert df["Estimated Load"].dtype in [np.int64, np.float64]
        assert df["Estimated BTM Solar"].dtype in [np.int64, np.float64]
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=5)
        ).all()

    def test_get_zonal_load_estimated_5_min_latest(self):
        with api_vcr.use_cassette(
            "test_get_zonal_load_estimated_5_min_latest.yaml",
        ):
            result = self.iso.get_zonal_load_estimated_5_min(date="latest")

        self._check_zonal_load_estimated_5_min(result)

    @pytest.mark.parametrize(
        "date,end",
        [
            ("2025-11-01", "2025-11-03"),
            ("2025-03-08", "2025-03-10"),
        ],
    )
    def test_get_zonal_load_estimated_5_min_date_range(
        self,
        date: str,
        end: str,
    ):
        cassette_name = f"test_get_zonal_load_estimated_5_min_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_zonal_load_estimated_5_min(date=date, end=end)

        self._check_zonal_load_estimated_5_min(result)

        assert result["Interval Start"].min() == pd.Timestamp(date).tz_localize(
            self.iso.default_timezone,
        )
        assert result["Interval Start"].max() == pd.Timestamp(end).tz_localize(
            self.iso.default_timezone,
        ) - pd.Timedelta(minutes=5)

    """get_load_forecast_by_zone_5_min"""

    def _check_load_forecast_by_zone_5_min(self, df: pd.DataFrame) -> None:
        assert list(df.columns) == ISONE_FIVE_MIN_ZONAL_LOAD_FORECAST_COLUMNS
        assert df["Load Zone ID"].dtype in [np.int64, np.float64]
        assert df["Load Forecast"].dtype in [np.int64, np.float64]
        assert df["BTM Solar Forecast"].dtype in [np.int64, np.float64]
        assert df["Publish Time"].nunique() == 1
        assert (df["Publish Time"] >= df["Interval Start"].min()).all()
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=5)
        ).all()

    def test_get_load_forecast_by_zone_5_min_latest(self):
        with api_vcr.use_cassette(
            "test_get_load_forecast_by_zone_5_min_latest.yaml",
        ):
            result = self.iso.get_load_forecast_by_zone_5_min(date="latest")

        self._check_load_forecast_by_zone_5_min(result)
        assert result["Load Zone Name"].nunique() == 8
        assert result.index.tolist() == list(range(len(result)))

    """get_total_demand"""

    def _check_total_demand(self, df: pd.DataFrame) -> None:
        assert list(df.columns) == ISONE_TOTAL_DEMAND_COLUMNS
        for col in ISONE_TOTAL_DEMAND_COLUMNS[2:]:
            assert df[col].dtype in [np.int64, np.float64]
        assert (
            (df["Interval End"] - df["Interval Start"]) == pd.Timedelta(minutes=5)
        ).all()
        assert df["Interval Start"].is_unique
        assert df["Interval Start"].is_monotonic_increasing

    def test_get_total_demand_latest(self):
        with api_vcr.use_cassette("test_get_total_demand_latest.yaml"):
            result = self.iso.get_total_demand(date="latest")

        self._check_total_demand(result)
        assert len(result) == 1

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_total_demand_date_range(
        self,
        date: str,
        end: str,
    ):
        cassette_name = f"test_get_total_demand_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_total_demand(date=date, end=end)

        self._check_total_demand(result)

        assert result["Interval Start"].min() == pd.Timestamp(date).tz_localize(
            self.iso.default_timezone,
        )
        assert result["Interval Start"].max() == pd.Timestamp(end).tz_localize(
            self.iso.default_timezone,
        ) - pd.Timedelta(minutes=5)

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

    """get_morning_report"""

    def _check_morning_report_columns(self, result: pd.DataFrame) -> None:
        assert list(result.columns) == ISONE_MORNING_REPORT_COLUMNS
        assert result["Report Date"].dtype == object
        assert result["Prior Day"].dtype == object
        assert result["Prior Day Peak Hour"].dtype in [np.int64, np.float64]
        assert result["Capacity Supply Obligation"].dtype in [np.int64, np.float64]
        assert result["Boston High Temperature"].dtype in [np.int64, np.float64]
        assert result["Comments"].dtype == object

    @pytest.mark.parametrize(
        "date,end",
        [
            ("2024-06-15", "2024-06-16"),
            ("2019-06-15", "2019-06-16"),
        ],
    )
    def test_get_morning_report_date_range(self, date: str, end: str):
        cassette_name = (
            f"test_get_morning_report_{pd.Timestamp(date).strftime('%Y%m%d')}_"
            f"{pd.Timestamp(end).strftime('%Y%m%d')}.yaml"
        )
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_morning_report(date=date, end=end)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            self._check_morning_report_columns(result)

    def test_get_morning_report_multi_day(self):
        cassette_name = "test_get_morning_report_20240615_20240617.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_morning_report(
                date="2024-06-15",
                end="2024-06-17",
            )

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            self._check_morning_report_columns(result)

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
        assert "ARA" in df.columns
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
            assert result["ARA"].isin(["1", "2", "3"]).all()

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
            assert result["ARA"].isin(["1", "2", "3"]).all()
            unique_ara_values = result["ARA"].unique()
            assert len(unique_ara_values) >= 1
            cp_start_year = date.year if date.month >= 6 else date.year - 1
            expected_cp_start = pd.Timestamp(
                year=cp_start_year,
                month=6,
                day=1,
                tz=date.tz,
            )
            assert result["Interval Start"].min().date() == expected_cp_start.date()


HISTORICAL_OFFER_DATASETS = [
    (
        "ancillary_services_ard_offers_day_ahead",
        ISONE_ANCILLARY_SERVICES_ARD_OFFERS_COLUMNS,
        96,
    ),
    (
        "ancillary_services_drr_offers_day_ahead",
        ISONE_ANCILLARY_SERVICES_DRR_OFFERS_COLUMNS,
        24,
    ),
    (
        "ancillary_services_energy_offers_day_ahead",
        ISONE_ANCILLARY_SERVICES_ENERGY_OFFERS_COLUMNS,
        3844,
    ),
    ("ard_bids_day_ahead", ISONE_ARD_BIDS_COLUMNS, 624),
    ("drr_offers_day_ahead", ISONE_DRR_OFFERS_COLUMNS, 5544),
    (
        "import_offers_export_bids_day_ahead",
        ISONE_IMPORT_OFFERS_EXPORT_BIDS_COLUMNS,
        3723,
    ),
]


class TestISONEHistoricalOffersAPI:
    def setup_method(self):
        self.iso = ISONEAPI(sleep_seconds=0.1, max_retries=2)

    @pytest.mark.parametrize(
        "dataset,columns,first_day_rows",
        HISTORICAL_OFFER_DATASETS,
    )
    def test_historical_offers_date_range(self, dataset, columns, first_day_rows):
        with api_vcr.use_cassette(
            f"historical_offers_{dataset}_20260601_20260603.yaml",
        ):
            df = getattr(self.iso, f"get_{dataset}")(
                date=("2026-06-01", "2026-06-03"),
                error="raise",
            )
        assert list(df) == columns
        start = pd.Timestamp("2026-06-01", tz=self.iso.default_timezone)
        end = pd.Timestamp("2026-06-03", tz=self.iso.default_timezone)
        assert df["Interval Start"].min() == start
        assert df["Interval End"].max() == end
        assert (
            (df["Interval End"] - df["Interval Start"]).eq(pd.Timedelta(hours=1)).all()
        )
        assert df["Interval Start"].is_monotonic_increasing
        first = df[df["Interval Start"] < start + pd.Timedelta(days=1)]
        assert len(first) == first_day_rows
        for col in columns:
            if col.endswith(" ID"):
                assert df[col].dtype == "int64"
        if dataset.startswith("import_"):
            assert first.duplicated().sum() == 1240
            assert df["Market Type"].eq("DA").all()
            assert df.loc[df["Transaction Type"] == "FIXED", "Price"].isna().all()
        else:
            assert not df.duplicated(
                ["Interval Start", "Masked Lead Participant ID", "Masked Asset ID"],
            ).any()
            assert (
                df[["Masked Lead Participant ID", "Masked Asset ID"]]
                .notna()
                .all()
                .all()
            )
        if dataset == "ard_bids_day_ahead":
            assert first["Max Daily Award Limit"].gt(0).sum() == 120
            assert first["ARD Bid Curve"].map(len).eq(10).sum() == 10
        if dataset == "drr_offers_day_ahead":
            assert first["Max Daily Energy"].isna().sum() == 5376
            assert first["DRR Offer Curve"].map(len).ge(2).sum() == 72
        if dataset == "ancillary_services_energy_offers_day_ahead":
            row = first[
                (first["Masked Asset ID"] == 11031)
                & (first["Interval Start"].dt.hour == 5)
            ].iloc[0]
            assert row["TMSR Price"] == 2.4
            assert row["Offer Max MW"] == 47.0

    @pytest.mark.parametrize("dataset,columns,_", HISTORICAL_OFFER_DATASETS)
    @pytest.mark.parametrize("date,hours", [("2025-11-02", 25), ("2026-03-08", 23)])
    def test_historical_offers_dst(self, dataset, columns, _, date, hours):
        with api_vcr.use_cassette(f"historical_offers_{dataset}_{date}.yaml"):
            df = getattr(self.iso, f"get_{dataset}")(date=date)
        assert list(df) == columns
        if dataset == "ancillary_services_drr_offers_day_ahead":
            assert df.empty
            assert str(df["Interval Start"].dtype) == "datetime64[ns, US/Eastern]"
            return
        assert df["Interval Start"].nunique() == hours
        assert (
            (df["Interval End"] - df["Interval Start"]).eq(pd.Timedelta(hours=1)).all()
        )
        if hours == 25:
            repeated = df.loc[df["Interval Start"].dt.hour == 1, "Interval Start"]
            assert repeated.dt.tz_convert("UTC").nunique() == 2

    @pytest.mark.parametrize("dataset,_,__", HISTORICAL_OFFER_DATASETS)
    def test_historical_offers_reject_latest(self, dataset, _, __):
        with pytest.raises(ValueError, match="specific operating date"):
            getattr(self.iso, f"get_{dataset}")(date="latest")

    @pytest.mark.parametrize(
        "dataset",
        [
            "ard_bids_day_ahead",
            "drr_offers_day_ahead",
            "import_offers_export_bids_day_ahead",
        ],
    )
    def test_historical_offers_legacy(self, dataset):
        with api_vcr.use_cassette(f"historical_offers_{dataset}_20240101.yaml"):
            df = getattr(self.iso, f"get_{dataset}")(date="2024-01-01")
        assert not df.empty
        assert df["Interval Start"].nunique() == 24
        if not dataset.startswith("import_"):
            assert df["Max Daily Award Limit"].isna().all()

    def test_historical_offers_schema_drift(self):
        with patch.object(
            self.iso,
            "make_api_call",
            return_value={"RenamedContainer": {}},
        ):
            with pytest.raises(KeyError):
                self.iso.get_ard_bids_day_ahead("2026-06-01")
        with patch.object(
            self.iso,
            "make_api_call",
            return_value={
                "HbDayAheadArdBids": {
                    "HbDayAheadArdBid": [{"BeginDate": "2026-06-01T00:00:00-04:00"}],
                },
            },
        ):
            with pytest.raises(ValueError, match="Missing historical offer fields"):
                self.iso.get_ard_bids_day_ahead("2026-06-01")

    @pytest.mark.parametrize(
        "value",
        [
            {"Segment": {"Number": 2, "Price": -10, "Mw": 4.5}},
            {"Segment": [{"Number": 2, "Price": -10, "Mw": 4.5}]},
            [{"Segment": {"@Number": "2", "Price": -10, "Mw": 4.5}}],
        ],
    )
    def test_historical_offer_segment_shapes(self, value):
        assert self.iso._parse_offer_segments(value) == {
            "Segment 2 Price": -10,
            "Segment 2 MW": 4.5,
        }

    @pytest.mark.parametrize("numbers", [[11], [1, 1]])
    def test_historical_offer_invalid_segments(self, numbers):
        with pytest.raises(ValueError, match="Invalid or duplicate"):
            self.iso._parse_offer_segments(
                {"Segment": [{"Number": n, "Price": 1, "Mw": 2} for n in numbers]},
            )

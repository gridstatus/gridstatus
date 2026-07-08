import pandas as pd
import polars as pl
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
    ISONE_RESERVE_ZONE_FLOAT_COLUMNS,
    ISONE_TOTAL_DEMAND_COLUMNS,
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

    @staticmethod
    def _cell(df: pl.DataFrame, col: str, row: int = 0):
        return df.row(row, named=True)[col]

    @staticmethod
    def _is_numeric_dtype(dtype: pl.DataType) -> bool:
        return dtype.is_numeric()

    @staticmethod
    def _is_tz_datetime(dtype: pl.DataType, tz: str) -> bool:
        return isinstance(dtype, pl.Datetime) and dtype.time_zone == tz

    @staticmethod
    def _interval_equals(df: pl.DataFrame, duration: pd.Timedelta) -> bool:
        delta = df.select(
            (pl.col("Interval End") - pl.col("Interval Start")).alias("delta"),
        )["delta"]
        return delta.eq(duration).all()

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
            assert isinstance(result, pl.DataFrame)
            assert result.height == 20
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

            assert isinstance(result, pl.DataFrame)
            assert result.height == 1
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
            ]
            assert self._cell(result, "Location") == location
            assert self._cell(result, "Location Id") == ZONE_LOCATIONID_MAP[location]
            assert self._is_numeric_dtype(result.schema["Load"])

    def test_get_dayahead_hourly_demand_latest(self):
        with api_vcr.use_cassette("test_get_dayahead_hourly_demand_latest.yaml"):
            result = self.iso.get_dayahead_hourly_demand(
                date="latest",
                locations=["NEPOOL AREA"],
            )
            assert isinstance(result, pl.DataFrame)
            assert result.height == 1
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
            ]
            assert self._cell(result, "Location") == "NEPOOL AREA"
            assert self._cell(result, "Location Id") == 32
            assert self._is_numeric_dtype(result.schema["Load"])

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

            assert isinstance(result, pl.DataFrame)
            assert result.height == len(locations)
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
            assert result.schema["Load"].is_numeric()

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

            assert isinstance(result, pl.DataFrame)
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
            assert result.schema["Load"].is_numeric()
            assert result["Interval Start"].min().date() == pd.Timestamp(date).date()
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

            assert isinstance(result, pl.DataFrame)
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
            assert result.schema["Load"].is_numeric()
            assert result["Interval Start"].min().date() == pd.Timestamp(date).date()
            assert max(result["Interval End"]).date() == pd.Timestamp(end).date()

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_load_forecast_hourly(self, date, end):
        cassette_name = f"test_get_load_forecast_hourly_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_load_forecast_hourly(date=date, end=end)

            assert isinstance(result, pl.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Load Forecast",
                "Net Load Forecast",
            ]
            assert (
                result["Interval Start"].min().date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert self._is_numeric_dtype(result.schema["Load Forecast"])
            assert self._is_numeric_dtype(result.schema["Net Load Forecast"])
            assert result.select(pl.col("Load Forecast") > 0).to_series().all()
            assert self._interval_equals(result, pd.Timedelta(hours=1))

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_reliability_region_load_forecast(self, date, end):
        cassette_name = f"test_get_reliability_region_load_forecast_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_reliability_region_load_forecast(date=date, end=end)

            assert isinstance(result, pl.DataFrame)
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Publish Time",
                "Location",
                "Load Forecast",
                "Regional Percentage",
            ]
            assert (
                result["Interval Start"].min().date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert self._interval_equals(result, pd.Timedelta(hours=1))
            assert set(result["Location"].unique().to_list()) == {
                ".Z.CONNECTICUT",
                ".Z.MAINE",
                ".Z.NEWHAMPSHIRE",
                ".Z.RHODEISLAND",
                ".Z.VERMONT",
                ".Z.SEMASS",
                ".Z.WCMASS",
                ".Z.NEMASSBOST",
            }
            assert self._is_numeric_dtype(result.schema["Load Forecast"])
            assert result.select(pl.col("Load Forecast") > 0).to_series().all()
            assert result.schema["Regional Percentage"] == pl.Float64
            assert (
                (result["Regional Percentage"] >= 0)
                & (result["Regional Percentage"] <= 100)
            ).all()
            grouped = result.group_by(["Interval Start", "Publish Time"]).agg(
                pl.col("Regional Percentage").sum(),
            )
            assert (
                grouped.select(pl.col("Regional Percentage").is_between(99.9, 100.1))
                .to_series()
                .all()
            )

    def test_get_fuel_mix_latest(self):
        with api_vcr.use_cassette("test_get_fuel_mix_latest.yaml"):
            result = self.iso.get_fuel_mix(date="latest")

            assert isinstance(result, pl.DataFrame)
            assert result.height == 1
            assert "Time" in result.columns

            assert self._is_tz_datetime(
                result.schema["Time"],
                self.iso.default_timezone,
            )
            numeric_cols = [col for col in result.columns if col != "Time"]
            for col in numeric_cols:
                assert self._is_numeric_dtype(result.schema[col])
                assert result.select(pl.col(col) >= 0).to_series().all()

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_fuel_mix_date_range(self, date, end):
        cassette_name = f"test_get_fuel_mix_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_fuel_mix(date=date, end=end)

            assert isinstance(result, pl.DataFrame)
            assert "Time" in result.columns

            assert min(result["Time"]).date() == pd.Timestamp(date).date()
            assert max(result["Time"]).date() == pd.Timestamp(
                end,
            ).date() - pd.Timedelta(
                days=1,
            )

            assert self._is_tz_datetime(
                result.schema["Time"],
                self.iso.default_timezone,
            )
            numeric_cols = [col for col in result.columns if col != "Time"]
            for col in numeric_cols:
                assert self._is_numeric_dtype(result.schema[col])
                assert (
                    result.select(
                        pl.sum_horizontal([pl.col(c) for c in numeric_cols]) > 0,
                    )
                    .to_series()
                    .all()
                )

    def test_get_marginal_fuel_type_latest(self):
        with api_vcr.use_cassette("test_get_marginal_fuel_type_latest.yaml"):
            result = self.iso.get_marginal_fuel_type(date="latest")

            assert isinstance(result, pl.DataFrame)
            assert result.height == 1
            assert "Time" in result.columns
            assert self._is_tz_datetime(
                result.schema["Time"],
                self.iso.default_timezone,
            )

            fuel_cols = [col for col in result.columns if col != "Time"]
            assert len(fuel_cols) > 0
            for col in fuel_cols:
                assert result.schema[col] == pl.Boolean

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_marginal_fuel_type_date_range(self, date, end):
        cassette_name = f"test_get_marginal_fuel_type_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_marginal_fuel_type(date=date, end=end)

            assert isinstance(result, pl.DataFrame)
            assert "Time" in result.columns

            assert min(result["Time"]).date() == pd.Timestamp(date).date()
            assert max(result["Time"]).date() == pd.Timestamp(
                end,
            ).date() - pd.Timedelta(days=1)

            assert self._is_tz_datetime(
                result.schema["Time"],
                self.iso.default_timezone,
            )

            fuel_cols = [col for col in result.columns if col != "Time"]
            assert len(fuel_cols) > 0
            for col in fuel_cols:
                # Cross-day concat can upgrade bool->object when a fuel
                # category is absent on some days (e.g. Coal in newer data).
                assert result.select(
                    pl.col(col).drop_nulls().is_in([True, False]).all(),
                ).item()
            assert (
                result.select(pl.any_horizontal([pl.col(c) for c in fuel_cols]))
                .to_series()
                .sum()
                > 0
            )

    def test_get_load_hourly_latest(self):
        with api_vcr.use_cassette("test_get_load_hourly_latest.yaml"):
            result = self.iso.get_load_hourly(date="latest")

            assert isinstance(result, pl.DataFrame)
            assert result.height == 1
            assert list(result.columns) == [
                "Interval Start",
                "Interval End",
                "Location",
                "Location Id",
                "Load",
                "Native Load",
                "ARD Demand",
            ]
            assert self._cell(result, "Location") == "NEPOOL AREA"
            assert self._cell(result, "Location Id") == 32
            assert self._is_numeric_dtype(result.schema["Load"])
            assert self._is_numeric_dtype(result.schema["Native Load"])
            assert self._is_numeric_dtype(result.schema["ARD Demand"])

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

            assert isinstance(result, pl.DataFrame)
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
            assert result.schema["Load"].is_numeric()
            assert result.schema["Native Load"].is_numeric()
            assert result.schema["ARD Demand"].is_numeric()
            assert result["Interval Start"].min().date() == pd.Timestamp(date).date()
            assert max(result["Interval End"]).date() == pd.Timestamp(end).date()
            assert self._interval_equals(result, pd.Timedelta(hours=1))

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

            assert self._interval_equals(
                df,
                pd.Timedelta(
                    hours=1,
                ),
            )

            assert sorted(df["Location"].unique().to_list()) == [
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
            assert df.height == 24 * 2 * 6 + 6
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
            assert df.height == 24 * 2 * 6 - 6

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

            assert self._interval_equals(
                df,
                pd.Timedelta(
                    minutes=15,
                ),
            )

            assert sorted(df["Location"].unique().to_list()) == [".I.ROSETON 345 1"]

    def test_get_interchange_15_min_dst_end(self):
        start = self.local_start_of_day(DST_CHANGE_TEST_DATES[0][0])
        end = self.local_start_of_day(DST_CHANGE_TEST_DATES[0][1])
        with api_vcr.use_cassette(
            f"test_get_interchange_15_min_dst_end_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.yaml",
        ):
            df = self.iso.get_interchange_15_min(start, end)

            # ISONE does not publish data for the repeated hour so there are no extra
            # data points. 24 hours * 4 intervals per hour * 2 days
            assert df.height == 24 * 4 * 2

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
            assert df.height == 24 * 4 * 2 - 4

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

            assert self._interval_equals(
                df,
                pd.Timedelta(
                    minutes=5,
                ),
            )

            assert sorted(df["Location"].unique().to_list()) == [
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
            assert df.height == 12 * 24 * 2 * 6 + 6 * 12

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
            assert df.height == 12 * 24 * 2 * 6 - 6 * 12

            assert df["Interval Start"].min() == start
            assert df["Interval End"].max() == end

    """get_zonal_load_estimated_5_min"""

    def _check_zonal_load_estimated_5_min(self, df: pl.DataFrame) -> None:
        assert list(df.columns) == ISONE_FIVE_MIN_ESTIMATED_ZONAL_LOAD_COLUMNS
        assert self._is_numeric_dtype(df.schema["Load Zone ID"])
        assert self._is_numeric_dtype(df.schema["Estimated Load"])
        assert self._is_numeric_dtype(df.schema["Estimated BTM Solar"])
        assert self._interval_equals(df, pd.Timedelta(minutes=5))

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

    def _check_load_forecast_by_zone_5_min(self, df: pl.DataFrame) -> None:
        assert list(df.columns) == ISONE_FIVE_MIN_ZONAL_LOAD_FORECAST_COLUMNS
        assert self._is_numeric_dtype(df.schema["Load Zone ID"])
        assert self._is_numeric_dtype(df.schema["Load Forecast"])
        assert self._is_numeric_dtype(df.schema["BTM Solar Forecast"])
        assert df["Publish Time"].n_unique() == 1
        assert (
            df.select(pl.col("Publish Time") >= pl.col("Interval Start").min())
            .to_series()
            .all()
        )
        assert self._interval_equals(df, pd.Timedelta(minutes=5))

    def test_get_load_forecast_by_zone_5_min_latest(self):
        with api_vcr.use_cassette(
            "test_get_load_forecast_by_zone_5_min_latest.yaml",
        ):
            result = self.iso.get_load_forecast_by_zone_5_min(date="latest")

        self._check_load_forecast_by_zone_5_min(result)
        assert result["Load Zone Name"].n_unique() == 8
        assert result.height == result.height

    """get_total_demand"""

    def _check_total_demand(self, df: pl.DataFrame) -> None:
        assert list(df.columns) == ISONE_TOTAL_DEMAND_COLUMNS
        for col in ISONE_TOTAL_DEMAND_COLUMNS[2:]:
            assert self._is_numeric_dtype(df.schema[col])
        assert self._interval_equals(df, pd.Timedelta(minutes=5))
        assert df["Interval Start"].is_unique()
        assert df["Interval Start"].is_sorted()

    def test_get_total_demand_latest(self):
        with api_vcr.use_cassette("test_get_total_demand_latest.yaml"):
            result = self.iso.get_total_demand(date="latest")

        self._check_total_demand(result)
        assert result.height == 1

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

            assert isinstance(result, pl.DataFrame)
            assert result.height > 0
            self._check_lmp_columns(result)

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_hourly_prelim_date_range(self, date, end):
        cassette_name = f"test_get_lmp_real_time_hourly_prelim_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_hourly_prelim(date=date, end=end)

            assert isinstance(result, pl.DataFrame)
            self._check_lmp_columns(result)
            assert (
                result["Interval Start"].min().date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert self._interval_equals(result, pd.Timedelta(hours=1))

    def test_get_lmp_real_time_hourly_final_latest(self):
        with api_vcr.use_cassette("test_get_lmp_real_time_hourly_final_latest.yaml"):
            result = self.iso.get_lmp_real_time_hourly_final(date="latest")

            assert isinstance(result, pl.DataFrame)
            assert result.height > 0
            self._check_lmp_columns(result)

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_hourly_final_date_range(self, date, end):
        cassette_name = f"test_get_lmp_real_time_hourly_final_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_hourly_final(date=date, end=end)

            assert isinstance(result, pl.DataFrame)
            self._check_lmp_columns(result)
            assert (
                result["Interval Start"].min().date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert self._interval_equals(result, pd.Timedelta(hours=1))

    def test_get_lmp_real_time_5_min_prelim_latest(self):
        with api_vcr.use_cassette("test_get_lmp_real_time_5_min_prelim_latest.yaml"):
            result = self.iso.get_lmp_real_time_5_min_prelim(date="latest")

            assert isinstance(result, pl.DataFrame)
            assert result.height > 0
            self._check_lmp_columns(result)
            assert self._interval_equals(result, pd.Timedelta(minutes=5))

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_5_min_prelim_date_range(self, date: str, end: str):
        cassette_name = f"test_get_lmp_real_time_5_min_prelim_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_5_min_prelim(date=date, end=end)

            assert isinstance(result, pl.DataFrame)
            self._check_lmp_columns(result)
            assert (
                result["Interval Start"].min().date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert self._interval_equals(result, pd.Timedelta(minutes=5))

    def test_get_lmp_real_time_5_min_final_latest(self):
        with api_vcr.use_cassette("test_get_lmp_real_time_5_min_final_latest.yaml"):
            result = self.iso.get_lmp_real_time_5_min_final(date="latest")

            assert isinstance(result, pl.DataFrame)
            assert result.height > 0
            self._check_lmp_columns(result)
            assert self._interval_equals(result, pd.Timedelta(minutes=5))

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_lmp_real_time_5_min_final_date_range(self, date: str, end: str):
        cassette_name = f"test_get_lmp_real_time_5_min_final_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_lmp_real_time_5_min_final(date=date, end=end)

            assert isinstance(result, pl.DataFrame)
            self._check_lmp_columns(result)
            assert (
                result["Interval Start"].min().date()
                == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
            )
            assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
                self.iso.default_timezone,
            )
            assert self._interval_equals(result, pd.Timedelta(minutes=5))

    """get_capacity_forecast_7_day"""

    def _check_capacity_forecast_7_day_columns(self, result: pl.DataFrame) -> None:
        """Validate the DataFrame columns against the Pydantic model fields."""
        assert list(result.columns) == ISONE_CAPACITY_FORECAST_7_DAY_COLUMNS
        for col in ["Interval Start", "Interval End", "Publish Time"]:
            assert self._is_tz_datetime(result.schema[col], self.iso.default_timezone)
        numeric_cols = [
            "High Temperature Boston",
            "Dew Point Boston",
            "High Temperature Hartford",
            "Dew Point Hartford",
            "Total Capacity Supply Obligation",
            "Anticipated Cold Weather Outages",
            "Other Generation Outages",
            "Anticipated Delist MW Offered",
            "Total Generation Available",
            "Import at Time of Peak",
            "Total Available Generation and Imports",
            "Projected Peak Load",
            "Replacement Reserve Requirement",
            "Required Reserve",
            "Required Reserve Including Replacement",
            "Total Load Plus Required Reserve",
            "Projected Surplus or Deficiency",
            "Available Demand Response Resources",
        ]
        for col in numeric_cols:
            assert self._is_numeric_dtype(result.schema[col])
        for col in [
            "Load Relief Actions Anticipated",
            "Power Watch",
            "Power Warning",
            "Cold Weather Watch",
            "Cold Weather Warning",
            "Cold Weather Event",
        ]:
            assert result.schema[col] in (pl.Utf8, pl.Null)

    def test_get_capacity_forecast_7_day_latest(self):
        with api_vcr.use_cassette("test_get_capacity_forecast_7_day_latest.yaml"):
            result = self.iso.get_capacity_forecast_7_day(date="latest")

            assert isinstance(result, pl.DataFrame)
            assert result.height > 0
            self._check_capacity_forecast_7_day_columns(result)

    @pytest.mark.parametrize(
        "date,end",
        DST_CHANGE_TEST_DATES,
    )
    def test_get_capacity_forecast_7_day_date_range(self, date: str, end: str):
        cassette_name = f"test_get_capacity_forecast_7_day_{pd.Timestamp(date).strftime('%Y%m%d')}_{pd.Timestamp(end).strftime('%Y%m%d')}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_capacity_forecast_7_day(date=date, end=end)

            assert isinstance(result, pl.DataFrame)
            self._check_capacity_forecast_7_day_columns(result)

    """get_morning_report"""

    def _check_morning_report_columns(self, result: pl.DataFrame) -> None:
        assert list(result.columns) == ISONE_MORNING_REPORT_COLUMNS
        assert result.schema["Report Date"] == pl.Date
        assert result.schema["Prior Day"] == pl.Date
        assert self._is_numeric_dtype(result.schema["Prior Day Peak Hour"])
        assert self._is_numeric_dtype(result.schema["Capacity Supply Obligation"])
        assert self._is_numeric_dtype(result.schema["Boston High Temperature"])
        assert result.schema["Comments"] == pl.Utf8

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

            assert isinstance(result, pl.DataFrame)
            assert result.height == 1
            self._check_morning_report_columns(result)

    def test_get_morning_report_multi_day(self):
        cassette_name = "test_get_morning_report_20240615_20240617.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_morning_report(
                date="2024-06-15",
                end="2024-06-17",
            )

            assert isinstance(result, pl.DataFrame)
            assert result.height == 2
            self._check_morning_report_columns(result)

    """get_regulation_clearing_prices_real_time_5_min"""

    def _check_regulation_clearing_prices_real_time_5_min(self, df: pl.DataFrame):
        assert list(df.columns) == [
            "Interval Start",
            "Interval End",
            "Reg Service Clearing Price",
            "Reg Capacity Clearing Price",
        ]
        assert df.schema["Reg Service Clearing Price"] == pl.Float64
        assert df.schema["Reg Capacity Clearing Price"] == pl.Float64

        assert self._interval_equals(df, pd.Timedelta(minutes=5))

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
        df: pl.DataFrame,
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
        assert df.schema["EIR Designation MW"] == pl.Float64
        assert df.schema["FER Clearing Price"] == pl.Float64
        assert df.schema["Forecasted Energy Req MW"] == pl.Float64
        assert df.schema["Ten Min Spin Req MW"] == pl.Float64
        assert df.schema["TMNSR Clearing Price"] == pl.Float64
        assert df.schema["TMNSR Designation MW"] == pl.Float64
        assert df.schema["TMOR Clearing Price"] == pl.Float64
        assert df.schema["TMOR Designation MW"] == pl.Float64
        assert df.schema["TMSR Clearing Price"] == pl.Float64
        assert df.schema["TMSR Designation MW"] == pl.Float64
        assert df.schema["Total Ten Min Req MW"] == pl.Float64
        assert df.schema["Total Thirty Min Req MW"] == pl.Float64

        assert self._interval_equals(df, pd.Timedelta(hours=1))

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

    def _check_lmp_columns(self, df: pl.DataFrame):
        """Shared helper to validate LMP column structure and dtypes."""
        assert list(df.columns) == LMP_COLUMNS
        for col in ["LMP", "Energy", "Congestion", "Loss"]:
            assert self._is_numeric_dtype(df.schema[col])

    """get_reserve_zone_prices_designations_real_time_5_min"""

    def _check_reserve_zone_prices_designations(
        self,
        df: pl.DataFrame,
        interval: pd.Timedelta,
    ):
        """Shared helper to validate reserve zone price data across different intervals."""
        assert list(df.columns) == ISONE_RESERVE_ZONE_ALL_COLUMNS

        assert df.schema["Reserve Zone Id"] == pl.Int64
        assert df.schema["Reserve Zone Name"] == pl.Utf8
        for col in ISONE_RESERVE_ZONE_FLOAT_COLUMNS:
            assert df.schema[col] == pl.Float64

        assert sorted(df["Reserve Zone Id"].unique().to_list()) == [
            7000,
            7001,
            7002,
            7003,
        ]

        assert self._interval_equals(df, interval)

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
        df: pl.DataFrame,
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
        assert df.schema["Expected Closeout Charge"] == pl.Float64
        assert df.schema["Expected Closeout Charge Override"] == pl.Float64
        assert df.schema["Expected RT Hub LMP"] == pl.Float64
        assert df.schema["Percentile 10 RT Hub LMP"] == pl.Float64
        assert df.schema["Percentile 25 RT Hub LMP"] == pl.Float64
        assert df.schema["Percentile 75 RT Hub LMP"] == pl.Float64
        assert df.schema["Percentile 90 RT Hub LMP"] == pl.Float64
        assert df.schema["SPC Load Forecast MW"] == pl.Float64
        assert df.schema["Strike Price"] == pl.Float64

        assert self._interval_equals(df, pd.Timedelta(hours=1))

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
        df: pl.DataFrame,
        expected_columns: list[str],
        expected_interval: pd.Timedelta,
    ) -> None:
        assert list(df.columns) == expected_columns
        assert self._is_tz_datetime(
            df.schema["Interval Start"],
            self.iso.default_timezone,
        )
        assert self._is_tz_datetime(
            df.schema["Interval End"],
            self.iso.default_timezone,
        )
        assert self._interval_equals(df, expected_interval)
        assert self._is_numeric_dtype(df.schema["Marginal Value"])

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

    def _check_fcm_reconfiguration(self, df: pl.DataFrame) -> None:
        assert isinstance(df, pl.DataFrame)
        assert df.height > 0
        assert list(df.columns) == ISONE_FCM_RECONFIGURATION_COLUMNS
        assert "ARA" in df.columns
        assert (
            df.select(
                pl.col("Location Type").is_in(["Capacity Zone", "External Interface"]),
            )
            .to_series()
            .all()
        )
        assert self._is_numeric_dtype(df.schema["Location ID"])
        assert df.schema["Location Name"] == pl.Utf8
        assert df["Capacity Zone Type"].is_not_null().any()
        numeric_cols = [
            "Total Supply Offers Submitted",
            "Total Demand Bids Submitted",
            "Total Supply Offers Cleared",
            "Total Demand Bids Cleared",
            "Net Capacity Cleared",
            "Clearing Price",
        ]
        for col in numeric_cols:
            assert self._is_numeric_dtype(df.schema[col])

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
            assert result.select(pl.col("ARA").is_in([1, 2, 3])).to_series().all()

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
            assert result.select(pl.col("ARA").is_in([1, 2, 3])).to_series().all()
            unique_ara_values = result["ARA"].unique().to_list()
            assert len(unique_ara_values) >= 1
            cp_start_year = date.year if date.month >= 6 else date.year - 1
            expected_cp_start = pd.Timestamp(
                year=cp_start_year,
                month=6,
                day=1,
                tz=date.tz,
            )
            assert result["Interval Start"].min().date() == expected_cp_start.date()

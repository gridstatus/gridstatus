import numpy as np
import pandas as pd
import pytest

from gridstatus.isone_api.isone_api import ISONEAPI, ZONE_LOCATIONID_MAP
from gridstatus.tests.vcr_utils import RECORD_MODE, setup_vcr

api_vcr = setup_vcr(
    source="isone_api",
    record_mode=RECORD_MODE,
)

TEST_DATES = [
    ("2024-11-02", "2024-11-04"),
    ("2024-03-09", "2024-03-11"),
]

TEST_MULTIPLE_LOCATIONS = [
    (".Z.MAINE", ".Z.NEWHAMPSHIRE"),
]

TEST_SINGLE_LOCATIONS = [loc for pair in TEST_MULTIPLE_LOCATIONS for loc in pair]


class TestISONEAPI:
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

    @api_vcr.use_cassette("test_get_locations.yaml")
    def test_get_locations(self):
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

    @api_vcr.use_cassette("test_get_dayahead_hourly_demand_latest.yaml")
    def test_get_dayahead_hourly_demand_latest(self):
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
        [(date, end, TEST_SINGLE_LOCATIONS) for date, end in TEST_DATES],
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
        [(date, end, TEST_SINGLE_LOCATIONS) for date, end in TEST_DATES],
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
        TEST_DATES,
    )
    def test_get_hourly_load_forecast(self, date, end):
        cassette_name = f"test_get_hourly_load_forecast_{date}_{end}.yaml"
        with api_vcr.use_cassette(cassette_name):
            result = self.iso.get_hourly_load_forecast(date=date, end=end)

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
            (result["Interval End"] - result["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    @pytest.mark.parametrize(
        "date,end",
        TEST_DATES,
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

    @api_vcr.use_cassette("test_get_fuel_mix_latest.yaml")
    def test_get_fuel_mix_latest(self):
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
        TEST_DATES,
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
        TEST_DATES,
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

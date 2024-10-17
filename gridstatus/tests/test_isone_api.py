import numpy as np
import pandas as pd
import pytest

from gridstatus.isone_api.isone_api import ISONEAPI, ZONE_LOCATIONID_MAP
from tests.vcr_utils import record_mode, setup_vcr

# NOTE(Kladar): Since these support date ranges and make multiple API calls in a single get
# request, vcr needs to combine them into a single cassette.
DATE_RANGE_ENDPOINTS = [
    "realtimehourlydemand",
    "dayaheadhourlydemand",
    "hourlyloadforecast",
    "reliabilityregionloadforecast",
]

api_vcr = setup_vcr(
    source="isone",
    record_mode=record_mode,
    date_range_endpoints=DATE_RANGE_ENDPOINTS,
)


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
        assert len(result) == 19
        assert list(result.columns) == [
            "LocationID",
            "LocationType",
            "LocationName",
            "AreaType",
        ]

    @api_vcr.use_cassette("test_get_realtime_hourly_demand_latest.yaml")
    def test_get_realtime_hourly_demand_latest(self):
        result = self.iso.get_realtime_hourly_demand(
            date="latest",
            locations=[".Z.MAINE"],
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
        assert result["Location"].iloc[0] == ".Z.MAINE"
        assert result["Location Id"].iloc[0] == 4001
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

    @api_vcr.use_cassette("test_get_realtime_hourly_demand_multiple_locations.yaml")
    def test_get_realtime_hourly_demand_multiple_locations(self):
        result = self.iso.get_realtime_hourly_demand(
            date="latest",
            locations=[".Z.MAINE", ".Z.NEWHAMPSHIRE"],
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == [
            "Interval Start",
            "Interval End",
            "Location",
            "Location Id",
            "Load",
        ]
        assert set(result["Location"]) == {".Z.MAINE", ".Z.NEWHAMPSHIRE"}
        assert set(result["Location Id"]) == {4001, 4002}
        assert all(isinstance(load, (int, float)) for load in result["Load"])

    @pytest.mark.parametrize(
        "date,end,locations,expected_rows,expected_location_ids",
        [
            (
                "2024-10-06",
                "2024-10-08",
                [".Z.MAINE", ".Z.NEWHAMPSHIRE"],
                96,
                {4001, 4002},
            ),
        ],
    )
    @api_vcr.use_cassette("test_get_realtime_hourly_demand_date_range.yaml")
    def test_get_realtime_hourly_demand_date_range(
        self,
        date,
        end,
        locations,
        expected_rows,
        expected_location_ids,
    ):
        result = self.iso.get_realtime_hourly_demand(
            date=date,
            end=end,
            locations=locations,
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == expected_rows
        assert list(result.columns) == [
            "Interval Start",
            "Interval End",
            "Location",
            "Location Id",
            "Load",
        ]
        assert set(result["Location"]) == set(locations)
        assert set(result["Location Id"]) == expected_location_ids
        assert all(isinstance(load, (int, float)) for load in result["Load"])
        assert min(result["Interval Start"]).date() == pd.Timestamp(date).date()
        assert max(result["Interval End"]).date() == pd.Timestamp(end).date()
        for location in locations:
            assert len(result[result["Location"] == location]) == expected_rows // len(
                locations,
            )

    @pytest.mark.parametrize(
        "date,end,locations,expected_rows,expected_location_ids",
        [
            (
                "2024-10-06",
                "2024-10-08",
                [".Z.MAINE", ".Z.NEWHAMPSHIRE"],
                96,
                {4001, 4002},
            ),
        ],
    )
    @api_vcr.use_cassette("test_get_dayahead_hourly_demand_date_range.yaml")
    def test_get_dayahead_hourly_demand_date_range(
        self,
        date,
        end,
        locations,
        expected_rows,
        expected_location_ids,
    ):
        result = self.iso.get_dayahead_hourly_demand(
            date=date,
            end=end,
            locations=locations,
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == expected_rows
        assert list(result.columns) == [
            "Interval Start",
            "Interval End",
            "Location",
            "Location Id",
            "Load",
        ]
        assert set(result["Location"]) == set(locations)
        assert set(result["Location Id"]) == expected_location_ids
        assert all(isinstance(load, (int, float)) for load in result["Load"])
        assert min(result["Interval Start"]).date() == pd.Timestamp(date).date()
        assert max(result["Interval End"]).date() == pd.Timestamp(end).date()
        for location in locations:
            assert len(result[result["Location"] == location]) == expected_rows // len(
                locations,
            )

    @pytest.mark.parametrize(
        "date,end,expected_columns",
        [
            (
                "2023-05-01",
                "2023-05-03",
                ["Interval Start", "Interval End", "Publish Time", "Load", "Net Load"],
            ),
        ],
    )
    @api_vcr.use_cassette("test_get_hourly_load_forecast.yaml")
    def test_get_hourly_load_forecast(self, date, end, expected_columns):
        result = self.iso.get_hourly_load_forecast(date=date, end=end)

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns
        assert (
            min(result["Interval Start"]).date()
            == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
        )
        assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
            self.iso.default_timezone,
        )
        assert result["Load"].dtype in [np.int64, np.float64]
        assert result["Net Load"].dtype in [np.int64, np.float64]
        assert (result["Load"] > 0).all()
        assert (
            (result["Interval End"] - result["Interval Start"]) == pd.Timedelta(hours=1)
        ).all()

    @pytest.mark.parametrize(
        "date,end,expected_columns",
        [
            (
                "2023-05-01",
                "2023-05-03",
                [
                    "Interval Start",
                    "Interval End",
                    "Publish Time",
                    "Location",
                    "Load",
                    "Regional Percentage",
                ],
            ),
        ],
    )
    @api_vcr.use_cassette("test_get_reliability_region_load_forecast.yaml")
    def test_get_reliability_region_load_forecast(self, date, end, expected_columns):
        result = self.iso.get_reliability_region_load_forecast(date=date, end=end)

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns
        assert (
            min(result["Interval Start"]).date()
            == pd.Timestamp(date).tz_localize(self.iso.default_timezone).date()
        )
        assert max(result["Interval End"]) == pd.Timestamp(end).tz_localize(
            self.iso.default_timezone,
        )
        assert (
            (result["Interval End"] - result["Interval Start"]) == pd.Timedelta(hours=1)
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
        assert result["Load"].dtype in [np.int64, np.float64]
        assert (result["Load"] > 0).all()
        assert result["Regional Percentage"].dtype == np.float64
        assert (
            (result["Regional Percentage"] >= 0)
            & (result["Regional Percentage"] <= 100)
        ).all()
        grouped = result.groupby(["Interval Start", "Publish Time"])
        assert (grouped["Regional Percentage"].sum().between(99.9, 100.1)).all()

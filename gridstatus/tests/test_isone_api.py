import json
import os
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import vcr

from gridstatus.isone_api.isone_api import ISONEAPI, ZONE_LOCATIONID_MAP

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "isone")


@pytest.fixture
def isone_locations():
    with open(os.path.join(FIXTURES_DIR, "isone_locations.json"), "r") as f:
        return json.load(f)


@pytest.fixture
def isone_realtime_hourly_demand_latest():
    with open(
        os.path.join(FIXTURES_DIR, "isone_realtime_hourly_demand_latest.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_realtime_hourly_demand_latest_multiple():
    with open(
        os.path.join(FIXTURES_DIR, "isone_realtime_hourly_demand_latest_multiple.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_dayahead_hourly_demand_latest():
    with open(
        os.path.join(FIXTURES_DIR, "isone_dayahead_hourly_demand_latest.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_realtime_hourly_demand_location1_date1():
    with open(
        os.path.join(FIXTURES_DIR, "isone_realtime_hourly_demand_location1_date1.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_realtime_hourly_demand_location1_date2():
    with open(
        os.path.join(FIXTURES_DIR, "isone_realtime_hourly_demand_location1_date2.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_realtime_hourly_demand_location2_date1():
    with open(
        os.path.join(FIXTURES_DIR, "isone_realtime_hourly_demand_location2_date1.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_realtime_hourly_demand_location2_date2():
    with open(
        os.path.join(FIXTURES_DIR, "isone_realtime_hourly_demand_location2_date2.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_dayahead_hourly_demand_location1_date1():
    with open(
        os.path.join(FIXTURES_DIR, "isone_dayahead_hourly_demand_location1_date1.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_dayahead_hourly_demand_location1_date2():
    with open(
        os.path.join(FIXTURES_DIR, "isone_dayahead_hourly_demand_location1_date2.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_dayahead_hourly_demand_location2_date1():
    with open(
        os.path.join(FIXTURES_DIR, "isone_dayahead_hourly_demand_location2_date1.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_dayahead_hourly_demand_location2_date2():
    with open(
        os.path.join(FIXTURES_DIR, "isone_dayahead_hourly_demand_location2_date2.json"),
        "r",
    ) as f:
        return json.load(f)


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

    @patch("gridstatus.isone_api.isone_api.requests.get")
    def test_make_api_call(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_get.return_value = mock_response

        result = self.iso.make_api_call("test_url")
        assert result == {"data": "test"}

        mock_get.assert_called_once()

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_locations(self, mock_make_api_call, isone_locations):
        mock_make_api_call.return_value = isone_locations

        result = self.iso.get_locations()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 19
        assert list(result.columns) == [
            "LocationID",
            "LocationType",
            "LocationName",
            "AreaType",
        ]
        assert result["LocationName"].tolist() == [
            ".H.INTERNAL_HUB",
            ".Z.MAINE",
            ".Z.NEWHAMPSHIRE",
            ".Z.VERMONT",
            ".Z.CONNECTICUT",
            ".Z.RHODEISLAND",
            ".Z.SEMASS",
            ".Z.WCMASS",
            ".Z.NEMASSBOST",
            ".I.SALBRYNB345 1",
            ".I.ROSETON 345 1",
            ".I.HQ_P1_P2345 5",
            ".I.HQHIGATE120 2",
            ".I.SHOREHAM138 99",
            ".I.NRTHPORT138 5",
            "ROS",
            "SWCT",
            "CT",
            "NEMABSTN",
        ]

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_realtime_hourly_demand_latest(
        self,
        mock_make_api_call,
        isone_realtime_hourly_demand_latest,
    ):
        mock_make_api_call.return_value = isone_realtime_hourly_demand_latest

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

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_dayahead_hourly_demand_latest(
        self,
        mock_make_api_call,
        isone_dayahead_hourly_demand_latest,
    ):
        mock_make_api_call.return_value = isone_dayahead_hourly_demand_latest

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

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_dayahead_hourly_demand_no_data(self, mock_make_api_call):
        mock_make_api_call.return_value = {}

        with pytest.raises(KeyError) as exc_info:
            self.iso.get_dayahead_hourly_demand(date="latest", locations=[".Z.MAINE"])

        assert str(exc_info.value) == "'HourlyDaDemand'"

    # NOTE(kladar): I'm envisioning a future where we update the fixtures occasionally when we run integration tests
    # and add them to the s3 bucket. We can also have that process update the unit test params here when we update the
    # fixtures. Also noting that the point of separating the tests is so that we can run them without running the full
    # integration tests, which is 100x faster for developing and can reasonably autorun the tests on commit.

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_realtime_hourly_demand_multiple_locations(
        self,
        mock_make_api_call,
        isone_realtime_hourly_demand_latest,
        isone_realtime_hourly_demand_latest_multiple,
    ):
        mock_make_api_call.side_effect = [
            {"HourlyRtDemand": isone_realtime_hourly_demand_latest["HourlyRtDemand"]},
            {
                "HourlyRtDemand": isone_realtime_hourly_demand_latest_multiple[
                    "HourlyRtDemand"
                ],
            },
        ]

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
    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_realtime_hourly_demand_date_range(
        self,
        mock_make_api_call,
        isone_realtime_hourly_demand_location1_date1,
        isone_realtime_hourly_demand_location1_date2,
        isone_realtime_hourly_demand_location2_date1,
        isone_realtime_hourly_demand_location2_date2,
        date,
        end,
        locations,
        expected_rows,
        expected_location_ids,
    ):
        mock_responses = [
            isone_realtime_hourly_demand_location1_date1,
            isone_realtime_hourly_demand_location1_date2,
            isone_realtime_hourly_demand_location2_date1,
            isone_realtime_hourly_demand_location2_date2,
        ]

        mock_make_api_call.side_effect = mock_responses

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

        # Check that the date range is correct
        assert min(result["Interval Start"]).date() == pd.Timestamp(date).date()
        assert max(result["Interval End"]).date() == pd.Timestamp(end).date()

        # Check that we have data for all locations
        for location in locations:
            assert len(result[result["Location"] == location]) == expected_rows // len(
                locations,
            )

        # Verify that the API was called the correct number of times
        assert (
            mock_make_api_call.call_count
            == len(locations) * (pd.Timestamp(end) - pd.Timestamp(date)).days
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
    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_dayahead_hourly_demand_date_range(
        self,
        mock_make_api_call,
        isone_dayahead_hourly_demand_location1_date1,
        isone_dayahead_hourly_demand_location1_date2,
        isone_dayahead_hourly_demand_location2_date1,
        isone_dayahead_hourly_demand_location2_date2,
        date,
        end,
        locations,
        expected_rows,
        expected_location_ids,
    ):
        mock_responses = [
            isone_dayahead_hourly_demand_location1_date1,
            isone_dayahead_hourly_demand_location1_date2,
            isone_dayahead_hourly_demand_location2_date1,
            isone_dayahead_hourly_demand_location2_date2,
        ]

        mock_make_api_call.side_effect = mock_responses

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

        # Check that the date range is correct
        assert min(result["Interval Start"]).date() == pd.Timestamp(date).date()
        assert max(result["Interval End"]).date() == pd.Timestamp(end).date()

        # Check that we have data for all locations
        for location in locations:
            assert len(result[result["Location"] == location]) == expected_rows // len(
                locations,
            )

        # Verify that the API was called the correct number of times
        assert (
            mock_make_api_call.call_count
            == len(locations) * (pd.Timestamp(end) - pd.Timestamp(date)).days
        )

    @vcr.use_cassette(
        "gridstatus/tests/fixtures/vcr_cassettes/hourly_load_forecast.yaml",
    )
    def test_get_hourly_load_forecast(self):
        result = self.iso.get_hourly_load_forecast(date="2023-05-01", end="2023-05-02")

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert list(result.columns) == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Load",
            "Net Load",
        ]
        # Add more assertions as needed

    @vcr.use_cassette(
        "gridstatus/tests/fixtures/vcr_cassettes/reliability_region_load_forecast.yaml",
    )
    def test_get_reliability_region_load_forecast(self):
        result = self.iso.get_reliability_region_load_forecast(
            date="2023-05-01",
            end="2023-05-02",
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert list(result.columns) == [
            "Interval Start",
            "Interval End",
            "Publish Time",
            "Location",
            "Load",
            "Regional Percentage",
        ]
        # Add more assertions as needed

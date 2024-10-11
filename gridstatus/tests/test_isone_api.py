import json
import os
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from gridstatus.base import NoDataFoundException
from gridstatus.isone_api.isone_api import ISONEAPI, ZONE_LOCATIONID_MAP

# Define the fixtures directory
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "isone")

# Create fixtures for each JSON file in the fixtures directory
@pytest.fixture
def isone_locations():
    with open(os.path.join(FIXTURES_DIR, "isone_locations.json"), "r") as f:
        return json.load(f)


@pytest.fixture
def isone_realtime_hourly_demand_current():
    with open(
        os.path.join(FIXTURES_DIR, "isone_realtime_hourly_demand_current.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_dayahead_hourly_demand_current():
    with open(
        os.path.join(FIXTURES_DIR, "isone_dayahead_hourly_demand_current.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_realtime_hourly_demand_range():
    with open(
        os.path.join(FIXTURES_DIR, "isone_realtime_hourly_demand_range.json"),
        "r",
    ) as f:
        return json.load(f)


@pytest.fixture
def isone_dayahead_hourly_demand_range():
    with open(
        os.path.join(FIXTURES_DIR, "isone_dayahead_hourly_demand_range.json"),
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
    def test_get_realtime_hourly_demand_current(
        self,
        mock_make_api_call,
        isone_realtime_hourly_demand_current,
    ):
        mock_make_api_call.return_value = isone_realtime_hourly_demand_current

        result = self.iso.get_realtime_hourly_demand_current(locations=[".Z.MAINE"])

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert list(result.columns) == ["Interval Start", "Location", "LocId", "Load"]
        assert result["Location"].iloc[0] == ".Z.MAINE"
        assert result["LocId"].iloc[0] == 4001
        assert isinstance(result["Load"].iloc[0], (int, float))

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_dayahead_hourly_demand_current(
        self,
        mock_make_api_call,
        isone_dayahead_hourly_demand_current,
    ):
        mock_make_api_call.return_value = isone_dayahead_hourly_demand_current

        result = self.iso.get_dayahead_hourly_demand_current(locations=["NEPOOL AREA"])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert list(result.columns) == ["Interval Start", "Location", "LocId", "Load"]
        assert result["Location"].iloc[0] == "NEPOOL AREA"
        assert result["LocId"].iloc[0] == 32
        assert isinstance(result["Load"].iloc[0], np.number)

    # NOTE(kladar): These two are not super useful as tests go, but starting to think about API failure modes and
    # how to catch them.
    def test_get_dayahead_hourly_demand_invalid_location(self):
        with pytest.raises(NoDataFoundException):
            self.iso.get_dayahead_hourly_demand_current(locations=["INVALID_LOCATION"])

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_dayahead_hourly_demand_no_data(self, mock_make_api_call):
        mock_make_api_call.return_value = {}

        with pytest.raises(KeyError) as exc_info:
            self.iso.get_dayahead_hourly_demand_current(locations=[".Z.MAINE"])

        assert str(exc_info.value) == "'HourlyDaDemand'"

    # NOTE(kladar): I'm envisioning a future where we update the fixtures occasionally when we run integration tests
    # and add them to the s3 bucket. We can also have that process update the unit test params here when we update the
    # fixtures. Also noting that the point of separating the tests is so that we can run them without running the full
    # integration tests, which is 100x faster for developing and can reasonably autorun the tests on commit.
    @pytest.mark.parametrize(
        "date,end",
        [
            ("2024-07-01", "2024-07-02"),
        ],
    )
    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_realtime_hourly_demand_historical_range(
        self,
        mock_make_api_call,
        isone_realtime_hourly_demand_range,
        date,
        end,
    ):
        mock_make_api_call.return_value = isone_realtime_hourly_demand_range

        result = self.iso.get_realtime_hourly_demand_historical_range(
            date=date,
            end=end,
            locations=[".Z.MAINE"],
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert list(result.columns) == ["Interval Start", "Location", "LocId", "Load"]
        assert result["Location"].iloc[0] == ".Z.MAINE"
        assert result["LocId"].iloc[0] == 4001
        assert isinstance(result["Load"].iloc[0], (int, float))

    @pytest.mark.parametrize(
        "date,end",
        [
            ("2024-07-01", "2024-07-02"),
        ],
    )
    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_dayahead_hourly_demand_historical_range(
        self,
        mock_make_api_call,
        isone_dayahead_hourly_demand_range,
        date,
        end,
    ):
        mock_make_api_call.return_value = isone_dayahead_hourly_demand_range

        result = self.iso.get_dayahead_hourly_demand_historical_range(
            date=date,
            end=end,
            locations=[".Z.MAINE"],
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert list(result.columns) == ["Interval Start", "Location", "LocId", "Load"]
        assert result["Location"].iloc[0] == ".Z.MAINE"
        assert result["LocId"].iloc[0] == 4001
        assert isinstance(result["Load"].iloc[0], (int, float))

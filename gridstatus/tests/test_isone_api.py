import json
import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gridstatus.base import NoDataFoundException
from gridstatus.isone_api.isone_api import ISONEAPI, ZONE_LOCATIONID_MAP


class TestISONEAPI:
    @classmethod
    def setup_class(cls):
        cls.iso = ISONEAPI(sleep_seconds=0.1, max_retries=2)
        cls.fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures", "isone")

    def load_fixture(self, filename):
        with open(os.path.join(self.fixtures_dir, filename), "r") as f:
            return json.load(f)

    def test_init(self):
        assert self.iso.sleep_seconds == 0.1
        assert self.iso.max_retries == 2
        assert self.iso.username is not None
        assert self.iso.password is not None

    def test_zone_locationid_map(self):
        for zone, location_id in ZONE_LOCATIONID_MAP.items():
            assert ZONE_LOCATIONID_MAP[zone] == location_id
            # Test that the zone name without ".Z." prefix also works
            if zone.startswith(".Z."):
                assert ZONE_LOCATIONID_MAP[zone.replace(".Z.", "")] == location_id

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
    def test_get_locations(self, mock_make_api_call):
        mock_response = self.load_fixture("isone_locations.json")
        mock_make_api_call.return_value = mock_response

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
    def test_get_realtime_hourly_demand_current(self, mock_make_api_call):
        mock_response = self.load_fixture("isone_realtime_hourly_demand_current.json")
        mock_make_api_call.return_value = mock_response

        result = self.iso.get_realtime_hourly_demand_current(locations=[".Z.MAINE"])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert list(result.columns) == ["BeginDate", "Load", "Location", "LocId"]
        assert result["Location"].iloc[0] == ".Z.MAINE"
        assert isinstance(
            result["Load"].iloc[0],
            (int, float),
        )  # Check if Load is a number
        assert result["LocId"].iloc[0] == "4001"

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_dayahead_hourly_demand_current(self, mock_make_api_call):
        mock_response = self.load_fixture("isono_dayahead_hourly_demand_current.json")
        mock_make_api_call.return_value = mock_response

        result = self.iso.get_dayahead_hourly_demand_current(locations=["NEPOOL AREA"])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert list(result.columns) == ["BeginDate", "Load", "Location", "LocId"]
        assert result["Location"].iloc[0] == "NEPOOL AREA"
        assert isinstance(
            result["Load"].iloc[0],
            (int, float),
        )  # Check if Load is a number
        assert result["LocId"].iloc[0] == "32"

    # NOTE(kladar): These two are not super useful as tests go, but starting to think about API failure modes and
    # how to catch them.
    def test_get_dayahead_hourly_demand_invalid_location(self):
        with pytest.raises(NoDataFoundException):
            self.iso.get_dayahead_hourly_demand_current(locations=["INVALID_LOCATION"])

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_dayahead_hourly_demand_no_data(self, mock_make_api_call):
        mock_make_api_call.return_value = {"HourlyDaDemand": {}}

        with pytest.raises(NoDataFoundException):
            self.iso.get_dayahead_hourly_demand_current(locations=["MAINE"])

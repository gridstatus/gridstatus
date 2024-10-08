from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from gridstatus.isone_api.isone_api import ISONEAPI, ZONE_LOCATIONID_MAP


class TestISONEAPI:
    @classmethod
    def setup_class(cls):
        cls.iso = ISONEAPI(sleep_seconds=0.1, max_retries=2)

    def test_init(self):
        assert self.iso.sleep_seconds == 0.1
        assert self.iso.max_retries == 2
        assert self.iso.username is not None
        assert self.iso.password is not None

    def test_get_location_id(self):
        for zone, location_id in ZONE_LOCATIONID_MAP.items():
            assert self.iso._get_location_id(zone) == location_id
            assert self.iso._get_location_id(zone.replace(".Z.", "")) == location_id

        with pytest.raises(ValueError):
            self.iso._get_location_id("INVALID_ZONE")

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
        mock_make_api_call.return_value = {
            "Locations": {
                "Location": [
                    {"@LocId": "1", "$": "Location1"},
                    {"@LocId": "2", "$": "Location2"},
                ],
            },
        }

        result = self.iso.get_locations()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["LocId", "Name"]

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_location_by_id(self, mock_make_api_call):
        mock_make_api_call.return_value = {
            "Location": {"@LocId": "1", "$": "Location1"},
        }

        result = self.iso.get_location_by_id(1)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert list(result.columns) == ["LocId", "Name"]

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_all_locations(self, mock_make_api_call):
        mock_make_api_call.return_value = {
            "Locations": {
                "Location": [
                    {"@LocId": "1", "$": "Location1"},
                    {"@LocId": "2", "$": "Location2"},
                ],
            },
        }

        result = self.iso.get_all_locations()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["LocId", "Name"]

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_realtime_hourly_demand_current(self, mock_make_api_call):
        mock_make_api_call.return_value = {
            "HourlyRtDemands": {
                "HourlyRtDemand": [
                    {
                        "BeginDate": "2023-05-01T00:00:00-04:00",
                        "Location": {"$": "MAINE", "@LocId": "4001"},
                        "Load": "1000",
                    },
                ],
            },
        }

        result = self.iso.get_realtime_hourly_demand_current()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert list(result.columns) == ["BeginDate", "Location", "LocId", "Load"]

    @patch("gridstatus.isone_api.isone_api.ISONEAPI.make_api_call")
    def test_get_realtime_hourly_demand_historical_range(self, mock_make_api_call):
        mock_make_api_call.return_value = {
            "HourlyRtDemands": {
                "HourlyRtDemand": [
                    {
                        "BeginDate": "2023-05-01T00:00:00-04:00",
                        "Location": {"$": "MAINE", "@LocId": "4001"},
                        "Load": "1000",
                    },
                ],
            },
        }

        result = self.iso.get_realtime_hourly_demand_historical_range(
            date="2023-05-01",
            end="2023-05-02",
            location="MAINE",
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert list(result.columns) == ["BeginDate", "Location", "LocId", "Load"]

    # Add more test methods for other ISONEAPI methods as needed

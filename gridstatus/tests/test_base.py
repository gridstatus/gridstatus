from unittest.mock import Mock, patch

import pytest
import requests

from gridstatus.base import ISOBase


class TestISOBase:
    # Test Case 1: Successful request without retry
    def test_get_json_successful(self):
        # Have to mock the module where the requests.get method is called
        with patch("gridstatus.base.requests.get") as mocked_get:
            mocked_get.return_value.json.return_value = {"key": "value"}
            mocked_get.return_value.raise_for_status = Mock()

            iso = ISOBase()
            response = iso._get_json("http://example.com", False)
            assert response == {"key": "value"}
            mocked_get.assert_called_once()

    # Test Case 2: Successful request on a retry
    def test_get_json_success_after_retry(self):
        with patch("gridstatus.base.requests.get") as mocked_get:
            mocked_get.side_effect = [
                requests.RequestException("Error"),
                Mock(json=Mock(return_value={"key": "value"}), raise_for_status=Mock()),
            ]

            iso = ISOBase()
            response = iso._get_json("http://example.com", False, retries=1)
            assert response == {"key": "value"}
            assert mocked_get.call_count == 2

    # Test Case 3: Exhaust retries and raise exception
    def test_get_json_exhaust_retries(self):
        with patch("gridstatus.base.requests.get") as mocked_get:
            mocked_get.side_effect = requests.RequestException("Error")

            iso = ISOBase()
            with pytest.raises(requests.RequestException):
                iso._get_json("http://example.com", False, retries=2)
            # Total of 3 calls (1 original + 2 retries)
            assert mocked_get.call_count == 3

    # Test Case 4: No retries (retries is None)
    def test_get_json_no_retries(self):
        with patch("gridstatus.base.requests.get") as mocked_get:
            mocked_get.side_effect = requests.RequestException("Error")

            iso = ISOBase()
            with pytest.raises(requests.RequestException):
                iso._get_json("http://example.com", False, retries=None)
            mocked_get.assert_called_once()

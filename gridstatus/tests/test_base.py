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


SECRET = "data-miner-subscription-key"


def _logged_text(mocked_logger: Mock) -> str:
    return " ".join(str(c) for c in mocked_logger.info.call_args_list)


def test_get_json_does_not_log_header_values():
    with (
        patch("gridstatus.base.requests.get") as mocked_get,
        patch("gridstatus.base.logger") as mocked_logger,
    ):
        mocked_get.return_value.json.return_value = {"key": "value"}
        ISOBase()._get_json(
            "http://example.com",
            False,
            params={"rowCount": 1},
            headers={"Ocp-Apim-Subscription-Key": SECRET},
        )

    logged = _logged_text(mocked_logger)
    assert SECRET not in logged
    assert "Ocp-Apim-Subscription-Key" in logged
    assert "rowCount" in logged
    # The request itself still carries the real header.
    assert mocked_get.call_args.kwargs["headers"] == {
        "Ocp-Apim-Subscription-Key": SECRET,
    }


def test_pjm_api_call_does_not_log_subscription_key():
    from gridstatus.pjm import PJM

    with (
        patch("gridstatus.pjm.requests.get") as mocked_get,
        patch("gridstatus.pjm.logger") as mocked_logger,
    ):
        mocked_get.return_value.status_code = 200
        mocked_get.return_value.json.return_value = {"items": []}
        PJM(api_key=SECRET)._data_miner_page("gen_by_fuel", {"rowCount": 1})

    logged = _logged_text(mocked_logger)
    assert SECRET not in logged
    assert "Ocp-Apim-Subscription-Key" in logged
    assert mocked_get.call_args.kwargs["headers"] == {
        "Ocp-Apim-Subscription-Key": SECRET,
    }


def test_loggable_request_kwargs_redacts_auth_and_leaves_input_untouched():
    from gridstatus.base import REDACTED, loggable_request_kwargs

    kwargs = {"headers": {"Authorization": "Bearer t"}, "auth": ("u", "p"), "x": 1}
    assert loggable_request_kwargs(kwargs) == {
        "headers": {"Authorization": REDACTED},
        "auth": REDACTED,
        "x": 1,
    }
    assert kwargs["headers"] == {"Authorization": "Bearer t"}

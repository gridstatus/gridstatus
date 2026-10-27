import logging
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from gridstatus import IESO
from gridstatus.base import NoDataFoundException
from gridstatus.ieso_constants import RESOURCE_ADEQUACY_REPORT_BASE_URL


@pytest.fixture(params=["date", "last_modified"])
def fetch_adequacy_files(request):
    iso = IESO()
    last_modified = pd.Timestamp("2026-10-27 17:00", tz=iso.default_timezone)
    if request.param == "date":
        return lambda: iso._get_all_resource_adequacy_jsons(
            "2026-11-26",
            last_modified,
        )
    return lambda: iso._get_all_resource_adequacy_jsons_by_last_modified(last_modified)


@pytest.mark.parametrize("failure", ["missing", "invalid_xml", "server_error"])
def test_adequacy_file_failure_preserves_other_reports(
    fetch_adequacy_files,
    failure,
    monkeypatch,
    caplog,
):
    missing_file = "PUB_Adequacy3_20261126_v10.xml"
    valid_file = "PUB_Adequacy3_20261126_v11.xml"
    listing = (
        f'<a href="{missing_file}">{missing_file}</a> 27-Oct-2026 17:06\n'
        f'<a href="{valid_file}">{valid_file}</a> 27-Oct-2026 17:07\n'
    )

    def get(url, **kwargs):
        response = requests.Response()
        response.status_code = 200
        if url == RESOURCE_ADEQUACY_REPORT_BASE_URL:
            response._content = listing.encode()
        elif url.endswith(missing_file):
            response.status_code = {
                "missing": 404,
                "invalid_xml": 200,
                "server_error": 500,
            }[failure]
            response._content = b"not xml"
        else:
            assert url.endswith(valid_file)
            response._content = b"<Document><value>available</value></Document>"
        return response

    get_mock = Mock(side_effect=get)
    monkeypatch.setattr("gridstatus.ieso.requests.get", get_mock)
    with caplog.at_level(logging.WARNING, logger="gridstatus"):
        reports = fetch_adequacy_files()

    assert reports == [
        (
            {"Document": {"value": "available"}},
            pd.Timestamp("2026-10-27 17:07", tz=IESO.default_timezone),
        ),
    ]
    if failure == "missing":
        assert "Skipping unavailable resource adequacy file" in caplog.text
        assert missing_file in caplog.text
        assert not any(record.levelno >= logging.ERROR for record in caplog.records)
        assert (
            sum(call.args[0].endswith(missing_file) for call in get_mock.call_args_list)
            == 1
        )
    else:
        assert any(record.levelno >= logging.ERROR for record in caplog.records)
        assert "Unexpected error processing file" in caplog.text


def test_ieso_direct_request_still_raises_for_missing_file(monkeypatch):
    response = requests.Response()
    response.status_code = 404
    get_mock = Mock(return_value=response)
    monkeypatch.setattr("gridstatus.ieso.requests.get", get_mock)

    with pytest.raises(NoDataFoundException, match="File not found"):
        IESO()._request(f"{RESOURCE_ADEQUACY_REPORT_BASE_URL}/missing.xml")

    get_mock.assert_called_once()

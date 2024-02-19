import pytest

from gridstatus.base import NoDataFoundException
from gridstatus.decorators import support_date_range


class MockClass:
    default_timezone = "US/Central"

    @support_date_range("DAY_START")
    def mock_function(self, start, end=None):
        return None


def test_support_date_range_raises_no_data_found_exception():
    with pytest.raises(NoDataFoundException):
        MockClass().mock_function(start="2024-01-01", end="2024-01-02")

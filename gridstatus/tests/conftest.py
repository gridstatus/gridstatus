from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def disable_exponential_backoff_sleep():
    """Disable time.sleep in all modules that use exponential backoff.

    This prevents tests from waiting during retry logic, making them run faster.
    The fixture is autouse=True so it applies to all tests automatically.
    """
    modules_with_backoff = [
        "gridstatus.pjm",
        "gridstatus.ercot_api.ercot_api",
        "gridstatus.miso_api",
        "gridstatus.ieso",
        "gridstatus.isone_api.isone_api",
        "gridstatus.caiso.caiso",
    ]

    patchers = []
    for module in modules_with_backoff:
        patcher = patch(f"{module}.time.sleep", return_value=None)
        patchers.append(patcher)
        patcher.start()

    yield

    for patcher in patchers:
        patcher.stop()

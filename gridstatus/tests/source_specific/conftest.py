import time

import pytest

from gridstatus.tests.vcr_utils import RECORD_MODE

CAISO_OASIS_RECORD_COOLDOWN_SECONDS = 15


@pytest.fixture(autouse=True)
def caiso_oasis_record_cooldown(request: pytest.FixtureRequest):
    """Sleep after CAISO OASIS tests when re-recording VCR cassettes.

    CAISO rate-limits OASIS to roughly one request every few seconds. When
    ``VCR_RECORD_MODE=all``, back-to-back tests that hit the same endpoint can
    get HTTP 429 unless we pause between them.
    """
    if RECORD_MODE != "all" or request.node.get_closest_marker("caiso_oasis") is None:
        yield
        return

    yield
    time.sleep(CAISO_OASIS_RECORD_COOLDOWN_SECONDS)

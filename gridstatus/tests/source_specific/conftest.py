import os
import time

import pytest

CAISO_OASIS_RECORD_COOLDOWN_SECONDS = 30

_last_caiso_oasis_record_time: float | None = None


@pytest.fixture(autouse=True)
def caiso_oasis_record_cooldown(request: pytest.FixtureRequest):
    """Pause between CAISO OASIS tests when re-recording VCR cassettes.

    CAISO rate-limits OASIS to roughly one request every few seconds. When
    ``VCR_RECORD_MODE=all``, back-to-back tests that hit the same endpoint can
    get HTTP 429 unless we pause between them.

    Pair with ``@pytest.mark.real_sleep`` on the test so ``time.sleep`` is not
    mocked during recording.
    """
    global _last_caiso_oasis_record_time

    recording = os.getenv("VCR_RECORD_MODE", "new_episodes") == "all"
    marked = request.node.get_closest_marker("caiso_oasis") is not None

    if recording and marked and _last_caiso_oasis_record_time is not None:
        elapsed = time.time() - _last_caiso_oasis_record_time
        wait = CAISO_OASIS_RECORD_COOLDOWN_SECONDS - elapsed
        if wait > 0:
            time.sleep(wait)

    yield

    if recording and marked:
        _last_caiso_oasis_record_time = time.time()

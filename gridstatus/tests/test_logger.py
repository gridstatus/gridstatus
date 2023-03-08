import pytest

from gridstatus.logging import log


@pytest.fixture
def msg():
    return "testing from test_logger.py"


def test_log_stdout(msg, caplog):
    log(msg, verbose=True)
    assert msg, "\n" in caplog.text

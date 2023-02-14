import pytest

from gridstatus.logging import configure_logging, log


@pytest.fixture
def msg():
    configure_logging()
    return "testing from test_logger.py"


def test_log_stdout(msg, capsys):
    log(msg, verbose=True)
    out, _ = capsys.readouterr()
    assert msg, "\n" in out

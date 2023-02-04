import pytest

from gridstatus.logging import configure_logging, log


@pytest.fixture
def msg():
    configure_logging()
    return "testing from test_logger.py"


def test_log_stdout(msg, capsys):
    log(msg, verbose=True)
    captured = capsys.readouterr()
    assert msg, "\n" in captured.out


def test_log_stderr(msg, capsys):
    log(msg, debug=True)
    captured = capsys.readouterr()
    assert msg, "\n" in captured.err


def test_log_raise_error(msg):
    with pytest.raises(ValueError):
        log(msg, verbose=True, debug=True)

import logging
import sys

from gridstatus.config import config


def log(msg, verbose=False, log_stream="stdout", level=logging.INFO):
    if not verbose:
        return
    fmt = config.get_option("log_format")

    if log_stream == "stdout":
        handler = logging.StreamHandler(sys.stdout)
    elif log_stream == "stderr":
        handler = logging.StreamHandler(sys.stderr)
    else:
        raise ValueError("No log stream specified.")

    handler.setFormatter(logging.Formatter(fmt))

    logger = logging.getLogger("gridstatus")
    logger.addHandler(handler)

    if level == logging.INFO:
        logger.log(msg)
    elif level == logging.DEBUG:
        logger.debug(msg)
    else:
        raise ValueError("No valid log level specified.")

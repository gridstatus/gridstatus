import logging
import sys

from gridstatus.config import config


def log(msg, verbose=False, file="stdout", level=logging.INFO):
    if not verbose:
        return
    fmt = config.get_option("log_format")

    if file == "stdout":
        handler = logging.StreamHandler(sys.stdout)
    elif file == "stderr":
        handler = logging.StreamHandler(sys.stderr)
    else:
        raise ValueError(f"{file} is an invalid log stream specified.")

    handler.setFormatter(logging.Formatter(fmt))

    logger = logging.getLogger("gridstatus")
    logger.addHandler(handler)

    if level == logging.INFO:
        logger.log(msg)
    elif level == logging.DEBUG:
        logger.debug(msg)
    else:
        raise ValueError("No valid log level specified.")

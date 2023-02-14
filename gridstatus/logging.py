import logging
import sys

from gridstatus.config import config


def configure_logging():
    # instantiate logger
    logger = logging.getLogger("gridstatus")
    if not len(logger.handlers):
        return logger

    logger.setLevel(logging.INFO)

    fmt = config.get_option("log_format")
    # by default, StreamHandler logs to stderr
    handler = logging.StreamHandler(sys.stderr)
    # log info, debug, warning, error, critical to stdout
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(fmt=fmt))
    logger.addHandler(handler)

    return logger


def log(msg, verbose=False):
    # verbose --> info level, meant for all users
    # info level --> confirmation things are working as expected

    if verbose is False:
        return

    logging.INFO
    logger = logging.getLogger("gridstatus")
    logger.info(msg=msg)

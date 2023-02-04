import logging

from gridstatus.config import config


def configure_logging():
    # instantiate logger
    logger = logging.getLogger("gridstatus")
    if not len(logger.handlers):
        return logger

    logger.setLevel(logging.INFO)

    fmt = config.get_option("log_format")
    # by default, StreamHandler logs to stderr
    handler = logging.StreamHandler()
    # log info, debug, warning, error, critical to stdout
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(fmt=fmt))
    logger.addHandler(handler)

    return logger


def log(msg, verbose=False, debug=False):
    # verbose --> info level, meant for all users
    # info level --> confirmation things are working as expected

    # debug --> developers of gridstatus
    # debug --> Detailed info, typically of interest only when diagnosing problems

    if verbose and debug:
        raise ValueError("verbose and debug cannot both be True")

    level = logging.WARNING
    if verbose:
        level = logging.INFO
    elif debug:
        level = logging.DEBUG

    logger = logging.getLogger("gridstatus")
    logger.log(level=level, msg=msg)

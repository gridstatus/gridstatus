import logging
import sys


def configure_logging():
    logger = logging.getLogger("gridstatus")
    if not len(logger.handlers):
        logger.setLevel(logging.INFO)
        fmt = ("%(name)s: %(levelname)s: %(message)s",)
        # by default, StreamHandler logs to stderr
        handler = logging.StreamHandler(sys.stderr)
        # log info, debug, warning, error, critical to stdout
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter(fmt=fmt))
        logger.addHandler(handler)
    return logger


def log(msg, verbose=False):
    if verbose is False:
        return
    logger = configure_logging()
    logger.info(msg=msg)

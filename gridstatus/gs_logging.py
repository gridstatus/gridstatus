import logging


def setup_gs_logger(level=logging.DEBUG):
    logger = logging.getLogger("gs_etl")
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)  # Set handler level to the same as logger
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ),
    )
    logger.addHandler(handler)
    return logger


logger = setup_gs_logger()


def log(msg, verbose=False):
    # TODO: use logging
    if verbose is False:
        return
    print(msg)

import logging


def setup_gs_logger(param):
    logger = logging.getLogger("gs_etl")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
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

import logging


def setup_gs_etl_logger():
    logger = logging.getLogger("gs_etl")
    logger.setLevel(logging.INFO)  # Set the logger level to DEBUG
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


logger = setup_gs_etl_logger()


def log(msg, verbose=False):
    # TODO: use logging
    if verbose is False:
        return
    print(msg)

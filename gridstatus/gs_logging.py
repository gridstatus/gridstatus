import logging


def setup_gs_logger(level: int = logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger("gridstatus")

    if not logger.handlers:
        logger.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ),
        )
        logger.addHandler(handler)

    return logger


logger = setup_gs_logger()


def log(msg: str, verbose: bool = False) -> None:
    if verbose is False:
        return
    print(msg)

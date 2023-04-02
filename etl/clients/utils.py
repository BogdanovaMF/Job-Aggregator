import sys
import logging


def get_logger():
    """Object logging function"""

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(stream=sys.stdout)
    logger.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(levelname)s  %(asctime)s: %(message)s"))
    logger.addHandler(handler)

    fh = logging.FileHandler('file.log')
    fh.setFormatter(logging.Formatter("%(levelname)s  %(asctime)s: %(message)s"))
    logger.addHandler(fh)
    return logger


logger = get_logger()

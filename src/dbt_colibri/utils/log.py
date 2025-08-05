import logging
import sys


def setup_logging(level=logging.INFO):
    """
    Set up logging configuration.
    Args:
        level (int): Logging level (e.g. logging.DEBUG)
    Returns:
        Logger object
    """
    logger = logging.getLogger("dbt_column_lineage")

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(level)
    logger.propagate = False
    return logger




import logging
import sys


def setup_logging(level=logging.INFO):
    """
    Set up logging configuration with timestamp, level, module, and message.
    Args:
        level (int): Logging level (e.g. logging.DEBUG)
    Returns:
        Logger object
    """
    logger = logging.getLogger("dbt_column_lineage")

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s.%(module)s.%(funcName)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(level)
    logger.propagate = False
    return logger




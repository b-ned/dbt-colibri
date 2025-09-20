import logging
import sys

def setup_logging(level=logging.INFO):
    """
    Set up logging configuration.
    
    INFO -> user-friendly format
    DEBUG -> detailed developer format
    """
    logger = logging.getLogger("colibri")

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        if level == logging.DEBUG:
            # Detailed developer logs
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s.%(module)s.%(funcName)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
        else:
            # Cleaner user-facing logs
            formatter = logging.Formatter(
                fmt="[%(levelname)s] %(message)s"
            )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(level)
    logger.propagate = True
    return logger





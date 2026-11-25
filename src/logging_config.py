"""Central logging setup used by the pipeline entry points."""

import logging
import os

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def configure_logging(level=None):
    """Configure root logging once.

    The level can be overridden with the ``LOG_LEVEL`` environment variable
    (DEBUG, INFO, WARNING, ERROR). Calling this more than once is harmless.
    """
    level_name = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(level=getattr(logging, level_name, logging.INFO), format=LOG_FORMAT)
    # py4j is very chatty at INFO level.
    logging.getLogger("py4j").setLevel(logging.WARNING)

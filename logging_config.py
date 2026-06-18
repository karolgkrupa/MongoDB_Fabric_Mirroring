"""Centralized logging configuration for the MongoDB -> Fabric mirroring app.

All entry points (``mongodb_generic_mirroring.mirror``, ``app.py``,
``service_runner.py``) call ``setup_logging()`` so logging is configured the same
way everywhere: a console handler plus a rotating ``mirroring.log`` file handler.
The function is idempotent, so repeated calls (e.g. app.py then mirror()) do not
add duplicate handlers.
"""

import logging
import logging.handlers
import os

_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_LOG_FILE_NAME = "mirroring.log"
_configured = False


def setup_logging() -> None:
    """Configure root logging once. Safe to call multiple times."""
    global _configured
    if _configured:
        return

    # changed to _nameToLevel as getLevelNamesMapping is only available from
    # Python 3.11; _nameToLevel keeps this working on 3.9 and lower.
    log_level = logging._nameToLevel.get(os.getenv("APP_LOG_LEVEL"), logging.INFO)

    logging.basicConfig(level=log_level, format=_LOG_FORMAT)

    root_logger = logging.getLogger()
    formatter = logging.Formatter(_LOG_FORMAT)
    # Rotating file handler: 50 MB per file, 5 backups.
    file_handler = logging.handlers.RotatingFileHandler(
        _LOG_FILE_NAME, maxBytes=50 * 1024 * 1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    _configured = True
    logging.getLogger(__name__).info(
        "logging configured at level %s", logging.getLevelName(log_level)
    )

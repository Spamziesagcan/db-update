from __future__ import annotations

import logging
import os
import sys


def _has_handler(root_logger: logging.Logger, handler_type: type, predicate) -> bool:
    for handler in root_logger.handlers:
        if isinstance(handler, handler_type) and predicate(handler):
            return True
    return False


def configure_logging(log_file: str) -> logging.Logger:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    normalized_log_file = os.path.abspath(log_file)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    if not _has_handler(
        root_logger,
        logging.FileHandler,
        lambda handler: os.path.abspath(getattr(handler, "baseFilename", "")) == normalized_log_file,
    ):
        file_handler = logging.FileHandler(normalized_log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    if not _has_handler(
        root_logger,
        logging.StreamHandler,
        lambda handler: getattr(handler, "stream", None) is sys.stdout,
    ):
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    return logging.getLogger(__name__)
from __future__ import annotations

import logging
import os
import sys

from app.core.observability import ContextFilter, JsonFormatter


def _has_handler(root_logger: logging.Logger, handler_type: type, predicate) -> bool:
    for handler in root_logger.handlers:
        if isinstance(handler, handler_type) and predicate(handler):
            return True
    return False


def configure_logging(log_file: str) -> logging.Logger:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    normalized_log_file = os.path.abspath(log_file)

    formatter = JsonFormatter()
    context_filter = ContextFilter()

    if not _has_handler(
        root_logger,
        logging.FileHandler,
        lambda handler: os.path.abspath(getattr(handler, "baseFilename", "")) == normalized_log_file,
    ):
        file_handler = logging.FileHandler(normalized_log_file)
        root_logger.addHandler(file_handler)
    else:
        file_handler = next(
            handler
            for handler in root_logger.handlers
            if isinstance(handler, logging.FileHandler)
            and os.path.abspath(getattr(handler, "baseFilename", "")) == normalized_log_file
        )

    file_handler.setFormatter(formatter)
    if not any(isinstance(existing_filter, ContextFilter) for existing_filter in file_handler.filters):
        file_handler.addFilter(context_filter)

    if not _has_handler(
        root_logger,
        logging.StreamHandler,
        lambda handler: getattr(handler, "stream", None) is sys.stdout,
    ):
        stream_handler = logging.StreamHandler(sys.stdout)
        root_logger.addHandler(stream_handler)
    else:
        stream_handler = next(
            handler
            for handler in root_logger.handlers
            if isinstance(handler, logging.StreamHandler)
            and getattr(handler, "stream", None) is sys.stdout
        )

    stream_handler.setFormatter(formatter)
    if not any(isinstance(existing_filter, ContextFilter) for existing_filter in stream_handler.filters):
        stream_handler.addFilter(context_filter)

    return logging.getLogger(__name__)
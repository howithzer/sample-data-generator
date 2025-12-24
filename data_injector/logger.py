"""
Structured logging module for Data Injector.

Provides JSON formatting for cloud environments (CloudWatch/Datadog)
and human-readable formatting for local development.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Literal

LogFormat = Literal["user", "json"]


class JsonFormatter(logging.Formatter):
    """JSON log formatter for cloud environments (CloudWatch, Datadog)."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add batch_id if present in extra
        if hasattr(record, "batch_id"):
            log_entry["batch_id"] = record.batch_id

        # Add any additional extra fields
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


class UserFormatter(logging.Formatter):
    """Human-readable log formatter for local development."""

    FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    BATCH_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | [batch:%(batch_id)s] %(message)s"

    def format(self, record: logging.LogRecord) -> str:
        if hasattr(record, "batch_id"):
            formatter = logging.Formatter(self.BATCH_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
        else:
            formatter = logging.Formatter(self.FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


def setup_logging(
    log_format: LogFormat = "user",
    level: str = "INFO",
    logger_name: str = "data_injector",
) -> logging.Logger:
    """
    Configure and return a logger with the specified format.

    Args:
        log_format: "user" for human-readable, "json" for structured JSON logs
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        logger_name: Name for the logger instance

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove existing handlers if reconfiguring
    logger.handlers.clear()

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logger.level)

    # Set formatter based on format type
    if log_format == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(UserFormatter())

    logger.addHandler(handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


class BatchLogger:
    """Context manager for batch-aware logging."""

    def __init__(self, logger: logging.Logger, batch_id: str):
        self.logger = logger
        self.batch_id = batch_id

    def _log(self, level: int, message: str, **kwargs):
        """Log with batch_id attached."""
        extra = {"batch_id": self.batch_id}
        if kwargs:
            extra["extra_fields"] = kwargs
        self.logger.log(level, message, extra=extra)

    def debug(self, message: str, **kwargs):
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs):
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs):
        self._log(logging.ERROR, message, **kwargs)

    def critical(self, message: str, **kwargs):
        self._log(logging.CRITICAL, message, **kwargs)

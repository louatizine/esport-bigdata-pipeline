"""
Centralized logging configuration for Spark streaming jobs.
Provides structured logging with consistent formatting across all components.
"""

import os
import logging
from datetime import datetime


class SparkLogger:
    """
    Custom logger for Spark streaming applications.
    Provides structured logging with configurable log levels.
    """

    def __init__(self, name: str, log_level: str = None):
        """
        Initialize logger with specified name and log level.

        Args:
            name: Logger name (typically the module name)
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.logger = logging.getLogger(name)

        # Get log level from environment or use provided level or default to INFO
        level = log_level or os.getenv("LOG_LEVEL", "INFO")
        self.logger.setLevel(getattr(logging, level.upper()))

        # Avoid duplicate handlers
        if not self.logger.handlers:
            # Console handler with formatting
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)

            # Structured log format
            formatter = logging.Formatter(
                fmt='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(formatter)

            self.logger.addHandler(console_handler)

    def debug(self, message: str, **kwargs):
        """Log debug message with optional context."""
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs):
        """Log info message with optional context."""
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with optional context."""
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error message with optional context."""
        self._log(logging.ERROR, message, **kwargs)

    def critical(self, message: str, **kwargs):
        """Log critical message with optional context."""
        self._log(logging.CRITICAL, message, **kwargs)

    def _log(self, level: int, message: str, **kwargs):
        """
        Internal method to log messages with additional context.

        Args:
            level: Logging level
            message: Log message
            **kwargs: Additional context to include in log
        """
        if kwargs:
            context = " | ".join(f"{k}={v}" for k, v in kwargs.items())
            full_message = f"{message} | {context}"
        else:
            full_message = message

        self.logger.log(level, full_message)


def get_logger(name: str, log_level: str = None) -> SparkLogger:
    """
    Factory function to create and return a logger instance.

    Args:
        name: Logger name (typically __name__ from calling module)
        log_level: Optional log level override

    Returns:
        SparkLogger: Configured logger instance
    """
    return SparkLogger(name, log_level)

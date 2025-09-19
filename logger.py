"""
Simple logging configuration for Book Sales Platform
"""

import logging
import sys
from datetime import datetime


def setup_logger(name: str = "book_sales", level: str = "INFO") -> logging.Logger:
    """Setup simple logger with console output"""

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper()))

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_logger(name: str = "book_sales") -> logging.Logger:
    """Get existing logger or create new one"""
    return logging.getLogger(name)


default_logger = setup_logger()


def log_info(message: str, logger: logging.Logger = None):
    """Log info message with emoji"""
    logger = logger or default_logger
    logger.info(f"{message}")


def log_success(message: str, logger: logging.Logger = None):
    """Log success message with emoji"""
    logger = logger or default_logger
    logger.info(f"{message}")


def log_warning(message: str, logger: logging.Logger = None):
    """Log warning message with emoji"""
    logger = logger or default_logger
    logger.warning(f"{message}")


def log_error(message: str, logger: logging.Logger = None):
    """Log error message with emoji"""
    logger = logger or default_logger
    logger.error(f"{message}")


def log_start(message: str, logger: logging.Logger = None):
    """Log start message with emoji"""
    logger = logger or default_logger
    logger.info(f"{message}")


def log_complete(message: str, logger: logging.Logger = None):
    """Log completion message with emoji"""
    logger = logger or default_logger
    logger.info(f"{message}")

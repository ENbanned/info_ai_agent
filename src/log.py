"""Loguru configuration for the crypto intelligence system."""

import logging
import sys
from datetime import time, timedelta, timezone

from loguru import logger

MSK = timezone(timedelta(hours=3))

# Remove default loguru handler
logger.remove()

# Main handler — colorful, structured
logger.add(
    sys.stdout,
    format=(
        "<level>{time:HH:mm:ss}</level> │ "
        "<level>{message}</level>"
    ),
    level="DEBUG",
    colorize=True,
)

# File handler — full detail, daily rotation, compress after 14 days
logger.add(
    "data/logs/system.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
    rotation=time(0, 0, 0, tzinfo=MSK),
    retention="30 days",
    compression="gz",
    level="DEBUG",
)


class InterceptHandler(logging.Handler):
    """Route stdlib logging to loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging() -> None:
    """Intercept all stdlib logging and route to loguru."""
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    # Silence noisy libraries
    for name in ("pyrogram", "pyrogram.session", "pyrogram.connection", "httpx", "httpcore", "neo4j"):
        logging.getLogger(name).setLevel(logging.WARNING)

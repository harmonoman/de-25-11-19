import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class AppLogger:
    """Application-wide logger for console and file output."""

    def __init__(self, name: str = "app", level: int = logging.DEBUG):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Make sure logs/ folder exists
        Path("logs").mkdir(exist_ok=True)

        # Prevent duplicate handlers if logger imported multiple times
        if not self.logger.handlers:

            # === Console formatter (human-readable) ===
            console_format = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

            # === File formatter (JSON-like) ===
            file_format = logging.Formatter(
                "{'time':'%(asctime)s', 'name':'%(name)s', "
                "'level':'%(levelname)s', 'message':'%(message)s'}"
            )

            # === Console handler ===
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)   # everything to console
            console_handler.setFormatter(console_format)

            # === Rotating file handler ===
            file_handler = RotatingFileHandler(
                "logs/app.log",
                maxBytes=5 * 1024 * 1024,  # 5 MB
                backupCount=5
            )
            file_handler.setLevel(logging.ERROR)  # only log ERROR+ to file
            file_handler.setFormatter(file_format)

            # Add handlers
            self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger

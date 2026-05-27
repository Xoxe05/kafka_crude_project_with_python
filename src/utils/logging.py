import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys


def setup_logging(log_file: str = "kafka.log"):

    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(log_level)

    # console_handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    console_formatter = logging.Formatter(log_format)
    console_handler.setFormatter(console_formatter)

    # file_handler
    file_handler = RotatingFileHandler(
        filename=log_dir / log_file, maxBytes=1000000, backupCount=3, encoding="utf-8"
    )

    file_handler.setLevel(log_level)

    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


logger = setup_logging("kafka.log")

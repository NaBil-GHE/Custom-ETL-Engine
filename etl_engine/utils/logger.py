import logging
import os
from datetime import datetime


def setup_logger(name: str = "etl_engine", log_dir: str = "logs", level: int = logging.INFO) -> logging.Logger:
    """
    Configure and return a logger that writes to both console and a rotating log file.
    """
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"etl_{datetime.now().strftime('%Y%m%d')}.log")

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(level)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger

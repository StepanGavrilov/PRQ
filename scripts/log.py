import os
import logging

from pathlib import Path
from functools import lru_cache
from typing import Union, Optional

from pydantic import BaseModel


def convert_bytes(b, to, bsize=1024):
    a = {"k": 1, "m": 2, "g": 3, "t": 4, "p": 5, "e": 6}
    return b / (bsize ** a[to])


LOGGER_FILE = Path(os.getenv("PRQ_LOGS", f"{os.curdir}/logs"))
if LOGGER_FILE.exists() and convert_bytes(LOGGER_FILE.stat().st_size, to="m") > 512:
    os.system(f"rm {LOGGER_FILE.absolute()}")

DATE_FORMAT = "%d %b %Y | %H:%M:%S"
LOGGER_FORMAT = "%(asctime)s | %(message)s"


class LoggerConfig(BaseModel):
    handlers: list
    format: str
    date_format: Union[str, None] = None
    logger_file: Optional[Path] = None
    level: int = logging.INFO


@lru_cache
def get_logger_config():
    from rich.logging import RichHandler

    output_file_handler = logging.FileHandler(LOGGER_FILE)
    handler_format = logging.Formatter(LOGGER_FORMAT, datefmt=DATE_FORMAT)
    output_file_handler.setFormatter(handler_format)

    return LoggerConfig(
        handlers=[
            RichHandler(
                rich_tracebacks=True, tracebacks_show_locals=True, show_time=False
            ),
            output_file_handler,
        ],
        format=LOGGER_FORMAT,
        date_format=DATE_FORMAT,
        logger_file=LOGGER_FILE,
        level=logging.CRITICAL,
    )


def setup_rich_logger():
    for name in logging.root.manager.loggerDict.keys():
        logging.getLogger(name).handlers = []
        logging.getLogger(name).propagate = True

    logger_config = get_logger_config()
    logging.basicConfig(
        level=logger_config.level,
        format=logger_config.format,
        datefmt=logger_config.date_format,
        handlers=logger_config.handlers,
    )


setup_rich_logger()
lloger = logging.getLogger()

# pipeline/logger.py

import logging
import os
import sys

_logger_cache = {}

def setup_logger(name: str, log_file: str = "logs/pipeline.log", level: str = "INFO", stream_to_stdout: bool = False) -> logging.Logger:
    if name in _logger_cache:
        return _logger_cache[name]

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False

    if not logger.handlers:

        file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
        console_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')

        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(file_formatter)
        logger.addHandler(fh)

        ch = logging.StreamHandler(sys.stdout if stream_to_stdout else sys.stderr)
        ch.setLevel(getattr(logging, level.upper(), logging.INFO))
        ch.setFormatter(console_formatter)
        logger.addHandler(ch)

    _logger_cache[name] = logger
    return logger
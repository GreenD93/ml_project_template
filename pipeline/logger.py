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
    logger.propagate = False  # 중복 출력 방지

    if not logger.handlers:
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')

        # File handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # Console handler
        console_stream = sys.stdout if stream_to_stdout else sys.stderr
        ch = logging.StreamHandler(console_stream)
        ch.setLevel(getattr(logging, level.upper(), logging.INFO))
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    _logger_cache[name] = logger
    return logger

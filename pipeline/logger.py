# pipeline/logger.py

import logging
import os
import sys
import traceback

_logger_cache = {}

def setup_logger(
    name: str,
    log_file: str = "logs/pipeline.log",
    level: str = "INFO",
    stream_to_stdout: bool = True,
    split_streams: bool = True,
    env: str = "DEV"  # "DEV" 또는 "PRD"
) -> logging.Logger:
    
    if name in _logger_cache:
        return _logger_cache[name]

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False

    if not logger.handlers:

        env_tag = f"[{env.upper()}]"

        file_formatter = logging.Formatter(f"{env_tag} [%(asctime)s] %(levelname)s %(name)s: %(message)s")
        console_formatter = logging.Formatter(f"{env_tag} [%(asctime)s] [%(levelname)s] %(message)s")

        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(file_formatter)
        logger.addHandler(fh)

        if split_streams:
            # ✅ stdout handler (DEBUG, INFO)
            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setLevel(logging.DEBUG)
            stdout_handler.addFilter(lambda record: record.levelno < logging.WARNING)
            stdout_handler.setFormatter(console_formatter)
            logger.addHandler(stdout_handler)

            # ✅ stderr handler (WARNING 이상)
            stderr_handler = logging.StreamHandler(sys.stderr)
            stderr_handler.setLevel(logging.WARNING)
            stderr_handler.setFormatter(console_formatter)
            logger.addHandler(stderr_handler)

        else:
            # 기존 방식: 전체 로그를 stdout 또는 stderr로만
            stream = sys.stdout if stream_to_stdout else sys.stderr
            ch = logging.StreamHandler(stream)
            ch.setLevel(getattr(logging, level.upper(), logging.INFO))
            ch.setFormatter(console_formatter)
            logger.addHandler(ch)

    _logger_cache[name] = logger
    return logger
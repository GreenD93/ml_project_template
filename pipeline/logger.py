# pipeline/logger.py

import logging
import os

_logger_cache = {}

def setup_logger(name: str, log_file: str = "logs/pipeline.log", level: str = "INFO") -> logging.Logger:
    if name in _logger_cache:
        return _logger_cache[name]

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False  # 중복 출력 방지

    if not logger.handlers:  # 중복 방지
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')

        # File handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)  # 파일에는 항상 상세히 기록
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(getattr(logging, level.upper(), logging.INFO))
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    _logger_cache[name] = logger
    return logger
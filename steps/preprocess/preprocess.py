# steps/train/train.py

import argparse
import os
from pipeline.config_loader import ConfigLoader
from pipeline.logger import setup_logger  # ✅ 변경됨
import time

def parse_args():
    parser = argparse.ArgumentParser(description="Step: preprocess")
    parser.add_argument('--config_file', type=str, help='Path to config file', required=True)
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    config_loader = ConfigLoader(args.config_file, validate=False)

    logger_name = os.environ.get("STEP_LOGGER_NAME", "preprocess")
    log_file = config_loader.get_log_file()
    log_level = config_loader.get_log_level()  # ✅ config.yaml과 연동

    logger = setup_logger(logger_name, log_file=log_file, level=log_level)
    logger.info("Running step 'preprocess' with config: %s", args.config_file)

    # step 로직 here...

    time.sleep(10)
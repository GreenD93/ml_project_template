# steps/inference/inference.py

import argparse
import os
from pipeline.config_loader import ConfigLoader
from pipeline.logger import setup_logger

def parse_args():
    parser = argparse.ArgumentParser(description="Step: inference")
    parser.add_argument('--config_file', type=str, help='Path to config file', required=True)
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    config_loader = ConfigLoader(args.config_file, validate=False)

    # 환경 변수에서 STEP_LOGGER_NAME 가져오기 (fallback: inference)
    logger_name = os.environ.get("STEP_LOGGER_NAME", "inference")
    logger = setup_logger(logger_name, log_file=config_loader.get_log_file())

    logger.info("Running step inference with config: %s", args.config_file)
    # 여기에 step 로직
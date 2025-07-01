# steps/train/train.py
import json
import argparse
import os
from pipeline.config_loader import ConfigLoader
from pipeline.logger import setup_logger  # ✅ 변경됨

def parse_args():
    parser = argparse.ArgumentParser(description="Step: inference")
    parser.add_argument('--config_file', type=str, help='Path to config file', required=True)
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    config_loader = ConfigLoader(args.config_file, validate=False)

    logger_name = os.environ.get("STEP_LOGGER_NAME", "inference")
    log_file = config_loader.get_log_file()
    log_level = config_loader.get_log_level()  # ✅ config.yaml과 연동

    logger = setup_logger(logger_name, log_file=log_file, level=log_level)
    logger.info("Running step 'inference' with config: %s", args.config_file)

    # ✅ 실제 파라미터 출력
    inference_config = config_loader.config_data.get("config", {})
    logger.info(f"Training config loaded: {inference_config}")
    logger.info(f"[INFO] Inference config: {json.dumps(inference_config, indent=2)}")

    # 실제 로직 실행
    logger.info(json.dumps({"success": True}))
    # step 로직 here...
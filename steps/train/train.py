# steps/train/train.py
import sys, json
import argparse
import os
from pipeline.config_loader import ConfigLoader
from pipeline.logger import setup_logger  # ✅ 변경됨

def training_needed():
    # 파일 존재 여부, 날짜, 메타데이터 판단 등
    return False

def parse_args():
    parser = argparse.ArgumentParser(description="Step: train")
    parser.add_argument('--config_file', type=str, help='Path to config file', required=True)
    return parser.parse_args()

if __name__ == "__main__":

    if not training_needed():
        print(json.dumps({"skipped": True}))
        sys.exit(0)

    args = parse_args()
    config_loader = ConfigLoader(args.config_file, validate=False)

    logger_name = os.environ.get("STEP_LOGGER_NAME", "train")
    log_file = config_loader.get_log_file()
    log_level = config_loader.get_log_level()  # ✅ config.yaml과 연동

    logger = setup_logger(logger_name, log_file=log_file, level=log_level)

    # ✅ 실제 파라미터 출력
    train_config = config_loader.config_data.get("config", {})
    logger.info(f"Training config loaded: {train_config}")
    logger.info(f"[INFO] Training config: {json.dumps(train_config, indent=2)}")

    # 실제 로직 실행
    print(json.dumps({"success": True}))

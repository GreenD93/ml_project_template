# steps/a.py
import argparse
from pipeline.config_loader import ConfigLoader
from pipeline.logger import setup_logger

def parse_args():
    parser = argparse.ArgumentParser(description="Step A")
    parser.add_argument('--config_file', type=str, help='Path to config file', required=True)
    return parser.parse_args()

if __name__ == "__main__":
    # argparse로 인자 받기
    args = parse_args()

    # config_file을 여전히 config_loader에서 읽어옴
    config_loader = ConfigLoader(args.config_file)

    # 로거 설정
    logger = setup_logger("step_a", log_file=config_loader.get_log_file())

    logger.info("Running step A with config: %s", args.config_file)
    # 여기에 실제 step 로직 넣기

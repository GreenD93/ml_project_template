from pipeline.pipeline_builder import PipelineBuilder
from pipeline.config_loader import ConfigLoader
from pipeline.logger import setup_logger

import argparse

config_file = "configs/config.yaml"

def parse_args():
    parser = argparse.ArgumentParser(description="Configuration")
    parser.add_argument('--config_file', 
                        type=str, 
                        help='Path to config file',
                        default='configs/config.yaml',
                        required=True)
    
    return parser.parse_args()

if __name__ == "__main__":

    # argparse로 인자 받기
    args = parse_args()

    # config_file을 여전히 config_loader에서 읽어옴
    config_loader = ConfigLoader(args.config_file)

    # 로거 설정
    logger = setup_logger("main", log_file=config_loader.get_log_file())
    logger.info("Running ML process with config: %s", args.config_file)

    # PipelineBuilder 생성
    builder = PipelineBuilder(config_file)

    # config.yaml에 정의된 절대 경로로 변환된 각 step config 파일을 넘겨줌
    builder.add_step("step_a", "steps/a.py", builder.config_files[0])  # a.yaml 경로
    builder.add_step("step_b", "steps/b.py", builder.config_files[1])  # b.yaml 경로

    # 전체 파이프라인 실행
    builder.run_all()
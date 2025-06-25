from pipeline.pipeline_builder import PipelineBuilder
from pipeline.logger import setup_logger

if __name__ == "__main__":
    config_file = "configs/config.yaml"

    # PipelineBuilder 생성
    builder = PipelineBuilder(config_file)

    # config.yaml에 정의된 절대 경로로 변환된 각 step config 파일을 넘겨줌
    builder.add_step("step_a", "steps/a.py", builder.config_files[0])  # a.yaml 경로
    builder.add_step("step_b", "steps/b.py", builder.config_files[1])  # b.yaml 경로

    # 전체 파이프라인 실행
    builder.run_all()
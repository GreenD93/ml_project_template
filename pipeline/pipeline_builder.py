import os
from pipeline.step_runner import StepRunner
from pipeline.logger import setup_logger
from typing import List
from concurrent.futures import ThreadPoolExecutor
from config_loader import ConfigLoader

class PipelineBuilder:
    def __init__(self, config_file: str, logger=None):
        # config_loader를 생성합니다.
        self.config_loader = ConfigLoader(config_file)
        
        # 로거 설정
        self.logger = logger or setup_logger("pipeline", self.config_loader.get_log_file())
        
        # config 파일과 로그 설정
        self.config_files = self.config_loader.get_absolute_config_files()
        self.steps = []
        self.failed_steps = []

    def add_step(self, name: str, script_path: str, config_path: str = None):
        # config_path가 없으면 기본 config 파일을 사용합니다.
        config_path = config_path or self.config_files[0]
        self.steps.append(StepRunner(name, script_path, config_path, self.config_loader.get_log_file(), self.logger, self.config_loader.project_dir))

    def run_all(self):
        self.logger.info("Pipeline execution started.")

        # 병렬 실행을 위해 ThreadPoolExecutor 사용
        with ThreadPoolExecutor(max_workers=len(self.steps)) as executor:
            futures = {executor.submit(step.run, "subprocess"): step.name for step in self.steps}
            for future in futures:
                try:
                    future.result()  # 예외 발생 시 여기서 처리
                except Exception as e:
                    self.logger.error(f"[{futures[future]}] Error: {e}")
                    self.failed_steps.append(futures[future])

        if self.failed_steps:
            self.logger.error("\n❌ Some steps failed:")
            for name in self.failed_steps:
                self.logger.error(f"  - {name}")
        else:
            self.logger.info("\n✅ All steps completed successfully.")
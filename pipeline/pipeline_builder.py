from pipeline.step_runner import StepRunner
from pipeline.logger import setup_logger
from pipeline.config_loader import ConfigLoader

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

        # DAG 정의를 읽어들여 실행 순서 정리
        self.dag = self.config_loader.config_data.get("dag", [])

    def add_step(self, name: str, script_path: str, config_path: str = None):
        # config_path가 없으면 기본 config 파일을 사용합니다.
        config_path = config_path or self.config_files[0]
        self.steps.append(StepRunner(name, script_path, config_path, self.config_loader.get_log_file(), self.logger, self.config_loader.project_dir))

    def run_all(self):
        self.logger.info("Pipeline execution started.")
        
        # DAG에 정의된 순서대로 실행
        for step_name in self.dag:
            step = next((s for s in self.steps if s.name == step_name), None)
            if step:
                if not step.run("subprocess"):
                    self.failed_steps.append(step.name)

        if self.failed_steps:
            self.logger.error("\n❌ Some steps failed:")
            for name in self.failed_steps:
                self.logger.error(f"  - {name}")
        else:
            self.logger.info("\n✅ All steps completed successfully.")
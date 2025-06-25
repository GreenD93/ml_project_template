import subprocess
import os
import time
from pipeline.logger import setup_logger
from typing import Literal
import multiprocessing

class StepRunner:
    def __init__(self, name: str, script_path: str, config_path: str, log_file: str = None, logger=None, project_dir: str = None, retries: int = 3):
        self.name = name
        self.script = script_path
        self.config = config_path
        self.log_file = log_file or "logs/pipeline.log"
        self.logger = logger or setup_logger(name, self.log_file)
        self.project_dir = project_dir
        self.retries = retries  # 재시도 횟수

    def run_subprocess(self) -> bool:
        self.logger.info(f"[{self.name}] Starting subprocess...")
        attempt = 0
        while attempt < self.retries:
            try:
                # 환경 변수 설정
                env = os.environ.copy()
                env['PROJECT_DIR'] = self.project_dir

                # subprocess에서 config_file을 --config_file로 넘겨서 실행
                result = subprocess.run(
                    ["python", self.script, "--config_file", self.config],
                    check=True,
                    env=env
                )

                self.logger.info(f"[{self.name}] ✅ Step completed successfully.")
                return True
            except subprocess.CalledProcessError as e:
                self.logger.error(f"[{self.name}] ❌ Step failed (code {e.returncode}). Attempt {attempt + 1}/{self.retries}")
                attempt += 1
                time.sleep(2)  # 잠시 대기 후 재시도
            except Exception as e:
                self.logger.exception(f"[{self.name}] ❌ Unexpected error occurred.")
                return False
        return False  # 재시도 횟수 초과 시 실패 처리

    def run(self, mode: Literal["subprocess", "sagemaker", "shell"] = "subprocess") -> bool:
        if mode == "subprocess":
            return self.run_subprocess()
        else:
            raise ValueError(f"Unsupported run mode: {mode}")
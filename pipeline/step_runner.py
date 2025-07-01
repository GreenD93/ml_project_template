# pipeline/step_runner.py

import subprocess
import os
import time
from typing import Literal
from pipeline.logger import setup_logger  # ✅ 개선된 로거 사용

class StepRunner:
    def __init__(
        self,
        name,
        script_path,
        config_path,
        log_file=None,
        logger=None,
        project_dir=None,
        retries=1,
        log_level=None
    ):
        self.name = name
        self.script = script_path
        self.config = config_path

        # 기본값 설정
        self.log_file = log_file or "logs/pipeline.log"
        self.log_level = log_level or os.environ.get("LOG_LEVEL", "INFO")

        # logger가 전달되지 않았을 경우 setup
        self.logger = logger or setup_logger(
            name,
            log_file=self.log_file,
            level=self.log_level
        )

        self.project_dir = project_dir
        self.retries = retries

    def run_subprocess(self) -> dict:
        self.logger.info(f"[{self.name}] Starting subprocess...")
        attempt = 0

        while attempt < self.retries:
            try:
                env = os.environ.copy()
                if self.project_dir:
                    env["PROJECT_DIR"] = self.project_dir

                result = subprocess.run(
                    ["python", self.script, "--config_file", self.config],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env,
                    text=True,
                    check=True
                )

                self.logger.info(f"[{self.name}] ✅ Success")
                if result.stdout.strip():
                    self.logger.info(f"[{self.name}] stdout:\n{result.stdout.strip()}")
                if result.stderr.strip():
                    self.logger.warning(f"[{self.name}] stderr:\n{result.stderr.strip()}")

                return {
                    "success": True,
                    "stdout": result.stdout.strip(),
                    "stderr": result.stderr.strip()
                }

            except subprocess.CalledProcessError as e:
                attempt += 1
                error_msg = f"Return code {e.returncode}. stderr: {e.stderr.strip()}"
                self.logger.error(f"[{self.name}] ❌ Failed attempt {attempt}: {error_msg}")
                if e.stdout.strip():
                    self.logger.error(f"[{self.name}] stdout:\n{e.stdout.strip()}")
                if e.stderr.strip():
                    self.logger.error(f"[{self.name}] stderr:\n{e.stderr.strip()}")
                time.sleep(1)

            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                self.logger.exception(f"[{self.name}] ❌ {error_msg}")
                return {
                    "success": False,
                    "error": error_msg
                }

        return {
            "success": False,
            "error": f"Step '{self.name}' failed after {self.retries} attempt(s)."
        }

    def run(self, mode: Literal["subprocess", "sagemaker", "shell"] = "subprocess") -> dict:
        if mode == "subprocess":
            return self.run_subprocess()
        else:
            return {
                "success": False,
                "error": f"Unsupported mode: {mode}"
            }

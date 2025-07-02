# pipeline/step_runner.py
import subprocess
import os
import time
import json
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
        retries=1,
        log_level=None,
        target_date=None
    ):
        self.name = name
        self.script = script_path
        self.config = config_path

        self.log_file = log_file or "logs/pipeline.log"
        self.log_level = log_level or os.environ.get("LOG_LEVEL", "INFO")
        self.logger = logger or setup_logger(
            name,
            log_file=self.log_file,
            level=self.log_level
        )
        self.retries = retries

        self.target_date = target_date

    def run_subprocess(self) -> dict:
        self.logger.info(f"[{self.name}] Starting subprocess...")
        attempt = 0

        while attempt < self.retries:
            try:
                env = os.environ.copy()

                result = subprocess.run(
                    ["python", self.script, "--config_file", self.config, "--target_date", self.target_date],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env,
                    text=True,
                    check=True
                )

                stdout_clean = result.stdout.strip()
                stderr_clean = result.stderr.strip()

                # 🔍 try parsing JSON from stdout
                try:
                    output_json = json.loads(stdout_clean)
                    if output_json.get("skipped"):
                        self.logger.warning(f"[{self.name}] ⚠️ Step skipped by logic.")
                        return {
                            "skipped": True,
                            "stdout": stdout_clean,
                            "stderr": stderr_clean
                        }
                except Exception:
                    pass  # Ignore if not JSON

                self.logger.info(f"[{self.name}] ✅ Success")
                if stdout_clean:
                    self.logger.info(f"[{self.name}] stdout:\n{stdout_clean}")
                if stderr_clean:
                    self.logger.warning(f"[{self.name}] stderr:\n{stderr_clean}")

                return {
                    "success": True,
                    "stdout": stdout_clean,
                    "stderr": stderr_clean
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
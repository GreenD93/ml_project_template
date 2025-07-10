# pipeline/step_runner.py

import subprocess
import os
import time
import json
from typing import Literal, Optional
from pipeline.logger import setup_logger


class StepRunner:
    def __init__(
        self,
        name: str,
        script_path: str,
        config_path: str,
        log_file: Optional[str] = None,
        logger=None,
        retries: int = 1,
        log_level: Optional[str] = None,
        target_date: Optional[str] = None
    ):
        self.name = name
        self.script = script_path
        self.config = config_path
        self.log_file = log_file or "logs/pipeline.log"
        self.log_level = log_level or os.environ.get("LOG_LEVEL", "INFO")
        self.logger = logger or setup_logger(name, log_file=self.log_file, level=self.log_level)
        self.retries = retries
        self.target_date = target_date

    def run_subprocess(self) -> dict:
        self.logger.info(f"[{self.name}] Starting subprocess...")
        attempt = 0

        while attempt < self.retries:
            attempt += 1
            try:
                cmd = ["python", self.script, "--config_file", self.config]
                if self.target_date:
                    cmd += ["--target_date", self.target_date]

                env = os.environ.copy()
                result = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env,
                    text=True,
                    check=True
                )

                stdout_clean = result.stdout.strip()
                stderr_clean = result.stderr.strip()

                try:
                    output_json = json.loads(stdout_clean)
                    if output_json.get("skipped"):
                        self.logger.warning(f"[{self.name}] ⚠️ Step skipped by logic.")
                        return {"skipped": True, "stdout": stdout_clean, "stderr": stderr_clean}
                except json.JSONDecodeError:
                    pass  # 정상적으로 JSON이 아닐 수 있음

                self.logger.info(f"[{self.name}] ✅ Success")
                if stdout_clean:
                    self.logger.info(f"[{self.name}] stdout:\n{stdout_clean}")
                if stderr_clean:
                    self.logger.warning(f"[{self.name}] stderr:\n{stderr_clean}")

                return {"success": True, "stdout": stdout_clean, "stderr": stderr_clean}

            except subprocess.CalledProcessError as e:
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
                return {"success": False, "error": error_msg}

        return {
            "success": False,
            "error": f"Step '{self.name}' failed after {self.retries} attempt(s)."
        }

    def run(self, mode: Literal["subprocess", "sagemaker", "shell"] = "subprocess") -> dict:
        if mode == "subprocess":
            return self.run_subprocess()
        else:
            raise NotImplementedError(f"Run mode '{mode}' is not supported yet.")

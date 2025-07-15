# pipeline/step_runner.py

import subprocess
import os
import time
import json
import threading
from typing import Literal, Optional
from pipeline.logger import setup_logger

ERROR_KEYWORDS = {"traceback", "error", "exception", "failed", "fatal"}

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

    def _log_stream(self, pipe, collector: list):
        try:
            for line in iter(pipe.readline, ''):
                line = line.rstrip()
                collector.append(line)

                if any(kw in line.lower() for kw in ERROR_KEYWORDS):
                    self.logger.error(f"[{self.name}] {line}")
                else:
                    self.logger.info(f"[{self.name}] {line}")
        except Exception as e:
            self.logger.error(f"[{self.name}] ⚠️ stream error: {str(e)}")

    def run_subprocess(self) -> dict:
        self.logger.info(f"[{self.name}] Starting subprocess...")
        attempt = 0

        while attempt < self.retries:
            attempt += 1
            try:
                cmd = ["python", "-u", self.script, "--config_file", self.config]
                if self.target_date:
                    cmd += ["--target_date", self.target_date]

                env = os.environ.copy()
                env["PYTHONUNBUFFERED"] = "1"

                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    env=env,
                    text=True,
                    bufsize=1
                )

                stdout_lines = []
                t_out = threading.Thread(target=self._log_stream, args=(process.stdout, stdout_lines), daemon=True)
                t_out.start()

                return_code = process.wait()
                t_out.join()

                stdout_clean = "\n".join(stdout_lines)

                if return_code == 0:
                    try:
                        output_json = json.loads(stdout_clean)
                        if output_json.get("skipped"):
                            self.logger.warning(f"[{self.name}] ⚠️ Step skipped by logic.")
                            return {"skipped": True, "stdout": stdout_clean, "stderr": ""}
                    except json.JSONDecodeError:
                        pass

                    self.logger.info(f"[{self.name}] ✅ Success")
                    return {"success": True, "stdout": stdout_clean, "stderr": ""}
                else:
                    self.logger.error(f"[{self.name}] ❌ Failed with return code {return_code}")
                    return {"success": False, "stdout": stdout_clean, "stderr": ""}

            except Exception as e:
                self.logger.exception(f"[{self.name}] ❌ Unexpected error: {str(e)}")
                time.sleep(1)

        return {
            "success": False,
            "error": f"Step '{self.name}' failed after {self.retries} attempt(s)."
        }

    def run(self, mode: Literal["subprocess", "sagemaker", "shell"] = "subprocess") -> dict:
        if mode == "subprocess":
            return self.run_subprocess()
        else:
            raise NotImplementedError(f"Run mode '{mode}' is not supported yet.")
import subprocess
import os
import time
import json
import threading
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

    def _log_stream(self, pipe, log_func, collector):
        try:
            for line in iter(pipe.readline, ''):
                log_func(f"[{self.name}] {line.rstrip()}")
                collector.append(line.rstrip())
        except Exception as e:
            log_func(f"[{self.name}] ⚠️ stream error: {str(e)}")

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
                    stderr=subprocess.PIPE,
                    env=env,
                    text=True,
                    bufsize=1  # line buffered
                )

                stdout_lines = []
                stderr_lines = []

                t_out = threading.Thread(target=self._log_stream, args=(process.stdout, self.logger.info, stdout_lines), daemon=True)
                t_err = threading.Thread(target=self._log_stream, args=(process.stderr, self.logger.warning, stderr_lines), daemon=True)

                t_out.start()
                t_err.start()

                return_code = process.wait()
                
                t_out.join()
                t_err.join()

                stdout_clean = "\n".join(stdout_lines)
                stderr_clean = "\n".join(stderr_lines)

                if return_code == 0:
                    try:
                        output_json = json.loads(stdout_clean)
                        if output_json.get("skipped"):
                            self.logger.warning(f"[{self.name}] ⚠️ Step skipped by logic.")
                            return {"skipped": True, "stdout": stdout_clean, "stderr": stderr_clean}
                    except json.JSONDecodeError:
                        pass

                    self.logger.info(f"[{self.name}] ✅ Success")
                    return {"success": True, "stdout": stdout_clean, "stderr": stderr_clean}
                else:
                    self.logger.error(f"[{self.name}] ❌ Failed with return code {return_code}")
                    return {"success": False, "stdout": stdout_clean, "stderr": stderr_clean}

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
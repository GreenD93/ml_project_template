# pipeline/step_runner.py

import subprocess
import os
import time
import json
import threading
from typing import Literal, Optional
from pipeline.logger import setup_logger
import re

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

    def _log_stream(self, pipe, collector: list, default_level="INFO"):
        try:
            traceback_buffer = []
            in_traceback = False

            for line in iter(pipe.readline, ''):
                line = line.rstrip()
                collector.append(line)

                # start traceback block
                if "Traceback (most recent call last):" in line:
                    in_traceback = True
                    traceback_buffer.append(line)
                    continue

                # if already in traceback, keep collecting
                if in_traceback:
                    traceback_buffer.append(line)
                    if re.match(r"^\w*(Error|Exception|SyntaxError):", line.strip()):
                        # traceback block ends here
                        for tb_line in traceback_buffer:
                            self.logger.error(f"[{self.name}] {tb_line}")
                        traceback_buffer.clear()
                        in_traceback = False
                    continue

                # fallback classification
                if any(kw in line.lower() for kw in ERROR_KEYWORDS):
                    self.logger.error(f"[{self.name}] {line}")
                else:
                    if default_level == "WARNING":
                        self.logger.warning(f"[{self.name}] {line}")
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
                    stderr=subprocess.PIPE,
                    env=env,
                    text=True,
                    bufsize=1
                )

                stdout_lines = []
                stderr_lines = []

                t_out = threading.Thread(target=self._log_stream, args=(process.stdout, stdout_lines), daemon=True)
                t_err = threading.Thread(target=self._log_stream, args=(process.stderr, stderr_lines), daemon=True)

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
        
    def extract_traceback_block(self, stderr_lines: list[str]) -> str:
        """stderr에서 Traceback 블록만 추출"""
        start_idx = None
        end_idx = None

        for i, line in enumerate(stderr_lines):
            if "Traceback (most recent call last):" in line:
                start_idx = i
                end_idx = None  # reset
            elif start_idx is not None and re.match(r"^\w*(Error|Exception|Warning):", line.strip()):
                end_idx = i

        if start_idx is not None and end_idx is not None:
            return "\n".join(stderr_lines[start_idx:end_idx + 1])
        elif start_idx is not None:
            # fallback: Traceback은 떴는데 끝줄을 못찾은 경우
            return "\n".join(stderr_lines[start_idx:])
        else:
            # fallback: 마지막 에러 키워드 줄
            for line in reversed(stderr_lines):
                if any(k in line.lower() for k in ("error", "exception", "traceback", "failed", "fatal")):
                    return line
            return "Unknown error"
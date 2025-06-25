import os
import yaml
from typing import Any, List

class ConfigLoader:
    def __init__(self, config_file: str, project_dir: str = None):
        self.config_file = config_file
        self.config_data = self._load_config(config_file)
        if not self.config_data:
            raise ValueError(f"Config data is empty or not loaded correctly from {config_file}")

        # project_dir을 상위 config.yaml에서만 가져옵니다.
        self.project_dir = project_dir or self.config_data.get("paths", {}).get("project_dir", None)

    def _load_config(self, config_file: str) -> dict[str, Any]:
        """ config.yaml 파일을 읽어옵니다. """
        try:
            with open(config_file, "r") as f:
                data = yaml.safe_load(f)
                if data is None:
                    raise ValueError(f"YAML file is empty: {config_file}")
                return data
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file '{config_file}' not found.")
        except yaml.YAMLError as e:
            raise ValueError(f"Error reading YAML file '{config_file}': {e}")

    def get_absolute_config_files(self) -> List[str]:
        """ config_files 항목을 절대 경로로 변환하여 반환 """
        config_files = self.config_data["paths"].get("config_files", [])
        if not config_files:
            raise ValueError(f"Missing 'config_files' in config file {self.config_file}")
        return [self._get_absolute_path(f) for f in config_files]

    def _get_absolute_path(self, relative_path: str) -> str:
        """ 상대 경로를 절대 경로로 변환 """
        return os.path.join(self.project_dir, relative_path)

    def get_log_file(self) -> str:
        """ log_file 경로를 가져옵니다. """
        return self.config_data.get("logging", {}).get("log_file", "logs/pipeline.log")
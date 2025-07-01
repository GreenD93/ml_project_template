# pipeline/config_loader.py
import os
import yaml

class ConfigLoader:
    def __init__(self, config_file: str, project_dir: str = None, validate: bool = True):
        self.config_file = config_file
        self.config_data = self._load_config(config_file)

        self.project_dir = (
            project_dir
            or self.config_data.get("paths", {}).get("project_dir")
            or os.environ.get("PROJECT_DIR")
        )

        if not self.project_dir:
            raise ValueError("project_dir must be specified in config.yaml or via env var PROJECT_DIR")

        if validate:
            self._validate_config()

    def _load_config(self, config_file):
        with open(config_file, "r") as f:
            data = yaml.safe_load(f)
            if not data:
                raise ValueError(f"Empty config: {config_file}")
            return data

    def _validate_config(self):
        if "logging" not in self.config_data:
            raise ValueError("Missing 'logging' section in config file.")
        if "dag" not in self.config_data:
            raise ValueError("Missing 'dag' section in config file.")
        if "paths" not in self.config_data or "project_dir" not in self.config_data["paths"]:
            raise ValueError("Missing 'paths.project_dir' in config file.")

    def get_log_file(self):
        return self.config_data.get("logging", {}).get("log_file", "logs/pipeline.log")

    def get_log_level(self):
        return self.config_data.get("logging", {}).get("level", "INFO")
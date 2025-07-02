# pipeline/config_loader.py
import os
import yaml
from steps.settings import *

class ConfigLoader:
    def __init__(self, config_file: str, validate: bool = True):

        self.config_file = config_file
        self.config_data = self._load_config(config_file)

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
    
    def get_global_config(self) -> GlobalConfig:
        global_data = self.config_data.get("global")
        if not global_data:
            raise ValueError("Missing 'global' section in config file")
        return GlobalConfig.from_dict(global_data)
# pipeline/config_loader.py

import yaml
from steps.settings import GlobalConfig

class ConfigLoader:
    def __init__(self, config_file: str, validate: bool = True):
        self.config_file = config_file
        self.config_data = self._load_config(config_file)

        if validate:
            self._validate_config()

    def _load_config(self, config_file: str) -> dict:
        try:
            with open(config_file, "r") as f:
                data = yaml.safe_load(f)
                if not data:
                    raise ValueError(f"Empty config: {config_file}")
                return data
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {config_file}: {str(e)}")

    def _validate_config(self):
        required_fields = ["logging", "dag"]
        for field in required_fields:
            if field not in self.config_data:
                raise ValueError(f"Missing '{field}' section in config file.")

    def get_log_file(self, step_name=None) -> str:
        
        if step_name:
            return self.config_data.get("logging", {}).get(step_name, "logs/pipeline.log")
        else:
            return self.config_data.get("logging", {}).get("log_file", "logs/pipeline.log")

    def get_log_level(self) -> str:
        return self.config_data.get("logging", {}).get("level", "INFO")

    def get_global_config(self) -> GlobalConfig:
        global_data = self.config_data.get("global")
        if not global_data:
            raise ValueError("Missing 'global' section in config file")
        return GlobalConfig.from_dict(global_data)
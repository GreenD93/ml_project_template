# pipeline/pipeline_builder.py
import os
from pipeline.step_runner import StepRunner
from pipeline.logger import setup_logger

class PipelineBuilder:
    def __init__(self, config_loader, logger=None):
        self.config_loader = config_loader
        self.logger = logger or setup_logger("pipeline", self.config_loader.get_log_file(), self.config_loader.get_log_level())
        self.steps = []
        self.failed_steps = []
        self._register_steps()

    def _register_steps(self):
        log_file = self.config_loader.get_log_file()
        log_level = self.config_loader.get_log_level()
        dag_config = self.config_loader.config_data.get("dag", {})

        for step_name, step_info in dag_config.items():
            script = step_info.get("script")
            config_rel_path = step_info.get("config")
            retries = step_info.get("retries", 1)

            if not script or not config_rel_path:
                raise ValueError(f"Step '{step_name}' must have 'script' and 'config'.")

            config_path = os.path.join(self.config_loader.project_dir, config_rel_path)
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Config for step '{step_name}' not found: {config_path}")

            self.logger.info(f"Registering step: {step_name} -> {script}")
            step_logger = setup_logger(step_name, log_file, log_level)
            self.steps.append(StepRunner(step_name, script, config_path, log_file, step_logger, self.config_loader.project_dir, retries, log_level))

    def get_step_names(self):
        return [step.name for step in self.steps]

    def run_all(self):
        self.logger.info("🚀 Pipeline execution started.")
        success_steps = []

        for step in self.steps:
            self.logger.info(f"▶️ Running step: {step.name}")
            result = step.run("subprocess")

            if result.get("success"):
                success_steps.append(step.name)
            else:
                self.logger.error(f"❌ {step.name} failed: {result.get('error')}")
                self.logger.error(f"stdout:\n{result.get('stdout')}\nstderr:\n{result.get('stderr')}")
                self.failed_steps.append((step.name, result.get("error")))

        self.logger.info("📋 Pipeline Summary")
        if success_steps:
            self.logger.info(f"✅ Successful: {', '.join(success_steps)}")
        if self.failed_steps:
            self.logger.error("❌ Failed steps:")
            for name, reason in self.failed_steps:
                self.logger.error(f" - {name}: {reason}")
        else:
            self.logger.info("🎉 All steps completed successfully.")

    def run_step(self, step_name):
        step_dict = {s.name: s for s in self.steps}
        step = step_dict.get(step_name)
        if not step:
            self.logger.error(f"Step '{step_name}' not found in DAG.")
            return

        result = step.run("subprocess")
        if result.get("success"):
            self.logger.info(f"✅ Step '{step_name}' completed successfully.")
        else:
            self.logger.error(f"❌ Step '{step_name}' failed: {result.get('error')}")
            self.logger.error(f"stdout:\n{result.get('stdout')}\nstderr:\n{result.get('stderr')}")

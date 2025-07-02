import os
from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from pipeline.step_runner import StepRunner
from pipeline.logger import setup_logger

def _run_step_wrapper(step: StepRunner):
    return step.name, step.run()

class PipelineBuilder:
    def __init__(self, config_loader, logger=None, target_date=None):
        self.config_loader = config_loader
        self.logger = logger or setup_logger(
            "pipeline", self.config_loader.get_log_file(), self.config_loader.get_log_level()
        )
        self.target_date = target_date
        self.steps = []
        self.failed_steps = []
        self.skipped_steps = []
        self._register_steps()

    def _register_steps(self):
        log_file = self.config_loader.get_log_file()
        log_level = self.config_loader.get_log_level()
        dag_config = self.config_loader.config_data.get("dag", {})

        for step_name, step_info in dag_config.items():
            script = step_info.get("script")
            config_path = step_info.get("config")
            retries = step_info.get("retries", 1)

            if not script or not config_path:
                raise ValueError(f"Step '{step_name}' must have 'script' and 'config'.")

            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Config for step '{step_name}' not found: {config_path}")

            self.logger.info(f"Registering step: {step_name} -> {script}")

            self.steps.append(StepRunner(
                name=step_name,
                script_path=script,
                config_path=config_path,
                log_file=log_file,
                logger=self.logger,
                retries=retries,
                log_level=log_level,
                target_date=self.target_date
            ))

    def _build_dependency_graph(self):
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        dag_config = self.config_loader.config_data.get("dag", {})

        for step_name, step_info in dag_config.items():
            deps = step_info.get("depends_on", [])
            for dep in deps:
                graph[dep].append(step_name)
                in_degree[step_name] += 1
            if step_name not in in_degree:
                in_degree[step_name] = 0
        return graph, in_degree

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
            elif result.get("skipped"):
                self.logger.warning(f"⚠️ Step '{step.name}' was skipped.")
                self.skipped_steps.append(step.name)
            else:
                self.logger.error(f"❌ Step '{step.name}' failed: {result.get('error')}")
                self.logger.error(f"stdout:\n{result.get('stdout')}\nstderr:\n{result.get('stderr')}")
                self.failed_steps.append((step.name, result.get("error")))

        self._print_summary(success_steps)

    def run_step(self, step_name):
        step_dict = {s.name: s for s in self.steps}
        step = step_dict.get(step_name)
        if not step:
            self.logger.error(f"Step '{step_name}' not found in DAG.")
            return

        result = step.run("subprocess")
        if result.get("success"):
            self.logger.info(f"✅ Step '{step_name}' completed successfully.")
        elif result.get("skipped"):
            self.logger.warning(f"⚠️ Step '{step_name}' was skipped by logic.")
            self.skipped_steps.append(step_name)
        else:
            self.logger.error(f"❌ Step '{step_name}' failed: {result.get('error')}")
            self.logger.error(f"stdout:\n{result.get('stdout')}\nstderr:\n{result.get('stderr')}")
            self.failed_steps.append((step_name, result.get("error")))

    def run_all_parallel(self, max_workers=4):
        
        self.logger.info("🚀 DAG parallel execution started.")
        graph, in_degree = self._build_dependency_graph()
        name_to_step = {step.name: step for step in self.steps}
        completed = set()
        queue = deque([name for name in in_degree if in_degree[name] == 0])
        success_steps = []

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            while queue:

                futures = {
                    executor.submit(_run_step_wrapper, name_to_step[step_name]): step_name
                    for step_name in list(queue)
                }
                queue.clear()

                for future in as_completed(futures):
                    step_name, result = future.result()
                    if result.get("success"):
                        self.logger.info(f"✅ Step '{step_name}' completed.")
                        success_steps.append(step_name)
                        completed.add(step_name)
                        for neighbor in graph[step_name]:
                            in_degree[neighbor] -= 1
                            if in_degree[neighbor] == 0:
                                queue.append(neighbor)
                    elif result.get("skipped"):
                        self.logger.warning(f"⚠️ Step '{step_name}' was skipped.")
                        self.skipped_steps.append(step_name)
                        completed.add(step_name)
                        for neighbor in graph[step_name]:
                            in_degree[neighbor] -= 1
                            if in_degree[neighbor] == 0:
                                queue.append(neighbor)
                    else:
                        self.logger.error(f"❌ Step '{step_name}' failed: {result.get('error')}")
                        self.failed_steps.append((step_name, result.get("error")))
                        self.logger.error("🛑 Aborting DAG execution due to failure.")
                        return

        self._print_summary(success_steps)

    def _print_summary(self, success_steps):
        self.logger.info("📋 Pipeline Summary")
        if success_steps:
            self.logger.info(f"✅ Successful: {', '.join(success_steps)}")
        if self.skipped_steps:
            self.logger.warning(f"⚠️ Skipped: {', '.join(self.skipped_steps)}")
        if self.failed_steps:
            self.logger.error("❌ Failed steps:")
            for name, reason in self.failed_steps:
                self.logger.error(f" - {name}: {reason}")
        else:
            self.logger.info("🎉 All steps completed successfully.")
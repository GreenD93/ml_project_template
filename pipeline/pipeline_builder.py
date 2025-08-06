import os
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline.step_runner import StepRunner
from pipeline.logger import setup_logger


def _run_step_wrapper(step: StepRunner):
    return step.name, step.run()


class PipelineBuilder:
    def __init__(self, config_loader, logger=None, target_date=None, selected_step=None):
        self.config_loader = config_loader

        self.env = self.config_loader.config_data.get("global")['env']

        self.logger = logger or setup_logger(
            "pipeline", self.config_loader.get_log_file(), self.config_loader.get_log_level()
        )
        self.target_date = target_date
        self.selected_step = selected_step

        self.steps = []
        self.failed_steps = []
        self.skipped_steps = []
        self._register_steps()

    def _register_steps(self):
        log_level = self.config_loader.get_log_level()
        dag_config = self.config_loader.config_data.get("dag", {})

        for step_name, step_info in dag_config.items():
            script = step_info.get("script")
            config_path = step_info.get("config")
            retries = step_info.get("retries", 1)

            step_logger = setup_logger(
                step_name, self.config_loader.get_log_file(step_name), self.config_loader.get_log_level()
            )

            if not script or not config_path:
                raise ValueError(f"Step '{step_name}' must have 'script' and 'config'.")

            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Config for step '{step_name}' not found: {config_path}")

            self.logger.info(f"Registering step: {step_name} -> {script}")

            self.steps.append(StepRunner(
                name=step_name,
                script_path=script,
                config_path=config_path,
                logger=step_logger,
                retries=retries,
                log_level=log_level,
                target_date=self.target_date
            ))
        self._print_dag_structure()

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
                self.logger.error(f"❌ Step '{step.name}' failed: {result.get('stderr')}")
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
            self.logger.error(f"❌ Step '{step_name}' failed: {result.get('stderr')}")
            self.failed_steps.append((step_name, result.get("error")))

    def run_all_parallel(self, max_workers=4):
        self.logger.info("🚀 DAG parallel execution started.")
        graph, in_degree = self._build_dependency_graph()
        name_to_step = {step.name: step for step in self.steps}
        completed = set()
        queue = deque([name for name in in_degree if in_degree[name] == 0])
        success_steps = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while queue:
                futures = {
                    executor.submit(_run_step_wrapper, name_to_step[step_name]): step_name
                    for step_name in list(queue)
                }
                queue.clear()

                for future in as_completed(futures):
                    step_name, result = future.result()

                    # 실시간 출력은 StepRunner 내부에서 로깅됨
                    # 여기선 결과 출력만 요약용으로 보여줌
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
                        self.logger.error(f"❌ Step '{step_name}' failed: {result.get('stderr')}")
                        self.failed_steps.append((step_name, result.get("stderr")))
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

    def _print_dag_structure(self):
        self.logger.info("📊 DAG Structure:")
        graph, _ = self._build_dependency_graph()
        visited = set()

        def dfs(node, depth=0):
            indent = "  " * depth
            self.logger.info(f"{indent}- {node}")
            visited.add(node)
            for child in graph.get(node, []):
                if child not in visited:
                    dfs(child, depth + 1)

        if self.selected_step:
            reverse_graph = defaultdict(list)
            for parent, children in graph.items():
                for child in children:
                    reverse_graph[child].append(parent)

            def get_ancestors(step):
                ancestors = set()
                stack = [step]
                while stack:
                    current = stack.pop()
                    for parent in reverse_graph.get(current, []):
                        if parent not in ancestors:
                            ancestors.add(parent)
                            stack.append(parent)
                return ancestors

            focus_steps = get_ancestors(self.selected_step) | {self.selected_step}

            def dfs_limited(node, depth=0):
                if node not in focus_steps or node in visited:
                    return
                indent = "  " * depth
                self.logger.info(f"{indent}- {node}")
                visited.add(node)
                for child in graph.get(node, []):
                    dfs_limited(child, depth + 1)

            dfs_limited(self.selected_step)

        else:
            roots = [step for step in self.get_step_names() if step not in {n for deps in graph.values() for n in deps}]
            for root in roots:
                dfs(root)

    def visualize_dag(self, output_file="dag_parallel.png"):
        import networkx as nx
        import pydot
        from networkx.drawing.nx_pydot import to_pydot

        graph, _ = self._build_dependency_graph()
        G = nx.DiGraph()

        for step in self.get_step_names():
            G.add_node(step)
        for parent, children in graph.items():
            for child in children:
                G.add_edge(parent, child)

        failed_set = {s[0] for s in self.failed_steps}
        skipped_set = set(self.skipped_steps)
        success_set = set(self.get_step_names()) - failed_set - skipped_set

        in_degree = defaultdict(int)
        for u, v in G.edges():
            in_degree[v] += 1

        queue = deque([node for node in G.nodes() if in_degree[node] == 0])
        level = {node: 0 for node in queue}

        while queue:
            current = queue.popleft()
            for neighbor in G.successors(current):
                if neighbor not in level:
                    level[neighbor] = level[current] + 1
                else:
                    level[neighbor] = max(level[neighbor], level[current] + 1)
                queue.append(neighbor)

        pydot_graph = to_pydot(G)
        pydot_graph.set("rankdir", "LR")
        pydot_graph.set("splines", "ortho")

        for node in pydot_graph.get_nodes():
            name = node.get_name().strip('"')
            if name in failed_set:
                node.set_style("filled")
                node.set_fillcolor("red")
            elif name in skipped_set:
                node.set_style("filled")
                node.set_fillcolor("orange")
            elif name in success_set:
                node.set_style("filled")
                node.set_fillcolor("green")
            else:
                node.set_style("filled")
                node.set_fillcolor("lightgray")

        level_dict = defaultdict(list)
        for node, lvl in level.items():
            level_dict[lvl].append(node)

        for same_level_nodes in level_dict.values():
            subgraph = pydot.Subgraph(rank='same')
            for node_name in same_level_nodes:
                subgraph.add_node(pydot.Node(node_name))
            pydot_graph.add_subgraph(subgraph)

        pydot_graph.write_png(output_file)
        self.logger.info(f"📊 DAG visualization saved to '{output_file}'")
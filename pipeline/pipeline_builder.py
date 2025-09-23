# pipeline/pipeline_builder.py
import os
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline.step_runner import StepRunner
from pipeline.logger import setup_logger


def _run_step_wrapper(step: StepRunner):
    return step.name, step.run()


def _to_bool(value, default=False):
    """입력값을 안전하게 bool로 변환"""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "t", "yes", "y", "1", "on"}:
            return True
        if v in {"false", "f", "no", "n", "0", "off"}:
            return False
    return default


class PipelineBuilder:
    def __init__(self, config_loader, logger=None, target_date=None, selected_step=None):
        self.config_loader = config_loader

        self.env = self.config_loader.config_data.get("global")['env']

        self.logger = logger or setup_logger(
            "pipeline", self.config_loader.get_log_file(), self.config_loader.get_log_level()
        )
        self.target_date = target_date
        self.selected_step = selected_step

        # ✅ 입력/설정 안정성: force 안전 변환
        options = self.config_loader.config_data.get("options", {}) or {}
        self.global_force = _to_bool(options.get("force", False), default=False)

        # DAG 섹션 캐시 (depends_on None → [])
        raw_dag = self.config_loader.config_data.get("dag", {}) or {}
        self.dag_cfg = {}
        for name, info in raw_dag.items():
            info = dict(info or {})
            deps = info.get("depends_on")
            if deps is None:
                info["depends_on"] = []
            self.dag_cfg[name] = info

        self.steps = []
        self.failed_steps = []
        self.skipped_steps = []
        self._register_steps()

    def _register_steps(self):
        log_level = self.config_loader.get_log_level()

        for step_name, step_info in self.dag_cfg.items():
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
        """
        graph: parent -> [children]
        in_degree: child -> #parents
        reverse: child -> [parents]
        """
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        reverse = defaultdict(list)

        for step_name, step_info in self.dag_cfg.items():
            deps = step_info.get("depends_on", []) or []
            for dep in deps:
                graph[dep].append(step_name)
                reverse[step_name].append(dep)
                in_degree[step_name] += 1
            if step_name not in in_degree:
                in_degree[step_name] = 0

        return graph, in_degree, reverse

    def get_step_names(self):
        return [step.name for step in self.steps]

    def run_all(self):
        """순차 실행 (기존 동작 유지)"""
        self.logger.info("🚀 Pipeline execution started.")
        success_steps = []

        for step in self.steps:
            self.logger.info(f"▶️ Running step: {step.name}")
            result = step.run("subprocess")
            reason = result.get("error") or result.get("stderr") or "unknown error"

            if result.get("success"):
                success_steps.append(step.name)
            elif result.get("skipped"):
                self.logger.warning(f"⚠️ Step '{step.name}' was skipped.")
                self.skipped_steps.append(step.name)
            else:
                self.logger.error(f"❌ Step '{step.name}' failed: {reason}")
                self.failed_steps.append((step.name, reason))

        self._print_summary(success_steps)

    def run_step(self, step_name):
        step_dict = {s.name: s for s in self.steps}
        step = step_dict.get(step_name)
        if not step:
            self.logger.error(f"Step '{step_name}' not found in DAG.")
            return

        result = step.run("subprocess")
        reason = result.get("error") or result.get("stderr") or "unknown error"

        if result.get("success"):
            self.logger.info(f"✅ Step '{step_name}' completed successfully.")
        elif result.get("skipped"):
            self.logger.warning(f"⚠️ Step '{step_name}' was skipped by logic.")
            self.skipped_steps.append(step_name)
        else:
            self.logger.error(f"❌ Step '{step_name}' failed: {reason}")
            self.failed_steps.append((step_name, reason))

    def run_all_parallel(self, max_workers=4):
        """
        병렬 실행 + 의존성 제어.
        - 기본: 부모 성공이어야 자식 실행. 부모 실패/스킵 시 자식 스킵.
        - 전역/스텝 force 활성: 부모 실패/스킵이어도 자식 강제 실행.
        - force가 하나도 없으면, 최초 실패 시 전체 중단(기존 동작 유지).
        """
        self.logger.info("🚀 DAG parallel execution started.")
        graph, in_degree, reverse = self._build_dependency_graph()
        name_to_step = {step.name: step for step in self.steps}

        # ✅ 스텝별 강제 실행 플래그 (입력 안정 변환)
        step_force = {name: _to_bool(info.get("force", False), default=False) for name, info in self.dag_cfg.items()}
        force_any = self.global_force or any(step_force.values())

        # 상태 추적
        status = {}   # name -> "success" | "skipped" | "failed"
        completed = set()
        success_steps = []

        # in_degree==0 루트 노드 큐
        queue = deque([name for name, deg in in_degree.items() if deg == 0])

        def _format_parent_statuses(child):
            parents = reverse.get(child, [])
            parts = [f"{p}={status.get(p, 'pending')}" for p in parents]
            return ", ".join(parts) if parts else "(no-parents)"

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            # 초기 큐 제출
            while queue:
                step_name = queue.popleft()
                futures[executor.submit(_run_step_wrapper, name_to_step[step_name])] = step_name

            while futures:
                for future in as_completed(list(futures.keys())):
                    step_name = futures.pop(future)
                    try:
                        _name, result = future.result()
                    except Exception as e:
                        result = {"success": False, "stderr": str(e)}

                    reason = result.get("error") or result.get("stderr") or "unknown error"

                    if result.get("success"):
                        self.logger.info(f"✅ Step '{step_name}' completed.")
                        status[step_name] = "success"
                        success_steps.append(step_name)
                    elif result.get("skipped"):
                        self.logger.warning(f"⚠️ Step '{step_name}' was skipped.")
                        status[step_name] = "skipped"
                        self.skipped_steps.append(step_name)
                    else:
                        self.logger.error(f"❌ Step '{step_name}' failed: {reason}")
                        status[step_name] = "failed"
                        self.failed_steps.append((step_name, reason))

                    completed.add(step_name)

                    # 자식 후보 in_degree 갱신 및 평가
                    for child in graph.get(step_name, []):
                        in_degree[child] -= 1
                        if in_degree[child] == 0:
                            parents = reverse.get(child, [])
                            parent_statuses = [status.get(p) for p in parents]
                            any_parent_not_success = any(s != "success" for s in parent_statuses)

                            if any_parent_not_success and not (self.global_force or step_force.get(child, False)):
                                # 강제 아님 → 스킵 (부모 상태 함께 로깅)
                                self.logger.warning(
                                    f"⏭️  Skipping '{child}' due to non-success dependency "
                                    f"(parents: {_format_parent_statuses(child)}); force is off."
                                )
                                status[child] = "skipped"
                                self.skipped_steps.append(child)
                                completed.add(child)

                                # 손자들 in_degree 감소 전파
                                for gchild in graph.get(child, []):
                                    in_degree[gchild] -= 1
                                    if in_degree[gchild] == 0:
                                        queue.append(gchild)
                                continue

                            # 실행 가능 (정상 또는 강제)
                            if any_parent_not_success and (self.global_force or step_force.get(child, False)):
                                self.logger.warning(
                                    f"⚡ Forcing run of '{child}' "
                                    f"(parents: {_format_parent_statuses(child)})."
                                )
                            queue.append(child)

                # 큐에 쌓인 작업 제출
                while queue:
                    nxt = queue.popleft()
                    if nxt in completed or nxt in (futures.values()):
                        continue
                    futures[executor.submit(_run_step_wrapper, name_to_step[nxt])] = nxt

                # 기존 동작 유지: force 전혀 없고 실패 발생 시 중단
                if not force_any and any(v == "failed" for v in status.values()):
                    self.logger.error("🛑 Aborting DAG execution due to failure (force mode is off).")
                    break

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
        """DAG를 컴포넌트별 + 레벨(계층) 단위로 출력.
        예)
        - ddl
        - preprocess, train
        - inference
        """
        self.logger.info("📊 DAG Structure:")
        from collections import defaultdict, deque

        graph, in_degree, _ = self._build_dependency_graph()

        # --- 0) 무방향(weak) 컴포넌트 나누기 ---
        undirected = defaultdict(set)
        nodes = set(in_degree.keys()) | set(graph.keys())
        for u in nodes:
            for v in graph.get(u, []):
                undirected[u].add(v)
                undirected[v].add(u)
        # 고립 노드 보정
        for n in nodes:
            undirected[n]  # ensure key exists

        visited = set()
        components = []
        for n in sorted(nodes):  # 정렬해 출력 순서 안정화 (알파벳 기준)
            if n in visited:
                continue
            # BFS/DFS로 컴포넌트 수집
            comp = set()
            q = deque([n])
            visited.add(n)
            while q:
                cur = q.popleft()
                comp.add(cur)
                for nb in undirected[cur]:
                    if nb not in visited:
                        visited.add(nb)
                        q.append(nb)
            components.append(sorted(comp))  # 컴포넌트 내부도 정렬

        # --- 1) 각 컴포넌트별로 레벨 계산(Kahn + longest path) 후 출력 ---
        for comp in components:
            # 부분 in_degree/graph 재계산 (컴포넌트 한정)
            sub_graph = defaultdict(list)
            sub_in = {n: 0 for n in comp}
            for u in comp:
                for v in graph.get(u, []):
                    if v in sub_in:
                        sub_graph[u].append(v)
                        sub_in[v] += 1

            # 레벨 계산
            level = {}
            q = deque([n for n, d in sub_in.items() if d == 0])
            for n in q:
                level[n] = 0
            indeg = dict(sub_in)

            while q:
                u = q.popleft()
                for v in sub_graph.get(u, []):
                    level[v] = max(level.get(v, 0), level[u] + 1)
                    indeg[v] -= 1
                    if indeg[v] == 0:
                        q.append(v)

            if not level:  # 이론상 없을 수 없지만 방어
                self.logger.info("- (empty)")
                continue

            by_level = defaultdict(list)
            for n, lv in level.items():
                by_level[lv].append(n)

            for lv in range(0, max(by_level.keys()) + 1):
                names = ", ".join(sorted(by_level[lv]))
                indent = "  " * lv
                self.logger.info(f"{indent}- {names}")


    def visualize_dag(self, output_file="dag_parallel.png"):
        import networkx as nx
        import pydot
        from networkx.drawing.nx_pydot import to_pydot

        graph, _, _ = self._build_dependency_graph()
        G = nx.DiGraph()

        for step in self.get_step_names():
            G.add_node(step)
        for parent, children in graph.items():
            for child in children:
                G.add_edge(parent, child)

        failed_set = {s[0] for s in self.failed_steps}
        skipped_set = set(self.skipped_steps)
        success_set = set(self.get_step_names()) - failed_set - skipped_set

        # 레벨 계산
        from collections import defaultdict as _dd
        in_degree = _dd(int)
        for u, v in G.edges():
            in_degree[v] += 1

        q = deque([node for node in G.nodes() if in_degree[node] == 0])
        level = {node: 0 for node in q}

        while q:
            current = q.popleft()
            for neighbor in G.successors(current):
                if neighbor not in level:
                    level[neighbor] = level[current] + 1
                else:
                    level[neighbor] = max(level[neighbor], level[current] + 1)
                q.append(neighbor)

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

        level_dict = _dd(list)
        for node, lvl in level.items():
            level_dict[lvl].append(node)

        for same_level_nodes in level_dict.values():
            subgraph = pydot.Subgraph(rank='same')
            for node_name in same_level_nodes:
                subgraph.add_node(pydot.Node(node_name))
            pydot_graph.add_subgraph(subgraph)

        pydot_graph.write_png(output_file)
        self.logger.info(f"📊 DAG visualization saved to '{output_file}'")

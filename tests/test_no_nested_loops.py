# Avoid nested loops for task creation; prefer dynamic task mapping
# This test ensures: Deep, nested task creation loops cause long parse times and large static DAGs.
# Guidance: https://docs.astronomer.io/learn/dynamic-tasks

import ast

from conftest import DAGS_DIR, call_name, dag_files

_OPERATOR_HINTS = {"Operator", "Sensor"}  # broad heuristic


def _looks_like_task_creation(call: ast.Call) -> bool:
    name = call_name(call)
    if not name:
        return False
    if any(hint in name for hint in _OPERATOR_HINTS):
        return True
    if name.endswith((".expand",)):
        return True
    return False


class _NestedLoopTaskVisitor(ast.NodeVisitor):
    def __init__(self):
        self.violations = []
        self.loop_depth = 0

    def visit_For(self, node: ast.For):
        self.loop_depth += 1
        self.generic_visit(node)
        self.loop_depth -= 1

    def visit_While(self, node: ast.While):
        self.loop_depth += 1
        self.generic_visit(node)
        self.loop_depth -= 1

    def visit_Call(self, node: ast.Call):
        if self.loop_depth >= 2 and _looks_like_task_creation(node):
            self.violations.append((node.lineno, node.col_offset, self.loop_depth, call_name(node)))
        self.generic_visit(node)


def _find_nested_loop_task_creation(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except Exception:
        return []
    v = _NestedLoopTaskVisitor()
    v.visit(tree)
    return v.violations


def test_no_nested_loops_for_task_creation():
    offenders = {}
    for py in dag_files():
        hits = _find_nested_loop_task_creation(py)
        if hits:
            offenders[py.name] = hits
    assert not offenders, (
        "Avoid nested loops when creating tasks; use dynamic task mapping instead. Offending locations:\n"
        + "\n".join(
            f"  {fname}: " + ", ".join(f"line {ln}:{col} depth={depth} call={name}" for ln, col, depth, name in hits)
            for fname, hits in offenders.items()
        )
    )

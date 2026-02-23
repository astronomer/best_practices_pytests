# Avoid code execution at import time
# This test ensures: Expensive or side-effectful code (print, sleeps, I/O, network) should not run at module import.
# Guidance: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code

import ast

from conftest import DAGS_DIR, call_name, dag_files

ALLOWED_TOPLEVEL = {
    "DAG",  # used in `with DAG(...):`
}


def _is_side_effect_call(name: str) -> bool:
    if name == "print":
        return True
    if name.endswith(("sleep",)):
        return True
    if name.startswith("os.system"):
        return True
    if name.startswith("subprocess."):
        return True
    return False


def _find_top_level_side_effects(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except Exception:
        return []
    violations = []
    for node in tree.body:  # only strict top-level statements
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            name = call_name(node.value)
            if name not in ALLOWED_TOPLEVEL and _is_side_effect_call(name):
                violations.append((node.lineno, node.col_offset, name))
        elif isinstance(node, ast.With):
            for item in node.items:
                if isinstance(item.context_expr, ast.Call):
                    name = call_name(item.context_expr)
                    if _is_side_effect_call(name):
                        violations.append((node.lineno, node.col_offset, f"with {name}"))
    return violations


def test_no_top_level_execution_side_effects():
    offenders = {}
    for py in dag_files():
        hits = _find_top_level_side_effects(py)
        if hits:
            offenders[py.name] = hits
    assert not offenders, (
        "Avoid side-effectful code at module import time (e.g., print/sleep/os.system/subprocess). Offending locations:\n"
        + "\n".join(
            f"  {fname}: " + ", ".join(f"line {ln}:{col} call={name}" for ln, col, name in hits)
            for fname, hits in offenders.items()
        )
    )

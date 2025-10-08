# Prevent random/time-based DAG IDs
# This test ensures: Random or time-based DAG IDs prevent consistent history and break backfills.
# Guidance: https://docs.astronomer.io/learn/dynamically-generating-dags

import ast
from pathlib import Path

DAGS_DIR = Path("dags")

_BAD_PATTERNS = {"uuid", "random", "datetime", "time"}

def _find_dynamic_dag_id(py_path: Path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except Exception:
        return []
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "dag_id":
            if isinstance(node.value, ast.JoinedStr) or isinstance(node.value, ast.BinOp):
                violations.append((node.lineno, node.col_offset, "dag_id built dynamically"))
            elif isinstance(node.value, ast.Call):
                name = getattr(node.value.func, "id", "") or getattr(node.value.func, "attr", "")
                if any(bad in name.lower() for bad in _BAD_PATTERNS):
                    violations.append((node.lineno, node.col_offset, f"dag_id from call {name}"))
            elif isinstance(node.value, ast.Attribute):
                attr = node.value.attr.lower()
                if any(bad in attr for bad in _BAD_PATTERNS):
                    violations.append((node.lineno, node.col_offset, f"dag_id from attribute {attr}"))
    return violations

def test_non_deterministic_dag_ids():
    assert DAGS_DIR.is_dir(), "dags directory not found"
    offenders = {}
    for py in sorted(DAGS_DIR.glob("*.py")):
        if py.name.startswith("._"):
            continue
        hits = _find_dynamic_dag_id(py)
        if hits:
            offenders[py.name] = hits
    assert not offenders, (
        "DAG IDs must be static and deterministic (avoid uuid/random/time-based). Offending locations:\n"
        + "\n".join(
            f"  {fname}: " + ", ".join(f"line {ln}:{col} {desc}" for ln, col, desc in hits)
            for fname, hits in offenders.items()
        )
    )
# Use DAG.get_parsing_context() for heavy parse-time logic
# This test ensures: Heavy operations should be guarded to avoid running during DAG parsing.
# Guidance: https://docs.astronomer.io/learn/dynamically-generating-dags

import ast
from pathlib import Path

DAGS_DIR = Path("dags")

# Heuristics: operations considered "heavy" or external at import time
_HEAVY_CALL_PREFIXES = (
    "requests.",  # network
    "httpx.",
    "boto3.",
    "google.",    # gcloud libs
    "open",       # file I/O at import time
)

def _call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        parts = []
        cur = node.func
        while isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value
        if isinstance(cur, ast.Name):
            parts.append(cur.id)
        return ".".join(reversed(parts))
    return ""

def _build_parent_map(tree):
    parent = {}
    for p in ast.walk(tree):
        for ch in ast.iter_child_nodes(p):
            parent[ch] = p
    return parent

def _call_from_node(call: ast.Call) -> str:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        parts = []
        cur = call.func
        while isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value
        if isinstance(cur, ast.Name):
            parts.append(cur.id)
        return ".".join(reversed(parts))
    return ""

def _guarded_by_parsing_context(node, parent_map) -> bool:
    cur = node
    while cur in parent_map:
        cur = parent_map[cur]
        if isinstance(cur, ast.If):
            # Accept DAG.get_parsing_context() or airflow.models.DAG.get_parsing_context()
            cond = cur.test
            if isinstance(cond, ast.Call):
                name = _call_from_node(cond)
                if name.endswith("DAG.get_parsing_context"):
                    return True
    return False

def _find_heavy_unGuarded_ops(py_path: Path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except Exception:
        return []
    parent_map = _build_parent_map(tree)
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _call_from_node(node)
            if any(name.startswith(pref) for pref in _HEAVY_CALL_PREFIXES):
                if not _guarded_by_parsing_context(node, parent_map):
                    violations.append((node.lineno, node.col_offset, name))
    return violations

def test_parsing_context_required():
    assert DAGS_DIR.is_dir(), "dags directory not found"
    offenders = {}
    for py in sorted(DAGS_DIR.glob("*.py")):
        if py.name.startswith("._"):
            continue
        hits = _find_heavy_unGuarded_ops(py)
        if hits:
            offenders[py.name] = hits
    assert not offenders, (
        "Heavy import-time ops must be guarded with `if DAG.get_parsing_context():` to skip during parse. Offenders:\n"
        + "\n".join(
            f"  {fname}: " + ", ".join(f"line {ln}:{col} call={name}" for ln, col, name in hits)
            for fname, hits in offenders.items()
        )
    )
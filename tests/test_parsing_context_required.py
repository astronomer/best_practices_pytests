# Use DAG.get_parsing_context() for heavy parse-time logic
# This test ensures: Heavy operations should be guarded to avoid running during DAG parsing.
# Guidance: https://docs.astronomer.io/learn/dynamically-generating-dags

import ast

from conftest import DAGS_DIR, call_name, dag_files

# Heuristics: operations considered "heavy" or external at import time
_HEAVY_CALL_PREFIXES = (
    "requests.",  # network
    "httpx.",
    "boto3.",
    "google.",    # gcloud libs
    "open",       # file I/O at import time
)


def _build_parent_map(tree):
    parent = {}
    for p in ast.walk(tree):
        for ch in ast.iter_child_nodes(p):
            parent[ch] = p
    return parent


def _guarded_by_parsing_context(node, parent_map) -> bool:
    cur = node
    while cur in parent_map:
        cur = parent_map[cur]
        if isinstance(cur, ast.If):
            cond = cur.test
            if isinstance(cond, ast.Call):
                name = call_name(cond)
                if name.endswith("DAG.get_parsing_context"):
                    return True
    return False


def _find_heavy_unguarded_ops(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except Exception:
        return []
    parent_map = _build_parent_map(tree)
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = call_name(node)
            if any(name.startswith(pref) for pref in _HEAVY_CALL_PREFIXES):
                if not _guarded_by_parsing_context(node, parent_map):
                    violations.append((node.lineno, node.col_offset, name))
    return violations


def test_parsing_context_required():
    offenders = {}
    for py in dag_files():
        hits = _find_heavy_unguarded_ops(py)
        if hits:
            offenders[py.name] = hits
    assert not offenders, (
        "Heavy import-time ops must be guarded with `if DAG.get_parsing_context():` to skip during parse. Offenders:\n"
        + "\n".join(
            f"  {fname}: " + ", ".join(f"line {ln}:{col} call={name}" for ln, col, name in hits)
            for fname, hits in offenders.items()
        )
    )

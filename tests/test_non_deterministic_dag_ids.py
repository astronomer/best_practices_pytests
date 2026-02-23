# Prevent random/time-based DAG IDs
# This test ensures: Random or time-based DAG IDs prevent consistent history and break backfills.
# Guidance: https://docs.astronomer.io/learn/dynamically-generating-dags

import ast

from conftest import DAGS_DIR, call_name, dag_files

# Calls that produce non-deterministic values
_NONDETERMINISTIC_CALLS = {
    "uuid.uuid1", "uuid.uuid3", "uuid.uuid4", "uuid.uuid5",
    "random.random", "random.randint", "random.choice", "random.uniform",
    "random.sample", "random.randrange",
    "datetime.now", "datetime.utcnow", "datetime.datetime.now", "datetime.datetime.utcnow",
    "time.time", "time.time_ns",
}

# Module prefixes — any call starting with these is suspect
_NONDETERMINISTIC_PREFIXES = ("uuid.", "random.")


def _contains_nondeterministic_call(node):
    """Walk an AST subtree and return True if it contains a non-deterministic call."""
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            name = call_name(child)
            if name in _NONDETERMINISTIC_CALLS:
                return True
            if any(name.startswith(p) for p in _NONDETERMINISTIC_PREFIXES):
                return True
    return False


def _find_dynamic_dag_id(py_path):
    text = py_path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text, filename=str(py_path))
    except Exception:
        return []

    # Build a map of variable name -> assignment value node for simple tracing
    var_assignments = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            tgt = node.targets[0]
            if isinstance(tgt, ast.Name):
                var_assignments[tgt.id] = node.value

    violations = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.keyword) and node.arg == "dag_id"):
            continue
        val = node.value

        # Direct non-deterministic call: dag_id=uuid.uuid4()
        if isinstance(val, ast.Call):
            name = call_name(val)
            if name in _NONDETERMINISTIC_CALLS or any(name.startswith(p) for p in _NONDETERMINISTIC_PREFIXES):
                violations.append((node.lineno, node.col_offset, f"dag_id from call {name}"))
            continue

        # f-string or concatenation — only flag if the subtree contains a bad call
        if isinstance(val, (ast.JoinedStr, ast.BinOp)):
            if _contains_nondeterministic_call(val):
                violations.append((node.lineno, node.col_offset, "dag_id built with non-deterministic expression"))
            continue

        # Variable reference: dag_id=some_var — trace one level
        if isinstance(val, ast.Name) and val.id in var_assignments:
            assigned = var_assignments[val.id]
            if _contains_nondeterministic_call(assigned):
                violations.append((node.lineno, node.col_offset, f"dag_id from variable '{val.id}' containing non-deterministic call"))
            continue

    return violations


def test_non_deterministic_dag_ids():
    offenders = {}
    for py in dag_files():
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

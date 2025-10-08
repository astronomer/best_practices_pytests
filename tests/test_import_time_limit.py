# Avoid slow imports due to heavy logic
# This test ensures: Heavy logic at DAG parse time can delay the scheduler; move work into tasks.
# Guidance: https://docs.astronomer.io/learn/dynamically-generating-dags

import os
import sys
import time
import importlib.util
import importlib
from pathlib import Path

DAGS_PATH = Path("dags")

# Default per-file budget in seconds; can be overridden for CI via env
DEFAULT_LIMIT_S = float(os.getenv("AIRFLOW_DAG_IMPORT_TIME_LIMIT_S", "1.0"))

def _timed_import(py_path: Path) -> float:
    """Import a module from a path with a unique name and return elapsed seconds."""
    name = f"_dagmod_{py_path.stem}_{int(time.time()*1000000)}"
    spec = importlib.util.spec_from_file_location(name, str(py_path))
    module = importlib.util.module_from_spec(spec)
    start = time.perf_counter()
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    elapsed = time.perf_counter() - start
    # Best-effort cleanup to avoid caching effects between files
    sys.modules.pop(name, None)
    return elapsed

def test_import_time_limit_per_file():
    assert DAGS_PATH.is_dir(), f"Missing dags directory: {DAGS_PATH}"
    slow = []
    for py in sorted(DAGS_PATH.glob("*.py")):
        # Skip dunder or hidden files defensively
        if py.name.startswith("._"):
            continue
        try:
            elapsed = _timed_import(py)
            if elapsed > DEFAULT_LIMIT_S:
                slow.append((py.name, elapsed))
        except Exception as e:
            # Let other tests catch functional errors; here we only track slowness
            slow.append((py.name, f"import error: {e}"))
    assert not slow, (
        "DAG import too slow or error-prone. Per-file limit is "
        f"{DEFAULT_LIMIT_S:.2f}s. Offenders:\n" +
        "\n".join(f"  {name}: {elapsed}" for name, elapsed in slow)
    )
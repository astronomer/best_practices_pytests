# Detect duplicate DAG IDs across files
# This test ensures: Duplicate DAG IDs can cause one DAG to overwrite another and disappear from the UI.
# References:
# - Airflow DAGs & IDs: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html

import os
import importlib.util
from collections import defaultdict
from airflow.models import DAG

DAGS_PATH = "dags"

def _load_dags_from(filepath: str):
    """Import a module from a file path and yield (dag_id, dag) pairs declared in it."""
    spec = importlib.util.spec_from_file_location(os.path.basename(filepath)[:-3], filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    for name, obj in vars(module).items():
        if isinstance(obj, DAG):
            yield obj.dag_id, obj

def test_duplicate_dag_ids_across_files():
    dag_id_to_files = defaultdict(list)

    for filename in sorted(os.listdir(DAGS_PATH)):
        if not filename.endswith(".py"):
            continue
        filepath = os.path.join(DAGS_PATH, filename)
        for dag_id, _dag in _load_dags_from(filepath):
            dag_id_to_files[dag_id].append(filepath)

    duplicates = {dag_id: files for dag_id, files in dag_id_to_files.items() if len(files) > 1}
    assert not duplicates, (
        "Duplicate DAG IDs detected (a later file will overwrite the earlier one in the UI):\n"
        + "\n".join(f"  {dag_id}: {', '.join(sorted(files))}" for dag_id, files in sorted(duplicates.items()))
    )
# Ensure concurrency control via pools and max_active_runs
# This test ensures: Without concurrency controls, dynamic DAGs can overwhelm resources and block other workflows.
# References:
# - Pools: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/pools.html#using-pools
# - Astronomer Docs: https://docs.astronomer.io/docs/learn/airflow-pools

import os

# Configurable thresholds via env vars
POOL_REQUIRED_TASK_THRESHOLD = int(os.getenv("AIRFLOW_POOL_REQUIRED_TASK_THRESHOLD", "5"))
MAX_ACTIVE_RUNS_UPPER = int(os.getenv("AIRFLOW_MAX_ACTIVE_RUNS_UPPER", "16"))


def test_missing_concurrency_controls(generated_dags):
    violations = []
    for dag_id, dag in generated_dags.items():
        # DAG-level check: require max_active_runs to be set to a reasonable number
        if dag.max_active_runs is None or dag.max_active_runs > MAX_ACTIVE_RUNS_UPPER:
            violations.append(f"{dag_id} missing or excessive max_active_runs (={dag.max_active_runs})")

        # Task-level check: require a pool for tasks if task count exceeds threshold
        if len(dag.tasks) > POOL_REQUIRED_TASK_THRESHOLD:
            for task in dag.tasks:
                if not getattr(task, "pool", None) or task.pool == "default_pool":
                    violations.append(f"{dag_id}.{task.task_id} has no explicit pool assignment")

    assert not violations, (
        "Concurrency controls missing:\n" + "\n".join(f"  - {msg}" for msg in violations)
    )

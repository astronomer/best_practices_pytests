# Limit excessive tasks per DAG
# This test ensures: Too many tasks in a single DAG can slow the scheduler and clutter the UI.
# Reference: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#scaling-dags

import os

# Default hard cap, adjustable via env var for CI or projects with special needs
TASK_COUNT_LIMIT = int(os.getenv("AIRFLOW_DAG_TASK_COUNT_LIMIT", "50"))

def test_task_count_limit(generated_dags):
    offenders = [(dag_id, len(dag.tasks)) for dag_id, dag in generated_dags.items() if len(dag.tasks) > TASK_COUNT_LIMIT]
    assert not offenders, (
        f"Too many tasks in a single DAG (limit={TASK_COUNT_LIMIT}). Offenders:\n"
        + "\n".join(f"  {dag_id}: {count} tasks" for dag_id, count in sorted(offenders, key=lambda x: x[1], reverse=True))
    )
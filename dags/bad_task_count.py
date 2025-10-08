"""
BAD PRACTICE: Excessive Task Count

Problem:
- Generates a very large number of tasks in a single DAG (e.g., thousands).

Why It’s Bad:
- Excessive tasks slow parsing, scheduling, and UI rendering.
- Hard to troubleshoot failures in huge DAGs.
- Best practice: Break workflows into multiple DAGs or use dynamic mapping.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="bad_task_count",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    # BAD: Too many tasks (exceeds max_tasks threshold in pytest.ini)
    for i in range(2000):  # Assuming max_tasks = 1000
        EmptyOperator(task_id=f"task_{i}")
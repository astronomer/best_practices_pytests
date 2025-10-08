"""
BAD PRACTICE: Missing Concurrency Controls

Problem:
- DAG does not specify max_active_runs, and tasks do not specify pools.

Why It’s Bad:
- Dynamic DAGs may create too many concurrent runs/tasks, overwhelming
  the scheduler and workers.
- Best practice: Set `max_active_runs` and use task-level pools to limit concurrency.

"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# BAD: No max_active_runs, no pools defined for tasks
with DAG(
    dag_id="bad_missing_concurrency",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    EmptyOperator(task_id="no_concurrency_control")
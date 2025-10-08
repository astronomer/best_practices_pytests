"""
BAD PRACTICE: Top-Level Execution

Problem:
- This DAG runs code (e.g., print statements, data loads, API calls) at import time.
- Airflow parses all DAG files frequently; top-level execution slows parsing and can fail unexpectedly.

Why It’s Bad:
- Slows down the scheduler and webserver (parsing DAGs repeatedly).
- Can break DAG parsing if external systems are unavailable.
- Best practice: Only define DAGs and tasks at import time; do heavy work inside tasks.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# BAD: Code executing at import time
print("This should not run at import time!")

with DAG(
    dag_id="bad_top_level_execution",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    t1 = EmptyOperator(task_id="task_one")
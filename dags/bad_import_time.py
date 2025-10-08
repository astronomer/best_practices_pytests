"""
BAD PRACTICE: Slow Import Time

Problem:
- Performs slow operations (e.g., time.sleep, API calls) at DAG import time.

Why It’s Bad:
- Airflow repeatedly parses DAGs; slow imports delay the scheduler and webserver.
- Can cause performance degradation across all DAGs.
- Heavy work should be done inside tasks, not at parse time.
"""

import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# BAD: Simulate heavy computation at import
time.sleep(2)

with DAG(
    dag_id="bad_import_time",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    EmptyOperator(task_id="slow_task")
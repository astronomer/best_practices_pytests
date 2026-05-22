"""
BAD PRACTICE: Dynamic start_date

Problem:
- DAG source uses a moving wall-clock value for `start_date`.

Why It’s Bad:
- Airflow scheduling expects `start_date` to be stable across parses.
- Dynamic start dates can make scheduling behavior confusing and non-deterministic.
- Best practice: use a fixed date, such as `datetime(2023, 1, 1)`.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="bad_dynamic_start_date",
    start_date=datetime.now(),
    schedule="@daily",
    catchup=False,
) as dag:
    EmptyOperator(task_id="dynamic_start_date_task")

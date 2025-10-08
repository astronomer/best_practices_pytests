"""
BAD PRACTICE: Duplicate DAG ID (File 1)

Problem:
- Two different DAG files define the same DAG ID.

Why It’s Bad:
- One DAG overwrites the other during parsing.
- Only one DAG appears in the UI; the other silently disappears.

"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="duplicate_dag_id",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    EmptyOperator(task_id="first_duplicate")
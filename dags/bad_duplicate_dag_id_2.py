"""
BAD PRACTICE: Duplicate DAG ID (File 2)

Problem:
- This DAG file defines the same DAG ID as another file.

Why It’s Bad:
- Leads to DAG ID collisions and unpredictable behavior in Airflow.

"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="duplicate_dag_id",  # Same as bad_duplicate_dag_id_1.py
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    EmptyOperator(task_id="second_duplicate")
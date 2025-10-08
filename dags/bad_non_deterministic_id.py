"""
BAD PRACTICE: Non-Deterministic DAG ID

Problem:
- DAG ID is generated using a random or time-based value.

Why It’s Bad:
- Each parse may create a new DAG ID, causing duplicate DAGs in the UI
  and breaking historical tracking or backfills.
- DAG IDs should be stable and predictable.

"""

import uuid
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# BAD: DAG ID changes on every import
dynamic_id = f"dynamic_dag_{uuid.uuid4()}"

with DAG(
    dag_id=dynamic_id,
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    EmptyOperator(task_id="unstable_task")
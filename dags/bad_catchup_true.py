"""
BAD PRACTICE: Catchup Enabled by Default

Problem:
- DAG defaults to catchup=True (Airflow default) and generates a backlog
  of runs on first deploy.

Why It’s Bad:
- Can trigger hundreds of unnecessary backfills.
- Best practice: Set catchup=False unless explicitly required.

"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="bad_catchup_true",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    # catchup not set → defaults to True (bad for most dynamic DAGs)
) as dag:
    EmptyOperator(task_id="catchup_enabled_task")
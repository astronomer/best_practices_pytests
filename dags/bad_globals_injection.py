"""
BAD PRACTICE: Globals Injection

Problem:
- Dynamically injecting DAGs into globals() to register them.

Why It’s Bad:
- Makes it unclear where DAGs are defined.
- Harder to debug and test (DAG IDs may appear/disappear dynamically).
- Airflow supports explicit DAG assignment (e.g., `dag = DAG(...)`), which is clearer and safer.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# BAD: Dynamically inject DAG into globals
dag = DAG(
    dag_id="bad_globals_injection",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
)

t1 = EmptyOperator(task_id="task_one", dag=dag)

globals()["bad_globals_injection"] = dag  # BAD PRACTICE
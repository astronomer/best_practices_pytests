"""
BAD PRACTICE: Direct Airflow Metadata DB Access

Problem:
- DAG code imports Airflow's internal session API and queries metadata tables directly.

Why It’s Bad:
- DAGs become coupled to Airflow's internal database schema and session lifecycle.
- Metadata DB access at parse time can slow or break DAG parsing.
- Best practice: use Airflow APIs, runtime context, XComs, Variables, Connections, or external application tables.
"""

from airflow import DAG
from airflow.models import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.settings import Session
from datetime import datetime

session = Session()
recent_runs = session.query(DagRun).limit(5).all()

with DAG(
    dag_id="bad_metadata_db_access",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    EmptyOperator(task_id="uses_metadata_db")

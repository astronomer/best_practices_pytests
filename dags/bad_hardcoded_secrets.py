"""
BAD PRACTICE: Hardcoded Secrets

Problem:
- DAG source contains literal credentials and credential-bearing connection strings.

Why It’s Bad:
- Secrets committed to DAG code can leak through git history, logs, code review, and backups.
- Best practice: store credentials in Airflow connections, variables, or a secrets backend.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

api_key = "abc123-production-api-key"
database_url = "postgresql://analytics:supersecret@warehouse.example.com:5432/events"

with DAG(
    dag_id="bad_hardcoded_secrets",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    EmptyOperator(task_id="uses_hardcoded_secret")

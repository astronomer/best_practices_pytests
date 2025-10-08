"""
BAD PRACTICE: Missing Parsing Context Guard

Problem:
- Performs heavy operations (e.g., reading files, API calls) at import time
  without using `DAG.get_parsing_context()` to prevent them during parsing.

Why It’s Bad:
- Airflow parses DAGs frequently; running heavy logic every parse slows
  down the scheduler and dag processor and can cause unnecessary load on external systems.
- Best practice: Wrap heavy logic inside a conditional:

    if DAG.get_parsing_context():
        # safe to skip heavy work at parse time

"""

import json
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# BAD: Simulate heavy work at import time (e.g., reading a large config file)
with open("/etc/hosts", "r") as f:  # using /etc/hosts just as an example
    data = json.load(f) if f.readable() else {}

with DAG(
    dag_id="bad_missing_parsing_context",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    EmptyOperator(task_id="task_after_heavy_import")
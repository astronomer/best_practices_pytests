"""
BAD PRACTICE: Nested Loops for Task Creation

Problem:
- Creates too many static tasks using nested loops (O(n^2) pattern).

Why It’s Bad:
- Exponential increase in tasks for large datasets/configurations.
- Slows DAG parsing and bloats the DAG unnecessarily.
- Better approach: Flatten loops or use dynamic task mapping in Airflow 2+.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="bad_nested_loops",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    # BAD: Large nested loops creating static tasks at parse time
    for i in range(10):
        for j in range(10):
            EmptyOperator(task_id=f"task_{i}_{j}")
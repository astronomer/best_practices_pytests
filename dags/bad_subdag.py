"""
BAD PRACTICE: SubDAGs

Problem:
- DAG source uses SubDagOperator to model nested workflows.

Why It’s Bad:
- SubDAGs complicate scheduling and can create confusing concurrency behavior.
- Best practice: use TaskGroups for UI organization or separate DAGs connected by datasets/assets.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from datetime import datetime


def build_subdag(parent_dag_id, child_dag_id):
    with DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",
        start_date=datetime(2023, 1, 1),
        schedule="@daily",
        catchup=False,
    ) as subdag:
        EmptyOperator(task_id="child_task")
    return subdag


with DAG(
    dag_id="bad_subdag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    SubDagOperator(
        task_id="subdag_task",
        subdag=build_subdag("bad_subdag", "subdag_task"),
    )

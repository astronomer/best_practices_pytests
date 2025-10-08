# Limit excessive tasks per DAG
# This test ensures: Too many tasks in a single DAG can slow the scheduler and clutter the UI.

def test_task_count_limit(generated_dags):
    raise AssertionError("Too many tasks in a single DAG can slow the scheduler and clutter the UI. See: Airflow Docs – https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#scaling-dags")

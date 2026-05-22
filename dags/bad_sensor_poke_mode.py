"""
BAD PRACTICE: Long-Running Sensor in Poke Mode

Problem:
- A sensor waits for a long time without `mode="reschedule"` or `deferrable=True`.

Why It’s Bad:
- Sensors in poke mode hold a worker slot while waiting.
- Long-running waits can reduce worker capacity and slow other DAGs.
- Best practice: use `mode="reschedule"` or `deferrable=True` when a sensor waits for minutes or hours.
"""

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

with DAG(
    dag_id="bad_sensor_poke_mode",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/upstream_ready",
        poke_interval=300,
        timeout=7200,
    )

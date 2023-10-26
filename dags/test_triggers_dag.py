from __future__ import annotations
import datetime
import pendulum
from airflow import DAG
from operators.WaitOneHourSensor import WaitOneHourSensor 

with DAG(
    dag_id="TestTriggers",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["Triggers"],
) as dag:

    WaitOneHourSensor = WaitOneHourSensor()

    WaitOneHourSensor

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="debug_pipeline",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["pipeline"],
) as dag:
    python_version = BashOperator(task_id="pythonversion", bash_command="python --version")
    python_version

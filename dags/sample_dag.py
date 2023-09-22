from __future__ import annotations
import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from operators.RunSynapsePipelineOperator import AzureSynapseRunPipelineOperator

with DAG(
    dag_id="AzureSynapseRunPipelineDag",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["pipeline"],
) as dag:
    
    show_apache_airflow = BashOperator(
        task_id="Show_apache_airflow_install",
        bash_command="echo pip show apache-airflow"
    )

    trigger_synapse_pipeline = AzureSynapseRunPipelineOperator(
        azure_synapse_conn_id="azure_synapse_connection",
        task_id="trigger_synapse_pipeline",
        pipeline_name="Pipeline 1",
        azure_synapse_workspace_dev_endpoint="https://ambika-synapse-workspace.dev.azuresynapse.net",
        deferrable=True 
    )

    show_apache_airflow >> trigger_synapse_pipeline

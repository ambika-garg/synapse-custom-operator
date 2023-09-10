from __future__ import annotations
import datetime
import pendulum
from airflow import DAG
from operators.RunSynapsePipelineOperator import AzureSynapseRunPipelineOperator

with DAG(
    dag_id="AzureSynapseRunPipelineDag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["pipeline"],
) as dag:

    trigger_synapse_pipeline = AzureSynapseRunPipelineOperator(
        azure_synapse_conn_id="azure_synapse_connection",
        task_id="trigger_synapse_pipeline",
        pipeline_name="Pipeline 1",
        azure_synapse_workspace_dev_endpoint="https://ambika-synapse-workspace.dev.azuresynapse.net"
    )

    trigger_synapse_pipeline

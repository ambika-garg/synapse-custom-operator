import datetime
from airflow import DAG
from operators.RunSynapsePipelineOperator import AzureSynapseRunPipelineOperator
# from operators.googleOperator import MyFirstOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="AzureSynapseRunPipelineDag",
    schedule=None,
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["pipeline"],
) as dag:
    trigger_synapse_pipeline = AzureSynapseRunPipelineOperator(
        azure_synapse_conn_id="azure_synapse_connection",
        task_id="trigger_synapse_pipeline",
        pipeline_name="Pipeline 1",
        azure_synapse_workspace_dev_endpoint="https://ambika-synapse-workspace.dev.azuresynapse.net",
        deferrable=False 
    )

    # trigger_google_operator = MyFirstOperator(
    #     task_id="Google_operator"
    # )

    trigger_synapse_pipeline
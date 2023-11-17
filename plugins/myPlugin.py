
from operators.RunSynapsePipelineOperator import AzureSynapsePipelineRunLink
from airflow.plugins_manager import AirflowPlugin

# Defining the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "my_namespace"
    operator_extra_links = [
        AzureSynapsePipelineRunLink(),
    ]
    operators = ["RunSynapsePipelineOperator.AzureSynapseRunPipelineOperator"]
    hooks = ["azureSynapseHook.AzureSynapsePipelineHook"]
    connection_types = ["azure_synapse_pipeline"]

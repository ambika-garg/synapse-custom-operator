from airflow.plugins_manager import AirflowPlugin
from hooks.azureSynapseHook import AzureSynapsePipelineHook
from operators.RunSynapsePipelineOperator import AzureSynapsePipelineRunLink, AzureSynapseRunPipelineOperator


# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "test_plugin"
    hooks = [AzureSynapsePipelineHook]
    operator_extra_links = [
        AzureSynapsePipelineRunLink(),
     ]
    extra_links = [
        AzureSynapsePipelineRunLink(),
    ]
    operators = [AzureSynapseRunPipelineOperator]
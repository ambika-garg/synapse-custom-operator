from airflow.models import BaseOperator
import requests
from airflow.configuration import conf
from functools import cached_property
from hooks.azureSynapseHook import AzureSynapseHook
# , AzureSynapseSparkBatchRunStatus
from typing import TYPE_CHECKING, Any, Optional, Sequence, Dict

class SynapseRunPipelineOperator(BaseOperator):
    """
    Executes a Synapse Pipeline.

    :param workspace_name: The name of the Azure Synapse workspace.
    :param pipeline_name: The name of the pipeline to execute.
    :param azure_synapse_conn_id: The Airflow connection ID for Azure Synapse.
    :param spark_pool: The name of the Spark pool (optional).

    """

    def __init__(
        self,
        pipeline_name: str,
        azure_synapse_conn_id: str,
        azure_synapse_workspace_dev_endpoint: str,
        wait_for_termination: bool = True,
        reference_pipeline_run_id: Optional[str] = None,
        is_recovery: Optional[bool] = None,
        start_activity_name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        *args, **kwargs
    ) -> None:
        self.azure_synapse_conn_id = azure_synapse_conn_id
        self.pipeline_name = pipeline_name
        self.azure_synapse_workspace_dev_endpoint = azure_synapse_workspace_dev_endpoint
        self.wait_for_termination = wait_for_termination
        self.reference_pipeline_run_id = reference_pipeline_run_id
        self.is_recovery = is_recovery
        self.start_activity_name = start_activity_name
        self.parameters = parameters
        super().__init__(*args, **kwargs)

    @cached_property
    def hook(self):
        """Create and return an AzureSynapseHook (cached)."""
        return AzureSynapseHook(
            azure_synapse_conn_id=self.azure_synapse_conn_id,
            azure_synapse_workspace_dev_endpoint=self.azure_synapse_workspace_dev_endpoint
        )

    def execute(self, context) -> None:
        self.log.info("Executing the %s pipeline.", self.pipeline_name)
        response = self.hook.run_pipeline(
            pipeline_name=self.pipeline_name,
            reference_pipeline_run_id=self.reference_pipeline_run_id,
            is_recovery=self.is_recovery,
            start_activity_name=self.start_activity_name,
            parameters=self.parameters,
        )
        self.run_id = vars(response)["run_id"]
        # Push the ``run_id`` value to XCom regardless of what happens during execution. This allows for
        # retrieval the executed pipeline's ``run_id`` for downstream tasks especially if performing an
        # asynchronous wait.
        context["ti"].xcom_push(key="run_id", value=self.run_id)

        if self.wait_for_termination:
            if self.deferrable is False:
                self.log.info("Waiting for pipeline run %s to terminate.", self.run_id)

                response = self.hook.wait_for_pipeline_run_status(
                    run_id=self.run_id,
                    # expected_statuses=AzureSynapseSparkBatchRunStatus.SUCCESS,
                    check_interval=self.check_interval,
                    timeout=self.timeout,
                    resource_group_name=self.resource_group_name,
                    factory_name=self.factory_name,
                )

                self.log.info("Pipeline status", response)
                # if self.hook.wait_for_pipeline_run_status(
                #     run_id=self.run_id,
                #     expected_statuses=AzureSynapseSparkBatchRunStatus.SUCCESS,
                #     check_interval=self.check_interval,
                #     timeout=self.timeout,
                #     resource_group_name=self.resource_group_name,
                #     factory_name=self.factory_name,
                # ):
                #     self.log.info("Pipeline run %s has completed successfully.", self.run_id)




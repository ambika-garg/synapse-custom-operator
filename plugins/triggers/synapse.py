import time
import asyncio
from azure.core.exceptions import ServiceRequestError
from airflow.triggers.base import BaseTrigger, TriggerEvent
from typing import Any, AsyncIterator, Dict, Tuple
from hooks.azureSynapseHook import *
# from hooks.azureSynapseHook import AzureSynapseAsyncHook, AzureSynapsePipelineRunStatus

class AzureSynapseTrigger(BaseTrigger):
    # TODO: Add documentation.

    def __init__(
        self,
        run_id: str,
        azure_synapse_conn_id: str,
        azure_synapse_workspace_dev_endpoint: str,
        end_time: float,
        wait_for_termination: bool = True,
        check_interval: int = 60,
    ):
        super().__init__()
        self.azure_synapse_conn_id = azure_synapse_conn_id
        self.azure_synapse_workspace_dev_endpoint = azure_synapse_workspace_dev_endpoint
        self.check_interval = check_interval
        self.run_id = run_id
        self.wait_for_termination = wait_for_termination
        self.end_time = end_time

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes AzureSynapseTrigger arguments and classpath."""

        return (
            "triggers.synapse.AzureSynapseTrigger",
            {
                "azure_synapse_conn_id": self.azure_synapse_conn_id,
                "check_interval": self.check_interval,
                "run_id": self.run_id,
                "wait_for_termination": self.wait_for_termination,
                "end_time": self.end_time,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to Azure Synapse, polls for the pipeline run status"""

        self.log.info("In the trigger run")

        # hook = AzureSynapseAsyncHook(
        #     azure_synapse_workspace_dev_endpoint=self.azure_synapse_workspace_dev_endpoint,
        #     azure_synapse_conn_id=self.azure_synapse_conn_id
        # )

        # try:
        #     if self.wait_for_termination:
        #         self.log.info("End time: %s, Current time: %s",
        #                       self.end_time, time.time())

        #         while self.end_time > time.time():
        #             try:
        #                 pipeline_status = await hook.get_azure_pipeline_run_status(
        #                     run_id=self.run_id
        #                 )

        #                 if pipeline_status in AzureSynapsePipelineRunStatus.FAILURE_STATES:
        #                     yield TriggerEvent(
        #                         {
        #                             "status": "error",
        #                             "message": f"The pipeline run {self.run_id} has {pipeline_status}.",
        #                             "run_id": self.run_id,
        #                         }
        #                     )
        #                     return

        #                 elif pipeline_status == AzureSynapsePipelineRunStatus.SUCCEEDED:
        #                     yield TriggerEvent(
        #                         {
        #                             "status": "success",
        #                             "message": f"The pipeline run {self.run_id} has {pipeline_status}.",
        #                             "run_id": self.run_id,
        #                         }
        #                     )
        #                     return
        #                 self.log.info(
        #                     "Sleeping for %s. The pipeline state is %s.", self.check_interval, pipeline_status
        #                 )
        #                 await asyncio.sleep(self.check_interval)
        #             except ServiceRequestError:
        #                 # conn might expire during long running pipeline.
        #                 # If expcetion is caught, it tries to refresh connection once.
        #                 # If it still doesn't fix the issue,
        #                 # than the execute_after_token_refresh would still be False
        #                 # and an exception will be raised
        #                 if executed_after_token_refresh:
        #                     await hook.refresh_conn()
        #                     executed_after_token_refresh = False
        #                 else:
        #                     raise
        #         yield TriggerEvent(
        #             {
        #                 "status": "error",
        #                 "message": f"Timeout: The pipeline run {self.run_id} has {pipeline_status}.",
        #                 "run_id": self.run_id,
        #             }
        #         )
        #     else:
        #         yield TriggerEvent(
        #             {
        #                 "status": "success",
        #                 "message": f"The pipeline run {self.run_id} has {pipeline_status} status.",
        #                 "run_id": self.run_id,
        #             }
        #         )
        # except Exception as e:
        #     if self.run_id:
        #         try:
        #             await hook.cancel_pipeline_run(
        #                 run_id=self.run_id
        #             )
        #             self.log.info(
        #                 "Unexpected error %s caught. Cancel pipeline run %s", e, self.run_id)
        #         except Exception as err:
        #             yield TriggerEvent({"status": "error", "message": str(err), "run_id": self.run_id})
        #     yield TriggerEvent({"status": "error", "message": str(e), "run_id": self.run_id})

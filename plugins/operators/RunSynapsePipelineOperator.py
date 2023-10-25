import time
import warnings
from airflow.models import BaseOperator
from airflow.configuration import conf
from functools import cached_property
from hooks.azureSynapseHook import (
    AzureSynapseHook,
    # AzureSynapsePipelineRunException,
)
from airflow.exceptions import AirflowException
from typing import Any, Optional, Dict, TYPE_CHECKING

from triggers.synapse import AzureSynapseTrigger
if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context
# TODO: Move this to hook.py only and import it here.


class AzureSynapsePipelineRunStatus:
    """Azure Synapse pipeline operation statuses."""

    QUEUED = "Queued"
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELING = "Canceling"
    CANCELLED = "Cancelled"
    TERMINAL_STATUSES = {CANCELLED, FAILED, SUCCEEDED}
    INTERMEDIATE_STATES = {QUEUED, IN_PROGRESS, CANCELING}
    FAILURE_STATES = {FAILED, CANCELLED}

# TODO: Move this to hook.py only and import it here.


class AzureSynapsePipelineRunException(AirflowException):
    """An exception that indicates a pipeline run failed to complete."""


class AzureSynapseRunPipelineOperator(BaseOperator):
    """
    Executes a Synapse Pipeline.

    :param pipeline_name: The name of the pipeline to execute.
    :param azure_synapse_conn_id: The Airflow connection ID for Azure Synapse.
    :param azure_synapse_workspace_dev_endpoint: The Azure Synapse workspace development endpoint.
    :param wait_for_termination: Flag to wait on a pipeline run's termination. // TODO: Complete it. 
    :param reference_pipeline_run_id: The pipeline run identifier. If this run ID is specified the parameters
        of the specified run will be used to create a new run.
    :param is_recovery: Recovery mode flag. If recovery mode is set to `True`, the specified referenced
        pipeline run and the new run will be grouped under the same ``groupId``.
    :param start_activity_name: In recovery mode, the rerun will start from this activity. If not specified,
        all activities will run.
    :param parameters: Parameters of the pipeline run. These parameters are referenced in a pipeline via
        ``@pipeline().parameters.parameterName`` and will be used only if the ``reference_pipeline_run_id`` is
        not specified.
    :param timeout: Time in seconds to wait for a pipeline to reach a terminal status for non-asynchronous
        waits. Used only if ``wait_for_termination`` is True.
    :param check_interval: Time in seconds to check on a pipeline run's status for non-asynchronous waits.
        Used only if ``wait_for_termination`` is True.
    :param deferrable: Run operator in deferrable mode.

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
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False),
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
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable
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
                self.log.info(
                    "Waiting for pipeline run %s to terminate.", self.run_id)

                if self.hook.wait_for_pipeline_run_status(
                    run_id=self.run_id,
                    expected_statuses=AzureSynapsePipelineRunStatus.SUCCEEDED,
                    check_interval=self.check_interval,
                    timeout=self.timeout,
                ):
                    self.log.info(
                        "Pipeline run %s has completed successfully.", self.run_id)
                else:
                    raise AzureSynapsePipelineRunException(
                        f"Pipeline run {self.run_id} has failed or has been cancelled."
                    )
            else:
                self.log.info("Operator is in defferable mode")
                end_time = time.time() + self.timeout
                pipeline_run_status = self.hook.get_pipeline_run_status(
                    run_id=self.run_id
                )
                if pipeline_run_status not in AzureSynapsePipelineRunStatus.TERMINAL_STATUSES:
                    self.defer(
                        timeout=self.execution_timeout,
                        trigger=AzureSynapseTrigger(
                            azure_synapse_conn_id=self.azure_synapse_conn_id,
                            azure_synapse_workspace_dev_endpoint=self.azure_synapse_workspace_dev_endpoint,
                            run_id=self.run_id,
                            wait_for_termination=self.wait_for_termination,
                            check_interval=self.check_interval,
                            end_time=end_time,
                        ),
                        method_name="execute_complete",
                    )
                elif pipeline_run_status == AzureSynapsePipelineRunStatus.SUCCEEDED:
                    self.log.info(
                        "Pipeline run %s has completed successfully.", self.run_id)
                elif pipeline_run_status in AzureSynapsePipelineRunStatus.FAILURE_STATES:
                    raise AzureSynapsePipelineRunException(
                        f"Pipeline run {self.run_id} has failed or has been cancelled."
                    )
        else:
            if self.deferrable is True:
                warnings.warn(
                    "Argument `wait_for_termination` is False and `deferrable` is True , hence "
                    "`deferrable` parameter doesn't have any effect",
                )

    def execute_complete(self, event: Dict[str, str]) -> None:
        """
        Callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])

    def on_kill(self) -> None:
        if self.run_id:
            self.hook.cancel_pipeline_run(
                run_id=self.run_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
            )

            # Check to ensure the pipeline run was cancelled as expected.
            if self.hook.wait_for_pipeline_run_status(
                run_id=self.run_id,
                expected_statuses=AzureSynapsePipelineRunStatus.CANCELLED,
                check_interval=self.check_interval,
                timeout=self.timeout,
            ):
                self.log.info(
                    "Pipeline run %s has been cancelled successfully.", self.run_id)
            else:
                raise AzureSynapsePipelineRunException(
                    f"Pipeline run {self.run_id} was not cancelled.")

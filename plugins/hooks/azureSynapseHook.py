from __future__ import annotations
import time
from typing import TYPE_CHECKING, Any, Union
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from asgiref.sync import sync_to_async
from azure.synapse.artifacts.models import CreateRunResponse, PipelineRun
# from airflow.exceptions import AirflowTaskTimeout
from azure.core.exceptions import ServiceRequestError
from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.azure.utils import get_field
from azure.synapse.artifacts import ArtifactsClient
from azure.synapse.artifacts.aio import ArtifactsClient as AsyncArtifactsClient
from azure.identity.aio import (
    ClientSecretCredential as AsyncClientSecretCredential,
    DefaultAzureCredential as AsyncDefaultAzureCredential,
)
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]
AsyncCredentials = Union[AsyncClientSecretCredential, AsyncDefaultAzureCredential]

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

class AzureSynapsePipelineRunException(AirflowException):
    """An exception that indicates a pipeline run failed to complete."""

class AzureSynapseHook(BaseHook):
    """
    A hook to interact with Azure Synapse.

    :param conn_id: The :ref:`Azure Synapse connection id<howto/connection:synapse>`.
    :param spark_pool: The Apache Spark pool used to submit the job
    """

    conn_type: str = "azure_synapse"
    conn_name_attr: str = "azure_synapse_conn_id"
    default_conn_name: str = "azure_synapse_connection"
    hook_name: str = "Azure Synapse"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenantId": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
            "subscriptionId": StringField(lazy_gettext("Subscription ID"), widget=BS3TextFieldWidget()),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour."""

        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {"login": "Client ID", "password": "Secret", "host": "Synapse Workspace URL"},
        }

    def __init__(self, azure_synapse_workspace_dev_endpoint: str, azure_synapse_conn_id: str = default_conn_name):
        self._conn: ArtifactsClient = None
        self.conn_id = azure_synapse_conn_id
        self.azure_synapse_workspace_dev_endpoint = azure_synapse_workspace_dev_endpoint
        super().__init__()

    def _get_field(self, extras, name):
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=name,
        )

    def __get_fields_from_url(self, workspace_url):
        """
        Extracts the workspace_name, subscription_id and resource_group from the workspace url.

        :param workspace_url: The workspace url.
        """
        from urllib.parse import urlparse, unquote
        import re

        try:
            pattern = r'https://web\.azuresynapse\.net\?workspace=(.*)'
            match = re.search(pattern, workspace_url)

            if match:
                extracted_text = match.group(1)
                parsed_url = urlparse(extracted_text)
                path = unquote(parsed_url.path)
                path_segments = path.split('/')
                if len(path_segments) == 0:
                    raise

                return {
                    "workspace_name": path_segments[-1],
                    "subscription_id": path_segments[2],
                    "resource_group": path_segments[4]
                }
        except:
            self.log.error("No segment found in the workspace URL.")

    def run_pipeline(
        self,
        pipeline_name: str,
        **config: Any,
    ) -> CreateRunResponse:
        """
        Run a Synapse pipeline.

        :param pipeline_name: The pipeline name.
        :param config: Extra parameters for the Synapse Artifact Client.
        :return: The pipeline run Id.
        """

        return self.get_conn().pipeline.create_pipeline_run(pipeline_name, **config)

    def get_conn(self) -> ArtifactsClient:
        if self._conn is not None:
            return self._conn

        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        tenant = self._get_field(extras, "tenantId")

        credential: Credentials
        if conn.login is not None and conn.password is not None:
            if not tenant:
                raise ValueError(
                    "A Tenant ID is required when authenticating with Client ID and Secret.")

            credential = ClientSecretCredential(
                client_id=conn.login, client_secret=conn.password, tenant_id=tenant
            )
        else:
            credential = DefaultAzureCredential()
        self._conn = self._create_client(
            credential, self.azure_synapse_workspace_dev_endpoint)

        return self._conn

    @staticmethod
    def _create_client(credential: Credentials, endpoint: str):
        return ArtifactsClient(
            endpoint=endpoint,
            credential=credential
        )

    def get_pipeline_run(
        self,
        run_id: str,
        **config: Any,
    ) -> PipelineRun:
        """
        Get the pipeline run.

        :param run_id: The pipeline run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The pipeline run.
        """

        return self.get_conn().pipeline_run.get_pipeline_run(run_id=run_id)

    def get_pipeline_run_status(
        self,
        run_id: str,
    ) -> str:
        """
        Get a pipeline run's current status.

        :param run_id: The pipeline run identifier.

        :return: The status of the pipeline run.
        """
        self.log.info("Getting the status of run ID %s.", run_id)
        pipeline_run_status = self.get_pipeline_run(
            run_id=run_id,
        ).status
        self.log.info("Current status of pipeline run %s: %s",
                      run_id, pipeline_run_status)

        return pipeline_run_status

    def refresh_conn(self) -> ArtifactsClient:
        self._conn = None
        return self.get_conn()

    def wait_for_pipeline_run_status(
        self,
        run_id: str,
        expected_statuses: str | set[str],
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Waits for a pipeline run to match an expected status.

        :param run_id: The pipeline run identifier.
        :param expected_statuses: The desired status(es) to check against a pipeline run's current status.
        :param check_interval: Time in seconds to check on a pipeline run's status.
        :param timeout: Time in seconds to wait for a pipeline to reach a terminal status or the expected
            status.

        :return: Boolean indicating if the pipeline run has reached the ``expected_status``.
        """

        pipeline_run_status = self.get_pipeline_run_status(run_id=run_id)
        executed_after_token_refresh = True
        start_time = time.monotonic()

        while (
            pipeline_run_status not in AzureSynapsePipelineRunStatus.TERMINAL_STATUSES
            and pipeline_run_status not in expected_statuses
        ):
            if start_time + timeout < time.monotonic():
                raise AzureSynapsePipelineRunException(
                    f"Pipeline run {run_id} has not reached a terminal status after {timeout} seconds."
                )

            # Wait to check the status of the pipeline run based on the ``check_interval`` configured.
            time.sleep(check_interval)

            try:
                pipeline_run_status = self.get_pipeline_run_status(
                    run_id=run_id)
                executed_after_token_refresh = True
            except ServiceRequestError:
                if executed_after_token_refresh:
                    self.refresh_conn()
                else:
                    raise

        return pipeline_run_status in expected_statuses

    def cancel_run_pipeline(
        self,
        run_id: str
    ) -> None:
        """
        Cancel the pipeline run.

        :param run_id: The pipeline run identifier.
        """
         
        self.get_conn().pipeline_run.cancel_pipeline_run(run_id)

class AzureSynapseAsyncHook(AzureSynapseHook):
    """
    An Async hook that connects to Azure Synapse to perform pipeline operations.

    :param azure_synapse_conn_id:  The :ref:`Azure Synapse connection id<howto/connection:synapse>`.
    """

    default_conn_name: str = "azure_synapse_default"

    def __init__(
        self,
        azure_synapse_workspace_dev_endpoint: str,
        azure_synapse_conn_id: str = default_conn_name
    ):
        self.log.info("Enter Async hook")
        AzureSynapseHook.__init__(self, azure_synapse_workspace_dev_endpoint, azure_synapse_conn_id)
        self.log.info("Initialization complete for hook")
        self._async_conn: AsyncArtifactsClient = None
        self.conn_id = azure_synapse_conn_id

    async def get_async_conn(self) -> AsyncArtifactsClient:
        """Get async connection and connects to azure synapse"""

        if self._async_conn is not None:
            return self._async_conn

        conn = await sync_to_async(self.get_connection)(self.conn_id)
        extras = conn.extra_dejson
        tenant = get_field(extras, "tenantId")

        credential: AsyncCredentials
        if conn.login is not None and conn.password is not None:
            if not tenant:
                raise ValueError(
                    "A Tenant ID is required when authenticating with Client ID and Secret.")

            credential = AsyncClientSecretCredential(
                client_id=conn.login, client_secret=conn.password, tenant_id=tenant
            )
        else:
            credential = AsyncDefaultAzureCredential()

        self._async_conn = AsyncArtifactsClient(
            endpoint=self.azure_synapse_workspace_dev_endpoint,
            credential=credential
        )

        return self._async_conn

    async def refresh_conn(self) -> AsyncArtifactsClient:
        self._conn = None
        return await self.get_async_conn()

    async def get_azure_pipeline_run_status(
        self,
        run_id: str
    ) -> str:
        """
        Connect to Azure Synapse asynchronously and get the pipeline status by run_id.

        :param run_id: The pipeline run identifier.

        :returns: The pipeline status.
        """

        pipeline_run = await self.get_pipeline_run(
            run_id=run_id,
        )
        status: str = pipeline_run.status
        return status

    async def get_pipeline_run(
        self,
        run_id: str,
    ) -> PipelineRun:
        """
        Connect to Azure Data Factory asynchronously to get the pipeline run details by run id.

        :param run_id: The pipeline run identifier.

        """

        client = await self.get_async_conn()
        pipeline_run = await client.pipeline_run.get_pipeline_run(run_id=run_id)
        return pipeline_run

    async def cancel_pipeline_run(self, run_id) -> None:
        """
        Cancel a pipeline run by its run ID.

        :param run_id: The pipeline run ID.
        """
        client = await self.get_async_conn()
        await client.cancel_pipeline_run(run_id=run_id)

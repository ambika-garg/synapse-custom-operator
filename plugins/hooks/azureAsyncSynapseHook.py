from plugins.hooks.azureSynapseHook import AzureSynapseHook
from azure.synapse.artifacts.aio import ArtifactsClient as AsyncArtifactsClient
from asgiref.sync import sync_to_async
from typing import TYPE_CHECKING, Any, Union
from airflow.providers.microsoft.azure.utils import get_field
from azure.identity.aio import (
    ClientSecretCredential as AsyncClientSecretCredential,
    DefaultAzureCredential as AsyncDefaultAzureCredential,
)
from azure.synapse.artifacts.models import CreateRunResponse, PipelineRun


AsyncCredentials = Union[AsyncClientSecretCredential, AsyncDefaultAzureCredential]


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
        super().__init__(azure_synapse_workspace_dev_endpoint, azure_synapse_conn_id)
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

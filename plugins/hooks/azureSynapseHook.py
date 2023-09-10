from __future__ import annotations
import time
from typing import TYPE_CHECKING, Any, Union
from azure.identity import ClientSecretCredential, DefaultAzureCredential
import requests
# from airflow.exceptions import AirflowTaskTimeout
from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.azure.utils import get_field
from azure.synapse.artifacts import ArtifactsClient

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]


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

    def __init__(self, azure_synapse_conn_id: str = default_conn_name):
        super().__init__()
        self.conn_id = azure_synapse_conn_id

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

    def get_auth_token(self, tenant_id, client_id, client_secret, workspace_name):

        try:
            url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
            payload = {
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'client_credentials',
                'resource': 'https://dev.azuresynapse.net',
                'scope': f'workspaces/{workspace_name}/*.'
            }
            headers = {
                'accept': 'application/json',
            }

            response = requests.request(
                "POST", url, headers=headers, data=payload)
            return response.json()["access_token"]

        except requests.exceptions.RequestException as e:
            self.log.error(f"Failed to authenticate: {e}")

    def run_pipeline(
        self,
        pipeline_name: str
    ):
        """
        Run a Synapse pipeline.

        """

        # job = self.get_conn()
        # job = PipelineOperations.create_pipeline_run(pipeline_name)
        # return job
        # response = self.hook.get_pipeline()

        # if self._conn is not None:
        #     return self._conn

        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        tenant = self._get_field(extras, "tenantId")

        subscription_id = self._get_field(extras, "subscriptionId")
        if not subscription_id:
            raise ValueError("A Subscription ID is required to connect to Azure Synapse.")

        if conn.login is not None and conn.password is not None:
            if not tenant:
                raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")


        client = ArtifactsClient(endpoint="https://ambika-synapse-workspace.dev.azuresynapse.net", credential=ClientSecretCredential(
            client_id=conn.login, client_secret=conn.password, tenant_id=tenant
        ))
        run_operations = client.pipeline.create_pipeline_run(pipeline_name)
        self.log.info("run operations %s", run_operations)
        return run_operations

        # from urllib.parse import quote

        # try:
        #     conn = self.get_connection(self.conn_id)
        #     extras = conn.extra_dejson
        #     tenant_id = self._get_field(extras, "tenantId")

        #     fields = self.__get_fields_from_url(conn.host)
        #     workspace_name = fields["workspace_name"]
        #     subscription_id = fields["subscription_id"]
        #     resource_group = fields["resource_group"]

        #     api_url = f"https://{workspace_name}.dev.azuresynapse.net/pipelines/{pipeline_name}/createRun?api-version=2020-12-01"

        #     auth_token = self.get_auth_token(tenant_id, conn.login, conn.password, workspace_name)

        #     headers = {
        #         "Authorization": f"Bearer {auth_token}",
        #         "Content-Type": "application/json"
        #     }
        #     response = requests.post(api_url, headers=headers)

        #     workspace_encoded = quote(f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Synapse/workspaces/{workspace_name}", safe='')
        #     run_id = response.json().get("runId")
        #     pipeline_run_url = f"https://ms.web.azuresynapse.net/en/monitoring/pipelineruns/{run_id}?workspace={workspace_encoded}"
        #     return {
        #         "pipeline_run_url": pipeline_run_url
        #     }

        # except requests.exceptions.RequestException as e:
        #     self.log.error(f"Failed to trigger Synapse pipeline: {e}")
        #     raise

    # def get_conn(self) -> SynapseManagementClient:
    #     if self._conn is not None:
    #         return self._conn

    #     conn = self.get_connection(self.conn_id)
    #     extras = conn.extra_dejson
    #     tenant = self._get_field(extras, "tenantId")
    #     spark_pool = self.spark_pool
    #     livy_api_version = "2022-02-22-preview"

    #     subscription_id = self._get_field(extras, "subscriptionId")
    #     if not subscription_id:
    #         raise ValueError("A Subscription ID is required to connect to Azure Synapse.")

    #     credential: Credentials
    #     if conn.login is not None and conn.password is not None:
    #         if not tenant:
    #             raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

    #         credential = ClientSecretCredential(
    #             client_id=conn.login, client_secret=conn.password, tenant_id=tenant
    #         )
    #     else:
    #         credential = DefaultAzureCredential()

    #     self._conn = self._create_client(credential, conn.host, spark_pool, livy_api_version, subscription_id)

    #     return self._conn

    # @staticmethod
    # def _create_client(credential: Credentials, subscription_id: str):
    #     return SynapseManagementClient(
    #         credential=credential,
    #         subscription_id=subscription_id,
    #     )

    # def run_spark_job(
    #     self,
    #     payload: SparkBatchJobOptions,
    # ):
    #     """
    #     Run a job in an Apache Spark pool.

    #     :param payload: Livy compatible payload which represents the spark job that a user wants to submit.
    #     """
    #     job = self.get_conn().spark_batch.create_spark_batch_job(payload)
    #     self.job_id = job.id
    #     return job

    # def get_job_run_status(self):
    #     """Get the job run status."""
    #     job_run_status = self.get_conn().spark_batch.get_spark_batch_job(batch_id=self.job_id).state
    #     return job_run_status

    # def wait_for_job_run_status(
    #     self,
    #     job_id: int | None,
    #     expected_statuses: str | set[str],
    #     check_interval: int = 60,
    #     timeout: int = 60 * 60 * 24 * 7,
    # ) -> bool:
    #     """
    #     Waits for a job run to match an expected status.

    #     :param job_id: The job run identifier.
    #     :param expected_statuses: The desired status(es) to check against a job run's current status.
    #     :param check_interval: Time in seconds to check on a job run's status.
    #     :param timeout: Time in seconds to wait for a job to reach a terminal status or the expected
    #         status.

    #     """
    #     job_run_status = self.get_job_run_status()
    #     start_time = time.monotonic()

    #     while (
    #         job_run_status not in AzureSynapseSparkBatchRunStatus.TERMINAL_STATUSES
    #         and job_run_status not in expected_statuses
    #     ):
    #         # Check if the job-run duration has exceeded the ``timeout`` configured.
    #         if start_time + timeout < time.monotonic():
    #             raise AirflowTaskTimeout(
    #                 f"Job {job_id} has not reached a terminal status after {timeout} seconds."
    #             )

    #         # Wait to check the status of the job run based on the ``check_interval`` configured.
    #         self.log.info("Sleeping for %s seconds", check_interval)
    #         time.sleep(check_interval)

    #         job_run_status = self.get_job_run_status()
    #         self.log.info("Current spark job run status is %s", job_run_status)

    #     return job_run_status in expected_statuses

    # def cancel_job_run(
    #     self,
    #     job_id: int,
    # ) -> None:
    #     """
    #     Cancel the spark job run.

    #     :param job_id: The synapse spark job identifier.
    #     """
    #     self.get_conn().spark_batch.cancel_spark_batch_job(job_id)

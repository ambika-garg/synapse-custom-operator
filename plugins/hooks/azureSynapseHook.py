from __future__ import annotations
import time
from typing import TYPE_CHECKING, Any, Union
from azure.identity import ClientSecretCredential, DefaultAzureCredential
import requests
from azure.synapse.artifacts.models import CreateRunResponse
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
    ) -> CreateRunResponse:
        """
        Run a Synapse pipeline.

        :param pipeline_name: The pipeline name.
        """
        self.log.info("Azure Synapse workspace development url: %s", self.azure_synapse_workspace_dev_endpoint)
        self.log.info("Pipeline Name: %s", self.conn_id)
        
        return self.get_conn().pipeline.create_pipeline_run(pipeline_name)
      

    def get_conn(self) -> ArtifactsClient:
        if self._conn is not None:
            return self._conn

        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        tenant = self._get_field(extras, "tenantId")

        credential: Credentials
        if conn.login is not None and conn.password is not None:
            if not tenant:
                raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

            credential = ClientSecretCredential(
                client_id=conn.login, client_secret=conn.password, tenant_id=tenant
            )
        else:
            credential = DefaultAzureCredential()
        self._conn = self._create_client(credential, "https://ambika-synapse-workspace.dev.azuresynapse.net")

        return self._conn

    @staticmethod
    def _create_client(credential: Credentials, endpoint: str):
        return ArtifactsClient(
            endpoint=endpoint,
            credential=credential
        )

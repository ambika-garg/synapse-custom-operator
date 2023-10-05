from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.plugins_manager import AirflowPlugin
from airflow.models.taskinstancekey import TaskInstanceKey


class GoogleLink(BaseOperatorLink):
    name = "Google"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        return "https://www.google.com"


class MyFirstOperator(BaseOperator):

    operator_extra_links = (GoogleLink(),)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        self.log.info("Hello World!")


# Defining the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [
        GoogleLink(),
    ]
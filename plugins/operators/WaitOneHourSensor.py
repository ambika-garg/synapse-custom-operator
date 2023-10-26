import time
from datetime import timedelta
from typing import Any, Dict

from airflow.configuration import conf
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context


class WaitOneHourSensor(BaseSensorOperator):
    def __init__(
        self, **kwargs
    ) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        self.defer(
            trigger=TimeDeltaTrigger(timedelta(minutes=2)),
            method_name="execute_complete",
        )

    def execute_complete(
        self,
        context: Context,
        event: Dict[str, Any],
    ) -> None:
        # We have no more work to do here. Mark as complete.
        return
import logging
from typing import Optional, Dict, Any

from airflow.models.baseoperator import BaseOperator


from hooks.alert_hook import AlertHook
from hooks.worklog_hook import WorkLogHook, SeverityLevel


logger = logging.getLogger(__name__)


class SendAlertOperator(BaseOperator):
    """
    Sends an alert using AlertHook and optionally logs to a WorkLog.

    This operator constructs an alert payload based on the provided parameters
    and uses the AlertHook to send it to Alertmanager via a generic template.
    If a worklog_id is provided, it logs the alert action to the specified worklog.

    Requires a Jinja2 template named 'generic_alert.j2' in the AlertHook's
    template directory (default: /opt/airflow/templates). See operator docstring
    for expected template structure.

    :param name: The name of the alert (maps to 'alertname' label).
    :param severity: The severity of the alert (maps to 'severity' label).
    :param labels: Optional dictionary of additional labels for the alert.
    :param annotations: Optional dictionary of annotations for the alert.
    :param alert_conn_id: Optional Airflow connection ID for Alertmanager.
                          Defaults to AlertHook's default ('alertmanager').
    :param worklog_id: Optional ID of an existing worklog to add an entry to.
    :param worklog_conn_id: Optional Airflow connection ID for the WorkLog service.
                            Defaults to WorkLogHook's default ('worklog_default').
    """
    template_fields = ('name', 'severity', 'labels', 'annotations', 'worklog_id')
    ui_color = '#f9d770'  # Light yellow

    # Default template name expected by this operator
    DEFAULT_ALERT_TEMPLATE = "common_alert_tpl.j2"

    def __init__(
        self,
        *,
        name: str,
        severity: str,
        labels: Optional[Dict[str, Any]] = None,
        annotations: Optional[Dict[str, Any]] = None,
        alert_conn_id: Optional[str] = None,
        worklog_id: Optional[str] = None,
        worklog_conn_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.severity = severity
        self.labels = labels or {}
        self.annotations = annotations or {}
        self.alert_conn_id = alert_conn_id
        self.worklog_id = worklog_id
        self.worklog_conn_id = worklog_conn_id

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Executes the operator: sends the alert and optionally logs to worklog.
        """
        # Initialize AlertHook
        alert_hook = AlertHook(conn_id=self.alert_conn_id)

        # Construct the alert object for the template
        alert_object = {
            "labels": {
                "alertname": self.name,
                "severity": self.severity,
                **self.labels  # Merge additional labels
            },
            "annotations": self.annotations
            # AlertHook will handle timestamping automatically if needed by template
        }

        try:
            logger.info(f"Sending alert '{self.name}' with severity '{self.severity}'...")
            logger.debug(f"Alert object: {alert_object}")

            # Send the alert using the generic template
            alert_response = alert_hook.send_alerts(
                template_name=self.DEFAULT_ALERT_TEMPLATE,
                alert_objects=[alert_object]
            )
            logger.info(f"Alert sent successfully. Response: {alert_response}")

        except Exception as e:
            logger.error(f"Failed to send alert '{self.name}': {e}", exc_info=True)
            # Optionally re-raise the exception if the DAG should fail
            raise

        # Log to worklog if specified
        if self.worklog_id:
            logger.info(f"Logging alert event to worklog ID: {self.worklog_id}")
            try:
                # Initialize WorkLogHook with the specific worklog_id
                worklog_hook = WorkLogHook(conn_id=self.worklog_conn_id, worklog_id=self.worklog_id)

                log_message = f"Alert Sent: Name='{self.name}', Severity='{self.severity}'"
                if self.labels:
                    log_message += f", Labels={self.labels}"
                if self.annotations:
                    log_message += f", Annotations={self.annotations}"

                # Add an info entry to the worklog
                # The hook will check if the worklog exists and is open
                worklog_hook.info(message=log_message)
                logger.info(f"Successfully logged alert event to worklog {self.worklog_id}")

            except Exception as e:
                # Log the error but don't fail the task, as the primary goal (sending alert) succeeded.
                # If logging is critical, you might want to re-raise here.
                logger.warning(f"Failed to log alert event to worklog {self.worklog_id}: {e}", exc_info=True)
        else:
            logger.info("No worklog_id provided, skipping worklog entry.")

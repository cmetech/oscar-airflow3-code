import os
import logging
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from airflow.sdk.bases.hook import BaseHook
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

logger = logging.getLogger(__name__)


class AlertHook(BaseHook):
    """
    Generic hook for sending alerts via Alertmanager.

    The caller passes in a Jinja2 template name and a list of alert objects.
    Each alert object is rendered using the template. The rendered string must
    be valid JSON so that it can be aggregated into an alerts payload. The payload
    is then POSTed to Alertmanager.

    The connection details for Alertmanager are pulled from the Airflow connection
    specified by the environment variable ALERTMANAGER_CONNECTION_ID (default: "alertmanager").
    """

    def __init__(self, conn_id: Optional[str] = None, template_dir: str = "/opt/airflow/templates"):
        """
        :param conn_id: Optional Airflow connection id, otherwise uses ALERTMANAGER_CONNECTION_ID env variable.
        :param template_dir: Directory where alert templates are kept, default is "/opt/airflow/templates"
        """
        super().__init__()
        self.conn_id = conn_id or os.getenv("ALERTMANAGER_CONNECTION_ID", "alertmanager")
        self.template_dir = template_dir

        # Set up SSL verification
        self.verify_ssl = os.environ.get("SSL_VERIFY", "false").lower() == "true"

        # Set up Jinja2 environment with custom filters.
        self.env = Environment(loader=FileSystemLoader(self.template_dir), autoescape=True)
        self.env.filters["escape_string"] = self._escape_string

        logger.info(
            f"Initialized AlertHook with connection: {self.conn_id} and template directory: {self.template_dir}"
        )

    @staticmethod
    def _escape_string(s: Any) -> str:
        """
        Escape backslashes and quotes in a string.
        Converts non-string inputs to string.
        """
        s = s if isinstance(s, str) else str(s)
        return s.replace("\\", "\\\\").replace('"', '\\"')

    def send_alerts(self, template_name: str, alert_objects: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Render and send alerts to Alertmanager.

        :param template_name: The name of the Jinja2 template (e.g., "my_alert_template.j2")
        :param alert_objects: A list of alert objects. Each should be a dictionary.
        :return: The JSON response from Alertmanager.
        :raises: RuntimeError if rendering or JSON conversion fails.
        """
        # Ensure the template name ends with ".j2".
        if not template_name.endswith(".j2"):
            template_name = f"{template_name}.j2"

        # Retrieve Alertmanager connection details
        alert_conn = BaseHook.get_connection(self.conn_id)
        url = f"http://{alert_conn.host}:{alert_conn.port}/api/v2/alerts"
        logger.info(f"Using Alertmanager URL: {url}")

        try:
            template = self.env.get_template(template_name)
        except TemplateNotFound as tnfe:
            error_msg = f"Alert template '{template_name}' not found in {self.template_dir}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from tnfe

        alerts_payload = []
        for idx, alert_obj in enumerate(alert_objects):
            # Escape all top-level string values in the alert object but leave dictionaries intact.
            escaped_alert = {
                key: (value if isinstance(value, dict) else self._escape_string(value))
                for key, value in alert_obj.items()
            }

            # Check for timestamp fields in order of preference
            timestamp_field = None
            for field in ["event_timestamp", "alert_timestamp", "timestamp"]:
                if alert_obj.get(field):
                    timestamp_field = field
                    break

            if timestamp_field:
                try:
                    dt = datetime.fromisoformat(alert_obj[timestamp_field])

                    # If the datetime is naive (no timezone info), assume it's in UTC
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    # If the datetime has a timezone but it's not UTC, convert it to UTC
                    elif dt.tzinfo != timezone.utc:
                        dt = dt.astimezone(timezone.utc)

                    starts_at = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception as e:
                    logger.warning(
                        f"Failed to parse {timestamp_field} ({alert_obj[timestamp_field]}) for alert {idx}: {e}"
                    )
                    starts_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                starts_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            try:
                # Render the alert template.
                # The template is rendered with the alert object's data under "alert",
                # along with "starts_at" and a generator URL based on the Alertmanager connection.
                generator_url = f"http://{alert_conn.host}:{alert_conn.port}/alert_generator"
                rendered = template.render(alert=escaped_alert, starts_at=starts_at, generator_url=generator_url)
                logger.debug(f"Rendered template for alert {idx}: {rendered}")

                # Convert rendered text to JSON.
                try:
                    alert_json = json.loads(rendered)
                    alerts_payload.append(alert_json)
                except json.JSONDecodeError as je:
                    error_msg = f"JSON decode error for alert {idx}: {str(je)}"
                    logger.error(error_msg)
                    logger.error(f"Raw template output for alert {idx}: {rendered}")
                    logger.error(f"Original alert object: {alert_obj}")
                    raise RuntimeError(error_msg) from je

            except Exception as e:
                error_msg = f"Template rendering error for alert {idx}: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Alert object: {alert_obj}")
                raise RuntimeError(error_msg) from e

        # Send alerts to Alertmanager.
        logger.info(f"Sending {len(alerts_payload)} alert(s) to Alertmanager.")
        try:
            with httpx.Client(verify=self.verify_ssl) as client:
                response = client.post(
                    url, headers={"Content-Type": "application/json", "X-Internal-Service": "airflow"}, json=alerts_payload, timeout=30.0
                )
                response.raise_for_status()

                # Check if the response is empty.
                response_text = response.text.strip()
                if not response_text:
                    logger.info("Alertmanager returned an empty response.")
                    return {}
                else:
                    try:
                        response_json = response.json()
                    except json.JSONDecodeError as je:
                        error_msg = f"Failed to decode JSON response from Alertmanager: {str(je)}"
                        logger.error(error_msg)
                        raise RuntimeError(error_msg) from je
                    logger.info(f"Alerts sent successfully. Response: {response_json}")
                    return response_json
        except Exception as e:
            error_msg = f"Error sending alerts to Alertmanager: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

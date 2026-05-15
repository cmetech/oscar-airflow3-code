from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
import requests
import os
import httpx
import logging
import uuid
import re
from hooks.worklog_hook import WorkLogHook, SeverityLevel, WorkLogType
from jinja2 import Template


logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.today('UTC').add(days=-1),
    "retries": 1,
}

# Generate a unique task ID for this run
task_id = f"WORKLOG-TEST-{uuid.uuid4().hex[:8]}"

def create_worklog(**context):
    """Create a new worklog and add initial entries"""
    hook = WorkLogHook()

    buildmode = os.getenv('BUILD_MODE', 'production')

    conf_object = context["dag_run"].conf

    # Get parent worklof id
    worklog_id = conf_object.get("worklog_id")

    # Create metadata for the worklog
    metadata = [
        {"key": "task_id", "value": task_id},
        {"key": "parent_worklog_id", "value": worklog_id},
        {"key": "environment", "value": buildmode},
        {"key": "initiated_by", "value": "airflow"}
    ]

    # Create the worklog
    worklog = hook.create_worklog(
        name="Process alert and initiate autocaller worklog",
        description="Worklog for automation of autocaller for generated alerts",
        worklog_type=WorkLogType.DB,
        metadata=metadata
    )

    logger.info(f"Created worklog with ID: {worklog['id']}")

    # Add some initial entries
    hook.info("Starting the worklog test workflow for process_alert_initiate_autocaller")

    # Store the worklog ID in XCom for later tasks
    context['ti'].xcom_push(key='worklog_id', value=worklog['id'])

    return worklog['id']

def close_worklog(**context):
    """Close the worklog and add final entries"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    # Add a final entry
    hook.info(f"Workflow completed, closing worklog for worklog id: {worklog_id}")

    # Close the worklog
    closed_worklog = hook.close_worklog()

    logger.info(f"Closed worklog with ID: {closed_worklog['id']}")

    return closed_worklog['id']


def receive_alert_prepare_email_config(**context):
    """Receive alert info with ITSM ticket info from Netcool processor DAG"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    conf_object = context["dag_run"].conf

    # 'conf' contains the alert details
    raw_alert = conf_object.get("alert", {})

    hook.info("Starting receive alert for autocaller task")

    if not raw_alert:
        hook.error(f"No alert data received from parent DAG")
        raise ValueError("No alert data received from parent DAG")

    # Validate required fields
    required_fields = ["TICKETNUMBER", "SEVERITY", "NODE", "SUMMARY"]
    missing_fields = [field for field in required_fields if not raw_alert.get(field)]

    if missing_fields:
        hook.error(f"Alert missing required fields: {missing_fields}")
        raise ValueError(f"Alert missing required fields: {missing_fields}")

    # Receive alert info from Netcool processor DAG
    processed_alert = {
        "LASTOCCURRENCE": raw_alert.get("LASTOCCURRENCE") or raw_alert.get("lastoccurence", "Unknown"),
        "FIRSTOCCURRENCE": raw_alert.get("FIRSTOCCURRENCE") or raw_alert.get("firstoccurence", "Unknown"),
        "TICKETNUMBER": raw_alert.get("TICKETNUMBER") or raw_alert.get("ticketnumber", "").strip(),
        "CLASS": raw_alert.get("CLASS") or raw_alert.get("class"),
        "TICKETQUENAME": raw_alert.get("TICKETQUENAME") or raw_alert.get("ticket_queue_name"),
        "SEVERITY": raw_alert.get("SEVERITY") or raw_alert.get("severity"),
        "NODE": raw_alert.get("NODE") or raw_alert.get("node"),
        "DATACENTER": raw_alert.get("DATACENTER") or raw_alert.get("datacenter"),
        "DATACENTER_ENCLAVE": raw_alert.get("DATACENTER_ENCLAVE") or raw_alert.get("datacenter_enclave"),        
        "RBC_SOURCE": raw_alert.get("RBC_SOURCE") or raw_alert.get("rbc_source"),
        "REFERENCE": raw_alert.get("REFERENCE") or raw_alert.get("reference"),
        "HOSTNAME": raw_alert.get("HOSTNAME") or raw_alert.get("hostname"),
        "SUMMARY": raw_alert.get("SUMMARY") or raw_alert.get("summary")
    }

    hook.info(f"Processed Alert detail from netcool {processed_alert}")

    autocaller_template_ns = os.getenv('AUTO_CALLER_TEMPLATE_MAP_NS', 'magenta')
    autocaller_template_map = os.getenv('AUTO_CALLER_TEMPLATE_MAP_REMEDY', 'autocaller')
    autocaller_template_element_key = os.getenv('AUTO_CALLER_TEMPLATE_ELEMENT_KEY', 'email-template')

    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))

    # Fetch email template from OSCAR API
    OSCAR_API_URL = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/mapping-data/mapping/element"

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    params = {
        "mapping_name": autocaller_template_map,
        "mapping_namespace_name": autocaller_template_ns,
        "mapping_key": autocaller_template_element_key
    }

    hook.info(f"Fetching Email template from Oscar Map")

    try:
        with httpx.Client(verify=False) as client:  # Disable SSL verification if needed
            response = client.get(OSCAR_API_URL, headers=headers, params=params)
            response.raise_for_status()
            email_templates = response.json()

            email_template = email_templates[0].get("value", "") if email_templates else ""

            if not email_template:
                hook.error(f"No email template found.")
                raise ValueError("No email template found.")

    except httpx.HTTPStatusError as e:
        hook.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        raise RuntimeError(f"HTTP error: {e.response.status_code} - {e.response.text}")
    except httpx.RequestError as e:
        hook.error(f"Request error: {e}")
        raise RuntimeError(f"Request error: {e}")
    except ValueError as e:
        hook.error(f"Value error: verify email template map {e}")
        raise RuntimeError(f"Value error: verify email template map {e}")
    except Exception as e:
        hook.error(f"Value error: verify email template map {e}")
        raise RuntimeError(f"Unexpected error: {e}") from e

    hook.info(f"Email content with updated data {email_template}")

    # Store template and processed alert in XCom
    context['ti'].xcom_push(key="email_template", value=email_template)
    context['ti'].xcom_push(key="processed_alert", value=processed_alert)

def format_email_content(**context):

    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    """Format the email content by replacing placeholders with actual alert data."""

    email_template = context['ti'].xcom_pull(task_ids="receive_alert_prepare_email_config", key="email_template")
    processed_alert = context['ti'].xcom_pull(task_ids="receive_alert_prepare_email_config", key="processed_alert")

    hook.info(f"Processed Alert detail from netcool {processed_alert}")

    # Render email content using Jinja2 template rendering
    try:
        template = Template(email_template)
        formatted_email = template.render(processed_alert)
    except Exception as e:

        hook.error(f"Error rendering email content: {e}")
        raise RuntimeError(f"Jinja2 template rendering error: {e}")

    context['ti'].xcom_push(key="formatted_email", value=formatted_email)
    # Forward the ticket ID saperately to the next task
    context['ti'].xcom_push(key="ticket_number", value=processed_alert.get("TICKETNUMBER"))

def send_email_autocaller(**context):
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    formatted_email = context['ti'].xcom_pull(task_ids="format_email_content", key="formatted_email")
    ticket_number = context['ti'].xcom_pull(task_ids="format_email_content", key="ticket_number")

    if not formatted_email:
        hook.error("No formatted email content found")
        raise ValueError("No formatted email content found")

    if not ticket_number:
        hook.error("No ticket number found")
        raise ValueError("No ticket number found")

    hook.info(f"Preparing to send email for ticket: {ticket_number}")

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))

    notifier_name: str = os.environ.get("OSCAR_AUTOCALLER_NOTIFIER_NAME", "OSCAR-NOTIFIER-AUTOCALLER")
    notifier_send_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/notifiers/send"

    payload = {
        "name": notifier_name,
        "message": formatted_email,
        "subject": "Incident Notification " + ticket_number,
    }

    params = {
        "muted": "false",
        "notifier_type": "email"
    }

    try:
        with httpx.Client(verify=False) as client:
            response = client.post(notifier_send_url, headers=headers, params=params, json=payload)
            response.raise_for_status()
            task_info = response.json()
            hook.info(f"Email task started with task_history_id: {task_info.get('original_task_id')} and task name {task_info.get('name')}")

    except httpx.HTTPError as e:
        hook.error(f"Failed to send email: HTTP error {e.response.status_code} - {e.response.text}")
        logging.error(f"Failed to send email: HTTP error {e.response.status_code} - {e.response.text}")
        raise
    except httpx.RequestError as e:
        hook.error(f"Failed to send email: Request error - {str(e)}")
        logging.error(f"Failed to send email: Request error - {str(e)}")
        raise
    except Exception as e:
        hook.error(f"Unexpected error sending email: {str(e)}")
        logging.error(f"Unexpected error sending email: {str(e)}")
        raise RuntimeError(f"Unexpected error: {e}") from e

with DAG(
    dag_id="process_alert_initiate_autocaller",
    default_args=default_args,
    description="DAG to process alerts and trigger the auto-caller email notification",
    schedule=None,
    tags=['incident-management', 'autocaller', 'alert-processing'],
    catchup=False,
) as dag:

    create_worklog_task = PythonOperator(
        task_id="create_worklog",
        python_callable=create_worklog,
    )

    receive_alert_prepare_email_config_task = PythonOperator(
        task_id="receive_alert_prepare_email_config",
        python_callable=receive_alert_prepare_email_config,
    )

    format_email_content_task = PythonOperator(
        task_id="format_email_content",
        python_callable=format_email_content,
    )

    send_email_autocaller_task = PythonOperator(
        task_id="send_email_autocaller",
        python_callable=send_email_autocaller,
    )

    close_worklog_task = PythonOperator(
        task_id="close_worklog",
        python_callable=close_worklog,
    )

create_worklog_task >> receive_alert_prepare_email_config_task >> format_email_content_task >> send_email_autocaller_task >> close_worklog_task

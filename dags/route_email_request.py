from datetime import datetime, timedelta, timezone
from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
import logging
import uuid
import json
import base64
import os
import asyncio
from typing import Dict, Any, List, Optional

# Import hooks:
from hooks.worklog_hook import WorkLogHook  # type: ignore
from hooks.rule_hook import RuleHook  # type: ignore
from hooks.tasks_hook import TasksHook  # type: ignore
from hooks.cache_hook import CacheHook  # type: ignore

logger = logging.getLogger(__name__)

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def process_email_request(**context):
    """
    This DAG task processes an incoming email request solely for routing purposes.

    It performs the following steps:
      1. Retrieves the email data from dag_run configuration.
      2. Creates an initial worklog entry via WorkLogHook and logs basic email information.
      3. Uses RuleHook to evaluate the email based on its subject and other fields.
      4. Based on the returned applied_labels, it triggers either a downstream task or workflow.
         (The labels must include either:
              "task": <task_name>
           or
              "workflow": <dag_id>
         )
      5. The response from the triggered downstream process is logged to the worklog.
    """
    # Retrieve email data from dag_run.conf.
    dag_run = context.get("dag_run")
    email_data: Dict[str, Any] = dag_run.conf.get("email_data", {}) if dag_run and dag_run.conf else {}

    if not email_data:
        logger.error("No email data provided in the DAG run configuration")
        return None

    # Extract email information.
    subject = email_data.get("subject", "(No Subject)")
    from_addr = email_data.get("from", "(No From Address)")
    to_addr = email_data.get("to", "(No To Address)")
    date = email_data.get("date", str(datetime.now(timezone.utc)))
    body = email_data.get("body", "")
    attachments = email_data.get("attachments", [])

    # Create initial worklog.
    worklog_hook = WorkLogHook()
    initial_worklog = worklog_hook.create_worklog(
        name="Email Routing Worklog", description="Worklog for processing email routing"
    )
    worklog_id = initial_worklog["id"]

    # Log basic email info to the worklog.
    worklog_hook.info(f"Received email: {subject}")
    worklog_hook.info(f"From: {from_addr}")
    worklog_hook.info(f"To: {to_addr}")
    worklog_hook.info(f"Date: {date}")
    if body:
        worklog_hook.info("Email Body:")
        max_chunk_size = 1000
        if len(body) > max_chunk_size:
            chunks = [body[i : i + max_chunk_size] for i in range(0, len(body), max_chunk_size)]
            for i, chunk in enumerate(chunks):
                worklog_hook.info(f"Body Part {i+1}/{len(chunks)}: {chunk}")
        else:
            worklog_hook.info(body)
    else:
        worklog_hook.info("No email body content")

    # Log attachment information
    if attachments:
        worklog_hook.info(f"Processing {len(attachments)} attachments")
        for attachment in attachments:
            worklog_hook.info(
                f"Attachment: {attachment.get('filename', 'unnamed')} ({attachment.get('size', 0)} bytes)"
            )

    # Use RuleHook to evaluate the email for routing.
    rule_hook = RuleHook(namespace="email_processing")
    properties = {
        "subject": subject,
        "body": body,
        "from": from_addr,
        "to": to_addr,
        "attachments": attachments,  # Include attachments in properties for rule evaluation
    }
    logger.info(f"Evaluating email with properties: {properties}")
    evaluation_result = rule_hook.evaluate_rules("email_processing", properties)
    logger.info(f"Rule evaluation result: {evaluation_result}")

    if evaluation_result and evaluation_result.get("evaluation_status"):
        applied_labels = evaluation_result.get("applied_labels", {})
        logger.info(
            f"Email routing evaluation passed. Will process downstream trigger for worklog_id: {worklog_id} "
            f"with applied_labels: {applied_labels}"
        )
        # Initialize TasksHook to trigger downstream processing.
        tasks_hook = TasksHook()
        trigger_response = None

        # Check if the applied labels indicate a task or workflow.
        if "task" in applied_labels:
            task_name = applied_labels.get("task")
            user_data = {"worklog_id": worklog_id, "email_data": email_data, "applied_labels": applied_labels}
            logger.info(f"Triggering downstream task: {task_name} with user_data: {user_data}")
            trigger_response = tasks_hook.trigger_task(task_name, user_data=user_data)
        elif "workflow" in applied_labels:
            workflow_id = applied_labels.get("workflow")
            dag_run_payload = {
                "conf": {"worklog_id": worklog_id, "email_data": email_data, "applied_labels": applied_labels},
                "note": "Triggered from email routing",
            }
            logger.info(f"Triggering downstream workflow: {workflow_id} with payload: {dag_run_payload}")
            trigger_response = tasks_hook.trigger_workflow(workflow_id, workflow_payload=dag_run_payload)
        else:
            logger.warning("No valid task/workflow label found in applied_labels; cannot proceed with triggering.")
            worklog_hook.info("No valid downstream trigger label found. Stopping further processing.")
            return {
                "worklog_id": worklog_id,
                "routing_status": "stopped",
                "message": "No valid downstream process defined.",
            }

        # Log the downstream trigger response into the worklog.
        worklog_hook.info(f"Received downstream trigger response: {trigger_response}")
        return trigger_response

    else:
        # If rule evaluation is false, update and close the worklog.
        worklog_hook.info("No email processor defined for handling this email request.")
        closed_worklog = worklog_hook.close_worklog()
        logger.info(f"Closed worklog with ID: {closed_worklog['id']} due to no processor defined.")
        return {
            "worklog_id": closed_worklog["id"],
            "routing_status": "stopped",
            "message": "No email processor defined.",
        }


# Create the DAG
with DAG(
    "route_email_request",
    default_args=default_args,
    description="Process and route email requests based on rule evaluation",
    schedule=None,  # Only triggered manually or via API
    start_date=pendulum.today('UTC').add(days=-1),
    tags=["email", "request", "routing"],
) as dag:

    # Define the task
    process_email_task = PythonOperator(
        task_id="process_email_request",
        python_callable=process_email_request,
    )

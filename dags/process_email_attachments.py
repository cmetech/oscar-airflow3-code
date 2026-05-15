from datetime import datetime, timedelta, timezone
from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
import logging
import json
import base64
import os
import csv
import io
import asyncio
from typing import Dict, Any, List, Optional

# Import hooks
from hooks.worklog_hook import WorkLogHook  # type: ignore
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


async def process_email_attachments(**context):
    """
    This DAG task processes email attachments from route_email_request.py.

    It performs the following steps:
      1. Retrieves the email data from dag_run configuration.
      2. Creates or uses an existing worklog entry via WorkLogHook.
      3. Processes any attachments by:
         - Retrieving from Redis if storage_type is 'redis'
         - Decoding base64 content
         - For text files: Logging first 5 and last 5 lines
         - For non-text files: Logging size only
         - Logging total lines (for text files) and size
      4. Closes the worklog when complete.
    """
    # Retrieve email data from dag_run.conf
    dag_run = context.get("dag_run")
    email_data: Dict[str, Any] = dag_run.conf.get("email_data", {}) if dag_run and dag_run.conf else {}
    worklog_id = dag_run.conf.get("worklog_id") if dag_run and dag_run.conf else None

    if not email_data:
        logger.error("No email data provided in the DAG run configuration")
        return {"status": "error", "message": "No email data provided in the DAG run configuration"}

    # Extract email information
    subject = email_data.get("subject", "(No Subject)")
    from_addr = email_data.get("from", "(No From Address)")
    to_addr = email_data.get("to", "(No To Address)")
    date = email_data.get("date", str(datetime.now(timezone.utc)))
    body = email_data.get("body", "")
    attachments = email_data.get("attachments", [])

    # Create or use existing worklog
    worklog_hook = WorkLogHook()
    if worklog_id:
        worklog_hook.set_worklog_id(worklog_id)
        worklog_hook.info(f"Using existing worklog for email attachment processing: {subject}")
    else:
        worklog_hook.create_worklog(
            name=f"Email Attachment Processing: {subject}", description=f"Processing attachments from email: {subject}"
        )
        worklog_hook.info(f"Created new worklog for email attachment processing: {subject}")

    # Log basic email info to the worklog
    worklog_hook.info(f"Received email: {subject}")
    worklog_hook.info(f"From: {from_addr}")
    worklog_hook.info(f"To: {to_addr}")
    worklog_hook.info(f"Date: {date}")

    if body:
        worklog_hook.info("Email Body:")
        # Handle large bodies by chunking
        max_chunk_size = 1000
        if len(body) > max_chunk_size:
            chunks = [body[i : i + max_chunk_size] for i in range(0, len(body), max_chunk_size)]
            for i, chunk in enumerate(chunks):
                worklog_hook.info(f"Body Part {i+1}/{len(chunks)}: {chunk}")
        else:
            worklog_hook.info(body)
    else:
        worklog_hook.info("No email body content")

    # Process attachments
    if not attachments:
        worklog_hook.info("No attachments found in the email")
    else:
        worklog_hook.info(f"Found {len(attachments)} attachment(s)")

        async with CacheHook() as cache_hook:
            for i, attachment in enumerate(attachments):
                try:
                    filename = attachment.get("filename", f"attachment_{i+1}")
                    content_type = attachment.get("content_type", "application/octet-stream")
                    size = attachment.get("size", 0)
                    storage_type = attachment.get("storage_type", "inline")
                    status = attachment.get("status", "unknown")

                    worklog_hook.info(f"Processing attachment {i+1}/{len(attachments)}: {filename}")
                    worklog_hook.info(f"Content Type: {content_type}")
                    worklog_hook.info(f"Size: {size} bytes")
                    worklog_hook.info(f"Storage Type: {storage_type}")
                    worklog_hook.info(f"Status: {status}")

                    # Get content based on storage type
                    content = None
                    if storage_type == "redis" and "reference_id" in attachment:
                        try:
                            binary_data = await cache_hook.get_binary_content(attachment["reference_id"])
                            if binary_data and "data" in binary_data:
                                content = binary_data["data"]
                                # Get original content type from metadata if available
                                if "metadata" in binary_data and "original_content_type" in binary_data["metadata"]:
                                    content_type = binary_data["metadata"]["original_content_type"]
                                    worklog_hook.info(f"Retrieved original content type: {content_type}")
                                worklog_hook.info(
                                    f"Retrieved content from Redis for reference_id: {attachment['reference_id']}"
                                )
                            else:
                                worklog_hook.warning(
                                    f"Failed to retrieve content from Redis for reference_id: {attachment['reference_id']}"
                                )
                                continue
                        except Exception as e:
                            worklog_hook.error(f"Error retrieving from Redis: {str(e)}")
                            continue
                    else:
                        content = attachment.get("content", "")

                    if not content:
                        worklog_hook.warning(f"No content found in attachment: {filename}")
                        continue

                    # Decode the base64 content
                    try:
                        decoded_content = base64.b64decode(content)

                        # Check if content type is text-based
                        is_text = any(
                            content_type.startswith(prefix)
                            for prefix in ["text/", "application/json", "application/xml", "application/csv"]
                        )

                        if is_text:
                            # For text files, decode and split lines
                            text_content = decoded_content.decode("utf-8")
                            lines = text_content.splitlines()
                            total_lines = len(lines)

                            worklog_hook.info(f"Total lines in {filename}: {total_lines}")
                            worklog_hook.info(f"Total size: {size} bytes")

                            # Log first 5 lines
                            worklog_hook.info(f"First 5 lines of {filename}:")
                            for line in lines[:5]:
                                worklog_hook.info(line)

                            # Log last 5 lines if there are more than 10 lines
                            if total_lines > 10:
                                worklog_hook.info(f"Last 5 lines of {filename}:")
                                for line in lines[-5:]:
                                    worklog_hook.info(line)
                            elif total_lines > 5:
                                worklog_hook.info(f"Remaining lines of {filename}:")
                                for line in lines[5:]:
                                    worklog_hook.info(line)
                        else:
                            # For non-text files, just log the size
                            worklog_hook.info(f"Non-text file: {filename}")
                            worklog_hook.info(f"Content Type: {content_type}")
                            worklog_hook.info(f"Size: {size} bytes")

                    except Exception as decode_error:
                        error_msg = f"Error decoding attachment {filename}: {str(decode_error)}"
                        logger.error(error_msg)
                        worklog_hook.error(error_msg)

                except Exception as attachment_error:
                    error_msg = f"Error processing attachment {i+1}: {str(attachment_error)}"
                    logger.error(error_msg)
                    worklog_hook.error(error_msg)

    # Close the worklog
    closed_worklog = worklog_hook.close_worklog()
    logger.info(f"Closed worklog with ID: {closed_worklog['id']}")

    return {
        "worklog_id": closed_worklog["id"],
        "status": "success",
        "message": f"Processed {len(attachments)} attachment(s) from email: {subject}",
    }


# Create the DAG
with DAG(
    "process_email_attachments",
    default_args=default_args,
    description="Process and decode email attachments",
    schedule=None,  # Only triggered manually or via API
    start_date=pendulum.today('UTC').add(days=-1),
    tags=["email", "attachments", "processing"],
) as dag:

    # Define the task
    process_attachments_task = PythonOperator(
        task_id="process_email_attachments",
        python_callable=lambda **context: asyncio.run(process_email_attachments(**context)),
    )

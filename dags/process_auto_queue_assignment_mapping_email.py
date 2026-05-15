import logging
import base64
import json
import openpyxl
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List
from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from helpers.schema.email_processing import AttachmentStorageType  # type: ignore
from helpers.email_helper import retrieve_stored_attachment  # type: ignore
from hooks.worklog_hook import WorkLogHook, WorkLogType  # type: ignore
from hooks.rule_hook import RuleHook  # type: ignore
from hooks.alert_hook import AlertHook  # type: ignore
import re
from io import BytesIO
from hooks.cache_hook import CacheHook
import os
import uuid
import httpx
from hooks.access_management_db_hook import AccessManagementSQLHook
from jinja2 import Template
from hooks.mapping_hook import MappingHook
from hooks.notify_hook import NotifyHook
import asyncio

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Connection ID for DB (set via Airflow connection/environment)
DB_CONN_ID = os.getenv('OSCAR_DB_EXT_CONNECTION_ID', 'oscar_db_ext')
ENABLE_AM_SUPPORT_DATA_PROCESSING = os.getenv('ENABLE_AM_SUPPORT_DATA_PROCESSING', 'True').lower() == 'true'

# this is configurable and should be set to the namespace of the rules
AUTOQUEUE_RULE_NAMESPACE = os.getenv('OSCAR_AUTOQUEUE_RULE_NS', 'enable')


def parse_excel_to_rules(excel_content: bytes) -> List[Dict[str, Any]]:
    """
    Parse Excel content and convert to rule format for auto queue assignment.

    Excel columns mapping:
    - Application Queue           -> support_group
    - Group ID                    -> support_group_id
    - Support Organization        -> support_organization
    - Eng Name                    -> assigned, first_name, last_name
    - Assignee Login              -> assignee_login_id
    - Issue Summary Consists of   -> name, condition

    Special logic:
    - If summary is exactly 'Service', the rule will match any summary containing 'service'
      but will exclude all more specific rules whose summary also contain 'service'
      (by adding exclusion conditions).
    """
    try:
        df = pd.read_excel(BytesIO(excel_content))
        logger.info(f"Successfully read Excel with {len(df)} rows")
        rules = []
        # Collect all summaries containing 'service' (case-insensitive), except exactly 'Service'
        service_related = []
        for index, row in df.iterrows():
            summary = str(row.get('Issue Summary Consists of', '')).strip()
            if summary and 'service' in summary.lower() and summary.lower() != 'service':
                service_related.append(summary)
        for index, row in df.iterrows():
            try:
                summary = str(row.get('Issue Summary Consists of', '')).strip()
                if not summary:
                    logger.warning(f"Row {index + 1}: Empty summary, skipping")
                    continue
                rule_name = summary
                # Special handling for the general 'Service' rule
                if summary.lower() == 'service':
                    # Single condition: contains service but not 'service not'
                    condition = 'summary.as_lower =~ ".*service.*" and summary.as_lower !~ ".*service not.*"'
                else:
                    condition = f'summary.as_lower =~ ".*{summary.lower()}.*"'
                support_group = str(row.get('Application Queue', '')).strip()
                support_group_id = str(row.get('Group ID', '')).strip()
                support_organization = str(row.get('Support Organization', '')).strip()
                assigned = str(row.get('Eng Name', '')).strip()
                assignee_login_id = str(row.get('Assignee Login', '')).strip()
                # Split Eng Name into first_name and last_name
                first_name = ''
                last_name = ''
                if assigned:
                    parts = assigned.split()
                    if len(parts) > 1:
                        first_name = parts[0]
                        last_name = ' '.join(parts[1:])
                    else:
                        first_name = assigned
                        last_name = ''
                # Build add_labels dict
                add_labels = {}
                if support_group:
                    add_labels['support_group'] = support_group
                if support_group_id:
                    add_labels['support_group_id'] = support_group_id
                if support_organization:
                    add_labels['support_organization'] = support_organization
                if assigned:
                    add_labels['assigned'] = assigned
                if first_name:
                    add_labels['first_name'] = first_name
                if last_name:
                    add_labels['last_name'] = last_name
                if assignee_login_id:
                    add_labels['assignee_login_id'] = assignee_login_id
                rule_data = {
                    "name": rule_name,
                    "description": "",
                    "condition": condition,
                    "actions": {
                        "suppress": False,
                        "add_labels": add_labels
                    },
                    "namespace": AUTOQUEUE_RULE_NAMESPACE,
                    "status": "enabled"
                }
                rules.append(rule_data)
                logger.info(f"Row {index + 1}: Prepared rule '{rule_name}' from summary '{summary}'")
            except Exception as e:
                logger.error(f"Row {index + 1}: Error processing row: {e}")
                continue
        logger.info(f"Successfully parsed {len(rules)} rules from Excel")
        return rules
    except Exception as e:
        logger.error(f"Error parsing Excel content: {e}")
        raise


def upsert_rule(rule_hook: RuleHook, rule_data: Dict[str, Any], namespace: str = "default") -> bool:
    """
    Create or update a rule using the rule hook.
    Only updates non-empty fields to prevent overwriting with empty data.

    Args:
        rule_hook: The rule hook instance
        rule_data: Rule data dictionary
        namespace: Namespace for the rule

    Returns:
        True if successful, False otherwise
    """
    rule_name = rule_data.get("name")
    if not rule_name:
        logger.error("Rule data must contain a 'name' field")
        return False

    try:
        # Check if rule exists
        existing_rule = rule_hook.get_rule(rule_name, namespace)

        if existing_rule:
            # Update existing rule - only update non-empty fields
            update_data = {}
            for key, value in rule_data.items():
                if key != "name" and value is not None and value != "":
                    update_data[key] = value

            if update_data:
                rule_hook.update_rule(rule_name, update_data, namespace)
                logger.info(f"Successfully updated rule '{rule_name}' with non-empty fields")
            else:
                logger.info(f"No non-empty fields to update for rule '{rule_name}'")

            return True
        else:
            # Create new rule
            rule_hook.create_rule(rule_data, namespace)
            logger.info(f"Successfully created new rule '{rule_name}'")
            return True

    except Exception as e:
        logger.error(f"Error upserting rule '{rule_name}': {str(e)}")
        return False


def get_excel_bytes_from_attachment(attachment):
    storage_type = attachment.get("storage_type", "INLINE")
    filename = attachment.get("filename", "unknown")
    logger.info(f"Storage type for attachment {storage_type}")
    if storage_type.lower() == "inline":
        content = attachment.get("content")
        if not content:
            logger.error(f"No content found for inline attachment: {filename}")
            return None
        try:
            return base64.b64decode(content)
        except Exception as e:
            logger.error(f"Error decoding base64 content for {filename}: {e}")
            return None
    elif storage_type == "REDIS":
        reference_id = attachment.get("reference_id")
        if not reference_id:
            logger.error(f"No reference_id found for Redis attachment: {filename}")
            return None
        try:
            async def get_redis_content(ref_id):
                async with CacheHook() as cache_hook:
                    binary_data = await cache_hook.get_binary_content(ref_id)
                    return binary_data["data"] if binary_data and "data" in binary_data else None
            return asyncio.run(get_redis_content(reference_id))
        except Exception as e:
            logger.error(f"Error retrieving Redis content for {filename}: {e}")
            return None
    else:
        logger.error(f"Unsupported storage type for attachment: {filename}")
        return None


def process_auto_queue_assignment_mapping_email(**context):
    """
    Main function to process auto queue assignment mapping email
    Supports both INLINE and REDIS storage types for Excel attachments.
    """
    try:
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        request_data = conf.get("email_data", {})
        attachments = request_data.get("attachments", [])

        if not attachments:
            logger.error("No attachments found in request data")
            return

        rule_hook = RuleHook(namespace=AUTOQUEUE_RULE_NAMESPACE)
        success_count = 0
        error_count = 0
        processed_any = False

        for attachment in attachments:
            filename = attachment.get("filename", "unknown")
            content_type = attachment.get("content_type", "")
            logger.info(f"Processing attachment: {filename} ({content_type})")

            # Only process Excel files
            if not content_type.startswith("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") and not content_type.startswith("application/vnd.ms-excel"):
                logger.info(f"Skipping non-Excel attachment: {filename}")
                continue

            excel_bytes = get_excel_bytes_from_attachment(attachment)
            if not excel_bytes:
                logger.error(f"No Excel content found for attachment: {filename}")
                continue

            logger.info(f"Parsing Excel content from attachment: {filename}")
            rules = parse_excel_to_rules(excel_bytes)
            if not rules:
                logger.warning(f"No valid rules found in Excel file: {filename}")
                continue
            processed_any = True
            for rule_data in rules:
                try:
                    rule_name = rule_data.get('name')
                    logger.info(f"Processing rule: {rule_name}")
                    success = upsert_rule(rule_hook, rule_data, namespace=AUTOQUEUE_RULE_NAMESPACE)
                    if success:
                        success_count += 1
                        logger.info(f"Successfully processed rule: {rule_name}")
                    else:
                        error_count += 1
                        logger.error(f"Failed to process rule: {rule_name}")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing rule {rule_data.get('name', 'unknown')}: {e}")
        if not processed_any:
            logger.error("No Excel attachments were processed.")
        logger.info(f"Processing complete. Success: {success_count}, Errors: {error_count}")
    except Exception as e:
        logger.error(f"Error in process_auto_queue_assignment_mapping_email: {e}")
        raise


with DAG(
    dag_id="process_auto_queue_assignment_mapping_email",
    default_args=default_args,
    description="Process auto queue assignment mapping email attachment and tries to update and create oscar rules otherwise uploaded directly to Oscar Rule through Oscar Supported Excel format of By Oscar GUI",
    schedule=None,  # This DAG is triggered via an API call or upstream process.
    start_date=pendulum.today('UTC').add(days=-1),
    tags=["autoqueue", "assignment", "mapping-excel-upload", "processing"],
) as dag:

    process_auto_queue_assignment_mapping_email_task = PythonOperator(
        task_id="process_access_management_requests",
        python_callable=process_auto_queue_assignment_mapping_email,
    )

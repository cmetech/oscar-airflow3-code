import logging
import re
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
import requests
import os

# Import hooks
from hooks.worklog_hook import WorkLogHook  # type: ignore
from hooks.notify_hook import NotifyHook  # type: ignore
from hooks.cache_hook import CacheHook  # type: ignore
from helpers.utils import normalize_boolean  # type: ignore
import asyncio

logger = logging.getLogger(__name__)

# Email validation regex
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

# Redis key prefix for cache warm tracking
CACHE_WARM_TRACKER_PREFIX = "maint_cache_warm_tracker:"
CACHE_WARM_TRACKER_TTL = 86400  # 24 hours - should be longer than max maintenance duration


def set_cache_warm_tracker(cr_number: str, worklog_hook: 'WorkLogHook' = None) -> bool:
    """
    Set Redis tracker key when maintenance is enabled.
    This tracks that cache warming was done for this CR.
    
    Args:
        cr_number: The CR number to track
        worklog_hook: Optional worklog hook for logging
        
    Returns:
        bool: True if tracker was set successfully
    """
    try:
        async def _set_tracker():
            async with CacheHook() as cache_hook:
                key = f"{CACHE_WARM_TRACKER_PREFIX}{cr_number}"
                value = {
                    "cr_number": cr_number,
                    "enabled_at": datetime.now(timezone.utc).isoformat(),
                    "cache_warmed": True
                }
                await cache_hook.store_item(key, value, expiry_seconds=CACHE_WARM_TRACKER_TTL)
                return True
        
        result = asyncio.run(_set_tracker())
        if worklog_hook:
            worklog_hook.info(f"Set cache warm tracker for CR: {cr_number}")
        logger.info(f"Set cache warm tracker for CR: {cr_number}")
        return result
    except Exception as e:
        logger.warning(f"Failed to set cache warm tracker for CR {cr_number}: {e}")
        if worklog_hook:
            worklog_hook.warning(f"Failed to set cache warm tracker: {e}")
        return False


def check_cache_warm_tracker(cr_number: str, worklog_hook: 'WorkLogHook' = None) -> bool:
    """
    Check if Redis tracker key exists for this CR.
    
    Args:
        cr_number: The CR number to check
        worklog_hook: Optional worklog hook for logging
        
    Returns:
        bool: True if tracker exists (enable was done with cache warming)
    """
    try:
        async def _check_tracker():
            async with CacheHook() as cache_hook:
                key = f"{CACHE_WARM_TRACKER_PREFIX}{cr_number}"
                try:
                    result = await cache_hook.get_item(key)
                    return result is not None and "value" in result
                except Exception:
                    return False
        
        exists = asyncio.run(_check_tracker())
        if worklog_hook:
            worklog_hook.info(f"Cache warm tracker {'exists' if exists else 'not found'} for CR: {cr_number}")
        logger.info(f"Cache warm tracker {'exists' if exists else 'not found'} for CR: {cr_number}")
        return exists
    except Exception as e:
        logger.warning(f"Failed to check cache warm tracker for CR {cr_number}: {e}")
        if worklog_hook:
            worklog_hook.warning(f"Failed to check cache warm tracker: {e}")
        return False


def delete_cache_warm_tracker(cr_number: str, worklog_hook: 'WorkLogHook' = None) -> bool:
    """
    Delete Redis tracker key after maintenance is disabled.
    
    Args:
        cr_number: The CR number to delete tracker for
        worklog_hook: Optional worklog hook for logging
        
    Returns:
        bool: True if tracker was deleted successfully
    """
    try:
        async def _delete_tracker():
            async with CacheHook() as cache_hook:
                key = f"{CACHE_WARM_TRACKER_PREFIX}{cr_number}"
                await cache_hook.delete_item(key)
                return True
        
        result = asyncio.run(_delete_tracker())
        if worklog_hook:
            worklog_hook.info(f"Deleted cache warm tracker for CR: {cr_number}")
        logger.info(f"Deleted cache warm tracker for CR: {cr_number}")
        return result
    except Exception as e:
        logger.warning(f"Failed to delete cache warm tracker for CR {cr_number}: {e}")
        if worklog_hook:
            worklog_hook.warning(f"Failed to delete cache warm tracker: {e}")
        return False


def trigger_cache_warm_dag(
    cr_number: str,
    worklog_id: str = None,
    worklog_hook: 'WorkLogHook' = None
) -> bool:
    """
    Trigger the warm_rule_cache DAG asynchronously.
    
    This function triggers the cache warming DAG without waiting for it to complete.
    It is designed to be non-blocking and should not disrupt the main maintenance flow.
    
    Args:
        cr_number: The CR number for logging/tracking
        worklog_id: Parent worklog ID to pass to the cache warm DAG
        worklog_hook: WorkLog hook for logging (optional)
    
    Returns:
        bool: True if trigger was successful, False otherwise
    """
    import time
    
    try:
        middleware_host = os.getenv('OSCAR_MIDDLEWARE_HOST', 'https://middleware:5200')
        
        # Get number of workers from environment (same as taskmanager uses)
        workers = int(os.getenv('TASKMANAGER_UVICORN_WORKERS', '4'))
        
        # Build DAG trigger URL
        dag_url = f"{middleware_host}/api/v1/workflows/warm_rule_cache"
        
        # Prepare payload
        dag_run_id = f"cache_warm_{cr_number}_{int(time.time())}"
        payload = {
            "conf": {
                "cr_number": cr_number,
                "parent_worklog_id": worklog_id,
                "workers": workers,
                "multiplier": 3.0,
                "delay": 0.4,
                "namespace": "notifier_autocaller"  # Target maintenance suppression namespace
            },
            "dag_run_id": dag_run_id
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        if worklog_hook:
            worklog_hook.info(f"Triggering cache warm DAG for CR: {cr_number}")
        
        # Trigger the DAG asynchronously (short timeout, don't wait for completion)
        response = requests.post(
            dag_url,
            json=payload,
            headers=headers,
            verify=False,
            timeout=10  # Short timeout - just trigger, don't wait
        )
        
        if response.status_code in [200, 201]:
            logger.info(f"Successfully triggered warm_rule_cache DAG: {dag_run_id}")
            if worklog_hook:
                worklog_hook.info(f"Cache warm DAG triggered successfully: {dag_run_id}")
            return True
        else:
            logger.warning(f"Failed to trigger cache warm DAG: {response.status_code} - {response.text}")
            if worklog_hook:
                worklog_hook.warning(f"Cache warm DAG trigger failed: {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        # Timeout is acceptable - the DAG may have been triggered
        logger.info(f"Cache warm DAG trigger timed out (may still be running)")
        if worklog_hook:
            worklog_hook.info("Cache warm DAG trigger timed out (may still be running)")
        return True  # Assume success on timeout
        
    except Exception as e:
        # Log but don't fail - cache warming is secondary priority
        logger.error(f"Error triggering cache warm DAG: {e}")
        if worklog_hook:
            worklog_hook.warning(f"Failed to trigger cache warm DAG: {e}")
        return False

# CR validation - any non-empty string is valid
# Removed strict format requirement per user request

# Default values
DEFAULT_DURATION_HOURS = 8
MAX_DURATION_HOURS = 72
DEFAULT_HANDLER = "autocaller"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def validate_email_address(email: str) -> bool:
    """
    Validate email address format.

    Args:
        email: Email address to validate

    Returns:
        bool: True if valid email format
    """
    return EMAIL_REGEX.match(email.strip()) is not None


def validate_cr(cr: str) -> bool:
    """
    Validate change request identifier.

    Args:
        cr: Change request identifier to validate

    Returns:
        bool: True if CR is non-empty string
    """
    return bool(cr and cr.strip())


def validate_action(action: str) -> bool:
    """
    Validate maintenance action.

    Args:
        action: Action to validate

    Returns:
        bool: True if action is 'enable' or 'disable' (case-insensitive)
    """
    return action.lower() in ['enable', 'disable'] if action else False


def parse_maintenance_subject(subject: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse maintenance action and CR number from email subject.

    Expected formats:
    - MAINTENANCE MODE: ENABLE - CR000042424
    - MAINTENANCE MODE: DISABLE - CR000042424
    - Maintenance Mode: Enable CR000042424

    Args:
        subject: Email subject line

    Returns:
        Tuple of (action, cr) or (None, None) if parsing fails
    """
    # Clean subject - normalize unicode characters
    # Replace common special characters that appear in emails
    subject = subject.strip()
    # Replace em-dash (—) and en-dash (–) with regular hyphen
    subject = re.sub(r'[—–]', '-', subject)
    # Replace non-breaking spaces and other whitespace variants with regular space
    subject = re.sub(r'[\u00A0\u2000-\u200B\u202F\u205F\u3000]', ' ', subject)
    subject = subject.upper()

    # Pattern 1: "MAINTENANCE MODE: ACTION - CR_IDENTIFIER"
    # Now handles normalized dashes and any separator characters
    pattern1 = r'MAINTENANCE\s*MODE\s*:\s*(ENABLE|DISABLE)\s*[-]?\s*(\S+)'
    match = re.search(pattern1, subject, re.IGNORECASE)
    if match:
        action = match.group(1).lower()
        cr = match.group(2)
        return action, cr

    # Pattern 2: "MAINTENANCE: ACTION CR_IDENTIFIER"
    pattern2 = r'MAINTENANCE\s*:\s*(ENABLE|DISABLE)\s+(\S+)'
    match = re.search(pattern2, subject, re.IGNORECASE)
    if match:
        action = match.group(1).lower()
        cr = match.group(2)
        return action, cr

    return None, None


def strip_quotes(value: str) -> str:
    """
    Strip matching quotes from a string.

    Args:
        value: String that may have quotes

    Returns:
        String with matching quotes removed, or original if quotes don't match
    """
    value = value.strip()
    if len(value) >= 2:
        # Check for matching single quotes
        if value[0] == "'" and value[-1] == "'":
            return value[1:-1].strip()
        # Check for matching double quotes
        elif value[0] == '"' and value[-1] == '"':
            return value[1:-1].strip()
    return value


def parse_list_from_body(text: str, key: str) -> List[str]:
    """
    Parse a list of values from email body for a given key.

    Supports formats:
    - key: value1, value2, value3
    - key:
        - value1
        - value2

    Args:
        text: Email body text
        key: Key to search for

    Returns:
        List of parsed values
    """
    values = []

    # Pattern for inline comma-separated values
    inline_pattern = rf'^{key}\s*:\s*([^:\n]+(?:,[^:\n]+)+)$'
    inline_match = re.search(inline_pattern, text, re.IGNORECASE | re.MULTILINE)
    logger.info(f"inline_match: {inline_match}")

    if inline_match:
        # Check if this is followed by a list
        value_text = inline_match.group(1).strip()
        if value_text and not value_text.endswith(':'):
            # First check if the entire value is quoted
            value_text = strip_quotes(value_text)
            # Parse comma-separated values and strip quotes from each
            values = [strip_quotes(v.strip()) for v in value_text.split(',') if v.strip()]
            return values

    # Pattern for list format
    list_pattern = rf'{key}\s*:\s*\n((?:\s*[-*]\s*[^\n]+\n?)+)'
    list_match = re.search(list_pattern, text, re.IGNORECASE | re.MULTILINE)
    logger.info(f"list_match: {list_match}")
    if list_match:
        list_text = list_match.group(1)
        # Extract each list item
        item_pattern = r'[-*]\s*([^\n]+)'
        values = re.findall(item_pattern, list_text)
        # Strip whitespace and quotes from each value
        values = [strip_quotes(v.strip()) for v in values if v.strip()]

    return values


def parse_single_value(text: str, key: str) -> Optional[str]:
    """
    Parse a single value from email body for a given key.

    Args:
        text: Email body text
        key: Key to search for

    Returns:
        Parsed value or None
    """
    pattern = rf'{key}\s*:\s*([^\n]+)'
    match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
    if match:
        return strip_quotes(match.group(1).strip())
    return None


def parse_maintenance_body(body: str) -> Dict[str, Any]:
    """
    Parse maintenance parameters from email body.

    Args:
        body: Email body text

    Returns:
        Dictionary of parsed parameters
    """
    params = {}

    # Parse CR
    cr_value = parse_single_value(body, 'cr')
    if cr_value:
        params['cr'] = cr_value.strip()

    # Parse action
    action_value = parse_single_value(body, 'action') or parse_single_value(body, 'enable')
    if action_value:
        if action_value.lower() in ['true', 'yes', '1', 'enable']:
            params['action'] = 'enable'
        elif action_value.lower() in ['false', 'no', '0', 'disable']:
            params['action'] = 'disable'

    # Parse duration
    duration_value = parse_single_value(body, 'duration')
    if duration_value:
        # Extract numeric value
        duration_match = re.search(r'(\d+)', duration_value)
        if duration_match:
            params['duration'] = int(duration_match.group(1))

    # Parse servers
    servers = parse_list_from_body(body, 'servers') or parse_list_from_body(body, 'server')

    logger.info(f"Parsed servers: {servers}")
    if not servers:
        # Try single line format
        server_value = parse_single_value(body, 'servers') or parse_single_value(body, 'server')
        if server_value:
            servers = [strip_quotes(s.strip()) for s in server_value.split(',') if s.strip()]
    if servers:
        params['servers'] = servers

    # Parse notification emails
    notify_emails = parse_list_from_body(body, 'notify') or parse_list_from_body(body, 'notifications')
    if not notify_emails:
        # Try single line format
        notify_value = parse_single_value(body, 'notify') or parse_single_value(body, 'notifications')
        if notify_value:
            notify_emails = [strip_quotes(e.strip()) for e in notify_value.split(',') if e.strip()]
    if notify_emails:
        params['notify_emails'] = notify_emails

    # Parse timezone
    timezone_value = parse_single_value(body, 'timezone') or parse_single_value(body, 'tz')
    if timezone_value:
        params['timezone'] = timezone_value

    # Parse environments
    environments = parse_list_from_body(body, 'environments') or parse_list_from_body(body, 'environment')
    if not environments:
        env_value = parse_single_value(body, 'environments') or parse_single_value(body, 'environment')
        if env_value:
            environments = [strip_quotes(e.strip()) for e in env_value.split(',') if e.strip()]
    if environments:
        params['environments'] = environments

    # Parse handlers
    handlers = parse_list_from_body(body, 'handlers') or parse_list_from_body(body, 'handler')
    if not handlers:
        handler_value = parse_single_value(body, 'handlers') or parse_single_value(body, 'handler')
        if handler_value:
            handlers = [strip_quotes(h.strip()) for h in handler_value.split(',') if h.strip()]
    if handlers:
        params['handlers'] = handlers

    return params


def send_notification_email(
    notify_hook: NotifyHook,
    to_addresses: List[str],
    subject: str,
    body: str,
    worklog_hook: Optional[WorkLogHook] = None
) -> bool:
    """
    Send notification email using NotifyHook.

    Args:
        notify_hook: NotifyHook instance
        to_addresses: List of recipient email addresses
        subject: Email subject
        body: Email body
        worklog_hook: Optional WorkLogHook for logging

    Returns:
        bool: True if email sent successfully
    """
    try:
        # Validate email addresses
        valid_emails = [email for email in to_addresses if validate_email_address(email)]
        if not valid_emails:
            logger.error(f"No valid email addresses found in: {to_addresses}")
            return False

        # Get mail notifier name from environment, default to 'mail_notifier'
        mail_notifier_name = os.getenv('MAIL_NOTIFIER_NAME', 'mail_notifier')

        # Send notification using the expected format
        notification_data = {
            "name": mail_notifier_name,  # Use mail notifier from env
            "recipients": ",".join(valid_emails),  # Comma-separated string
            "subject": subject,
            "message": body
        }

        result = notify_hook.send_notification(notification_data)

        if worklog_hook:
            worklog_hook.info(f"Sent notification to {', '.join(valid_emails)}")

        return True

    except Exception as e:
        logger.error(f"Failed to send notification email: {e}")
        if worklog_hook:
            worklog_hook.error(f"Failed to send notification: {e}")
        return False


def call_maintenance_api(
    action: str,
    cr: str,
    params: Dict[str, Any],
    worklog_hook: WorkLogHook
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Call the maintenance mode API.

    Args:
        action: 'enable' or 'disable'
        cr: Change request identifier
        params: Additional parameters for the API call
        worklog_hook: WorkLog hook for logging

    Returns:
        Tuple of (success, message, response_data)
    """
    # Get configuration from environment
    import os
    # Use internal Docker service name for internal communication
    middleware_host = os.getenv('OSCAR_MIDDLEWARE_HOST', 'https://middleware:5200')

    # Build API URL - use internal path without /ext/mw prefix
    base_url = f"{middleware_host}/api/v1/notifiers/maintenance"

    try:
        if action == 'enable':
            # Prepare enable request
            url = f"{base_url}/enable"
            data = {
                "cr_number": cr,
                "servers": params.get('servers', []),
                "duration_hours": params.get('duration', DEFAULT_DURATION_HOURS),
                "handlers": params.get('handlers', [DEFAULT_HANDLER]),
                "timezone": params.get('timezone', 'UTC')
            }

            logger.info(f"enable paylaod: {data} enable url: {url}")

            if params.get('notify_emails'):
                data['notify_emails'] = params['notify_emails']
            if params.get('environments'):
                data['environments'] = params['environments']

            worklog_hook.info(f"Calling maintenance enable API with data: {json.dumps(data, indent=2)}")

            response = requests.post(
                url,
                json=data,
                headers={
                    'Content-Type': 'application/json',
                    'X-Internal-Service': 'airflow'
                },
                verify=False
            )

        elif action == 'disable':
            # Prepare disable request
            url = f"{base_url}/disable"
            data = {"cr_number": cr}

            worklog_hook.info(f"Calling maintenance disable API for CR: {cr}")

            response = requests.post(
                url,
                json=data,
                headers={
                    'Content-Type': 'application/json',
                    'X-Internal-Service': 'airflow'
                },
                verify=False
            )

        else:
            return False, f"Invalid action: {action}", {}

        # Check response
        if response.status_code in [200, 201]:
            response_data = response.json()
            worklog_hook.info(f"Maintenance {action} successful: {json.dumps(response_data, indent=2)}")
            return True, f"Maintenance {action}d successfully", response_data
        else:
            error_msg = f"API call failed with status {response.status_code}: {response.text}"
            worklog_hook.error(error_msg)
            return False, error_msg, {}

    except Exception as e:
        error_msg = f"Error calling maintenance API: {str(e)}"
        worklog_hook.error(error_msg)
        return False, error_msg, {}


def extract_email(address: str) -> str:
    """
    Extracts the email address from an email header string (e.g., "Name <email@example.com>").

    Args:
        address: The email header string.

    Returns:
        The extracted email address.
    """
    match = re.search(r'<([^>]+)>', address)
    if match:
        return match.group(1).strip()
    return address.strip()


def process_maintenance_request(**context):
    """
    Process incoming maintenance request email.

    This function:
    1. Extracts email data from the DAG run context
    2. Parses the subject and body for maintenance instructions
    3. Validates all inputs
    4. Calls the maintenance API
    5. Sends notifications about the result
    """
    # Initialize variables to ensure they're available in finally block
    worklog_hook = None
    worklog_id = None
    routing_worklog_id = None
    result = None

    try:
        # Get email data from context
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        email_data = conf.get("email_data", {})

        # Get routing worklog ID if passed from route_email_request
        routing_worklog_id = conf.get("worklog_id")

        if not email_data:
            logger.error("No email data provided in DAG run configuration")
            return None

        # Extract email fields
        subject = email_data.get("subject", "(No Subject)")
        from_addr = email_data.get("from", "(No From Address)")
        to_addr = email_data.get("to", "(No To Address)")
        cc_addr = email_data.get("cc", "")
        # Apply extraction to to_addr and from_addr (they will come in following format: Name <email@example.com>)
        to_addr = extract_email(to_addr) if to_addr else to_addr
        from_addr = extract_email(from_addr) if from_addr else from_addr

        # Log extracted email addresses
        logger.info(f"Extracted from_addr: {from_addr}")
        logger.info(f"Extracted to_addr: {to_addr}")
        date = email_data.get("date", str(datetime.now(timezone.utc)))
        body = email_data.get("body", "")

        # Initialize hooks
        worklog_hook = WorkLogHook()
        notify_hook = NotifyHook()

        # Create worklog
        worklog = worklog_hook.create_worklog(
            name="Maintenance Request Processing",
            description=f"Processing maintenance request from {from_addr}"
        )
        worklog_id = worklog["id"]

        # Log email details
        worklog_hook.info("Processing maintenance request email")
        worklog_hook.info(f"Subject: {subject}")
        worklog_hook.info(f"From: {from_addr}")
        worklog_hook.info(f"To: {to_addr}")
        if cc_addr:
            worklog_hook.info(f"CC: {cc_addr}")
        worklog_hook.info(f"Date: {date}")

        # Initialize notification list with sender
        notification_emails = [from_addr]

        # Add to/cc addresses to notification list
        if to_addr:
            to_emails = [e.strip() for e in to_addr.split(',') if e.strip()]
            notification_emails.extend(to_emails)
        if cc_addr:
            cc_emails = [e.strip() for e in cc_addr.split(',') if e.strip()]
            notification_emails.extend(cc_emails)

        # Remove duplicates and validate
        notification_emails = list(set(email for email in notification_emails if validate_email_address(email)))

        # Start of main processing logic
        # Initialize tracking variables
        action = None
        cr = None
        action_source = None
        cr_source = None

        # Parse body first
        worklog_hook.info("Parsing email body for parameters")
        body_params = parse_maintenance_body(body)

        # Get action from body
        if 'action' in body_params:
            action = body_params['action']
            action_source = 'body'
            worklog_hook.info(f"Action found in body: {action}")

        # Get CR from body
        if 'cr' in body_params:
            cr = body_params['cr']
            cr_source = 'body'
            worklog_hook.info(f"CR found in body: {cr}")

        # Parse subject for missing parameters
        if not action or not cr:
            worklog_hook.info("Parsing email subject for missing parameters")
            subject_action, subject_cr = parse_maintenance_subject(subject)

            # Get action from subject if not in body
            if not action and subject_action:
                action = subject_action
                action_source = 'subject'
                worklog_hook.info(f"Action found in subject: {action}")
            elif action and subject_action and action != subject_action:
                worklog_hook.info(f"Body action '{action}' overrides subject action '{subject_action}'")

            # Get CR from subject if not in body
            if not cr and subject_cr:
                cr = subject_cr
                cr_source = 'subject'
                worklog_hook.info(f"CR found in subject: {cr}")
            elif cr and subject_cr and cr != subject_cr:
                worklog_hook.info(f"Body CR '{cr}' overrides subject CR '{subject_cr}'")

        # Log parsing summary
        worklog_hook.info(f"Email parsing summary: CR from {cr_source or 'not found'}, Action from {action_source or 'not found'}")

        # Validate action
        if not action:
            error_msg = "Could not determine maintenance action from email (neither subject nor body)"
            worklog_hook.error(error_msg)

            # Try to send error notification
            try:
                send_notification_email(
                    notify_hook,
                    notification_emails,
                    "Maintenance Request Failed - Missing Action",
                    f"""Your maintenance request could not be processed.

Error: {error_msg}

Please use the following format in your email subject:
MAINTENANCE MODE: ENABLE - CR000042424
or
MAINTENANCE MODE: DISABLE - CR000042424

Original email:
Subject: {subject}
From: {from_addr}
Date: {date}
""",
                    worklog_hook
                )
            except Exception as notify_error:
                worklog_hook.error(f"Failed to send error notification: {notify_error}")

            result = {"status": "error", "message": error_msg}
            return result

        # Validate action value
        if action and not validate_action(action):
            error_msg = f"Invalid maintenance action: '{action}'. Must be 'enable' or 'disable'"
            worklog_hook.error(f"Action validation failed: '{action}' is not a valid action")

            # Try to send error notification
            try:
                send_notification_email(
                    notify_hook,
                    notification_emails,
                    "Maintenance Request Failed - Invalid Action",
                    f"""Your maintenance request could not be processed.

Error: {error_msg}

Please use one of the following actions:
- enable: To start maintenance mode
- disable: To stop maintenance mode

Original email:
Subject: {subject}
From: {from_addr}
Date: {date}
""",
                    worklog_hook
                )
            except Exception as notify_error:
                worklog_hook.error(f"Failed to send error notification: {notify_error}")

            result = {"status": "error", "message": error_msg}
            return result

        worklog_hook.info(f"Action validation passed: {action}")

        if not cr:
            error_msg = "Could not find change request identifier in email (neither subject nor body)"
            worklog_hook.error(error_msg)

            # Try to send error notification
            try:
                send_notification_email(
                    notify_hook,
                    notification_emails,
                    "Maintenance Request Failed - Missing CR",
                    f"""Your maintenance request could not be processed.

Error: {error_msg}

Please include a change request identifier in your email subject or body.
The CR can be any text identifier, for example:
- CR12345
- PROJ-123
- maintenance-2024-01

Format in subject: MAINTENANCE MODE: ENABLE - <your-cr-id>
Format in body: cr: <your-cr-id>

Original email:
Subject: {subject}
From: {from_addr}
Date: {date}
""",
                    worklog_hook
                    )
            except Exception as notify_error:
                worklog_hook.error(f"Failed to send error notification: {notify_error}")

            result = {"status": "error", "message": error_msg}
            return result

        # Validate CR format
        if not validate_cr(cr):
            error_msg = "Invalid change request identifier: CR cannot be empty"
            worklog_hook.error("CR validation failed: empty or whitespace-only CR")

            # Try to send error notification
            try:
                send_notification_email(
                    notify_hook,
                    notification_emails,
                    "Maintenance Request Failed - Invalid CR Format",
                    f"""Your maintenance request could not be processed.

Error: {error_msg}

The change request identifier must not be empty or contain only whitespace.

Original email:
Subject: {subject}
From: {from_addr}
Date: {date}
""",
                    worklog_hook
                    )
            except Exception as notify_error:
                worklog_hook.error(f"Failed to send error notification: {notify_error}")

            result = {"status": "error", "message": error_msg}
            return result

        worklog_hook.info(f"CR validation passed: {cr}")
        worklog_hook.info(f"Final parsed values - Action: {action} (from {action_source}), CR: {cr} (from {cr_source})")

        # Parse additional parameters from body (already parsed above)
        worklog_hook.info(f"Additional body parameters: {json.dumps({k: v for k, v in body_params.items() if k not in ['action', 'cr']}, indent=2)}")

        # Add notification emails from body
        if body_params.get('notify_emails'):
            notification_emails.extend(body_params['notify_emails'])
            notification_emails = list(set(email for email in notification_emails if validate_email_address(email)))

        # Validate required fields for enable action
        if action == 'enable' and not body_params.get('servers'):
            error_msg = "Server list is required for enabling maintenance mode"
            worklog_hook.error(error_msg)

            # Try to send error notification
            try:
                send_notification_email(
                    notify_hook,
                    notification_emails,
                    "Maintenance Request Failed - Missing Servers",
                    f"""Your maintenance request could not be processed.

Error: {error_msg}

Please include a list of servers in your email body:
servers: web*.prod.example.com, db*.prod.example.com

Or in list format:
servers:
  - web01.prod.example.com
  - web02.prod.example.com

Original email:
Subject: {subject}
From: {from_addr}
Date: {date}
""",
                    worklog_hook
                    )
            except Exception as notify_error:
                worklog_hook.error(f"Failed to send error notification: {notify_error}")

            result = {"status": "error", "message": error_msg}
            return result

        # Validate duration
        if body_params.get('duration'):
            duration = body_params['duration']
            if duration > MAX_DURATION_HOURS:
                worklog_hook.warning(f"Duration {duration} exceeds maximum {MAX_DURATION_HOURS}, using maximum")
                body_params['duration'] = MAX_DURATION_HOURS

        # Call maintenance API
        success, message, api_response = call_maintenance_api(action, cr, body_params, worklog_hook)

        if success:
            # Send success notification
            if action == 'enable':
                notification_body = f"""Maintenance mode has been successfully enabled for {cr}.

Details:
- Suppression ID: {api_response.get('suppression_id', 'N/A')}
- Servers: {', '.join(body_params.get('servers', []))}
- Duration: {body_params.get('duration', DEFAULT_DURATION_HOURS)} hours
- Start Time: {api_response.get('start_time', 'N/A')}
- End Time: {api_response.get('end_time', 'N/A')}
- Handlers: {', '.join(body_params.get('handlers', [DEFAULT_HANDLER]))}

Original request from: {from_addr}
Processed at: {datetime.now(timezone.utc).isoformat()}
"""
            else:
                notification_body = f"""Maintenance mode has been successfully disabled for {cr}.

Details:
- Suppression ID: {api_response.get('suppression_id', 'N/A')}

Original request from: {from_addr}
Processed at: {datetime.now(timezone.utc).isoformat()}
"""

            logger.info(f"Send notification emails: {notification_emails}")
            # Try to send success notification
            try:
                send_notification_email(
                    notify_hook,
                    notification_emails,
                    f"Maintenance Request Successful - {cr}",
                    notification_body,
                    worklog_hook
                )
            except Exception as notify_error:
                worklog_hook.error(f"Failed to send success notification: {notify_error}")
                # Don't fail the entire request just because notification failed

            worklog_hook.info("Maintenance request processed successfully")
            result = {"status": "success", "message": message, "data": api_response}
            
            # Cache warming logic based on action type
            # This ensures all uvicorn workers have consistent rule cache state
            # This is secondary priority - failure should not affect main flow
            if action == 'enable':
                # On enable: trigger cache warm and set Redis tracker
                try:
                    # Small delay to ensure rule is fully propagated before cache warming
                    time.sleep(1)
                    dag_trigger_success = trigger_cache_warm_dag(
                        cr_number=cr,
                        worklog_id=worklog_id,
                        worklog_hook=worklog_hook
                    )
                    # Set Redis tracker to track that we warmed cache for this CR
                    # Only set tracker if DAG trigger actually succeeded (not just timed out)
                    if dag_trigger_success:
                        set_cache_warm_tracker(cr, worklog_hook)
                    else:
                        worklog_hook.warning(f"Cache warm DAG trigger failed, not setting tracker for CR: {cr}")
                except Exception as cache_warm_error:
                    # Log but don't fail - cache warming is secondary priority
                    worklog_hook.warning(f"Cache warm trigger failed (non-critical): {cache_warm_error}")
                    logger.warning(f"Cache warm trigger failed: {cache_warm_error}")
            
            elif action == 'disable':
                # On disable: check if enable tracker exists, warm cache again, then delete tracker
                try:
                    tracker_exists = check_cache_warm_tracker(cr, worklog_hook)
                    if tracker_exists:
                        worklog_hook.info(f"Found cache warm tracker for CR {cr}, triggering cache warm after disable")
                        # Small delay to ensure rule removal is fully propagated before cache warming
                        time.sleep(1)
                        # Trigger cache warm to ensure all workers remove the suppression rule from cache
                        trigger_cache_warm_dag(
                            cr_number=cr,
                            worklog_id=worklog_id,
                            worklog_hook=worklog_hook
                        )
                        # Delete the tracker after successful disable
                        delete_cache_warm_tracker(cr, worklog_hook)
                    else:
                        worklog_hook.info(f"No cache warm tracker found for CR {cr}, skipping cache warm on disable")
                except Exception as cache_warm_error:
                    # Log but don't fail - cache warming is secondary priority
                    worklog_hook.warning(f"Cache warm on disable failed (non-critical): {cache_warm_error}")
                    logger.warning(f"Cache warm on disable failed: {cache_warm_error}")

        else:
            # Try to send failure notification
            try:
                send_notification_email(
                    notify_hook,
                    notification_emails,
                    f"Maintenance Request Failed - {cr}",
                    f"""Your maintenance request could not be processed.

Error: {message}

Request Details:
- Action: {action}
- CR: {cr}
- Servers: {', '.join(body_params.get('servers', []))}

Please check the error message and try again. If the problem persists, contact the system administrator.

Original email:
Subject: {subject}
From: {from_addr}
Date: {date}
""",
                    worklog_hook
                    )
            except Exception as notify_error:
                worklog_hook.error(f"Failed to send failure notification: {notify_error}")

            worklog_hook.error(f"Maintenance request failed: {message}")
            result = {"status": "error", "message": message}

        return result

    except Exception as e:
        error_msg = f"Unexpected error processing maintenance request: {str(e)}"
        logger.error(error_msg, exc_info=True)

        if worklog_hook:
            try:
                worklog_hook.error(error_msg)
            except Exception as log_error:
                logger.error(f"Failed to log error to worklog: {log_error}")

        # Try to send error notification if we have enough context
        if 'notify_hook' in locals() and 'notification_emails' in locals() and notification_emails:
            try:
                send_notification_email(
                    notify_hook,
                    notification_emails,
                    "Maintenance Request Failed - System Error",
                    f"""Your maintenance request could not be processed due to a system error.

Error: {error_msg}

Please try again later or contact the system administrator.

Original email:
Subject: {subject if 'subject' in locals() else 'Unknown'}
From: {from_addr if 'from_addr' in locals() else 'Unknown'}
Date: {date if 'date' in locals() else 'Unknown'}
""",
                    worklog_hook
                )
            except Exception as notify_error:
                logger.error(f"Failed to send error notification: {notify_error}")

        result = {"status": "error", "message": error_msg}
        return result

    finally:
        # Always close the worklog if it was created
        if worklog_hook and worklog_id:
            try:
                worklog_hook.close_worklog(worklog_id)
                logger.info(f"Closed worklog {worklog_id}")
            except Exception as close_error:
                logger.error(f"Failed to close worklog {worklog_id}: {close_error}")

        # Handle routing worklog if it exists
        if worklog_hook and routing_worklog_id:
            try:
                # Create a new hook instance for the routing worklog
                routing_worklog_hook = WorkLogHook(worklog_id=routing_worklog_id)

                # Add summary entry to routing worklog
                if result and result.get("status") == "success":
                    routing_worklog_hook.info(
                        f"Email routed successfully to process_maintenance_request. "
                        f"Maintenance request completed with status: {result.get('message', 'success')}. "
                        f"See worklog {worklog_id} for details."
                    )
                else:
                    error_msg = result.get("message", "Unknown error") if result else "Unknown error occurred"
                    routing_worklog_hook.error(
                        f"Email routed to process_maintenance_request but processing failed: {error_msg}. "
                        f"See worklog {worklog_id} for details."
                    )

                # Close the routing worklog
                routing_worklog_hook.close_worklog()
                logger.info(f"Closed routing worklog {routing_worklog_id}")

            except Exception as routing_close_error:
                logger.error(f"Failed to handle routing worklog {routing_worklog_id}: {routing_close_error}")

        # Return the result if set, otherwise return error
        if result:
            return result
        else:
            return {"status": "error", "message": "Unknown error occurred"}


# Create the DAG
with DAG(
    dag_id="process_maintenance_request",
    default_args=default_args,
    description="Process maintenance mode requests from email",
    schedule=None,  # Triggered by route_email_request
    start_date=pendulum.today('UTC').add(days=-1),
    tags=["email", "maintenance", "processing"],
    catchup=False
) as dag:

    # Define the task
    process_maintenance_task = PythonOperator(
        task_id="process_maintenance_request",
        python_callable=process_maintenance_request,
    )

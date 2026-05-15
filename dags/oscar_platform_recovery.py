"""
OSCAR Platform Auto-Recovery DAG

This DAG is triggered when an OscarPlatformUnhealthy alert is received.
It automatically attempts to recover the OSCAR platform by connecting via SSH
and restarting the service.

Alert Structure Expected:
{
  "labels": {
    "alertname": "OscarPlatformUnhealthy",
    "meta_ipaddress": "10.0.1.100",
    "severity": "critical",
    "instance": "prod-oscar-01"
  },
  "annotations": {
    "description": "OSCAR platform is unhealthy",
    "summary": "OSCAR service not responding"
  }
}
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException, AirflowException
import logging
import json
import time
from typing import Dict, Any
from fabric import Connection
from paramiko.ssh_exception import SSHException, NoValidConnectionsError
from jinja2 import Environment, FileSystemLoader

# Import custom hooks
from hooks.worklog_hook import WorkLogHook, WorkLogType  # type: ignore
from hooks.notify_hook import NotifyHook  # type: ignore
from hooks.ticketing_hook import TicketingHook  # type: ignore

logger = logging.getLogger(__name__)

# Constants
ALERT_NAME_TARGET = "OscarPlatformUnhealthy"
DEFAULT_SSH_USER = "oscar"
DEFAULT_SSH_PORT = 22
OSCAR_BASE_PATH = "/oscar_app/oscar"
TEMPLATE_DIR = "/opt/airflow/templates/email/html"

# Initialize Jinja2 template environment
try:
    template_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=False)
    logger.info(f"Initialized Jinja2 template environment with directory: {TEMPLATE_DIR}")
    logger.debug(f"Available templates: {template_env.list_templates()}")
except Exception as e:
    logger.error(f"Failed to initialize Jinja2 template environment: {str(e)}")
    template_env = None
SSH_TIMEOUT = 30
COMMAND_TIMEOUT = 240
RETRY_DELAY = 10
START_TIMEOUT = 900  # allow up to 15 minutes for full start sequence

# Sections that do not warrant a full platform restart if down (configurable)
_non_critical_env = os.environ.get("OSCAR_NON_CRITICAL_SECTIONS", "docs")
NON_CRITICAL_SECTIONS = {
    s.strip().lower() for s in _non_critical_env.split(",") if s.strip()
}

# Critical containers list (same as monitor service for consistency)
MONITOR_REQUIRED_CONTAINERS_JSON = os.environ.get("MONITOR_REQUIRED_CONTAINERS", "[]")
NAMESPACE = os.environ.get("NAMESPACE", "oscar")
# Note: PLATFORM_ARCH from env is only used as fallback - we detect remote platform via SSH
PLATFORM_ARCH = os.environ.get("PLATFORM_ARCH", "amd64")

# Optional receiver service flags
TRAPRECEIVER_ENABLED = os.environ.get("TRAPRECEIVER_ENABLED", "true").lower() == "true"
SMTPRECEIVER_ENABLED = os.environ.get("SMTPRECEIVER_ENABLED", "true").lower() == "true"

# Notifier configuration
NOTIFIER_NAME = os.environ.get("OSCAR_ENABLE_NOTIFIER_NAME", "mail_notifier")
NOTIFIER_GROUP_ID = os.environ.get("OSCAR_NOTIFIER_GROUP_ID", "oscaradmin_group")

# Testing/Development mode - bypasses actual SSH recovery
# Set OSCAR_RECOVERY_STUB=true to simulate successful recovery without SSH
STUB_MODE = os.environ.get("OSCAR_RECOVERY_STUB", "false").lower() == "true"
if STUB_MODE:
    logger.warning("⚠️ STUB MODE ENABLED - SSH recovery will be simulated, not executed")

# Default DAG arguments
default_args = {
    'owner': 'oscar-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10)
}


def _parse_simple_status(output: str) -> tuple:
    """
    Parse the output of `./oscar status --simple` and return:
    - List of services that are down
    - List of services that are up

    Output format is: "service:status" where status is up/down/not_found
    If --simple is not supported, falls back to parsing regular status output.
    """
    down_services = []
    up_services = []

    if not output:
        return down_services, up_services

    # First try to parse simple format (service:status)
    has_simple_format = False
    for line in output.splitlines():
        line = line.strip()
        if ':' in line and not line.startswith('Checking status'):
            parts = line.split(':', 1)
            if len(parts) == 2:
                service, status = parts
                service = service.strip()
                status = status.strip().lower()
                if status == 'up':
                    up_services.append(service)
                    has_simple_format = True
                elif status in ['down', 'not_found', 'stopped']:
                    down_services.append(service)
                    has_simple_format = True

    # If we didn't find simple format, try to parse regular status output
    if not has_simple_format:
        # Look for sections with "Checking status of X" followed by no container rows
        current_section = None
        has_containers = False

        for line in output.splitlines():
            line = line.strip()

            # Detect section headers
            if line.startswith('Checking status of '):
                # Save previous section status
                if current_section:
                    if has_containers:
                        up_services.append(current_section)
                    else:
                        down_services.append(current_section)

                # Start new section
                current_section = line.replace('Checking status of ', '').replace('...', '').strip()
                has_containers = False

            # Check if line looks like a container row (has multiple columns)
            elif current_section and line and not line.startswith('NAME'):
                # If line has spaces/tabs, it's likely a container row
                if '\t' in line or len(line.split()) > 1:
                    has_containers = True

        # Don't forget the last section
        if current_section:
            if has_containers:
                up_services.append(current_section)
            else:
                down_services.append(current_section)

    return down_services, up_services


def _matches_oscar_pattern(container_name: str, patterns: list) -> bool:
    """
    Check if container name matches any OSCAR service pattern.
    Supports wildcard patterns like '*_middleware' and 'adminui-*'.
    """
    import re

    for pattern in patterns:
        # Convert wildcard pattern to regex
        regex_pattern = re.escape(pattern).replace(r'\*', '.*')
        regex_pattern = f'^{regex_pattern}$'

        if re.match(regex_pattern, container_name):
            return True

    return False


def _preprocess_required_containers(containers_json: str, namespace: str, platform_arch: str) -> list:
    """
    Preprocess container list same as oscar-monitor service:
    - Filter out disabled optional receiver services
    - Prepend namespace to OSCAR services (no '/' in name)
    - Adjust platform suffix for adminui containers
    - Keep third-party containers as-is

    Returns list of expected container image names (without versions).
    """
    import re

    try:
        containers = json.loads(containers_json)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse MONITOR_REQUIRED_CONTAINERS: {e}")
        return []

    # OSCAR service patterns (from monitor service settings.py)
    OSCAR_SERVICE_PATTERNS = [
        'monitor', 'trapreceiver', 'smtpreceiver', 'taskmanager', 'scheduler',
        'notifier', 'middleware', 'workflow', 'ui', 'util',
        '*_middleware',  # Matches all *_middleware services
        'adminui-*',     # Matches adminui-arm64, adminui-amd64, etc.
    ]

    adjusted = []
    for container in containers:
        # Skip disabled optional receiver services
        container_lower = container.lower()
        if "trapreceiver" in container_lower and not TRAPRECEIVER_ENABLED:
            logger.info(f"Skipping disabled service: {container}")
            continue
        if "smtpreceiver" in container_lower and not SMTPRECEIVER_ENABLED:
            logger.info(f"Skipping disabled service: {container}")
            continue

        # Step 1: Add namespace if OSCAR service
        if '/' not in container:
            container_name = container.split(':')[0] if ':' in container else container
            if _matches_oscar_pattern(container_name, OSCAR_SERVICE_PATTERNS):
                container = f"{namespace}/{container}"

        # Step 2: Adjust platform for adminui
        if "adminui-" in container:
            container = re.sub(r'adminui-(amd64|arm64)', f'adminui-{platform_arch}', container)

        adjusted.append(container)

    return adjusted


def _detect_remote_platform(conn) -> str:
    """
    Detect the platform architecture of the remote target system via SSH.

    Returns: 'amd64' or 'arm64', defaults to 'amd64' if detection fails
    """
    try:
        # Run uname -m to get the machine architecture
        result = conn.run("uname -m", hide=True, timeout=10, warn=True)

        if result.ok and result.stdout:
            arch = result.stdout.strip().lower()
            logger.info(f"Detected remote platform architecture: {arch}")

            # Map common architecture names to our platform identifiers
            if arch in ('x86_64', 'amd64', 'x64'):
                return 'amd64'
            elif arch in ('aarch64', 'arm64', 'armv8'):
                return 'arm64'
            else:
                logger.warning(f"Unknown architecture '{arch}', defaulting to amd64")
                return 'amd64'
        else:
            logger.warning("Failed to detect remote platform, defaulting to amd64")
            return 'amd64'

    except Exception as e:
        logger.warning(f"Error detecting remote platform: {e}, defaulting to amd64")
        return 'amd64'


def _check_critical_containers_running(conn, oscar_home: str, expected_containers: list) -> tuple:
    """
    Check if all expected containers are running.
    expected_containers should already have namespace and platform preprocessing applied.

    Returns: (missing_containers, running_containers)
    """
    # Get all running container images
    docker_cmd = f"cd {oscar_home} && docker ps --format '{{{{.Image}}}}'"
    result = conn.run(docker_cmd, hide=True, timeout=COMMAND_TIMEOUT, warn=True)

    if not result.stdout:
        logger.warning("No docker ps output - all containers may be down")
        return expected_containers, []

    running_images = [img.strip() for img in result.stdout.strip().split('\n') if img.strip()]
    logger.debug(f"Found {len(running_images)} running containers")

    missing = []
    running = []

    for expected in expected_containers:
        # Version-agnostic: strip tag from expected
        expected_name = expected.split(':')[0] if ':' in expected else expected

        # Check if this image (any version) is running
        # Compare: "oscar/monitor" with "oscar/monitor:v1.0.0"
        found = any(
            expected_name in running_img or running_img.startswith(expected_name + ':')
            for running_img in running_images
        )

        if found:
            running.append(expected)
        else:
            missing.append(expected)

    return missing, running


def _append_timeline(recovery_result: Dict[str, Any], message: str, level: str = "INFO") -> None:
    """
    Append a timestamped progress entry to the recovery_result timeline.
    Stored as a list of {timestamp, level, message} for later ticketing.
    """
    try:
        ts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        entry = {"timestamp": ts, "level": level.upper(), "message": message}
        if 'timeline' not in recovery_result or not isinstance(recovery_result['timeline'], list):
            recovery_result['timeline'] = []
        recovery_result['timeline'].append(entry)
    except Exception:
        # Timeline recording should never break execution
        pass


def validate_and_parse_alert(**context) -> Dict[str, Any]:
    """
    Validate and parse the alert from the DAG trigger.
    Extract necessary information for recovery process.
    """
    worklog_hook = WorkLogHook()

    try:
        # Get alert from DAG conf
        conf = context.get('dag_run').conf
        if not conf:
            raise AirflowException("No configuration provided to DAG run")

        # Parse alert from conf
        alert_json = conf.get('alert')
        if not alert_json:
            raise AirflowException("No alert data in DAG configuration")

        # Parse the alert if it's a string
        if isinstance(alert_json, str):
            alert = json.loads(alert_json)
        else:
            alert = alert_json

        logger.info(f"Received alert: {json.dumps(alert, indent=2)}")

        # Create worklog for this recovery attempt
        alert_labels = alert.get('labels', {})
        # Use oscar_fingerprint from labels if available, otherwise use fingerPrint field
        alert_fingerprint = alert_labels.get('oscar_fingerprint') or alert.get('fingerPrint') or alert.get('fingerprint', 'unknown')
        alertname = alert_labels.get('alertname', 'unknown')

        # Extract target IP early for use in worklog name
        target_ip = alert_labels.get('meta_ipaddress', 'unknown')

        metadata = [
            {"key": "alert_fingerprint", "value": alert_fingerprint},
            {"key": "alertname", "value": alertname},
            {"key": "target_ip", "value": target_ip},
            {"key": "dag_id", "value": context['dag'].dag_id},
            {"key": "run_id", "value": context['run_id']},
            {"key": "triggered_by", "value": "alert_workflow"},
            {"key": "recovery_type", "value": "auto_restart"}
        ]

        worklog = worklog_hook.create_worklog(
            name=f"OSCAR Platform Recovery - {target_ip}",
            description=f"Automated recovery attempt for {alertname} alert (fingerprint: {alert_fingerprint})",
            worklog_type=WorkLogType.DB,
            metadata=metadata
        )

        worklog_id = worklog['id']
        logger.info(f"Created worklog with ID: {worklog_id}")

        # Log initial information with clearer format
        worklog_hook.info("="*60)
        worklog_hook.info(f"Alert Received: {alertname}")
        worklog_hook.info(f"Fingerprint: {alert_fingerprint}")
        worklog_hook.info(f"Target IP: {target_ip}")
        worklog_hook.info("="*60)
        worklog_hook.debug(f"Full alert details: {json.dumps(alert, indent=2)}")

        # Validate alert name
        alertname = alert_labels.get('alertname')
        if alertname != ALERT_NAME_TARGET:
            worklog_hook.error(f"Invalid alert name: {alertname}. Expected: {ALERT_NAME_TARGET}")
            raise AirflowSkipException(f"Alert name {alertname} does not match target {ALERT_NAME_TARGET}")

        worklog_hook.info(f"Alert validation successful: {alertname}")

        # Validate target IP address (already extracted above)
        if not target_ip or target_ip == 'unknown':
            worklog_hook.error("No meta_ipaddress found in alert labels")
            raise AirflowException("Missing meta_ipaddress in alert")

        # Extract additional metadata
        instance = alert_labels.get('instance', 'unknown')
        severity = alert_labels.get('severity', 'unknown')
        hostname = alert_labels.get('meta_hostname', 'unknown')
        datacenter = alert_labels.get('meta_datacenter', 'unknown')
        environment = alert_labels.get('meta_environment', 'unknown')
        annotations = alert.get('annotations', {}) or alert.get('specific_annotations', {})
        description = annotations.get('description', 'No description')
        summary = annotations.get('summary', 'No summary')

        worklog_hook.info("Alert Details:")
        worklog_hook.info(f"  Instance: {instance}")
        worklog_hook.info(f"  Hostname: {hostname}")
        worklog_hook.info(f"  Severity: {severity}")
        worklog_hook.info(f"  Datacenter: {datacenter}")
        worklog_hook.info(f"  Environment: {environment}")
        worklog_hook.info(f"  Summary: {summary}")
        worklog_hook.debug(f"  Description: {description}")

        # Extract incident information early (like in alert_workflow_test.py)
        incident_number = alert_labels.get('incident_number')
        ticket_id = alert_labels.get('ticket_id')
        create_ticket = alert_labels.get('create_ticket')  # System: remedy/elastic
        incident_severity = alert_labels.get('incident_severity')

        # Only show incident section if incident information is present
        if incident_number or ticket_id or create_ticket:
            worklog_hook.info("-"*60)
            worklog_hook.info("Incident Information:")
            if incident_number:
                worklog_hook.info(f"  Incident Number: {incident_number}")
            if incident_severity:
                worklog_hook.info(f"  Incident Severity: {incident_severity}")
            if ticket_id:
                worklog_hook.info(f"  Ticket ID: {ticket_id}")
            if create_ticket:
                worklog_hook.info(f"  Ticketing System: {create_ticket}")
        else:
            worklog_hook.debug("No incident information present in alert")

        # Add incident metadata to worklog if present
        if incident_number:
            worklog_hook.add_metadata({"incident_number": incident_number})
        if incident_severity:
            worklog_hook.add_metadata({"incident_severity": incident_severity})
        if ticket_id:
            worklog_hook.add_metadata({"ticket_id": ticket_id})
        if create_ticket:
            worklog_hook.add_metadata({"ticketing_system": create_ticket})

        # Store parsed data in XCom
        parsed_data = {
            'worklog_id': worklog_id,
            'alertname': alertname,
            'target_ip': target_ip,
            'hostname': hostname,
            'instance': instance,
            'severity': severity,
            'datacenter': datacenter,
            'environment': environment,
            'alert_fingerprint': alert_fingerprint,
            'description': description,
            'summary': summary,
            'incident_number': incident_number,
            'incident_severity': incident_severity,
            'ticket_id': ticket_id,
            'create_ticket': create_ticket,
            'alert': alert  # Store full alert for later use
        }

        context['ti'].xcom_push(key='parsed_alert', value=parsed_data)
        context['ti'].xcom_push(key='worklog_id', value=worklog_id)

        worklog_hook.info("Alert validation and parsing completed successfully")
        worklog_hook.info("-"*60)
        return parsed_data

    except Exception as e:
        logger.error(f"Error validating/parsing alert: {str(e)}")
        if 'worklog_hook' in locals():
            worklog_hook.error(f"Failed to validate/parse alert: {str(e)}")
            worklog_hook.close_worklog()
        raise


def get_connection_details(target_ip: str) -> Dict[str, Any]:
    """
    Get SSH connection details from Vault-backed Airflow Connection.
    Password authentication only, no key files.

    Connection naming convention:
    - Primary: oscar_ssh_{ip_with_underscores} (e.g., oscar_ssh_10_0_1_100)
    - Fallback: oscar_ssh_default

    Returns dict with host, user, password, port, oscar_home
    """
    # Try IP-specific connection first, then default
    conn_id_ip = f"oscar_ssh_{target_ip.replace('.', '_')}"

    for conn_id in [conn_id_ip, "oscar_ssh_default"]:
        try:
            connection = BaseHook.get_connection(conn_id)
            logger.info(f"Found SSH connection: {conn_id}")

            # Parse extra field for oscar_home if present
            extra = {}
            if connection.extra:
                try:
                    extra = connection.extra_dejson
                except Exception:
                    extra = {}

            return {
                'host': connection.host or target_ip,  # Use connection host or fallback to IP
                'user': connection.login or DEFAULT_SSH_USER,
                'password': connection.password,  # Required for auth
                'port': connection.port or DEFAULT_SSH_PORT,
                'oscar_home': extra.get('oscar_home', '/opt/oscar/app'),  # Default path
                'connection_id': conn_id,
                'connection_found': True
            }

        except Exception as e:
            logger.debug(f"Connection {conn_id} not found: {str(e)}")
            continue

    # No connection found - raise exception
    error_msg = f"No SSH connection found for {target_ip} (tried: {conn_id_ip}, oscar_ssh_default)"
    logger.error(error_msg)
    raise AirflowException(error_msg)


def check_ssh_connectivity(**context) -> str:
    """
    Check if we can establish SSH connection to the target server.
    Returns 'ssh_recovery' if successful, 'notify_failure' if not.
    """
    worklog_id = context['ti'].xcom_pull(key='worklog_id')
    parsed_alert = context['ti'].xcom_pull(key='parsed_alert')
    target_ip = parsed_alert['target_ip']

    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)

    worklog_hook.info("="*60)
    worklog_hook.info(f"SSH Connectivity Check")
    worklog_hook.info(f"Target: {parsed_alert.get('hostname', 'unknown')} ({target_ip})")
    worklog_hook.info("="*60)

    # Check if we're in STUB mode for testing
    if STUB_MODE:
        worklog_hook.warning("🔧 STUB MODE: Simulating successful SSH connectivity check")
        logger.warning("STUB MODE: Bypassing actual SSH connectivity check")

        # Create fake connection details for testing
        conn_details = {
            'host': target_ip,
            'user': 'oscar',
            'password': 'stub_password',
            'port': 22,
            'oscar_home': '/opt/oscar/app',
            'connection_id': 'stub_connection',
            'connection_found': True
        }

        worklog_hook.info("✓ STUB: SSH connectivity simulated as successful")
        worklog_hook.info(f"  STUB Connection: {conn_details['connection_id']}")
        worklog_hook.info(f"  STUB User: {conn_details['user']}")
        worklog_hook.info(f"  STUB Host: {conn_details['host']}")

        # Store connection details for recovery task
        context['ti'].xcom_push(key='connection_details', value=conn_details)
        context['ti'].xcom_push(key='actual_host', value=target_ip)

        return 'ssh_recovery'

    try:
        # Get SSH connection details from Vault
        conn_details = get_connection_details(target_ip)

        actual_host = conn_details['host']
        worklog_hook.info(f"Connection found: {conn_details['connection_id']}")
        worklog_hook.info(f"  User: {conn_details['user']}")
        worklog_hook.info(f"  Host: {actual_host}")
        worklog_hook.info(f"  Port: {conn_details['port']}")
        worklog_hook.info(f"  OSCAR Home: {conn_details['oscar_home']}")

        # Password-only authentication
        connect_kwargs = {
            'password': conn_details['password'],
            'timeout': SSH_TIMEOUT
        }

        # Attempt SSH connection
        conn = Connection(
            host=actual_host,
            user=conn_details['user'],
            port=conn_details['port'],
            connect_kwargs=connect_kwargs
        )

        # Test connection with simple command
        result = conn.run('echo "SSH connection test"', hide=True, timeout=SSH_TIMEOUT)

        if result.ok:
            worklog_hook.info("✓ SSH connectivity confirmed")
            worklog_hook.info("-"*60)

            # Store connection details in XCom for downstream tasks
            context['ti'].xcom_push(key='connection_details', value=conn_details)
            context['ti'].xcom_push(key='actual_host', value=actual_host)
            context['ti'].xcom_push(key='target_ip', value=target_ip)

            conn.close()
            return 'ssh_recovery'
        else:
            worklog_hook.error(f"✗ SSH test command failed: {result.stderr}")
            worklog_hook.error("-"*60)
            conn.close()
            return 'notify_failure'

    except AirflowException as e:
        # Connection not found error
        worklog_hook.error(f"✗ Connection configuration error: {str(e)}")
        worklog_hook.error("-"*60)
        return 'notify_failure'
    except (SSHException, NoValidConnectionsError) as e:
        worklog_hook.error(f"✗ SSH connection failed: {str(e)}")
        worklog_hook.error("-"*60)
        return 'notify_failure'
    except Exception as e:
        worklog_hook.error(f"✗ Unexpected error: {str(e)}")
        worklog_hook.error("-"*60)
        return 'notify_failure'


def execute_ssh_recovery(**context) -> Dict[str, Any]:
    """
    Execute the OSCAR platform recovery via SSH.
    """
    worklog_id = context['ti'].xcom_pull(key='worklog_id')
    parsed_alert = context['ti'].xcom_pull(key='parsed_alert')
    conn_details = context['ti'].xcom_pull(key='connection_details')
    target_ip = parsed_alert['target_ip']
    actual_host = context['ti'].xcom_pull(key='actual_host') or conn_details['host']

    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)

    # Note: Container preprocessing will happen after SSH connection is established
    # so we can detect the remote platform architecture

    # Log to both WorkLog and Airflow
    worklog_hook.info("="*60)
    logger.info("="*60)
    worklog_hook.info(f"OSCAR Platform Recovery")
    logger.info(f"OSCAR Platform Recovery")
    worklog_hook.info(f"Hostname: {parsed_alert.get('hostname', 'unknown')}")
    logger.info(f"Hostname: {parsed_alert.get('hostname', 'unknown')}")
    worklog_hook.info(f"IP Address: {actual_host}")
    logger.info(f"IP Address: {actual_host}")
    worklog_hook.info(f"Datacenter: {parsed_alert.get('datacenter', 'unknown')}")
    logger.info(f"Datacenter: {parsed_alert.get('datacenter', 'unknown')}")
    worklog_hook.info(f"Environment: {parsed_alert.get('environment', 'unknown')}")
    logger.info(f"Environment: {parsed_alert.get('environment', 'unknown')}")
    worklog_hook.info("="*60)
    logger.info("="*60)

    # Check if we're in STUB mode for testing
    if STUB_MODE:
        worklog_hook.warning("🔧 STUB MODE: Simulating successful recovery without actual SSH connection")
        logger.warning("STUB MODE: Bypassing actual SSH recovery")

        # Simulate a successful recovery
        recovery_result = {
            'success': True,
            'target_ip': target_ip,
            'actual_host': actual_host,
            'steps_completed': ['status_check', 'stop_services', 'start_services', 'verify_running', 'verify_components'],
            'error': None,
            'service_status': 'running',
            'timeline': [
                {'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 'level': 'INFO', 'message': 'STUB: Checking current OSCAR service status'},
                {'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 'level': 'INFO', 'message': 'STUB: Detected services down, initiating recovery'},
                {'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 'level': 'INFO', 'message': 'STUB: Stopping OSCAR services'},
                {'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 'level': 'INFO', 'message': 'STUB: Starting OSCAR services'},
                {'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 'level': 'INFO', 'message': 'STUB: Verification succeeded - all services running'}
            ]
        }

        worklog_hook.info("STUB: Simulated recovery completed successfully")
        worklog_hook.info(f"STUB: Steps completed: {', '.join(recovery_result['steps_completed'])}")
        worklog_hook.info("STUB: Service status: running")
        logger.info(f"STUB MODE: Recovery simulated - success={recovery_result['success']}, status={recovery_result['service_status']}")

        # Store result in XCom and return immediately
        context['ti'].xcom_push(key='recovery_result', value=recovery_result)
        return recovery_result

    # Initialize recovery result for real execution
    recovery_result = {
        'success': False,
        'target_ip': target_ip,
        'actual_host': actual_host,
        'steps_completed': [],
        'error': None,
        'service_status': None,
        'timeline': []
    }

    conn = None
    try:
        # Password-only authentication
        connect_kwargs = {
            'password': conn_details['password'],
            'timeout': SSH_TIMEOUT
        }

        # Establish SSH connection
        conn = Connection(
            host=actual_host,
            user=conn_details['user'],
            port=conn_details['port'],
            connect_kwargs=connect_kwargs
        )

        # Use oscar_home from connection
        oscar_home = conn_details['oscar_home']
        worklog_hook.info(f"OSCAR Installation Path: {oscar_home}")
        logger.info(f"OSCAR Installation Path: {oscar_home}")

        # Detect remote platform architecture before preprocessing containers
        remote_platform = _detect_remote_platform(conn)
        worklog_hook.info(f"Remote Platform: {remote_platform}")
        logger.info(f"Remote Platform: {remote_platform}")

        # Preprocess expected containers using REMOTE platform (not source platform)
        expected_containers = _preprocess_required_containers(
            MONITOR_REQUIRED_CONTAINERS_JSON,
            NAMESPACE,
            remote_platform  # Use detected remote platform, not PLATFORM_ARCH env var
        )
        worklog_hook.info(f"Monitoring {len(expected_containers)} critical containers")
        logger.info(f"Monitoring {len(expected_containers)} critical containers (after namespace/platform preprocessing)")
        logger.debug(f"Expected containers: {expected_containers[:5]}... (showing first 5)")

        worklog_hook.info("-"*60)
        logger.info("-"*60)

        # Step 1: Check current OSCAR status (try simple format first for easier parsing)
        worklog_hook.info("Step 1: Checking current OSCAR service status")
        logger.info("Step 1: Checking current OSCAR service status")
        _append_timeline(recovery_result, "Checking current OSCAR service status")
        # Try --simple flag, but don't fail if it's not supported
        status_cmd = f"cd {oscar_home} && ./oscar status --simple 2>/dev/null || ./oscar status"
        logger.info(f"Executing command: {status_cmd}")
        result = conn.run(status_cmd, hide=True, timeout=COMMAND_TIMEOUT, warn=True)

        worklog_hook.debug(f"Status check output: {result.stdout}")
        logger.debug(f"Status check output: {result.stdout}")
        if result.stderr:
            worklog_hook.debug(f"Status check stderr: {result.stderr}")
            logger.debug(f"Status check stderr: {result.stderr}")

        recovery_result['steps_completed'].append('status_check')
        _append_timeline(recovery_result, "Collected pre-recovery status output")
        worklog_hook.info(f"Initial status: {'Running' if result.ok else 'Not running/Error'}")
        logger.info(f"Initial status: {'Running' if result.ok else 'Not running/Error'}")

        # Parse the simple status output to find down services
        down_services, up_services = _parse_simple_status(result.stdout or "")
        worklog_hook.info(f"Service status - Up: {len(up_services)}, Down: {len(down_services)}")
        logger.info(f"Service status - Up: {len(up_services)}, Down: {len(down_services)}")
        if down_services:
            worklog_hook.error(f"Detected services with no running containers: {', '.join(down_services)}")
            logger.error(f"Detected services with no running containers: {', '.join(down_services)}")
            _append_timeline(recovery_result, f"Detected down sections: {', '.join(down_services)}", level="WARN")
            # Log only the headings in worklog for each empty section
            for svc in down_services:
                worklog_hook.error(f"Checking status of {svc}...")
                logger.error(f"Service down: {svc}")
        else:
            worklog_hook.info("All service sections have rows; no immediate down sections detected")
            logger.info("All service sections have rows; no immediate down sections detected")

        # Also check individual containers (aligns with oscar-monitor service)
        worklog_hook.info("Checking individual critical containers...")
        logger.info("Checking individual critical containers...")
        missing_containers, running_containers = _check_critical_containers_running(
            conn, oscar_home, expected_containers
        )
        worklog_hook.info(f"Container status - Running: {len(running_containers)}/{len(expected_containers)}, Missing: {len(missing_containers)}")
        logger.info(f"Container status - Running: {len(running_containers)}/{len(expected_containers)}, Missing: {len(missing_containers)}")

        if missing_containers:
            # Show first 10 missing containers to avoid log spam
            missing_sample = missing_containers[:10]
            more_text = f" (+{len(missing_containers) - 10} more)" if len(missing_containers) > 10 else ""
            worklog_hook.error(f"Missing critical containers: {', '.join(missing_sample)}{more_text}")
            logger.error(f"Missing critical containers: {', '.join(missing_sample)}{more_text}")
            _append_timeline(recovery_result, f"Detected {len(missing_containers)} missing containers", level="WARN")
        else:
            worklog_hook.info("All critical containers are running")
            logger.info("All critical containers are running")

        # Trigger recovery if either:
        # 1. Critical service sections are down, OR
        # 2. Critical containers are missing
        critical_down = [s for s in down_services if s not in NON_CRITICAL_SECTIONS]
        logger.info(f"Critical services down: {critical_down}")
        logger.info(f"Non-critical sections configured: {NON_CRITICAL_SECTIONS}")

        should_recover = len(critical_down) >= 1 or len(missing_containers) >= 1

        if should_recover:
            # Step 2: Stop OSCAR services (clean stop before restart)
            recovery_reason = []
            if critical_down:
                recovery_reason.append(f"{len(critical_down)} critical sections down")
            if missing_containers:
                recovery_reason.append(f"{len(missing_containers)} containers missing")
            reason_str = ", ".join(recovery_reason)

            worklog_hook.info(f"Step 2: Stopping OSCAR services ({reason_str})")
            logger.info(f"Step 2: Stopping OSCAR services ({reason_str})")
            _append_timeline(recovery_result, f"Stopping OSCAR services (critical down: {', '.join(critical_down)})")
            stop_cmd = f"cd {oscar_home} && ./oscar stop"
            logger.info(f"Executing stop command: {stop_cmd}")
            logger.info(f"Timeout set to {COMMAND_TIMEOUT} seconds")
            result = conn.run(stop_cmd, hide=True, timeout=COMMAND_TIMEOUT, warn=True)

            if result.ok:
                worklog_hook.info("OSCAR services stopped successfully")
                logger.info("OSCAR services stopped successfully")
                recovery_result['steps_completed'].append('stop_services')
                _append_timeline(recovery_result, "OSCAR services stopped successfully")
            else:
                worklog_hook.warning(f"Stop command completed with warnings: {result.stderr}")
                logger.warning(f"Stop command completed with warnings: {result.stderr}")
                _append_timeline(recovery_result, f"Stop completed with warnings: {result.stderr}", level="WARN")

            # Confirm all stopped by checking status once (stop is blocking)
            worklog_hook.info("Confirming all services are stopped via status")
            logger.info("Confirming all services are stopped via status")
            confirm_stop_cmd = f"cd {oscar_home} && ./oscar status --simple 2>/dev/null || ./oscar status"
            logger.info(f"Executing confirmation command: {confirm_stop_cmd}")
            confirm_result = conn.run(confirm_stop_cmd, hide=True, timeout=COMMAND_TIMEOUT, warn=True)

            # Parse simple status to check if all services are down
            down_after_stop, up_after_stop = _parse_simple_status(confirm_result.stdout or "")
            logger.info(f"After stop - Up: {len(up_after_stop)}, Down: {len(down_after_stop)}")

            if len(up_after_stop) == 0:
                worklog_hook.info("All services confirmed as stopped")
                logger.info("All services confirmed as stopped")
                recovery_result['steps_completed'].append('confirm_stop')
                _append_timeline(recovery_result, "Confirmed all services stopped")
            else:
                worklog_hook.warning("Some services may not have fully stopped yet")
                logger.warning(f"Some services may not have fully stopped yet. Still up: {up_after_stop}")
                _append_timeline(recovery_result, "Services may not have fully stopped yet", level="WARN")

            # Step 3: Start OSCAR services (blocking; start script itself orchestrates all services)
            worklog_hook.info("Step 3: Starting OSCAR services (this may take several minutes)")
            logger.info("Step 3: Starting OSCAR services")
            logger.info(f"Note: Start timeout is set to {START_TIMEOUT} seconds ({START_TIMEOUT/60:.1f} minutes)")
            _append_timeline(recovery_result, "Starting OSCAR services")
            start_cmd = f"cd {oscar_home} && ./oscar start"
            logger.info(f"Executing start command: {start_cmd}")
            logger.info("Waiting for services to start... This may take several minutes.")
            # Allow a longer timeout for the start orchestration
            result = conn.run(start_cmd, hide=True, timeout=START_TIMEOUT, warn=True)

            if not result.ok:
                worklog_hook.error(f"Start command failed: {result.stderr}")
                logger.error(f"Start command failed with exit code: {result.return_code}")
                logger.error(f"stderr: {result.stderr}")
                recovery_result['error'] = f"Start failed: {result.stderr}"
                raise Exception(f"Failed to start OSCAR services: {result.stderr}")

            worklog_hook.info("OSCAR start command completed")
            logger.info("OSCAR start command completed successfully")
            recovery_result['steps_completed'].append('start_services')
            _append_timeline(recovery_result, "OSCAR start command completed")

        # Step 4: Verify OSCAR is running using status check
        worklog_hook.info("Step 4: Verifying OSCAR services are running")
        logger.info("Step 4: Verifying OSCAR services are running")
        verify_cmd = f"cd {oscar_home} && ./oscar status --simple 2>/dev/null || ./oscar status"
        logger.info(f"Executing verification command: {verify_cmd}")
        res_verify = conn.run(verify_cmd, hide=True, timeout=COMMAND_TIMEOUT, warn=True)

        # Parse simple status to find any services still down
        last_down, last_up = _parse_simple_status(res_verify.stdout or "")
        worklog_hook.info(f"Final service status - Up: {len(last_up)}, Down: {len(last_down)}")
        logger.info(f"Final service status - Up: {len(last_up)}, Down: {len(last_down)}")

        if last_up:
            logger.info(f"Services UP: {', '.join(last_up)}")
        if last_down:
            logger.warning(f"Services DOWN: {', '.join(last_down)}")

        # Also verify individual containers after recovery
        worklog_hook.info("Verifying all critical containers are running...")
        logger.info("Verifying all critical containers are running...")
        final_missing_containers, final_running_containers = _check_critical_containers_running(
            conn, oscar_home, expected_containers
        )
        worklog_hook.info(f"Post-recovery container status - Running: {len(final_running_containers)}/{len(expected_containers)}, Missing: {len(final_missing_containers)}")
        logger.info(f"Post-recovery container status - Running: {len(final_running_containers)}/{len(expected_containers)}, Missing: {len(final_missing_containers)}")

        if final_missing_containers:
            missing_sample = final_missing_containers[:10]
            more_text = f" (+{len(final_missing_containers) - 10} more)" if len(final_missing_containers) > 10 else ""
            worklog_hook.warning(f"Missing containers after recovery: {', '.join(missing_sample)}{more_text}")
            logger.warning(f"Missing containers after recovery: {', '.join(missing_sample)}{more_text}")
            _append_timeline(recovery_result, f"Still missing {len(final_missing_containers)} containers after recovery", level="WARN")
        else:
            worklog_hook.info("✓ All critical containers verified as running")
            logger.info("✓ All critical containers verified as running")
            _append_timeline(recovery_result, "All critical containers verified running")

        # Determine overall success based on BOTH section status AND container status
        sections_ok = len(last_down) == 0
        containers_ok = len(final_missing_containers) == 0

        if sections_ok and containers_ok:
            worklog_hook.info("OSCAR platform verified as fully operational (sections + containers)")
            logger.info("✓ OSCAR platform verified as fully operational (sections + containers)")
            recovery_result['steps_completed'].append('verify_running')
            recovery_result['service_status'] = 'running'
            recovery_result['success'] = True
            _append_timeline(recovery_result, "Verification succeeded: all sections Up, all containers running")
        elif sections_ok and not containers_ok:
            worklog_hook.warning(f"Sections Up but {len(final_missing_containers)} containers still missing")
            logger.warning(f"⚠ Sections Up but {len(final_missing_containers)} containers still missing")
            recovery_result['service_status'] = 'partial'
            recovery_result['success'] = False
            recovery_result['error'] = f"Services started but {len(final_missing_containers)} containers missing"
            _append_timeline(recovery_result, f"Partial recovery: sections OK but {len(final_missing_containers)} containers missing", level="WARN")
        elif not sections_ok and containers_ok:
            worklog_hook.warning(f"Containers running but {len(last_down)} sections still Down")
            logger.warning(f"⚠ Containers running but {len(last_down)} sections still Down: {', '.join(last_down)}")
            recovery_result['service_status'] = 'partial'
            recovery_result['success'] = False
            recovery_result['error'] = f"Services started but {len(last_down)} sections still Down"
            _append_timeline(recovery_result, f"Partial recovery: containers OK but {len(last_down)} sections Down", level="WARN")
        else:
            worklog_hook.error(f"Both sections ({len(last_down)}) and containers ({len(final_missing_containers)}) have issues")
            logger.error(f"⚠ Both sections and containers have issues - Sections Down: {', '.join(last_down)}, Containers Missing: {len(final_missing_containers)}")
            recovery_result['service_status'] = 'failed'
            recovery_result['success'] = False
            recovery_result['error'] = f"Recovery incomplete: {len(last_down)} sections Down, {len(final_missing_containers)} containers missing"
            _append_timeline(recovery_result, f"Recovery failed: sections and containers both have issues", level="ERROR")

        worklog_hook.debug(f"Final status output: {res_verify.stdout}")
        logger.debug(f"Final status output length: {len(res_verify.stdout) if res_verify.stdout else 0} chars")

        # Step 5: Log all running containers for audit trail
        worklog_hook.info("Step 5: Recording all running containers for audit trail")
        logger.info("Step 5: Recording all running containers for audit trail")
        check_cmd = f"cd {oscar_home} && docker ps --format 'table {{{{.Names}}}}\\t{{{{.Image}}}}\\t{{{{.Status}}}}' | head -30"
        logger.info(f"Executing container listing command")

        result = conn.run(check_cmd, hide=True, timeout=COMMAND_TIMEOUT, warn=True)

        if result.ok and result.stdout:
            container_lines = result.stdout.strip().split('\n')
            worklog_hook.info(f"Container audit - {len(container_lines)-1} containers running:")
            logger.info(f"Container audit - {len(container_lines)-1} containers running")
            # Log first 20 lines to worklog for audit
            for line in container_lines[:20]:
                worklog_hook.debug(line)
            if len(container_lines) > 20:
                worklog_hook.debug(f"... and {len(container_lines)-20} more containers")
            recovery_result['steps_completed'].append('audit_containers')
        else:
            logger.warning("Container listing returned no output or failed")
            if result.stderr:
                logger.warning(f"stderr: {result.stderr}")

        worklog_hook.info("="*60)
        logger.info("="*60)
        if recovery_result['success']:
            worklog_hook.info("✓ Recovery SUCCEEDED")
            logger.info("✓ Recovery SUCCEEDED")
        else:
            worklog_hook.warning("⚠ Recovery completed with warnings")
            logger.warning("⚠ Recovery completed with warnings")
        worklog_hook.info("="*60)
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Recovery execution failed: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        worklog_hook.error(f"Recovery failed: {str(e)}")
        recovery_result['error'] = str(e)
        logger.info(f"Steps completed before failure: {recovery_result.get('steps_completed', [])}")

    finally:
        if conn:
            logger.info("Closing SSH connection")
            conn.close()

        # Store result in XCom
        logger.info(f"Recovery result summary: success={recovery_result.get('success')}, status={recovery_result.get('service_status')}, steps={recovery_result.get('steps_completed')}")
        context['ti'].xcom_push(key='recovery_result', value=recovery_result)

    return recovery_result


def update_incident_ticket(**context):
    """
    Update incident ticket with recovery results.
    This runs BEFORE notifications so ticket status can be included in the notification.
    Non-critical task - failures are logged but don't stop the workflow.
    """
    worklog_id = context['ti'].xcom_pull(key='worklog_id')
    parsed_alert = context['ti'].xcom_pull(key='parsed_alert')
    recovery_result = context['ti'].xcom_pull(key='recovery_result', task_ids='ssh_recovery')

    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)

    logger.info(f"Starting ticket update for worklog {worklog_id}")

    # Initialize ticket update result
    ticket_update_result = {
        'updated': False,
        'ticket_id': None,
        'system': None,
        'message': 'No ticket to update'
    }

    # Check if we have incident information
    system = parsed_alert.get('create_ticket')
    incident_no = parsed_alert.get('incident_number')

    if not system or not incident_no:
        worklog_hook.info("No ticket information found (create_ticket/incident_number labels missing)")
        logger.info(f"Ticket update skipped: system={system}, incident_no={incident_no}")
        context['ti'].xcom_push(key='ticket_update_result', value=ticket_update_result)
        return ticket_update_result

    # Normalize system name
    sys_norm = system.strip().lower()
    if sys_norm not in ("elastic", "remedy"):
        sys_norm = "remedy"  # Default to remedy if unknown
        worklog_hook.info(f"Unknown ticket system '{system}', defaulting to Remedy")

    ticket_update_result['system'] = sys_norm.upper()

    # Build recovery summary for ticket update
    hostname = parsed_alert.get('hostname', parsed_alert['target_ip'])

    # Determine overall status
    if recovery_result and recovery_result.get('success'):
        recovery_status = "SUCCESS"
        status_emoji = "✅"
    elif recovery_result and recovery_result.get('service_status') == 'partial':
        recovery_status = "PARTIAL"
        status_emoji = "⚠️"
    else:
        recovery_status = "FAILED"
        status_emoji = "❌"

    # Build comprehensive summary
    summary_lines = [
        f"{status_emoji} OSCAR Auto-Recovery Summary",
        "="*40,
        f"Recovery Status: {recovery_status}",
        f"Target: {hostname} ({parsed_alert['target_ip']})",
        f"Alert: {parsed_alert.get('alertname', 'unknown')}",
        f"Fingerprint: {parsed_alert.get('alert_fingerprint', 'unknown')}",
        ""
    ]

    if recovery_result:
        # Add steps completed
        steps = recovery_result.get('steps_completed', [])
        if steps:
            summary_lines.append(f"Actions Completed: {', '.join(steps)}")

        # Add service status
        summary_lines.append(f"Final Service Status: {recovery_result.get('service_status', 'unknown')}")

        # Add error if present
        if recovery_result.get('error'):
            summary_lines.append(f"Error: {recovery_result['error']}")
    else:
        summary_lines.append("Recovery was not attempted (SSH connectivity failed)")

    summary_lines.extend([
        "",
        f"WorkLog ID: {worklog_id}",
        f"DAG Run: {context['run_id']}",
        f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "For detailed logs, refer to the WorkLog ID above."
    ])

    ticket_summary = "\n".join(summary_lines)

    # Attempt to update the ticket
    try:
        hook_t = TicketingHook()
        ticket_id = None

        if sys_norm == 'elastic':
            # For Elastic, incident number IS the ticket ID
            ticket_id = incident_no
            worklog_hook.info(f"Using Elastic ticket ID: {ticket_id}")
        else:
            # For Remedy, search by incident number to get ticket ID
            worklog_hook.info(f"Searching Remedy for incident: {incident_no}")
            try:
                results = hook_t.search_tickets(system='REMEDY', incident_no=incident_no, page=1, per_page=1)
                if isinstance(results, list) and results:
                    ticket_id = results[0].get('id')
                    worklog_hook.info(f"Found Remedy ticket ID: {ticket_id}")
                else:
                    worklog_hook.warning(f"No Remedy ticket found for incident {incident_no}")
            except Exception as e:
                worklog_hook.warning(f"Failed to search Remedy tickets: {str(e)}")
                ticket_id = None

        if ticket_id:
            # Update the ticket with recovery summary as a comment
            # Using add_comment field (standard across all ticketing systems)
            worklog_hook.info(f"Updating {sys_norm.upper()} ticket {ticket_id} with recovery summary")
            logger.info(f"Updating ticket: system={sys_norm.upper()}, id={ticket_id}, incident={incident_no}")

            hook_t.update_ticket(
                ticket_id=ticket_id,
                system=sys_norm.upper(),
                ticket_update={
                    "name": parsed_alert.get('alertname', 'OscarPlatformUnhealthy'),
                    "add_comment": ticket_summary
                }
            )

            ticket_update_result['updated'] = True
            ticket_update_result['ticket_id'] = ticket_id
            ticket_update_result['message'] = f"Successfully updated {sys_norm.upper()} ticket {ticket_id}"

            worklog_hook.info(f"✓ Successfully updated {sys_norm.upper()} ticket {ticket_id} for incident {incident_no}")
            logger.info(f"Ticket update successful: {ticket_update_result['message']}")

        else:
            ticket_update_result['message'] = f"Could not find ticket ID for incident {incident_no} in {sys_norm.upper()}"
            worklog_hook.warning(ticket_update_result['message'])

    except Exception as e:
        error_msg = f"Failed to update ticket: {str(e)}"
        ticket_update_result['message'] = error_msg
        worklog_hook.warning(error_msg)
        logger.error(f"Ticket update failed: {str(e)}", exc_info=True)

    # Store result for notification task
    context['ti'].xcom_push(key='ticket_update_result', value=ticket_update_result)

    # Add ticket update status to worklog metadata
    worklog_hook.add_metadata({
        "ticket_updated": str(ticket_update_result['updated']),
        "ticket_system": ticket_update_result.get('system', 'N/A'),
        "ticket_id": ticket_update_result.get('ticket_id', 'N/A')
    })

    logger.info(f"Ticket update completed: updated={ticket_update_result['updated']}, system={ticket_update_result.get('system')}, id={ticket_update_result.get('ticket_id')}")

    return ticket_update_result


def notify_recovery_status(**context):
    """
    Send notification about the recovery attempt status.
    Includes ticket update status if available.
    """
    worklog_id = context['ti'].xcom_pull(key='worklog_id')
    parsed_alert = context['ti'].xcom_pull(key='parsed_alert')
    recovery_result = context['ti'].xcom_pull(key='recovery_result', task_ids='ssh_recovery')
    ticket_update_result = context['ti'].xcom_pull(key='ticket_update_result', task_ids='update_incident_ticket')

    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    notify_hook = NotifyHook()

    # Determine notification details based on result
    hostname = parsed_alert.get('hostname', parsed_alert['target_ip'])

    # More nuanced status handling for partial recovery
    if recovery_result and recovery_result.get('success'):
        status = "SUCCESSFUL"
        status_emoji = "✅"
        status_color = "#28a745"  # Green
        subject = f"✅ OSCAR Platform Recovery Successful - {hostname}"
        severity = "info"
        worklog_hook.info("Sending success notification")
    elif recovery_result and recovery_result.get('service_status') == 'partial':
        # PARTIAL recovery - some services still down
        status = "PARTIAL"
        status_emoji = "⚠️"
        status_color = "#ffc107"  # Yellow/Orange
        subject = f"⚠️ OSCAR Platform PARTIALLY Recovered - {hostname}"
        severity = "warning"
        worklog_hook.warning("Sending partial recovery notification")
    else:
        status = "FAILED"
        status_emoji = "❌"
        status_color = "#dc3545"  # Red
        subject = f"❌ OSCAR Platform Recovery Failed - {hostname}"
        severity = "critical"
        worklog_hook.error("Sending failure notification")

    # Load and render HTML template
    try:
        if template_env is None:
            raise Exception("Template environment not initialized")

        template = template_env.get_template("platform_recovery_notification.j2")
        message = template.render(
            status=status,
            status_emoji=status_emoji,
            status_color=status_color,
            alert=parsed_alert,
            recovery_result=recovery_result,
            ticket_update=ticket_update_result,
            worklog_id=worklog_id,
            dag_run_id=context['run_id'],
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        )

        worklog_hook.debug(f"Rendered HTML email template successfully")

    except Exception as e:
        # Fallback to plain text if template rendering fails
        logger.error(f"Failed to render HTML template: {str(e)}")
        worklog_hook.error(f"Failed to render HTML template, falling back to plain text: {str(e)}")

        # Build fallback plain text message
        message_lines = [
            f"Alert: {parsed_alert.get('alertname', 'unknown')}",
            f"Fingerprint: {parsed_alert.get('alert_fingerprint', 'unknown')}",
            f"Hostname: {parsed_alert.get('hostname', 'unknown')}",
            f"IP Address: {parsed_alert['target_ip']}",
            f"Instance: {parsed_alert.get('instance', 'unknown')}",
            f"Severity: {parsed_alert.get('severity', 'unknown')}",
            f"Datacenter: {parsed_alert.get('datacenter', 'unknown')}",
            f"Environment: {parsed_alert.get('environment', 'unknown')}",
            "",
            "Recovery Attempt Details:",
        ]

        if recovery_result:
            message_lines.append(f"- Success: {recovery_result.get('success', False)}")
            message_lines.append(f"- Service Status: {recovery_result.get('service_status', 'unknown')}")
            message_lines.append(f"- Steps Completed: {', '.join(recovery_result.get('steps_completed', []))}")

            if recovery_result.get('error'):
                message_lines.append(f"- Error: {recovery_result['error']}")

            if recovery_result.get('service_status') == 'partial':
                message_lines.extend([
                    "",
                    "⚠️ WARNING: Some services are still not running properly.",
                    "Manual intervention may be required to fully restore services."
                ])
        else:
            message_lines.append("- Recovery was not attempted (SSH connectivity failed)")

        if ticket_update_result:
            message_lines.extend([
                "",
                "Ticket Update Status:"
            ])

            if ticket_update_result.get('updated'):
                message_lines.append(f"✓ {ticket_update_result['message']}")
            else:
                message_lines.append(f"ℹ {ticket_update_result.get('message', 'No ticket update performed')}")

        message_lines.extend([
            "",
            f"Worklog ID: {worklog_id}",
            f"DAG Run: {context['run_id']}",
            f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        ])

        message = "\\n".join(message_lines)

    try:
        # Send notification via configured notifier
        response = notify_hook.send_notification({
            "name": NOTIFIER_NAME,  # Configurable notifier
            "subject": subject,
            "message": message,
            "severity": severity,
            "notifier_id": NOTIFIER_GROUP_ID,  # Configurable notification group
            "metadata": {
                "alert_fingerprint": parsed_alert.get('alert_fingerprint'),
                "target_ip": parsed_alert['target_ip'],
                "worklog_id": worklog_id,
                "recovery_success": str(recovery_result.get('success', False)) if recovery_result else "False"
            }
        })

        worklog_hook.info(f"Notification sent successfully: {response}")

    except Exception as e:
        logger.error(f"Failed to send notification: {str(e)}")
        worklog_hook.error(f"Failed to send notification: {str(e)}")
        # Continue to close worklog even if notification fails

    finally:
        # ALWAYS close the worklog, regardless of success or failure
        worklog_hook.info("Recovery workflow completed")
        worklog_hook.close_worklog()
        logger.info(f"WorkLog {worklog_id} closed")


def notify_failure(**context):
    """
    Send notification when SSH connectivity fails.
    Also attempts to update the incident ticket with failure details.
    """
    worklog_id = context['ti'].xcom_pull(key='worklog_id')
    parsed_alert = context['ti'].xcom_pull(key='parsed_alert')

    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    notify_hook = NotifyHook()

    worklog_hook.error("SSH connectivity failed - sending failure notification")

    # First, try to update the ticket with failure information
    ticket_update_result = {
        'updated': False,
        'ticket_id': None,
        'system': None,
        'message': 'No ticket to update'
    }

    system = parsed_alert.get('create_ticket')
    incident_no = parsed_alert.get('incident_number')

    if system and incident_no:
        # Normalize system name
        sys_norm = system.strip().lower()
        if sys_norm not in ("elastic", "remedy"):
            sys_norm = "remedy"

        ticket_update_result['system'] = sys_norm.upper()

        # Build failure summary for ticket
        hostname = parsed_alert.get('hostname', parsed_alert['target_ip'])
        failure_summary = [
            "❌ OSCAR Auto-Recovery FAILED - SSH Unreachable",
            "="*40,
            f"Recovery Status: FAILED",
            f"Failure Reason: Unable to establish SSH connection",
            f"Target: {hostname} ({parsed_alert['target_ip']})",
            f"Alert: {parsed_alert.get('alertname', 'unknown')}",
            f"Fingerprint: {parsed_alert.get('alert_fingerprint', 'unknown')}",
            "",
            "Manual intervention required:",
            "1. Check network connectivity to the server",
            "2. Verify SSH service is running on the target",
            "3. Confirm SSH credentials are correct",
            "4. Manually restart OSCAR services if needed",
            "",
            f"WorkLog ID: {worklog_id}",
            f"DAG Run: {context['run_id']}",
            f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "",
            "Automated recovery could not proceed due to connectivity issues."
        ]

        ticket_summary = "\n".join(failure_summary)

        try:
            hook_t = TicketingHook()
            ticket_id = None

            if sys_norm == 'elastic':
                ticket_id = incident_no
            else:
                # For Remedy, search by incident number
                try:
                    results = hook_t.search_tickets(system='REMEDY', incident_no=incident_no, page=1, per_page=1)
                    if isinstance(results, list) and results:
                        ticket_id = results[0].get('id')
                except Exception as e:
                    worklog_hook.warning(f"Failed to search Remedy tickets: {str(e)}")

            if ticket_id:
                # Update ticket with failure details as a comment
                # Using add_comment field (standard across all ticketing systems)
                hook_t.update_ticket(
                    ticket_id=ticket_id,
                    system=sys_norm.upper(),
                    ticket_update={
                        "name": parsed_alert.get('alertname', 'OscarPlatformUnhealthy'),
                        "add_comment": ticket_summary
                    }
                )

                ticket_update_result['updated'] = True
                ticket_update_result['ticket_id'] = ticket_id
                ticket_update_result['message'] = f"Updated {sys_norm.upper()} ticket {ticket_id} with failure details"
                worklog_hook.info(f"✓ Updated {sys_norm.upper()} ticket {ticket_id} with SSH failure details")

        except Exception as e:
            worklog_hook.warning(f"Failed to update ticket: {str(e)}")

    hostname = parsed_alert.get('hostname', parsed_alert['target_ip'])
    subject = f"🔴 OSCAR Platform Recovery Failed - SSH Unreachable - {hostname}"

    # Load and render HTML template
    try:
        if template_env is None:
            raise Exception("Template environment not initialized")

        template = template_env.get_template("platform_recovery_ssh_failure.j2")
        message = template.render(
            alert=parsed_alert,
            ticket_update=ticket_update_result,
            worklog_id=worklog_id,
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        )

        worklog_hook.debug(f"Rendered HTML email template for SSH failure successfully")

    except Exception as e:
        # Fallback to plain text if template rendering fails
        logger.error(f"Failed to render HTML template: {str(e)}")
        worklog_hook.error(f"Failed to render HTML template, falling back to plain text: {str(e)}")

        message_lines = [
            "CRITICAL: Unable to establish SSH connection to recover OSCAR platform",
            "",
            f"Alert: {parsed_alert.get('alertname', 'unknown')}",
            f"Fingerprint: {parsed_alert.get('alert_fingerprint', 'unknown')}",
            f"Hostname: {parsed_alert.get('hostname', 'unknown')}",
            f"IP Address: {parsed_alert['target_ip']}",
            f"Instance: {parsed_alert.get('instance', 'unknown')}",
            f"Datacenter: {parsed_alert.get('datacenter', 'unknown')}",
            f"Environment: {parsed_alert.get('environment', 'unknown')}",
            "",
            "Manual intervention required:",
            "1. Check network connectivity to the server",
            "2. Verify SSH service is running on the target",
            "3. Confirm SSH credentials are correct in Airflow Connections",
            "4. Manually restart OSCAR services if needed",
            ""
        ]

        # Add ticket update status
        if ticket_update_result.get('updated'):
            message_lines.append(f"✓ Ticket Update: {ticket_update_result['message']}")
        elif system and incident_no:
            message_lines.append(f"ℹ Ticket Update: {ticket_update_result.get('message', 'Failed to update ticket')}")

        message_lines.extend([
            "",
            f"Worklog ID: {worklog_id}",
            f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        ])

        message = "\\n".join(message_lines)

    try:
        response = notify_hook.send_notification({
            "name": NOTIFIER_NAME,
            "subject": subject,
            "message": message,
            "severity": "critical",
            "notifier_id": NOTIFIER_GROUP_ID,  # Configurable notification group
            "metadata": {
                "alert_fingerprint": parsed_alert.get('alert_fingerprint'),
                "target_ip": parsed_alert['target_ip'],
                "worklog_id": worklog_id,
                "failure_reason": "ssh_unreachable"
            }
        })

        worklog_hook.info(f"Failure notification sent: {response}")

    except Exception as e:
        logger.error(f"Failed to send failure notification: {str(e)}")
        worklog_hook.error(f"Failed to send failure notification: {str(e)}")

    # Always close worklog before raising exception
    worklog_hook.error("Recovery process failed due to SSH connectivity issues")
    worklog_hook.close_worklog()
    logger.info(f"WorkLog {worklog_id} closed after SSH failure")

    # Raise exception to mark the task as failed
    raise AirflowException(f"Cannot establish SSH connection to {parsed_alert['target_ip']}")


def handle_task_failure(**context):
    """
    Failure callback to ensure WorkLog is properly closed on any task failure.
    This runs when any task in the DAG fails.
    """
    logger.error("Task failure detected - attempting to close WorkLog")

    try:
        # Try to get worklog ID from any previous task
        worklog_id = None

        # Check each possible source of worklog_id (include all tasks)
        task_ids_to_check = ['validate_and_parse_alert', 'check_ssh_connectivity', 'ssh_recovery', 'update_incident_ticket', 'notify_recovery_status', 'notify_failure']
        for task_id in task_ids_to_check:
            try:
                worklog_id = context['ti'].xcom_pull(task_ids=task_id, key='worklog_id')
                if worklog_id:
                    logger.info(f"Found worklog ID from task: {task_id}")
                    break
            except Exception:
                continue

        # Also try from parsed_alert if available
        if not worklog_id:
            try:
                parsed_alert = context['ti'].xcom_pull(key='parsed_alert')
                if parsed_alert and isinstance(parsed_alert, dict):
                    worklog_id = parsed_alert.get('worklog_id')
            except Exception:
                pass

        if worklog_id:
            worklog_hook = WorkLogHook()
            worklog_hook.set_worklog_id(worklog_id)

            # Log the failure details
            task_instance = context.get('task_instance')
            exception = context.get('exception')

            worklog_hook.error(f"Task '{task_instance.task_id}' failed: {str(exception) if exception else 'Unknown error'}")
            worklog_hook.error("Recovery workflow terminated due to task failure")

            # Add failure metadata
            worklog_hook.add_metadata({
                "failed_task": task_instance.task_id if task_instance else "unknown",
                "failure_reason": str(exception) if exception else "unknown",
                "failure_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            })

            # Close the worklog
            worklog_hook.close_worklog()
            logger.info(f"WorkLog {worklog_id} closed due to task failure")
        else:
            logger.warning("No worklog ID found - unable to close WorkLog")

    except Exception as e:
        logger.error(f"Failed to handle task failure cleanup: {str(e)}")


# Create the DAG
dag = DAG(
    'oscar_platform_recovery',
    default_args=default_args,
    description='Automated OSCAR platform recovery triggered by OscarPlatformUnhealthy alerts',
    schedule=None,  # Triggered on-demand only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=5,  # Allow multiple concurrent recoveries
    tags=['incident-response', 'auto-recovery', 'critical', 'oscar'],
)

# Task 1: Validate and parse the alert
validate_alert_task = PythonOperator(
    task_id='validate_and_parse_alert',
    python_callable=validate_and_parse_alert,
    on_failure_callback=handle_task_failure,
    dag=dag,
)

# Task 2: Check SSH connectivity (branching)
check_connectivity_task = BranchPythonOperator(
    task_id='check_ssh_connectivity',
    python_callable=check_ssh_connectivity,
    on_failure_callback=handle_task_failure,
    dag=dag,
)

# Task 3a: Execute SSH recovery
ssh_recovery_task = PythonOperator(
    task_id='ssh_recovery',
    python_callable=execute_ssh_recovery,
    on_failure_callback=handle_task_failure,
    dag=dag,
)

# Task 3b: Notify failure (if SSH unreachable)
notify_failure_task = PythonOperator(
    task_id='notify_failure',
    python_callable=notify_failure,
    on_failure_callback=handle_task_failure,
    trigger_rule='all_success',
    dag=dag,
)

# Task 4: Update incident ticket (if applicable)
# This task is non-critical - if it fails, we still want to send notifications
update_ticket_task = PythonOperator(
    task_id='update_incident_ticket',
    python_callable=update_incident_ticket,
    on_failure_callback=handle_task_failure,
    trigger_rule='all_success',
    retries=1,  # Try once more if ticket update fails
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

# Task 5: Send recovery status notification
notify_status_task = PythonOperator(
    task_id='notify_recovery_status',
    python_callable=notify_recovery_status,
    on_failure_callback=handle_task_failure,
    trigger_rule='all_success',
    dag=dag,
)

# End marker — two upstreams, one always skipped due to branching
end_task = EmptyOperator(
    task_id='end',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Define task dependencies
validate_alert_task >> check_connectivity_task
check_connectivity_task >> [ssh_recovery_task, notify_failure_task]
ssh_recovery_task >> update_ticket_task >> notify_status_task >> end_task
notify_failure_task >> end_task

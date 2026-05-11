from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import requests
import os
import httpx
import logging
import uuid
import json
import re
from hooks.worklog_hook import WorkLogHook, SeverityLevel, WorkLogType
from jinja2 import Template

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Generate a unique task ID for this run
task_id = f"WORKLOG-TEST-{uuid.uuid4().hex[:8]}"

filter_assigned_group = "Eric_MS_SD"
filter_status = "Assigned"

def create_worklog(**context):
    """Create a new worklog and add initial entries"""
    hook = WorkLogHook()

    buildmode = os.getenv('BUILD_MODE', 'production')

    # Create metadata for the worklog
    metadata = [
        {"key": "task_id", "value": task_id},
        {"key": "environment", "value": buildmode},
        {"key": "initiated_by", "value": "airflow"}
    ]

    logger.debug("Initializing worklog hook")
    # Create the worklog
    worklog = hook.create_worklog(
        name="Auto Queue Assignment Worklog",
        description="Worklog for autoqueue assignment automation",
        worklog_type=WorkLogType.DB,
        metadata=metadata
    )

    logger.info(f"Created worklog with ID: {worklog['id']}")

    # Add some initial entries
    hook.info("Starting the worklog test workflow for process_alert_initiate_autocaller")

    logger.info("push new worklog id for cross communication to next task")

    # Store the worklog ID in XCom for later tasks
    context['ti'].xcom_push(key='worklog_id', value=worklog['id'])

    logger.info("create worklog task ends")

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

def process_tickets(**context):
    """Fetch ITSM tickets, get labels, update them, return update info."""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')    

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    hook.info("Starting to fetch ticket with Assigned Group")

    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))
    ticketing_system: str = os.environ.get("DEFAULT_TICKETING_SYSTEM", "REMEDY")
    rule_namespace_enable: str = os.environ.get("RULE_NAMESPACE_ENABLE", "enable")
    evaluate_rule_autoqueue: str = os.environ.get("EVALUATE_RULE_NAME_AUTOQUEUE", "autoqueue")

    logger.debug("Fethced necessary details for oscar-rule for queue assignment information ")

    initial_assign_group: str = os.environ.get("AUTO_QUEUE_INIT_ASSIGN_GROUP", filter_assigned_group)
    assign_status: str = os.environ.get("AUTO_QUEUE_INIT_ASSIGN_STATUS", filter_status)

    # Fetch tickets by search parameters
    ticketing_system_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets?system={ticketing_system}"

    logger.debug(f"Fetch ticket information via fetch ticket url: {ticketing_system_url}")

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    params = {
        "assigned_group": initial_assign_group,
        "status": assign_status,
        "page": 1,
        "per_page": 30,
        "system": ticketing_system
    }

    # Fetch SD tickets from the ticketing system to assign to auto-queue
    try:
        with httpx.Client(verify=False, timeout=480.0) as client:  # Added 8 min timeout
            response = client.get(
                ticketing_system_url,
                headers=headers,
                params=params
            )
            response.raise_for_status()

            ticket_data = response.json()

            hook.info(f"Response ticket data: {ticket_data}")

            logger.debug("Fetched ticket information: ticket_data")

            # Validate response structure
            if not isinstance(ticket_data, list):
                hook.error("Invalid response format: expected list of dictionary")
                raise ValueError("Invalid response format: expected list of dictionary")

            # Validate that the first element (if present) is a dictionary
            if ticket_data and not isinstance(ticket_data[0], dict):
                hook.error("Invalid response format: first element is not a dictionary")
                raise ValueError("Invalid response format: first element is not a dictionary")

        hook.info(f"Ticket data fetched: {ticket_data}")
        hook.info(f"Ticket data fetched: {ticket_data}")
        logger.info(f"List of ticket data has been fectehd {ticket_data}")

    except httpx.TimeoutException as e:
        hook.error(f"Request timed out: {str(e)}")
        raise RuntimeError(f"Request timed out: {str(e)}")
    except httpx.HTTPStatusError as e:
        hook.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        raise RuntimeError(f"HTTP error: {e.response.status_code} - {e.response.text}")
    except httpx.RequestError as e:
        hook.error(f"Request error: {e}")
        raise RuntimeError(f"Request error: {e}")
    except Exception as e:
        hook.error(f"Unexpected error: {e}")
        raise RuntimeError(f"Unexpected error: {e}") from e

    updates = []
    no_ticket_found = False

    if ticket_data is None:
        hook.info("Received a null response from ITSM.")
        logger.error("Null Ticket response from ITSM")
        raise ValueError("Received a null response from ITSM")

    if not ticket_data:  # Handles empty dictionary, list, or string
        hook.info("Received an empty ticket response from ITSM. No applicable ticket to assign into proper queue")
        logger.warning("Received an empty ticket list from ITSM. No applicable ticket to assign into proper queue")
        no_ticket_found = True

    ticket_list = ticket_data

    # Extract all incident numbers before the loop
    incident_numbers = [ticket.get("values", {}).get("Incident Number", "NA") for ticket in ticket_list]
    hook.info(f"the incident numbers to be updated are:{incident_numbers}")

    all_ticket_process_failed = True

    for ticket in ticket_list:
        try:

            incident_no = ticket.get("incident_number", "NA")
            ticket_entry_id = ticket.get("id", "NA")
            hook.info(f"Processing ticket with Incident Number: {incident_no} and {ticket_entry_id} starts")
            logger.info(f"Processing ticket with Incident Number: {incident_no} and {ticket_entry_id} starts")

            # Extract the unique identifiers of the ticket and main ticket attributes
            incident_number = ticket.get("incident_number")
            request_id = ticket.get("id")
            entry_id = ticket.get("id")

            description = ticket.get("description")
            urgency = ticket.get("urgency")
            impact = ticket.get("impact")
            priority = ticket.get("priority")

            # Skip tickets with SOS, Service, and Affecting in description
            if description and isinstance(description, str):
                description_lower = description.lower()
                if all(word in description_lower for word in ['sos', 'service', 'affecting']):
                    hook.info(f"Skipping ticket {incident_no} - contains SOS, Service, and Affecting in description: {description}")
                    logger.info(f"Skipping ticket {incident_no} - contains SOS, Service, and Affecting in description: {description}")
                    continue

            # extract the values that are to be updated
            last_name = ticket.get("owner_last_name")
            first_name = ticket.get("owner_first_name")
            support_company = ticket.get("assigned_support_company")
            support_organization = ticket.get("assigned_support_organaization")
            assigned_group = ticket.get("assigned_group")
            status = ticket.get("status")
            assignee = ticket.get("assignee")
            assignee_login_id = ticket.get("assignee_login_id")
            support_group_id = ticket.get("assigned_group_id")

            # Validate required fields
            required_fields = ["incident_number", "id"]
            missing_fields = [field for field in required_fields if not ticket.get(field)]

            logger.info(f"Process ticket operation for Ticket with {incident_number} is in progress")

            hook.info(f"Ticket data for {incident_number} is in process")

            if missing_fields:
                hook.error(f"Ticket missing required fields: {missing_fields}")
                logger.error(f"Ticket missing required fields: {missing_fields}")
                continue

            # Construct the properties dictionary
            properties = {
                "summary": description,
                "urgency": urgency,
                "impact": impact,
                "priority": priority
            }

            # Construct the final payload
            payload = {
                "namespace": rule_namespace_enable,
                "name": evaluate_rule_autoqueue,
                "properties": properties
            }

            logger.info(f"Payload for rule evaluation: {payload} for ticket {incident_number}")
            hook.info(f"Payload for rule evaluation: {payload} for ticket {incident_number}")

            evaluate_rule_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/rules/evaluate"

            logger.info(f"Rule evaluation URL: {evaluate_rule_url}")
            hook.info(f"Rule evaluation URL: {evaluate_rule_url}")

            hook.info(f"Evaluate rules with properties: {properties}")

            # Fetch labels with timeout
            hook.info("Fetch rule labels from rule evaluation")
            try:
                with httpx.Client(verify=False) as client:
                    response = client.post(evaluate_rule_url, headers=headers, json=payload)
                    response.raise_for_status()

                    data = response.json()

                    hook.info(f"For {incident_number} ticket response data: {data}")
                    logger.info(f"For {incident_number} ticket response data: {data}")

                    # Validate response structure
                    if not isinstance(data, dict):
                        hook.error("Invalid rule evaluation response format")
                        logger.error("Invalid rule evaluation response format")
                        continue

                    # Extract applied_labels
                    applied_labels = data.get("applied_labels", {})
                    logger.info(f"Evaluated labels: {applied_labels}  for ticket {incident_number}")
                    hook.info(f"Evaluated labels: {applied_labels} for ticket {incident_number}")

            except httpx.TimeoutException as e:
                logger.error(f"for {incident_number} Rule evaluation timed out: {str(e)}")
                hook.error(f"for {incident_number} Rule evaluation timed out: {str(e)}")
                continue
            except httpx.HTTPStatusError as e:
                logger.error(f"for {incident_number} HTTP Error evaluating rules: {e.response.status_code}, {e.response.text}")
                hook.error(f"for {incident_number} HTTP Error evaluating rules: {e.response.status_code}, {e.response.text}")
                continue
            except httpx.RequestError as e:
                logger.error(f"for {incident_number} Request Error evaluating rules: {str(e)}")
                hook.error(f"for {incident_number} Request Error evaluating rules: {str(e)}")
                continue
            except Exception as e:
                logger.error(f"for {incident_number} Unexpected exception while updating ITSM ticketsd with new queue assignment")
                hook.error(f"for {incident_number} Unexpected exception while updating ITSM ticketsd with new queue assignment")
                continue

            # check the applied labels are available
            hook.info("Evaluate and determine applicable labels based on Auto Queue Assignment rules")
            logger.info("Evaluate and determine applicable labels based on Auto Queue Assignment rules")

            if not applied_labels:
                logger.error("No labels have been applied, system will proceed with default values")
                hook.error("No labels have been applied, system will proceed with default values")

            if applied_labels is None:
                applied_labels = {}

            hook.info("Ticket queue assignemnt proceeds either with rule evaluated or default data")
            logger.info("Ticket queue assignemnt proceeds either with rule evaluated or default data")

            support_company_new = applied_labels.get("support_company", "MS Uprising")
            support_organization_new = applied_labels.get("support_organization", "Ericsson MS Application Support Tier 1")
            assigned_group_new = applied_labels.get("support_group", "AS_T1_OMON")
            status_new = applied_labels.get("status", "Assigned")
            assignee_new = applied_labels.get("assigned", "Sunil Sharma")

            last_name_new = applied_labels.get("last_name")
            first_name_new = applied_labels.get("first_name")

            # If first name or last name is missing, extract from Assignee
            if not first_name_new or not last_name_new:
                assignee_parts = assignee_new.split()
                if len(assignee_parts) == 1:
                    first_name_new = assignee_parts[0]
                    last_name_new = assignee_parts[0]
                elif len(assignee_parts) > 1:
                    first_name_new = assignee_parts[0]
                    last_name_new = assignee_parts[-1]

            assignee_login_id_new = applied_labels.get("assignee_login_id", "esshar01")
            support_group_id_new = applied_labels.get("support_group_id", "SGP000000003037")

            logger.info(f"proceeding for ticket update with tikcet request_id: {request_id} tikcet incident number: {incident_no}")

            update_ticket_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/{request_id}?system={ticketing_system}"
            logger.info(f"proceeding for ticket update with ticket url {update_ticket_url} for incident numner: {incident_no}")
            logger.info(f"first_name: {first_name} last_name: {last_name}")

            # Construct the payload for calling the api
            data = {
                "name": description,
                "owner_first_name": first_name,
                "owner_last_name": last_name,
                "assigned_support_company": support_company_new,
                "assigned_support_organaization": support_organization_new,
                "assigned_group": assigned_group_new,
                "assignee": assignee_new,
                "assignee_login_id": assignee_login_id_new,
                "assigned_group_id": support_group_id_new,
                "status": status_new
            }

            hook.info(f"Update the tickets for re-assign to ITSM, request_id: {request_id} incident number: {incident_number} with new assignee details {json.dumps(data)}")

            logger.info(f"proceeding to call update ticket call with payload: {json.dumps(data)} api url: {update_ticket_url}")
            hook.info(f"proceeding to call update ticket call with payload: {json.dumps(data)} api url: {update_ticket_url}")

            try:
                with httpx.Client(verify=False, timeout=180.0) as client:  # 3 minutes timeout
                    response = client.patch(update_ticket_url, headers=headers, json=data)
                    response.raise_for_status()
                    update_ticket_data = response.json()

                    logger.debug(f"Updated ticket data {update_ticket_data}")
                    hook.info(f"{incident_number} : Updated ticket data {update_ticket_data}")

                if response.status_code != 200:
                    hook.error(f"Ticket update for incident_number {incident_number} and entry_id {entry_id} failed")

                hook.info(f"{incident_number} : Ticket update for incident_number {incident_number} and entry_id {entry_id} successful")
                logger.info(f"{incident_number} : Ticket update for incident_number {incident_number} and entry_id {entry_id} successful")

                # Store update information only if update was successful
                updates.append({
                    "incident_number": incident_number,
                    "request_id": request_id,
                    "entry_id": entry_id,
                    "old_assigned_group": assigned_group,
                    "assigned_group": assigned_group_new,
                    "old_assignee": assignee,
                    "assignee": assignee_new,
                    "description": description,
                    "status": status_new,
                })

            except httpx.TimeoutException as e:
                logger.error(f"Ticket update timed out: {str(e)}")
                hook.error(f"Ticket update timed out: {str(e)}")
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error for updating tickets: {e.response.status_code}, {e.response.text}")
                hook.error(f"HTTP error for updating tickets: {e.response.status_code}, {e.response.text}")
            except httpx.RequestError as e:
                logger.error(f"Request Error updating tickets: {str(e)}")
                hook.error(f"Request Error updating tickets: {str(e)}")
            except Exception as e:
                logger.error(f"Unexpected exception while updating ITSM tickets with new queue assignment {str(e)}")
                hook.error(f"Unexpected exception while updating ITSM tickets with new queue assignment {str(e)}")

            logger.info(f"Processing ticket with Incident Number: {incident_no} and Entry ID: {ticket_entry_id} ends")
            hook.info(f"Processing ticket with Incident Number: {incident_no} and Entry ID: {ticket_entry_id} ends")
            all_ticket_process_failed = False

        except Exception as e:
            hook.error(f"Error processing ticket: {str(e)} for inicident number {incident_no}")
            logger.error(f"Error processing ticket: {str(e)} for inicident number {incident_no}")
            continue

    hook.info(f"Updating for reassignment for all updated tickets ends")
    logger.info(f"Updating for reassignment for all updated tickets ends")

    # If no tickets were successfully processed, even though tickets were found, raise an exception
    if not updates and not no_ticket_found:
        hook.error("All ticket processing failed. No tickets were updated.")
        logger.error("None of the fetched tickets has been updated")
        raise Exception("All ticket processing failed. No tickets were updated.")

    # Get original ticket count from context
    original_ticket_count = len(ticket_list)
    successful_updates = len(updates)

    if successful_updates == original_ticket_count:
        hook.info(f"All {original_ticket_count} tickets were successfully processed and updated")
        logger.info(f"All {original_ticket_count} tickets were successfully processed and updated")
    else:
        hook.info(f"Partially successful: {successful_updates} out of {original_ticket_count} tickets were updated")
        logger.info(f"Partially successful: {successful_updates} out of {original_ticket_count} tickets were updated")
        logger.error(f"Failed to update {original_ticket_count - successful_updates} tickets")
        hook.error(f"Failed to update {original_ticket_count - successful_updates} tickets")
    # Store updates in XCom for the next task
    context['ti'].xcom_push(key="ticket_updates", value=updates)

def send_update_email(**context):
    """Send a summary email of all ticket updates."""

    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')    

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    ti = context['ti']
    updated_tickets = ti.xcom_pull(task_ids='process_tickets', key='ticket_updates')

    if updated_tickets is None:
        updated_tickets = []

    autoqueue_template_ns = os.getenv('AUTO_QUEUE_ASSIGNEMENT_TEMPLATE_MAP_NS', 'magenta')
    autoqueue_template_map = os.getenv('AUTO_QUEUE_ASSIGNEMENT_MAP_REMEDY', 'autoqueue-assignment')
    autoqueue_template_element_key = os.getenv('AUTO_CALLER_TEMPLATE_ELEMENT_KEY', 'autoqueue-assignment-email-template')

    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))

    # Fetch email template from OSCAR API
    oscar_mapping_element_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/mapping-data/mapping/element"

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    params = {
        "mapping_name": autoqueue_template_map,
        "mapping_namespace_name": autoqueue_template_ns,
        "mapping_key": autoqueue_template_element_key
    }

    logger.debug(f"Oscar email etmplate map elemet url {oscar_mapping_element_url} and params:{params}")

    # fetch the email Jinja Template for update
    hook.info(f"Fetching Email template from Oscar Map")
    logger.info(f"Fetching Email template from Oscar Map")

    time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ticket_query_output = "Summary of automation results"

    table_heads = ["Incident", "Summary", "Application", "Assignee", "Update Status"]

    try:
        with httpx.Client(verify=False) as client:  # Disable SSL verification if needed
            response = client.get(oscar_mapping_element_url, headers=headers, params=params)
            response.raise_for_status()
            email_templates = response.json()

            email_template = email_templates[0].get("value", "") if email_templates else ""

            logger.debug("Email template has been fetched successfully")

            if not email_template:
                hook.error(f"No email template found.")
                logger.error(f"No email template found")
                raise ValueError("No email template found.")

    except httpx.HTTPStatusError as e:
        hook.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        raise RuntimeError(f"HTTP error: {e.response.status_code} - {e.response.text}")
    except httpx.RequestError as e:
        hook.error(f"Request error: {e}")
        logger.error(f"Request error: {e}")
        raise RuntimeError(f"Request error: {e}")
    except ValueError as e:
        hook.error(f"Value error: verify email template map {e}")
        logger.error(f"Value error: verify email template map {e}")
        raise RuntimeError(f"Value error: verify email template map {e}")
    except Exception as e:
        hook.error(f"Value error: verify email template map {e}")
        logger.error(f"Value error: verify email template map {e}")
        raise RuntimeError(f"Unexpected error: {e}") from e

    hook.info(f"Email content with updated data {email_template}")
    logger.info(f"Email content with updated data {email_template}")

    # Render email content using Jinja2 template rendering
    try:
        template = Template(email_template)

        rendered_email = template.render(
            time_stamp=time_stamp,
            ticket_query_output=ticket_query_output,
            tickets=updated_tickets,
            table_heads=table_heads
        )

        logger.debug(f"rendered email:{rendered_email}")

    except Exception as e:
        hook.error(f"Error rendering email content: {e}")
        logger.error(f"Error rendering email content: {e}")
        raise RuntimeError(f"Jinja2 template rendering error: {e}")

    # Get the notifier name from environment variables

    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))

    notifier_name: str = os.environ.get("OSCAR_AUTOQUEUE_NOTIFIER_NAME", "OSCAR-NOTIFIER-AUTOQUEUE")

    # Oscar Notifier name configured with smtp details, recipient list etc
    notifier_send_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/notifiers/send"

    logger.info(f"Oscar notifer to send email {notifier_send_url} with notifier: {notifier_name}")

    payload = {
        "name": notifier_name,
        "message": rendered_email,
        "subject": "Oscar - Auto Queue Assignment | Execution Report"
    }

    params = {
        "muted": "false",
        "notifier_type": "email"
    }

    logger.info(f"payload for notifer-send request: {payload} and request params: {params}")

    try:
        with httpx.Client(verify=False) as client:  # Disable SSL verification for internal API
            response = client.post(notifier_send_url, headers=headers, params=params, json=payload)
            response.raise_for_status()
            task_info = response.json()

            logger.info(f"Email task started with task_history_id: {task_info.get('original_task_id')} and task name {task_info.get('name')}")
            hook.info(f"Email task started with task_history_id: {task_info.get('original_task_id')} and task name {task_info.get('name')}")

    except httpx.HTTPError as e:
        hook.error("Error initiating task for email notification")
        logger.error(f"Request failed: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error: {e}") from e

with DAG('auto_queue_assignment_ITSM_ticket',
         default_args=default_args,
         schedule="*/10 * * * *",  # Runs every 10 minutes
         catchup=False) as dag:

    # Task 1: Create worklog
    task_create_worklog = PythonOperator(
        task_id='create_worklog',
        python_callable=create_worklog,
    )

    # Task 2: Process tickets
    task_process_tickets = PythonOperator(
        task_id='process_tickets',
        python_callable=process_tickets,
    )

    # Task 3: Send update email
    task_send_email = PythonOperator(
        task_id='send_update_email',
        python_callable=send_update_email,
    )

    # Task 4: Close worklog
    task_close_worklog = PythonOperator(
        task_id='close_worklog',
        python_callable=close_worklog,
    )

    # Set task dependencies
    task_create_worklog >> task_process_tickets >> task_send_email >> task_close_worklog

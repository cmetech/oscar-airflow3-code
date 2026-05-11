from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import json
import uuid
from typing import Dict, Any, Optional

# Import our custom hooks
from hooks.worklog_hook import WorkLogHook, SeverityLevel, WorkLogType  # type: ignore

logger = logging.getLogger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'oscar-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 1, 1),
}


def validate_and_parse_alert(**context) -> Dict[str, Any]:
    """
    Task 1: Validate and parse the alert from DAG run configuration.
    Creates a WorkLog immediately to track any errors that occur.

    The alert is passed as a JSON string in the 'alert' key of dag_run.conf.
    This simulates how alerts are passed from the notification system.
    """
    logger.info("Starting alert validation and parsing")

    # Create WorkLog immediately for error tracking
    hook = WorkLogHook()
    worklog = None
    run_id = f"ALERT-WORKFLOW-{uuid.uuid4().hex[:8]}"

    try:
        # Create initial worklog with minimal info
        worklog = hook.create_worklog(
            name=f"Alert Workflow - {run_id}",
            description="Processing alert workflow - validating input",
            worklog_type=WorkLogType.DB,
            metadata=[
                {"key": "run_id", "value": run_id},
                {"key": "dag_id", "value": context['dag'].dag_id},
                {"key": "task_instance", "value": str(context['ti'])},
                {"key": "execution_date", "value": str(context['execution_date'])}
            ]
        )
        worklog_id = worklog.get('id')
        logger.info(f"Created initial worklog with ID: {worklog_id}")
        hook.info(f"Alert workflow processing started for run: {run_id}")

        # Store worklog ID for all tasks to use
        context['ti'].xcom_push(key='worklog_id', value=worklog_id)

        # Get DAG run configuration
        dag_run = context.get('dag_run')
        if not dag_run or not dag_run.conf:
            hook.error("No DAG run configuration found")
            raise ValueError("No DAG run configuration found")

        conf = dag_run.conf
        hook.debug(f"DAG run conf keys: {list(conf.keys())}")
        logger.info(f"DAG run conf keys: {list(conf.keys())}")

        # Extract alert JSON string
        alert_json_str = conf.get('alert')
        if not alert_json_str:
            hook.error("No 'alert' key found in DAG run configuration")
            hook.warning("Available conf keys: " + str(list(conf.keys())))
            raise ValueError("No 'alert' key found in DAG run configuration")

        hook.debug(f"Alert JSON string length: {len(alert_json_str)}")
        logger.info(f"Alert JSON string length: {len(alert_json_str)}")

        # Parse the alert JSON
        try:
            alert_data = json.loads(alert_json_str)
            hook.info("Alert JSON parsed successfully")
        except json.JSONDecodeError as e:
            hook.error(f"Failed to parse alert JSON: {str(e)}")
            hook.debug(f"Raw JSON (first 500 chars): {alert_json_str[:500]}")
            logger.error(f"Failed to parse alert JSON: {str(e)}")
            logger.error(f"Raw JSON (first 500 chars): {alert_json_str[:500]}")
            raise

        # Validate alert structure
        if not isinstance(alert_data, dict):
            error_msg = f"Alert data is not a dictionary: {type(alert_data)}"
            hook.error(error_msg)
            raise ValueError(error_msg)

        # Extract key alert information
        labels = alert_data.get('labels', {})
        annotations = alert_data.get('annotations', {})

        # Log key alert properties
        hook.info("=== ALERT DETAILS ===")
        alert_name = labels.get('alertname', 'Unknown')
        severity = labels.get('severity', 'Unknown')
        status = alert_data.get('status', 'Unknown')
        fingerprint = labels.get('oscar_fingerprint', 'Unknown')

        hook.info(f"Alert Name: {alert_name}")
        hook.info(f"Severity: {severity}")
        hook.info(f"Status: {status}")
        hook.info(f"Fingerprint: {fingerprint}")

        logger.info("=== ALERT DETAILS ===")
        logger.info(f"Alert Name: {alert_name}")
        logger.info(f"Severity: {severity}")
        logger.info(f"Status: {status}")
        logger.info(f"Fingerprint: {fingerprint}")

        # Log incident information (this is what we're testing for)
        incident_number = labels.get('incident_number')
        ticket_id = labels.get('ticket_id')

        hook.info("=== INCIDENT DETAILS ===")
        hook.info(f"Incident Number: {incident_number or 'NOT PRESENT'}")
        hook.info(f"Ticket ID: {ticket_id or 'NOT PRESENT'}")

        logger.info("=== INCIDENT DETAILS ===")
        logger.info(f"Incident Number: {incident_number or 'NOT PRESENT'}")
        logger.info(f"Ticket ID: {ticket_id or 'NOT PRESENT'}")

        # Log task/workflow execution info
        fired_tasks = annotations.get('fired_tasks', '')
        fired_workflows = annotations.get('fired_workflows', '')

        hook.info("=== EXECUTION TRACKING ===")
        hook.info(f"Fired Tasks: {fired_tasks or 'None'}")
        hook.info(f"Fired Workflows: {fired_workflows or 'None'}")

        logger.info("=== EXECUTION TRACKING ===")
        logger.info(f"Fired Tasks: {fired_tasks or 'None'}")
        logger.info(f"Fired Workflows: {fired_workflows or 'None'}")

        # Update worklog with alert details
        hook.add_metadata({"alert_name": alert_name})
        hook.add_metadata({"severity": severity})
        hook.add_metadata({"fingerprint": fingerprint})
        if incident_number:
            hook.add_metadata({"incident_number": incident_number})
        if ticket_id:
            hook.add_metadata({"ticket_id": ticket_id})

        # Store parsed alert for downstream tasks
        context['ti'].xcom_push(key='parsed_alert', value=alert_data)
        context['ti'].xcom_push(key='incident_number', value=incident_number)
        context['ti'].xcom_push(key='ticket_id', value=ticket_id)
        context['ti'].xcom_push(key='alert_name', value=alert_name)
        context['ti'].xcom_push(key='severity', value=severity)
        context['ti'].xcom_push(key='fingerprint', value=fingerprint)

        hook.info("Alert validation and parsing completed successfully")
        logger.info("Alert validation and parsing completed successfully")
        return alert_data

    except Exception as e:
        # Ensure we log the error and close worklog on any failure
        if hook and worklog:
            hook.error(f"Alert validation failed: {str(e)}")
            hook.error("Workflow will be terminated due to validation failure")
            try:
                hook.close_worklog()
                logger.info("WorkLog closed due to validation failure")
            except Exception as close_error:
                logger.error(f"Failed to close WorkLog: {str(close_error)}")

        logger.error(f"Alert validation failed: {str(e)}")
        raise


def update_worklog_with_details(**context) -> str:
    """
    Task 2: Update the existing WorkLog with detailed alert information.

    This demonstrates how to enhance WorkLog tracking with complete incident information.
    """
    logger.info("Updating WorkLog with detailed alert information")

    # Get worklog ID from previous task
    worklog_id = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found from previous task")
        raise ValueError("No worklog ID found from previous task")

    # Create WorkLog hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    try:
        # Get parsed alert data from XCom
        alert_data = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='parsed_alert')
        incident_number = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='incident_number')
        ticket_id = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='ticket_id')
        alert_name = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='alert_name')
        severity = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='severity')
        fingerprint = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='fingerprint')

        if not alert_data:
            hook.error("No alert data found from previous task - workflow cannot continue")
            raise ValueError("No alert data found from previous task")

        # Extract alert details
        labels = alert_data.get('labels', {})
        annotations = alert_data.get('annotations', {})

        hook.info("=== UPDATING WORKLOG WITH DETAILED INFORMATION ===")
        hook.info(f"Processing alert: {alert_name} (severity: {severity})")

        if incident_number:
            hook.info(f"✅ Incident number found: {incident_number}")
        else:
            hook.warning("⚠️ No incident number in alert")

        if ticket_id:
            hook.info(f"✅ Ticket ID found: {ticket_id}")
        else:
            hook.warning("⚠️ No ticket ID in alert")

        hook.debug(f"Alert fingerprint: {fingerprint}")
        hook.debug(f"Alert status: {alert_data.get('status', 'Unknown')}")

        # Log task/workflow execution info
        fired_tasks = annotations.get('fired_tasks', '')
        fired_workflows = annotations.get('fired_workflows', '')

        if fired_tasks:
            hook.info(f"Previously fired tasks: {fired_tasks}")
        if fired_workflows:
            hook.info(f"Previously fired workflows: {fired_workflows}")

        # Log support group info if available
        support_group = labels.get('support_group')
        if support_group:
            hook.info(f"Support group: {support_group}")

        hook.info("WorkLog update completed - ready for incident processing")
        logger.info("WorkLog update with details completed")
        return worklog_id

    except Exception as e:
        # Ensure we log the error to worklog if possible
        hook.error(f"Failed to update worklog with details: {str(e)}")
        hook.error("Workflow may continue with limited information")
        logger.error(f"WorkLog update failed: {str(e)}")
        raise


def process_incident(**context) -> Dict[str, Any]:
    """
    Task 3: Process the incident and demonstrate access to incident context.

    This shows how a workflow can access and use incident information.
    """
    logger.info("Starting incident processing")
    hook = WorkLogHook()

    try:
        # Get worklog ID from previous task with multiple fallbacks
        worklog_id = context['ti'].xcom_pull(task_ids='update_worklog_with_details', key='worklog_id')
        if not worklog_id:
            # Fallback to original task if needed
            worklog_id = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='worklog_id')

        if worklog_id:
            hook.set_worklog_id(worklog_id)
        else:
            # Try to find worklog by metadata if ID not available
            dag_run_id = context['dag_run'].run_id
            found_worklog = hook.find_open_worklog({"run_id": dag_run_id})
            if found_worklog:
                hook.set_worklog_id(found_worklog)
            else:
                # Create emergency worklog for error tracking
                worklog = hook.create_worklog(
                    name=f"EMERGENCY-{context['task'].task_id}-{context['ds']}",
                    description="Emergency worklog created due to missing worklog ID",
                    worklog_type=WorkLogType.DB,
                    metadata=[{"key": "emergency_recovery", "value": "true"}]
                )
                hook.set_worklog_id(worklog['id'])

        # Get data from previous tasks
        alert_data = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='parsed_alert')
        incident_number = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='incident_number')
        ticket_id = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='ticket_id')

        hook.info("=== INCIDENT PROCESSING PHASE ===")

        if not alert_data:
            hook.warning("No alert data available for incident processing - continuing with limited functionality")
            hook.add_metadata({"processing_issue": "missing_alert_data"})
            return {"processed": False, "reason": "no_alert_data"}

        # Process incident information
        if incident_number:
            hook.info(f"Processing incident: {incident_number}")

            # Simulate incident-related processing steps
            hook.debug("Retrieving incident details from ticketing system")
            hook.info("Incident details retrieved successfully")

            hook.debug("Checking incident priority and escalation rules")

            if ticket_id:
                hook.info(f"Associated ticket: {ticket_id}")
                hook.debug("Updating ticket with workflow execution details")

            # Simulate some processing time and steps
            import time
            time.sleep(1)

            hook.info("Incident context successfully integrated into workflow")

            # Extract additional context
            labels = alert_data.get('labels', {})
            annotations = alert_data.get('annotations', {})

            # Log support group info if available
            support_group = labels.get('support_group')
            if support_group:
                hook.info(f"Support group: {support_group}")

            # Log any fired tasks/workflows
            fired_tasks = annotations.get('fired_tasks', '')
            fired_workflows = annotations.get('fired_workflows', '')

            if fired_tasks:
                hook.info(f"Previously fired tasks: {fired_tasks}")
            if fired_workflows:
                hook.info(f"Previously fired workflows: {fired_workflows}")

            processing_result = {
                "incident_processed": True,
                "incident_number": incident_number,
                "ticket_id": ticket_id,
                "support_group": support_group,
                "fired_tasks": fired_tasks,
                "fired_workflows": fired_workflows
            }

        else:
            hook.warning("No incident number found - processing as standard alert")
            hook.info("Executing standard alert processing workflow")

            processing_result = {
                "incident_processed": False,
                "processing_type": "standard_alert"
            }

        # Log completion
        hook.info("Incident processing phase completed")

        # Store result for final task
        context['ti'].xcom_push(key='processing_result', value=processing_result)

        logger.info("Incident processing completed")
        return processing_result

    except Exception as e:
        # Ensure we log the error to worklog if possible
        hook.error(f"Error during incident processing: {str(e)}")
        hook.add_metadata({"error": str(e), "processing_stage": "incident_analysis"})
        logger.error(f"Incident processing failed: {str(e)}")
        raise


def handle_task_failure(**context):
    """
    Failure callback to ensure WorkLog is properly closed on any task failure.
    This runs when any task in the DAG fails.
    """
    logger.error("Task failure detected - attempting to close WorkLog")

    try:
        # Try to get worklog ID from any previous task
        worklog_id = None

        # Check each possible source of worklog_id
        task_ids = ['validate_and_parse_alert', 'update_worklog_with_details', 'process_incident']
        for task_id in task_ids:
            try:
                worklog_id = context['ti'].xcom_pull(task_ids=task_id, key='worklog_id')
                if worklog_id:
                    break
            except Exception:
                continue

        if worklog_id:
            hook = WorkLogHook()
            hook.set_worklog_id(worklog_id)

            # Log the failure
            task_instance = context.get('task_instance')
            exception = context.get('exception')

            hook.error(f"Task '{task_instance.task_id}' failed: {str(exception) if exception else 'Unknown error'}")
            hook.error("Workflow terminated due to task failure")

            # Close the worklog
            hook.close_worklog()
            logger.info(f"WorkLog {worklog_id} closed due to task failure")
        else:
            logger.warning("No worklog ID found - unable to close WorkLog")

    except Exception as e:
        logger.error(f"Failed to handle task failure cleanup: {str(e)}")


def complete_workflow(**context) -> bool:
    """
    Task 4: Complete the workflow and close the WorkLog.

    This demonstrates proper workflow completion and audit trail.
    """
    logger.info("Starting workflow completion")

    # Get worklog ID - try multiple sources for robustness
    worklog_id = None
    task_ids = ['validate_and_parse_alert', 'update_worklog_with_details', 'process_incident']
    for task_id in task_ids:
        try:
            worklog_id = context['ti'].xcom_pull(task_ids=task_id, key='worklog_id')
            if worklog_id:
                logger.info(f"Found worklog ID from task: {task_id}")
                break
        except Exception:
            continue

    if not worklog_id:
        logger.error("No worklog ID found from any previous task")
        raise ValueError("No worklog ID found from any previous task")

    # Create WorkLog hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    try:
        # Get data from previous tasks
        alert_data = context['ti'].xcom_pull(task_ids='validate_and_parse_alert', key='parsed_alert')
        processing_result = context['ti'].xcom_pull(task_ids='process_incident', key='processing_result')

        hook.info("=== WORKFLOW COMPLETION PHASE ===")

        # Handle missing data gracefully
        if not alert_data:
            hook.warning("No alert data available for completion summary")
            alert_data = {"labels": {}, "annotations": {}}

        if not processing_result:
            hook.warning("No processing result available - assuming standard processing")
            processing_result = {"incident_processed": False, "processing_type": "unknown"}

        # Summarize processing
        if processing_result.get('incident_processed'):
            hook.info("✅ Incident-based workflow completed successfully")
            incident_number = processing_result.get('incident_number')
            ticket_id = processing_result.get('ticket_id')

            hook.info(f"Summary: Processed incident {incident_number}")
            if ticket_id:
                hook.info(f"Summary: Associated with ticket {ticket_id}")

            # Log any automation that was triggered
            fired_tasks = processing_result.get('fired_tasks')
            fired_workflows = processing_result.get('fired_workflows')

            if fired_tasks:
                hook.info(f"Summary: Tasks executed: {fired_tasks}")
            if fired_workflows:
                hook.info(f"Summary: Workflows executed: {fired_workflows}")

        else:
            hook.info("✅ Standard alert workflow completed successfully")
            hook.info("Summary: Processed alert without incident context")

        # Extract final alert details
        labels = alert_data.get('labels', {})
        alert_name = labels.get('alertname', 'Unknown')
        severity = labels.get('severity', 'Unknown')

        hook.info(f"Final status: Alert {alert_name} (severity: {severity}) processed")

        # Add performance metrics
        execution_time = context.get('execution_date')
        if execution_time:
            hook.debug(f"Workflow execution started: {execution_time}")

        # Mark completion
        hook.info("Workflow execution completed - closing worklog")

        # Close the worklog
        try:
            result = hook.close_worklog()
            logger.info(f"WorkLog closed successfully: {result}")
            hook.info("WorkLog closed successfully")
        except Exception as e:
            logger.error(f"Failed to close WorkLog: {str(e)}")
            # Don't fail the workflow if worklog close fails

        logger.info("Workflow completion finished")
        return True

    except Exception as e:
        # Ensure worklog is closed even if completion fails
        hook.error(f"Workflow completion failed: {str(e)}")
        try:
            hook.close_worklog()
            logger.info("WorkLog closed after completion failure")
        except Exception as close_error:
            logger.error(f"Failed to close WorkLog after completion failure: {str(close_error)}")
        raise


# Create the DAG
dag = DAG(
    'alert_workflow_test',
    default_args=default_args,
    description='Test DAG to verify alert workflow triggering with incident tracking',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'alert', 'workflow', 'incident', 'worklog'],
    doc_md="""
    # Alert Workflow Test DAG

    This DAG tests the complete alert workflow triggering mechanism, demonstrating:

    1. **Alert Reception**: Receives alert object from notification system via `dag_run.conf['alert']`
    2. **Incident Context**: Extracts and processes incident_number and ticket_id from alert
    3. **WorkLog Integration**: Creates and maintains detailed audit trail
    4. **Workflow Processing**: Simulates incident-aware workflow execution

    ## Test Trigger

    Trigger this DAG manually with a configuration like:

    ```json
    {
      "alert": "{\\"labels\\":{\\"alertname\\":\\"TestAlert\\",\\"severity\\":\\"critical\\",\\"incident_number\\":\\"INC0012345\\",\\"ticket_id\\":\\"TKT-2024-001234\\",\\"oscar_fingerprint\\":\\"test123\\",\\"support_group\\":\\"database-team\\"},\\"annotations\\":{\\"fired_tasks\\":\\"task_abc123\\",\\"fired_workflows\\":\\"manual__2024-01-15T10:45:00\\",\\"description\\":\\"Test alert for workflow validation\\"},\\"status\\":\\"firing\\"}"
    }
    ```

    ## Expected Results

    1. Alert is parsed and validated
    2. WorkLog is created with incident metadata
    3. Incident processing demonstrates access to ticket information
    4. Workflow completes with proper audit trail

    This verifies the notification system → workflow triggering chain works correctly.
    """
)

# Define tasks with proper error handling
validate_alert = PythonOperator(
    task_id='validate_and_parse_alert',
    python_callable=validate_and_parse_alert,
    dag=dag,
    on_failure_callback=handle_task_failure,
    doc_md="Parse and validate the alert from DAG run configuration, extract incident information, create initial WorkLog"
)

update_worklog_task = PythonOperator(
    task_id='update_worklog_with_details',
    python_callable=update_worklog_with_details,
    dag=dag,
    on_failure_callback=handle_task_failure,
    doc_md="Update WorkLog with detailed incident metadata for comprehensive audit tracking"
)

process_incident_task = PythonOperator(
    task_id='process_incident',
    python_callable=process_incident,
    dag=dag,
    on_failure_callback=handle_task_failure,
    doc_md="Process incident information and demonstrate context usage"
)

complete_workflow_task = PythonOperator(
    task_id='complete_workflow',
    python_callable=complete_workflow,
    dag=dag,
    on_failure_callback=handle_task_failure,
    doc_md="Complete workflow execution and close WorkLog"
)

# Set task dependencies
validate_alert >> update_worklog_task >> process_incident_task >> complete_workflow_task

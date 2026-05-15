from datetime import datetime, timedelta
import logging
import json
import uuid
from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator

# Import our custom hook
from hooks.worklog_hook import WorkLogHook, SeverityLevel, WorkLogType  # type: ignore

logger = logging.getLogger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def process_alert(**context):
    """
    Process the alert from the DAG run configuration.

    The alert is passed as a JSON string in the 'alert' key of the dag_run.conf.
    """
    # Generate a unique ID for this alert processing run
    run_id = f"ALERT-HANDLER-{uuid.uuid4().hex[:8]}"

    # Initialize the WorkLogHook
    hook = None
    worklog_id = None

    try:
        hook = WorkLogHook()

        # Create worklog metadata with safe access to context values
        metadata = [
            {"key": "run_id", "value": run_id}
        ]

        # Safely access dag and add to metadata if available
        if context and 'dag' in context and context['dag']:
            metadata.append({"key": "dag_id", "value": context['dag'].dag_id})

        # Safely get dag_run from context
        dag_run = context.get('dag_run') if context else None
        if dag_run:
            # Add dag_run_id to metadata
            metadata.append({"key": "dag_run_id", "value": dag_run.run_id})

            # Add logical_date if available (execution_date renamed in AF3)
            logical_date = context.get('logical_date') or context.get('execution_date')
            if logical_date:
                metadata.append({"key": "execution_date", "value": str(logical_date)})
        else:
            # Log warning if dag_run is not available
            logger.warning("No dag_run available in context")

        # Create the worklog
        worklog = hook.create_worklog(
            name="Alert Handler Worklog",
            description="Worklog for processing alerts from notification system",
            worklog_type=WorkLogType.DB,
            metadata=metadata
        )

        worklog_id = worklog.get('id') if isinstance(worklog, dict) else None
        logger.info(f"Created worklog with ID: {worklog_id}")
        hook.info(f"Starting alert processing run: {run_id}")

        # Safely extract conf from dag_run
        conf = {}
        if dag_run:
            conf = dag_run.conf if dag_run.conf is not None else {}
            if not isinstance(conf, dict):
                error_msg = f"DAG run conf is not a dictionary: {type(conf)}"
                logger.error(error_msg)
                hook.error(error_msg)
                hook.close_worklog()
                return None

            # Log entire conf object for debugging
            hook.info(f"DAG run conf received with {len(conf)} keys: {list(conf.keys())}")
            logger.info(f"DAG run conf keys: {list(conf.keys())}")

            # Log sanitized version of the entire conf
            try:
                # Create a copy of conf to avoid modifying the original
                sanitized_conf = conf.copy()

                # Handle the alert key specially - if it exists but we'll process it in detail later
                if 'alert' in sanitized_conf:
                    alert_preview = sanitized_conf['alert']
                    if isinstance(alert_preview, str) and len(alert_preview) > 100:
                        sanitized_conf['alert'] = f"{alert_preview[:100]}... (truncated, {len(alert_preview)} chars)"

                # Log the sanitized conf
                conf_json = json.dumps(sanitized_conf, indent=2)
                if len(conf_json) > 5000:
                    hook.debug(f"Full dag_run.conf (truncated): {conf_json[:5000]}...")
                else:
                    hook.debug(f"Full dag_run.conf: {conf_json}")
            except Exception as e:
                hook.warning(f"Error logging conf contents: {str(e)}")
        else:
            hook.error("No DAG run found in context")
            hook.close_worklog()
            return None

        # Get the alert JSON string safely
        alert_json_str = conf.get('alert')

        # Check if alert_json_str exists and is not empty
        if not alert_json_str:
            error_msg = "No alert found in DAG run configuration"
            hook.error(error_msg)
            logger.error(error_msg)

            # Log the available conf keys to help debugging
            available_keys = list(conf.keys()) if conf else []
            hook.warning(f"Available conf keys: {available_keys}")
            logger.warning(f"Available conf keys: {available_keys}")

            # Log more details about conf contents for debugging
            if conf:
                hook.debug("Contents of conf keys:")
                for key, value in conf.items():
                    try:
                        value_type = type(value).__name__
                        value_preview = str(value)
                        if len(value_preview) > 100:
                            value_preview = f"{value_preview[:100]}... (truncated, {len(value_preview)} chars)"
                        hook.debug(f"  - {key} ({value_type}): {value_preview}")
                    except Exception as e:
                        hook.debug(f"  - {key}: Error getting value: {str(e)}")

            hook.close_worklog()
            return None

        # Log details about the alert_json_str before parsing
        hook.info(f"Alert JSON string found, type: {type(alert_json_str).__name__}, length: {len(alert_json_str) if isinstance(alert_json_str, str) else 'N/A'}")
        if isinstance(alert_json_str, str):
            preview_length = min(200, len(alert_json_str))
            hook.debug(f"Alert JSON preview: {alert_json_str[:preview_length]}...")

        # Process the alert
        try:
            # Parse the alert JSON string
            hook.info("Parsing alert JSON")
            try:
                alert_data = json.loads(alert_json_str)
            except json.JSONDecodeError as e:
                hook.error(f"Failed to parse alert JSON: {str(e)}")
                logger.error(f"Failed to parse alert JSON: {str(e)}")
                hook.debug(f"Raw alert JSON (first 500 chars): {alert_json_str[:500]}")
                raise

            # Log key alert information
            hook.info("Alert received from notification system")

            # Log key alert properties with safe dictionary access
            if not isinstance(alert_data, dict):
                hook.warning(f"Alert data is not a dictionary, it's a {type(alert_data)}")
                hook.debug(f"Alert content: {alert_data}")
                return worklog_id

            # Extract and log key alert information with safe dictionary access
            fingerprint = alert_data.get('fingerPrint', alert_data.get('oscar_fingerprint', 'unknown'))
            hook.info(f"Alert fingerprint: {fingerprint}")

            # Extract labels safely
            labels = alert_data.get('labels', {})
            if labels and isinstance(labels, dict):
                hook.info(f"Alert name: {labels.get('alertname', 'unknown')}")
                hook.info(f"Alert severity: {labels.get('severity', 'unknown')}")

                # Log other important labels
                for key in ['instance', 'job', 'service', 'team', 'category']:
                    if key in labels:
                        hook.info(f"Alert {key}: {labels[key]}")
            elif labels:
                hook.warning(f"Labels is not a dictionary: {type(labels)}")

            # Extract annotations safely
            annotations = alert_data.get('annotations', {})
            if annotations and isinstance(annotations, dict):
                hook.info(f"Alert summary: {annotations.get('summary', 'No summary provided')}")
                description = annotations.get('description')
                if description:
                    hook.info(f"Alert description: {description}")
            elif annotations:
                hook.warning(f"Annotations is not a dictionary: {type(annotations)}")

            # Log alert status and timing with safe access
            hook.info(f"Alert status: {alert_data.get('status', 'unknown')}")
            hook.info(f"Alert started at: {alert_data.get('startsAt', 'unknown')}")

            # Log full alert data at debug level, with size limit check
            alert_json = json.dumps(alert_data, indent=2)
            if len(alert_json) > 10000:  # Limit very large alerts to prevent worklog overload
                hook.debug(f"Full alert data (truncated): {alert_json[:10000]}...")
            else:
                hook.debug(f"Full alert data: {alert_json}")

            hook.info("Alert processing completed successfully")

        except json.JSONDecodeError as e:
            hook.error(f"Failed to parse alert JSON: {str(e)}")
            logger.error(f"Failed to parse alert JSON: {str(e)}")
            # Show a preview of the problematic JSON instead of the full string
            if isinstance(alert_json_str, str):
                preview = alert_json_str[:500] + "..." if len(alert_json_str) > 500 else alert_json_str
                hook.debug(f"Raw alert JSON preview: {preview}")
                logger.debug(f"Raw alert JSON preview: {preview}")
            else:
                hook.error(f"alert_json_str is not a string: {type(alert_json_str)}")

        except Exception as e:
            hook.error(f"Error processing alert: {str(e)}")
            logger.exception("Error processing alert")
            # Include exception type in logs for better debugging
            hook.error(f"Exception type: {type(e).__name__}")

    except Exception as outer_e:
        # Catch any exceptions in the outer scope (worklog creation, etc.)
        logger.exception(f"Critical error in alert processing: {str(outer_e)}")

        # Try to log to worklog if available, otherwise just log to airflow logs
        if hook:
            try:
                hook.error(f"Critical error in alert processing: {str(outer_e)}")
                hook.error(f"Exception type: {type(outer_e).__name__}")
            except:
                pass

    finally:
        # Close the worklog if it was created
        if hook:
            try:
                hook.info("Closing alert processing worklog")
                closed_worklog = hook.close_worklog()
                logger.info(f"Closed worklog with ID: {closed_worklog.get('id') if isinstance(closed_worklog, dict) else None}")

                # Store the worklog ID in XCom for later tasks if context is available
                if context and 'ti' in context and context['ti']:
                    context['ti'].xcom_push(key='worklog_id', value=closed_worklog.get('id') if isinstance(closed_worklog, dict) else None)
            except Exception as close_e:
                logger.error(f"Error closing worklog: {str(close_e)}")

        return worklog_id


# Create the DAG - note the schedule=None to ensure it only runs when triggered externally
with DAG(
    'alert_handler_demo',
    default_args=default_args,
    description='Demo DAG for handling alerts from the notification system',
    schedule=None,  # This DAG will only be triggered externally
    start_date=pendulum.today('UTC').add(days=-1),
    tags=['demo', 'alert', 'worklog'],
    catchup=False,  # Don't run for historical dates
) as dag:

    # Define the task to process the alert
    process_alert_task = PythonOperator(
        task_id='process_alert',
        python_callable=process_alert,
    )

    # The DAG consists of a single task for now
    # Additional tasks could be added later for more complex processing
    process_alert_task

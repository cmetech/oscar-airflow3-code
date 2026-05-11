from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import base64
from hooks.notify_hook import NotifyHook  # type: ignore
from hooks.worklog_hook import WorkLogHook  # type: ignore

logger = logging.getLogger(__name__)


def test_notifications():
    """Test sending notifications with and without attachments."""
    worklog_hook = None
    worklog_id = None
    
    try:
        # Initialize hooks
        worklog_hook = WorkLogHook()
        notify_hook = NotifyHook()
        
        # Create worklog
        logger.info("Creating worklog for notification testing")
        worklog = worklog_hook.create_worklog(
            name="Test Notifications DAG",
            description="Testing email notifications with and without attachments"
        )
        worklog_id = worklog["id"]
        worklog_hook.info("Starting notification tests")
        
        # Step 1: Send regular notification without attachment
        worklog_hook.info("Step 1: Sending regular notification without attachment")
        try:
            regular_payload = {
                "name": "mail_notifier",  # Changed to 'mail_notifier' as default
                "subject": "Test Email Notification - No Attachment",
                "message": "This is a test email notification sent from an Airflow DAG without any attachments.",
                "recipients": "corey.m.ellis@gmail.com,corey.m.ellis@icloud.com"
            }
            
            worklog_hook.info(f"Sending regular notification payload: {regular_payload}")
            response = notify_hook.send_notification(regular_payload)
            worklog_hook.info(f"Regular notification sent successfully. Response: {response}")
            logger.info(f"Regular notification response: {response}")
            
        except Exception as e:
            error_msg = f"Failed to send regular notification: {str(e)}"
            worklog_hook.error(error_msg)
            logger.error(error_msg)
            raise
        
        # Step 2: Send notification with attachment
        worklog_hook.info("Step 2: Sending notification with attachment")
        try:
            # Create sample CSV content
            csv_content = """timestamp,server,metric,value
2025-01-06 10:00:00,server01,cpu,45.2
2025-01-06 10:15:00,server01,cpu,48.7
2025-01-06 10:30:00,server01,cpu,42.1
2025-01-06 10:00:00,server01,memory,72.5
2025-01-06 10:15:00,server01,memory,73.8
2025-01-06 10:30:00,server01,memory,71.2"""
            
            # Base64 encode the content
            encoded_content = base64.b64encode(csv_content.encode()).decode()
            
            attachment_payload = {
                "name": "mail_notifier",  # Changed to 'mail_notifier' as default
                "subject": "Test Email Notification - With Attachment",
                "message": "This is a test email notification sent from an Airflow DAG with a CSV attachment.",
                "recipients": "corey.m.ellis@gmail.com,corey.m.ellis@icloud.com",
                "attachments": [
                    {
                        "filename": "test_metrics_report.csv",
                        "content": encoded_content,
                        "content_type": "text/csv"
                    }
                ]
            }
            
            worklog_hook.info(f"Sending notification with attachment. Payload size: {len(str(attachment_payload))} bytes")
            response = notify_hook.send_notification(attachment_payload)
            worklog_hook.info(f"Notification with attachment sent successfully. Response: {response}")
            logger.info(f"Attachment notification response: {response}")
            
        except Exception as e:
            error_msg = f"Failed to send notification with attachment: {str(e)}"
            worklog_hook.error(error_msg)
            logger.error(error_msg)
            raise
        
        # Success
        worklog_hook.info("All notification tests completed successfully")
        
    except Exception as e:
        # Log overall failure
        error_msg = f"Notification testing failed: {str(e)}"
        if worklog_hook:
            worklog_hook.error(error_msg)
        logger.error(error_msg)
        raise
        
    finally:
        # Always close the worklog
        if worklog_hook and worklog_id:
            try:
                worklog_hook.info("Closing worklog")
                worklog_hook.close_worklog(worklog_id)
                logger.info(f"Worklog {worklog_id} closed successfully")
            except Exception as e:
                logger.error(f"Failed to close worklog {worklog_id}: {str(e)}")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0
}

with DAG(
    "test_notification_dag",
    default_args=default_args,
    description="Test email notifications with and without attachments",
    schedule="@once",
    catchup=False,
    tags=["test", "notification", "email"]
) as dag:
    test_notification_task = PythonOperator(
        task_id="test_notifications",
        python_callable=test_notifications
    )

    test_notification_task

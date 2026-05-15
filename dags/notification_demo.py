from datetime import datetime, timedelta
import uuid
import json
import logging
from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from hooks.notify_hook import NotifyHook  # type: ignore
from hooks.worklog_hook import WorkLogHook, WorkLogType, SeverityLevel  # type: ignore

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'notification_demo',
    default_args=default_args,
    description='A demo DAG that exercises the NotifyHook and logs to WorkLog',
    schedule=None,
    start_date=pendulum.today('UTC').add(days=-1),
    tags=['demo', 'notification', 'worklog'],
)

# Function to create a worklog for this demo


def create_demo_worklog(**context):
    worklog_hook = WorkLogHook()
    worklog = worklog_hook.create_worklog(
        name="Notification Demo Worklog",
        description="A worklog to track the notification demo execution",
        worklog_type=WorkLogType.DB,
        metadata=[{"key": "demo_type", "value": "notification"}]
    )

    # Store the worklog ID in XCom for other tasks to use
    context['task_instance'].xcom_push(key='worklog_id', value=worklog['id'])

    worklog_hook.info(f"Created worklog for notification demo: {worklog['id']}")
    return worklog['id']

# Function to create a notification


def create_notification(**context):
    worklog_id = context['task_instance'].xcom_pull(task_ids='create_demo_worklog', key='worklog_id')
    worklog_hook = WorkLogHook(worklog_id=worklog_id)

    notify_hook = NotifyHook()

    # Create a notification with the correct payload structure
    # The API expects specific fields and formats
    notification_payload = {
        "user_id": "demo_user",
        "notification_type": "EMAIL",
        "subject": "Notification Demo Test",
        "body": "This is a test notification created by the notification_demo DAG",
        "base_template_id": "default_template",  # Added a default template ID
        "meta_data": {  # Changed from metadata to meta_data to match API expectation
            "demo": True,
            "created_by": "airflow",
            "worklog_id": worklog_id
        },
        "max_reminders": 3,  # Added required field
        "expires_at": (datetime.now() + timedelta(days=1)).isoformat()  # Added required field
    }

    worklog_hook.info(f"Creating notification with payload: {json.dumps(notification_payload)}")

    try:
        notification = notify_hook.create_notification(notification_payload)
        worklog_hook.info(f"Successfully created notification with ID: {notification['id']}")

        # Store the notification ID in XCom for other tasks to use
        context['task_instance'].xcom_push(key='notification_id', value=notification['id'])

        return notification['id']
    except Exception as e:
        worklog_hook.error(f"Failed to create notification: {str(e)}")
        raise

# Function to get a notification


def get_notification(**context):
    worklog_id = context['task_instance'].xcom_pull(task_ids='create_demo_worklog', key='worklog_id')
    notification_id = context['task_instance'].xcom_pull(task_ids='create_notification', key='notification_id')

    worklog_hook = WorkLogHook(worklog_id=worklog_id)
    notify_hook = NotifyHook()

    worklog_hook.info(f"Retrieving notification with ID: {notification_id}")

    try:
        notification = notify_hook.get_notification(notification_id)
        worklog_hook.info(f"Successfully retrieved notification: {json.dumps(notification)}")
        return notification
    except Exception as e:
        worklog_hook.error(f"Failed to retrieve notification: {str(e)}")
        raise

# Function to update a notification


def update_notification(**context):
    worklog_id = context['task_instance'].xcom_pull(task_ids='create_demo_worklog', key='worklog_id')
    notification_id = context['task_instance'].xcom_pull(task_ids='create_notification', key='notification_id')

    worklog_hook = WorkLogHook(worklog_id=worklog_id)
    notify_hook = NotifyHook()

    # Update the notification with the correct field name
    update_payload = {
        "meta_data": {  # Changed from metadata to meta_data to match API expectation
            "updated": True,
            "updated_at": datetime.now().isoformat(),
            "updated_by": "airflow"
        }
    }

    worklog_hook.info(f"Updating notification {notification_id} with payload: {json.dumps(update_payload)}")

    try:
        updated_notification = notify_hook.update_notification(notification_id, update_payload)
        worklog_hook.info(f"Successfully updated notification: {json.dumps(updated_notification)}")
        return updated_notification
    except Exception as e:
        worklog_hook.error(f"Failed to update notification: {str(e)}")
        raise

# Function to list notifications


def list_notifications(**context):
    worklog_id = context['task_instance'].xcom_pull(task_ids='create_demo_worklog', key='worklog_id')

    worklog_hook = WorkLogHook(worklog_id=worklog_id)
    notify_hook = NotifyHook()

    worklog_hook.info("Listing notifications")

    try:
        notifications = notify_hook.list_notifications(
            user_id="demo_user",
            status="PENDING"
        )
        worklog_hook.info(f"Successfully listed notifications: {json.dumps(notifications)}")
        return notifications
    except Exception as e:
        worklog_hook.error(f"Failed to list notifications: {str(e)}")
        raise

# Function to resend a notification


def resend_notification(**context):
    worklog_id = context['task_instance'].xcom_pull(task_ids='create_demo_worklog', key='worklog_id')
    notification_id = context['task_instance'].xcom_pull(task_ids='create_notification', key='notification_id')

    worklog_hook = WorkLogHook(worklog_id=worklog_id)
    notify_hook = NotifyHook()

    worklog_hook.info(f"Resending notification with ID: {notification_id}")

    try:
        resent_notification = notify_hook.resend_notification(notification_id, force=True)
        worklog_hook.info(f"Successfully resent notification: {json.dumps(resent_notification)}")
        return resent_notification
    except Exception as e:
        worklog_hook.error(f"Failed to resend notification: {str(e)}")
        raise

# Function to respond to a notification


def respond_notification(**context):
    worklog_id = context['task_instance'].xcom_pull(task_ids='create_demo_worklog', key='worklog_id')
    notification_id = context['task_instance'].xcom_pull(task_ids='create_notification', key='notification_id')

    worklog_hook = WorkLogHook(worklog_id=worklog_id)
    notify_hook = NotifyHook()

    worklog_hook.info(f"Marking notification {notification_id} as responded")

    try:
        responded_notification = notify_hook.respond_notification(notification_id)
        worklog_hook.info(f"Successfully marked notification as responded: {json.dumps(responded_notification)}")
        return responded_notification
    except Exception as e:
        worklog_hook.error(f"Failed to mark notification as responded: {str(e)}")
        raise

# Function to escalate a notification


def escalate_notification(**context):
    worklog_id = context['task_instance'].xcom_pull(task_ids='create_demo_worklog', key='worklog_id')
    notification_id = context['task_instance'].xcom_pull(task_ids='create_notification', key='notification_id')

    worklog_hook = WorkLogHook(worklog_id=worklog_id)
    notify_hook = NotifyHook()

    worklog_hook.info(f"Escalating notification with ID: {notification_id}")

    try:
        escalated_notification = notify_hook.escalate_notification(notification_id)
        worklog_hook.info(f"Successfully escalated notification: {json.dumps(escalated_notification)}")
        return escalated_notification
    except Exception as e:
        worklog_hook.error(f"Failed to escalate notification: {str(e)}")
        raise

# Function to cancel a notification


def cancel_notification(**context):
    worklog_id = context['task_instance'].xcom_pull(task_ids='create_demo_worklog', key='worklog_id')
    notification_id = context['task_instance'].xcom_pull(task_ids='create_notification', key='notification_id')

    worklog_hook = WorkLogHook(worklog_id=worklog_id)
    notify_hook = NotifyHook()

    worklog_hook.info(f"Canceling notification with ID: {notification_id}")

    try:
        canceled_notification = notify_hook.cancel_notification(notification_id, reason="Demo completed")
        worklog_hook.info(f"Successfully canceled notification: {json.dumps(canceled_notification)}")
        return canceled_notification
    except Exception as e:
        worklog_hook.error(f"Failed to cancel notification: {str(e)}")
        raise

# Function to close the worklog


def close_worklog(**context):
    worklog_id = context['task_instance'].xcom_pull(task_ids='create_demo_worklog', key='worklog_id')

    worklog_hook = WorkLogHook(worklog_id=worklog_id)

    worklog_hook.info(f"Closing worklog with ID: {worklog_id}")

    try:
        closed_worklog = worklog_hook.close_worklog()
        logger.info(f"Successfully closed worklog: {json.dumps(closed_worklog)}")
        return closed_worklog
    except Exception as e:
        logger.error(f"Failed to close worklog: {str(e)}")
        raise


# Create tasks
create_worklog_task = PythonOperator(
    task_id='create_demo_worklog',
    python_callable=create_demo_worklog,
    dag=dag,
)

create_notification_task = PythonOperator(
    task_id='create_notification',
    python_callable=create_notification,
    dag=dag,
)

get_notification_task = PythonOperator(
    task_id='get_notification',
    python_callable=get_notification,
    dag=dag,
)

update_notification_task = PythonOperator(
    task_id='update_notification',
    python_callable=update_notification,
    dag=dag,
)

list_notifications_task = PythonOperator(
    task_id='list_notifications',
    python_callable=list_notifications,
    dag=dag,
)

resend_notification_task = PythonOperator(
    task_id='resend_notification',
    python_callable=resend_notification,
    dag=dag,
)

respond_notification_task = PythonOperator(
    task_id='respond_notification',
    python_callable=respond_notification,
    dag=dag,
)

escalate_notification_task = PythonOperator(
    task_id='escalate_notification',
    python_callable=escalate_notification,
    dag=dag,
)

cancel_notification_task = PythonOperator(
    task_id='cancel_notification',
    python_callable=cancel_notification,
    dag=dag,
)

close_worklog_task = PythonOperator(
    task_id='close_worklog',
    python_callable=close_worklog,
    dag=dag,
)

# Set task dependencies
create_worklog_task >> create_notification_task

# Set up parallel tasks after create_notification
create_notification_task >> get_notification_task
create_notification_task >> update_notification_task
create_notification_task >> list_notifications_task

# Set up the main task chain
get_notification_task >> resend_notification_task
resend_notification_task >> respond_notification_task
respond_notification_task >> escalate_notification_task
escalate_notification_task >> cancel_notification_task
cancel_notification_task >> close_worklog_task

# Ensure worklog_id is available to all tasks
for task in [get_notification_task, update_notification_task, list_notifications_task,
             resend_notification_task, respond_notification_task, escalate_notification_task,
             cancel_notification_task, close_worklog_task]:
    task.set_upstream(create_worklog_task)

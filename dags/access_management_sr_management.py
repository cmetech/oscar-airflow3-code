from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from hooks.worklog_hook import WorkLogHook, WorkLogType
from hooks.access_management_db_hook import AccessManagementSQLHook
from airflow.hooks.base import BaseHook
from typing import Dict, Any
import os
import re
import json
import uuid
import logging
from helpers.access_management_activity_track_helper import update_or_create_request_activity

from helpers.access_management_sr_helper import (
    handle_service_account_modification,
    handle_user_account_modification,
    handle_disable_service_account,
    handle_disable_user_account,
    handle_other_requests,
)

from helpers.access_management_sr_helper_other_requests_flow import (
    fetch_activity_info_other_requests,
    sr_creation_other_requests,
    sr_worklog_update_other_requests,
    sr_creation_activity_update_other_requests,
    sr_creation_email_notification_other_requests,
    sr_creation_failed_email_notification_other_requests,
    sr_activity_status_update_process_sr_error_other_requests,
)

from helpers.access_management_sr_helper_password_reset_flow import (
    fetch_activity_info_user_password_reset,
    ad_connect_user_password_reset,
    sr_creation_password_reset,
    sr_worklog_update_password_reset,
    sr_creation_activity_update_password_reset,
    sr_creation_email_notification_password_reset,
    user_inactive_ad_activity_status_update_complete_password_reset,
    send_communication_user_inactive_ad_activity_status_update_complete_password_reset,
    ad_activity_status_update_process_error_password_reset,
    send_email_ad_activity_status_update_process_error_password_reset,
    sr_creation_failed_activity_update_user_password_reset,
    sr_creation_failed_email_notification_user_password_reset,
)

from helpers.access_management_sr_helper_new_service_acc_flow import (
    check_service_account_name_new_service_account,
    fetch_data_activity_info_new_service_account,
    ad_process_new_service_account,
    sr_creation_new_service_account,
    sr_worklog_update_new_service_account,
    sr_creation_activity_update_new_service_account,
    fetch_itsm_approver_new_service_account,
    send_communication_sr_creation_new_service_account,
    existing_service_account_activity_update_new_service_account,
    send_communication_existing_service_account_error_new_service_account,
    update_activity_status_ad_process_error_new_service_account,
    send_communication_ad_process_error_new_service_account,
    sr_creation_failed_activity_update_new_service_account,
    sr_creation_failed_email_notification_new_service_account,
    send_comm_service_account_name_change,
)

from helpers.access_management_sr_helper_new_user_acc_flow import (
    fetch_data_activity_info_new_user_account,
    ad_process_new_user_account,
    sr_creation_new_user_account,
    sr_worklog_update_new_user_account,
    sr_creation_activity_update_new_user_account,
    send_communication_sr_creation_new_user_account,
    send_communication_ad_process_existing_user_error_new_user_account,
    sr_creation_activity_update_ad_process_user_exists_error_new_user_account,
    sr_creation_activity_update_ad_process_errored_new_user_account,
    send_communication_ad_process_errored_new_user_account,
    fetch_itsm_approver_new_user_account,
    sr_creation_failed_activity_update_new_user_account,
    sr_creation_failed_email_notification_new_user_account,
)

from helpers.access_management_sr_helper_user_account_modify_flow import (
    fetch_data_activity_info_user_account_modify,
    ad_connect_user_account_modification,
    sr_creation_user_account_modify,
    sr_worklog_update_user_account_modify,
    sr_creation_activity_update_user_account_modify,
    fetch_itsm_approver_user_account_modify,
    send_communication_sr_creation_user_account_modify,
    sr_activity_status_update_existing_user_add_user_account_modify,
    send_communication_existing_user_add_error_user_account_modify,
    sr_activity_status_update_non_existing_user_remove_user_account_modify,
    send_communication_non_existing_user_remove_error_user_account_modify,
    sr_creation_activity_update_ad_process_errored_user_account_modify,
    send_communication_ad_process_errored_user_account_modify,
    sr_creation_failed_activity_update_user_account_modify,
    sr_creation_failed_email_notification_user_account_modify
)

from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Generate a unique task ID for this run
task_id = f"WORKLOG-TEST-{uuid.uuid4().hex[:8]}"

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

    # Create the worklog
    worklog = hook.create_worklog(
        name="Access Management Service Request Worklog",
        description="Worklog for access management service request processing",
        worklog_type=WorkLogType.DB,
        metadata=metadata
    )

    logger.info(f"Created worklog with ID: {worklog['id']}")

    # Add some initial entries
    hook.info("Starting access management service request processing")

    # Store the worklog ID in XCom for later tasks
    context['ti'].xcom_push(key='worklog_id', value=worklog['id'])

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

def branch_based_on_email_subject(**context):
    """
    Branch based on the email subject to determine which task group to execute.
    Returns the task_group_id of the appropriate handler group.
    """
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    ti = context['ti']
    result = ti.xcom_pull(task_ids='update_or_create_request_activity')
    email_subject = result['data']['email_subject']

    hook.info(f"Determining branch based on email subject: {email_subject}")

    # Determine the branch
    if "User Account Password Reset".lower() in email_subject.lower() or "User Password Reset".lower() in email_subject.lower():
        activity_branch = 'user_password_reset_group'
        hook.info("Selected branch: User Password Reset")
    elif "New User Account Creation".lower() in email_subject.lower() or "New User Account".lower() in email_subject.lower():
        activity_branch = 'new_user_account_group'
        hook.info("Selected branch: New User Account Creation")
    elif "New Service Account Creation".lower() in email_subject.lower() or "New Service Account".lower() in email_subject.lower():
        activity_branch = 'new_service_account_group'
        hook.info("Selected branch: New Service Account Creation")
    elif "Service Account Modification".lower() in email_subject.lower() or "Service Account Modify".lower() in email_subject.lower():
        activity_branch = 'service_account_modification_group'
        hook.info("Selected branch: Service Account Modification")
    elif "User Account Modification".lower() in email_subject.lower() or "User Account Modify".lower() in email_subject.lower():
        activity_branch = 'user_account_modification_group'
        hook.info("Selected branch: User Account Modification")
    elif "Disable Service Account".lower() in email_subject.lower():
        activity_branch = 'disable_service_account_group'
        hook.info("Selected branch: Disable Service Account")
    elif "Disable User Account".lower() in email_subject.lower():
        activity_branch = 'disable_user_account_group'
        hook.info("Selected branch: Disable User Account")
    else:
        # Handle both Service Account Password Reset and other requests in the same way
        activity_branch = 'other_requests_group'
        if "Service Account Password Reset".lower() in email_subject.lower() or "ServiceAc Passwd Reset".lower() in email_subject.lower():
            hook.info("Selected branch: Service Account Password Reset (handled by other_requests_group)")
        else:
            hook.info("Selected branch: Other Requests")

    # Add activity_branch to the result data
    result['data']['activity_branch'] = activity_branch

    # Push the updated result back to XCom
    ti.xcom_push(key='activity_data', value=result)

    hook.info(f"Branching complete. Selected task group: {activity_branch}")

    return activity_branch

with DAG(
    'access_management_sr_management',
    default_args=default_args,
    description='Automated access management service request tasks',
    schedule=None,
    catchup=False,
) as dag:

    create_worklog_task = PythonOperator(
        task_id='create_worklog',
        python_callable=create_worklog,
    )

    update_or_create_request_activity_task = PythonOperator(
        task_id='update_or_create_request_activity',
        python_callable=update_or_create_request_activity,
        do_xcom_push=True
    )

    # Branching task
    branch_by_activity = BranchPythonOperator(
        task_id='branch_based_on_email_subject',
        python_callable=branch_based_on_email_subject,
        dag=dag
    )

    # 1 User Password Reset Group
    with TaskGroup(group_id='user_password_reset_group') as user_password_reset_group:
        # First task: Fetch activity data
        fetch_data_activity_info_user_password_reset = PythonOperator(
            task_id='fetch_data_activity_info_user_password_reset',
            python_callable=fetch_activity_info_user_password_reset,
        )

        # Second task: AD Connect and process execution
        ad_connect_user_password_reset_task = PythonOperator(
            task_id='ad_connect_user_password_reset',
            python_callable=ad_connect_user_password_reset,
            retries=12,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=20)
        )

        # Branch based on AD output
        def branch_based_on_ad_output_user_password_reset(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='user_password_reset_group.ad_connect_user_password_reset', key='activity_data')
            ad_output = activity_data['data']['ad_output_user_password_reset']

            if 'User Account is not disabled'.lower() in ad_output.lower():
                return 'user_password_reset_group.sr_creation_password_reset'
            else:
                return 'user_password_reset_group.branch_on_ad_output_ad_failure'

        branch_on_ad_output = BranchPythonOperator(
            task_id='branch_on_ad_output',
            python_callable=branch_based_on_ad_output_user_password_reset,
        )

        # Success path - SR creation for password reset
        sr_creation_password_reset_task = PythonOperator(
            task_id='sr_creation_password_reset',
            python_callable=sr_creation_password_reset,
        )

        def branch_based_on_sr_creation_success_user_password_reset(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='user_password_reset_group.sr_creation_password_reset', key='activity_data')
            sr_creation_status = activity_data['data']['sr_creation_result'].get('status', '')

            if 'failed'.lower() in sr_creation_status.lower():
                return 'user_password_reset_group.sr_creation_failed_activity_update_user_password_reset'
            else:
                return 'user_password_reset_group.sr_worklog_update_password_reset'

        branch_based_on_sr_creation_success_user_password_reset_task = BranchPythonOperator(
            task_id='branch_based_on_sr_creation_success_user_password_reset',
            python_callable=branch_based_on_sr_creation_success_user_password_reset,
        )

        sr_worklog_update_password_reset_task = PythonOperator(
            task_id='sr_worklog_update_password_reset',
            python_callable=sr_worklog_update_password_reset,
        )

        sr_creation_activity_update_password_reset_task = PythonOperator(
            task_id='sr_creation_activity_update_password_reset',
            python_callable=sr_creation_activity_update_password_reset,
        )

        sr_creation_email_update_password_reset_task = PythonOperator(
            task_id='sr_creation_email_update_password_reset',
            python_callable=sr_creation_email_notification_password_reset,
        )

        # SR creation failed flow
        sr_creation_failed_activity_update_user_password_reset_task = PythonOperator(
            task_id='sr_creation_failed_activity_update_user_password_reset',
            python_callable=sr_creation_failed_activity_update_user_password_reset,
        )

        sr_creation_failed_email_notification_user_password_reset_task = PythonOperator(
            task_id='sr_creation_failed_email_notification_user_password_reset',
            python_callable=sr_creation_failed_email_notification_user_password_reset,
        )

        # Branch based on AD output
        def branch_on_ad_output_ad_failure(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='user_password_reset_group.ad_connect_user_password_reset', key='activity_data')
            ad_output = activity_data['data']['ad_output_user_password_reset']

            if 'automatically disabled'.lower() in ad_output.lower():
                return 'user_password_reset_group.user_inactive_ad_activity_status_update_complete_password_reset'
            else:
                return 'user_password_reset_group.ad_activity_status_update_process_error_password_reset'

        # Error path - handle AD failure
        branch_on_ad_output_ad_failure_task = PythonOperator(
            task_id='branch_on_ad_output_ad_failure',
            python_callable=branch_on_ad_output_ad_failure,
        )

        user_inactive_ad_activity_status_update_complete_password_reset_task = PythonOperator(
            task_id='user_inactive_ad_activity_status_update_complete_password_reset',
            python_callable=user_inactive_ad_activity_status_update_complete_password_reset,
        )

        send_communication_user_inactive_ad_activity_status_update_complete_password_reset_task = PythonOperator(
            task_id='send_communication_user_inactive_ad_activity_status_update_complete_password_reset',
            python_callable=send_communication_user_inactive_ad_activity_status_update_complete_password_reset,
        )

        ad_activity_status_update_process_error_password_reset_task = PythonOperator(
            task_id='ad_activity_status_update_process_error_password_reset',
            python_callable=ad_activity_status_update_process_error_password_reset,
        )

        send_email_ad_activity_status_update_process_error_password_reset_task = PythonOperator(
            task_id='send_email_ad_activity_status_update_process_error_password_reset',
            python_callable=send_email_ad_activity_status_update_process_error_password_reset,
        )

        # Set task dependencies within the group
        fetch_data_activity_info_user_password_reset >> ad_connect_user_password_reset_task >> branch_on_ad_output >> sr_creation_password_reset_task >> branch_based_on_sr_creation_success_user_password_reset_task >> sr_worklog_update_password_reset_task >> sr_creation_activity_update_password_reset_task >> sr_creation_email_update_password_reset_task
        fetch_data_activity_info_user_password_reset >> ad_connect_user_password_reset_task >> branch_on_ad_output >> sr_creation_password_reset_task >> branch_based_on_sr_creation_success_user_password_reset_task >> sr_creation_failed_activity_update_user_password_reset_task >> sr_creation_failed_email_notification_user_password_reset_task
        fetch_data_activity_info_user_password_reset >> ad_connect_user_password_reset_task >> branch_on_ad_output >> branch_on_ad_output_ad_failure_task >> user_inactive_ad_activity_status_update_complete_password_reset_task >> send_communication_user_inactive_ad_activity_status_update_complete_password_reset_task
        fetch_data_activity_info_user_password_reset >> ad_connect_user_password_reset_task >> branch_on_ad_output >> branch_on_ad_output_ad_failure_task >> ad_activity_status_update_process_error_password_reset_task >> send_email_ad_activity_status_update_process_error_password_reset_task

    # Service Account Password Reset is handled by other_requests_group
    # When email subject contains "Service Account Password Reset" or "ServiceAc Passwd Reset",
    # the branch_based_on_email_subject function routes it to other_requests_group

    # 2 New User Account Group
    with TaskGroup(group_id='new_user_account_group') as new_user_account_group:
        # Fetch activity data
        fetch_data_activity_info_new_user_account_task = PythonOperator(
            task_id='fetch_data_activity_info_new_user_account',
            python_callable=fetch_data_activity_info_new_user_account,
        )

        ad_process_new_user_account_task = PythonOperator(
            task_id='ad_connect_new_user_account',
            python_callable=ad_process_new_user_account,
            retries=12,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=20)
        )

        # Branch based on AD output
        def branch_based_on_ad_output_new_user_account(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_user_account_group.ad_connect_new_user_account', key='activity_data')
            ad_output = activity_data['data']['ad_output_new_user_account']

            if 'doesn\'t exist in AD'.lower() in ad_output.lower():
                return 'new_user_account_group.sr_creation_new_user_account'
            elif 'Name exist in AD'.lower() in ad_output.lower():
                return 'new_user_account_group.send_communication_ad_process_existing_user_error_new_user_account'
            else:
                return 'new_user_account_group.sr_creation_activity_update_ad_process_errored_new_user_account'

        branch_on_ad_output_new_user_account_task = BranchPythonOperator(
            task_id='branch_on_ad_output_new_user_account',
            python_callable=branch_based_on_ad_output_new_user_account,
        )

        # SR creation flow when new user account is not present in AD.
        sr_creation_new_user_account_task = PythonOperator(
            task_id='sr_creation_new_user_account',
            python_callable=sr_creation_new_user_account,
        )

        def branch_based_on_sr_creation_success_new_user_account(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_user_account_group.sr_creation_new_user_account', key='activity_data')
            sr_creation_status = activity_data['data']['sr_creation_result'].get('status', '')

            if 'failed'.lower() in sr_creation_status.lower():
                return 'new_user_account_group.sr_creation_failed_activity_update_new_user_account'
            else:
                return 'new_user_account_group.sr_worklog_update_new_user_account'

        branch_based_on_sr_creation_success_new_user_account_task = BranchPythonOperator(
            task_id='branch_based_on_sr_creation_success_new_user_account',
            python_callable=branch_based_on_sr_creation_success_new_user_account,
        )

        sr_worklog_update_new_user_account_task = PythonOperator(
            task_id='sr_worklog_update_new_user_account',
            python_callable=sr_worklog_update_new_user_account,
        )

        sr_creation_activity_update_new_user_account_task = PythonOperator(
            task_id='sr_creation_activity_update_new_user_account',
            python_callable=sr_creation_activity_update_new_user_account,
        )

        fetch_itsm_approver_new_user_account_task = PythonOperator(
            task_id='fetch_itsm_approver_new_user_account',
            python_callable=fetch_itsm_approver_new_user_account,
        )

        send_communication_sr_creation_new_user_account_task = PythonOperator(
            task_id='send_communication_sr_creation_new_user_account',
            python_callable=send_communication_sr_creation_new_user_account,
        )

        # AD process success flow when new user account is present in AD.
        send_communication_ad_process_existing_user_error_new_user_account_task = PythonOperator(
            task_id='send_communication_ad_process_existing_user_error_new_user_account',
            python_callable=send_communication_ad_process_existing_user_error_new_user_account,
        )

        sr_creation_activity_update_ad_process_user_exists_error_new_user_account_task = PythonOperator(
            task_id='sr_creation_activity_update_ad_process_user_exists_error_new_user_account',
            python_callable=sr_creation_activity_update_ad_process_user_exists_error_new_user_account,
        )

        # AD process errored flow when AD process fails during new user account creation.
        sr_creation_activity_update_ad_process_errored_new_user_account_task = PythonOperator(
            task_id='sr_creation_activity_update_ad_process_errored_new_user_account',
            python_callable=sr_creation_activity_update_ad_process_errored_new_user_account,
        )

        send_communication_ad_process_errored_new_user_account_task = PythonOperator(
            task_id='send_communication_ad_process_errored_new_user_account',
            python_callable=send_communication_ad_process_errored_new_user_account,
        )

        # SR creation failed flow
        sr_creation_failed_activity_update_new_user_account_task = PythonOperator(
            task_id='sr_creation_failed_activity_update_new_user_account',
            python_callable=sr_creation_failed_activity_update_new_user_account,
        )

        sr_creation_failed_email_notification_new_user_account_task = PythonOperator(
            task_id='sr_creation_failed_email_notification_new_user_account',
            python_callable=sr_creation_failed_email_notification_new_user_account,
        )

        fetch_data_activity_info_new_user_account_task >> ad_process_new_user_account_task >> branch_on_ad_output_new_user_account_task >> sr_creation_new_user_account_task >> branch_based_on_sr_creation_success_new_user_account_task >> sr_worklog_update_new_user_account_task >> sr_creation_activity_update_new_user_account_task >> fetch_itsm_approver_new_user_account_task >> send_communication_sr_creation_new_user_account_task
        fetch_data_activity_info_new_user_account_task >> ad_process_new_user_account_task >> branch_on_ad_output_new_user_account_task >> sr_creation_new_user_account_task >> branch_based_on_sr_creation_success_new_user_account_task >> sr_creation_failed_activity_update_new_user_account_task >> sr_creation_failed_email_notification_new_user_account_task
        fetch_data_activity_info_new_user_account_task >> ad_process_new_user_account_task >> branch_on_ad_output_new_user_account_task >> send_communication_ad_process_existing_user_error_new_user_account_task >> sr_creation_activity_update_ad_process_user_exists_error_new_user_account_task
        fetch_data_activity_info_new_user_account_task >> ad_process_new_user_account_task >> branch_on_ad_output_new_user_account_task >> sr_creation_activity_update_ad_process_errored_new_user_account_task >> send_communication_ad_process_errored_new_user_account_task
        # More tasks will be added here
        # Example: fetch_data_activity_info_new_user_account_task >> next_task_in_group

    # 3 New Service Account Group
    with TaskGroup(group_id='new_service_account_group') as new_service_account_group:

        check_service_account_name_task_new_service_account_task = PythonOperator(
            task_id='check_service_account_name_new_service_account',
            python_callable=check_service_account_name_new_service_account,
        )

        def branch_based_on_svc_acc_name_creation_success_new_service_account(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_service_account_group.check_service_account_name_new_service_account', key='activity_data')
            svc_name_creation_failed = activity_data['data']['service_account_name_check'].get('svc_name_creation_failed')
            if svc_name_creation_failed:
                return 'new_service_account_group.send_comm_service_account_name_change'
            else:
                return 'new_service_account_group.fetch_data_activity_info_new_service_account'

        branch_based_on_svc_acc_name_creation_success_new_service_account_task = BranchPythonOperator(
            task_id='branch_based_on_svc_acc_name_creation_success_new_service_account',
            python_callable=branch_based_on_svc_acc_name_creation_success_new_service_account,
        )

        fetch_data_activity_info_new_service_account_task = PythonOperator(
            task_id='fetch_data_activity_info_new_service_account',
            python_callable=fetch_data_activity_info_new_service_account,
        )

        ad_process_new_service_account_task = PythonOperator(
            task_id='ad_process_new_service_account',
            python_callable=ad_process_new_service_account,
            retries=12,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=20)
        )

        def branch_based_on_ad_output_new_service_account(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_service_account_group.ad_process_new_service_account', key='activity_data')
            ad_output = activity_data['data']['ad_output_new_service_account']

            if 'doesn\'t exist in AD'.lower() in ad_output.lower():
                return 'new_service_account_group.sr_creation_new_service_account'
            elif 'account exist in ad'.lower() in ad_output.lower():
                return 'new_service_account_group.existing_service_account_activity_update_new_service_account'
            else:
                return 'new_service_account_group.update_activity_status_ad_process_error_new_service_account'

        branch_on_ad_output_new_service_account_task = BranchPythonOperator(
            task_id='branch_on_ad_output_new_service_account',
            python_callable=branch_based_on_ad_output_new_service_account,
        )

        # SR creation flow when new service account is not present in AD.
        sr_creation_new_service_account_task = PythonOperator(
            task_id='sr_creation_new_service_account',
            python_callable=sr_creation_new_service_account,
        )

        def branch_based_on_sr_creation_new_service_account(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_service_account_group.sr_creation_new_service_account', key='activity_data')
            sr_creation_result_status = activity_data['data']['sr_creation_result']['status']

            if 'Failed'.lower() not in sr_creation_result_status.lower():
                return 'new_service_account_group.sr_worklog_update_new_service_account'
            else:
                return 'new_service_account_group.sr_creation_failed_activity_update_new_service_account'

        branch_based_on_sr_creation_new_service_account_task = BranchPythonOperator(
            task_id='branch_based_on_sr_creation_new_service_account',
            python_callable=branch_based_on_sr_creation_new_service_account,
        )

        # Sr creation success flow.
        sr_worklog_update_new_service_account_task = PythonOperator(
            task_id='sr_worklog_update_new_service_account',
            python_callable=sr_worklog_update_new_service_account,
        )

        sr_creation_activity_update_new_service_account_task = PythonOperator(
            task_id='sr_creation_activity_update_new_service_account',
            python_callable=sr_creation_activity_update_new_service_account,
        )

        fetch_itsm_approver_new_service_account_task = PythonOperator(
            task_id='fetch_itsm_approver_new_service_account',
            python_callable=fetch_itsm_approver_new_service_account,
        )

        send_communication_sr_creation_new_service_account_task = PythonOperator(
            task_id='send_communication_sr_creation_new_service_account',
            python_callable=send_communication_sr_creation_new_service_account,
        )

        # Task for not new Account in AD flow.
        existing_service_account_activity_update_new_service_account_task = PythonOperator(
            task_id='existing_service_account_activity_update_new_service_account',
            python_callable=existing_service_account_activity_update_new_service_account,
        )

        send_communication_existing_service_account_error_new_service_account_task = PythonOperator(
            task_id='send_communication_existing_service_account_error_new_service_account',
            python_callable=send_communication_existing_service_account_error_new_service_account,
        )

        # task for AD process error
        update_activity_status_ad_process_error_new_service_account_task = PythonOperator(
            task_id='update_activity_status_ad_process_error_new_service_account',
            python_callable=update_activity_status_ad_process_error_new_service_account,
        )

        send_communication_ad_process_error_new_service_account_task = PythonOperator(
            task_id='send_communication_ad_process_error_new_service_account',
            python_callable=send_communication_ad_process_error_new_service_account,
        )

        # task for sr creation failed flow.
        sr_creation_failed_activity_update_new_service_account_task = PythonOperator(
            task_id='sr_creation_failed_activity_update_new_service_account',
            python_callable=sr_creation_failed_activity_update_new_service_account,
        )

        sr_creation_failed_email_notification_new_service_account_task = PythonOperator(
            task_id='sr_creation_failed_email_notification_new_service_account',
            python_callable=sr_creation_failed_email_notification_new_service_account,
        )

        # send_comm_service_account_name_change flow
        send_comm_service_account_name_change_task = PythonOperator(
            task_id='send_comm_service_account_name_change',
            python_callable=send_comm_service_account_name_change,
        )

        check_service_account_name_task_new_service_account_task >> branch_based_on_svc_acc_name_creation_success_new_service_account_task >> send_comm_service_account_name_change_task
        check_service_account_name_task_new_service_account_task >> branch_based_on_svc_acc_name_creation_success_new_service_account_task >> fetch_data_activity_info_new_service_account_task >> ad_process_new_service_account_task >> branch_on_ad_output_new_service_account_task >> sr_creation_new_service_account_task >> branch_based_on_sr_creation_new_service_account_task >> sr_worklog_update_new_service_account_task >> sr_creation_activity_update_new_service_account_task >> fetch_itsm_approver_new_service_account_task >> send_communication_sr_creation_new_service_account_task
        check_service_account_name_task_new_service_account_task >> branch_based_on_svc_acc_name_creation_success_new_service_account_task >> fetch_data_activity_info_new_service_account_task >> ad_process_new_service_account_task >> branch_on_ad_output_new_service_account_task >> sr_creation_new_service_account_task >> branch_based_on_sr_creation_new_service_account_task >> sr_creation_failed_activity_update_new_service_account_task >> sr_creation_failed_email_notification_new_service_account_task
        check_service_account_name_task_new_service_account_task >> branch_based_on_svc_acc_name_creation_success_new_service_account_task >> fetch_data_activity_info_new_service_account_task >> ad_process_new_service_account_task >> branch_on_ad_output_new_service_account_task >> existing_service_account_activity_update_new_service_account_task >> send_communication_existing_service_account_error_new_service_account_task
        check_service_account_name_task_new_service_account_task >> branch_based_on_svc_acc_name_creation_success_new_service_account_task >> fetch_data_activity_info_new_service_account_task >> ad_process_new_service_account_task >> branch_on_ad_output_new_service_account_task >> update_activity_status_ad_process_error_new_service_account_task >> send_communication_ad_process_error_new_service_account_task

    with TaskGroup(group_id='service_account_modification_group') as service_account_modification_group:
        # Initial task

        # Dummy task indicating feature not supported
        def handle_service_account_modification_not_supported(**context):
            # Get the worklog ID from XCom
            worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

            # Create hook and set the worklog ID
            hook = WorkLogHook()
            hook.set_worklog_id(worklog_id)

            # Log that feature is not supported
            hook.error("Disable Service Account feature is not supported yet")

            return {
                "success": False,
                "error": "Disable Service Account feature is not supported yet"
            }

        service_account_modification_task = PythonOperator(
            task_id='service_account_modification_task',
            python_callable=handle_service_account_modification_not_supported,
            dag=dag
        )
        # More tasks can be added here later
        # Example: service_account_modification_task >> next_task_in_group

    # 5 User Account Modification Group
    with TaskGroup(group_id='user_account_modification_group') as user_account_modification_group:

        fetch_data_activity_info_user_account_modify_task = PythonOperator(
            task_id='fetch_data_activity_info_user_account_modify',
            python_callable=fetch_data_activity_info_user_account_modify,
        )

        ad_connect_user_account_modification_task = PythonOperator(
            task_id='ad_connect_user_account_modification',
            python_callable=ad_connect_user_account_modification,
            retries=12,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=20)
        )

        def branch_based_on_ad_output_user_account_modify(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='user_account_modification_group.ad_connect_user_account_modification', key='activity_data')
            ad_output = activity_data['data']['ad_output_user_account_modify']
            modification_action = activity_data['data']['activity_info_fetched']['modification_action']

            if 'role is not'.lower() in ad_output.lower():
                return 'user_account_modification_group.sr_creation_user_account_modify'
            elif 'role is already'.lower() in ad_output.lower() and 'add' in modification_action.lower():
                return 'user_account_modification_group.sr_activity_status_update_existing_user_add_user_account_modify'
            elif 'role is already'.lower() in ad_output.lower() and 'remove' in modification_action.lower():
                return 'user_account_modification_group.sr_activity_status_update_non_existing_user_remove_user_account_modify'
            else:
                return 'user_account_modification_group.sr_creation_activity_update_ad_process_errored_user_account_modify'

        branch_based_on_ad_output_user_account_modify_task = BranchPythonOperator(
            task_id='branch_based_on_ad_output_user_account_modify',
            python_callable=branch_based_on_ad_output_user_account_modify,
        )

        sr_creation_user_account_modify_task = PythonOperator(
            task_id='sr_creation_user_account_modify',
            python_callable=sr_creation_user_account_modify,
        )

        def branch_based_on_sr_creation_success_user_account_modify(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='user_account_modification_group.sr_creation_user_account_modify', key='activity_data')
            sr_creation_status = activity_data['data']['sr_creation_result'].get('status', '')

            if 'failed'.lower() in sr_creation_status.lower():
                return 'user_account_modification_group.sr_creation_failed_activity_update_user_account_modify'
            else:
                return 'user_account_modification_group.sr_worklog_update_user_account_modify'

        branch_based_on_sr_creation_success_user_account_modify_task = BranchPythonOperator(
            task_id='branch_based_on_sr_creation_success_user_account_modify',
            python_callable=branch_based_on_sr_creation_success_user_account_modify,
        )

        sr_worklog_update_user_account_modify_task = PythonOperator(
            task_id='sr_worklog_update_user_account_modify',
            python_callable=sr_worklog_update_user_account_modify,
        )

        sr_creation_activity_update_user_account_modify_task = PythonOperator(
            task_id='sr_creation_activity_update_user_account_modify',
            python_callable=sr_creation_activity_update_user_account_modify,
        )

        fetch_itsm_approver_user_account_modify_task = PythonOperator(
            task_id='fetch_itsm_approver_user_account_modify',
            python_callable=fetch_itsm_approver_user_account_modify,
        )

        send_communication_sr_creation_user_account_modify_task = PythonOperator(
            task_id='send_communication_sr_creation_user_account_modify',
            python_callable=send_communication_sr_creation_user_account_modify,
        )

        # existing user add flow
        sr_activity_status_update_existing_user_add_user_account_modify_task = PythonOperator(
            task_id='sr_activity_status_update_existing_user_add_user_account_modify',
            python_callable=sr_activity_status_update_existing_user_add_user_account_modify,
        )

        send_communication_existing_user_add_error_user_account_modify_task = PythonOperator(
            task_id='send_communication_existing_user_add_error_user_account_modify',
            python_callable=send_communication_existing_user_add_error_user_account_modify,
        )

        # non existing user remove flow
        sr_activity_status_update_non_existing_user_remove_user_account_modify_task = PythonOperator(
            task_id='sr_activity_status_update_non_existing_user_remove_user_account_modify',
            python_callable=sr_activity_status_update_non_existing_user_remove_user_account_modify,
        )

        send_communication_non_existing_user_remove_error_user_account_modify_task = PythonOperator(
            task_id='send_communication_non_existing_user_remove_error_user_account_modify',
            python_callable=send_communication_non_existing_user_remove_error_user_account_modify,
        )

        # ad process error flow
        sr_creation_activity_update_ad_process_errored_user_account_modify_task = PythonOperator(
            task_id='sr_creation_activity_update_ad_process_errored_user_account_modify',
            python_callable=sr_creation_activity_update_ad_process_errored_user_account_modify,
        )

        send_communication_ad_process_errored_user_account_modify_task = PythonOperator(
            task_id='send_communication_ad_process_errored_user_account_modify',
            python_callable=send_communication_ad_process_errored_user_account_modify,
        )

        # sr creation failed flow
        sr_creation_failed_activity_update_user_account_modify_task = PythonOperator(
            task_id='sr_creation_failed_activity_update_user_account_modify',
            python_callable=sr_creation_failed_activity_update_user_account_modify,
        )

        sr_creation_failed_email_notification_user_account_modify_task = PythonOperator(
            task_id='sr_creation_failed_email_notification_user_account_modify',
            python_callable=sr_creation_failed_email_notification_user_account_modify,
        )

        fetch_data_activity_info_user_account_modify_task >> ad_connect_user_account_modification_task >> branch_based_on_ad_output_user_account_modify_task >> sr_creation_user_account_modify_task >> branch_based_on_sr_creation_success_user_account_modify_task >> sr_worklog_update_user_account_modify_task >> sr_creation_activity_update_user_account_modify_task >> fetch_itsm_approver_user_account_modify_task >> send_communication_sr_creation_user_account_modify_task
        fetch_data_activity_info_user_account_modify_task >> ad_connect_user_account_modification_task >> branch_based_on_ad_output_user_account_modify_task >> sr_creation_user_account_modify_task >> branch_based_on_sr_creation_success_user_account_modify_task >> sr_creation_failed_activity_update_user_account_modify_task >> sr_creation_failed_email_notification_user_account_modify_task
        fetch_data_activity_info_user_account_modify_task >> ad_connect_user_account_modification_task >> branch_based_on_ad_output_user_account_modify_task >> sr_activity_status_update_existing_user_add_user_account_modify_task >> send_communication_existing_user_add_error_user_account_modify_task
        fetch_data_activity_info_user_account_modify_task >> ad_connect_user_account_modification_task >> branch_based_on_ad_output_user_account_modify_task >> sr_activity_status_update_non_existing_user_remove_user_account_modify_task >> send_communication_non_existing_user_remove_error_user_account_modify_task
        fetch_data_activity_info_user_account_modify_task >> ad_connect_user_account_modification_task >> branch_based_on_ad_output_user_account_modify_task >> sr_creation_activity_update_ad_process_errored_user_account_modify_task >> send_communication_ad_process_errored_user_account_modify_task

        # More tasks can be added here later
        # Example: user_account_modification_task >> next_task_in_group

    with TaskGroup(group_id='disable_service_account_group') as disable_service_account_group:
        # Dummy task indicating feature not supported
        def handle_disable_service_account_not_supported(**context):
            # Get the worklog ID from XCom
            worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

            # Create hook and set the worklog ID
            hook = WorkLogHook()
            hook.set_worklog_id(worklog_id)

            # Log that feature is not supported
            hook.error("Disable Service Account feature is not supported yet")

            return {
                "success": False,
                "error": "Disable Service Account feature is not supported yet"
            }

        disable_service_account_task = PythonOperator(
            task_id='disable_service_account_task',
            python_callable=handle_disable_service_account_not_supported,
            dag=dag
        )
        # More tasks can be added here later
        # Example: disable_service_account_task >> next_task_in_group

    with TaskGroup(group_id='disable_user_account_group') as disable_user_account_group:
        # Dummy task indicating feature not supported
        def handle_disable_user_account_not_supported(**context):
            # Get the worklog ID from XCom
            worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

            # Create hook and set the worklog ID
            hook = WorkLogHook()
            hook.set_worklog_id(worklog_id)

            # Log that feature is not supported
            hook.error("Disable User Account feature is not supported yet")

            return {
                "success": False,
                "error": "Disable User Account feature is not supported yet"
            }

        disable_user_account_task = PythonOperator(
            task_id='disable_user_account_task',
            python_callable=handle_disable_user_account_not_supported,
            dag=dag
        )
        # More tasks can be added here later
        # Example: disable_user_account_task >> next_task_in_group

    # 4 Service Account Password Reset and other requests Task Group
    with TaskGroup(group_id='other_requests_group') as other_requests_group:

        fetch_activity_info_other_requests_task = PythonOperator(
            task_id='fetch_activity_info_other_requests',
            python_callable=fetch_activity_info_other_requests,
        )

        sr_creation_other_requests_task = PythonOperator(
            task_id='sr_creation_other_requests',
            python_callable=sr_creation_other_requests,
        )

        def branch_based_on_sr_creation_other_requests(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='other_requests_group.sr_creation_other_requests', key='activity_data')
            sr_creation_result = activity_data['data']['sr_creation_result']['status']

            if 'Failed'.lower() not in sr_creation_result.lower():
                return 'other_requests_group.sr_worklog_update_other_requests'
            else:
                return 'other_requests_group.sr_activity_status_update_process_sr_error_other_requests'

        branch_based_on_sr_creation_other_requests_task = BranchPythonOperator(
            task_id='branch_on_sr_creation_other_requests',
            python_callable=branch_based_on_sr_creation_other_requests,
        )

        # Sr Creation Success Flow
        sr_worklog_update_other_requests_task = PythonOperator(
            task_id='sr_worklog_update_other_requests',
            python_callable=sr_worklog_update_other_requests,
        )

        sr_creation_activity_update_other_requests_task = PythonOperator(
            task_id='sr_creation_activity_update_other_requests',
            python_callable=sr_creation_activity_update_other_requests,
        )

        sr_creation_email_notification_other_requests_task = PythonOperator(
            task_id='sr_creation_email_notification_other_requests',
            python_callable=sr_creation_email_notification_other_requests,
        )

        # Sr Creation Failed Flow
        sr_activity_status_update_process_sr_error_other_requests_task = PythonOperator(
            task_id='sr_activity_status_update_process_sr_error_other_requests',
            python_callable=sr_activity_status_update_process_sr_error_other_requests,
        )

        sr_creation_failed_email_notification_other_requests_task = PythonOperator(
            task_id='sr_creation_failed_email_notification_other_requests',
            python_callable=sr_creation_failed_email_notification_other_requests,
        )

        fetch_activity_info_other_requests_task >> sr_creation_other_requests_task >> branch_based_on_sr_creation_other_requests_task >> sr_worklog_update_other_requests_task >> sr_creation_activity_update_other_requests_task >> sr_creation_email_notification_other_requests_task

        fetch_activity_info_other_requests_task >> sr_creation_other_requests_task >> branch_based_on_sr_creation_other_requests_task >> sr_activity_status_update_process_sr_error_other_requests_task >> sr_creation_failed_email_notification_other_requests_task

    close_worklog_task = PythonOperator(
        task_id='close_worklog',
        python_callable=close_worklog,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Set task dependencies
    create_worklog_task >> update_or_create_request_activity_task >> branch_by_activity >> user_password_reset_group >> close_worklog_task
    create_worklog_task >> update_or_create_request_activity_task >> branch_by_activity >> new_user_account_group >> close_worklog_task
    create_worklog_task >> update_or_create_request_activity_task >> branch_by_activity >> new_service_account_group >> close_worklog_task
    create_worklog_task >> update_or_create_request_activity_task >> branch_by_activity >> service_account_modification_group >> close_worklog_task
    create_worklog_task >> update_or_create_request_activity_task >> branch_by_activity >> user_account_modification_group >> close_worklog_task
    create_worklog_task >> update_or_create_request_activity_task >> branch_by_activity >> disable_service_account_group >> close_worklog_task
    create_worklog_task >> update_or_create_request_activity_task >> branch_by_activity >> disable_user_account_group >> close_worklog_task
    create_worklog_task >> update_or_create_request_activity_task >> branch_by_activity >> other_requests_group >> close_worklog_task

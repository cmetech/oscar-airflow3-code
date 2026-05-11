from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from hooks.worklog_hook import WorkLogHook, WorkLogType
from hooks.access_management_db_hook import AccessManagementSQLHook
from airflow.hooks.base import BaseHook
from typing import Dict, Any
import os
import re
import json
import uuid
import logging
from helpers.access_management_req_resolv_common import (
    check_activity_request_resolve_common,
    check_wo_status,
    update_activity_data_wo_approved,
    fetch_data_activity_for_response,
    send_email_communication_awaiting_response_date_flag_1,
    send_email_communication_awaiting_response_date_flag_2,
    sr_activity_update_awaiting_response_date_flag_1,
    sr_activity_update_awaiting_response_date_flag_2,
    fetch_data_activity_for_sr_reminder,
    fetch_itsm_approver_sr_reminder,
    send_email_communication_sr_reminder_date_flag_1,
    sr_activity_update_sr_reminder_date_flag_1,
    send_email_communication_sr_reminder_date_flag_2,
    sr_activity_update_sr_reminder_date_flag_2,
    update_activity_info_service_request_cancel,
    send_communication_service_request_cancel,
    update_activity_info_service_request_wo_fetch_fail,
    send_communication_service_request_request_wo_fetch_fail,
)
from helpers.access_management_req_resolv_user_passwd_reset import (
    fetch_activity_info_user_password_reset_rs,
    ad_process_user_password_reset_rs,
    rs_update_activity_new_password,
    check_shift_rooster_password_reset_rs,
    send_communication_shift_wise_fo_password_reset,
    update_wo_comment_user_passwd_reset,
    update_wo_status_user_passwd_reset,
    update_activity_info_wo_comment_user_passwd_reset_err,
    send_comm_err_wo_comment_user_passwd_reset_err,
    update_activity_info_wo_status_user_passwd_reset_err,
    send_comm_err_wo_status_user_passwd_reset_err,
    update_activity_info_non_finished_ad_user_password_reset,
    update_activity_password_reset_decryption_failed,
    send_communication_password_reset_decryption_failed,
    send_communication_non_finished_ad_user_password_reset
)

from helpers.access_management_req_resolv_new_user_account import (
    create_new_user_adid,
    fetch_activity_info_new_user_account,
    ad_process_new_user_account,
    rs_update_activity_new_user_account,
    send_notification_new_user_account_create,
    check_shift_rooster_new_user_account_rs,
    send_communication_shift_wise_fo_new_user_account,
    update_wo_comment_new_user_account,
    update_wo_status_new_user_account,
    update_activity_info_wo_comment_new_user_account_err,
    send_comm_err_wo_comment_new_user_account_err,
    update_activity_info_wo_status_new_user_account_err,
    send_comm_err_wo_status_new_user_account_err,
    send_communication_non_finished_ad_new_user_account,
    insert_new_user_data_into_user_list,
    update_activity_info_non_finished_ad_new_user_account,
    update_activity_new_user_account_decryption_failed,
    send_communication_new_user_account_decryption_failed
)

from helpers.access_management_req_resolv_new_service_account import (
    fetch_activity_info_new_service_account,
    ad_process_new_service_account,
    rs_update_activity_new_service_account,
    send_notification_new_service_account_create,
    check_shift_rooster_new_service_account_rs,
    send_communication_shift_wise_fo_new_service_account,
    update_wo_comment_new_service_account,
    update_wo_status_new_service_account,
    update_activity_info_wo_comment_new_service_account_err,
    send_comm_err_wo_comment_new_service_account_err,
    update_activity_info_wo_status_new_service_account_err,
    send_comm_err_wo_status_new_service_account_err,
    send_communication_non_finished_ad_new_service_account,
    update_activity_info_non_finished_ad_new_service_account,
    update_activity_new_service_account_decryption_failed,
    send_communication_new_service_account_decryption_failed
)

from helpers.access_management_req_resolv_user_account_modification import (
    fetch_activity_info_user_account_modification,
    ad_process_user_account_modification,
    rs_update_activity_user_account_modify,
    send_notification_user_account_modify,
    update_wo_comment_user_account_modify,
    update_wo_status_user_account_modify,
    update_activity_info_wo_comment_user_account_modify_err,
    send_comm_err_wo_comment_user_account_modify_err,
    update_activity_info_wo_status_user_account_modify_err,
    send_comm_err_wo_status_user_account_modify_err,
    send_communication_non_finished_ad_user_account_modify,
    update_activity_info_non_finished_ad_user_account_modify
)

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
        name="Access Management Request Resolve Worklog",
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

def branch_on_sr_submitted(**context):
    """
    Branches based on whether status contains 'SR Submitted' or 'Awaiting response' in activity_data_request_resolve XCom.
    If SR Submitted, goes to check_wo_status_task (main flow), otherwise goes to awaiting_response_task_group.
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='check_activity_request_resolve', key='activity_data_request_resolve')
    status = None
    if activity_data and 'data' in activity_data:
        status = activity_data['data'].get('request_resolve_initial_data', {}).get('status')

    if status and 'SR Submitted'.lower() in status.lower():
        return 'check_wo_status'
    else:
        return 'awaiting_response_task_group'

def task_type_request_resolve_branch(**context):
    """
    Branches based on task_type from activity_data in the previous task.
    For invalid or missing task types, ends the task group by returning None.
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='update_activity_data_wo_approved', key='activity_data_request_resolve')
    task_type = None
    if activity_data and 'data' in activity_data:
        task_type = activity_data['data'].get('request_resolve_initial_data', {}).get('task_type')

    if not task_type:
        return None

    # validate the task types are as per SR-management dag
    task_type = task_type.lower()
    if 'user account password reset' in task_type:  # User Account Password Reset
        return 'password_reset_task_group'
    elif 'new user account creation' in task_type or 'new user account' in task_type:  # New User Account
        return 'new_user_account_task_group'
    elif 'new service account creation' in task_type or 'new service account' in task_type:  # New Service Account
        return 'new_service_account_task_group'
    elif 'user account modification' in task_type or 'user account modify' in task_type:  # User Account Modify
        return 'user_account_modification_task_group'
    else:
        return None

with DAG(
    'access_management_request_resolve',
    default_args=default_args,
    description='Automated access management request resolve tasks',
    schedule=None,
    catchup=False,
    tags=['access_management'],
) as dag:

    create_worklog_task = PythonOperator(
        task_id='create_worklog',
        python_callable=create_worklog,
    )

    close_worklog_task = PythonOperator(
        task_id='close_worklog',
        python_callable=close_worklog,
        trigger_rule=TriggerRule.ALL_DONE
    )

    check_activity_request_resolve_task = PythonOperator(
        task_id='check_activity_request_resolve',
        python_callable=check_activity_request_resolve_common,
    )

    branch_on_sr_submitted_task = BranchPythonOperator(
        task_id='branch_on_sr_submitted',
        python_callable=branch_on_sr_submitted,
    )

    # Check WO Status task
    check_wo_status_task = PythonOperator(
        task_id='check_wo_status',
        python_callable=check_wo_status,
    )

    def check_wo_status_branch(**context):
        """Branch based on flag_wo value from previous task"""
        ti = context['ti']
        activity_data = ti.xcom_pull(task_ids='check_wo_status', key='activity_data_request_resolve')
        flag_wo = activity_data['data']['wo_status_data'].get('flag_wo', '')
        logger.info(f"flag_wo: {flag_wo}")

        if flag_wo == 'true':
            return 'update_activity_data_wo_approved'
        elif flag_wo == 'errored':
            return 'wo_fetch_fail_task_group'
        elif flag_wo == 'cancel':
            return 'wo_cancel_task_group'
        else:
            return 'service_request_reminder_task_group'

    check_wo_status_branch_task = BranchPythonOperator(
        task_id='check_wo_status_branch',
        python_callable=check_wo_status_branch,
    )

    # Main flow task with workorder
    update_activity_data_wo_approved_task = PythonOperator(
        task_id='update_activity_data_wo_approved',
        python_callable=update_activity_data_wo_approved,
    )

    # Task Type Branch Task
    task_type_request_resolve_branch_task = BranchPythonOperator(
        task_id='task_type_request_resolve_branch',
        python_callable=task_type_request_resolve_branch,
    )

    # Password Reset Task Group
    with TaskGroup(group_id='password_reset_task_group') as password_reset_task_group:
        # First task: Fetch activity data for password reset
        fetch_activity_info_user_password_reset_rs_task = PythonOperator(
            task_id='fetch_activity_info_user_password_reset_rs',
            python_callable=fetch_activity_info_user_password_reset_rs,
        )

        # AD Process task for password reset
        ad_process_user_password_reset_rs_task = PythonOperator(
            task_id='ad_process_user_password_reset_rs',
            python_callable=ad_process_user_password_reset_rs,
            retries=8,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=15)
        )

        # Branch based on AD process result
        def branch_based_on_ad_process_result(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='password_reset_task_group.ad_process_user_password_reset_rs', key='activity_data_request_resolve')

            if not activity_data or 'data' not in activity_data:
                return 'password_reset_task_group.activity_update_password_reset_decryption_failed'

            ad_python_status = activity_data['data']['ad_python_status'].lower()
            ad_new_password = activity_data['data'].get('ad_new_password', None)
            if ad_new_password:
                ad_new_password = ad_new_password.strip()

            if ad_python_status != 'finished':
                return 'password_reset_task_group.send_communication_non_finished_ad_user_password_reset'
            elif ad_python_status == 'finished' and ad_new_password:
                return 'password_reset_task_group.rs_update_activity_new_password'
            else:
                return 'password_reset_task_group.activity_update_password_reset_decryption_failed'

        branch_on_ad_process_result_task = BranchPythonOperator(
            task_id='branch_on_ad_process_result',
            python_callable=branch_based_on_ad_process_result,
        )

        # Main flow task - Update activity with new password
        rs_update_activity_new_password_task = PythonOperator(
            task_id='rs_update_activity_new_password',
            python_callable=rs_update_activity_new_password,
        )

        # Main flow task - Check shift roster for current shift engineers
        check_shift_rooster_password_reset_rs_task = PythonOperator(
            task_id='check_shift_rooster_password_reset_rs',
            python_callable=check_shift_rooster_password_reset_rs,
        )

        # Main flow task - Send communication to shift engineers
        send_communication_shift_wise_fo_password_reset_task = PythonOperator(
            task_id='send_communication_shift_wise_fo_password_reset',
            python_callable=send_communication_shift_wise_fo_password_reset,
        )

        # Main flow task - Update work order with work info comment
        update_wo_comment_user_passwd_reset_task = PythonOperator(
            task_id='update_wo_comment_user_passwd_reset',
            python_callable=update_wo_comment_user_passwd_reset,
        )

        def branch_on_wo_comment_status(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='password_reset_task_group.update_wo_comment_user_passwd_reset', key='activity_data_request_resolve')
            status = activity_data['data']['wo_comment_update_user_password_reset'].get('status', 'failed')
            if 'failed' not in status.lower():
                return 'password_reset_task_group.update_wo_status_user_passwd_reset'
            return 'password_reset_task_group.update_activity_info_wo_comment_user_passwd_reset_err'

        branch_on_wo_comment_status_task = BranchPythonOperator(
            task_id='branch_on_wo_comment_status',
            python_callable=branch_on_wo_comment_status,
        )

        # Main flow task - Update work order status
        update_wo_status_user_passwd_reset_task = PythonOperator(
            task_id='update_wo_status_user_passwd_reset',
            python_callable=update_wo_status_user_passwd_reset,
        )

        def branch_on_wo_status_update_status(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='password_reset_task_group.update_wo_status_user_passwd_reset', key='activity_data_request_resolve')
            status = activity_data['data']['wo_result_update_user_password_reset'].get('status', 'failed')
            if 'failed' not in status.lower():
                return 'password_reset_task_group.end_task_new_password_reset'
            return 'password_reset_task_group.update_activity_info_wo_status_user_passwd_reset_err'

        branch_on_wo_status_update_status_task = BranchPythonOperator(
            task_id='branch_on_wo_status_update_status',
            python_callable=branch_on_wo_status_update_status,
        )

        # Main flow err scenario update_wo_comment_user_passwd_reset_task
        update_activity_info_wo_comment_user_passwd_reset_err_task = PythonOperator(
            task_id='update_activity_info_wo_comment_user_passwd_reset_err',
            python_callable=update_activity_info_wo_comment_user_passwd_reset_err,
        )

        # Main flow err scenario send_comm_err_wo_comment_user_passwd_reset_err_task
        send_comm_err_wo_comment_user_passwd_reset_err_task = PythonOperator(
            task_id='send_comm_err_wo_comment_user_passwd_reset_err',
            python_callable=send_comm_err_wo_comment_user_passwd_reset_err,
        )

        # Main flow err scenario update_wo_status_user_passwd_reset_task
        update_activity_info_wo_status_user_passwd_reset_err_task = PythonOperator(
            task_id='update_activity_info_wo_status_user_passwd_reset_err',
            python_callable=update_activity_info_wo_status_user_passwd_reset_err,
        )

        # Main flow err scenario send_comm_err_wo_status_user_passwd_reset_err_task
        send_comm_err_wo_status_user_passwd_reset_err_task = PythonOperator(
            task_id='send_comm_err_wo_status_user_passwd_reset_err',
            python_callable=send_comm_err_wo_status_user_passwd_reset_err,
        )

        # alternate flow: Communication task for non-finished status
        send_communication_non_finished_ad_user_password_reset_task = PythonOperator(
            task_id='send_communication_non_finished_ad_user_password_reset',
            python_callable=send_communication_non_finished_ad_user_password_reset,
        )

        # alternate flow: Activity task for non-finished status
        update_activity_info_non_finished_ad_user_password_reset_task = PythonOperator(
            task_id='update_activity_info_non_finished_ad_user_password_reset',
            python_callable=update_activity_info_non_finished_ad_user_password_reset,
        )

        # alternate flow: Activity Task for decryption failure
        activity_update_password_reset_decryption_failed_task = PythonOperator(
            task_id='activity_update_password_reset_decryption_failed',
            python_callable=update_activity_password_reset_decryption_failed,
        )

        # alternate flow: Communication task for decryption failure
        send_communication_password_reset_decryption_failed_task = PythonOperator(
            task_id='send_communication_password_reset_decryption_failed',
            python_callable=send_communication_password_reset_decryption_failed,
        )

        # end task for password reset
        end_task_new_password_reset = EmptyOperator(
            task_id='end_task_new_password_reset'
        )

        # Set task dependencies

        fetch_activity_info_user_password_reset_rs_task >> ad_process_user_password_reset_rs_task >> branch_on_ad_process_result_task >> rs_update_activity_new_password_task >> check_shift_rooster_password_reset_rs_task >> send_communication_shift_wise_fo_password_reset_task >> update_wo_comment_user_passwd_reset_task >> branch_on_wo_comment_status_task >> update_wo_status_user_passwd_reset_task >> branch_on_wo_status_update_status_task >> update_activity_info_wo_status_user_passwd_reset_err_task >> send_comm_err_wo_status_user_passwd_reset_err_task

        fetch_activity_info_user_password_reset_rs_task >> ad_process_user_password_reset_rs_task >> branch_on_ad_process_result_task >> rs_update_activity_new_password_task >> check_shift_rooster_password_reset_rs_task >> send_communication_shift_wise_fo_password_reset_task >> update_wo_comment_user_passwd_reset_task >> branch_on_wo_comment_status_task >> update_wo_status_user_passwd_reset_task >> branch_on_wo_status_update_status_task >> end_task_new_password_reset

        fetch_activity_info_user_password_reset_rs_task >> ad_process_user_password_reset_rs_task >> branch_on_ad_process_result_task >> rs_update_activity_new_password_task >> check_shift_rooster_password_reset_rs_task >> send_communication_shift_wise_fo_password_reset_task >> update_wo_comment_user_passwd_reset_task >> branch_on_wo_comment_status_task >> update_activity_info_wo_comment_user_passwd_reset_err_task >> send_comm_err_wo_comment_user_passwd_reset_err_task

        fetch_activity_info_user_password_reset_rs_task >> ad_process_user_password_reset_rs_task >> branch_on_ad_process_result_task >> send_communication_non_finished_ad_user_password_reset_task >> update_activity_info_non_finished_ad_user_password_reset_task

        fetch_activity_info_user_password_reset_rs_task >> ad_process_user_password_reset_rs_task >> branch_on_ad_process_result_task >> activity_update_password_reset_decryption_failed_task >> send_communication_password_reset_decryption_failed_task

    # New User Account Task Group
    with TaskGroup(group_id='new_user_account_task_group') as new_user_account_task_group:

        create_new_user_adid_task = PythonOperator(
            task_id='create_new_user_adid',
            python_callable=create_new_user_adid,
        )

        fetch_activity_info_new_user_account_task = PythonOperator(
            task_id='fetch_activity_info_new_user_account',
            python_callable=fetch_activity_info_new_user_account,
        )

        ad_process_new_user_account_task = PythonOperator(
            task_id='ad_process_new_user_account',
            python_callable=ad_process_new_user_account,
            retries=8,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=15)
        )

        # Branch based on AD process result
        def branch_based_on_ad_process_result(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.ad_process_new_user_account', key='activity_data_request_resolve')

            if not activity_data or 'data' not in activity_data:
                return 'new_user_account_task_group.activity_update_password_reset_decryption_failed'

            ad_python_status = activity_data['data']['ad_python_status'].lower()
            ad_new_password = activity_data['data'].get('ad_new_password', None)
            if ad_new_password:
                ad_new_password = ad_new_password.strip()

            if ad_python_status != 'finished':
                return 'new_user_account_task_group.send_communication_non_finished_ad_new_user_account'
            elif ad_python_status == 'finished' and ad_new_password:
                return 'new_user_account_task_group.rs_update_activity_new_user_account'
            else:
                return 'new_user_account_task_group.activity_update_password_reset_decryption_failed'

        branch_on_ad_process_result_task = BranchPythonOperator(
            task_id='branch_on_ad_process_result',
            python_callable=branch_based_on_ad_process_result,
        )

        rs_update_activity_new_user_account_task = PythonOperator(
            task_id='rs_update_activity_new_user_account',
            python_callable=rs_update_activity_new_user_account,
        )

        send_notification_new_user_account_create_task = PythonOperator(
            task_id='send_notification_new_user_account_create',
            python_callable=send_notification_new_user_account_create,
        )

        check_shift_rooster_new_user_account_rs_task = PythonOperator(
            task_id='check_shift_rooster_new_user_account_rs',
            python_callable=check_shift_rooster_new_user_account_rs,
        )

        send_communication_shift_wise_fo_new_user_account_task = PythonOperator(
            task_id='send_communication_shift_wise_fo_new_user_account',
            python_callable=send_communication_shift_wise_fo_new_user_account,
        )

        update_wo_comment_new_user_account_task = PythonOperator(
            task_id='update_wo_comment_new_user_account',
            python_callable=update_wo_comment_new_user_account,
        )

        def branch_on_wo_comment_status(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.update_wo_comment_new_user_account', key='activity_data_request_resolve')
            status = activity_data['data']['wo_comment_update_new_user_account'].get('status', 'failed')
            if 'failed' not in status.lower():
                return 'new_user_account_task_group.update_wo_status_new_user_account'
            return 'new_user_account_task_group.update_activity_info_wo_comment_new_user_account_err'

        branch_on_wo_comment_status_task = BranchPythonOperator(
            task_id='branch_on_wo_comment_status',
            python_callable=branch_on_wo_comment_status,
        )

        update_wo_status_new_user_account_task = PythonOperator(
            task_id='update_wo_status_new_user_account',
            python_callable=update_wo_status_new_user_account,
        )

        def branch_on_wo_status_update_status(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.update_wo_status_new_user_account', key='activity_data_request_resolve')
            status = activity_data['data']['wo_result_update_new_user_account'].get('status', 'failed')
            if 'failed' in status.lower():
                return 'new_user_account_task_group.update_activity_info_wo_status_new_user_account_err'
            else:
                return 'new_user_account_task_group.insert_new_user_data_into_user_list'

        branch_on_wo_status_update_status_task = BranchPythonOperator(
            task_id='branch_on_wo_status_update_status',
            python_callable=branch_on_wo_status_update_status,
        )

        insert_new_user_data_into_user_list_task = PythonOperator(
            task_id='insert_new_user_data_into_user_list',
            python_callable=insert_new_user_data_into_user_list,
        )

        # Alrernate flow wo comment update failed
        update_activity_info_wo_comment_new_user_account_err_task = PythonOperator(
            task_id='update_activity_info_wo_comment_new_user_account_err',
            python_callable=update_activity_info_wo_comment_new_user_account_err,
        )

        send_comm_err_wo_comment_new_user_account_err_task = PythonOperator(
            task_id='send_comm_err_wo_comment_new_user_account_err',
            python_callable=send_comm_err_wo_comment_new_user_account_err,
        )

        # Alrernate flow wo status update failed
        update_activity_info_wo_status_new_user_account_err_task = PythonOperator(
            task_id='update_activity_info_wo_status_new_user_account_err',
            python_callable=update_activity_info_wo_status_new_user_account_err,
        )

        send_comm_err_wo_status_new_user_account_err_task = PythonOperator(
            task_id='send_comm_err_wo_status_new_user_account_err',
            python_callable=send_comm_err_wo_status_new_user_account_err,
        )

        # alternate flow: Communication task for non-finished status
        send_communication_non_finished_ad_new_user_account_task = PythonOperator(
            task_id='send_communication_non_finished_ad_new_user_account',
            python_callable=send_communication_non_finished_ad_new_user_account,
        )

        update_activity_info_non_finished_ad_new_user_account_task = PythonOperator(
            task_id='update_activity_info_non_finished_ad_new_user_account',
            python_callable=update_activity_info_non_finished_ad_new_user_account,
        )

        # alternate flow: Activity task for decryption failure
        update_activity_new_user_account_decryption_failed_task = PythonOperator(
            task_id='update_activity_new_user_account_decryption_failed',
            python_callable=update_activity_new_user_account_decryption_failed,
        )

        send_communication_new_user_account_decryption_failed_task = PythonOperator(
            task_id='send_communication_new_user_account_decryption_failed',
            python_callable=send_communication_new_user_account_decryption_failed,
        )

        # Set task dependencies
        create_new_user_adid_task >> fetch_activity_info_new_user_account_task >> ad_process_new_user_account_task >> branch_on_ad_process_result_task >> rs_update_activity_new_user_account_task >> send_notification_new_user_account_create_task >> check_shift_rooster_new_user_account_rs_task >> send_communication_shift_wise_fo_new_user_account_task >> update_wo_comment_new_user_account_task >> branch_on_wo_comment_status_task >> update_wo_status_new_user_account_task >> branch_on_wo_status_update_status_task >> insert_new_user_data_into_user_list_task

        create_new_user_adid_task >> fetch_activity_info_new_user_account_task >> ad_process_new_user_account_task >> branch_on_ad_process_result_task >> rs_update_activity_new_user_account_task >> send_notification_new_user_account_create_task >> check_shift_rooster_new_user_account_rs_task >> send_communication_shift_wise_fo_new_user_account_task >> update_wo_comment_new_user_account_task >> branch_on_wo_comment_status_task >> update_wo_status_new_user_account_task >> branch_on_wo_status_update_status_task >> update_activity_info_wo_status_new_user_account_err_task >> send_comm_err_wo_status_new_user_account_err_task

        create_new_user_adid_task >> fetch_activity_info_new_user_account_task >> ad_process_new_user_account_task >> branch_on_ad_process_result_task >> rs_update_activity_new_user_account_task >> send_notification_new_user_account_create_task >> check_shift_rooster_new_user_account_rs_task >> send_communication_shift_wise_fo_new_user_account_task >> update_wo_comment_new_user_account_task >> branch_on_wo_comment_status_task >> update_activity_info_wo_comment_new_user_account_err_task >> send_comm_err_wo_comment_new_user_account_err_task

        create_new_user_adid_task >> fetch_activity_info_new_user_account_task >> ad_process_new_user_account_task >> branch_on_ad_process_result_task >> send_communication_non_finished_ad_new_user_account_task >> update_activity_info_non_finished_ad_new_user_account_task

        create_new_user_adid_task >> fetch_activity_info_new_user_account_task >> ad_process_new_user_account_task >> branch_on_ad_process_result_task >> update_activity_new_user_account_decryption_failed_task >> send_communication_new_user_account_decryption_failed_task

    # New Service Account Task Group
    with TaskGroup(group_id='new_service_account_task_group') as new_service_account_task_group:

        fetch_activity_info_new_service_account_task = PythonOperator(
            task_id='fetch_activity_info_new_service_account',
            python_callable=fetch_activity_info_new_service_account,
        )

        ad_process_new_service_account_task = PythonOperator(
            task_id='ad_process_new_service_account',
            python_callable=ad_process_new_service_account,
            retries=8,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=15)
        )

        # Branch based on AD process result
        def branch_based_on_ad_process_result(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_service_account_task_group.ad_process_new_service_account', key='activity_data_request_resolve')

            ad_python_status = activity_data['data']['ad_python_status'].lower()
            ad_new_password = activity_data['data'].get('ad_new_password', None)
            if ad_new_password:
                ad_new_password = ad_new_password.strip()

            if ad_python_status != 'finished':
                return 'new_service_account_task_group.send_communication_non_finished_ad_new_service_account' 
            elif ad_python_status == 'finished' and ad_new_password:
                return 'new_service_account_task_group.rs_update_activity_new_service_account'
            else:
                return 'new_service_account_task_group.update_activity_new_service_account_decryption_failed'

        branch_on_ad_process_result_task = BranchPythonOperator(
            task_id='branch_on_ad_process_result',
            python_callable=branch_based_on_ad_process_result,
        )

        # Manin Flow
        rs_update_activity_new_service_account_task = PythonOperator(
            task_id='rs_update_activity_new_service_account',
            python_callable=rs_update_activity_new_service_account,
        )

        send_notification_new_service_account_create_task = PythonOperator(
            task_id='send_notification_new_service_account_create',
            python_callable=send_notification_new_service_account_create,
        )

        check_shift_rooster_new_service_account_rs_task = PythonOperator(
            task_id='check_shift_rooster_new_service_account_rs',
            python_callable=check_shift_rooster_new_service_account_rs,
        )

        send_communication_shift_wise_fo_new_service_account_task = PythonOperator(
            task_id='send_communication_shift_wise_fo_new_service_account',
            python_callable=send_communication_shift_wise_fo_new_service_account,
        )

        update_wo_comment_new_service_account_task = PythonOperator(
            task_id='update_wo_comment_new_service_account',
            python_callable=update_wo_comment_new_service_account,
        )

        def branch_on_wo_comment_status(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_service_account_task_group.update_wo_comment_new_service_account', key='activity_data_request_resolve')
            status = activity_data['data']['wo_comment_update_new_service_account'].get('status', 'failed')
            if 'failed' not in status.lower():
                return 'new_service_account_task_group.update_wo_status_new_service_account'
            return 'new_service_account_task_group.update_activity_info_wo_comment_new_service_account_err'

        branch_on_wo_comment_status_task = BranchPythonOperator(
            task_id='branch_on_wo_comment_status',
            python_callable=branch_on_wo_comment_status,
        )

        update_wo_status_new_service_account_task = PythonOperator(
            task_id='update_wo_status_new_service_account',
            python_callable=update_wo_status_new_service_account,
        )

        def branch_on_wo_status_update_status(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='new_service_account_task_group.update_wo_status_new_service_account', key='activity_data_request_resolve')
            status = activity_data['data']['wo_result_update_new_service_account'].get('status', 'failed')
            if 'failed' not in status.lower():
                return 'new_service_account_task_group.end_task_new_service_account'
            return 'new_service_account_task_group.update_activity_info_wo_status_new_service_account_err'

        branch_on_wo_status_update_status_task = BranchPythonOperator(
            task_id='branch_on_wo_status_update_status',
            python_callable=branch_on_wo_status_update_status,
        )

        # Alrernate flow wo comment update failed step1
        update_activity_info_wo_comment_new_service_account_err_task = PythonOperator(
            task_id='update_activity_info_wo_comment_new_service_account_err',
            python_callable=update_activity_info_wo_comment_new_service_account_err,
        )

        # Alrernate flow wo comment update failed step2
        send_comm_err_wo_comment_new_service_account_err_task = PythonOperator(
            task_id='send_comm_err_wo_comment_new_service_account_err',
            python_callable=send_comm_err_wo_comment_new_service_account_err,
        )

        # Alrernate flow wo status update failed step1
        update_activity_info_wo_status_new_service_account_err_task = PythonOperator(
            task_id='update_activity_info_wo_status_new_service_account_err',
            python_callable=update_activity_info_wo_status_new_service_account_err,
        )

        # Alrernate flow wo status update failed step2
        send_comm_err_wo_status_new_service_account_err_task = PythonOperator(
            task_id='send_comm_err_wo_status_new_service_account_err',
            python_callable=send_comm_err_wo_status_new_service_account_err,
        )

        # alternate flow: non-finished status step1
        send_communication_non_finished_ad_new_service_account_task = PythonOperator(
            task_id='send_communication_non_finished_ad_new_service_account',
            python_callable=send_communication_non_finished_ad_new_service_account,
        )

        # alternate flow: non-finished status step2
        update_activity_info_non_finished_ad_new_service_account_task = PythonOperator(
            task_id='update_activity_info_non_finished_ad_new_service_account',
            python_callable=update_activity_info_non_finished_ad_new_service_account,
        )

        # alternate flow: decryption failure step1
        update_activity_new_service_account_decryption_failed_task = PythonOperator(
            task_id='update_activity_new_service_account_decryption_failed',
            python_callable=update_activity_new_service_account_decryption_failed,
        )

        # alternate flow: decryption failure step2
        send_communication_new_service_account_decryption_failed_task = PythonOperator(
            task_id='send_communication_new_service_account_decryption_failed',
            python_callable=send_communication_new_service_account_decryption_failed,
        )

        # end task for new service account
        end_task_new_service_account = EmptyOperator(
            task_id='end_task_new_service_account'
        )

        fetch_activity_info_new_service_account_task >> ad_process_new_service_account_task >> branch_on_ad_process_result_task >> rs_update_activity_new_service_account_task >> send_notification_new_service_account_create_task >> check_shift_rooster_new_service_account_rs_task >> send_communication_shift_wise_fo_new_service_account_task >> update_wo_comment_new_service_account_task >> branch_on_wo_comment_status_task >> update_wo_status_new_service_account_task >> branch_on_wo_status_update_status_task >> end_task_new_service_account
        fetch_activity_info_new_service_account_task >> ad_process_new_service_account_task >> branch_on_ad_process_result_task >> rs_update_activity_new_service_account_task >> send_notification_new_service_account_create_task >> check_shift_rooster_new_service_account_rs_task >> send_communication_shift_wise_fo_new_service_account_task >> update_wo_comment_new_service_account_task >> branch_on_wo_comment_status_task >> update_wo_status_new_service_account_task >> branch_on_wo_status_update_status_task >> update_activity_info_wo_status_new_service_account_err_task >> send_comm_err_wo_status_new_service_account_err_task
        fetch_activity_info_new_service_account_task >> ad_process_new_service_account_task >> branch_on_ad_process_result_task >> rs_update_activity_new_service_account_task >> send_notification_new_service_account_create_task >> check_shift_rooster_new_service_account_rs_task >> send_communication_shift_wise_fo_new_service_account_task >> update_wo_comment_new_service_account_task >> branch_on_wo_comment_status_task >> update_activity_info_wo_comment_new_service_account_err_task >> send_comm_err_wo_comment_new_service_account_err_task
        fetch_activity_info_new_service_account_task >> ad_process_new_service_account_task >> branch_on_ad_process_result_task >> send_communication_non_finished_ad_new_service_account_task >> update_activity_info_non_finished_ad_new_service_account_task
        fetch_activity_info_new_service_account_task >> ad_process_new_service_account_task >> branch_on_ad_process_result_task >> update_activity_new_service_account_decryption_failed_task >> send_communication_new_service_account_decryption_failed_task

    # User Account Modification Task Group
    with TaskGroup(group_id='user_account_modification_task_group') as user_account_modification_task_group:
        fetch_activity_info_user_account_modification_task = PythonOperator(
            task_id='fetch_activity_info_user_account_modification',
            python_callable=fetch_activity_info_user_account_modification,
        )

        ad_process_user_account_modification_task = PythonOperator(
            task_id='ad_process_user_account_modification',
            python_callable=ad_process_user_account_modification,
            retries=8,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=15)
        )

        # Branch based on AD process result
        def branch_based_on_ad_process_result(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.ad_process_user_account_modification', key='activity_data_request_resolve')

            ad_python_status = activity_data['data']['ad_python_status'].lower()

            if ad_python_status == 'finished' :
                return 'user_account_modification_task_group.rs_update_activity_user_account_modify'
            else:
                return 'user_account_modification_task_group.send_communication_non_finished_ad_user_account_modify'        

        branch_on_ad_process_result_task = BranchPythonOperator(
            task_id='branch_on_ad_process_result',
            python_callable=branch_based_on_ad_process_result,
        )

        rs_update_activity_user_account_modify_task = PythonOperator(
            task_id='rs_update_activity_user_account_modify',
            python_callable=rs_update_activity_user_account_modify,
        )

        send_notification_user_account_modify_task = PythonOperator(
            task_id='send_notification_user_account_modify',
            python_callable=send_notification_user_account_modify,
        )

        update_wo_comment_user_account_modify_task = PythonOperator(
            task_id='update_wo_comment_user_account_modify',
            python_callable=update_wo_comment_user_account_modify,
        )

        def branch_on_wo_comment_status(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.update_wo_comment_user_account_modify', key='activity_data_request_resolve')
            status = activity_data['data']['wo_comment_update_user_account_modify'].get('status', 'failed')
            if 'failed' not in status.lower():
                return 'user_account_modification_task_group.update_wo_status_user_account_modify'
            return 'user_account_modification_task_group.update_activity_info_wo_comment_user_account_modify_err'

        branch_on_wo_comment_status_task = BranchPythonOperator(
            task_id='branch_on_wo_comment_status',
            python_callable=branch_on_wo_comment_status,
        )

        update_wo_status_user_account_modify_task = PythonOperator(
            task_id='update_wo_status_user_account_modify',
            python_callable=update_wo_status_user_account_modify,
        )

        def branch_on_wo_status_update_status(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.update_wo_status_user_account_modify', key='activity_data_request_resolve')
            status = activity_data['data']['wo_result_update_user_account_modify'].get('status', 'failed')
            if 'failed' not in status.lower():
                return 'user_account_modification_task_group.end_task_user_account_modify'
            return 'user_account_modification_task_group.update_activity_info_wo_status_user_account_modify_err'

        branch_on_wo_status_update_status_task = BranchPythonOperator(
            task_id='branch_on_wo_status_update_status',
            python_callable=branch_on_wo_status_update_status,
        )

        # Alrernate flow wo comment update failed step1
        update_activity_info_wo_comment_user_account_modify_err_task = PythonOperator(
            task_id='update_activity_info_wo_comment_user_account_modify_err',
            python_callable=update_activity_info_wo_comment_user_account_modify_err,
        )

        # Alrernate flow wo comment update failed step2
        send_comm_err_wo_comment_user_account_modify_err_task = PythonOperator(
            task_id='send_comm_err_wo_comment_user_account_modify_err',
            python_callable=send_comm_err_wo_comment_user_account_modify_err,
        )

        # Alrernate flow wo status update failed step1
        update_activity_info_wo_status_user_account_modify_err_task = PythonOperator(
            task_id='update_activity_info_wo_status_user_account_modify_err',
            python_callable=update_activity_info_wo_status_user_account_modify_err,
        )

        # Alrernate flow wo status update failed step2
        send_comm_err_wo_status_user_account_modify_err_task = PythonOperator(
            task_id='send_comm_err_wo_status_user_account_modify_err',
            python_callable=send_comm_err_wo_status_user_account_modify_err,
        )

        # alternate flow: non-finished status step1
        send_communication_non_finished_ad_user_account_modify_task = PythonOperator(
            task_id='send_communication_non_finished_ad_user_account_modify',
            python_callable=send_communication_non_finished_ad_user_account_modify,
        )

        # alternate flow: non-finished status step2
        update_activity_info_non_finished_ad_user_account_modify_task = PythonOperator(
            task_id='update_activity_info_non_finished_ad_user_account_modify',
            python_callable=update_activity_info_non_finished_ad_user_account_modify,
        )

        end_task_user_account_modify_task = EmptyOperator(
            task_id='end_task_user_account_modify'
        )

        fetch_activity_info_user_account_modification_task >> ad_process_user_account_modification_task >> branch_on_ad_process_result_task >> rs_update_activity_user_account_modify_task >> send_notification_user_account_modify_task >> update_wo_comment_user_account_modify_task >> branch_on_wo_comment_status_task >> update_wo_status_user_account_modify_task >> branch_on_wo_status_update_status_task >> end_task_user_account_modify_task
        fetch_activity_info_user_account_modification_task >> ad_process_user_account_modification_task >> branch_on_ad_process_result_task >> rs_update_activity_user_account_modify_task >> send_notification_user_account_modify_task >> update_wo_comment_user_account_modify_task >> branch_on_wo_comment_status_task >> update_wo_status_user_account_modify_task >> branch_on_wo_status_update_status_task >> update_activity_info_wo_status_user_account_modify_err_task >> send_comm_err_wo_status_user_account_modify_err_task
        fetch_activity_info_user_account_modification_task >> ad_process_user_account_modification_task >> branch_on_ad_process_result_task >> rs_update_activity_user_account_modify_task >> send_notification_user_account_modify_task >> update_wo_comment_user_account_modify_task >> branch_on_wo_comment_status_task >> update_activity_info_wo_comment_user_account_modify_err_task >> send_comm_err_wo_comment_user_account_modify_err_task
        fetch_activity_info_user_account_modification_task >> ad_process_user_account_modification_task >> branch_on_ad_process_result_task >> send_communication_non_finished_ad_user_account_modify_task >> update_activity_info_non_finished_ad_user_account_modify_task

    # WO Fetch Fail Task Group
    with TaskGroup(group_id='wo_fetch_fail_task_group') as wo_fetch_fail_task_group:
        update_activity_info_service_request_wo_fetch_fail_task = PythonOperator(
            task_id='update_activity_info_service_request_wo_fetch_fail',
            python_callable=update_activity_info_service_request_wo_fetch_fail,
        )
        send_communication_service_request_request_wo_fetch_fail_task = PythonOperator(
            task_id='send_communication_service_request_request_wo_fetch_fail',
            python_callable=send_communication_service_request_request_wo_fetch_fail,
        )
        update_activity_info_service_request_wo_fetch_fail_task >> send_communication_service_request_request_wo_fetch_fail_task

    # WO Cancel Task Group
    with TaskGroup(group_id='wo_cancel_task_group') as wo_cancel_task_group:
        update_activity_info_service_request_cancel_task = PythonOperator(
            task_id='update_activity_info_service_request_cancel',
            python_callable=update_activity_info_service_request_cancel,
        )

        send_communication_service_request_cancel_task = PythonOperator(
            task_id='send_communication_service_request_cancel',
            python_callable=send_communication_service_request_cancel,
        )

        # Set task dependencies within the group
        update_activity_info_service_request_cancel_task >> send_communication_service_request_cancel_task

    # Service Request Reminder Task Group
    with TaskGroup(group_id='service_request_reminder_task_group') as service_request_reminder_task_group:
        fetch_data_activity_for_sr_reminder_task = PythonOperator(
            task_id='fetch_data_activity_for_sr_reminder',
            python_callable=fetch_data_activity_for_sr_reminder,
        )
        fetch_itsm_approver_sr_reminder_task = PythonOperator(
            task_id='fetch_itsm_approver_sr_reminder',
            python_callable=fetch_itsm_approver_sr_reminder,
        )

        def branch_on_flag_update_date(**context):
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='service_request_reminder_task_group.fetch_itsm_approver_sr_reminder', key='activity_data_request_resolve')
            flag_update_date = 0
            if activity_data and 'data' in activity_data:
                flag_update_date = activity_data['data'].get('request_resolve_initial_data', {}).get('flag_update_date')
            if flag_update_date == 1:
                return 'service_request_reminder_task_group.send_email_communication_sr_reminder_date_flag_1'
            elif flag_update_date == 2:
                return 'service_request_reminder_task_group.send_email_communication_sr_reminder_date_flag_2'
            return None

        branch_on_flag_update_date_task = BranchPythonOperator(
            task_id='branch_on_flag_update_date',
            python_callable=branch_on_flag_update_date,
        )

        send_email_communication_sr_reminder_date_flag_1_task = PythonOperator(
            task_id='send_email_communication_sr_reminder_date_flag_1',
            python_callable=send_email_communication_sr_reminder_date_flag_1,
        )
        sr_activity_update_sr_reminder_date_flag_1_task = PythonOperator(
            task_id='sr_activity_update_sr_reminder_date_flag_1',
            python_callable=sr_activity_update_sr_reminder_date_flag_1,
        )
        send_email_communication_sr_reminder_date_flag_2_task = PythonOperator(
            task_id='send_email_communication_sr_reminder_date_flag_2',
            python_callable=send_email_communication_sr_reminder_date_flag_2,
        )
        sr_activity_update_sr_reminder_date_flag_2_task = PythonOperator(
            task_id='sr_activity_update_sr_reminder_date_flag_2',
            python_callable=sr_activity_update_sr_reminder_date_flag_2,
        )

        fetch_data_activity_for_sr_reminder_task >> fetch_itsm_approver_sr_reminder_task >> branch_on_flag_update_date_task >> send_email_communication_sr_reminder_date_flag_1_task >> sr_activity_update_sr_reminder_date_flag_1_task
        fetch_data_activity_for_sr_reminder_task >> fetch_itsm_approver_sr_reminder_task >> branch_on_flag_update_date_task >> send_email_communication_sr_reminder_date_flag_2_task >> sr_activity_update_sr_reminder_date_flag_2_task

    # Awaiting Response Task Group
    with TaskGroup(group_id='awaiting_response_task_group') as awaiting_response_task_group:
        fetch_data_activity_for_response_task = PythonOperator(
            task_id='fetch_data_activity_for_response',
            python_callable=fetch_data_activity_for_response,
        )

        def branch_on_flag_update_date(**context):
            """
            Branch based on flag_update_date value from request_resolve_initial_data.
            Returns the task_id to execute next in the awaiting_response_task_group.
            When flag_update_date is not 1 or 2, returns None to end the task group without executing any tasks.
            """
            ti = context['ti']
            activity_data = ti.xcom_pull(task_ids='awaiting_response_task_group.fetch_data_activity_for_response', key='activity_data_request_resolve')
            flag_update_date = None
            if activity_data and 'data' in activity_data:
                flag_update_date = activity_data['data'].get('request_resolve_initial_data', {}).get('flag_update_date')

            if flag_update_date == 1:
                return 'awaiting_response_task_group.send_email_communication_awaiting_response_date_flag_1'
            elif flag_update_date == 2:
                return 'awaiting_response_task_group.send_email_communication_awaiting_response_date_flag_2'
            return None

        branch_on_flag_update_date_task = BranchPythonOperator(
            task_id='branch_on_flag_update_date',
            python_callable=branch_on_flag_update_date,
        )

        # Task for flag_update_date == 1
        send_email_communication_awaiting_response_date_flag_1_task = PythonOperator(
            task_id='send_email_communication_awaiting_response_date_flag_1',
            python_callable=send_email_communication_awaiting_response_date_flag_1,
        )

        # Next task for flag_update_date == 1
        sr_activity_update_awaiting_response_date_flag_1_task = PythonOperator(
            task_id='sr_activity_update_awaiting_response_date_flag_1',
            python_callable=sr_activity_update_awaiting_response_date_flag_1,
        )

        # Task for flag_update_date == 2
        send_email_communication_awaiting_response_date_flag_2_task = PythonOperator(
            task_id='send_email_communication_awaiting_response_date_flag_2',
            python_callable=send_email_communication_awaiting_response_date_flag_2,
        )

        # Next task for flag_update_date == 2
        sr_activity_update_awaiting_response_date_flag_2_task = PythonOperator(
            task_id='sr_activity_update_awaiting_response_date_flag_2',
            python_callable=sr_activity_update_awaiting_response_date_flag_2,
        )

        fetch_data_activity_for_response_task >> branch_on_flag_update_date_task >> send_email_communication_awaiting_response_date_flag_1_task >> sr_activity_update_awaiting_response_date_flag_1_task
        fetch_data_activity_for_response_task >> branch_on_flag_update_date_task >> send_email_communication_awaiting_response_date_flag_2_task >> sr_activity_update_awaiting_response_date_flag_2_task

    # Update task dependencies
    create_worklog_task >> check_activity_request_resolve_task >> branch_on_sr_submitted_task >> awaiting_response_task_group >> close_worklog_task

    create_worklog_task >> check_activity_request_resolve_task >> branch_on_sr_submitted_task >> check_wo_status_task >> check_wo_status_branch_task >> update_activity_data_wo_approved_task >> task_type_request_resolve_branch_task >> password_reset_task_group >> close_worklog_task
    create_worklog_task >> check_activity_request_resolve_task >> branch_on_sr_submitted_task >> check_wo_status_task >> check_wo_status_branch_task >> update_activity_data_wo_approved_task >> task_type_request_resolve_branch_task >> new_user_account_task_group >> close_worklog_task
    create_worklog_task >> check_activity_request_resolve_task >> branch_on_sr_submitted_task >> check_wo_status_task >> check_wo_status_branch_task >> update_activity_data_wo_approved_task >> task_type_request_resolve_branch_task >> new_service_account_task_group >> close_worklog_task
    create_worklog_task >> check_activity_request_resolve_task >> branch_on_sr_submitted_task >> check_wo_status_task >> check_wo_status_branch_task >> update_activity_data_wo_approved_task >> task_type_request_resolve_branch_task >> user_account_modification_task_group >> close_worklog_task

    create_worklog_task >> check_activity_request_resolve_task >> branch_on_sr_submitted_task >> check_wo_status_task >> check_wo_status_branch_task >> wo_fetch_fail_task_group >> close_worklog_task
    create_worklog_task >> check_activity_request_resolve_task >> branch_on_sr_submitted_task >> check_wo_status_task >> check_wo_status_branch_task >> wo_cancel_task_group >> close_worklog_task
    create_worklog_task >> check_activity_request_resolve_task >> branch_on_sr_submitted_task >> check_wo_status_task >> check_wo_status_branch_task >> service_request_reminder_task_group >> close_worklog_task

from hooks.worklog_hook import WorkLogHook
from hooks.access_management_db_hook import AccessManagementSQLHook
from typing import Dict, Any
import os
import logging
from airflow.sdk.bases.hook import BaseHook
from hooks.ad_process_hook import AD_ProcessHook
from datetime import datetime, timezone, timedelta
from jinja2 import Template
import httpx
from hooks.notify_hook import NotifyHook
from hooks.mapping_hook import MappingHook

logger = logging.getLogger(__name__)

def fetch_activity_info_user_password_reset_rs(**context) -> Dict[str, Any]:
    """
    Fetch activity information for password reset requests from EXT_ACCESS_MANAGEMENT_ACTIVITY table.

    This function:
    1. Gets activity_data from update_activity_data_wo_approved task via XCom
    2. Queries EXT_ACCESS_MANAGEMENT_ACTIVITY table for request details
    3. Updates activity_data with fetched information
    4. Returns updated activity_data

    Input (from update_activity_data_wo_approved task's activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            },
            'wo_approval_result': {
                'status': 'SR Approved',
                'wo_id': 'WO123456',
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }

    Output (extends activity_data with activity_info_fetched while preserving all previous data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            },
            'wo_approval_result': {
                'status': 'SR Approved',
                'wo_id': 'WO123456',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='update_activity_data_wo_approved', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get the activity identifier from the previous task's data
    activity_identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    if not activity_identifier:
        raise ValueError("No identifier found in activity data")

    # Get connection id from environment variable
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to fetch required fields
    query = f"""
        SELECT 
            u_task_type,
            u_user_name,
            u_user_adid,
            u_wo_no,
            u_user_email,
            u_user_ntid_signum,
            u_svc_name,
            u_env
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_identifier = '{activity_identifier}'
    """

    # Execute query
    result = db_hook.get_row_data(query)

    if not result.get('success') or not result.get('data'):
        hook.error(f"No activity found for identifier: {activity_identifier}")
        raise ValueError(f"No activity found for identifier: {activity_identifier}")

    # Add activity info to existing activity_data
    activity_data['data']['activity_info_fetched'] = {
        'task_type': result['data'][0],       # u_task_type
        'user_name': result['data'][1],       # u_user_name
        'user_adid': result['data'][2],       # u_user_adid
        'wo_no': result['data'][3],           # u_wo_no
        'user_email': result['data'][4],      # u_user_email
        'user_ntid_signum': result['data'][5],  # u_user_ntid_signum
        'svc_name': result['data'][6],        # u_svc_name
        'env': result['data'][7]              # u_env
    }

    hook.info(f"Fetched activity info for identifier: {activity_identifier}")

    # Push updated activity_data back to XCom
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def ad_process_user_password_reset_rs(**context) -> Dict[str, Any]:
    """
    Process AD operations for user password reset request.

    This function:
    1. Gets activity_data from fetch_activity_info_user_password_reset_rs task via XCom
    2. Gets worklog_id from XCom (set by create_worklog task)
    3. Prepares and executes AD process using AD_ProcessHook
    4. Updates activity_data with the process result and processed fields
    5. Stores processed fields for use in downstream tasks

    Input (from fetch_activity_info_user_password_reset_rs task's activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            },
            'wo_approval_result': {
                'status': 'SR Approved',
                'wo_id': 'WO123456',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',  # Original value from AD process
            'ad_service_account_list': '"svc1","svc2","svc3"',  # Formatted with quotes for display

            'ad_new_password_raw': 'P@ssw0rd123',  # Original value from AD process
            'ad_new_password': 'P@ssw0rd123',      # Decoded string for Jinja template use
            'ad_email_password': 'P@ssw0rd123',    # HTML escaped version for email

            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',  # HTML formatted

            'ad_python_status': 'Finished',
            'ad_python_exception': None
        }
    }

    Output (extends activity_data with processed fields from AD process):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            },
            'wo_approval_result': {
                'status': 'SR Approved',
                'wo_id': 'WO123456',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }

    Note: All processed fields (service_account_list, new_password, ad_output_user_password_reset_rs) 
    are only present if they exist in the AD process output. If any field is missing from the 
    AD process output, its corresponding processed fields will be None.
    """
    ti = context['ti']

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {
            "success": False,
            "error": "No worklog ID found in XCom"
        }

    # Initialize worklog hook
    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting AD password reset process")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='password_reset_task_group.fetch_activity_info_user_password_reset_rs', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    wo_no = activity_data['data']['activity_info_fetched']['wo_no']
    process_id = activity_data['data']['process_id']

    hook.info(f"Processing password reset for user: {user_adid}, work order: {wo_no}")

    # Get connection details from Airflow with fallback values
    try:
        # Choose connection based on environment
        if 'prod' not in activity_data['data']['activity_info_fetched']['env'].lower():
            conn = BaseHook.get_connection('ad_process_conn_lab')
            fallback_host = '10.253.228.29'
        else:
            conn = BaseHook.get_connection('ad_process_conn')
            fallback_host = '10.159.176.105'
        ssh_host = conn.host or fallback_host
        ssh_port = conn.port or 22
        ssh_user = conn.login or 'm2m_enable_auto@uprising.t-mobile.com'
        ssh_password = conn.password or 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'
    except Exception as e:
        hook.warning(f"Failed to get connection details from Airflow, using default values: {str(e)}")
        # Set fallback host based on environment
        fallback_host = '10.253.228.29' if 'prod' not in activity_data['data']['activity_info_fetched']['env'].lower() else '10.159.176.105'
        ssh_host = fallback_host
        ssh_port = 22
        ssh_user = 'm2m_enable_auto@uprising.t-mobile.com'
        ssh_password = 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'

    hook.info("Creating input file for password reset")
    # Create input file command for password reset with both user_adid and wo_no
    input_command = f'echo {{"inputvalues": [{{"WO_NO" : "{wo_no}", "USER_ADID" : "{user_adid}"}}],"action_name" : "RESET_PASSWORD - Change Password"}} > "C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\User Password Reset\\ResetUserPass\\resetuserpass_i_{process_id}.json"'

    # Script command to execute the batch file
    script_cmd = '"C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\User Password Reset\\reset_user_pass.bat"'

    # Output file paths
    output_file = f'C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\User Password Reset\\ResetUserPass\\resetuserpass_o_{process_id}.json'
    target_scp_file_path = 'm2m_enable_auto@uprising.t-mobile.com@10.253.29.123:/enable/Access_Management'

    hook.info("Executing AD process")
    # Create and execute hook
    with AD_ProcessHook(
        ssh_host=ssh_host,
        ssh_user=ssh_user,
        ssh_password=ssh_password,
        ssh_port=ssh_port,
        input_command=input_command,
        script_cmd=script_cmd,
        output_file=output_file,
        target_scp_file_path=target_scp_file_path,
        scp_target='m2m_enable_auto@uprising.t-mobile.com@10.253.29.123:/enable/Access_Management',
        scp_password='VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V',
        worklog_id=worklog_id
    ) as ad_process_hook:
        result = ad_process_hook.execute_process()

    if result['success']:
        hook.info("Password reset process completed successfully")
        activity_data['data']['ad_process_result'] = result['data']

        # Process service account list
        svc_acc_list = result['data'].get('SVC_ACCOUNT')
        if svc_acc_list:
            activity_data['data']['ad_service_account_list_raw'] = svc_acc_list
            if svc_acc_list != "No service accounts":
                # Convert to list if it's a string, then format with quotes
                if isinstance(svc_acc_list, str):
                    svc_list = [f'"{acc.strip()}"' for acc in svc_acc_list.split(',')]
                else:
                    svc_list = [f'"{acc}"' for acc in svc_acc_list]
                activity_data['data']['ad_service_account_list'] = ','.join(svc_list)
            else:
                activity_data['data']['ad_service_account_list'] = svc_acc_list

        # Process password
        new_pass = result['data'].get('New_Password')
        if new_pass:
            activity_data['data']['ad_new_password_raw'] = new_pass
            # Handle byte array conversion if needed
            if isinstance(new_pass, list) and all(isinstance(b, int) for b in new_pass):
                try:
                    new_pass_bytes = bytes(new_pass)
                    de_pass = new_pass_bytes.decode('utf-8')  # Or use 'latin1' if utf-8 fails
                except Exception as e:
                    de_pass = f"[DecodeError: {e}]"
            elif isinstance(new_pass, bytes):
                de_pass = new_pass.decode('utf-8')
            else:
                de_pass = str(new_pass)
            # Store just the password string for Jinja template
            activity_data['data']['ad_new_password'] = de_pass
            # Create HTML escaped version for email
            activity_data['data']['ad_email_password'] = de_pass.replace('>', '&gt;').replace('<', '&lt;')

        # Process AD output
        ad_output = result['data'].get('AD_OUTPUT')
        if ad_output:
            # Replace \r\n with <br> for HTML formatting
            activity_data['data']['ad_output_user_password_reset_rs'] = str(ad_output).replace('\\r\\n', '<br>')
        else:
            activity_data['data']['ad_output_user_password_reset_rs'] = None

        # Store other fields
        activity_data['data']['ad_python_status'] = result['data'].get('PYTHON_STATUS')
        activity_data['data']['ad_python_exception'] = result['data'].get('PYTHON_EXCEPTION')

        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
    else:
        hook.error(f"Password reset process failed: {result.get('error', 'Unknown error')}")
        activity_data['data']['ad_process_result'] = result['data']

        # Process service account list
        svc_acc_list = result['data'].get('SVC_ACCOUNT')
        if svc_acc_list:
            activity_data['data']['ad_service_account_list_raw'] = svc_acc_list
            if svc_acc_list != "No service accounts":
                # Convert to list if it's a string, then format with quotes
                if isinstance(svc_acc_list, str):
                    svc_list = [f'"{acc.strip()}"' for acc in svc_acc_list.split(',')]
                else:
                    svc_list = [f'"{acc}"' for acc in svc_acc_list]
                activity_data['data']['ad_service_account_list'] = ','.join(svc_list)
            else:
                activity_data['data']['ad_service_account_list'] = svc_acc_list

        # Process password
        new_pass = result['data'].get('New_Password')
        if new_pass:
            activity_data['data']['ad_new_password_raw'] = new_pass
            # Handle byte array conversion if needed
            if isinstance(new_pass, bytes):
                de_pass = new_pass.decode('utf-8')
            else:
                de_pass = str(new_pass)
            # Store just the password string for Jinja template
            activity_data['data']['ad_new_password'] = de_pass
            # Create HTML escaped version for email
            activity_data['data']['ad_email_password'] = de_pass.replace('>', '&gt;').replace('<', '&lt;')

        # Process AD output
        ad_output = result['data'].get('AD_OUTPUT')
        if ad_output:
            # Replace \r\n with <br> for HTML formatting
            activity_data['data']['ad_output_user_password_reset_rs'] = str(ad_output).replace('\\r\\n', '<br>')
        else:
            activity_data['data']['ad_output_user_password_reset_rs'] = None

        # Store other fields
        activity_data['data']['ad_python_status'] = result['data'].get('PYTHON_STATUS')
        activity_data['data']['ad_python_exception'] = result['data'].get('PYTHON_EXCEPTION')

        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def rs_update_activity_new_password(**context) -> Dict[str, Any]:
    """
    Update activity table with new password and status after successful password reset.

    Input (from ad_process_user_password_reset_rs task's activity_data_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            },
            'wo_approval_result': {
                'status': 'SR Approved',
                'wo_id': 'WO123456',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }

    Output (complete data structure with activity_update_result_user_password_reset added):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            },
            'wo_approval_result': {
                'status': 'SR Approved',
                'wo_id': 'WO123456',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='password_reset_task_group.ad_process_user_password_reset_rs', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    process_id = activity_data['data']['request_resolve_initial_data']['identifier']
    new_password = activity_data['data']['ad_new_password']

    hook.info(f"Updating activity for process_id: {process_id} with new password")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext"), worklog_id=worklog_id)

    # Update activity table
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_new_password = '{new_password}',
            u_status = 'Completed',
            u_remarks = 'Password reset successful.'
        WHERE u_identifier = '{process_id}'
    """

    result = db_hook.execute_query(query)
    if not result.get('success'):
        error_msg = result.get('error', 'Unknown error')
        hook.error(f"Failed to update activity table: {error_msg}")
        activity_data['data']['activity_update_result_user_password_reset'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(f"Failed to update activity table: {error_msg}")

    hook.info(f"Successfully updated activity for process_id: {process_id}")
    activity_data['data']['activity_update_result_user_password_reset'] = {
        'status': 'Updated',
        'message': 'Activity updated with new password',
        'timestamp': datetime.now().isoformat()
    }
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def check_shift_rooster_password_reset_rs(**context) -> Dict[str, Any]:
    """
    Check current shift roster to find who is on shift and get their email details.

    Input (from rs_update_activity_new_password task's activity_data_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            },
            'wo_approval_result': {
                'status': 'SR Approved',
                'wo_id': 'WO123456',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'shift_roster_result': {
                'status': 'Found',
                'shift_engineers': [
                    {
                        'name': 'John Smith',
                        'email': 'john.smith@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '08:00-16:00',
                        'is_current_shift': True
                    },
                    {
                        'name': 'Jane Doe',
                        'email': 'jane.doe@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '07:00-11:00',
                        'is_current_shift': True
                    }
                ],
                'email_list': 'john.smith@company.com; jane.doe@company.com;',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Found 2 engineers on current shift'
            }
        }
    }

    Output Examples:

    1. Engineers Found on Shift:
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            },
            'wo_approval_result': {
                'status': 'SR Approved',
                'wo_id': 'WO123456',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'shift_roster_result': {
                'status': 'Found',
                'shift_engineers': [
                    {
                        'name': 'John Smith',
                        'email': 'john.smith@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '08:00-16:00',
                        'is_current_shift': True
                    },
                    {
                        'name': 'Jane Doe',
                        'email': 'jane.doe@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '07:00-11:00',
                        'is_current_shift': True
                    }
                ],
                'email_list': 'john.smith@company.com; jane.doe@company.com;',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Found 2 engineers on current shift'
            }
        }
    }

    2. No Engineers on Shift:
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            },
            'wo_approval_result': {
                'status': 'SR Approved',
                'wo_id': 'WO123456',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'shift_roster_result': {
                'status': 'Not Found',
                'shift_engineers': [],
                'email_list': '',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'No shift roster entries found'
            }
        }
    }

    3. Error Case:
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            },
            'wo_approval_result': {
                'status': 'SR Approved',
                'wo_id': 'WO123456',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'shift_roster_result': {
                'status': 'Error',
                'error': 'Database connection failed',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Failed to check shift roster'
            }
        }
    }

    Returns:
        Dict[str, Any]: The input activity_data dictionary with shift_roster_result added

    Raises:
        ValueError: If no activity data is found from previous task or if there's an error checking shift roster
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='password_reset_task_group.rs_update_activity_new_password', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    # Get current time in UTC (server time)
    current_time = datetime.now(timezone.utc)
    # Convert to IST for shift calculations (UTC +5:30)
    current_time_ist = current_time.astimezone(timezone(timedelta(hours=5, minutes=30)))
    current_date = current_time_ist.date()
    yesterday_date = current_date - timedelta(days=1)

    # Date handling - commented for future reference
    # current_date = current_time_pst.date()
    # yesterday_date = (current_time_pst - timedelta(days=1)).date()
    # tomorrow_date = (current_time_pst + timedelta(days=1)).date()

    current_hour = current_time_ist.strftime('%H')

    hook.info(f"Checking shift roster for IST time: {current_hour}:00")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext"), worklog_id=worklog_id)

    # Query to get all shift roster entries
    # Note: u_current_date is kept in query for data consistency but not used in shift logic
    query = """
        SELECT
            u_name,
            u_email,
            u_current_date,  -- kept for data consistency
            u_full_time
        FROM EXT_AM_SHIFT_ROSTER_TABLE
        WHERE u_name IS NOT NULL
        AND u_email IS NOT NULL
        AND u_current_date IS NOT NULL  -- kept for data consistency
        AND u_full_time IS NOT NULL
    """

    try:
        logger.info(f"Executing query: {query}")
        hook.info(f"Executing query: {query}")
        result = db_hook.get_records(query)

        if not result:
            activity_data['data']['shift_roster_result'] = {
                'status': 'Not Found',
                'shift_engineers': [],
                'email_list': '',
                'timestamp': current_time.isoformat(),
                'details': 'No shift roster entries found'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return activity_data

        # First, check if there are any entries with today's date
        today_entries = []
        yesterday_entries = []

        for row in result:
            name, email, date_obj, time_range = row

            # Since u_current_date is a DATE column, date_obj is already a datetime.date object
            # No need to call .date() on it
            entry_date = date_obj

            logger.info(f"Processing date for {name}: {date_obj} (type: {type(date_obj)}) -> {entry_date}")
            hook.info(f"Processing date for {name}: {date_obj} (type: {type(date_obj)}) -> {entry_date}")

            if entry_date == current_date:
                today_entries.append(row)
            elif entry_date == yesterday_date:
                yesterday_entries.append(row)

        logger.info(f"Found {len(today_entries)} entries with today's date ({current_date})")
        hook.info(f"Found {len(today_entries)} entries with today's date ({current_date})")

        logger.info(f"Found {len(yesterday_entries)} entries with yesterday's date ({yesterday_date})")
        hook.info(f"Found {len(yesterday_entries)} entries with yesterday's date ({yesterday_date})")

        today_entries = []
        yesterday_entries = []

        for row in result:
            name, email, date_obj, time_range = row

            # Since u_current_date is a DATE column, date_obj is already a datetime.date object
            # No need to call .date() on it
            entry_date = date_obj
            logger.info(f"Processing date for {name}: {date_obj} (type: {type(date_obj)}) -> {entry_date}")

            if entry_date == current_date:
                today_entries.append(row)
            elif entry_date == yesterday_date:
                yesterday_entries.append(row)

        logger.info(f"Found {len(today_entries)} entries with today's date ({current_date})")
        hook.info(f"Found {len(today_entries)} entries with today's date ({current_date})")

        # Also check for overnight shifts that started yesterday and end today
        logger.info(f"Found {len(yesterday_entries)} entries with yesterday's date ({yesterday_date})")
        hook.info(f"Found {len(yesterday_entries)} entries with yesterday's date ({yesterday_date})")

        # For overnight shifts, we need to check both today's entries and yesterday's entries
        # that might be ongoing overnight shifts
        overnight_entries_from_yesterday = []
        if yesterday_entries:
            for row in yesterday_entries:
                name, email, date_obj, time_range = row
                try:
                    start_time, end_time = time_range.split('-')
                    start_hour = int(start_time.strip().split(':')[0])
                    end_hour = int(end_time.strip().split(':')[0])

                    # Check if this is an overnight shift (start > end) that might still be active
                    if start_hour > end_hour:
                        logger.info(f"Found potential ongoing overnight shift from yesterday: {name} ({time_range})")
                        overnight_entries_from_yesterday.append(row)
                except Exception as e:
                    logger.warning(f"Error parsing time range for {name}: {str(e)}")
                    continue

        logger.info(f"Found {len(overnight_entries_from_yesterday)} potential ongoing overnight shifts from yesterday")
        hook.info(f"Found {len(overnight_entries_from_yesterday)} potential ongoing overnight shifts from yesterday")

        # Determine the logic to use
        if today_entries or overnight_entries_from_yesterday:
            logger.info("Today's entries or ongoing overnight shifts found - using DATE-AWARE logic")
            use_date_aware = True
            # Combine today's entries with ongoing overnight shifts from yesterday
            entries_to_process = today_entries + overnight_entries_from_yesterday
            logger.info(f"Total entries to process with date-aware logic: {len(entries_to_process)}")
        else:
            logger.info("No today's entries or ongoing overnight shifts found - using TIME-ONLY logic for ALL entries regardless of date")
            use_date_aware = False
            entries_to_process = result
        shift_engineers = []
        email_list = []
        processed_count = 0
        current_shift_count = 0

        for row in entries_to_process:
            processed_count += 1
            name, email, date_obj, time_range = row
            logger.info(f"Processing entry {processed_count}: {name} ({email}) - Date: {date_obj}, Time: {time_range}")

            # Parse shift time range
            try:
                start_time, end_time = time_range.split('-')
                start_hour = start_time.strip().split(':')[0]
                end_hour = end_time.strip().split(':')[0]

                logger.info(f"  Parsed time range: {start_time.strip()} to {end_time.strip()}")
                logger.info(f"  Start hour: {start_hour}, End hour: {end_hour}")
                hook.info(f"  Parsed time range: {start_time.strip()} to {end_time.strip()}")
                hook.info(f"  Start hour: {start_hour}, End hour: {end_hour}")

                is_current_shift = False
                current_hour_int = int(current_hour)
                start_hour_int = int(start_hour)
                end_hour_int = int(end_hour)

                logger.info(f"  Current hour (int): {current_hour_int}")
                logger.info(f"  Start hour (int): {start_hour_int}")
                logger.info(f"  End hour (int): {end_hour_int}")

                if use_date_aware:
                    # Date-aware logic for today's entries
                    logger.info(f"Using DATE-AWARE logic for {name}")

                    if start_hour_int <= end_hour_int:
                        # Normal shift (e.g., 08:00-16:00)
                        logger.info(f"    Normal shift detected (start <= end)")
                        if start_hour_int <= current_hour_int < end_hour_int:
                            is_current_shift = True
                            logger.info(f"Current hour {current_hour_int} is within normal shift range")
                        else:
                            logger.info(f"Current hour {current_hour_int} is NOT within normal shift range")
                    else:
                        # Overnight shift (e.g., 22:00-06:00)
                        logger.info(f"Overnight shift detected (start > end)")
                        # For overnight shifts, we need to consider multiple scenarios:
                        # 1. Shift started yesterday and ends today (e.g., 22:00 yesterday to 06:00 today)
                        # 2. Shift starts today and ends tomorrow (e.g., 22:00 today to 06:00 tomorrow)

                        if date_obj == yesterday_date:
                            # Shift started yesterday, check if we're in the early morning hours (before end time)
                            logger.info(f"Shift started yesterday ({yesterday_date}), checking early morning hours")
                            if current_hour_int < end_hour_int:
                                is_current_shift = True
                                logger.info(f"Current hour {current_hour_int} is in early morning of overnight shift (started yesterday)")

                        elif date_obj == current_date:
                            # Shift starts today, check if we're in the late night hours (after start time)
                            logger.info(f"Shift starts today ({current_date}), checking late night hours")
                            if current_hour_int >= start_hour_int:
                                is_current_shift = True
                                logger.info(f"Current hour {current_hour_int} is in late night of overnight shift (starts today)")
                            else:
                                logger.info(f"Current hour {current_hour_int} is NOT in late night of overnight shift (starts today)")
                        else:
                            # For any other date, check if it's a recent overnight shift
                            date_diff = abs((date_obj - current_date).days)
                            logger.info(f"Shift date: {date_obj}, date difference: {date_diff} days")

                            if date_diff <= 1:
                                # Recent overnight shift - apply time-only logic
                                logger.info(f"    Recent overnight shift detected, applying time-only logic")
                                if current_hour_int >= start_hour_int or current_hour_int < end_hour_int:
                                    is_current_shift = True
                                    logger.info(f"Current hour {current_hour_int} is within recent overnight shift range")
                                else:
                                    logger.info(f"Current hour {current_hour_int} is NOT within recent overnight shift range")
                else:
                    # Time-only logic for all entries (when no today's date found)
                    logger.info(f"Using TIME-ONLY logic for {name} (ignoring date: {date_obj})")

                    if start_hour_int <= end_hour_int:
                        # Normal shift (e.g., 08:00-16:00)
                        logger.info(f"Normal shift detected (start <= end)")
                        if start_hour_int <= current_hour_int < end_hour_int:
                            is_current_shift = True
                            logger.info(f"Current hour {current_hour_int} is within normal shift range")
                    else:
                        # Overnight shift (e.g., 22:00-06:00)
                        logger.info(f"Overnight shift detected (start > end)")
                        if current_hour_int >= start_hour_int or current_hour_int < end_hour_int:
                            is_current_shift = True
                            logger.info(f"Current hour {current_hour_int} is within overnight shift range")

                if is_current_shift:
                    current_shift_count += 1
                    shift_engineers.append({
                        'name': name,
                        'email': email,
                        'shift_date': date_obj.strftime('%d-%b-%Y'),
                        'shift_time': time_range,
                        'is_current_shift': True,
                        'logic_used': 'date_aware' if use_date_aware else 'time_only_fallback'
                    })
                    email_list.append(email)
                    logger.info(f"{name} is ON CURRENT SHIFT")

            except Exception as e:
                hook.warning(f"Error processing shift entry for {name}: {str(e)}")
                continue

        # Prepare the result
        activity_data['data']['shift_roster_result'] = {
            'status': 'Found' if shift_engineers else 'Not Found',
            'shift_engineers': shift_engineers,
            'email_list': '; '.join(email_list) + ';' if email_list else '',
            'timestamp': current_time.isoformat(),  # Store UTC time
            'details': f"Found {len(shift_engineers)} engineers on current shift"
        }

        hook.info(f"Found {len(shift_engineers)} engineers on current shift")
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

        return activity_data
    except Exception as e:
        error_msg = str(e)
        hook.error(f"Error checking shift roster: {error_msg}")
        activity_data['data']['shift_roster_result'] = {
            'status': 'Error',
            'error': error_msg,
            'timestamp': current_time.isoformat(),  # Store UTC time
            'details': 'Failed to check shift roster'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(f"Error checking shift roster: {error_msg}")

def send_communication_shift_wise_fo_password_reset(**context) -> Dict[str, Any]:
    """
    Send email notification to shift engineers about password reset completion.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data and shift roster details from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification to shift engineers using email hook

    Input (from check_shift_rooster_password_reset_rs task's activity_data_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'shift_roster_result': {
                'status': 'Found',
                'shift_engineers': [
                    {
                        'name': 'John Smith',
                        'email': 'john.smith@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '08:00-16:00',
                        'is_current_shift': True
                    },
                    {
                        'name': 'Jane Doe',
                        'email': 'jane.doe@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '07:00-11:00',
                        'is_current_shift': True
                    }
                ],
                'email_list': 'john.smith@company.com; jane.doe@company.com;',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Found 2 engineers on current shift'
            }
        }
    }

    Output (same as input with shift_communication_result added):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'shift_roster_result': {
                'status': 'Found',
                'shift_engineers': [
                    {
                        'name': 'John Smith',
                        'email': 'john.smith@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '08:00-16:00',
                        'is_current_shift': True
                    },
                    {
                        'name': 'Jane Doe',
                        'email': 'jane.doe@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '07:00-11:00',
                        'is_current_shift': True
                    }
                ],
                'email_list': 'john.smith@company.com; jane.doe@company.com;',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Found 2 engineers on current shift'
            },
            'shift_communication_result': {
                'status': 'Sent',
                'recipients': ['john.smith@company.com', 'jane.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent to 2 shift engineers',
                'result': {
                    # This is a placeholder for the actual result from the notify_hook
                }
            }
        }
    }

    Returns:
        Dict[str, Any]: The input activity_data dictionary with shift_communication_result added

    Raises:
        ValueError: If no activity data is found from previous task or if there's an error sending email
    """
    ti = context['ti']

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {
            "success": False,
            "error": "No worklog ID found in XCom"
        }

    # Initialize worklog hook
    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting email notification to shift engineers")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='password_reset_task_group.check_shift_rooster_password_reset_rs', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get required data from activity_data
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    task_type = activity_data['data']['activity_info_fetched']['task_type']
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    email_password = activity_data['data']['ad_email_password']
    ad_new_password = activity_data['data']['ad_new_password']
    upn = activity_data['data']['request_resolve_initial_data']['upn']
    env = activity_data['data']['activity_info_fetched']['env']

    # Get shift engineers' email list
    shift_result = activity_data['data'].get('shift_roster_result', {})
    if shift_result.get('status') != 'Found':
        hook.warning("No shift engineers found to send notification")
        activity_data['data']['shift_communication_result'] = {
            'status': 'Not Sent',
            'recipients': [],
            'timestamp': datetime.now().isoformat(),
            'details': 'No shift engineers found to send notification'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return activity_data

    # Get recipients from email_list (remove trailing semicolon and split)
    recipients = [email.strip() for email in shift_result['email_list'].rstrip(';').split(';') if email.strip()]
    if not recipients:
        hook.warning("No valid email addresses found in shift roster")
        activity_data['data']['shift_communication_result'] = {
            'status': 'Not Sent',
            'recipients': [],
            'timestamp': datetime.now().isoformat(),
            'details': 'No valid email addresses found in shift roster'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return activity_data

    hook.info(f"Sending password reset notification to {len(recipients)} shift engineers")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="shift_engineer_notification_email_template_password_reset"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for shift engineer notification template")

        # Get the first matching element and extract its value
        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Transform data to match template placeholders
        template_data = {
            "user_name": user_name,
            "user_adid": user_adid,
            "task_type": task_type,
            "identifier": identifier,
            "email_password": email_password,
            "ad_new_password": ad_new_password,
            "upn": upn,
            "environment": env
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | {task_type} | Process Id: {identifier}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": recipients,
                "notifier_id": [user_email]
            })
            hook.info(f"Send email notification task initiated to {len(recipients)} recipients")

            # Update activity_data with success information including the result
            activity_data['data']['shift_communication_result'] = {
                'status': 'Sent',
                'recipients': recipients,
                'timestamp': datetime.now().isoformat(),
                'details': f'Email notification sent to {len(recipients)} shift engineers',
                'result': result
            }

            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return activity_data
        except httpx.HTTPError as e:
            error_msg = f"Failed to send email notification: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['shift_communication_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat(),
                'details': 'Failed to send email notification'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending email notification: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['shift_communication_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
            'details': 'Failed to send email notification'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

def update_wo_comment_user_passwd_reset(**context) -> Dict[str, Any]:
    """
    Update work order with work info comment after password reset.

    This function:
    1. Gets activity data from previous task
    2. Creates a work log entry for the work order
    3. Uses middleware API to create the work log

    Example input (from activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'shift_roster_result': {
                'status': 'Found',
                'shift_engineers': [
                    {
                        'name': 'John Smith',
                        'email': 'john.smith@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '08:00-16:00',
                        'is_current_shift': True
                    },
                    {
                        'name': 'Jane Doe',
                        'email': 'jane.doe@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '07:00-11:00',
                        'is_current_shift': True
                    }
                ],
                'email_list': 'john.smith@company.com; jane.doe@company.com;',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Found 2 engineers on current shift'
            },
            'shift_communication_result': {
                'status': 'Sent',
                'recipients': ['john.smith@company.com', 'jane.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent to 2 shift engineers',
                'result': {
                    # This is a placeholder for the actual result from the notify_hook
                }
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Password reset completed successfully\r\nUser account is not disabled\r\nNew password set',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',
            'ad_service_account_list': '"svc1","svc2","svc3"',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_user_password_reset_rs': 'Password reset completed successfully<br>User account is not disabled<br>New password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with new password',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'shift_roster_result': {
                'status': 'Found',
                'shift_engineers': [
                    {
                        'name': 'John Smith',
                        'email': 'john.smith@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '08:00-16:00',
                        'is_current_shift': True
                    },
                    {
                        'name': 'Jane Doe',
                        'email': 'jane.doe@company.com',
                        'shift_date': '07-Jun-2024',
                        'shift_time': '07:00-11:00',
                        'is_current_shift': True
                    }
                ],
                'email_list': 'john.smith@company.com; jane.doe@company.com;',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Found 2 engineers on current shift'
            },
            'shift_communication_result': {
                'status': 'Sent',
                'recipients': ['john.smith@company.com', 'jane.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent to 2 shift engineers',
                'result': {
                    # This is a placeholder for the actual result from the notify_hook
                }
            },
            'wo_comment_update_user_password_reset ': {
                'status': 'Created',
                'details': {
                    'work_log_id': 'WL-789012',
                    'work_order_id': 'WO123456',
                    // ... other work log details ...
                }
            }
        }
    }

    Example error output (added to activity_data):
    {
        'data': {
            // ... same as input ...
            'wo_comment_update_user_password_reset': {
                'status': 'Failed',
                'error': 'HTTP error occurred: Connection timeout'
            }
        }
    }

    Args:
        **context: Airflow context containing task instance and configuration

    Returns:
        Dict containing success status and work log creation details:
        {
            'success': True,
            'data': {
                'work_log_id': 'WL-789012'
            }
        }
        or
        {
            'success': False,
            'error': 'Error message describing what went wrong'
        }
    """
    ti = context['ti']

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {
            "success": False,
            "error": "No worklog ID found in XCom"
        }

    # Initialize worklog hook
    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting work log creation for password reset")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='password_reset_task_group.send_communication_shift_wise_fo_password_reset', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    # Get work order number from activity data
    wo_status_data = activity_data['data'].get('wo_status_data', {})
    wo_number = wo_status_data.get('wo_id')
    if not wo_number:
        error_msg = "No work order number found in activity data"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    hook.info(f"Creating work log for work order: {wo_number}")

    # Get ticketing system and middleware settings from environment
    ticketing_system: str = os.environ.get("DEFAULT_TICKETING_SYSTEM", "REMEDY")
    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))

    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    work_log_comment = f"Password has been reset for the account({user_adid})."

    # Create work log payload
    work_log_payload = {
        "name": f"Work Log {wo_number}",
        "detailed_description": work_log_comment,
        "short_description": ".",
        "work_order_id": wo_number,
        "work_log_type": "General Information",
        "communication_source": "Email",
        "secure_work_log": "Yes",
        "view_access": "Public",
        "work_order_entry_id": wo_number
    }

    # Call middleware API to create work log
    work_log_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/work-orders/work-logs?system={ticketing_system}"

    try:
        with httpx.Client(verify=False, timeout=480.0) as client:
            response = client.post(
                work_log_url,
                json=work_log_payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            work_log_data = response.json()

            # Extract work log ID from response
            work_log_id = work_log_data.get('id')
            if not work_log_id:
                raise ValueError("No work log ID in API response")

            hook.info(f"Successfully created work log {work_log_id} for work order {wo_number}")

            # Update activity data with just the work log ID
            activity_data['data']['wo_comment_update_user_password_reset'] = {
                'status': 'Created',
                'work_log_id': work_log_id
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return {
                "success": True,
                "work_log_id": work_log_id
            }

    except httpx.HTTPError as e:
        error_msg = f"HTTP error occurred: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['wo_comment_update_user_password_reset'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }
    except Exception as e:
        error_msg = f"Unexpected error occurred: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['wo_comment_update_user_password_reset'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def update_wo_status_user_passwd_reset(**context) -> Dict[str, Any]:
    """
    Update work order status after password reset.

    This function:
    1. Gets WO number from activity_data
    2. Searches for the work order using the API
    3. Gets the request_id from the response
    4. Updates the work order status and details after password reset

    Input (from update_wo_comment_user_passwd_reset task's activity_data_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'wo_id': 'WO123456',
                'sr_status': 'Assigned'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'wo_comment_update_user_password_reset': {
                'status': 'Created',
                'work_log_id': 'WL-789012'
            }
        }
    }

    Output (added to activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'wo_status_data': {
                'wo_id': 'WO123456',
                'sr_status': 'Assigned'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'wo_comment_update_user_password_reset': {
                'status': 'Created',
                'work_log_id': 'WL-789012'
            },
            'wo_result_update_user_password_reset': {
                'status': 'Updated',
                'request_id': 'WO66813',
                'wo_number': 'WO123456'
            }
        }
    }

    Error Output Example:
    {
        'data': {
            // ... same as input ...
            'wo_update_result': {
                'status': 'Failed',
                'error': 'HTTP error occurred: Connection timeout'
            }
        }
    }

    Returns:
        Dict containing success status and minimal work order details:
        {
            'success': True,
            'request_id': 'WO66813',
            'wo_number': 'WO123456'
        }
        or
        {
            'success': False,
            'error': 'Error message describing what went wrong'
        }
    """
    ti = context['ti']

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {
            "success": False,
            "error": "No worklog ID found in XCom"
        }

    # Initialize worklog hook
    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting work order update for password reset")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='password_reset_task_group.update_wo_comment_user_passwd_reset', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    # Get work order number from activity data
    wo_status_data = activity_data['data'].get('wo_status_data', {})
    wo_number = wo_status_data.get('wo_id')
    if not wo_number:
        error_msg = "No work order number found in activity data"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    hook.info(f"Searching for work order with number: {wo_number}")

    # Search for work order
    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))
    ticketing_system: str = os.environ.get("DEFAULT_TICKETING_SYSTEM", "REMEDY")
    search_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/work-orders?work_order_number={wo_number}&system={ticketing_system}"

    try:
        with httpx.Client(verify=False, timeout=480.0) as client:
            # Search for work order
            response = client.get(search_url)
            response.raise_for_status()
            wo_data = response.json()

            if not wo_data or not isinstance(wo_data, list) or len(wo_data) == 0:
                error_msg = f"No work order found with number: {wo_number}"
                hook.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }

            # Get the first matching work order
            work_order = wo_data[0]
            # Real Remedy populates 'request_id' with an internal instance ID.
            # xsystem-mock returns request_id=null but sets work_order_id — fall back to work_order_id.
            request_id = work_order.get('request_id') or work_order.get('work_order_id')
            description = work_order.get('description')

            if not request_id:
                error_msg = "No request ID found in work order response"
                hook.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }

            hook.info(f"Found work order with request ID: {request_id}")
            request_id = request_id.split('|')[0] if '|' in request_id else request_id

            # Get user details from activity data
            user_details = activity_data['data'].get('request_resolve_initial_data', {})
            user_adid = user_details.get('user_adid', '')
            user_name = user_details.get('user_name', '')
            user_email = user_details.get('user_email', '')
            new_password = activity_data['data'].get('ad_new_password', '')

            # Get environment from activity data
            env = activity_data['data']['activity_info_fetched']['env']

            env_val = "Production" if "prod" in env.lower() else "Lab" if "lab" in env.lower() else env

            # Prepare work order update payload
            update_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/work-orders/{request_id}?system={ticketing_system}"
            update_payload = {
                "status": "Completed",
                "status_reason": "Successful",
                # "assigned_change_account": "Svc Enable Automation",
                # "change_coordinator": "Svc Enable Automation",
                "environment": env_val
            }

            logger.info(f"Update URL: {update_url} Update payload: {update_payload}")

            # Update work order
            update_response = client.put(update_url, json=update_payload)
            update_response.raise_for_status()
            update_data = update_response.json()

            logger.info(f"Update response: {update_response.json()}")

            hook.info(f"Updated work order with request ID: {request_id}")

            # Update activity data with work order update result
            activity_data['data']['wo_result_update_user_password_reset'] = {
                'status': 'Updated',
                'request_id': request_id,
                'wo_number': wo_number,
                'task_description': description
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return {
                "success": True,
                "request_id": request_id,
                "wo_number": wo_number,
                "task_description": description
            }

    except httpx.HTTPError as e:
        error_msg = f"HTTP error occurred: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['wo_result_update_user_password_reset'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }
    except Exception as e:
        error_msg = f"Unexpected error occurred: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['wo_result_update_user_password_reset'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def update_activity_info_wo_comment_user_passwd_reset_err(**context) -> Dict[str, Any]:
    """
    Update activity status when WO comment task fails.

    This function:
    1. Gets worklog_id from XCom
    2. Gets process_id and WO number from activity_data
    3. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with error status and remarks
    4. Updates activity_data with the update result

    Example input (from activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'wo_status_data': {
                'wo_id': 'WO123456',
                'sr_status': 'Assigned'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'wo_status_data': {
                'wo_id': 'WO123456',
                'sr_status': 'Assigned'
            },
            'activity_update_result_wo_comment_err': {
                'status': 'Error in ITSM Process',
                'remarks': 'Error in WO Closure process for WO123456, Transfer to FO team. AD task completed and report send.',
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }
    """
    try:
        ti = context['ti']
        
        # Get worklog_id from XCom
        worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
        if not worklog_id:
            logger.error("No worklog ID found in XCom")
            return {
                "success": False,
                "error": "No worklog ID found in XCom"
            }

        # Initialize worklog hook
        try:
            hook = WorkLogHook()
            hook.set_worklog_id(worklog_id)
            hook.info("Starting activity update for WO comment error")
        except Exception as e:
            logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
            return {
                "success": False,
                "error": f"Error initializing worklog hook: {str(e)}"
            }

        # Get activity data from previous task
        activity_data = ti.xcom_pull(task_ids='password_reset_task_group.update_wo_comment_user_passwd_reset', key='activity_data_request_resolve')
        if not activity_data:
            hook.error("No activity data found from previous task")
            return {
                "success": False,
                "error": "No activity data found from previous task"
            }

        process_id = activity_data['data']['process_id']
        wo_number = activity_data['data']['wo_status_data']['wo_id']

        # Get connection id from environment variable
        oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

        # Get database connection
        db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

        # Update activity table
        query = f"""
            UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
            SET u_status = 'Error in ITSM Process',
                u_remarks = 'Error in WO Closure process for {wo_number}, Transfer to FO team. AD task completed and report send.'
            WHERE u_identifier = '{process_id}'
        """

        result = db_hook.execute_query(query)
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity table: {error_msg}")
            activity_data['data']['activity_update_result_wo_comment_err'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return activity_data

        hook.info(f"Successfully updated activity for process_id: {process_id}")
        activity_data['data']['activity_update_result_wo_comment_err'] = {
            'status': 'Error in ITSM Process',
            'remarks': f'Error in WO Closure process for {wo_number}, Transfer to FO team. AD task completed and report send.',
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

        return activity_data

    except Exception as e:
        logger.error(f"Error in update_activity_info_wo_comment_user_passwd_reset_err: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

def send_comm_err_wo_comment_user_passwd_reset_err(**context) -> Dict[str, Any]:
    """
    Send error email notification when work order status update task fails.

    This function:
    1. Gets worklog_id from XCom
    2. Gets work order ID and identifier from activity_data
    3. Prepares error email content using template from mapping
    4. Sends error notification email

    Input (from update_activity_info_wo_status_user_passwd_reset_err task's activity_data_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset'
            },
            'wo_status_data': {
                'wo_id': 'WO123456',
                'sr_status': 'Assigned'
            },
            'activity_update_result_wo_status_err': {
                'status': 'Error in ITSM Process',
                'remarks': 'Error in WO Closure process for WO123456, Transfer to FO team. AD task completed and report send.',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }

    Output (same as input with wo_status_communication_result_err added):
    {
        'data': {
            // ... same as input ...
            'wo_status_communication_result_err': {
                'status': 'Sent',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Error notification email sent',
                'result': {
                    // This is a placeholder for the actual result from the notify_hook
                }
            }
        }
    }

    Returns:
        Dict[str, Any]: The input activity_data dictionary with wo_status_communication_result_err added

    Raises:
        ValueError: If no activity data is found from previous task or if there's an error sending email
    """
    ti = context['ti']

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {
            "success": False,
            "error": "No worklog ID found in XCom"
        }

    # Initialize worklog hook
    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting error email notification for work order status update failure")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data from the error task
    activity_data = ti.xcom_pull(task_ids='password_reset_task_group.update_activity_info_wo_comment_user_passwd_reset_err', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get required data from activity_data
    identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    wo_number = activity_data['data']['wo_status_data']['wo_id']
    error_remarks = activity_data['data']['activity_update_result_wo_status_err']['remarks']
    task_type = activity_data['data']['request_resolve_initial_data']['task_type']
    process_id = activity_data['data']['process_id']
    sr_no = activity_data['data']['request_resolve_initial_data']['sr_no']

    hook.info(f"Sending error notification for work order {wo_number}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="wo_comment_error_notification_email_template_password_reset"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for work order status error notification template")

        # Get the first matching element and extract its value
        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Transform data to match template placeholders
        template_data = {
            "identifier": identifier,
            "wo_number": wo_number,
            "error_remarks": error_remarks,
            "task_type": task_type,
            "process_id": process_id,
            "sr_no": sr_no
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | Error in WO closure | Process Id: {identifier}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        user_email = activity_data['data']['activity_info_fetched']['user_email']

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": email_group,
                "notifier_id": [user_email]
            })
            hook.info(f"Send error notification email task initiated")

            # Update activity_data with success information including the result
            activity_data['data']['wo_status_communication_result_err'] = {
                'status': 'Sent',
                'timestamp': datetime.now().isoformat(),
                'details': 'Error notification email sent',
                'result': result
            }

            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return activity_data
        except httpx.HTTPError as e:
            error_msg = f"Failed to send error notification email: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['wo_status_communication_result_err'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat(),
                'details': 'Failed to send error notification email'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending error notification email: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['wo_status_communication_result_err'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
            'details': 'Failed to send error notification email'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)  


def update_activity_info_wo_status_user_passwd_reset_err(**context) -> Dict[str, Any]:
    """
    Update activity status when WO status task fails.

    This function:
    1. Gets worklog_id from XCom
    2. Gets process_id and WO number from activity_data
    3. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with error status and remarks
    4. Updates activity_data with the update result

    Example input (from activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'wo_status_data': {
                'wo_id': 'WO123456',
                'sr_status': 'Assigned'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'wo_status_data': {
                'wo_id': 'WO123456',
                'sr_status': 'Assigned'
            },
            'activity_update_result_wo_status_err': {
                'status': 'Error in ITSM Process',
                'remarks': 'Error in WO Closure process for WO123456, Transfer to FO team. AD task completed and report send.',
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }
    """
    try:
        ti = context['ti']
        
        # Get worklog_id from XCom
        worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
        if not worklog_id:
            logger.error("No worklog ID found in XCom")
            return {
                "success": False,
                "error": "No worklog ID found in XCom"
            }

        # Initialize worklog hook
        try:
            hook = WorkLogHook()
            hook.set_worklog_id(worklog_id)
            hook.info("Starting activity update for WO status error")
        except Exception as e:
            logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
            return {
                "success": False,
                "error": f"Error initializing worklog hook: {str(e)}"
            }

        # Get activity data from previous task
        activity_data = ti.xcom_pull(task_ids='password_reset_task_group.update_wo_status_user_passwd_reset', key='activity_data_request_resolve')
        if not activity_data:
            hook.error("No activity data found from previous task")
            return {
                "success": False,
                "error": "No activity data found from previous task"
            }

        process_id = activity_data['data']['process_id']
        wo_number = activity_data['data']['wo_status_data']['wo_id']

        # Get connection id from environment variable
        oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

        # Get database connection
        db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

        # Update activity table
        query = f"""
            UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
            SET u_status = 'Error in ITSM Process',
                u_remarks = 'Error in WO Closure process for {wo_number}, Transfer to FO team. AD task completed and report send.'
            WHERE u_identifier = '{process_id}'
        """

        result = db_hook.execute_query(query)
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity table: {error_msg}")
            activity_data['data']['activity_update_result_wo_status_err'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return activity_data

        hook.info(f"Successfully updated activity for process_id: {process_id}")
        activity_data['data']['activity_update_result_wo_status_err'] = {
            'status': 'Error in ITSM Process',
            'remarks': f'Error in WO Closure process for {wo_number}, Transfer to FO team. AD task completed and report send.',
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

        return activity_data

    except Exception as e:
        logger.error(f"Error in update_activity_info_wo_status_user_passwd_reset_err: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

def send_comm_err_wo_status_user_passwd_reset_err(**context) -> Dict[str, Any]:
    """
    Send error email notification when work order status update task fails.

    This function:
    1. Gets worklog_id from XCom
    2. Gets work order ID and identifier from activity_data
    3. Prepares error email content using template from mapping
    4. Sends error notification email

    Input (from update_activity_info_wo_status_user_passwd_reset_err task's activity_data_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset'
            },
            'wo_status_data': {
                'wo_id': 'WO123456',
                'sr_status': 'Assigned'
            },
            'activity_update_result_wo_status_err': {
                'status': 'Error in ITSM Process',
                'remarks': 'Error in WO Closure process for WO123456, Transfer to FO team. AD task completed and report send.',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }

    Output (same as input with wo_status_communication_result_err added):
    {
        'data': {
            // ... same as input ...
            'wo_status_communication_result_err': {
                'status': 'Sent',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Error notification email sent',
                'result': {
                    // This is a placeholder for the actual result from the notify_hook
                }
            }
        }
    }

    Returns:
        Dict[str, Any]: The input activity_data dictionary with wo_status_communication_result_err added

    Raises:
        ValueError: If no activity data is found from previous task or if there's an error sending email
    """
    ti = context['ti']

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {
            "success": False,
            "error": "No worklog ID found in XCom"
        }

    # Initialize worklog hook
    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting error email notification for work order status update failure")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data from the error task
    activity_data = ti.xcom_pull(task_ids='password_reset_task_group.update_activity_info_wo_status_user_passwd_reset_err', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get required data from activity_data
    identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    wo_number = activity_data['data']['wo_status_data']['wo_id']
    sr_no = activity_data['data']['request_resolve_initial_data']['sr_no']
    error_remarks = activity_data['data']['activity_update_result_wo_status_err']['remarks']
    task_type = activity_data['data']['request_resolve_initial_data']['task_type']
    process_id = activity_data['data']['process_id']

    hook.info(f"Sending error notification for work order {wo_number}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="wo_status_error_notification_email_template_password_reset"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for work order status error notification template")

        # Get the first matching element and extract its value
        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Transform data to match template placeholders
        template_data = {
            "identifier": identifier,
            "wo_number": wo_number,
            "error_remarks": error_remarks,
            "task_type": task_type,
            "process_id": process_id,
            "sr_no": sr_no
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | {task_type} | Error in WO closure | Process Id: {identifier}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        user_email = activity_data['data']['activity_info_fetched']['user_email']
        recipients = user_email

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "cc_notifier_id": email_group,
                "notifier_id": user_email
            })
            hook.info(f"Send error notification email task initiated")

            # Update activity_data with success information including the result
            activity_data['data']['wo_status_communication_result_err'] = {
                'status': 'Sent',
                'timestamp': datetime.now().isoformat(),
                'details': 'Error notification email sent',
                'result': result
            }

            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return activity_data
        except httpx.HTTPError as e:
            error_msg = f"Failed to send error notification email: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['wo_status_communication_result_err'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat(),
                'details': 'Failed to send error notification email'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending error notification email: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['wo_status_communication_result_err'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
            'details': 'Failed to send error notification email'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

def send_communication_non_finished_ad_user_password_reset(**context) -> Dict[str, Any]:
    """
    Send email notification for non-finished AD password reset status.

    This function:
    1. Gets worklog_id from XCom
    2. Gets activity data from ad_process_user_password_reset_rs task
    3. Prepares email content using template from mapping
    4. Sends email notification using notify hook

    Input (from ad_process_user_password_reset_rs task's activity_data_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Not Finished',
                'AD_OUTPUT': 'Error processing password reset',
                'New_Password': None,
                'SVC_ACCOUNT': None,
                'PYTHON_EXCEPTION': 'Connection timeout'
            },
            'ad_python_status': 'Not Finished',
            'ad_python_exception': 'Connection timeout'
        }
    }

    Output (same as input with communication_result added):
    {
        'data': {
           'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Not Finished',
                'AD_OUTPUT': 'Error processing password reset',
                'New_Password': None,
                'SVC_ACCOUNT': None,
                'PYTHON_EXCEPTION': 'Connection timeout'
            },
            'ad_python_status': 'Not Finished',
            'ad_python_exception': 'Connection timeout',
            'communication_result_non_finished_ad_user_password_reset': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent for non-finished AD status',
                'result': {
                    # This is a placeholder for the actual result from the notify_hook
                }
            }
        }
    }

    Returns:
        Dict[str, Any]: The input activity_data dictionary with communication_result added

    Raises:
        ValueError: If no activity data is found from previous task or if there's an error sending email
    """
    ti = context['ti']

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {
            "success": False,
            "error": "No worklog ID found in XCom"
        }

    # Initialize worklog hook
    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)

        # Get activity data from previous task
        activity_data = ti.xcom_pull(task_ids='password_reset_task_group.ad_process_user_password_reset_rs', key='activity_data_request_resolve')
        if not activity_data or 'data' not in activity_data:
            hook.error("No activity data found from previous task")
            raise ValueError("No activity data found from previous task")

        # Get required data from activity_data
        activity_info = activity_data['data'].get('activity_info_fetched', {})
        ad_process_result = activity_data['data'].get('ad_process_result', {})
        env = activity_info.get('env', 'Production')

        if not activity_info or not ad_process_result:
            hook.error("Missing required activity info or AD process result")
            raise ValueError("Missing required activity info or AD process result")

        # Get user email for notification
        user_email = activity_info.get('user_email')
        if not user_email:
            hook.error("No user email found in activity info")
            raise ValueError("No user email found in activity info")

        # Get mapping hook for email template
        mapping_hook = MappingHook()
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="password_reset_non_finished_notification_email_template"
        )

        if not mapping_elements:
            hook.error("No mapping element found for password reset non-finished notification template")
            raise ValueError("No mapping element found for password reset non-finished notification template")

        # Get the first matching element and extract its value
        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Prepare template variables
        template_vars = {
            'user_name': activity_info.get('user_name', ''),
            'user_adid': activity_info.get('user_adid', ''),
            'user_ntid_signum': activity_info.get('user_ntid_signum', ''),
            'ad_output': ad_process_result.get('AD_OUTPUT', ''),
            'ad_python_exception': ad_process_result.get('PYTHON_EXCEPTION', ''),
            'process_id': activity_data['data'].get('process_id', ''),
            'sr_no': activity_data['data']['request_resolve_initial_data'].get('sr_no', ''),
            'wo_no': activity_info.get('wo_no', ''),
            'env': activity_info.get('env', 'Production')
        }

        # Render email template
        template = Template(template_content)
        email_content = template.render(**template_vars)
        identifier = activity_data['data']['request_resolve_initial_data']['identifier']

        subject = f"Access Management | Password Reset Failed | Process Id: {identifier}"
        notifier_name = os.environ.get("OSCAR_NOTFIER_NAME", "oscar-notifier")
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        user_email = activity_data['data']['activity_info_fetched']['user_email']

        try:
            # Get notify hook and send email
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": email_content,
                "cc_notifier_id": email_group,
                "notifier_id": user_email
            })

            # Add communication result to activity data
            current_time = datetime.now(timezone.utc)
            hook.info(f"Send error notification email task initiated")

            # Update activity_data with success information including the result
            activity_data['data']['communication_result_non_finished_ad_user_password_reset'] = {
                'status': 'Sent',
                'timestamp': datetime.now().isoformat(),
                'details': 'Non finished ad process email sent',
                'result': result
            }

            hook.info(f"Sent non-finished status notification to {user_email}")
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return activity_data

        except httpx.HTTPError as e:
            error_msg = f"Failed to send error notification email: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['communication_result_non_finished_ad_user_password_reset'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat(),
                'details': 'Failed to send error notification email'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = str(e)
        hook.error(f"Error sending non-finished status notification: {error_msg}")
        current_time = datetime.now(timezone.utc)
        activity_data['data']['communication_result_non_finished_ad_user_password_reset'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': current_time.isoformat(),
            'details': 'Failed to send non-finished status notification'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(f"Error sending non-finished status notification: {error_msg}")

def update_activity_info_non_finished_ad_user_password_reset(**context) -> Dict[str, Any]:
    """
    Update activity table with error status for non-finished AD password reset.

    This function:
    1. Gets worklog_id from XCom
    2. Gets activity data from send_communication_non_finished_ad_user_password_reset task
    3. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with error status and details
    4. Updates activity_data with the update result

    Input (from send_communication_non_finished_ad_user_password_reset task's activity_data_request_resolve):
    {
        'data': {
           'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Not Finished',
                'AD_OUTPUT': 'Error processing password reset',
                'New_Password': None,
                'SVC_ACCOUNT': None,
                'PYTHON_EXCEPTION': 'Connection timeout'
            },
            'ad_python_status': 'Not Finished',
            'ad_python_exception': 'Connection timeout',
            'communication_result_non_finished_ad_user_password_reset': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent for non-finished AD status',
                'result': {
                    # This is a placeholder for the actual result from the notify_hook
                }
            }
        }
    }

    Output (added to activity_data):
    {
        'data': {
           'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Not Finished',
                'AD_OUTPUT': 'Error processing password reset',
                'New_Password': None,
                'SVC_ACCOUNT': None,
                'PYTHON_EXCEPTION': 'Connection timeout'
            },
            'ad_python_status': 'Not Finished',
            'ad_python_exception': 'Connection timeout',
            'communication_result_non_finished_ad_user_password_reset': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent for non-finished AD status',
                'result': {
                    # This is a placeholder for the actual result from the notify_hook
                }
            },
            'activity_update_result_non_finished_ad_user_password_reset': {
                'status': 'Updated',
                'message': 'Activity updated with error status',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }
    """
    ti = context['ti']

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {
            "success": False,
            "error": "No worklog ID found in XCom"
        }

    # Initialize worklog hook
    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting activity update for non-finished AD status")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data from previous task
    activity_data = ti.xcom_pull(task_ids='password_reset_task_group.send_communication_non_finished_ad_user_password_reset', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        hook.error("No activity data found from previous task")
        return {
            "success": False,
            "error": "No activity data found from previous task"
        }

    # Get required data from activity_data
    process_id = activity_data['data']['request_resolve_initial_data']['identifier']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    ad_process_result = activity_data['data'].get('ad_process_result', {})
    new_password = ad_process_result.get('New_Password')
    python_exception = ad_process_result.get('PYTHON_EXCEPTION', 'Unknown error')

    hook.info(f"Updating activity for process_id: {process_id}")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext"), worklog_id=worklog_id)

    # Update activity table
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_user_adid = '{user_adid}',
            u_new_password = {f"'{new_password}'" if new_password else 'NULL'},
            u_status = 'Error in AD Process',
            u_remarks = 'User creation not successful.\n {python_exception}'
        WHERE u_identifier = '{process_id}'
    """

    result = db_hook.execute_query(query)
    if not result.get('success'):
        error_msg = result.get('error', 'Unknown error')
        hook.error(f"Failed to update activity table: {error_msg}")
        activity_data['data']['activity_update_result_non_finished_ad_user_password_reset'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return activity_data

    hook.info(f"Successfully updated activity for process_id: {process_id}")
    activity_data['data']['activity_update_result_non_finished_ad_user_password_reset'] = {
        'status': 'Updated',
        'message': 'Activity updated with error status',
        'timestamp': datetime.now().isoformat()
    }
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def update_activity_password_reset_decryption_failed(**context) -> Dict[str, Any]:
    """
    Update activity table when password decryption fails during password reset.

    This function:
    1. Gets activity data from ad_process_user_password_reset_rs task
    2. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with error status and remarks
    3. Updates activity_data with the update result

    Input (from ad_process_user_password_reset_rs task's activity_data_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Not Finished',
                'AD_OUTPUT': 'Error processing password reset',
                'New_Password': None,
                'SVC_ACCOUNT': None,
                'PYTHON_EXCEPTION': 'Decryption failed: Invalid key format'
            },
            'ad_python_status': 'Not Finished',
            'ad_python_exception': 'Decryption failed: Invalid key format',
            'ad_new_password': None,
            'ad_new_password_raw': None
        }
    }

    Output (added to activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Not Finished',
                'AD_OUTPUT': 'Error processing password reset',
                'New_Password': None,
                'SVC_ACCOUNT': None,
                'PYTHON_EXCEPTION': 'Decryption failed: Invalid key format'
            },
            'ad_python_status': 'Not Finished',
            'ad_python_exception': 'Decryption failed: Invalid key format',
            'ad_new_password': None,
            'ad_new_password_raw': None,
            'activity_update_result_decryption_failed': {
                'status': 'Updated',
                'message': 'Activity updated with decryption failure status',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }

    Error Output Example:
    {
        'data': {
            // ... same as input ...
            'activity_update_result_decryption_failed': {
                'status': 'Failed',
                'error': 'Database connection failed',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }

    Returns:
        Dict[str, Any]: The input activity_data dictionary with activity_update_result_decryption_failed added

    Raises:
        ValueError: If no activity data is found from previous task or if there's an error updating the activity table
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='password_reset_task_group.ad_process_user_password_reset_rs', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    process_id = activity_data['data']['request_resolve_initial_data']['identifier']
    new_password = activity_data['data'].get('ad_new_password')

    hook.info(f"Updating activity for process_id: {process_id} with decryption failure status")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext"), worklog_id=worklog_id)

    # Update activity table
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_new_password = {f"'{new_password}'" if new_password else 'NULL'},
            u_status = 'Error in Decryption',
            u_remarks = 'Password decryption failed during reset process.'
        WHERE u_identifier = '{process_id}'
    """

    result = db_hook.execute_query(query)
    if not result.get('success'):
        error_msg = result.get('error', 'Unknown error')
        hook.error(f"Failed to update activity table: {error_msg}")
        activity_data['data']['activity_update_result_decryption_failed'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(f"Failed to update activity table: {error_msg}")

    hook.info(f"Successfully updated activity for process_id: {process_id}")
    activity_data['data']['activity_update_result_decryption_failed'] = {
        'status': 'Updated',
        'message': 'Activity updated with decryption failure status',
        'timestamp': datetime.now().isoformat()
    }
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def send_communication_password_reset_decryption_failed(**context) -> Dict[str, Any]:
    """
    Send email notification when password decryption fails during password reset.

    This function:
    1. Gets activity data from update_activity_password_reset_decryption_failed task
    2. Gets email template from mapping
    3. Sends email notification using notify hook
    4. Updates activity_data with communication result

    Input (from update_activity_password_reset_decryption_failed task's activity_data_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'Password Reset',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'svc_name': 'Service1',
                'env': 'production',
                'wo_no': 'WO12345'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Not Finished',
                'AD_OUTPUT': 'Error processing password reset',
                'New_Password': None,
                'SVC_ACCOUNT': None,
                'PYTHON_EXCEPTION': 'Decryption failed: Invalid key format'
            },
            'ad_python_status': 'Not Finished',
            'ad_python_exception': 'Decryption failed: Invalid key format',
            'ad_new_password': None,
            'ad_new_password_raw': None,
            'activity_update_result_decryption_failed': {
                'status': 'Updated',
                'message': 'Activity updated with decryption failure status',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }

    Output (added to activity_data):
    {
        'data': {
            // ... same as input ...
            'communication_result_decryption_failed': {
                'status': 'Sent',
                'recipients': ['team@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email sent successfully'
            }
        }
    }

    Error Output Example:
    {
        'data': {
            // ... same as input ...
            'communication_result_decryption_failed': {
                'status': 'Failed',
                'error': 'Failed to send email: Connection timeout',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }

    Returns:
        Dict[str, Any]: The input activity_data dictionary with communication_result_decryption_failed added.
        In case of errors, returns activity_data with error information in communication_result_decryption_failed.
    """
    try:
        ti = context['ti']
        activity_data = ti.xcom_pull(task_ids='password_reset_task_group.update_activity_password_reset_decryption_failed', key='activity_data_request_resolve')
        if not activity_data or 'data' not in activity_data:
            raise ValueError("No activity data found from previous task")

        # Get worklog_id from XCom
        worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)

        process_id = activity_data['data']['request_resolve_initial_data']['identifier']
        hook.info(f"Sending decryption failure notification for process_id: {process_id}")

        # Get mapping hook for email template
        mapping_hook = MappingHook()
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="password_reset",
            mapping_namespace_name="access_management",
            mapping_key="decryption_failed_notification_email_template_password_reset"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for decryption failure notification template")

        # Get the first matching element and extract its value
        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Send email using notify hook
        subject = f"Access Management | Service account creation | Process Id: {process_id}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        user_email = activity_data['data']['activity_info_fetched']['user_email']
        env = activity_data['data']['activity_info_fetched'].get('env', 'PROD')
        user_name = activity_data['data']['activity_info_fetched'].get('user_name', '')
        user_email = activity_data['data']['activity_info_fetched'].get('user_email', '')
        user_adid = activity_data['data']['activity_info_fetched'].get('user_adid', '')
        wo_no = activity_data['data']['activity_info_fetched'].get('wo_no', '')

        recipients = os.environ.get("PASSWORD_RESET_NOTIFICATION_RECIPIENTS", "ericsson.tmus.operate.services.-.fo@t-mobile.com").split(",")

        # Transform data to match template placeholders
        template_data = {
            "process_id": process_id,
            "environment": env,
            "user_name": user_name,
            "user_email": user_email,
            "user_adid": user_adid,
            "wo_no": wo_no
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": email_group,
                "notifier_id": user_email
            })
            hook.info(f"Send decryption failure notification email task initiated to {len(recipients)} recipients")

            # Update activity_data with success information
            activity_data['data']['communication_result_decryption_failed'] = {
                'status': 'Sent',
                'recipients': recipients,
                'timestamp': datetime.now().isoformat(),
                'details': f'Email notification sent to {len(recipients)} recipients',
                'result': result
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return activity_data

        except httpx.HTTPError as e:
            error_msg = f"Failed to send decryption failure notification: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['communication_result_decryption_failed'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat(),
                'details': 'Failed to send decryption failure notification'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending decryption failure notification: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['communication_result_decryption_failed'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
            'details': 'Failed to send decryption failure notification'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

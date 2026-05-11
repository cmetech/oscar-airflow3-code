from hooks.worklog_hook import WorkLogHook
from hooks.access_management_db_hook import AccessManagementSQLHook
from typing import Dict, Any
import os
import logging
from airflow.hooks.base import BaseHook
from hooks.ad_process_hook import AD_ProcessHook
from datetime import datetime, timezone, timedelta
from jinja2 import Template
import httpx
from hooks.mapping_hook import MappingHook
from hooks.notify_hook import NotifyHook

logger = logging.getLogger(__name__)

def fetch_activity_info_user_account_modification(**context) -> Dict[str, Any]:
    """
    Fetch activity information for user account modification requests from EXT_ACCESS_MANAGEMENT_ACTIVITY table.

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
                'task_type': 'User Account Modification',
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

    Output (extends activity_data with activity_info_fetched):
    {
        'data': {
            // ... previous data ...
            'activity_info_fetched': {
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='update_activity_data_wo_approved', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    activity_identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    if not activity_identifier:
        raise ValueError("No identifier found in activity data")

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to fetch required fields for user account modification
    query = f"""
        SELECT
            u_wo_no,
            u_user_name,
            u_user_email,
            u_user_adid,
            u_role,
            u_modification_action,
            u_sr_no,
            u_env,
            u_description,
            u_task_type
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_identifier = '{activity_identifier}'
    """

    result = db_hook.get_row_data(query)

    if not result.get('success') or not result.get('data'):
        hook.error(f"No activity found for identifier: {activity_identifier}")
        raise ValueError(f"No activity found for identifier: {activity_identifier}")

    # Add activity info to existing activity_data
    activity_data['data']['activity_info_fetched'] = {
        'wo_no': result['data'][0],           # u_wo_no
        'user_name': result['data'][1],       # u_user_name
        'user_email': result['data'][2],      # u_user_email
        'user_adid': result['data'][3],       # u_user_adid
        'role': result['data'][4],            # u_role
        'modification_action': result['data'][5],  # u_modification_action
        'sr_no': result['data'][6],           # u_sr_no
        'environment': result['data'][7],     # u_env
        'description': result['data'][8],     # u_description
        'task_type': result['data'][9]        # u_task_type
    }

    hook.info(f"Fetched activity info for identifier: {activity_identifier}")
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def ad_process_user_account_modification(**context) -> Dict[str, Any]:
    """
    Process AD operations for user account modification request.

    This function:
    1. Gets activity_data from fetch_activity_info_user_account_modification task via XCom
    2. Gets worklog_id from XCom (set by create_worklog task)
    3. Prepares and executes AD process using AD_ProcessHook
    4. Updates activity_data with the process result and processed fields
    5. Stores processed fields for use in downstream tasks

    Input (from fetch_activity_info_user_account_modification task's activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'User Account Modification',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            }
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
                'task_type': 'User Account Modification',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 0,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456'
            },
            'activity_info_fetched': {
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User account modified successfully\r\nAccount modified\r\nChanges applied',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            'ad_service_account_list_raw': 'svc1,svc2,svc3',  # Original value from AD process
            'ad_service_account_list': '"svc1","svc2","svc3"',  # Formatted with quotes for display
            'ad_new_password_raw': 'P@ssw0rd123',  # Original value from AD process
            'ad_new_password': 'P@ssw0rd123',      # Decoded string for Jinja template use
            'ad_email_password': 'P@ssw0rd123',    # HTML escaped version for email
            'ad_output_user_account_modification': 'User account modified successfully<br>Account modified<br>Changes applied',  # HTML formatted
            'ad_python_status': 'Finished',
            'ad_python_exception': None
        }
    }
    """
    ti = context['ti']

    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {
            "success": False,
            "error": "No worklog ID found in XCom"
        }

    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting AD user account modification process")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.fetch_activity_info_user_account_modification', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    activity_info = activity_data['data']['activity_info_fetched']
    process_id = activity_data['data']['process_id']

    hook.info(f"Processing user account modification for user: {activity_info['user_adid']}, work order: {activity_info['wo_no']}")

    # Get connection details from Airflow with fallback values
    try:
        # Choose connection based on environment
        if 'prod' not in activity_info['environment'].lower():
            conn = BaseHook.get_connection('ad_process_conn_lab')
            fallback_host = '10.253.228.29'
            fallback_user = 'm2m_enable_auto@lab.uprising.t-mobile.com'
            fallback_password = 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'
        else:
            conn = BaseHook.get_connection('ad_process_conn')
            fallback_host = '10.159.176.105'
            fallback_user = 'm2m_enable_auto@lab.uprising.t-mobile.com'
            fallback_password = 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'            
        ssh_host = conn.host or fallback_host
        ssh_port = conn.port or 22
        ssh_user = conn.login or fallback_user
        ssh_password = conn.password or fallback_password
    except Exception as e:
        hook.warning(f"Failed to get connection details from Airflow, using default values: {str(e)}")
        # Set fallback host based on environment
        fallback_host = '10.253.228.29' if 'prod' not in activity_info['environment'].lower() else '10.159.176.105'
        ssh_host = fallback_host
        ssh_port = 22
        ssh_user = 'm2m_enable_auto@uprising.t-mobile.com'
        ssh_password = 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'

    hook.info("Creating input file for user account modification")

    # Create input file command with modification details
    # input_command = f'echo {{"inputvalues":[{{"WO_NO":"{activity_info["wo_no"]}","USER_ADID":"{activity_info["user_adid"]}","USER_ROLE":"{activity_info["role"]}","USER_ACTION":"{activity_info["modification_action"]}"}}],"action_name":"Modify User Account"}} > "C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\Modify User Account\\ModifyUserAcc\\modifyuseracc_i_{process_id}.json"'
    input_command = f'''echo {{"inputvalues":[{{"WO_NO":"{activity_info["wo_no"]}","USER_ADID":"{activity_info["user_adid"]}","USER_ROLE":"{activity_info["role"]}","USER_ACTION":"{activity_info["modification_action"]}"}}],"action_name":"Modify User Account"}} > "C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\Modify User Account\\ModifyUserAcc\\modifyuseracc_i_{process_id}.json"'''

    # file cmd: echo {"inputvalues": [{"WO_NO" : "$FLOW{WO_NO}", "USER_ADID":"$FLOW{USER_ADID}", "USER_ROLE" : "$FLOW{ROLE}", "USER_ACTION" : "$FLOW{MODIFICATN_ACTION}"}],"action_name" : "Modify User Account"}  >  "C:\Users\m2m_enable_auto\Documents\Access Management Automation\Modify User Account\ModifyUserAcc\modifyuseracc_i_$PARAM{IDENTIFIER}.json"
    logger.info(f"Input command AD process: {input_command}")

    # file cmd: echo {"inputvalues": [{"WO_NO" : "$FLOW{WO_NO}", "USER_ADID":"$FLOW{USER_ADID}", "USER_ROLE" : "$FLOW{ROLE}", "USER_ACTION" : "$FLOW{MODIFICATN_ACTION}"}],"action_name" : "Modify User Account"}  >  "C:\Users\m2m_enable_auto\Documents\Access Management Automation\Modify User Account\ModifyUserAcc\modifyuseracc_i_$PARAM{IDENTIFIER}.json"

    script_cmd = '"C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\Modify User Account\\modify_useracc.bat"'

    logger.info(f"Script command AD process: {script_cmd}")

    # "C:\Users\m2m_enable_auto\Documents\Access Management Automation\Modify User Account\modify_useracc.bat"

    output_file = f'C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\Modify User Account\\ModifyUserAcc\\modifyuseracc_o_{process_id}.json'

    target_scp_file_path = 'm2m_enable_auto@uprising.t-mobile.com@10.253.29.123:/enable/Access_Management'

    hook.info("Executing AD process")
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

        logger.info(f"AD process result: {result}")

    if result['success']:
        hook.info("User account modification process completed successfully")
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
            activity_data['data']['ad_output_user_account_modification'] = str(ad_output).replace('\\r\\n', '<br>')
        else:
            activity_data['data']['ad_output_user_account_modification'] = None

        # Store other fields
        activity_data['data']['ad_python_status'] = result['data'].get('PYTHON_STATUS')
        activity_data['data']['ad_python_exception'] = result['data'].get('PYTHON_EXCEPTION')

        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
    else:
        hook.error(f"User account modification process failed: {result.get('error', 'Unknown error')}")
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
            activity_data['data']['ad_output_user_account_modification'] = str(ad_output).replace('\\r\\n', '<br>')
        else:
            activity_data['data']['ad_output_user_account_modification'] = None

        # Store other fields
        activity_data['data']['ad_python_status'] = result['data'].get('PYTHON_STATUS')
        activity_data['data']['ad_python_exception'] = result['data'].get('PYTHON_EXCEPTION')

        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def rs_update_activity_user_account_modify(**context) -> Dict[str, Any]:
    """
    Update activity table with user account modification details after successful modification.

    Input (from ad_process_user_account_modify task's activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'User Account Modification',
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
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Role modification completed successfully',
                'PYTHON_EXCEPTION': None
            },
            'ad_output_user_account_modify': 'Role modification completed successfully',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_account_modification': {
                'status': 'Updated',
                'message': 'Activity updated with user account modification details',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }

    Output (complete data structure with activity_update_result_user_account_modification added):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'User Account Modification',
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
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Role modification completed successfully',
                'PYTHON_EXCEPTION': None
            },
            'ad_output_user_account_modify': 'Role modification completed successfully',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_account_modification': {
                'status': 'Updated',
                'message': 'Activity updated with user account modification details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_user_account_modify': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.ad_process_user_account_modification', key='activity_data_request_resolve')

    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    process_id = activity_data['data']['request_resolve_initial_data']['identifier']
    role = activity_data['data']['activity_info_fetched']['role']
    modification_action = activity_data['data']['activity_info_fetched']['modification_action']

    hook.info(f"Updating activity for process_id: {process_id} with user account modification details")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext"), worklog_id=worklog_id)

    # Update activity table
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_status = 'Completed',
            u_remarks = '{role} - the role is successfully {modification_action}'
        WHERE u_identifier = '{process_id}'
    """

    result = db_hook.execute_query(query)
    if not result.get('success'):
        error_msg = result.get('error', 'Unknown error')
        hook.error(f"Failed to update activity table: {error_msg}")
        activity_data['data']['activity_update_result_user_account_modification'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(f"Failed to update activity table: {error_msg}")

    hook.info(f"Successfully updated activity for process_id: {process_id}")
    activity_data['data']['activity_update_result_user_account_modification'] = {
        'status': 'Updated',
        'message': 'Activity updated with user account modification details',
        'timestamp': datetime.now().isoformat()
    }
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def send_notification_user_account_modify(**context) -> Dict[str, Any]:
    """
    Send email notifications for new user account creation.

    This function:
    1. Gets activity_data from rs_update_activity_user_account_modify task via XCom
    2. Gets template from mapping using MappingHook
    3. Prepares email content using template with upn and user_name
    4. Sends email notification using NotifyHook

    Input (from rs_update_activity_user_account_modify task's activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'User Account Modification',
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
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Role modification completed successfully',
                'PYTHON_EXCEPTION': None
            },
            'ad_output_user_account_modify': 'Role modification completed successfully',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_account_modification': {
                'status': 'Updated',
                'message': 'Activity updated with user account modification details',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }

    Output (extends activity_data with notification_result):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'User Account Modification',
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
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Role modification completed successfully',
                'PYTHON_EXCEPTION': None
            },
            'ad_output_user_account_modify': 'Role modification completed successfully',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_account_modification': {
                'status': 'Updated',
                'message': 'Activity updated with user account modification details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_user_account_modify': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
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
        hook.info("Starting email notification for new user account creation")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.rs_update_activity_user_account_modify', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get only required data from activity_data
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    description = activity_data['data']['activity_info_fetched']['description']
    env = activity_data['data']['activity_info_fetched']['environment']
    wo_no = activity_data['data']['activity_info_fetched']['wo_no']

    modification_action = activity_data['data']['activity_info_fetched']['modification_action']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']

    if user_adid and 'prod' in env.lower():
        upn = user_adid + '@uprising.t-mobile.com'
    elif user_adid:
        upn = user_adid + '@@lab.uprising.t-mobile.com'

    # Validate recipient email
    if not user_email:
        hook.warning("No recipient email address found")
        activity_data['data']['notification_result_user_account_modify'] = {
            'status': 'Not Sent',
            'recipients': [],
            'timestamp': datetime.now().isoformat(),
            'details': 'No recipient email address found'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return activity_data

    hook.info(f"Sending new user account notification to {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="user_account_modify_notification_email_template"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for new user account notification template")

        # Get the first matching element and extract its value
        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Transform data to match template placeholders - only upn and user_name
        template_data = {
            "user_name": user_name,
            "upn": upn,
            "description": description,
            "modification_action": modification_action,
            "user_adid": user_adid,
            "environment": env,
            "wo_no": wo_no,
            "identifier": identifier
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | User Account Modify | Process Id: {identifier}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        user_email = activity_data['data']['activity_info_fetched']['user_email']

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": [email_group],
                "notifier_id": [user_email]
            })
            hook.info(f"Send email notification task initiated to {user_email}")

            # Update activity_data with success information including the result
            activity_data['data']['notification_result_user_account_modify'] = {
                'status': 'Sent',
                'recipients': [user_email],
                'timestamp': datetime.now().isoformat(),
                'details': 'Email notification sent successfully',
                'result': result
            }

            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return activity_data
        except httpx.HTTPError as e:
            error_msg = f"Failed to send email notification: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['notification_result_user_account_modify'] = {
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
        activity_data['data']['notification_result_user_account_modify'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
            'details': 'Failed to send email notification'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

def update_wo_comment_user_account_modify(**context) -> Dict[str, Any]:
    """
    Update work order with work info comment after new user account creation.

    This function:
    1. Gets activity data from previous task
    2. Creates a work log entry for the work order
    3. Uses middleware API to create the work log

    Example input (from activity_data):
    {
        'data': {
    Input (from check_shift_rooster_new_user_account_rs task's activity_data_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'User Account Modification',
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
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Role modification completed successfully',
                'PYTHON_EXCEPTION': None
            },
            'ad_output_user_account_modify': 'Role modification completed successfully',
            'ad_python_status': 'Finished',
            'ad_python_exception': None,
            'activity_update_result_user_account_modification': {
                'status': 'Updated',
                'message': 'Activity updated with user account modification details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_user_account_modify': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
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
                'task_type': 'New User Account Creation',
                'upn': 'newuser@uprising.t-mobile.com',
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
            'generated_adid': {
                'upn': 'newuser@uprising.t-mobile.com',
                'user_adid': 'newuser01',
                'generation_timestamp': '2024-06-07 12:34:56',
                'first_name': 'New',
                'last_name': 'User',
                'full_name': 'New User'
            },
            'activity_info_fetched': {
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User account created successfully\r\nAccount enabled\r\nPassword set',
                'New_Password': 'P@ssw0rd123',
                'USER_ADID': 'newuser01',
                'PYTHON_EXCEPTION': None
            },
            'ad_user_adid': 'newuser01',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_new_user_account': 'User account created successfully<br>Account enabled<br>Password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None
            'activity_update_result_user_account_modification': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_user_account_modify': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'wo_comment_update_user_account_modify': {
                'status': 'Created',
                'work_log_id': 'WL-789012'
            }
        }
    }

    Example error output (added to activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account Creation',
                'upn': 'newuser@uprising.t-mobile.com',
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
            'generated_adid': {
                'upn': 'newuser@uprising.t-mobile.com',
                'user_adid': 'newuser01',
                'generation_timestamp': '2024-06-07 12:34:56',
                'first_name': 'New',
                'last_name': 'User',
                'full_name': 'New User'
            },
            'activity_info_fetched': {
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User account created successfully\r\nAccount enabled\r\nPassword set',
                'New_Password': 'P@ssw0rd123',
                'USER_ADID': 'newuser01',
                'PYTHON_EXCEPTION': None
            },
            'ad_user_adid': 'newuser01',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_new_user_account': 'User account created successfully<br>Account enabled<br>Password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None
            'activity_update_result_user_account_modification': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_user_account_modify': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'wo_comment_update_user_account_modify': {
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
        hook.info("Starting work log creation for new user account")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.send_notification_user_account_modify', key='activity_data_request_resolve')
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

    role = activity_data['data']['activity_info_fetched']['role']
    modification_action = activity_data['data']['activity_info_fetched']['modification_action']
    env = activity_data['data']['activity_info_fetched']['environment']
    work_log_comment = f"The requested role {role} has been {modification_action} to/from the account."

    # Create work log payload
    work_log_payload = {
        "name": f"Work Log {wo_number}",
        "detailed_description": work_log_comment,
        "short_description": ".",
        "work_order_id": wo_number,
        "environment": env,
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
            activity_data['data']['wo_comment_update_user_account_modify'] = {
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
        activity_data['data']['wo_comment_update_user_account_modify'] = {
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
        activity_data['data']['wo_comment_update_user_account_modify'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def update_wo_status_user_account_modify(**context) -> Dict[str, Any]:
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
                'task_type': 'New User Account Creation',
                'upn': 'newuser@uprising.t-mobile.com',
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
            'generated_adid': {
                'upn': 'newuser@uprising.t-mobile.com',
                'user_adid': 'newuser01',
                'generation_timestamp': '2024-06-07 12:34:56',
                'first_name': 'New',
                'last_name': 'User',
                'full_name': 'New User'
            },
            'activity_info_fetched': {
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User account created successfully\r\nAccount enabled\r\nPassword set',
                'New_Password': 'P@ssw0rd123',
                'USER_ADID': 'newuser01',
                'PYTHON_EXCEPTION': None
            },
            'ad_user_adid': 'newuser01',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_new_user_account': 'User account created successfully<br>Account enabled<br>Password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None
            'activity_update_result_user_account_modification': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_user_account_modify': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'wo_comment_update_user_account_modify': {
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
                'task_type': 'New User Account Creation',
                'upn': 'newuser@uprising.t-mobile.com',
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
            'generated_adid': {
                'upn': 'newuser@uprising.t-mobile.com',
                'user_adid': 'newuser01',
                'generation_timestamp': '2024-06-07 12:34:56',
                'first_name': 'New',
                'last_name': 'User',
                'full_name': 'New User'
            },
            'activity_info_fetched': {
                'wo_no': 'WO12345',
                'user_name': 'John Doe',
                'user_email': 'john.doe@uprising.t-mobile.com',
                'user_adid': 'esshar01',
                'role': 'Developer',
                'modification_action': 'ADD',
                'sr_no': 'REQ123456',
                'environment': 'UPRISING',
                'description': 'Add user to Developer group',
                'task_type': 'User Account Modification'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User account created successfully\r\nAccount enabled\r\nPassword set',
                'New_Password': 'P@ssw0rd123',
                'USER_ADID': 'newuser01',
                'PYTHON_EXCEPTION': None
            },
            'ad_user_adid': 'newuser01',
            'ad_new_password_raw': 'P@ssw0rd123',
            'ad_new_password': 'P@ssw0rd123',
            'ad_email_password': 'P@ssw0rd123',
            'ad_output_new_user_account': 'User account created successfully<br>Account enabled<br>Password set',
            'ad_python_status': 'Finished',
            'ad_python_exception': None
            'activity_update_result_user_account_modification': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_user_account_modify': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'wo_comment_update_user_account_modify': {
                'status': 'Created',
                'work_log_id': 'WL-789012'
            },
            'wo_result_update_user_account_modify': {
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
    activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.update_wo_comment_user_account_modify', key='activity_data_request_resolve')
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
            env = activity_data['data']['activity_info_fetched']['environment']

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

            # Update work order
            update_response = client.put(update_url, json=update_payload)
            update_response.raise_for_status()
            update_data = update_response.json()

            hook.info(f"Updated work order with request ID: {request_id}")

            # Update activity data with work order update result
            activity_data['data']['wo_result_update_user_account_modify'] = {
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
        activity_data['data']['wo_result_update_user_account_modify'] = {
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
        activity_data['data']['wo_result_update_user_account_modify'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

# alter flows
# Alter flow functions : wo_comment_update task fails flow step1
def update_activity_info_wo_comment_user_account_modify_err(**context) -> Dict[str, Any]:
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
        activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.update_wo_comment_user_account_modify', key='activity_data_request_resolve')
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
        logger.error(f"Error in update_activity_info_wo_comment_user_account_modify_err: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

# Alter flow functions : wo_comment_update task fails flow step2
def send_comm_err_wo_comment_user_account_modify_err(**context) -> Dict[str, Any]:
    """
    Send error email notification when work order status update task fails.

    This function:
    1. Gets worklog_id from XCom
    2. Gets work order ID and identifier from activity_data
    3. Prepares error email content using template from mapping
    4. Sends error notification email

    Input (from update_activity_info_wo_comment_user_account_modify_err task's activity_data_request_resolve):
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
    activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.update_activity_info_wo_comment_user_account_modify_err', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get required data from activity_data
    identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    wo_number = activity_data['data']['wo_status_data']['wo_id']
    sr_number = activity_data['data']['wo_status_data']['sr_no']
    error_remarks = activity_data['data']['activity_update_result_wo_status_err']['remarks']
    task_type = activity_data['data']['request_resolve_initial_data']['task_type']

    hook.info(f"Sending error notification for work order {wo_number}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="wo_status_error_notification_email_template_user_account_modify"
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
            "wo_no": wo_number,
            "sr_no": sr_number,
            "error_remarks": error_remarks,
            "task_type": task_type,
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
                "cc_notifier_id": [email_group],
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

# Alter flow functions : wo_status_update task fails flow step1
def update_activity_info_wo_status_user_account_modify_err(**context) -> Dict[str, Any]:
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
        activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.update_wo_status_user_account_modify', key='activity_data_request_resolve')
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
        logger.error(f"Error in update_activity_info_wo_status_user_account_modify_err: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

# Alter flow functions : wo_status_update task fails flow step2
def send_comm_err_wo_status_user_account_modify_err(**context) -> Dict[str, Any]:
    """
    Send error email notification when work order status update task fails.

    This function:
    1. Gets worklog_id from XCom
    2. Gets work order ID and identifier from activity_data
    3. Prepares error email content using template from mapping
    4. Sends error notification email

    Input (from update_activity_info_wo_status_user_account_modify_err task's activity_data_request_resolve):
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
    activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.update_activity_info_wo_status_user_account_modify_err', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get required data from activity_data
    identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    wo_number = activity_data['data']['wo_status_data']['wo_id']
    sr_number = activity_data['data']['wo_status_data']['sr_no']

    task_type = activity_data['data']['request_resolve_initial_data']['task_type']

    hook.info(f"Sending error notification for work order {wo_number}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="wo_status_error_notification_email_template_user_account_modify"
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
            "wo_no": wo_number,
            "sr_no": sr_number,
            "task_type": task_type,
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
                "cc_notifier_id": [email_group],
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


# Non finished ad process flow step1
def send_communication_non_finished_ad_user_account_modify(**context) -> Dict[str, Any]:
    """
    Send email notification for non-finished AD password reset status.

    This function:
    1. Gets worklog_id from XCom
    2. Gets activity data from ad_process_user_account_modification task
    3. Prepares email content using template from mapping
    4. Sends email notification using notify hook

    Input (from ad_process_user_account_modification task's activity_data_request_resolve):
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
            'communication_result_non_finished_ad_user_account_modify': {
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
        activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.ad_process_user_account_modification', key='activity_data_request_resolve')
        if not activity_data or 'data' not in activity_data:
            hook.error("No activity data found from previous task")
            raise ValueError("No activity data found from previous task")

        # Get required data from activity_data
        activity_info = activity_data['data'].get('activity_info_fetched', {})
        ad_process_result = activity_data['data'].get('ad_process_result', {})

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
            mapping_key="user_account_modify_non_finished_notification_email_template"
        )

        if not mapping_elements:
            hook.error("No mapping element found for user account modification non-finished notification template")
            raise ValueError("No mapping element found for user account modification non-finished notification template")

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
            'ad_output': ad_process_result.get('AD_OUTPUT', ''),
            'ad_python_exception': ad_process_result.get('PYTHON_EXCEPTION', ''),
            'process_id': activity_data['data'].get('process_id', ''),
            'sr_no': activity_info.get('sr_no', ''),
            'wo_no': activity_info.get('wo_no', '')
        }

        # Render email template
        template = Template(template_content)
        email_content = template.render(**template_vars)
        identifier = activity_data['data']['request_resolve_initial_data']['identifier']

        subject = f"Access Management | User Account Modification | Process Id: {identifier}"
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
                "cc_notifier_id": [email_group],
                "notifier_id": [user_email]
            })

            # Add communication result to activity data
            current_time = datetime.now(timezone.utc)
            hook.info(f"Send error notification email task initiated")

            # Update activity_data with success information including the result
            activity_data['data']['communication_result_non_finished_ad_user_account_modify'] = {
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
            activity_data['data']['communication_result_non_finished_ad_user_account_modify'] = {
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
        activity_data['data']['communication_result_non_finished_ad_user_account_modify'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': current_time.isoformat(),
            'details': 'Failed to send non-finished status notification'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(f"Error sending non-finished status notification: {error_msg}")

# Non finished ad process flow step2
def update_activity_info_non_finished_ad_user_account_modify(**context) -> Dict[str, Any]:
    """
    Update activity table with error status for non-finished AD password reset.

    This function:
    1. Gets worklog_id from XCom
    2. Gets activity data from send_communication_non_finished_ad_user_account_modify task
    3. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with error status and details
    4. Updates activity_data with the update result

    Input (from send_communication_non_finished_ad_user_account_modify task's activity_data_request_resolve):
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
            'update_activity_info_non_finished_ad_user_account_modify': {
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
            'update_activity_info_non_finished_ad_user_account_modify': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent for non-finished AD status',
                'result': {
                    # This is a placeholder for the actual result from the notify_hook
                }
            },
            'activity_update_result_non_finished_ad_new_user_account': {
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
    activity_data = ti.xcom_pull(task_ids='user_account_modification_task_group.send_communication_non_finished_ad_user_account_modify', key='activity_data_request_resolve')
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
        activity_data['data']['activity_update_result_non_finished_ad_new_user_account'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return activity_data

    hook.info(f"Successfully updated activity for process_id: {process_id}")
    activity_data['data']['activity_update_result_non_finished_ad_new_user_account'] = {
        'status': 'Updated',
        'message': 'Activity updated with error status',
        'timestamp': datetime.now().isoformat()
    }
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

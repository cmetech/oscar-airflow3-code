from hooks.worklog_hook import WorkLogHook
from hooks.access_management_db_hook import AccessManagementSQLHook
from typing import Dict, Any
import os
import logging
from airflow.sdk.bases.hook import BaseHook
from hooks.ad_process_hook import AD_ProcessHook
from fabric import Connection as FabricConnection
from datetime import datetime, timezone, timedelta
from jinja2 import Template
import httpx
from hooks.mapping_hook import MappingHook
from hooks.notify_hook import NotifyHook

logger = logging.getLogger(__name__)


def create_new_user_adid(**context) -> Dict[str, Any]:
    """
    Create a new user ADID based on the user's email address.

    Input (from update_activity_data_wo_approved task's activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
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

    Output (for fetch_activity_info_new_user_account):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
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
            'generated_adid': {
                'upn': 'user@uprising.t-mobile.com',
                'user_adid': 'user',
                'generation_timestamp': '2024-06-07 12:34:56'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='update_activity_data_wo_approved', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from update_activity_data_wo_approved task")

    identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Legacy query (adapted) – query EXT_ACCESS_MANAGEMENT_ACTIVITY for u_user_name, u_organization, and u_env
    query = "select u_user_name, u_organization, u_env from EXT_ACCESS_MANAGEMENT_ACTIVITY where u_identifier = '{}'".format(identifier)
    logger.info(f"Query: {query}")

    result = db_hook.get_row_data(query)

    if not (result.get('success') and result.get('data')):
        hook.error("No data returned for identifier: " + identifier)
        raise ValueError("No data returned for identifier: " + identifier)

    (username, org, env) = (result['data'][0], result['data'][1], result['data'][2])
    logger.info(f"Username: {username}, Org: {org}, Env: {env}")

    # Legacy logic – prepend (‘t' or 'e') based on org
    adid = "t" if org.upper() == "T-MOBILE" else ("e" if org.upper() == "ERICSSON" else "")

    # Legacy logic – split username (using whitespace) and build adid
    username_l = username.split()
    if len(username_l) > 1:
        first_name = username_l[0].lower()
        last_name = "".join(username_l[1:]).lower()
        # (Legacy: if last_name is short, append "xxxx" – here we always append "xxxx" if last_name is less than 4 chars)
        if (len(last_name) < 4):
            last_name += "xxxx"
        adid += (first_name[0] + last_name[:4] + "01")
    else:
        first_name = username_l[0].lower()
        # Just first character - pad with extra x to match 7 character total
        adid += (first_name[0] + "xxxx" + "01")

    # Legacy logic – loop (up to 10) to check (via EXT_AM_ITSM_USER_LIST) if adid is already present; if so, increment counter (replacing last digit) and recheck
    count = 1
    hasuser = "true"
    while (hasuser == "true" and count < 10):
        query_user_itsm = "select * from EXT_AM_ITSM_USER_LIST where u_login_id = '{}' and u_env = '{}'".format(adid, env.upper())
        result_user_itsm = db_hook.get_row_data(query_user_itsm)
        if (result_user_itsm.get('success') and result_user_itsm.get('data') and len(result_user_itsm['data']) > 0):
            hook.info("AD Id already present; incrementing counter.")
            adid = adid[:-1] + str(count)
            count += 1
        else:
            hasuser = "false"

    # Real-time AD validation — verify adid is not already taken in live AD (DB list may be stale)
    try:
        if env.upper() == "LAB":
            try:
                _ad_conn_obj = BaseHook.get_connection('ad_process_conn_lab')
                _ad_ssh_host = _ad_conn_obj.host or '10.253.228.29'
                _ad_ssh_user = _ad_conn_obj.login or 'm2m_enable_auto@lab.uprising.t-mobile.com'
                _ad_ssh_password = _ad_conn_obj.password or 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'
            except Exception:
                _ad_ssh_host = '10.253.228.29'
                _ad_ssh_user = 'm2m_enable_auto@lab.uprising.t-mobile.com'
                _ad_ssh_password = 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'
        else:
            try:
                _ad_conn_obj = BaseHook.get_connection('ad_process_conn')
                _ad_ssh_host = _ad_conn_obj.host or '10.159.176.105'
                _ad_ssh_user = _ad_conn_obj.login or 'm2m_enable_auto@uprising.t-mobile.com'
                _ad_ssh_password = _ad_conn_obj.password or 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'
            except Exception:
                _ad_ssh_host = '10.159.176.105'
                _ad_ssh_user = 'm2m_enable_auto@uprising.t-mobile.com'
                _ad_ssh_password = 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'

        _ad_check_count = 0
        _max_ad_checks = 10
        while _ad_check_count < _max_ad_checks:
            with FabricConnection(host=_ad_ssh_host, user=_ad_ssh_user, port=22, connect_kwargs={"password": _ad_ssh_password}) as _fab_conn:
                _ps_result = _fab_conn.run(
                    f'powershell.exe -Command "(Get-ADUser -Filter \\"SamAccountName -eq \'{adid}\'\\") -ne $null"',
                    hide=True, warn=True
                )
            _ad_exists = _ps_result.stdout.strip().lower() == 'true'
            if _ad_exists:
                hook.info(f"ADID {adid} already exists in real AD; incrementing suffix.")
                adid = adid[:-2] + str(int(adid[-2:]) + 1).zfill(2)
                _ad_check_count += 1
            else:
                hook.info(f"ADID {adid} confirmed available in real AD.")
                break
        else:
            hook.warning(f"AD real-time check exhausted {_max_ad_checks} attempts; proceeding with DB-verified ADID.")
    except Exception as _ad_exc:
        hook.warning(f"AD real-time ADID validation failed: {str(_ad_exc)}; proceeding with DB-verified ADID.")

    # Legacy logic – prepend ('@lab.uprising.t-mobile.com' or ('@uprising.t-mobile.com') to adid to form UPN
    if (env.upper() == "LAB"):
        upn = adid + "@lab.uprising.t-mobile.com"
    else:
        upn = adid + "@uprising.t-mobile.com"

    # Update activity_data (extend with generated_adid) and push via XCom
    activity_data['data']['generated_adid'] = {
        'upn': upn,
        'user_adid': adid,
        'generation_timestamp': datetime.now().isoformat(),  # Use a real timestamp
        'first_name': first_name,
        'last_name': last_name if len(username_l) > 1 else '',
        'full_name': username
    }
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def fetch_activity_info_new_user_account(**context) -> Dict[str, Any]:
    """
    Fetch activity information for new user account requests from EXT_ACCESS_MANAGEMENT_ACTIVITY table.

    This function:
    1. Gets activity_data from create_new_user_adid task via XCom (which includes the generated ADID with keys 'upn', 'user_adid', 'generation_timestamp', 'first_name', 'last_name', and 'full_name').
    2. Queries EXT_ACCESS_MANAGEMENT_ACTIVITY table for request details (fields: u_user_name, u_wo_no, u_user_email, u_user_ntid_signum, u_role, u_ref_adid, u_env, u_task_type).
    3. Updates activity_data with fetched information.
    4. Returns updated activity_data for ad_process_new_user_account task.

    Input (from create_new_user_adid task's activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
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
            'generated_adid': {
                'upn': 'user@uprising.t-mobile.com',
                'user_adid': 'user',
                'generation_timestamp': '2024-06-07 12:34:56',
                'first_name': 'John',
                'last_name': 'Doe',
                'full_name': 'John Doe'
            }
        }
    }

    Output (extends activity_data with activity_info_fetched):
    {
        'data': {
            // ... previous data (including generated_adid) ...
            'activity_info_fetched': {
                'user_name': 'John Doe',
                'wo_no': 'WO12345',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.create_new_user_adid', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    logger.info(f"Activity data: {activity_data}")

    activity_identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    if not activity_identifier:
        raise ValueError("No identifier found in activity data")

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to fetch required fields for new user account (using the new query fields)
    query = f"""
        SELECT
            u_user_name, u_wo_no, u_user_email, u_user_ntid_signum, u_role, u_ref_adid, u_env, u_task_type
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_identifier = '{activity_identifier}'
    """
    logger.debug(f"Fetching activity info for identifier: {activity_identifier}")

    result = db_hook.get_row_data(query)

    if not result.get('success') or not result.get('data'):
        hook.error(f"No activity found for identifier: {activity_identifier}")
        raise ValueError(f"No activity found for identifier: {activity_identifier}")

    # Add activity info to existing activity_data (using the new query fields)
    activity_data['data']['activity_info_fetched'] = {
        'user_name': result['data'][0],       # u_user_name
        'wo_no': result['data'][1],           # u_wo_no
        'user_email': result['data'][2],      # u_user_email
        'user_ntid_signum': result['data'][3],  # u_user_ntid_signum
        'role': result['data'][4],            # u_role
        'ref_adid': result['data'][5],        # u_ref_adid
        'env': result['data'][6],             # u_env
        'task_type': result['data'][7]        # u_task_type
    }

    hook.info(f"Fetched activity info for identifier: {activity_identifier}")
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def ad_process_new_user_account(**context) -> Dict[str, Any]:
    """
    Process AD operations for new user account request.

    This function:
    1. Gets activity_data from fetch_activity_info_new_user_account task via XCom
    2. Gets worklog_id from XCom (set by create_worklog task)
    3. Prepares and executes AD process using AD_ProcessHook
    4. Updates activity_data with the process result and processed fields
    5. Stores processed fields for use in downstream tasks

    Input (from fetch_activity_info_new_user_account task's activity_data):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'SR Submitted',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
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
            'generated_adid': {
                'upn': 'user@uprising.t-mobile.com',
                'user_adid': 'user',
                'generation_timestamp': '2024-06-07 12:34:56',
                'first_name': 'John',
                'last_name': 'Doe',
                'full_name': 'John Doe'
            },
            'activity_info_fetched': {
                'user_name': 'John Doe',
                'wo_no': 'WO12345',
                'user_email': 'john.doe@company.com',
                'user_ntid_signum': 'JDOE123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
            }
        }
    }

    Output (extends activity_data with processed fields from AD process):
    {
        'data': {
            # ... (all input fields remain unchanged) ...
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User account created successfully\r\nPassword reset completed\r\nAccount enabled',
                'New_Password': 'P@ssw0rd123',
                'SVC_ACCOUNT': 'svc1,svc2,svc3',
                'PYTHON_EXCEPTION': None
            },
            # Processed service account list fields
            'ad_service_account_list_raw': 'svc1,svc2,svc3',  # Original value from AD process
            'ad_service_account_list': '"svc1","svc2","svc3"',  # Formatted with quotes for display
            
            # Processed password fields
            'ad_new_password_raw': 'P@ssw0rd123',  # Original value from AD process
            'ad_new_password': 'P@ssw0rd123',      # Decoded string for Jinja template use
            'ad_email_password': 'P@ssw0rd123',    # HTML escaped version for email
            
            # Processed AD output
            'ad_output_new_user_account': 'User account created successfully<br>Password reset completed<br>Account enabled',  # HTML formatted
            
            # Status fields
            'ad_python_status': 'Finished',
            'ad_python_exception': None
        }
    }

    Note: All processed fields (service_account_list, new_password, ad_output_new_user_account) 
    are only present if they exist in the AD process output. If any field is missing from the 
    AD process output, its corresponding processed fields will be None.
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
        hook.info("Starting AD new user account process")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.fetch_activity_info_new_user_account', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    activity_info = activity_data['data']['activity_info_fetched']
    generated_adid = activity_data['data']['generated_adid']

    process_id = activity_data['data']['process_id']

    hook.info(f"Processing new user account creation for user: {activity_info['user_name']}, work order: {activity_info['wo_no']}")

    try:
        # Choose connection based on environment
        if 'prod' not in activity_info['env'].lower():
            conn = BaseHook.get_connection('ad_process_conn_lab')
            fallback_host = '10.253.228.29'
            fallback_user = 'm2m_enable_auto@lab.uprising.t-mobile.com'
            fallback_password = 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'
        else:
            conn = BaseHook.get_connection('ad_process_conn')
            fallback_host = '10.159.176.105'
            fallback_user = 'm2m_enable_auto@uprising.t-mobile.com'
            fallback_password = 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'
        ssh_host = conn.host or fallback_host
        ssh_port = conn.port or 22
        ssh_user = conn.login or fallback_user
        ssh_password = conn.password or fallback_password
    except Exception as e:
        hook.warning(f"Failed to get connection details from Airflow, using default values: {str(e)}")
        # Set fallback host based on environment
        fallback_host = '10.253.228.29' if 'prod' not in activity_info['env'].lower() else '10.159.176.105'
        ssh_host = fallback_host
        ssh_port = 22
        ssh_user = 'm2m_enable_auto@uprising.t-mobile.com'
        ssh_password = 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'

    hook.info("Creating input file for new user account")

    wo_no_ad = activity_info['wo_no']
    user_email_ad = activity_info['user_email']
    user_full_name_ad = generated_adid['full_name']
    user_first_name_ad = generated_adid['first_name']
    user_last_name_ad = generated_adid['last_name']
    user_adid_ad = generated_adid['user_adid']
    user_upn_ad = generated_adid['upn']
    user_ntid_ad = activity_info['user_ntid_signum']

    # Create input file command (updated as per legacy command)
    input_command = f'''echo {{"inputvalues":[{{"WO_NO":"{wo_no_ad}","USER_FULL_NAME":"{user_full_name_ad}","USER_FIRST_NAME":"{user_first_name_ad}","USER_LAST_NAME":"{user_last_name_ad}","USER_ADID":"{user_adid_ad}","USER_EMAIL":"{user_email_ad}","USER_EMPID":"{user_ntid_ad}"}}],"action_name":"New User Account Creation"}} > "C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\New User Account Creation\\NewUserAcc\\newuseracc_i_{process_id}.json"'''

    # the folder name in AD machine is 'New User Account Creation'
    script_cmd = '"C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\New User Account Creation\\new_user_creation.bat"'
    output_file = f'C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\New User Account Creation\\NewUserAcc\\newuseracc_o_{process_id}.json'
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
        # Commenting out actual AD process since user was already created
        result = ad_process_hook.execute_process()

        logger.info(f"Result from AD Process: {result}")
        logger.debug(f"ssh_host: {ssh_host}, ssh_user: {ssh_user}, ssh_password: {ssh_password}, ssh_port: {ssh_port}, input_command: {input_command}, script_cmd: {script_cmd}, output_file: {output_file}, target_scp_file_path: {target_scp_file_path},  worklog_id: {worklog_id} ")

    if result['success']:
        hook.info("New user account process completed successfully")
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
            activity_data['data']['ad_output_new_user_account'] = str(ad_output).replace('\\r\\n', '<br>')
        else:
            activity_data['data']['ad_output_new_user_account'] = None

        # Store other fields
        activity_data['data']['ad_python_status'] = result['data'].get('PYTHON_STATUS')
        activity_data['data']['ad_python_exception'] = result['data'].get('PYTHON_EXCEPTION')

        logger.info(f"Activity data: {activity_data}")

        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
    else:
        hook.error(f"New user account process failed: {result.get('error', 'Unknown error')}")
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
            activity_data['data']['ad_output_new_user_account'] = str(ad_output).replace('\\r\\n', '<br>')
        else:
            activity_data['data']['ad_output_new_user_account'] = None

        # Store other fields
        activity_data['data']['ad_python_status'] = result['data'].get('PYTHON_STATUS')
        activity_data['data']['ad_python_exception'] = result['data'].get('PYTHON_EXCEPTION')

        logger.info(f"Activity data: {activity_data}")

        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def rs_update_activity_new_user_account(**context) -> Dict[str, Any]:
    """
    Update activity table with new user account details after successful user creation.

    Input (from ad_process_new_user_account task's activity_data):
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
        }
    }

    Output (complete data structure with activity_update_result_new_user_account added):
    {
        'data': {
            # ... (all input data remains the same) ...
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.ad_process_new_user_account', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    process_id = activity_data['data']['request_resolve_initial_data']['identifier']
    new_password = activity_data['data']['ad_new_password']
    user_adid = activity_data['data']['generated_adid'].get('user_adid')
    user_full_name = activity_data['data']['generated_adid'].get('full_name')
    user_upn = activity_data['data']['generated_adid'].get('upn')
    
    # Ensure ad_user_adid is set if not already present
    if 'ad_user_adid' not in activity_data['data']:
        activity_data['data']['ad_user_adid'] = user_adid

    hook.info(f"Updating activity for process_id: {process_id} with new user account details")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext"), worklog_id=worklog_id)

    # Update activity table
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_user_adid = '{user_adid}',
            u_new_password = '{new_password}',
            u_status = 'Completed',
            u_remarks = 'User created successfully.'
        WHERE u_identifier = '{process_id}'
    """

    result = db_hook.execute_query(query)
    if not result.get('success'):
        error_msg = result.get('error', 'Unknown error')
        hook.error(f"Failed to update activity table: {error_msg}")
        activity_data['data']['activity_update_result_new_user_account'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(f"Failed to update activity table: {error_msg}")

    hook.info(f"Successfully updated activity for process_id: {process_id}")
    activity_data['data']['activity_update_result_new_user_account'] = {
        'status': 'Updated',
        'message': 'Activity updated with new user account details',
        'timestamp': datetime.now().isoformat()
    }
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def send_notification_new_user_account_create(**context) -> Dict[str, Any]:
    """
    Send email notifications for new user account creation.

    This function:
    1. Gets activity_data from rs_update_activity_new_user_account task via XCom
    2. Gets template from mapping using MappingHook
    3. Prepares email content using template with upn and user_name
    4. Sends email notification using NotifyHook

    Input (from rs_update_activity_new_user_account task's activity_data):
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_new_user_account_create': {
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
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.rs_update_activity_new_user_account', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get only required data from activity_data
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    user_email = activity_data['data']['activity_info_fetched']['user_email']

    upn = activity_data['data']['generated_adid'].get('upn', '')
    identifier = activity_data['data']['request_resolve_initial_data']['identifier']

    # Validate recipient email
    if not user_email:
        hook.warning("No recipient email address found")
        activity_data['data']['notification_result_new_user_account_create'] = {
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
            mapping_key="new_user_account_notification_email_template"
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
            "upn": upn
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | User Account Creation| Process Id: {identifier}"
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
            activity_data['data']['notification_result_new_user_account_create'] = {
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
            activity_data['data']['notification_result_new_user_account_create'] = {
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
        activity_data['data']['notification_result_new_user_account_create'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
            'details': 'Failed to send email notification'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

def check_shift_rooster_new_user_account_rs(**context) -> Dict[str, Any]:
    """
    Check shift roster for new user account request and prepare shift engineer notification.

    This function:
    1. Gets activity_data from send_notification_new_user_account_create task via XCom
    2. Gets current shift roster information from EXT_AM_SHIFT_ROSTER_TABLE
    3. Prepares shift engineer email list for notification
    4. Updates activity_data with shift roster information

    Input (from send_notification_new_user_account_create task's activity_data_request_resolve):
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_new_user_account_create': {
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

    Output (extends activity_data with shift_roster_result_new_user_acc):
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_new_user_account_create': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'shift_roster_result_new_user_acc': {
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
        hook.info("Starting shift roster check for new user account request")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data from previous task
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.send_notification_new_user_account_create', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get current time in UTC (server time)
    current_time = datetime.now(timezone.utc)
    # Convert to IST for shift calculations (UTC + 5:30)
    current_time_ist = current_time.astimezone(timezone(timedelta(hours=5, minutes=30)))
    current_hour = current_time_ist.strftime('%H')
    current_date = current_time_ist.date()
    yesterday_date = current_date - timedelta(days=1)

    hook.info(f"Checking shift roster for IST time: {current_hour}:00")

    try:
        # Get database connection
        db_hook = AccessManagementSQLHook(connection_id=os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext"), worklog_id=worklog_id)

        # Query to get all shift roster entries
        query = """
            SELECT
                u_name,
                u_email,
                u_current_date,
                u_full_time
            FROM EXT_AM_SHIFT_ROSTER_TABLE
            WHERE u_name IS NOT NULL
            AND u_email IS NOT NULL
            AND u_current_date IS NOT NULL
            AND u_full_time IS NOT NULL
        """

        logger.info(f"Executing query: {query}")
        hook.info(f"Executing query: {query}")
        result = db_hook.get_records(query)

        if not result:
            activity_data['data']['shift_roster_result_new_user_acc'] = {
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
        activity_data['data']['shift_roster_result_new_user_acc'] = {
            'status': 'Found' if shift_engineers else 'Not Found',
            'shift_engineers': shift_engineers,
            'email_list': '; '.join(email_list) + ';' if email_list else '',
            'timestamp': current_time.isoformat(),
            'details': f"Found {len(shift_engineers)} engineers on current shift"
        }

        hook.info(f"Found {len(shift_engineers)} engineers on current shift")
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

        return activity_data

    except Exception as e:
        error_msg = str(e)
        hook.error(f"Error checking shift roster: {error_msg}")
        activity_data['data']['shift_roster_result_new_user_acc'] = {
            'status': 'Error',
            'error': error_msg,
            'timestamp': current_time.isoformat(),
            'details': 'Failed to check shift roster'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(f"Error checking shift roster: {error_msg}")

def send_communication_shift_wise_fo_new_user_account(**context) -> Dict[str, Any]:
    """
    Send email notification to shift engineers about new user account creation.

    This function:
    1. Gets worklog_id from XCom
    2. Gets User account data and shift roster details from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification to shift engineers using email hook

    Input (from check_shift_rooster_new_user_account_rs task's activity_data_request_resolve):
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_new_user_account_create': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'shift_roster_result_new_user_acc': {
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

    Output (extends activity_data with shift_communication_result_new_user_acc):
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_new_user_account_create': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'shift_roster_result_new_user_acc': {
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
            'shift_communication_result_new_user_acc': {
                'status': 'Sent',
                'recipients': ['john.smith@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent to 1 shift engineer',
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
        hook.info("Starting email notification to shift engineers for new user account")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.check_shift_rooster_new_user_account_rs', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get only required data from activity_data
    upn = activity_data['data']['generated_adid'].get('upn', '')
    email_password = activity_data['data']['ad_email_password']
    identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    task_type = activity_data['data']['activity_info_fetched']['task_type']
    ad_new_password = activity_data['data']['ad_new_password']
    ad_user_adid = activity_data['data']['generated_adid'].get('user_adid', '')

    # Get shift engineers' email list
    shift_result = activity_data['data'].get('shift_roster_result_new_user_acc', {})
    if shift_result.get('status') != 'Found':
        hook.warning("No shift engineers found to send notification")
        activity_data['data']['shift_communication_result_new_user_acc'] = {
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
        activity_data['data']['shift_communication_result_new_user_acc'] = {
            'status': 'Not Sent',
            'recipients': [],
            'timestamp': datetime.now().isoformat(),
            'details': 'No valid email addresses found in shift roster'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return activity_data

    hook.info(f"Sending new user account notification to {len(recipients)} shift engineers")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook, this template will have the actual password, send this to shft person only and user)
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="shift_engineer_notification_email_template_new_user_account"
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

        # Transform data to match template placeholders - only upn and email_password
        template_data = {
            "upn": upn,
            "email_password": email_password,
            "ad_user_adid": ad_user_adid,
            "ad_new_password": ad_new_password
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | {task_type} | Process Id: {identifier}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        user_email = activity_data['data']['activity_info_fetched']['user_email']

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": recipients,
                "notifier_id": [user_email],
            })
            hook.info(f"Send email notification task initiated to {len(recipients)} recipients")

            # Update activity_data with success information including the result
            activity_data['data']['shift_communication_result_new_user_acc'] = {
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
            activity_data['data']['shift_communication_result_new_user_acc'] = {
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
        activity_data['data']['shift_communication_result_new_user_acc'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
            'details': 'Failed to send email notification'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

def update_wo_comment_new_user_account(**context) -> Dict[str, Any]:
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_new_user_account_create': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'shift_roster_result_new_user_acc': {
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

    Output (extends activity_data with shift_communication_result_new_user_acc):
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_new_user_account_create': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'shift_roster_result_new_user_acc': {
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
            'shift_communication_result_new_user_acc': {
                'status': 'Sent',
                'recipients': ['john.smith@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent to 1 shift engineer',
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_new_user_account_create': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'shift_roster_result_new_user_acc': {
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
            'shift_communication_result_new_user_acc': {
                'status': 'Sent',
                'recipients': ['john.smith@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent to 1 shift engineer',
                'result': {
                    # Result from notify_hook
                }
            },
            'wo_comment_update_new_user_account': {
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_new_user_account_create': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'shift_roster_result_new_user_acc': {
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
            'shift_communication_result_new_user_acc': {
                'status': 'Sent',
                'recipients': ['john.smith@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent to 1 shift engineer',
                'result': {
                    # Result from notify_hook
                }
            },
            'wo_comment_update_new_user_account': {
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
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.send_communication_shift_wise_fo_new_user_account', key='activity_data_request_resolve')
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

    upn = activity_data['data']['request_resolve_initial_data']['upn']
    env = activity_data['data']['activity_info_fetched']['env']
    work_log_comment = f"User AD account has been created in {env} with UPN: {upn}."

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
            activity_data['data']['wo_comment_update_new_user_account'] = {
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
        activity_data['data']['wo_comment_update_new_user_account'] = {
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
        activity_data['data']['wo_comment_update_new_user_account'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def update_wo_status_new_user_account(**context) -> Dict[str, Any]:
    """
    Update work order status after new user account creation.

    This function:
    1. Gets WO number from activity_data
    2. Searches for the work order using the API
    3. Gets the request_id from the response
    4. Updates the work order status and details after user account creation

    Input (from update_wo_comment_new_user_account task's activity_data_request_resolve):
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
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
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
            'activity_update_result_new_user_account': {
                'status': 'Updated',
                'message': 'Activity updated with new user account details',
                'timestamp': '2024-06-07T12:34:56.789'
            },
            'notification_result_new_user_account_create': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent successfully',
                'result': {
                    # Result from notify_hook
                }
            },
            'shift_roster_result_new_user_acc': {
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
            'shift_communication_result_new_user_acc': {
                'status': 'Sent',
                'recipients': ['john.smith@company.com'],
                'timestamp': '2024-06-07T12:34:56.789',
                'details': 'Email notification sent to 1 shift engineer',
                'result': {
                    # Result from notify_hook
                }
            },
            'wo_comment_update_new_user_account': {
                'status': 'Created',
                'work_log_id': 'WL-789012'
            }
        }
    }

    Output (added to activity_data):
    {
        'data': {
            // ... same as input ...
            'wo_result_update_new_user_account': {
                'status': 'Updated',
                'request_id': 'WO66813',
                'wo_number': 'WO123456'
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
        hook.info("Starting work order update for new service account")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.update_wo_comment_new_user_account', key='activity_data_request_resolve')
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
            task_description = work_order.get('description') or "Access Management-Uprising Create Account"

            if not request_id:
                error_msg = "No request ID found in work order response"
                hook.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }

            # Take only the first part if request_id contains '|'
            request_id = request_id.split('|')[0] if '|' in request_id else request_id
            hook.info(f"Found work order with request ID: {request_id}")

            # Get service account details from activity data
            service_details = activity_data['data'].get('request_resolve_initial_data', {})
            upn = service_details.get('upn', '')
            svc_name = activity_data['data']['activity_info_fetched'].get('svc_name', '')
            role = activity_data['data']['activity_info_fetched'].get('role', '')
            user_name = activity_data['data']['activity_info_fetched'].get('user_name', '')
            user_email = activity_data['data']['activity_info_fetched'].get('user_email', '')
            user_adid = activity_data['data']['generated_adid'].get('user_adid', '')
            ref_adid = activity_data['data']['activity_info_fetched'].get('ref_adid', '')  # Using upn as ref_adid since it's the service account identifier

            # Get environment from activity data
            env = activity_data['data']['activity_info_fetched']['env']

            env_val = "Production" if "prod" in env.lower() else "Lab" if "lab" in env.lower() else env

            # Prepare work order update payload based on task description

            if task_description and task_description.lower() == "Access Management-Uprising Create Account".lower():
                update_payload = {
                    "status": "Completed",
                    "status_reason": "Successful",
                    # "assigned_change_account": "Svc Enable Automation",
                    # "change_coordinator": "Svc Enable Automation",
                    "wo_type_field_23": ref_adid,
                    "wo_type_field_28": user_adid,
                    "wo_type_field_29": user_email,
                    "wo_type_field_30": user_name,
                    "environment": env_val
                }
            else:
                update_payload = {
                    "status": "Completed",
                    "status_reason": "Successful",
                    # "assigned_change_account": "Svc Enable Automation",
                    # "change_coordinator": "Svc Enable Automation",
                    "environment": env_val
                }

            # Update work order
            update_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/work-orders/{request_id}?system={ticketing_system}"
            hook.info(f"Task Description: {task_description}")

            logger.info(f"Task Description: {task_description}")

            logger.info(f"Wo update URL: {update_url} Update payload: {update_payload}")
            hook.info(f"Wo update URL: {update_url} Update payload: {update_payload}")

            update_response = client.put(update_url, json=update_payload)

            logger.info(f"Update response: {update_response.json()}")

            update_response.raise_for_status()

            update_data = update_response.json()

            hook.info(f"Updated work order with request ID: {request_id}")

            # Update activity data with work order update result
            activity_data['data']['wo_result_update_new_user_account'] = {
                'status': 'Updated',
                'request_id': request_id,
                'wo_number': wo_number
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return {
                "success": True,
                "request_id": request_id,
                "wo_number": wo_number
            }

    except httpx.HTTPError as e:
        error_msg = f"HTTP error occurred: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['wo_result_update_new_user_account'] = {
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
        activity_data['data']['wo_result_update_new_user_account'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def insert_new_user_data_into_user_list(**context) -> Dict[str, Any]:
    """
    Insert new user data into EXT_AM_ITSM_USER_LIST table after successful user account creation.

    This function:
    1. Gets activity data from update_wo_status_new_user_account task via XCom
    2. Extracts user details from activity data
    3. Inserts user data into EXT_AM_ITSM_USER_LIST table
    4. Updates activity data with insert result

    Input (from update_wo_status_new_user_account task's activity_data_request_resolve):
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
            'generated_adid': {
                'upn': 'newuser@uprising.t-mobile.com',
                'user_adid': 'newuser01',
                'generation_timestamp': '2024-06-07 12:34:56',
                'first_name': 'New',
                'last_name': 'User',
                'full_name': 'New User'
            },
            'activity_info_fetched': {
                'user_name': 'New User',
                'wo_no': 'WO12345',
                'user_email': 'newuser@company.com',
                'user_ntid_signum': 'NUSER123',
                'role': 'Admin',
                'ref_adid': 'refadid01',
                'env': 'production',
                'task_type': 'New User Account'
            },
            'wo_result_update_new_user_account': {
                'status': 'Updated',
                'request_id': 'WO66813',
                'wo_number': 'WO123456'
            }
        }
    }

    Output (extends activity_data with user_list_insert_result):
    {
        'data': {
            // ... same as input ...
            'user_list_insert_result': {
                'status': 'Inserted',
                'message': 'User data inserted successfully into EXT_AM_ITSM_USER_LIST',
                'timestamp': '2024-06-07T12:34:56.789',
                'details': {
                    'user_adid': 'newuser01',
                    'user_name': 'New User',
                    'user_email': 'newuser@company.com',
                    'first_name': 'New',
                    'last_name': 'User',
                    'env': 'production'
                }
            }
        }
    }

    Returns:
        Dict containing success status and insert details:
        {
            'success': True,
            'data': {
                'user_adid': 'newuser01',
                'user_name': 'New User',
                'user_email': 'newuser@company.com',
                'first_name': 'New',
                'last_name': 'User',
                'env': 'production'
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
        hook.info("Starting user data insertion into EXT_AM_ITSM_USER_LIST")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.update_wo_status_new_user_account', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    # Extract user details from activity data
    generated_adid = activity_data['data'].get('generated_adid', {})
    activity_info = activity_data['data'].get('activity_info_fetched', {})

    user_adid = generated_adid.get('user_adid')
    user_name = activity_info.get('user_name')
    user_email = activity_info.get('user_email')
    first_name = generated_adid.get('first_name')
    last_name = generated_adid.get('last_name')
    env = activity_info.get('env')

    # Validate required fields
    if not all([user_adid, user_name, user_email, first_name, env]):
        error_msg = "Missing required user data fields"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    hook.info(f"Inserting user data for user: {user_adid}")

    try:
        # Get database connection
        db_hook = AccessManagementSQLHook(connection_id=os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext"), worklog_id=worklog_id)

        # Prepare insert query
        query = f"""
            INSERT INTO EXT_AM_ITSM_USER_LIST (
                u_login_id,
                u_full_name,
                u_email,
                u_first_name,
                u_last_name,
                u_env
            ) VALUES (
                '{user_adid}',
                '{user_name}',
                '{user_email}',
                '{first_name}',
                '{last_name}',
                '{env}'
            )
        """

        # Execute insert query
        result = db_hook.execute_query(query)
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to insert user data: {error_msg}")
            activity_data['data']['user_list_insert_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat(),
                'details': 'Failed to insert user data into EXT_AM_ITSM_USER_LIST'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return {
                "success": False,
                "error": error_msg
            }

        hook.info(f"Successfully inserted user data for user: {user_adid}")

        # Prepare success response
        insert_details = {
            'user_adid': user_adid,
            'user_name': user_name,
            'user_email': user_email,
            'first_name': first_name,
            'last_name': last_name,
            'env': env
        }

        # Update activity data with insert result
        activity_data['data']['user_list_insert_result'] = {
            'status': 'Inserted',
            'message': 'User data inserted successfully into EXT_AM_ITSM_USER_LIST',
            'timestamp': datetime.now().isoformat(),
            'details': insert_details
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

        return {
            "success": True,
            "data": insert_details
        }

    except Exception as e:
        error_msg = f"Error inserting user data: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['user_list_insert_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
            'details': 'Failed to insert user data into EXT_AM_ITSM_USER_LIST'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

# Alter flow functions : wo_comment_update task fails flow step1
def update_activity_info_wo_comment_new_user_account_err(**context) -> Dict[str, Any]:
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
        activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.update_wo_comment_new_user_account', key='activity_data_request_resolve')
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
        logger.error(f"Error in update_activity_info_wo_comment_new_user_account_err: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

# Alter flow functions : wo_comment_update task fails flow step2    
def send_comm_err_wo_comment_new_user_account_err(**context) -> Dict[str, Any]:
    """
    Send error email notification when work order status update task fails.

    This function:
    1. Gets worklog_id from XCom
    2. Gets work order ID and identifier from activity_data
    3. Prepares error email content using template from mapping
    4. Sends error notification email

    Input (from update_activity_info_wo_status_new_user_account_err task's activity_data_request_resolve):
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
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.update_activity_info_wo_comment_new_user_account_err', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get required data from activity_data
    identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    sr_number = activity_data['data']['request_resolve_initial_data']['sr_number']
    wo_number = activity_data['data']['wo_status_data']['wo_id']
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
            mapping_key="wo_status_error_notification_email_template_new_user_account"
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
            "sr_number": sr_number,
            "error_remarks": error_remarks,
            "task_type": task_type
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | {task_type} | Error in WO Closure | Process Id: {identifier}"
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
def update_activity_info_wo_status_new_user_account_err(**context) -> Dict[str, Any]:
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
        activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.update_wo_status_new_user_account', key='activity_data_request_resolve')
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
        logger.error(f"Error in update_activity_info_wo_status_new_user_account_err: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

# Alter flow functions : wo_status_update task fails flow step2
def send_comm_err_wo_status_new_user_account_err(**context) -> Dict[str, Any]:
    """
    Send error email notification when work order status update task fails.

    This function:
    1. Gets worklog_id from XCom
    2. Gets work order ID and identifier from activity_data
    3. Prepares error email content using template from mapping
    4. Sends error notification email

    Input (from update_activity_info_wo_status_new_user_account_err task's activity_data_request_resolve):
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
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.update_activity_info_wo_status_new_user_account_err', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        raise ValueError("No activity data found from previous task")

    # Get required data from activity_data
    identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    process_id = activity_data['data']['process_id']
    sr_number = activity_data['data']['request_resolve_initial_data']['sr_no']
    wo_number = activity_data['data']['wo_status_data']['wo_id']
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
            mapping_key="wo_status_error_notification_email_template_new_user_account"
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
            "process_id": process_id,
            "wo_number": wo_number,
            "error_remarks": error_remarks,
            "task_type": task_type,
            "sr_number": sr_number,
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | {task_type} | Error in WO Closure | Process Id: {identifier}"
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

# Non finished ad process flow step1
def send_communication_non_finished_ad_new_user_account(**context) -> Dict[str, Any]:
    """
    Send email notification for non-finished AD password reset status.

    This function:
    1. Gets worklog_id from XCom
    2. Gets activity data from ad_process_new_user_account task
    3. Prepares email content using template from mapping
    4. Sends email notification using notify hook

    Input (from ad_process_new_user_account task's activity_data_request_resolve):
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
            'communication_result_non_finished_ad_new_user_account': {
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
        activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.ad_process_new_user_account', key='activity_data_request_resolve')
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
            mapping_key="new_user_account_non_finished_notification_email_template"
        )

        if not mapping_elements:
            hook.error("No mapping element found for new user account non-finished notification template")
            raise ValueError("No mapping element found for new user account non-finished notification template")

        # Get the first matching element and extract its value
        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        process_id = activity_data['data'].get('process_id', '')
        wo_no = activity_info.get('wo_no', '')

        # Extract values from dictionaries
        user_name = activity_info.get('user_name', '')
        user_adid = activity_info.get('user_adid', '')
        user_ntid_signum = activity_info.get('user_ntid_signum', '')
        ad_output = ad_process_result.get('AD_OUTPUT', '')
        ad_python_exception = ad_process_result.get('PYTHON_EXCEPTION', '')
        sr_no = activity_data['data']['request_resolve_initial_data'].get('sr_no', '')

        # Prepare template variables
        template_vars = {
            'user_name': user_name,
            'user_adid': user_adid,
            'user_ntid_signum': user_ntid_signum,
            'ad_output': ad_output,
            'ad_python_exception': ad_python_exception,
            'process_id': process_id,
            'sr_no': sr_no,
            'wo_no': wo_no
        }

        # Render email template
        template = Template(template_content)
        email_content = template.render(**template_vars)
        identifier = activity_data['data']['request_resolve_initial_data']['identifier']

        subject = f"Access Management | User Account Creation | Process Id: {identifier}"
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
            activity_data['data']['communication_result_non_finished_ad_new_user_account'] = {
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
            activity_data['data']['wo_status_communication_result_err'] = {
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
        activity_data['data']['communication_result_non_finished_ad_new_user_account'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': current_time.isoformat(),
            'details': 'Failed to send non-finished status notification'
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(f"Error sending non-finished status notification: {error_msg}")

# Non finished ad process flow step2
def update_activity_info_non_finished_ad_new_user_account(**context) -> Dict[str, Any]:
    """
    Update activity table with error status for non-finished AD password reset.

    This function:
    1. Gets worklog_id from XCom
    2. Gets activity data from send_communication_non_finished_ad_new_user_account task
    3. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with error status and details
    4. Updates activity_data with the update result

    Input (from send_communication_non_finished_ad_new_user_account task's activity_data_request_resolve):
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
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.send_communication_non_finished_ad_new_user_account', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        hook.error("No activity data found from previous task")
        return {
            "success": False,
            "error": "No activity data found from previous task"
        }

    # Get required data from activity_data
    process_id = activity_data['data']['request_resolve_initial_data']['identifier']
    user_adid = activity_data['data']['activity_info_fetched'].get('user_adid', '')
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

# Decryption failed task step1
def update_activity_new_user_account_decryption_failed(**context) -> Dict[str, Any]:
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
    activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.ad_process_new_user_account', key='activity_data_request_resolve')
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
            u_status = 'Error in Process',
            u_remarks = 'Usere account created in AD but password decryption failed'
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

# Decryption failed task step2
def send_communication_new_user_account_decryption_failed(**context) -> Dict[str, Any]:
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
        # update_activity_new_user_account_decryption_failed
        activity_data = ti.xcom_pull(task_ids='new_user_account_task_group.update_activity_new_user_account_decryption_failed', key='activity_data_request_resolve')
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

        env = activity_data['data']['activity_info_fetched']['env']
        upn = activity_data['data']['request_resolve_initial_data']['upn']
        user_adid = activity_data['data']['activity_info_fetched']['user_adid']
        user_ntid_signum = activity_data['data']['activity_info_fetched']['user_ntid_signum']
        ad_output = activity_data['data']['ad_process_result']['AD_OUTPUT']
        ad_python_exception = activity_data['data']['ad_process_result']['PYTHON_EXCEPTION']

        # Transform data to match template placeholders
        template_data = {
            "process_id": process_id,
            "environment": env,
            "user_adid": user_adid,
            "upn": upn,
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | Service Account Creation | Process Id: {process_id}"
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
                "notifier_id": user_email
            })
            hook.info(f"Send decryption failure notification email task initiated to {len(user_email)} recipients")

            # Update activity_data with success information
            activity_data['data']['communication_result_decryption_failed'] = {
                'status': 'Sent',
                'recipients': user_email,
                'timestamp': datetime.now().isoformat(),
                'details': f'Email notification sent to {len(user_email)} recipients',
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

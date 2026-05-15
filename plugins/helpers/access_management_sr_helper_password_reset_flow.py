import logging
from typing import Dict, Any
from hooks.access_management_db_hook import AccessManagementSQLHook
from hooks.ad_process_hook import AD_ProcessHook
from airflow.sdk.bases.hook import BaseHook
from hooks.worklog_hook import WorkLogHook
import uuid
import os
from datetime import datetime
from hooks.tasks_hook import TasksHook
import httpx
from jinja2 import Template
from hooks.mapping_hook import MappingHook
from hooks.notify_hook import NotifyHook

logger = logging.getLogger(__name__)

def fetch_activity_info_user_password_reset(**context) -> Dict[str, Any]:
    """
    Fetch activity information for user password reset from EXT_ACCESS_MANAGEMENT_ACTIVITY table.

    This function:
    1. Gets activity_data from XCom
    2. Queries EXT_ACCESS_MANAGEMENT_ACTIVITY table for user details
    3. Updates activity_data with fetched information
    4. Returns updated activity_data

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'worklog_id': 'WL12345'
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'worklog_id': 'WL12345',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='update_or_create_request_activity', key='activity_data')
    activity_identifier = activity_data['data']['process_id']

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to fetch required fields
    query = f"""
        SELECT
            u_task_type,
            u_user_name,
            u_user_adid,
            u_user_email,
            u_organization,
            u_comments,
            u_env
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_identifier = '{activity_identifier}'
    """

    # Execute query
    result = db_hook.get_row_data(query)

    if not result.get('success') or not result.get('data'):
        logger.error(f"No activity found for process_id: {activity_identifier}")
        raise ValueError(f"No activity found for process_id: {activity_identifier}")

    # Add activity info to existing activity_data
    activity_data['data']['activity_info_fetched'] = {
        'task_type': result['data'][0],
        'user_name': result['data'][1],
        'user_adid': result['data'][2],
        'user_email': result['data'][3],
        'organization': result['data'][4],
        'comments': result['data'][5],
        'env': result['data'][6]
    }

    logger.info(f"Fetched activity info for process_id: {activity_identifier}")

    # Push updated activity_data back to XCom
    ti.xcom_push(key='activity_data', value=activity_data)

    return activity_data

def ad_connect_user_password_reset(**context) -> Dict[str, Any]:
    """
    Connect to AD and execute password reset process.

    This function:
    1. Gets user ADID from activity_data (set by previous task)
    2. Gets worklog_id from XCom (set by create_worklog task)
    3. Prepares and executes AD process using AD_ProcessHook
    4. Updates activity_data with the process result
    5. Stores AD_OUTPUT in ad_output_user_password_reset for branching in next task

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_adid': 'esshar01',
                'user_name': 'John Doe',
                ...
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_adid': 'esshar01',
                'user_name': 'John Doe',
                ...
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is not disabled'
            },
            'ad_output_user_password_reset': 'User Account is not disabled'  # Used for branching in next task
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
        hook.info("Starting AD password reset process")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.fetch_data_activity_info_user_password_reset', key='activity_data')
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    process_id = activity_data['data']['process_id']

    hook.info(f"Processing password reset for user: {user_adid}")

    # Get connection details from Airflow with fallback values
    try:
        # Choose connection based on environment
        if 'prod' not in activity_data['data']['activity_info_fetched']['env'].lower():
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
        fallback_host = '10.253.228.29' if 'prod' not in activity_data['data']['activity_info_fetched']['env'].lower() else '10.159.176.105'
        ssh_host = fallback_host
        ssh_port = 22
        ssh_user = 'm2m_enable_auto@uprising.t-mobile.com'
        ssh_password = 'VBR41w6a5vc88S9tKJ7mFQ4pZLXT2V'

    hook.info("Creating input file for password reset")
    # Create input file command for password reset
    input_command = f'echo {{"inputvalues": [{{"USER_ADID" : "{user_adid}"}}],"action_name" : "RESET_PASSWORD - Check Account Disabled"}} > "C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\User Password Reset\\CheckUserDisable\\checkuserdisable_i_{process_id}.json"'

    # Script command to execute the batch file
    script_cmd = '"C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\User Password Reset\\check_user_disable.bat"'

    # Output file paths
    output_file = f'C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\User Password Reset\\CheckUserDisable\\checkuserdisable_o_{process_id}.json'
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
        # Add the AD process result to activity_data
        activity_data['data']['ad_process_result'] = result['data']
        # Store AD output in new field for next step
        activity_data['data']['ad_output_user_password_reset'] = result['data'].get('AD_OUTPUT')
        ti.xcom_push(key='activity_data', value=activity_data)
    else:
        hook.error(f"Password reset process failed: {result.get('error', 'Unknown error')}")
        # Add the AD process result to activity_data even in error case
        activity_data['data']['ad_process_result'] = result['data']
        # Store AD output in new field for next step
        activity_data['data']['ad_output_user_password_reset'] = result['data'].get('AD_OUTPUT')
        ti.xcom_push(key='activity_data', value=activity_data)

    return result

def sr_creation_password_reset(**context) -> Dict[str, Any]:
    """
    Create service request for password reset.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data from activity_data
    3. Creates service request for password reset
    4. Updates activity_data with SR details

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_adid': 'esshar01',
                'user_name': 'John Doe',
                ...
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is not disabled'
            },
            'ad_output_user_password_reset': 'User Account is not disabled',
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is not disabled'
            },
            'ad_output_user_password_reset': 'User Account is not disabled',
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - esshar01;\nUser Email - john.doe@company.com;\nEnvironment - production;\nComments - Password reset request'
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
        hook.info("Starting SR creation for password reset")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.ad_connect_user_password_reset', key='activity_data')
    process_id = activity_data['data']['process_id']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']

    hook.info(f"Creating SR for password reset for user: {user_adid}")

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to fetch activity data
    query = f"""
        SELECT
            u_user_name,
            u_user_adid,
            u_user_email,
            u_organization,
            u_comments,
            u_mobile_no
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_identifier = '{process_id}'
    """

    # Execute query
    result = db_hook.get_row_data(query)

    if not result.get('success') or not result.get('data'):
        hook.error(f"No activity found for process_id: {process_id}")
        raise ValueError(f"No activity found for process_id: {process_id}")

    # Get the activity data
    activity_row = result['data']
    hook.info(f"Fetched activity data for process_id: {process_id}")

    # Extract fields from activity_row with explicit field names
    u_user_name = activity_row[0]  # u_user_name
    u_user_adid = activity_row[1]  # u_user_adid
    u_user_email = activity_row[2]  # u_user_email
    u_organization = activity_row[3]  # u_organization
    u_comments = activity_row[4]  # u_comments
    u_mobile_no = activity_row[5]  # u_mobile_no

    # Create formatted notes string
    notes = (
        f"User Name - {u_user_name};\n"
        f"User AD Id - {u_user_adid};\n"
        f"User Email - {u_user_email};\n"
        f"Environment - {os.getenv('BUILD_MODE', 'production')};\n"
        f"Comments - {u_comments}"
    )

    ti.xcom_push(key='activity_data', value=activity_data)

    hook.info(f"Added formatted notes to activity data")

    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))
    ticketing_system: str = os.environ.get("DEFAULT_TICKETING_SYSTEM", "REMEDY")

    create_servie_request_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/service-requests?system={ticketing_system}"

    # Prepare the request payload
    sr_name = f"Password Reset Request - {u_user_name} ({u_user_adid}) - {u_organization}"

    payload = {
        "name": sr_name,
        "offering_title": "Access Management - Uprising - Reset Password - User Account",
        "source_keyword": "MYIT",
        "title_instance_id": "SRHAA5V0G9R60AO8U2R8FP5BYUA1PH",
        "full_name": "Svc Enable Automation",
        "login_id": "svc_enable_itsm",
        "sr_type_field_1": u_comments,
        "sr_type_field_2": u_user_adid,
        "sr_type_field_3": "Access Management User Create",
        "sr_type_field_4": u_user_email,
        "sr_type_field_5": u_organization,
        "sr_type_field_11": u_mobile_no,
        "sr_type_field_15": u_user_name
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        with httpx.Client(verify=False, timeout=480.0) as client:
            response = client.post(
                create_servie_request_url,
                json=payload,
                headers=headers
            )
            response.raise_for_status()

            sr_data = response.json()
            hook.info(f"SR creation response: {sr_data}")

            # Validate response structure
            if not isinstance(sr_data, dict):
                hook.error("Invalid response format: expected dictionary")
                raise ValueError("Invalid response format: expected dictionary")

            # Validate required fields
            required_fields = ['request_number', 'status']
            for field in required_fields:
                if field not in sr_data:
                    hook.error(f"Missing required field in response: {field}")
                    raise ValueError(f"Missing required field in response: {field}")

            sr_result = {
                'request_number': sr_data['request_number'],
                'status': sr_data['status']
            }

            hook.info(f"Successfully created SR: {sr_result}")

            # Update activity_data with SR creation result
            activity_data['data']['sr_creation_result'] = sr_result
            activity_data['data']['sr_creation_result']['notes'] = notes
            ti.xcom_push(key='activity_data', value=activity_data)

            return {
                "success": True,
                "data": sr_result,
                "notes": notes
            }

    except httpx.HTTPError as e:
        hook.error(f"HTTP error occurred while creating SR: {str(e)}")

        if notes:
            activity_data['data']['sr_creation_result']['notes'] = notes

        # Update activity_data with error information
        activity_data['data']['sr_creation_result'] = {
            'status': 'Failed',
            'error': str(e),
            'notes': notes
        }

        ti.xcom_push(key='activity_data', value=activity_data)
        raise
    except Exception as e:
        hook.error(f"Error occurred while creating SR: {str(e)}")

        if notes:
            activity_data['data']['sr_creation_result']['notes'] = notes

        # Update activity_data with error information
        activity_data['data']['sr_creation_result'] = {
            'status': 'Failed',
            'error': str(e),
        }

        ti.xcom_push(key='activity_data', value=activity_data)
        raise

def sr_worklog_update_password_reset(**context) -> Dict[str, Any]:
    """
    Update worklog with service request information for password reset.

    This function:
    1. Gets SR ID from activity_data
    2. Searches for the service request using the API
    3. Gets the instance ID from the response
    4. Creates work info using the instance ID
    5. Updates activity_data with work info details

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is not disabled'
            },
            'ad_output_user_password_reset': 'User Account is not disabled',
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - esshar01;\nUser Email - john.doe@company.com;\nEnvironment - production;\nComments - Password reset request'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is not disabled'
            },
            'ad_output_user_password_reset': 'User Account is not disabled',
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - esshar01;\nUser Email - john.doe@company.com;\nEnvironment - production;\nComments - Password reset request'
            },
            'work_info_result': {
                'work_info_id': 'WI1234',
                'status': 'Created'
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
        hook.info("Starting worklog update for password reset")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.sr_creation_password_reset', key='activity_data')
    request_number = activity_data['data']['sr_creation_result']['request_number']

    hook.info(f"Searching for service request with number: {request_number}")

    # Search for service request
    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))
    ticketing_system: str = os.environ.get("DEFAULT_TICKETING_SYSTEM", "REMEDY")
    search_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/service-requests?request_number={request_number}&system={ticketing_system}"

    try:
        with httpx.Client(verify=False, timeout=480.0) as client:
            response = client.get(search_url)
            response.raise_for_status()
            sr_data = response.json()

            if not sr_data or not isinstance(sr_data, list) or len(sr_data) == 0:
                hook.error(f"No service request found with number: {request_number}")
                raise ValueError(f"No service request found with number: {request_number}")

            # Get the first matching service request
            sr = sr_data[0]
            instance_id = sr.get('instance')

            if not instance_id:
                hook.error("No instance ID found in service request response")
                raise ValueError("No instance ID found in service request response")

            hook.info(f"Found service request with instance ID: {instance_id}")

            # Create work info
            work_info_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/service-requests/work-info?system={ticketing_system}"
            work_info_payload = {
                "name": f"Password Reset Work Info {request_number}",
                "notes": activity_data['data']['sr_creation_result']['notes'],
                "summary": "Request Information",
                "work_info_type_selection": "General Information",
                "sr_instance_id": instance_id,
                "service_request_instance_id": instance_id,
                "request_number": request_number,
                "service_request_number": request_number,
                "sr_id": request_number,
                "secure_log": "Yes",
                "view_access": "Public",
                "work_info_type": "General Information",
            }

            work_info_response = client.post(work_info_url, json=work_info_payload)
            work_info_response.raise_for_status()
            work_info_data = work_info_response.json()

            hook.info(f"Created work info with ID: {work_info_data.get('id')}")

            # Update activity_data with work info result
            activity_data['data']['work_info_result'] = {
                'work_info_id': work_info_data.get('id'),
                'status': 'Created'
            }
            ti.xcom_push(key='activity_data', value=activity_data)

            return {
                "success": True,
                "data": work_info_data
            }

    except httpx.HTTPError as e:
        hook.error(f"HTTP error occurred: {str(e)}")
        # Update activity_data with error information
        activity_data['data']['work_info_result'] = {
            'status': 'Failed',
            'error': str(e)
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise
    except Exception as e:
        hook.error(f"Error occurred: {str(e)}")
        # Update activity_data with error information
        activity_data['data']['work_info_result'] = {
            'status': 'Failed',
            'error': str(e)
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise

def sr_creation_activity_update_password_reset(**context) -> Dict[str, Any]:
    """
    Update EXT_ACCESS_MANAGEMENT_ACTIVITY table with SR number and status.

    This function:
    1. Gets worklog_id from XCom
    2. Gets SR number and process_id from activity_data
    3. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with SR number and status

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is not disabled'
            },
            'ad_output_user_password_reset': 'User Account is not disabled',
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - esshar01;\nUser Email - john.doe@company.com;\nEnvironment - production;\nComments - Password reset request'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is not disabled'
            },
            'ad_output_user_password_reset': 'User Account is not disabled',
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - esshar01;\nUser Email - john.doe@company.com;\nEnvironment - production;\nComments - Password reset request'
            },
            'activity_update_result': {
                'status': 'Updated',
                'sr_number': 'REQ1234'
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
        hook.info("Starting activity table update for password reset")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.sr_worklog_update_password_reset', key='activity_data')
    process_id = activity_data['data']['process_id']
    sr_number = activity_data['data']['sr_creation_result']['request_number']

    hook.info(f"Updating activity table for process_id: {process_id} with SR number: {sr_number}")

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to update activity table
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_sr_no = '{sr_number}',
            u_status = 'SR Submitted'
        WHERE u_identifier = '{process_id}'
    """

    try:
        # Execute query
        result = db_hook.execute_query(query)

        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity table: {error_msg}")
            # Update activity_data with error information
            activity_data['data']['activity_update_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            raise ValueError(f"Failed to update activity table: {error_msg}")

        hook.info(f"Successfully updated activity table for process_id: {process_id}")

        # Update activity_data with success information
        activity_data['data']['activity_update_result'] = {
            'status': 'Updated',
            'sr_number': sr_number,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                "status": "Updated",
                "sr_number": sr_number
            }
        }

    except Exception as e:
        error_msg = f"Error updating activity table: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['activity_update_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

def sr_creation_email_notification_password_reset(**context) -> Dict[str, Any]:
    """
    Send email notification for password reset service request creation.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data and SR details from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification using email hook

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is not disabled'
            },
            'ad_output_user_password_reset': 'User Account is not disabled',
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - esshar01;\nUser Email - john.doe@company.com;\nEnvironment - production;\nComments - Password reset request'
            },
            'activity_update_result': {
                'status': 'Updated',
                'sr_number': 'REQ1234'
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
        hook.info("Starting email notification for password reset")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.sr_creation_activity_update_password_reset', key='activity_data')
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    sr_number = activity_data['data']['sr_creation_result']['request_number']
    notes = activity_data['data']['sr_creation_result']['notes']
    task_type = activity_data['data']['activity_info_fetched']['task_type']
    process_id = activity_data['data']['process_id']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']

    hook.info(f"Sending email notification for SR: {sr_number} to user: {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_sr_management",
            mapping_namespace_name="access_management_enable",
            mapping_key="sr_creation_notification_email_template_password_reset"  # EMAIL TEMPLATE MAPPING KEY 1
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for password reset template")

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
            "sr_number": sr_number,
            "user_adid": user_adid,
            "process_id": process_id,
            "notes": notes,
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | {task_type} | Process Id: {process_id}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": email_group,
                "notifier_id": [user_email]
            })
            hook.info("Send email notification task initiated")

            # Update activity_data with success information
            activity_data['data']['email_notification_result'] = {
                'status': 'Sent',
                'recipients': [user_email],
                'sr_number': sr_number,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)

            return {
                "success": True,
                "data": result
            }
        except httpx.HTTPError as e:
            error_msg = f"Failed to send email notification: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['email_notification_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending email notification: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['email_notification_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

def user_inactive_ad_activity_status_update_complete_password_reset(**context) -> Dict[str, Any]:
    """
    Update activity status when user is found to be automatically disabled in AD.

    This function is called in the failure path when:
    1. Initial AD check fails (branch_on_ad_output goes to failure path)
    2. The failure is specifically due to user being "automatically disabled" in AD
    3. This is determined by branch_on_ad_output_ad_failure task

    This function:
    1. Gets activity data from XCom
    2. Updates the activity status in the database to reflect user is inactive
    3. Logs the update in the worklog

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is automatically disabled'
            },
            'ad_output_user_password_reset': 'User Account is automatically disabled'
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is automatically disabled'
            },
            'ad_output_user_password_reset': 'User Account is automatically disabled',
            'inactive_user_status_update': {
                'status': 'Updated',
                'message': 'Activity status updated for inactive user',
                'timestamp': '2024-03-21T17:31:05.948'
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
        hook.info("Starting activity status update for inactive user")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.ad_connect_user_password_reset', key='activity_data')
    process_id = activity_data['data']['process_id']

    hook.info(f"Updating activity status for process_id: {process_id}")

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to update activity table
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_status = 'Completed',
            u_remarks = 'User is Inactive'
        WHERE u_identifier = '{process_id}'
    """

    try:
        # Execute query
        result = db_hook.execute_query(query)

        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity table: {error_msg}")
            # Update activity_data with error information
            activity_data['data']['inactive_user_status_update'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            raise ValueError(f"Failed to update activity table: {error_msg}")

        hook.info(f"Successfully updated activity status for process_id: {process_id}")

        # Update activity_data with success information
        activity_data['data']['inactive_user_status_update'] = {
            'status': 'Updated',
            'message': 'Activity status updated for inactive user',
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                "status": "Updated",
                "message": "Activity status updated for inactive user"
            }
        }

    except Exception as e:
        error_msg = f"Error updating activity table: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['inactive_user_status_update'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

def send_communication_user_inactive_ad_activity_status_update_complete_password_reset(**context) -> Dict[str, Any]:
    """
    Send email notification for inactive user status.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification using email hook

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is automatically disabled'
            },
            'ad_output_user_password_reset': 'User Account is automatically disabled',
            'inactive_user_status_update': {
                'status': 'Updated',
                'message': 'Activity status updated for inactive user',
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is automatically disabled'
            },
            'ad_output_user_password_reset': 'User Account is automatically disabled',
            'inactive_user_status_update': {
                'status': 'Updated',
                'message': 'Activity status updated for inactive user',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'inactive_user_communication_result': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-03-21T17:31:05.948'
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
        hook.info("Starting email notification for inactive user status")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.user_inactive_ad_activity_status_update_complete_password_reset', key='activity_data')
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    organization = activity_data['data']['activity_info_fetched']['organization']
    comments = activity_data['data']['activity_info_fetched']['comments']
    task_type = activity_data['data']['activity_info_fetched']['task_type']
    process_id = activity_data['data']['process_id']

    hook.info(f"Sending inactive user status notification to user: {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_sr_management",
            mapping_namespace_name="access_management_enable",
            mapping_key="inactive_user_notification_email_template_password_reset"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for inactive user notification template")

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
            "user_email": user_email,
            "organization": organization,
            "comments": comments,
            "process_id": process_id,
            "task_type": task_type
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | {task_type} | Process Id: {process_id}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": email_group,
                "notifier_id": user_email
            })
            hook.info("Send inactive user status notification email task initiated")

            # Update activity_data with success information
            activity_data['data']['inactive_user_communication_result'] = {
                'status': 'Sent',
                'recipients': [user_email],
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)

            return {
                "success": True,
                "data": result
            }
        except httpx.HTTPError as e:
            error_msg = f"Failed to send inactive user status notification: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['inactive_user_communication_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending inactive user status notification: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['inactive_user_communication_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

def ad_activity_status_update_process_error_password_reset(**context) -> Dict[str, Any]:
    """
    Update activity status when there is an error in the AD process.

    This function:
    1. Gets worklog_id from XCom
    2. Gets process_id from activity_data
    3. Updates the activity status in the database with an error message
    4. Logs the update in the worklog

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Error in AD process'
            },
            'ad_output_user_password_reset': 'Error in AD process'
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Error in AD process'
            },
            'ad_output_user_password_reset': 'Error in AD process',
            'ad_process_error_status_update': {
                'status': 'Updated',
                'message': 'Activity status updated for AD process error',
                'timestamp': '2024-03-21T17:31:05.948'
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
        hook.info("Starting activity status update for AD process error")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.ad_connect_user_password_reset', key='activity_data')
    process_id = activity_data['data']['process_id']
    action = activity_data['data'].get('action', 'Password Reset')

    hook.info(f"Updating activity status for process_id: {process_id}")

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to update activity table
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_status = 'Error in check AD process',
            u_remarks = 'Transfer to FO team. ITSM error in SR submission. Validation successful for request ({action})'
        WHERE u_identifier = '{process_id}'
    """

    try:
        # Execute query
        result = db_hook.execute_query(query)

        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity table: {error_msg}")
            # Update activity_data with error information
            activity_data['data']['ad_process_error_status_update'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            raise ValueError(f"Failed to update activity table: {error_msg}")

        hook.info(f"Successfully updated activity status for process_id: {process_id}")

        # Update activity_data with success information
        activity_data['data']['ad_process_error_status_update'] = {
            'status': 'Updated',
            'message': 'Activity status updated for AD process error',
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                "status": "Updated",
                "message": "Activity status updated for AD process error"
            }
        }

    except Exception as e:
        error_msg = f"Error updating activity table: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['ad_process_error_status_update'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

def send_email_ad_activity_status_update_process_error_password_reset(**context) -> Dict[str, Any]:
    """
    Send email notification about AD activity status update error.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification using email hook

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Error in AD process'
            },
            'ad_output_user_password_reset': 'Error in AD process',
            'ad_process_error_status_update': {
                'status': 'Updated',
                'message': 'Activity status updated for AD process error',
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'Error in AD process'
            },
            'ad_output_user_password_reset': 'Error in AD process',
            'ad_process_error_status_update': {
                'status': 'Updated',
                'message': 'Activity status updated for AD process error',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'error_notification_result': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-03-21T17:31:05.948'
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
        hook.info("Starting email notification for AD process error")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.ad_activity_status_update_process_error_password_reset', key='activity_data')
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    organization = activity_data['data']['activity_info_fetched']['organization']
    comments = activity_data['data']['activity_info_fetched']['comments']
    ad_output = activity_data['data']['ad_output_user_password_reset']
    python_status = activity_data['data']['ad_process_result']['PYTHON_STATUS']
    process_id = activity_data['data']['process_id']
    task_type = activity_data['data']['activity_info_fetched']['task_type']

    hook.info(f"Sending error notification to user: {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_sr_management",
            mapping_namespace_name="access_management_enable",
            mapping_key="ad_process_error_notification_email_template_password_reset"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for AD process error notification template")

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
            "organization": organization,
            "comments": comments,
            "ad_output": ad_output,
            "python_status": python_status,
            "process_id": process_id,
            "task_type": task_type
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | {task_type} | Process Id: {process_id}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": email_group,
                "notifier_id": user_email
            })
            hook.info("Send error notification email task initiated")

            # Update activity_data with success information
            activity_data['data']['error_notification_result'] = {
                'status': 'Sent',
                'recipients': [user_email],
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)

            return {
                "success": True,
                "data": result
            }
        except httpx.HTTPError as e:
            error_msg = f"Failed to send error notification: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['error_notification_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending error notification: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['error_notification_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

def sr_creation_failed_activity_update_user_password_reset(**context) -> Dict[str, Any]:
    """
    Update activity status when SR creation fails for user password reset.

    This function:
    1. Gets worklog_id from XCom
    2. Gets SR creation result from activity_data
    3. Updates activity status in database to reflect SR creation failure
    4. Returns updated activity_data

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'sr_creation_result': {
                'status': 'Failed',
                'message': 'Service request creation failed',
                'error': 'Connection timeout'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'sr_creation_result': {
                'status': 'Failed',
                'message': 'Service request creation failed',
                'error': 'Connection timeout'
            },
            'sr_creation_failed_activity_update': {
                'status': 'Updated',
                'message': 'Activity status updated for SR creation failure',
                'timestamp': '2024-03-21T17:31:05.948'
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
        hook.info("Starting activity status update for SR creation failure")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.sr_creation_password_reset', key='activity_data')
    process_id = activity_data['data']['process_id']
    error_msg = activity_data['data']['sr_creation_result'].get('error', 'Unknown error')

    hook.info(f"Updating activity status for SR creation failure - Process ID: {process_id}")

    try:
        # Get database connection
        oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
        db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

        # Query to update activity table
        query = f"""
            UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
            SET u_status = 'Error in ITSM Process',
                u_remarks = 'Transfer to FO team. ITSM error in SR submission. Validation successful for request (User Password Reset)'
            WHERE u_identifier = '{process_id}'
        """

        result = db_hook.execute_query(query)

        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity table: {error_msg}")
            # Update activity_data with error information
            activity_data['data']['sr_creation_failed_activity_update'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            raise ValueError(f"Failed to update activity table: {error_msg}")

        hook.info(f"Successfully updated activity status for process_id: {process_id}")

        # Update activity_data with success information
        activity_data['data']['sr_creation_failed_activity_update'] = {
            'status': 'Failed',
            'error': 'SR Creation Failed',
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                "process_id": process_id,
                "status": "Error in ITSM Process",
                "message": "Transfer to FO team. ITSM error in SR submission. Validation successful for request (User Password Reset)",
                "timestamp": datetime.now().isoformat()
            }
        }

    except Exception as e:
        error_msg = f"Error updating activity status for SR creation failure: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['sr_creation_failed_status_update'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

def sr_creation_failed_email_notification_user_password_reset(**context) -> Dict[str, Any]:
    """
    Send email notification when SR creation fails for user password reset.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data and SR creation result from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification using email hook

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'sr_creation_result': {
                'status': 'Failed',
                'message': 'Service request creation failed',
                'error_details': 'Connection timeout'
            },
            'sr_creation_failed_status_update': {
                'status': 'Updated',
                'message': 'Activity status updated for SR creation failure',
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Password Reset',
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'sr_creation_result': {
                'status': 'Failed',
                'message': 'Service request creation failed',
                'error_details': 'Connection timeout'
            },
            'sr_creation_failed_status_update': {
                'status': 'Updated',
                'message': 'Activity status updated for SR creation failure',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'sr_creation_failed_notification_result': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'timestamp': '2024-03-21T17:31:05.948'
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
        hook.info("Starting email notification for SR creation failure")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_password_reset_group.sr_creation_failed_activity_update_user_password_reset', key='activity_data')
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    user_name = activity_data['data']['activity_info_fetched']['user_name']

    sr_creation_result = activity_data['data']['sr_creation_result']

    # Initialize error and status variables
    error = None
    status = None

    # Extract error and status if sr_creation_result is available
    if sr_creation_result:
        error = sr_creation_result.get('error')
        status = sr_creation_result.get('status')

    process_id = activity_data['data']['process_id']
    task_type = activity_data['data']['activity_info_fetched']['task_type']

    hook.info(f"Sending SR creation failure notification to user: {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_sr_management",
            mapping_namespace_name="access_management_enable",
            mapping_key="sr_creation_failed_email_template_common"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for SR creation failure notification template")

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
            "process_id": process_id,
            "task_type": task_type,
            "error_msg": error,
            "status": status
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | {task_type} | Process Id: {process_id}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": email_group,
                "notifier_id": user_email
            })
            hook.info("Send SR creation failure notification email task initiated")

            # Update activity_data with success information
            activity_data['data']['sr_creation_failed_notification_result'] = {
                'status': 'Sent',
                'recipients': [user_email],
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)

            return {
                "success": True,
                "data": result
            }
        except httpx.HTTPError as e:
            error_msg = f"Failed to send SR creation failure notification: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['sr_creation_failed_notification_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending SR creation failure notification: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['sr_creation_failed_notification_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

from hooks.worklog_hook import WorkLogHook
from hooks.access_management_db_hook import AccessManagementSQLHook
from typing import Dict, Any
import logging
import os
from airflow.hooks.base import BaseHook
from hooks.ad_process_hook import AD_ProcessHook
import httpx
from jinja2 import Template
from datetime import datetime
from hooks.mapping_hook import MappingHook
from hooks.notify_hook import NotifyHook
import re

logger = logging.getLogger(__name__)

def fetch_data_activity_info_user_account_modify(**context) -> Dict[str, Any]:
    """
    Fetch activity information for user account modification from EXT_ACCESS_MANAGEMENT_ACTIVITY table.

    This function:
    1. Gets activity_data from XCom
    2. Queries EXT_ACCESS_MANAGEMENT_ACTIVITY table for request details
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
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='update_or_create_request_activity', key='activity_data')
    activity_identifier = activity_data['data']['process_id']

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
            u_user_adid,
            u_user_name,
            u_user_email,
            u_manager_name,
            u_manager_email,
            u_env,
            u_description,
            u_organization,
            u_platform,
            u_business_justification,
            u_modification_action,
            u_role
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_identifier = '{activity_identifier}'
    """

    # Execute query
    result = db_hook.get_row_data(query)

    if not result.get('success') or not result.get('data'):
        hook.error(f"No activity found for process_id: {activity_identifier}")
        raise ValueError(f"No activity found for process_id: {activity_identifier}")

    # Add activity info to existing activity_data
    activity_data['data']['activity_info_fetched'] = {
        'task_type': result['data'][0],      # u_task_type
        'user_adid': result['data'][1],      # u_user_adid
        'user_name': result['data'][2],      # u_user_name
        'user_email': result['data'][3],     # u_user_email
        'manager_name': result['data'][4],   # u_manager_name
        'manager_email': result['data'][5],  # u_manager_email
        'env': result['data'][6],           # u_env
        'description': result['data'][7],    # u_description
        'organization': result['data'][8],   # u_organization
        'platform': result['data'][9],       # u_platform
        'business_justification': result['data'][10],  # u_business_justification
        'modification_action': result['data'][11],     # u_modification_action
        'role': result['data'][12]           # u_role
    }

    hook.info(f"Fetched activity info for process_id: {activity_identifier}")

    # Push updated activity_data back to XCom
    ti.xcom_push(key='activity_data', value=activity_data)

    return activity_data

def ad_connect_user_account_modification(**context) -> Dict[str, Any]:
    """
    Connect to AD and execute user account modification process.

    This function:
    1. Gets user account details from activity_data
    2. Gets worklog_id from XCom
    3. Prepares and executes AD process using AD_ProcessHook
    4. Updates activity_data with the process result

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'esshar01',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change required',
                'modification_action': 'Update Role',
                'role': 'Developer'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'esshar01',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change required',
                'modification_action': 'Update Role',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account Modified Successfully',
                'ad_output_user_account_modify': 'User Account Modified Successfully'
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
        hook.info("Starting AD user account modification process")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.fetch_data_activity_info_user_account_modify', key='activity_data')

    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    process_id = activity_data['data']['process_id']
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    role = activity_data['data']['activity_info_fetched']['role']
    modification_action = activity_data['data']['activity_info_fetched']['modification_action']

    hook.info(f"Processing user account modification for user: {user_adid}")

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

    hook.info("Creating input file for user account modification")

    # Create input file command for user account modification
    input_command = f'echo {{"inputvalues": [{{"USER_ADID" : "{user_adid}","USER_ROLE": "{role}","USER_ACTION": "{modification_action}"}}]}} > "C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\Modify User Account\\CheckUserAccRoles\\checkuserroles_i_{process_id}.json"'
    # echo {"inputvalues": [{"USER_ADID" : "$FLOW{USER_ADID}","USER_ROLE": "$FLOW{ROLE}","USER_ACTION": "$FLOW{MODIFICATN_ACTION}"}]}  >  "C:\Users\m2m_enable_auto\Documents\Access Management Automation\Modify User Account\CheckUserAccRoles\checkuserroles_i_$PARAM{IDENTIFIER}.json"

    # Script command to execute the batch file
    script_cmd = '"C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\Modify User Account\\check_useracc_roles.bat"'
    # C:\Users\m2m_enable_auto\Documents\Access Management Automation\Modify User Account\check_useracc_roles.bat

    # Output file paths
    output_file = f'C:\\Users\\m2m_enable_auto\\Documents\\Access Management Automation\\Modify User Account\\CheckUserAccRoles\\checkuserroles_o_{process_id}.json'

    # path C:\Users\m2m_enable_auto\Documents\Access Management Automation\Modify User Account\CheckUserAccRoles  file: checkuserroles_o_$PARAM{IDENTIFIER}.json

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
        hook.info("User account modification process completed successfully")
        # Add the AD process result to activity_data
        activity_data['data']['ad_process_result'] = result['data']
        activity_data['data']['ad_output_user_account_modify'] = result['data']['AD_OUTPUT']
        ti.xcom_push(key='activity_data', value=activity_data)
    else:
        hook.error(f"User account modification process failed: {result.get('error', 'Unknown error')}")
        # Add the AD process result to activity_data even in error case
        activity_data['data']['ad_process_result'] = result['data']
        activity_data['data']['ad_output_user_account_modify'] = result['data']['AD_OUTPUT']
        ti.xcom_push(key='activity_data', value=activity_data)

    return result

def sr_creation_user_account_modify(**context) -> Dict[str, Any]:
    """
    Create service request for user account modification.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data from activity_data
    3. Creates service request for user account modification
    4. Updates activity_data with SR details

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer',
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account Modified Successfully',
                'ad_output_user_account_modify': 'User Account Modified Successfully'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account Modified Successfully',
                'ad_output_user_account_modify': 'User Account Modified Successfully'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - JDOE123;\nUser Email - john.doe@company.com;\nManager Name - Jane Smith;\nManager Email - jane.smith@company.com;\nEnvironment - Production;\nOrganization - IT;\nPlatform - Windows;\nBusiness Justification - Role change request;\nModification Action - Add Access;\nRole - Developer'
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
        hook.info("Starting SR creation for user account modification")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.ad_connect_user_account_modification', key='activity_data')
    process_id = activity_data['data']['process_id']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    task_type = activity_data['data']['activity_info_fetched']['task_type']

    hook.info(f"Creating SR for {task_type} for user: {user_adid}")

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Fetch activity data
    query = f"""
        SELECT
            u_business_justification,
            u_user_adid,
            u_user_name,
            u_manager_adid,
            u_modification_action,
            u_manager_name,
            u_manager_email,
            u_user_email,
            u_env,
            u_description,
            u_platform,
            u_organization,
            u_mobile_no
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_identifier = '{process_id}'
    """
    result = db_hook.get_row_data(query)
    if not result.get('success') or not result.get('data'):
        hook.error(f"No activity found for process_id: {process_id}")
        raise ValueError(f"No activity found for process_id: {process_id}")

    activity_row = result['data']
    hook.info(f"Fetched activity data for process_id: {process_id}")

    # Extract fields
    u_business_justification = activity_row[0]
    u_user_adid = activity_row[1]
    u_user_name = activity_row[2]
    u_manager_adid = activity_row[3]
    u_modification_action = activity_row[4]
    u_manager_name = activity_row[5]
    u_manager_email = activity_row[6]
    u_user_email = activity_row[7]
    u_description = activity_row[9]
    u_env = activity_row[8]
    u_platform = activity_row[10]
    u_organization = activity_row[11]
    u_mobile_no = activity_row[12]

    # Fetch approver using organization from activity data
    approver = None
    approver_query = f"""
        SELECT u_approver_list
        FROM EXT_AM_ITSM_APPROVER_ROLE
        WHERE u_company = '{u_organization}'
        AND u_platform = '{u_platform}'
    """
    approver_result = db_hook.get_row_data(approver_query)
    if approver_result.get('success') and approver_result.get('data'):
        approver = approver_result['data'][0]  # Get first column from tuple
        hook.info(f"Fetched approver: {approver}")
    else:
        hook.warning("No approver found, using empty string")

    # Create notes
    notes = (
        f"User Name - {u_user_name};\n"
        f"User AD Id - {u_user_adid};\n"
        f"User Email - {u_user_email};\n"
        f"Manager Name - {u_manager_name};\n"
        f"Manager AD Id - {u_manager_adid};\n"
        f"Manager Email - {u_manager_email};\n"
        f"Description - {u_description};\n"
        f"Environment - {u_env};\n"
        f"Organization - {u_organization};\n"
        f"Platform - {u_platform};\n"
        f"Business Justification - {u_business_justification};\n"
        f"Modification Action - {u_modification_action};\n"
        f"Mobile Number - {u_mobile_no};\n"
        f"Build Mode - {os.getenv('BUILD_MODE', 'production')}"
    )

    sr_name = f"User Account Modification - {u_user_name} ({u_user_adid}) - {u_organization}"

    # Create payload
    payload = {
        "name": sr_name,
        "offering_title": "Access Management - Uprising - Modify Account/Modify Access",
        "source_keyword": "MYIT",
        "title_instance_id": "SRHAA5V0G9R60AOHC2PJLKB0SDWH9O",
        "full_name": "Svc Enable Automation",
        "login_id": "svc_enable_itsm",
        "sr_type_field_1": u_business_justification,
        "sr_type_field_2": u_user_adid,
        "sr_type_field_3": "Access Management User Create",
        "sr_type_field_4": u_user_name,
        "sr_type_field_5": u_manager_adid.lower(),
        "sr_type_field_10": approver.lower() if approver else "",
        "sr_type_field_11": u_modification_action,
        "sr_type_field_12": u_manager_name,
        "sr_type_field_14": u_manager_email,
        "sr_type_field_19": u_user_email,
        "sr_type_field_20": u_env,
        "sr_type_field_21": u_platform,
        "sr_type_field_22": u_description[:80] if len(u_description) > 80 else u_description,
        "sr_type_field_42": u_mobile_no,
        "sr_type_field_43": "Add"
    }

    ti.xcom_push(key='activity_data', value=activity_data)

    hook.info(f"Added formatted notes to activity data")

    # Common API calling code
    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))
    ticketing_system: str = os.environ.get("DEFAULT_TICKETING_SYSTEM", "REMEDY")

    create_servie_request_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/service-requests?system={ticketing_system}"

    logger.info(f"Create Service Request for pauload: {payload} and url: {create_servie_request_url}")

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
                activity_data['data']['sr_creation_result'] = {
                    'status': 'Failed',
                    'error': 'Invalid response format: expected dictionary'
                }

                if notes:  # add notes in every case where an activity data is to be pushed
                    activity_data['data']['sr_creation_result']['notes'] = notes

                ti.xcom_push(key='activity_data', value=activity_data)
                return {
                    "success": False,
                    "data": activity_data['data']['sr_creation_result']
                }

            # Validate required fields
            required_fields = ['request_number', 'status']
            missing_fields = [field for field in required_fields if field not in sr_data]
            if missing_fields:
                hook.error(f"Missing required fields in response: {missing_fields}")
                activity_data['data']['sr_creation_result'] = {
                    'status': 'Failed',
                    'error': f"Missing required fields: {missing_fields}"
                }

                if notes:  # add notes in every case where an activity data is to be pushed
                    activity_data['data']['sr_creation_result']['notes'] = notes

                ti.xcom_push(key='activity_data', value=activity_data)
                return {
                    "success": False,
                    "data": activity_data['data']['sr_creation_result']
                }

            sr_result = {
                'request_number': sr_data['request_number'],
                'status': sr_data['status']
            }

            hook.info(f"Successfully created SR: {sr_result}")

            # Update activity_data with SR creation result
            activity_data['data']['sr_creation_result'] = sr_result

            if notes:  # add notes in every case where an activity data is to be pushed
                activity_data['data']['sr_creation_result']['notes'] = notes

            ti.xcom_push(key='activity_data', value=activity_data)

            return {
                "success": True,
                "data": sr_result
            }

    except httpx.HTTPError as e:
        hook.error(f"HTTP error occurred while creating SR: {str(e)}")
        # Update activity_data with error information
        activity_data['data']['sr_creation_result'] = {
            'status': 'Failed',
            'error': str(e)
        }

        if notes:  # add notes in every case where an activity data is to be pushed
            activity_data['data']['sr_creation_result']['notes'] = notes

        ti.xcom_push(key='activity_data', value=activity_data)
        return {
            "success": False,
            "data": activity_data['data']['sr_creation_result']
        }
    except Exception as e:
        hook.error(f"Error occurred while creating SR: {str(e)}")
        # Update activity_data with error information
        activity_data['data']['sr_creation_result'] = {
            'status': 'Failed',
            'error': str(e)
        }

        if notes:  # add notes in every case where an activity data is to be pushed
            activity_data['data']['sr_creation_result']['notes'] = notes

        ti.xcom_push(key='activity_data', value=activity_data)
        return {
            "success": False,
            "data": activity_data['data']['sr_creation_result']
        }

def sr_worklog_update_user_account_modify(**context) -> Dict[str, Any]:
    """
    Update worklog with service request information for user account modification.

    This function:
    1. Gets SR ID from activity_data
    2. Searches for the service request using the API
    3. Gets the instance ID from the response
    4. Creates work info using the instance ID
    5. Updates activity_data with work info details
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
        hook.info("Starting worklog update for user account modification")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.sr_creation_user_account_modify', key='activity_data')
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
                error_msg = f"No service request found with number: {request_number}"
                hook.error(error_msg)
                activity_data['data']['work_info_result'] = {
                    'status': 'Failed',
                    'error': error_msg
                }
                ti.xcom_push(key='activity_data', value=activity_data)
                return {
                    "success": False,
                    "error": error_msg
                }

            # Get the first matching service request
            sr = sr_data[0]
            instance_id = sr.get('instance')

            if not instance_id:
                error_msg = "No instance ID found in service request response"
                hook.error(error_msg)
                activity_data['data']['work_info_result'] = {
                    'status': 'Failed',
                    'error': error_msg
                }
                ti.xcom_push(key='activity_data', value=activity_data)
                return {
                    "success": False,
                    "error": error_msg
                }

            hook.info(f"Found service request with instance ID: {instance_id}")

            # Create work info
            work_info_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/service-requests/work-info?system={ticketing_system}"
            work_info_payload = {
                "name": f"User Account Modification Work Info {request_number}",
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
        error_msg = f"HTTP error occurred: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['work_info_result'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }
    except Exception as e:
        error_msg = f"Error occurred: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['work_info_result'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def sr_creation_activity_update_user_account_modify(**context) -> Dict[str, Any]:
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
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - JDOE123;\nUser Email - john.doe@company.com;\nManager Name - Jane Smith;\nManager Email - jane.smith@company.com;\nEnvironment - Production;\nOrganization - IT;\nPlatform - Windows;\nBusiness Justification - Role change request'
            },
            'work_info_result': {
                'work_info_id': 'WI1234',
                'status': 'Created'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - JDOE123;\nUser Email - john.doe@company.com;\nManager Name - Jane Smith;\nManager Email - jane.smith@company.com;\nEnvironment - Production;\nOrganization - IT;\nPlatform - Windows;\nBusiness Justification - Role change request'
            },
            'work_info_result': {
                'work_info_id': 'WI1234',
                'status': 'Created'
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
        hook.info("Starting activity update for user account modification")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.sr_worklog_update_user_account_modify', key='activity_data')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    process_id = activity_data['data']['process_id']
    sr_number = activity_data['data']['sr_creation_result']['request_number']

    hook.info(f"Updating activity for process_id: {process_id} with SR number: {sr_number}")

    # Get connection id from environment variable
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Update activity data
    update_query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_sr_no = '{sr_number}',
            u_status = 'SR Submitted'
        WHERE u_identifier = '{process_id}'
    """

    try:
        result = db_hook.execute_query(update_query)

        if not result.get('success'):
            error_msg = f"Failed to update activity data: {result.get('error', 'Unknown error')}"
            hook.error(error_msg)
            activity_data['data']['activity_update_result'] = {
                'status': 'Failed',
                'error': error_msg
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            return {
                "success": False,
                "error": error_msg
            }

        hook.info(f"Successfully updated activity data for process_id: {process_id}")

        # Update activity_data with update result
        activity_data['data']['activity_update_result'] = {
            'status': 'Updated',
            'sr_number': sr_number
        }
        ti.xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                'status': 'Updated',
                'sr_number': sr_number
            }
        }

    except Exception as e:
        error_msg = f"Error updating activity data: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['activity_update_result'] = {
            'status': 'Failed',
            'error': error_msg
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def fetch_itsm_approver_user_account_modify(**context) -> Dict[str, Any]:
    """
    Fetch ITSM approver information for user account modification.

    This function:
    1. Gets worklog_id from XCom
    2. Gets activity data from XCom (which includes SR creation and activity update results)
    3. Queries EXT_AM_ITSM_APPROVER_ROLE table for approver details
    4. Queries EXT_AM_ITSM_USER_LIST table for approver emails
    5. Updates activity_data with approver information

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            ...
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            ...
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],  # List of names
                'approver_emails': ['approver1@company.com', 'approver2@company.com'],  # List of emails
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
        hook.info("Starting ITSM approver fetch for user account modification")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.sr_creation_activity_update_user_account_modify', key='activity_data')
    if not activity_data or 'data' not in activity_data or 'activity_info_fetched' not in activity_data['data']:
        error_msg = "No activity data found"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    # Extract required fields from activity data
    organization = activity_data['data']['activity_info_fetched']['organization']
    platform = activity_data['data']['activity_info_fetched']['platform']
    description = activity_data['data']['activity_info_fetched']['description']

    hook.info(f"Fetching approver for organization: {organization}, platform: {platform}")

    # Get connection id from environment variable
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query EXT_AM_ITSM_APPROVER_ROLE table
    approver_query = f"""
        SELECT u_approver_list, u_approver_name
        FROM EXT_AM_ITSM_APPROVER_ROLE
        WHERE u_company = '{organization}'
        AND (u_role_des = '{description}' OR u_platform = '{platform}')
    """

    approver_result = db_hook.get_row_data(approver_query)
    if not approver_result.get('success') or not approver_result.get('data'):
        hook.warning(f"No approver found for organization: {organization}, platform: {platform}")
        activity_data['data']['approver_info'] = {
            'approver_names': [],
            'approver_emails': [],
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        return {
            "success": True,
            "data": activity_data['data']['approver_info']
        }

    # Extract approver list and names
    approver_list_str = approver_result['data'][0]
    approver_names_str = approver_result['data'][1]

    # Handle both semicolon and comma delimiters for approver list
    if ';' in approver_list_str:
        approver_list = approver_list_str.split(';')
    else:
        approver_list = approver_list_str.split(',')
    approver_list = [adid.strip() for adid in approver_list if adid.strip()]

    # Handle both semicolon and comma delimiters for approver names
    if ';' in approver_names_str:
        approver_names = approver_names_str.split(';')
    else:
        approver_names = approver_names_str.split(',')
    approver_names = [name.strip() for name in approver_names if name.strip()]

    hook.info(f"Found approver list: {approver_list}")
    hook.info(f"Found approver names: {approver_names}")

    # Query EXT_AM_ITSM_USER_LIST table for approver emails
    approver_emails = []
    for adid in approver_list:
        if not adid.strip():
            continue
        email_query = f"""
            SELECT DISTINCT u_email
            FROM EXT_AM_ITSM_USER_LIST
            WHERE u_login_id = '{adid.strip()}'
        """
        email_result = db_hook.get_row_data(email_query)
        if email_result.get('success') and email_result.get('data'):
            approver_emails.append(email_result['data'][0])

    hook.info(f"Found approver emails: {approver_emails}")

    # Update activity_data with approver information
    activity_data['data']['approver_info'] = {
        'approver_names': approver_names,  # List of names
        'approver_emails': approver_emails,  # List of emails
        'timestamp': datetime.now().isoformat()
    }
    ti.xcom_push(key='activity_data', value=activity_data)

    return {
        "success": True,
        "data": activity_data['data']['approver_info']
    }

def send_communication_sr_creation_user_account_modify(**context) -> Dict[str, Any]:
    """
    Send communication for service request creation for user account modification.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data and SR details from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification using email hook

    Example input (from activity_data):
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - JDOE123;\nUser Email - john.doe@company.com;\nManager Name - Jane Smith;\nManager Email - jane.smith@company.com;\nEnvironment - Production;\nOrganization - IT;\nPlatform - Windows;\nBusiness Justification - Role change request'
            },
            'work_info_result': {
                'work_info_id': 'WI1234',
                'status': 'Created'
            },
            'activity_update_result': {
                'status': 'Updated',
                'sr_number': 'REQ1234'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com'],
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - JDOE123;\nUser Email - john.doe@company.com;\nManager Name - Jane Smith;\nManager Email - jane.smith@company.com;\nEnvironment - Production;\nOrganization - IT;\nPlatform - Windows;\nBusiness Justification - Role change request'
            },
            'work_info_result': {
                'work_info_id': 'WI1234',
                'status': 'Created'
            },
            'activity_update_result': {
                'status': 'Updated',
                'sr_number': 'REQ1234'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com'],
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'email_notification_result': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'sr_number': 'REQ1234',
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
        hook.info("Starting email notification for service request creation")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.fetch_itsm_approver_user_account_modify', key='activity_data')
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    sr_number = activity_data['data']['sr_creation_result']['request_number']
    env = activity_data['data']['activity_info_fetched']['env']
    description = activity_data['data']['activity_info_fetched']['description']
    organization = activity_data['data']['activity_info_fetched']['organization']
    platform = activity_data['data']['activity_info_fetched']['platform']
    business_justification = activity_data['data']['activity_info_fetched']['business_justification']
    modification_action = activity_data['data']['activity_info_fetched']['modification_action']
    role = activity_data['data']['activity_info_fetched']['role']
    process_id = activity_data['data']['process_id']
    task_type = activity_data['data']['activity_info_fetched']['task_type']
    manager_email = activity_data['data']['activity_info_fetched']['manager_email']
    manager_name = activity_data['data']['activity_info_fetched']['manager_name']
    approver_emails = activity_data['data']['approver_info'].get('approver_emails', [])
    approver_names = activity_data['data']['approver_info'].get('approver_names', [])

    hook.info(f"Sending service request creation notification to user: {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook sr_creation_email_template_user_account_modify
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_sr_management",
            mapping_namespace_name="access_management_enable",
            mapping_key="sr_creation_email_template_user_account_modify"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for service request creation template")

        # Get the first matching element and extract its value
        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Extract all fields with .get(..., '') to avoid KeyError or NoneType error
        activity_info = activity_data['data'].get('activity_info_fetched', {})
        sr_result = activity_data['data'].get('sr_creation_result', {})
        approver_info = activity_data['data'].get('approver_info', {})

        user_email = activity_info.get('user_email', '')
        user_name = activity_info.get('user_name', '')
        user_adid = activity_info.get('user_adid', '')
        sr_number = sr_result.get('request_number', '')
        env = activity_info.get('env', '')
        description = activity_info.get('description', '')
        organization = activity_info.get('organization', '')
        platform = activity_info.get('platform', '')
        business_justification = activity_info.get('business_justification', '')
        modification_action = activity_info.get('modification_action', '')
        role = activity_info.get('role', '')
        process_id = activity_data['data'].get('process_id', '')
        task_type = activity_info.get('task_type', '')
        manager_email = activity_info.get('manager_email', '')
        manager_name = activity_info.get('manager_name', '')
        approver_names = approver_info.get('approver_names', [])
        approver_emails = approver_info.get('approver_emails', [])
        approver_names_for_email = ', '.join(approver_names) if approver_names else ''

        # Get email group with null check
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")

        # Construct cc_notifier_email_list with null checks and deduplication
        cc_notifier_email_list = [email_group]
        if manager_email and manager_email not in cc_notifier_email_list:
            cc_notifier_email_list.append(manager_email)
        for email in approver_emails:
            if email and email not in cc_notifier_email_list:
                cc_notifier_email_list.append(email)

        # Transform data to match template placeholders
        template_data = {
            "user_name": user_name,
            "user_email": user_email,
            "user_adid": user_adid,
            "sr_number": sr_number,
            "process_id": process_id,
            "task_type": task_type,
            "env": env,
            "description": description,
            "organization": organization,
            "platform": platform,
            "business_justification": business_justification,
            "modification_action": modification_action,
            "role": role,
            "manager_name": manager_name,
            "manager_email": manager_email,
            "approver": approver_names_for_email
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management | User Account Modification | Process Id: {process_id}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": cc_notifier_email_list,
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

def sr_activity_status_update_existing_user_add_user_account_modify(**context) -> Dict[str, Any]:
    """
    Update activity status when user already exists during user account modification.

    This function:
    1. Gets worklog_id from XCom
    2. Gets process_id from activity_data
    3. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with status and remarks
    4. Updates activity_data with the update result

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished with error',
                'AD_OUTPUT': 'intended user_already_exist'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished with error',
                'AD_OUTPUT': 'intended user_already_exist'
            },
            'activity_update_result_existing_user': {
                'status': 'Completed',
                'remarks': 'The requested role is already present.',
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
        hook.info("Starting activity status update for existing user")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.ad_connect_user_account_modification', key='activity_data')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    process_id = activity_data['data']['process_id']
    hook.info(f"Updating activity status for process_id: {process_id}")

    # Get connection id from environment variable
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Update activity data
    update_query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_status = 'Completed',
            u_remarks = 'The requested role is already present.'
        WHERE u_identifier = '{process_id}'
    """

    try:
        result = db_hook.execute_query(update_query)

        if not result.get('success'):
            error_msg = f"Failed to update activity data: {result.get('error', 'Unknown error')}"
            hook.error(error_msg)
            activity_data['data']['activity_update_result_existing_user'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            return {
                "success": False,
                "error": error_msg
            }

        hook.info(f"Successfully updated activity data for process_id: {process_id}")

        # Update activity_data with update result
        activity_data['data']['activity_update_result_existing_user'] = {
            'status': 'Completed',
            'remarks': 'The requested role is already present.',
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                'status': 'Completed',
                'remarks': 'The requested role is already present.'
            }
        }

    except Exception as e:
        error_msg = f"Error updating activity data: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['activity_update_result_existing_user'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def send_communication_existing_user_add_error_user_account_modify(**context) -> Dict[str, Any]:
    """
    Send communication for existing user add error in user account modification.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data and AD process details from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification using email hook

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Already Exists'
            },
            'activity_update_result_existing_user': {
                'status': 'Completed',
                'remarks': 'The requested role is already present.',
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Already Exists'
            },
            'activity_update_result_existing_user': {
                'status': 'Completed',
                'remarks': 'The requested role is already present.',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'email_notification_result_existing_user': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'user_adid': 'JDOE123',
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
        hook.info("Starting email notification for existing user")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.sr_activity_status_update_existing_user_add_user_account_modify', key='activity_data')
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    env = activity_data['data']['activity_info_fetched']['env']
    description = activity_data['data']['activity_info_fetched']['description']
    organization = activity_data['data']['activity_info_fetched']['organization']
    platform = activity_data['data']['activity_info_fetched']['platform']
    business_justification = activity_data['data']['activity_info_fetched']['business_justification']
    modification_action = activity_data['data']['activity_info_fetched']['modification_action']
    role = activity_data['data']['activity_info_fetched']['role']
    ad_output = activity_data['data']['ad_process_result']['AD_OUTPUT']
    process_id = activity_data['data']['process_id']
    task_type = activity_data['data']['activity_info_fetched']['task_type']

    hook.info(f"Sending existing user notification to user: {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_sr_management",
            mapping_namespace_name="access_management_enable",
            mapping_key="existing_user_email_template_user_account_modify"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for existing user template")

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
            "env": env,
            "description": description,
            "organization": organization,
            "platform": platform,
            "business_justification": business_justification,
            "modification_action": modification_action,
            "role": role,
            "ad_output": ad_output,
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
                "notifier_id": [user_email]
            })
            hook.info("Send email notification task initiated")

            # Update activity_data with success information
            activity_data['data']['email_notification_result_existing_user'] = {
                'status': 'Sent',
                'recipients': [user_email],
                'user_adid': user_adid,
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
            activity_data['data']['email_notification_result_existing_user'] = {
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
        activity_data['data']['email_notification_result_existing_user'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

def sr_activity_status_update_non_existing_user_remove_user_account_modify(**context) -> Dict[str, Any]:
    """
    Update activity status when user does not exist during user account modification.

    This function:
    1. Gets worklog_id from XCom
    2. Gets process_id from activity_data
    3. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with status and remarks
    4. Updates activity_data with the update result

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Remove Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Does Not Exist'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Remove Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Does Not Exist'
            },
            'activity_update_result_non_existing_user': {
                'status': 'Completed',
                'remarks': 'The requested role is already present. ',
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
        hook.info("Starting activity status update for non-existing user remove")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(key='activity_data')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    process_id = activity_data['data']['process_id']
    hook.info(f"Updating activity status for process_id: {process_id}")

    # Get connection id from environment variable
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Update activity data
    update_query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY 
        SET u_status = 'Completed',
            u_remarks = 'The requested role is already present. ' 
        WHERE u_identifier = '{process_id}'
    """

    try:
        result = db_hook.execute_query(update_query)

        if not result.get('success'):
            error_msg = f"Failed to update activity data: {result.get('error', 'Unknown error')}"
            hook.error(error_msg)
            activity_data['data']['activity_update_result_non_existing_user'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            return {
                "success": False,
                "error": error_msg
            }

        hook.info(f"Successfully updated activity data for process_id: {process_id}")

        # Update activity_data with update result
        activity_data['data']['activity_update_result_non_existing_user'] = {
            'status': 'Completed',
            'remarks': 'The requested role is already present. ',
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                'status': 'Completed',
                'remarks': 'The requested role is already present. '
            }
        }

    except Exception as e:
        error_msg = f"Error updating activity data: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['activity_update_result_non_existing_user'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def send_communication_non_existing_user_remove_error_user_account_modify(**context) -> Dict[str, Any]:
    """
    Send communication for non-existing user remove error in user account modification.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data and AD process details from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification using email hook

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Remove Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Does Not Exist'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Remove Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Does Not Exist'
            },
            'email_notification_result_non_existing_user': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'user_adid': 'JDOE123',
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
        hook.info("Starting email notification for non-existing user remove")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(key='activity_data')
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    env = activity_data['data']['activity_info_fetched']['env']
    description = activity_data['data']['activity_info_fetched']['description']
    organization = activity_data['data']['activity_info_fetched']['organization']
    platform = activity_data['data']['activity_info_fetched']['platform']
    business_justification = activity_data['data']['activity_info_fetched']['business_justification']
    modification_action = activity_data['data']['activity_info_fetched']['modification_action']
    role = activity_data['data']['activity_info_fetched']['role']
    ad_output = activity_data['data']['ad_process_result']['AD_OUTPUT']
    process_id = activity_data['data']['process_id']
    task_type = activity_data['data']['activity_info_fetched']['task_type']

    hook.info(f"Sending non-existing user remove notification to user: {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_sr_management",
            mapping_namespace_name="access_management_enable",
            mapping_key="non_existing_user_remove_email_template_user_account_modify"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for non-existing user remove template")

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
            "process_id": process_id,
            "env": env,
            "description": description,
            "organization": organization,
            "platform": platform,
            "business_justification": business_justification,
            "modification_action": modification_action,
            "role": role,
            "ad_output": ad_output,
            "support_email": os.getenv('SUPPORT_EMAIL', 'support@example.com'),
            "environment": os.getenv('BUILD_MODE', 'production')
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
            activity_data['data']['email_notification_result_non_existing_user'] = {
                'status': 'Sent',
                'recipients': [user_email],
                'user_adid': user_adid,
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
            activity_data['data']['email_notification_result_non_existing_user'] = {
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
        activity_data['data']['email_notification_result_non_existing_user'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

def sr_creation_activity_update_ad_process_errored_user_account_modify(**context) -> Dict[str, Any]:
    """
    Update activity status when AD process fails during user account modification.

    This function:
    1. Gets worklog_id from XCom
    2. Gets process_id from activity_data
    3. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with status and remarks
    4. Updates activity_data with the update result

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'role': 'Developer',
                'modification_action': 'Add Access'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished with error',
                'AD_OUTPUT': 'Error in AD process'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'role': 'Developer',
                'modification_action': 'Add Access'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished with error',
                'AD_OUTPUT': 'Error in AD process'
            },
            'activity_update_result_ad_process_error': {
                'status': 'Error in check AD process',
                'remarks': 'Transfer to FO team. ITSM error in SR submission. Validation successful for request User Account Modification',
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
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.ad_connect_user_account_modification', key='activity_data')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    process_id = activity_data['data']['process_id']
    hook.info(f"Updating activity status for process_id: {process_id}")

    # Get connection id from environment variable
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Update activity data
    update_query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY 
        SET u_status = 'Error in check AD process',
            u_remarks = 'Transfer to FO team. ITSM error in SR submission. Validation successful for request User Account Modification'
        WHERE u_identifier = '{process_id}'
    """

    try:
        result = db_hook.execute_query(update_query)

        if not result.get('success'):
            error_msg = f"Failed to update activity data: {result.get('error', 'Unknown error')}"
            hook.error(error_msg)
            activity_data['data']['activity_update_result_ad_process_error'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            return {
                "success": False,
                "error": error_msg
            }

        hook.info(f"Successfully updated activity data for process_id: {process_id}")

        # Update activity_data with update result
        activity_data['data']['activity_update_result_ad_process_error'] = {
            'status': 'Error in check AD process',
            'remarks': 'Transfer to FO team. ITSM error in SR submission. Validation successful for request User Account Modification',
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                'status': 'Error in check AD process',
                'remarks': 'Transfer to FO team. ITSM error in SR submission. Validation successful for request User Account Modification'
            }
        }

    except Exception as e:
        error_msg = f"Error updating activity data: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['activity_update_result_ad_process_error'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def send_communication_ad_process_errored_user_account_modify(**context) -> Dict[str, Any]:
    """
    Send communication for AD process error during user account modification.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data and AD process details from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification using email hook

    Example input (from activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'role': 'Developer',
                'modification_action': 'Add Access'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished with error',
                'AD_OUTPUT': 'Error in AD process'
            },
            'activity_update_result_ad_process_error': {
                'status': 'Error in check AD process',
                'remarks': 'Transfer to FO team. ITSM error in SR submission. Validation successful for request User Account Modification',
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'role': 'Developer',
                'modification_action': 'Add Access'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished with error',
                'AD_OUTPUT': 'Error in AD process'
            },
            'activity_update_result_ad_process_error': {
                'status': 'Error in check AD process',
                'remarks': 'Transfer to FO team. ITSM error in SR submission. Validation successful for request User Account Modification',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'email_notification_result_ad_process_error_user_account_modify': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'user_adid': 'JDOE123',
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
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.sr_creation_activity_update_ad_process_errored_user_account_modify', key='activity_data')
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    user_adid = activity_data['data']['activity_info_fetched']['user_adid']
    environment = activity_data['data']['activity_info_fetched'].get('environment', 'PROD')
    description = activity_data['data']['activity_info_fetched']['description']
    organization = activity_data['data']['activity_info_fetched']['organization']
    platform = activity_data['data']['activity_info_fetched']['platform']
    business_justification = activity_data['data']['activity_info_fetched']['business_justification']
    role = activity_data['data']['activity_info_fetched']['role']
    modification_action = activity_data['data']['activity_info_fetched']['modification_action']
    ad_output = activity_data['data']['ad_process_result'].get('AD_OUTPUT', '')
    python_status = activity_data['data']['ad_process_result'].get('PYTHON_STATUS', '')
    process_id = activity_data['data'].get('process_id', '')
    task_type = activity_data['data']['activity_info_fetched'].get('task_type', 'User Account Modification')

    hook.info(f"Sending AD process error notification to user: {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_sr_management",
            mapping_namespace_name="access_management_enable",
            mapping_key="ad_process_error_email_template_common"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for AD process error template")

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
            "env": environment,
            "description": description,
            "organization": organization,
            "platform": platform,
            "business_justification": business_justification,
            "task_type": task_type,
            "role": role,
            "modification_action": modification_action,
            "ad_output": ad_output,
            "process_id": process_id,
            "python_status": python_status
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"AD Process Error - User Account Modification for {user_adid}"
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
            activity_data['data']['email_notification_result_ad_process_error_user_account_modify'] = {
                'status': 'Sent',
                'recipients': [user_email],
                'user_adid': user_adid,
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
            activity_data['data']['email_notification_result_ad_process_error_user_account_modify'] = {
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
        activity_data['data']['email_notification_result_ad_process_error_user_account_modify'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

def sr_creation_failed_activity_update_user_account_modify(**context) -> Dict[str, Any]:
    """
    Update activity data for failed service request creation.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data and service account details from activity_data
    3. Updates activity_data with failed SR creation information
    
    Example input (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account Modified Successfully',
                'ad_output_user_account_modify': 'User Account Modified Successfully'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - JDOE123;\nUser Email - john.doe@company.com;\nManager Name - Jane Smith;\nManager Email - jane.smith@company.com;\nEnvironment - Production;\nOrganization - IT;\nPlatform - Windows;\nBusiness Justification - Role change request;\nModification Action - Add Access;\nRole - Developer'
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
        hook.info("Starting activity update for failed SR creation")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='user_account_modification_group.sr_creation_user_account_modify', key='activity_data')
    process_id = activity_data['data']['process_id']
    error_msg = activity_data['data']['sr_creation_result'].get('error', 'Unknown error')

    hook.info(f"Updating activity data for failed SR creation. Process ID: {process_id}")

    # Get connection id from environment variable
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to update activity table
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_status = 'Error in ITSM Process',
            u_remarks = 'Transfer to FO team. ITSM error in SR submission. Validation successful for request (User Account Modification)'
        WHERE u_identifier = '{process_id}'
    """
    try:
        # Execute query
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
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                "status": "Failed",
                "error": error_msg
            }
        }

    except Exception as e:
        error_msg = f"Error updating activity table: {str(e)}"
        hook.error(error_msg)
        # Update activity_data with error information
        activity_data['data']['sr_creation_failed_activity_update'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)  

def sr_creation_failed_email_notification_user_account_modify(**context) -> Dict[str, Any]:
    """
    Send communication for failed service request creation.

    This function:
    1. Gets worklog_id from XCom
    2. Gets user data and service account details from activity_data
    3. Prepares email content using template from mapping
    4. Sends email notification using email hook    

    Example input (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account Modified Successfully',
                'ad_output_user_account_modify': 'User Account Modified Successfully'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - JDOE123;\nUser Email - john.doe@company.com;\nManager Name - Jane Smith;\nManager Email - jane.smith@company.com;\nEnvironment - Production;\nOrganization - IT;\nPlatform - Windows;\nBusiness Justification - Role change request;\nModification Action - Add Access;\nRole - Developer'
            },
            'sr_creation_failed_activity_update': {
                'status': 'Failed',
                'error': 'Failed to create service request',
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'User Account Modification',
                'user_adid': 'JDOE123',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com',
                'manager_name': 'Jane Smith',
                'manager_email': 'jane.smith@company.com',
                'env': 'Production',
                'description': 'User account modification request',
                'organization': 'IT',
                'platform': 'Windows',
                'business_justification': 'Role change request',
                'modification_action': 'Add Access',
                'role': 'Developer'
            },
            'ad_process_result': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account Modified Successfully',
                'ad_output_user_account_modify': 'User Account Modified Successfully'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - JDOE123;\nUser Email - john.doe@company.com;\nManager Name - Jane Smith;\nManager Email - jane.smith@company.com;\nEnvironment - Production;\nOrganization - IT;\nPlatform - Windows;\nBusiness Justification - Role change request;\nModification Action - Add Access;\nRole - Developer'
            },
            'sr_creation_failed_activity_update': {
                'status': 'Failed',
                'error': 'Failed to create service request',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'email_notification_result': {
                'status': 'Sent',
                'recipients': ['john.doe@company.com'],
                'sr_number': 'REQ1234',
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
        hook.info("Starting email notification for failed SR creation")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    activity_data = ti.xcom_pull(task_ids='new_service_account_group.sr_creation_failed_activity_update_user_account_modify', key='activity_data')
    info = activity_data['data'].get('activity_info_fetched', {})

    process_id = activity_data['data']['process_id']
    sr_creation_result = activity_data['data'].get('sr_creation_result', {})

    # Initialize error and status variables
    error = None
    status = None

    # Extract error and status if sr_creation_result is available
    if sr_creation_result:
        error = sr_creation_result.get('error')
        status = sr_creation_result.get('status')

    user_email = info.get('user_email', '')
    user_name = info.get('user_name', '')
    task_type = info.get('task_type', '')

    hook.info(f"Sending SR creation failure notification to user: {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook #sr_creation_failed_email_template_new_service_account will be common
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_sr_management",
            mapping_namespace_name="access_management_enable",
            mapping_key="sr_creation_failed_email_template_common"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for SR creation failure template")

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
            "error_message": error,
            "task_type": task_type,
            "status": status
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Service Request Creation Failed - User Account Modification"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": [email_group],
                "notifier_id": user_email
            })
            hook.info("Send email notification task initiated")

            # Update activity_data with success information
            activity_data['data']['email_notification_result_sr_creation_failed'] = {
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
            error_msg = f"Failed to send email notification: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['email_notification_result_sr_creation_failed'] = {
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
        activity_data['data']['email_notification_result_sr_creation_failed'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)
        raise ValueError(error_msg)

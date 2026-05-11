import logging
from typing import Dict, Any
from hooks.access_management_db_hook import AccessManagementSQLHook
from hooks.ad_process_hook import AD_ProcessHook
from airflow.hooks.base import BaseHook
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

def fetch_activity_info_other_requests(**context) -> Dict[str, Any]:
    """
    Fetch activity information for Other Requests and Service Account Password Reset from EXT_ACCESS_MANAGEMENT_ACTIVITY table.

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
                'task_type': 'Service Account Password Reset',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='update_or_create_request_activity', key='activity_data')
    activity_identifier = activity_data['data']['process_id']

    # Get worklog_id from XCom
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to fetch required fields
    query = f"""
        SELECT
            u_task_type,
            u_user_name,
            u_user_email
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_identifier = '{activity_identifier}'
    """

    # Execute query
    result = db_hook.get_row_data(query)

    if not result.get('success') or not result.get('data'):
        logger.error(f"No activity found for process_id: {activity_identifier}")
        hook.error(f"No activity found for process_id: {activity_identifier}")
        raise ValueError(f"No activity found for process_id: {activity_identifier}")

    # Add activity info to existing activity_data
    activity_data['data']['activity_info_fetched'] = {
        'task_type': result['data'][0],
        'user_name': result['data'][1],
        'user_email': result['data'][2]
    }

    logger.info(f"Fetched activity info for process_id: {activity_identifier}")
    hook.info(f"Fetched activity info for process_id: {activity_identifier}")

    # Push updated activity_data back to XCom
    ti.xcom_push(key='activity_data', value=activity_data)

    return activity_data


def sr_creation_other_requests(**context) -> Dict[str, Any]:
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
                'process_id': '20250321173105948',
                'activity_info_fetched': {
                    'task_type': 'user_task_type',
                    'user_name': 'John Doe',
                    'user_email': 'john.doe@company.com'
                }
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'user_task_type',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com'
            },
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
    activity_data = ti.xcom_pull(task_ids='other_requests_group.fetch_activity_info_other_requests', key='activity_data')
    process_id = activity_data['data']['process_id']
    task_type = activity_data['data']['activity_info_fetched']['task_type']

    hook.info(f"Creating SR for {task_type}")

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Handle each case separately
    if task_type == 'ServiceAc Passwd Reset':
        # 1. Fetch activity data for ServiceAc Password Reset
        query = f"""
            SELECT
                u_business_justification,
                u_svc_name,
                u_user_name,
                u_user_email,
                u_user_adid,
                u_svc_owner_email,
                u_organization,
                u_env,
                u_platform
            FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
            WHERE u_identifier = '{process_id}'
        """
        result = db_hook.get_row_data(query)
        if not result.get('success') or not result.get('data'):
            hook.error(f"No activity found for process_id: {process_id}")
            raise ValueError(f"No activity found for process_id: {process_id}")

        activity_row = result['data']
        hook.info(f"Fetched activity data for process_id: {process_id}")

        # 2. Extract fields for ServiceAc Password Reset
        u_business_justification = activity_row[0]
        u_svc_name = activity_row[1]
        u_user_name = activity_row[2]
        u_user_email = activity_row[3]
        u_user_adid = activity_row[4]
        u_svc_owner_email = activity_row[5]
        u_organization = activity_row[6]
        u_env = activity_row[7]
        u_platform = activity_row[8]

        # 3. Fetch approver using organization from activity data
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

        # 4. Create notes for ServiceAc Password Reset
        notes = (
            f"Service Account Name - {u_svc_name};\n"
            f"User Name - {u_user_name};\n"
            f"User Email - {u_user_email};\n"
            f"Manager AD Id - {u_user_adid};\n"
            f"Service Owner Email - {u_svc_owner_email};\n"
            f"Organization - {u_organization};\n"
            f"Environment - {u_env};\n"
            f"Platform - {u_platform};\n"
            f"Business Justification - {u_business_justification};\n"
            f"Build Mode - {os.getenv('BUILD_MODE', 'production')}"
        )

        # 5. Create payload for ServiceAc Password Reset
        sr_name = f"Service Account Password Reset - {u_svc_name} ({u_user_name}) - {u_organization}"
        payload = {
            "name": sr_name,
            "offering_title": "Access Management - Uprising - Reset Password - Service Account",
            "source_keyword": "MYIT",
            "title_instance_id": "SRHAA5V0G9R60AOHC3NULKRZSWWIKY",
            "full_name": "Svc Enable Automation",
            "login_id": "svc_enable_itsm",
            "sr_type_field_1": u_business_justification,
            "sr_type_field_2": u_svc_name,
            "sr_type_field_3": "Access Management User Create",
            "sr_type_field_4": u_user_email,
            "sr_type_field_5": u_user_adid,
            "sr_type_field_10": approver.lower(),
            "sr_type_field_11": u_svc_owner_email,
            "sr_type_field_12": u_organization,
            "sr_type_field_13": u_env,
            "sr_type_field_14": u_platform,
            "sr_type_field_15": u_user_name
        }

    else:
        # 1. Fetch activity data for Other Requests
        query = f"""
            SELECT
                u_business_justification,
                u_user_adid,
                u_user_name,
                u_user_email,
                u_manager_adid,
                u_organization,
                u_manager_name,
                u_manager_email
            FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
            WHERE u_identifier = '{process_id}'
        """
        result = db_hook.get_row_data(query)
        if not result.get('success') or not result.get('data'):
            hook.error(f"No activity found for process_id: {process_id}")
            raise ValueError(f"No activity found for process_id: {process_id}")

        activity_row = result['data']
        hook.info(f"Fetched activity data for process_id: {process_id}")

        # 2. Extract fields for Other Requests
        u_business_justification = activity_row[0]
        u_user_adid = activity_row[1]
        u_user_name = activity_row[2]
        u_user_email = activity_row[3]
        u_manager_adid = activity_row[4]
        u_organization = activity_row[5]
        u_manager_name = activity_row[6]
        u_manager_email = activity_row[7]

        # 3. Create notes for Other Requests
        notes = (
            f"User Name - {u_user_name};\n"
            f"User AD Id - {u_user_adid};\n"
            f"User Email - {u_user_email};\n"
            f"Manager AD Id - {u_manager_adid};\n"
            f"Manager Name - {u_manager_name};\n"
            f"Manager Email - {u_manager_email};\n"
            f"Organization - {u_organization};\n"
            f"Business Justification - {u_business_justification};\n"
            f"Environment - {os.getenv('BUILD_MODE', 'production')}"
        )

        # 4. Create payload for Other Requests

        sr_name = f"Other Requests - {u_user_name} ({u_user_adid}) - {u_organization}"
        payload = {
            "name": sr_name,
            "offering_title": "Access Management - Uprising - Other",
            "source_keyword": "MYIT",
            "title_instance_id": "SRHAA5V0G9R60AO8U4FRFQ7TLAA6YD",
            "full_name": "Svc Enable Automation",
            "login_id": "svc_enable_itsm",
            "sr_type_field_1": u_business_justification,
            "sr_type_field_2": u_user_adid,
            "sr_type_field_3": "Access Management User Create",
            "sr_type_field_4": u_user_name,
            "sr_type_field_5": u_user_email,
            "sr_type_field_10": u_manager_adid.lower(),
            "sr_type_field_11": u_organization,
            "sr_type_field_12": u_manager_name,
            "sr_type_field_13": u_manager_adid,
            "sr_type_field_14": u_manager_email
        }

    ti.xcom_push(key='activity_data', value=activity_data)

    hook.info(f"Added formatted notes to activity data")

    # Common API calling code
    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))
    ticketing_system: str = os.environ.get("DEFAULT_TICKETING_SYSTEM", "REMEDY")

    create_servie_request_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/service-requests?system={ticketing_system}"

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

                if notes:
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

                if notes:
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

        if notes:
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

        if notes:
            activity_data['data']['sr_creation_result']['notes'] = notes

        ti.xcom_push(key='activity_data', value=activity_data)
        return {
            "success": False,
            "data": activity_data['data']['sr_creation_result']
        }

def sr_worklog_update_other_requests(**context) -> Dict[str, Any]:
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
                'task_type': 'Service Account Password Reset',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com'
            },
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
                'task_type': 'Service Account Password Reset',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com'
            },
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
    activity_data = ti.xcom_pull(task_ids='other_requests_group.sr_creation_other_requests', key='activity_data')
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

def sr_creation_activity_update_other_requests(**context) -> Dict[str, Any]:
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
                'task_type': 'Service Account Password Reset',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - esshar01;\nUser Email - john.doe@company.com;\nEnvironment - production;\nComments - Access request'
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
                'task_type': 'Service Account Password Reset',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - esshar01;\nUser Email - john.doe@company.com;\nEnvironment - production;\nComments - Access request'
            },
            'work_info_result': {
                'work_info_id': 'WI1234',
                'status': 'Created'
            },
            'activity_update_result': {
                'status': 'Completed',
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
        hook.info("Starting activity table update for password reset")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='other_requests_group.sr_worklog_update_other_requests', key='activity_data')
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
            u_status = 'Completed'
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
            'status': 'Completed',
            'sr_number': sr_number,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                "status": "Completed",
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

def sr_creation_email_notification_other_requests(**context) -> Dict[str, Any]:
    """
    Send email notification for other requests service request creation.

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
                'task_type': 'Service Account Password Reset',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - esshar01;\nUser Email - john.doe@company.com;\nEnvironment - production;\nComments - Access request'
            },
            'work_info_result': {
                'work_info_id': 'WI1234',
                'status': 'Created'
            },
            'activity_update_result': {
                'status': 'Completed',
                'sr_number': 'REQ1234',
                'timestamp': '2024-03-21T17:31:05.948'
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'task_type': 'Service Account Password Reset',
                'user_name': 'John Doe',
                'user_email': 'john.doe@company.com'
            },
            'sr_creation_result': {
                'request_number': 'REQ1234',
                'status': 'Assigned',
                'notes': 'User Name - John Doe;\nUser AD Id - esshar01;\nUser Email - john.doe@company.com;\nEnvironment - production;\nComments - Access request'
            },
            'work_info_result': {
                'work_info_id': 'WI1234',
                'status': 'Created'
            },
            'activity_update_result': {
                'status': 'Completed',
                'sr_number': 'REQ1234',
                'timestamp': '2024-03-21T17:31:05.948'
            },
            'email_notification_result': {
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
        hook.info("Starting email notification for password reset")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='other_requests_group.sr_creation_activity_update_other_requests', key='activity_data')
    task_type = activity_data['data']['activity_info_fetched']['task_type']
    user_name = activity_data['data']['activity_info_fetched']['user_name']
    sr_number = activity_data['data']['sr_creation_result']['request_number']
    user_email = activity_data['data']['activity_info_fetched']['user_email']
    process_id = activity_data['data']['process_id']
    notes = activity_data['data']['sr_creation_result']['notes']

    hook.info(f"Sending email notification for SR: {sr_number} to user: {user_email}")

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_sr_management",
            mapping_namespace_name="access_management_enable",
            mapping_key="sr_creation_notification_email_template_other_request"  # EMAIL TEMPLATE MAPPING KEY 3
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
            "process_id": process_id,
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

def sr_creation_failed_email_notification_other_requests(**context) -> Dict[str, Any]:
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
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'sr_creation_result': {
                "success": False,
                "error": "Error occurred while creating SR"
            }
        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'sr_creation_result': {
                "success": False,
                "error": "Error occurred while creating SR"
            },
            'sr_creation_failed_user_communication_result': {
                'status': 'Sent',
                'recipients': [user_email],
                'timestamp': datetime.now().isoformat()
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
    activity_data = ti.xcom_pull(task_ids='other_requests_group.sr_activity_status_update_process_sr_error_other_requests', key='activity_data')
    user_email = activity_data['data']['activity_info_fetched'].get('user_email', '')
    user_name = activity_data['data']['activity_info_fetched'].get('user_name', '')
    organization = activity_data['data']['activity_info_fetched'].get('organization', '')
    env = activity_data['data']['activity_info_fetched'].get('env', '')
    process_id = activity_data['data']['process_id']
    task_type = activity_data['data']['activity_info_fetched']['task_type']
    hook.info(f"Sending sr creation failed email notification to user: {user_email}")

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
            "user_email": user_email,
            "task_type": task_type,
            "organization": organization,
            "identifier": process_id,
            "process_id": process_id,
            "env": env
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
            hook.info("Send inactive user status notification email task initiated")

            # Update activity_data with success information
            activity_data['data']['sr_creation_failed_user_communication_result'] = {
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

def sr_activity_status_update_process_sr_error_other_requests(**context) -> Dict[str, Any]:
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
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'sr_creation_result': {
                "success": False,
                "error": "Error occurred while creating SR"
            },
            'sr_creation_failed_user_communication_result': {
                'status': 'Sent',
                'recipients': [user_email],
                'timestamp': datetime.now().isoformat()
            }

        }
    }

    Example output (added to activity_data):
    {
        'data': {
            'process_id': '20250321173105948',
            'activity_info_fetched': {
                'user_name': 'John Doe',
                'user_adid': 'esshar01',
                'user_email': 'john.doe@company.com',
                'organization': 'IT',
                'comments': 'Password reset request',
                'env': 'production'
            },
            'sr_creation_result': {
                "success": False,
                "error": "Error occurred while creating SR"
            },
            'sr_creation_failed_user_communication_result': {
                'status': 'Sent',
                'recipients': [user_email],
                  'timestamp': datetime.now().isoformat()
            },
            'sr_process_error_status_update': {
                'status': 'Updated',
                'message': 'Activity status updated for AD process error',
                'timestamp': datetime.now().isoformat()
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
    activity_data = ti.xcom_pull(task_ids='other_requests_group.sr_creation_other_requests', key='activity_data')
    process_id = activity_data['data']['process_id']
    action = activity_data['data'].get('action', 'Password Reset')

    hook.info(f"Updating activity status for process_id: {process_id}")

    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to update activity table
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_status = 'Error in ITSM Process',
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
            activity_data['data']['sr_process_error_status_update'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            raise ValueError(f"Failed to update activity table: {error_msg}")

        hook.info(f"Successfully updated activity status for process_id: {process_id}")

        # Update activity_data with success information
        activity_data['data']['sr_process_error_status_update'] = {
            'status': 'Updated',
            'message': 'SR creation failed. ITSM error in SR submission',
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

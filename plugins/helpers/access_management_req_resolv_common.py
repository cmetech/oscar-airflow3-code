from datetime import datetime, timedelta
from typing import Dict, Any
import logging
import os
from hooks.worklog_hook import WorkLogHook
from hooks.access_management_db_hook import AccessManagementSQLHook
import httpx
from jinja2 import Template
from hooks.mapping_hook import MappingHook
from hooks.notify_hook import NotifyHook
import re

logger = logging.getLogger(__name__)

def check_activity_request_resolve_common(**context) -> Dict[str, Any]:
    """
    Common function to check and resolve access management requests.
    Extracts u_identifier from conf data and worklog_id from XCom.
    Fetches and processes activity data including UPN generation and time-based flags.

    Example Input (context):
        context = {
            'conf': {
                'u_identifier': 'AM-20240607123456-1a2b3c4d',
                ...
            },
            'ti': <TaskInstance>,
            ...
        }
        # XCom (from previous task):
        context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id') -> 'WL-123456'

    Example Output (return value and XCom):
        {
            'data': {
                'process_id': 'AM-20240607123456-1a2b3c4d',
                'worklog_id': 'WL-123456',
                'request_resolve_initial_data': {
                    'status': 'Pending',
                    'identifier': 'AM-20240607123456-1a2b3c4d',
                    'remarks': 'Initial request',
                    'task_type': 'New User Account',
                    'upn': 'user@uprising.t-mobile.com',
                    'flag_update_date': 0,
                    'creation_date': '2024-06-07 12:34:56',
                    'update_date': '2024-06-07 12:34:56',
                    'sr_no': 'REQ123456',
                    'user_email': 'user@company.com'
                }
            }
        }
    The same structure is also pushed to XCom with key 'activity_data_request_resolve'.
    """
    try:
        # conf = context.get('conf', {})
        conf = context['dag_run'].conf if context.get('dag_run') else {}

        try:
            conf = context['dag_run'].conf if context.get('dag_run') else {}
            if not conf:
                logger.error("No configuration provided")
                return {
                    "success": False,
                    "error": "No configuration provided"
                }
        except Exception as e:
            logger.error(f"Error retrieving configuration: {str(e)}")
            return {
                "success": False,
                "error": f"Error retrieving configuration: {str(e)}"
            }

        u_identifier = conf.get('u_identifier')
        if not u_identifier:
            raise ValueError("u_identifier not found in conf data")

        ti = context['ti']
        worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
        if not worklog_id:
            raise ValueError("worklog_id not found in XCom")

        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting activity request resolve check")

        try:
            hook.add_metadata([
                {"key": "process_id", "value": u_identifier}
            ])
        except Exception as metadata_error:
            logger.warning(f"Failed to add metadata to worklog, but continuing with main process: {str(metadata_error)}")
            # Continue with main process - metadata failure should not impact the core functionality

        oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
        db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

        query = f"""
            SELECT
                u_identifier,
                u_status,
                u_remarks,
                u_task_type,
                u_env,
                u_user_adid,
                u_svc_name,
                created_on,
                updated_on,
                u_sr_no,
                u_user_email
            FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
            WHERE u_identifier = '{u_identifier}'
        """

        logger.debug(f"Query: {query}")
        result = db_hook.get_row_data(query)
        if not result.get('success') or not result.get('data'):
            hook.error(f"No activity found for u_identifier: {u_identifier}")
            raise ValueError(f"No activity found for u_identifier: {u_identifier}")

        # Use result['data'] directly since it already contains the first row
        activity_info = result['data']
        hook.info(f"Fetched activity info for u_identifier: {u_identifier}")

        # Log the raw updated_on value to understand what we're getting
        updated_on_raw = activity_info[8]
        hook.info(f"Raw updated_on value: {updated_on_raw}, Type: {type(updated_on_raw)}")

        # activity_info indices: 0=u_identifier, 1=u_status, 2=u_remarks, 3=u_task_type, 4=u_env, 5=u_user_adid, 6=u_svc_name, 7=created_on, 8=updated_on, 9=u_sr_no, 10=u_user_email
        upn = None
        env = activity_info[4] or ""  # u_env
        task_type = activity_info[3]  # u_task_type
        if task_type == "User Password Reset" or task_type == "User Account Password Reset":
            adid = activity_info[5] or ""  # u_user_adid
            upn = f"{adid}@{'lab.' if env.lower() == 'lab' else ''}uprising.t-mobile.com"
        elif task_type == "New Service Account":
            svc_name = activity_info[6] or ""  # u_svc_name
            upn = f"{svc_name}@{'lab.' if env.lower() == 'lab' else ''}uprising.t-mobile.com"

        # All time comparisons are done using naive datetimes (server local time),
        # matching how created_on/updated_on are set in the DB.
        now = datetime.now()
        # Handle both datetime and string types for updated_on
        if isinstance(activity_info[8], datetime):
            update_date = activity_info[8]
        else:
            # If it's a string, parse it into a datetime
            update_date = datetime.strptime(str(activity_info[8]), '%Y-%m-%d %H:%M:%S')

        hook.info(f"Type of update_date: {type(update_date)}, Value: {update_date}")
        date_24 = update_date + timedelta(hours=24)
        date_48 = update_date + timedelta(hours=48)
        date_72 = update_date + timedelta(hours=72)
        count_email = str(activity_info[2]).count("Email")  # u_remarks
        flag_update_date = 0

        if now > date_24 and count_email < 1 and now < date_48:
            flag_update_date = 1
        elif now > date_48 and count_email < 2 and now < date_72:
            flag_update_date = 1
        elif count_email >= 2 and now > date_72:
            flag_update_date = 2

        request_resolve_initial_data = {
            'status': activity_info[1],  # u_status
            'identifier': activity_info[0],  # u_identifier
            'remarks': activity_info[2],  # u_remarks
            'task_type': activity_info[3],  # u_task_type
            'upn': upn,
            'flag_update_date': flag_update_date,
            'creation_date': activity_info[7].strftime('%Y-%m-%d %H:%M:%S'),  # created_on
            'update_date': activity_info[8].strftime('%Y-%m-%d %H:%M:%S'),  # updated_on
            'sr_no': activity_info[9],  # u_sr_no
            'user_email': activity_info[10]  # u_user_email
        }

        activity_data = {
            'data': {
                'process_id': u_identifier,
                'worklog_id': worklog_id,
                'request_resolve_initial_data': request_resolve_initial_data
            }
        }
        context['ti'].xcom_push(key='activity_data_request_resolve', value=activity_data)

        return activity_data

    except Exception as e:
        logger.error(f"Error in check_activity_request_resolve_common: {str(e)}")
        raise

def check_wo_status(**context) -> Dict[str, Any]:
    """
    Check the status of a Work Order (WO) for a given Service Request (SR).

    This function:
    1. Gets activity_data from previous task (check_activity_request_resolve_common)
    2. Extracts SR number from request_resolve_initial_data
    3. Searches for service request using request_number
    4. Gets SR status from the response
    5. Searches for work orders with service_request_id and specific support group
    6. Gets work order ID from first work order
    7. Sets flag_wo based on SR status:
       - 'cancel' if status contains 'rejected' (case-insensitive)
       - 'true' if work order ID exists
       - 'false' otherwise
    8. Adds wo_status_data to the existing activity_data structure

    Input (from previous task's activity_data):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            }
        }
    }

    Output (extends activity_data with wo_status_data while preserving all previous data):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',  # Can be 'true', 'cancel', or 'false'
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,  # True if any error occurred
                'error_message': None  # Error message if error_flag is True
            }
        }
    }

    In case of errors:
    - All previous data is preserved
    - wo_status_data is added with error information
    - error_flag is set to true
    - error_message contains the error details
    - flag_wo is set to 'errored'

    Args:
        **context: Airflow context dictionary containing task instance and other metadata

    Returns:
        dict: The complete activity_data with all previous data plus added wo_status_data
    """
    ti = context['ti']
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        raise ValueError("worklog_id not found in XCom")

    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    hook.info("Starting check_wo_status")

    # Get activity data from previous task
    activity_data = ti.xcom_pull(task_ids='check_activity_request_resolve', key='activity_data_request_resolve')

    if not activity_data or 'data' not in activity_data:
        hook.error("Activity data not found in XCom")
        error_data = {
            'data': {
                'wo_status_data': {
                    'error_flag': True,
                    'error_message': "Activity data not found in XCom",
                    'flag_wo': 'errored'
                }
            }
        }
        # Ensure to activity data being updated 
        activity_data['data']['wo_status_data'] = error_data['data']['wo_status_data']

        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return error_data

    # Extract SR number from activity data
    sr_no = activity_data['data'].get('request_resolve_initial_data', {}).get('sr_no')

    if not sr_no:
        hook.error("Service Request number not found in activity data")
        error_data = {
            'data': {
                'wo_status_data': {
                    'error_flag': True,
                    'error_message': "Service Request number not found in activity data",
                    'flag_wo': 'errored'
                }
            }
        }
        # Ensure to activity data being updated 
        activity_data['data']['wo_status_data'] = error_data['data']['wo_status_data']        

        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return error_data

    hook.info(f"Checking service request status for SR: {sr_no}")

    # Setup middleware API parameters
    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))
    ticketing_system: str = os.environ.get("DEFAULT_TICKETING_SYSTEM", "REMEDY")

    try:
        with httpx.Client(verify=False, timeout=480.0) as client:
            # Step 1: Search for service request
            sr_search_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/service-requests?request_number={sr_no}&system={ticketing_system}"
            sr_response = client.get(sr_search_url)
            sr_response.raise_for_status()
            service_requests = sr_response.json()

            if not service_requests or not isinstance(service_requests, list) or len(service_requests) == 0:
                hook.error(f"No service request found for request number: {sr_no}")
                error_data = {
                    'data': {
                        'wo_status_data': {
                            'sr_no': sr_no,
                            'error_flag': True,
                            'error_message': f"No service request found for request number: {sr_no}",
                            'flag_wo': 'errored'
                        }
                    }
                }
                # Ensure to activity data being updated 
                activity_data['data']['wo_status_data'] = error_data['data']['wo_status_data']

                ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
                return error_data

            # Get the first service request and extract needed fields
            service_request = service_requests[0]
            sr_status = service_request.get('status', '')
            hook.info(f"Found service request with status: {sr_status}")

            # Step 2: Search for work orders with specific parameters

            # support group name varies in prod and lab ITSM so use an env variable
            ua_user_admin_support_group_name = os.environ.get("UA_USER_ADMIN_SUPPORT_GROUP_NAME", "UA_User admin")
            ua_user_admin_assigned_group = os.environ.get("UA_USER_ADMIN_ASSIGNED_GROUP", "UA_User admin")

            wo_search_url = (
                f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets/work-orders?"
                f"service_request_id={sr_no}&"
                f"support_group_name={ua_user_admin_support_group_name}&"
                f"assigned_group={ua_user_admin_assigned_group}&"
                f"system={ticketing_system}"
            )
            hook.info(f"WO search URL: {wo_search_url}")
            logger.debug(f"WO search URL: {wo_search_url}")
            wo_response = client.get(wo_search_url)
            wo_response.raise_for_status()
            work_orders = wo_response.json()
            hook.info(f"Work orders: {work_orders}")
            logger.debug(f"WO search URL: {wo_search_url}")

            # if work order is not found, set flag_wo to 'wait'
            flag_wo = 'false'

            if 'rejected' in sr_status.lower() or 'cancelled' in sr_status.lower():
                flag_wo = 'cancel'
                hook.error(f"Service request {sr_no} is rejected or cancelled")
                error_data = {
                    'data': {
                        'wo_status_data': {
                            'sr_no': sr_no,
                            'error_flag': True,
                            'error_message': f"Service request {sr_no} is rejected or cancelled",
                            'flag_wo': flag_wo  # has been rejected by at least one approver
                        }
                    }
                }
                # Ensure to activity data being updated 
                activity_data['data']['wo_status_data'] = error_data['data']['wo_status_data']
                ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
                return error_data

            if not work_orders or not isinstance(work_orders, list) or len(work_orders) == 0:
                hook.error(f"No work orders found for service request: {sr_no}")
                error_data = {
                    'data': {
                        'wo_status_data': {
                            'sr_no': sr_no,
                            'sr_status': sr_status,
                            'error_flag': True,
                            'error_message': f"No work orders found for service request: {sr_no}",
                            'flag_wo': flag_wo  # not yet attented for approval or rejecteion
                        }
                    }
                }
                # Ensure to activity data being updated
                activity_data['data']['wo_status_data'] = error_data['data']['wo_status_data']
                ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
                return error_data

            # Get the first work order and extract needed fields
            work_order = work_orders[0]
            wo_id = work_order.get('work_order_number')  # mapped to Work Order ID in remedy handler
            hook.info(f"Found work order ID: {wo_id}")

            # Set flag based on SR status and work order as per Enable's logic

            if "rejected" in sr_status.lower():
                flag_wo = 'cancel'
            elif wo_id:
                flag_wo = 'true'

            # Initialize status data
            wo_status_data = {
                'sr_no': sr_no,
                'sr_status': sr_status,
                'wo_id': wo_id,
                'flag_wo': flag_wo,
                'detail': f"Work Order Number: {wo_id}, SR Status: {sr_status}",
                'error_flag': False,
                'error_message': None
            }

            # Update activity data with status information
            activity_data['data']['wo_status_data'] = wo_status_data

            # Push updated activity_data back to XCom
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return activity_data

    except httpx.HTTPError as e:
        error_msg = f"HTTP error occurred while checking status: {str(e)}"
        hook.error(error_msg)
        error_data = {
            'data': {
                'wo_status_data': {
                    'sr_no': sr_no,
                    'error_flag': True,
                    'error_message': error_msg,
                    'flag_wo': 'errored'
                }
            }
        }
        # Ensure to activity data being updated 
        activity_data['data']['wo_status_data'] = error_data['data']['wo_status_data']
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return error_data
    except Exception as e:
        error_msg = f"Error occurred while checking status: {str(e)}"
        hook.error(error_msg)
        error_data = {
            'data': {
                'wo_status_data': {
                    'sr_no': sr_no,
                    'error_flag': True,
                    'error_message': error_msg,
                    'flag_wo': 'errored'
                }
            }
        }
        activity_data['data']['wo_status_data'] = error_data['data']['wo_status_data']
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return error_data

def update_activity_data_wo_approved(**context) -> Dict[str, Any]:
    """
    Update activity data when Work Order is approved.

    This function:
    1. Gets activity_data from previous task (check_wo_status)
    2. Extracts process_id and wo_id from the activity_data
    3. Updates EXT_ACCESS_MANAGEMENT_ACTIVITY table with WO number and status
    4. Adds wo_approval_result to the existing activity_data structure

    Input (from previous task's activity_data):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'wo_status_data': {
                'sr_no': 'REQ123456',
                'sr_status': 'Assigned',
                'wo_id': 'WO123456',
                'flag_wo': 'true',
                'detail': 'Work Order Number: WO123456, SR Status: Assigned',
                'error_flag': False,
                'error_message': None
            }
        }
    }

    Output (extends activity_data with wo_approval_result while preserving all previous data):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
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

    In case of errors:
    - All previous data is preserved
    - wo_approval_result is added with error information
    - status is set to 'Failed'
    - error contains the error details
    - timestamp is added

    Args:
        **context: Airflow context dictionary containing task instance and other metadata

    Returns:
        dict: The complete activity_data with all previous data plus added wo_approval_result
    """
    try:
        ti = context['ti']
        worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
        if not worklog_id:
            logger.error("No worklog ID found in XCom")
            return {
                "success": False,
                "error": "No worklog ID found in XCom"
            }

        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)

        # Get worklog_id from XCom
        worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
        if not worklog_id:
            logger.error("No worklog ID found in XCom")
            return {
                "success": False,
                "error": "No worklog ID found in XCom"
            }

        # Get activity data from previous task
        activity_data = ti.xcom_pull(task_ids='check_wo_status', key='activity_data_request_resolve')
        if not activity_data or 'data' not in activity_data:
            hook.error("Activity data not found in XCom")
            return {
                "success": False,
                "error": "Activity data not found in XCom"
            }

        # Extract required data
        wo_status_data = activity_data['data'].get('wo_status_data', {})
        identifier = activity_data['data'].get('process_id')
        wo_id = wo_status_data.get('wo_id')

        if not identifier or not wo_id:
            hook.error("Missing required data: identifier or wo_id")
            return {
                "success": False,
                "error": "Missing required data: identifier or wo_id"
            }

        hook.info(f"Updating activity data for identifier: {identifier} with WO ID: {wo_id}")

        # Setup database connection
        oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
        db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

        # Update query
        query = f"""
            UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
            SET u_wo_no = '{wo_id}',
                u_status = 'SR Approved'
            WHERE u_identifier = '{identifier}'
        """

        result = db_hook.execute_query(query)
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity data: {error_msg}")
            # Add error information to activity_data
            activity_data['data']['wo_approval_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return {
                "success": False,
                "error": error_msg
            }

        hook.info(f"Successfully updated activity data for identifier: {identifier}")

        # Add approval result to activity_data
        activity_data['data']['wo_approval_result'] = {
            'status': 'SR Approved',
            'wo_id': wo_id,
            'timestamp': datetime.now().isoformat()
        }

        # Push updated activity_data back to XCom
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

        return {
            "success": True,
            "data": {
                "status": "SR Approved",
                "wo_id": wo_id
            }
        }

    except Exception as e:
        error_msg = f"Error occurred while updating activity data: {str(e)}"
        hook.error(error_msg)
        # Add error information to activity_data
        activity_data['data']['wo_approval_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def fetch_data_activity_for_response(**context) -> Dict[str, Any]:
    """
    Fetch activity data for the awaiting response flow from EXT_ACCESS_MANAGEMENT_ACTIVITY table.

    Input (XCom key: 'activity_data_request_resolve', output of check_activity_request_resolve):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'Pending',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 1,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'Pending',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 1,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'awaiting_response_activity_details': {
                'u_status': 'Pending',
                'u_description': 'Access request pending approval',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_task_type': 'User Account Modification',
                'u_identifier': 'AM-20240607123456-1a2b3c4d',
                'u_user_adid': 'esshar01',
                'u_sr_no': 'REQ123456'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='check_activity_request_resolve', key='activity_data_request_resolve')
    activity_identifier = activity_data['data']['request_resolve_initial_data']['identifier']

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
            u_status,
            u_description,
            u_role,
            u_organization,
            u_platform,
            u_manager_email,
            u_user_email,
            u_task_type,
            u_identifier,
            u_user_adid,
            u_sr_no
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_identifier = '{activity_identifier}'
    """

    # Execute query
    result = db_hook.get_row_data(query)

    if not result.get('success') or not result.get('data'):
        hook.error(f"No activity found for identifier: {activity_identifier}")
        raise ValueError(f"No activity found for identifier: {activity_identifier}")

    # Add awaiting response activity details to existing activity_data
    activity_data['data']['awaiting_response_activity_details'] = {
        'u_status': result['data'][0],           # u_status
        'u_description': result['data'][1],      # u_description
        'u_role': result['data'][2],             # u_role
        'u_organization': result['data'][3],     # u_organization
        'u_platform': result['data'][4],         # u_platform
        'u_manager_email': result['data'][5],    # u_manager_email
        'u_user_email': result['data'][6],       # u_user_email
        'u_task_type': result['data'][7],        # u_task_type
        'u_identifier': result['data'][8],       # u_identifier
        'u_user_adid': result['data'][9],        # u_user_adid
        'u_sr_no': result['data'][10]            # u_sr_no
    }

    hook.info(f"Fetched awaiting response activity details for identifier: {activity_identifier}")

    # Push updated activity_data back to XCom
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

    return activity_data

def send_email_communication_awaiting_response_date_flag_1(**context) -> Dict[str, Any]:
    """
    Send email notification for awaiting response when flag_update_date is 1.

    Input (XCom key: 'activity_data_request_resolve', output of fetch_data_activity_for_response):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'Pending',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 1,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'awaiting_response_activity_details': {
                'u_status': 'Pending',
                'u_description': 'Access request pending approval',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_task_type': 'User Account Modification',
                'u_identifier': 'AM-20240607123456-1a2b3c4d',
                'u_user_adid': 'esshar01',
                'u_sr_no': 'REQ123456'
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'Pending',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 1,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'awaiting_response_activity_details': {
                'u_status': 'Pending',
                'u_description': 'Access request pending approval',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_task_type': 'User Account Modification',
                'u_identifier': 'AM-20240607123456-1a2b3c4d',
                'u_user_adid': 'esshar01',
                'u_sr_no': 'REQ123456'
            },
            'awaiting_response_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com'],
                'timestamp': '2024-06-07T12:35:00'
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
        hook.info("Starting email notification for awaiting response (flag 1)")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='awaiting_response_task_group.fetch_data_activity_for_response', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found in XCom"
        hook.error(error_msg)
        return {"success": False, "error": error_msg}

    # Get required data from awaiting_response_activity_details
    activity_details = activity_data['data'].get('awaiting_response_activity_details', {})
    task_type = activity_details.get('u_task_type')
    identifier = activity_details.get('u_identifier')
    user_adid = activity_details.get('u_user_adid')
    sr_no = activity_details.get('u_sr_no')
    user_email = activity_details.get('u_user_email')
    manager_email = activity_details.get('u_manager_email')

    recipients = []
    if user_email:
        recipients.append(user_email)
    if manager_email:
        recipients.append(manager_email)

    if not all([task_type, identifier, user_adid, sr_no]) or not recipients:
        error_msg = "Missing required data for email notification or recipients"
        hook.error(error_msg)
        return {"success": False, "error": error_msg}

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="awaiting_response_notification_email_template_flag"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for awaiting response notification template")

        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Transform data to match template placeholders
        template_data = {
            "task_type": task_type,
            "identifier": identifier,
            "user_adid": user_adid,
            "sr_no": sr_no
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management Service Request | {task_type} | Process Id: {identifier}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        fo_email = os.environ.get("OSCAR_ENABLE_FO_EMAIL", "ericsson.tmus.operate.services.-.fo@t-mobile.com")

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": email_group,
                "notifier_id": [user_email, fo_email]
            })
            hook.info(f"Sent awaiting response notification email for SR {sr_no}")

            # Update activity_data with success information
            activity_data['data']['awaiting_response_communication_result'] = {
                'status': 'Sent',
                'recipients': recipients,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return {
                "success": True,
                "data": result
            }
        except httpx.HTTPError as e:
            error_msg = f"Failed to send awaiting response notification: {str(e)}"
            hook.error(error_msg)
            activity_data['data']['awaiting_response_communication_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending awaiting response notification: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['awaiting_response_communication_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

def sr_activity_update_awaiting_response_date_flag_1(**context) -> Dict[str, Any]:
    """
    Update remarks in EXT_ACCESS_MANAGEMENT_ACTIVITY for follow-up email (flag 1).

    Input (XCom key: 'activity_data_request_resolve', output of send_email_communication_awaiting_response_date_flag_1):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'Pending',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 1,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'awaiting_response_activity_details': {
                'u_status': 'Pending',
                'u_description': 'Access request pending approval',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_task_type': 'User Account Modification',
                'u_identifier': 'AM-20240607123456-1a2b3c4d',
                'u_user_adid': 'esshar01',
                'u_sr_no': 'REQ123456'
            },
            'awaiting_response_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com'],
                'timestamp': '2024-06-07T12:35:00'
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'Pending',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 1,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'awaiting_response_activity_details': {
                'u_status': 'Pending',
                'u_description': 'Access request pending approval',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_task_type': 'User Account Modification',
                'u_identifier': 'AM-20240607123456-1a2b3c4d',
                'u_user_adid': 'esshar01',
                'u_sr_no': 'REQ123456'
            },
            'awaiting_response_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com'],
                'timestamp': '2024-06-07T12:35:00'
            },
            'awaiting_response_update_result_flag_1': {
                'status': 'Updated',
                'remarks': 'Send Follow-up Email. Access request pending approval',
                'timestamp': '2024-06-07T12:36:00'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='awaiting_response_task_group.send_email_communication_awaiting_response_date_flag_1', key='activity_data_request_resolve')
    activity_details = activity_data['data'].get('awaiting_response_activity_details', {})
    identifier = activity_details.get('u_identifier')
    remarks = activity_data['data'].get('request_resolve_initial_data', {}).get('remarks', '')
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)
    new_remarks = f"Send Follow-up Email. {remarks}"
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_remarks = '{new_remarks}'
        WHERE u_identifier = '{identifier}'
    """
    try:
        result = db_hook.execute_query(query)
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity data: {error_msg}")
            activity_data['data']['awaiting_response_update_result_flag_1'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return {"success": False, "error": error_msg}
        hook.info(f"Updated remarks for identifier: {identifier}")
        activity_data['data']['awaiting_response_update_result_flag_1'] = {
            'status': 'Updated',
            'remarks': new_remarks,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": True, "data": {'remarks': new_remarks}}
    except Exception as e:
        error_msg = f"Error occurred while updating remarks: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['awaiting_response_update_result_flag_1'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": False, "error": error_msg}

def send_email_communication_awaiting_response_date_flag_2(**context) -> Dict[str, Any]:
    """
    Send email notification for awaiting response when flag_update_date is 2.

    Input (XCom key: 'activity_data_request_resolve', output of fetch_data_activity_for_response):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'Pending',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 2,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'awaiting_response_activity_details': {
                'u_status': 'Pending',
                'u_description': 'Access request pending approval',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_task_type': 'User Account Modification',
                'u_identifier': 'AM-20240607123456-1a2b3c4d',
                'u_user_adid': 'esshar01',
                'u_sr_no': 'REQ123456'
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'Pending',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 2,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'awaiting_response_activity_details': {
                'u_status': 'Pending',
                'u_description': 'Access request pending approval',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_task_type': 'User Account Modification',
                'u_identifier': 'AM-20240607123456-1a2b3c4d',
                'u_user_adid': 'esshar01',
                'u_sr_no': 'REQ123456'
            },
            'awaiting_response_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com'],
                'timestamp': '2024-06-07T12:35:00'
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
        hook.info("Starting email notification for awaiting response (flag 2)")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='awaiting_response_task_group.fetch_data_activity_for_response', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found in XCom"
        hook.error(error_msg)
        return {"success": False, "error": error_msg}

    # Get required data from awaiting_response_activity_details
    activity_details = activity_data['data'].get('awaiting_response_activity_details', {})
    task_type = activity_details.get('u_task_type')
    identifier = activity_details.get('u_identifier')
    user_adid = activity_details.get('u_user_adid')
    sr_no = activity_details.get('u_sr_no')
    user_email = activity_details.get('u_user_email')
    manager_email = activity_details.get('u_manager_email')

    recipients = []
    if user_email:
        recipients.append(user_email)
    if manager_email:
        recipients.append(manager_email)

    if not all([task_type, identifier, user_adid, sr_no]) or not recipients:
        error_msg = "Missing required data for email notification or recipients"
        hook.error(error_msg)
        return {"success": False, "error": error_msg}

    try:
        # Initialize mapping hook
        mapping_hook = MappingHook()

        # Fetch template using mapping hook
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="awaiting_response_notification_email_template_flag"
        )

        if not mapping_elements:
            raise ValueError("No mapping element found for awaiting response notification template")

        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Transform data to match template placeholders
        template_data = {
            "task_type": task_type,
            "identifier": identifier,
            "user_adid": user_adid,
            "sr_no": sr_no
        }

        # Render content
        rendered_content = Template(template_content).render(**template_data)

        # Send email using notify hook
        subject = f"Access Management Request Update - {sr_no}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        fo_email = os.environ.get("OSCAR_ENABLE_FO_EMAIL", "ericsson.tmus.operate.services.-.fo@ericsson.com")

        try:
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": [email_group, user_email],
                "notifier_id": [fo_email]

            })
            hook.info(f"Sent awaiting response notification email for SR {sr_no}")

            # Update activity_data with success information
            activity_data['data']['awaiting_response_communication_result'] = {
                'status': 'Sent',
                'recipients': recipients,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

            return {
                "success": True,
                "data": result
            }
        except httpx.HTTPError as e:
            error_msg = f"Failed to send awaiting response notification: {str(e)}"
            hook.error(error_msg)
            activity_data['data']['awaiting_response_communication_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending awaiting response notification: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['awaiting_response_communication_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

def sr_activity_update_awaiting_response_date_flag_2(**context) -> Dict[str, Any]:
    """
    Update remarks and status in EXT_ACCESS_MANAGEMENT_ACTIVITY for manual intervention (flag 2).

    Input (XCom key: 'activity_data_request_resolve', output of send_email_communication_awaiting_response_date_flag_2):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'Pending',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 2,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'awaiting_response_activity_details': {
                'u_status': 'Pending',
                'u_description': 'Access request pending approval',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_task_type': 'User Account Modification',
                'u_identifier': 'AM-20240607123456-1a2b3c4d',
                'u_user_adid': 'esshar01',
                'u_sr_no': 'REQ123456'
            },
            'awaiting_response_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com'],
                'timestamp': '2024-06-07T12:35:00'
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
    {
        'data': {
            'process_id': 'AM-20240607123456-1a2b3c4d',
            'worklog_id': 'WL-123456',
            'request_resolve_initial_data': {
                'status': 'Pending',
                'identifier': 'AM-20240607123456-1a2b3c4d',
                'remarks': 'Initial request',
                'task_type': 'New User Account',
                'upn': 'user@uprising.t-mobile.com',
                'flag_update_date': 2,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'awaiting_response_activity_details': {
                'u_status': 'Pending',
                'u_description': 'Access request pending approval',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_task_type': 'User Account Modification',
                'u_identifier': 'AM-20240607123456-1a2b3c4d',
                'u_user_adid': 'esshar01',
                'u_sr_no': 'REQ123456'
            },
            'awaiting_response_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com'],
                'timestamp': '2024-06-07T12:35:00'
            },
            'awaiting_response_update_result_flag_2': {
                'status': 'Updated',
                'remarks': 'Transferred to FO for manual intervention',
                'u_status': 'Completed',
                'timestamp': '2024-06-07T12:36:00'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='awaiting_response_task_group.send_email_communication_awaiting_response_date_flag_2', key='activity_data_request_resolve')
    activity_details = activity_data['data'].get('awaiting_response_activity_details', {})
    identifier = activity_details.get('u_identifier')
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_remarks = 'Transferred to FO for manual intervention',
            u_status = 'Completed'
        WHERE u_identifier = '{identifier}'
    """
    try:
        result = db_hook.execute_query(query)
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity data: {error_msg}")
            activity_data['data']['awaiting_response_update_result_flag_2'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return {"success": False, "error": error_msg}
        hook.info(f"Updated remarks and status for identifier: {identifier}")
        activity_data['data']['awaiting_response_update_result_flag_2'] = {
            'status': 'Updated',
            'remarks': 'Transferred to FO for manual intervention',
            'u_status': 'Completed',
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": True, "data": {'remarks': 'Transferred to FO for manual intervention', 'u_status': 'Completed'}}
    except Exception as e:
        error_msg = f"Error occurred while updating remarks and status: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['awaiting_response_update_result_flag_2'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": False, "error": error_msg}

def fetch_data_activity_for_sr_reminder(**context) -> Dict[str, Any]:
    """
    Fetch activity data for the service request reminder flow from EXT_ACCESS_MANAGEMENT_ACTIVITY table.

    Input (XCom key: 'activity_data_request_resolve', output of check_wo_status):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='check_wo_status', key='activity_data_request_resolve')
    activity_identifier = activity_data['data']['request_resolve_initial_data']['identifier']
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)
    query = f"""
        SELECT
            u_status,
            u_task_type,
            u_sr_no,
            u_description,
            u_role,
            u_organization,
            u_platform,
            u_manager_email,
            u_user_email,
            u_user_name
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_identifier = '{activity_identifier}'
    """
    result = db_hook.get_row_data(query)
    if not result.get('success') or not result.get('data'):
        hook.error(f"No activity found for identifier: {activity_identifier}")
        raise ValueError(f"No activity found for identifier: {activity_identifier}")
    activity_data['data']['sr_reminder_activity_details'] = {
        'u_status': result['data'][0],
        'u_task_type': result['data'][1],
        'u_sr_no': result['data'][2],
        'u_description': result['data'][3],
        'u_role': result['data'][4],
        'u_organization': result['data'][5],
        'u_platform': result['data'][6],
        'u_manager_email': result['data'][7],
        'u_user_email': result['data'][8],
        'u_user_name': result['data'][9]
    }
    hook.info(f"Fetched service request reminder activity details for identifier: {activity_identifier}")
    ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
    return activity_data

def fetch_itsm_approver_sr_reminder(**context) -> Dict[str, Any]:
    """
    Fetch ITSM approver information for service request reminder.

    Input (XCom key: 'activity_data_request_resolve', output of fetch_data_activity_for_sr_reminder):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com']
            }
        }
    }
    """
    logger.debug("Fetching ITSM approver information for service request reminder")
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
        hook.info("Starting ITSM approver fetch for service request reminder")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {
            "success": False,
            "error": f"Error initializing worklog hook: {str(e)}"
        }

    # Get activity data
    activity_data = ti.xcom_pull(task_ids='service_request_reminder_task_group.fetch_data_activity_for_sr_reminder', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    # Get activity details with null checks
    details = activity_data['data'].get('sr_reminder_activity_details', {})
    if not details:
        error_msg = "No activity details found"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    # Get required fields with null checks
    organization = details.get('u_organization')
    platform = details.get('u_platform')
    description = details.get('u_description')

    # Validate required fields
    required_fields = {
        'organization': organization,
        'platform': platform,
        'description': description
    }

    # Check for missing required fields
    missing_fields = [field for field, value in required_fields.items() if not value]
    if missing_fields:
        error_msg = f"Missing required fields: {', '.join(missing_fields)}"
        hook.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }

    hook.info(f"Fetching approvers for organization: {organization}, platform: {platform}, description: {description}")

    # Get connection id from environment variable
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

    # Get database connection
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

    # Query to get approver list and name
    approver_query = f"""
        SELECT u_approver_list, u_approver_name
        FROM EXT_AM_ITSM_APPROVER_ROLE
        WHERE u_company = '{organization}'
        AND (u_role_des = '{description}' OR u_platform = '{platform}')
    """
    logger.debug(f"Fetch ITSM approver list, approver name Query: {approver_query}")

    try:
        # Execute first query
        approver_result = db_hook.get_row_data(approver_query)

        if not approver_result.get('success') or not approver_result.get('data'):
            hook.warning(f"No approvers found for organization: {organization}, platform: {platform}")
            activity_data['data']['approver_info'] = {
                'approver_names': [],
                'approver_emails': []
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return {
                "success": True,
                "data": activity_data['data']['approver_info']
            }

        # Get approver list and name from result
        approver_list_str = approver_result['data'][0]
        approver_names_str = approver_result['data'][1]

        # Handle both semicolon and comma delimiters for approver list
        if ';' in approver_list_str:
            approver_list = approver_list_str.split(';')
        else:
            approver_list = approver_list_str.split(',')
        approver_list = [adid.strip() for adid in approver_list if adid.strip() and adid.lower() != 'null']

        # Handle both semicolon and comma delimiters for approver names
        if ';' in approver_names_str:
            approver_names = approver_names_str.split(';')
        else:
            approver_names = approver_names_str.split(',')
        approver_names = [name.strip() for name in approver_names if name.strip()]

        hook.info(f"Found approver list: {approver_list}")
        hook.info(f"Found approver names: {approver_names}")

        if not approver_list:
            hook.warning("No valid approvers found")
            activity_data['data']['approver_info'] = {
                'approver_names': [],
                'approver_emails': []
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return {
                "success": True,
                "data": activity_data['data']['approver_info']
            }

        # Get emails for each approver
        # Get emails one by one
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

        if not approver_emails:
            error_msg = "No email addresses found for approvers"
            hook.error(error_msg)
            activity_data['data']['approver_result'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data', value=activity_data)
            return {
                "success": False,
                "error": error_msg
            }

        # Update activity_data with approver information
        activity_data['data']['approver_info'] = {
            'approver_names': approver_names,
            'approver_emails': approver_emails
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

        return {
            "success": True,
            "data": {
                "approver_names": approver_names,
                "approver_emails": approver_emails
            }
        }

    except Exception as e:
        error_msg = f"Error fetching approver information: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['approver_info'] = {
            'approver_names': [],
            'approver_emails': []
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {
            "success": False,
            "error": error_msg
        }

def send_email_communication_sr_reminder_date_flag_1(**context) -> dict:
    """
    Send email notification for SR reminder when flag_update_date is 1.

    Input (XCom key: 'activity_data_request_resolve', output of fetch_itsm_approver_sr_reminder):
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
                'flag_update_date': 1,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com']
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
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
                'flag_update_date': 1,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com']
            },
            'sr_reminder_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com', 'approver1@company.com', 'approver2@company.com'],
                'timestamp': '2024-06-07T12:35:00'
            }
        }
    }
    """
    ti = context['ti']
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {"success": False, "error": "No worklog ID found in XCom"}
    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting SR reminder email notification (flag 1)")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {"success": False, "error": f"Error initializing worklog hook: {str(e)}"}
    activity_data = ti.xcom_pull(task_ids='service_request_reminder_task_group.fetch_itsm_approver_sr_reminder', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found in XCom"
        hook.error(error_msg)
        return {"success": False, "error": error_msg}
    details = activity_data['data'].get('sr_reminder_activity_details', {})
    approver_info = activity_data['data'].get('approver_info', {})
    user_email = details.get('u_user_email')
    user_name = details.get('u_user_name')
    manager_email = details.get('u_manager_email')
    approver_emails = approver_info.get('approver_emails', [])
    approver_names = approver_info.get('approver_names', [])
    # First get the request_resolve_initial_data dictionary
    request_resolve_initial_data = activity_data['data'].get('request_resolve_initial_data', {})

    # Then extract individual fields
    status = request_resolve_initial_data.get('status')
    identifier = request_resolve_initial_data.get('identifier')
    remarks = request_resolve_initial_data.get('remarks')
    task_type = request_resolve_initial_data.get('task_type')
    upn = request_resolve_initial_data.get('upn')
    flag_update_date = request_resolve_initial_data.get('flag_update_date')
    creation_date = request_resolve_initial_data.get('creation_date')
    update_date = request_resolve_initial_data.get('update_date')
    sr_no = request_resolve_initial_data.get('sr_no')
    user_email = request_resolve_initial_data.get('user_email')

    try:
        mapping_hook = MappingHook()
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="sr_reminder_notification_email_template_flag"
        )
        if not mapping_elements:
            raise ValueError("No mapping element found for SR reminder notification template (flag 1)")
        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")
        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        template_data = {
            "task_type": task_type,
            "identifier": identifier,
            "user_name": user_name,
            "user_email": user_email,
            "sr_no": sr_no,
            "process_id": identifier
        }
        # template_data = {**details, **approver_info}

        rendered_content = Template(template_content).render(**template_data)
        subject = f"Access Management | {task_type} |Process Id:{identifier}"

        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        fo_email = os.environ.get("OSCAR_ENABLE_FO_EMAIL", "ericsson.tmus.operate.services.-.fo@t-mobile.com")

        # Build notifier_id list: user_email, fo_email, and all approver_emails (no duplicates, all non-empty)
        notifier_id = []
        # if user_email:
        #    notifier_id.append(user_email)

        if fo_email and fo_email not in notifier_id:
            notifier_id.append(fo_email)
        for email in approver_emails:
            if email and email not in notifier_id:
                notifier_id.append(email)
        if manager_email:
            notifier_id.append(manager_email)

        notify_hook = NotifyHook()
        result = notify_hook.send_notification({
            "name": notifier_name,
            "subject": subject,
            "message": rendered_content,
            "cc_notifier_id": [email_group, user_email],
            "notifier_id": notifier_id
        })
        hook.info(f"Sent SR reminder notification email (flag 1) for SR {details.get('u_sr_no', '')}")
        activity_data['data']['sr_reminder_communication_result'] = {
            'status': 'Sent',
            'recipients': notifier_id,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": True, "data": result}
    except Exception as e:
        error_msg = f"Error sending SR reminder notification: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['sr_reminder_communication_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": False, "error": error_msg}

def sr_activity_update_sr_reminder_date_flag_1(**context) -> dict:
    """
    Update remarks in EXT_ACCESS_MANAGEMENT_ACTIVITY for SR reminder follow-up (flag 1).

    Input (XCom key: 'activity_data_request_resolve', output of send_email_communication_sr_reminder_date_flag_1):
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
                'flag_update_date': 1,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com']
            },
            'sr_reminder_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com', 'approver1@company.com', 'approver2@company.com'],
                'timestamp': '2024-06-07T12:35:00'
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
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
                'flag_update_date': 1,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com']
            },
            'sr_reminder_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com', 'approver1@company.com', 'approver2@company.com'],
                'timestamp': '2024-06-07T12:35:00'
            },
            'sr_reminder_update_result_flag_1': {
                'status': 'Updated',
                'remarks': 'Send SR Reminder Follow-up Email. Initial request',
                'timestamp': '2024-06-07T12:36:00'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='service_request_reminder_task_group.send_email_communication_sr_reminder_date_flag_1', key='activity_data_request_resolve')
    details = activity_data['data'].get('sr_reminder_activity_details', {})
    identifier = details.get('u_sr_no')
    remarks = activity_data['data'].get('request_resolve_initial_data', {}).get('remarks', '')
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)
    new_remarks = f"Send SR Reminder Follow-up Email. {remarks}"
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_remarks = '{new_remarks}'
        WHERE u_sr_no = '{identifier}'
    """
    try:
        result = db_hook.execute_query(query)
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity data: {error_msg}")
            activity_data['data']['sr_reminder_update_result_flag_1'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return {"success": False, "error": error_msg}
        hook.info(f"Updated remarks for SR: {identifier}")
        activity_data['data']['sr_reminder_update_result_flag_1'] = {
            'status': 'Updated',
            'remarks': new_remarks,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": True, "data": {'remarks': new_remarks}}
    except Exception as e:
        error_msg = f"Error occurred while updating remarks: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['sr_reminder_update_result_flag_1'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": False, "error": error_msg}

def send_email_communication_sr_reminder_date_flag_2(**context) -> dict:
    """
    Send email notification for SR reminder when flag_update_date is 2.

    Input (XCom key: 'activity_data_request_resolve', output of fetch_itsm_approver_sr_reminder):
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
                'flag_update_date': 2,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com']
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
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
                'flag_update_date': 2,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com']
            },
            'sr_reminder_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com', 'approver1@company.com', 'approver2@company.com'],
                'timestamp': '2024-06-07T12:35:00'
            }
        }
    }
    """
    ti = context['ti']
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    if not worklog_id:
        logger.error("No worklog ID found in XCom")
        return {"success": False, "error": "No worklog ID found in XCom"}
    try:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting SR reminder email notification (flag 2)")
    except Exception as e:
        logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
        return {"success": False, "error": f"Error initializing worklog hook: {str(e)}"}
    activity_data = ti.xcom_pull(task_ids='service_request_reminder_task_group.fetch_itsm_approver_sr_reminder', key='activity_data_request_resolve')
    if not activity_data or 'data' not in activity_data:
        error_msg = "No activity data found in XCom"
        hook.error(error_msg)
        return {"success": False, "error": error_msg}
    details = activity_data['data'].get('sr_reminder_activity_details', {})

    approver_info = activity_data['data'].get('approver_info', {})
    user_email = details.get('u_user_email')
    user_name = details.get('u_user_name')
    manager_email = details.get('u_manager_email')
    approver_emails = approver_info.get('approver_emails', [])
    approver_names = approver_info.get('approver_names', [])

    try:
        mapping_hook = MappingHook()
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="sr_reminder_notification_email_template_flag_2"
        )
        if not mapping_elements:
            raise ValueError("No mapping element found for SR reminder notification template (flag 2)")
        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")
        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")
        # template_data = {**details, **approver_info}

        # First get the request_resolve_initial_data dictionary
        request_resolve_initial_data = activity_data['data'].get('request_resolve_initial_data', {})

        # Then extract individual fields
        status = request_resolve_initial_data.get('status')
        identifier = request_resolve_initial_data.get('identifier')
        remarks = request_resolve_initial_data.get('remarks')
        task_type = request_resolve_initial_data.get('task_type')
        upn = request_resolve_initial_data.get('upn')
        flag_update_date = request_resolve_initial_data.get('flag_update_date')
        creation_date = request_resolve_initial_data.get('creation_date')
        update_date = request_resolve_initial_data.get('update_date')
        sr_no = request_resolve_initial_data.get('sr_no')
        user_email = request_resolve_initial_data.get('user_email')

        subject = f"Access Management | {task_type} |Process Id:{identifier}"

        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        fo_email = os.environ.get("OSCAR_ENABLE_FO_EMAIL", "ericsson.tmus.operate.services.-.fo@ericsson.com")

        template_data = {
            "task_type": task_type,
            "identifier": identifier,
            "user_name": user_name,
            "user_email": user_email,
            "sr_no": sr_no,
            "process_id": identifier
        }

        rendered_content = Template(template_content).render(**template_data)

        # Build notifier_id list: user_email, fo_email, and all approver_emails (no duplicates, all non-empty)
        notifier_id = []
        if user_email:
            notifier_id.append(user_email)
        if fo_email:
            notifier_id.append(fo_email)

        # Build cc_notifier_id: email_group + all approver_emails (no duplicates, all non-empty)
        cc_notifier_id = []
        if email_group:
            cc_notifier_id.append(email_group)
        for email in approver_emails:
            if email and email not in cc_notifier_id:
                cc_notifier_id.append(email)

        if manager_email:
            cc_notifier_id.append(manager_email)

        notify_hook = NotifyHook()
        result = notify_hook.send_notification({
            "name": notifier_name,
            "subject": subject,
            "message": rendered_content,
            "cc_notifier_id": cc_notifier_id,
            "notifier_id": notifier_id
        })
        hook.info(f"Sent SR reminder notification email (flag 2) for SR {details.get('u_sr_no', '')}")
        activity_data['data']['sr_reminder_communication_result'] = {
            'status': 'Sent',
            'recipients': notifier_id,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": True, "data": result}
    except Exception as e:
        error_msg = f"Error sending SR reminder notification: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['sr_reminder_communication_result'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": False, "error": error_msg}

def sr_activity_update_sr_reminder_date_flag_2(**context) -> dict:
    """
    Update remarks and status in EXT_ACCESS_MANAGEMENT_ACTIVITY for SR reminder manual intervention (flag 2).

    Input (XCom key: 'activity_data_request_resolve', output of send_email_communication_sr_reminder_date_flag_2):
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
                'flag_update_date': 2,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com']
            },
            'sr_reminder_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com', 'approver1@company.com', 'approver2@company.com'],
                'timestamp': '2024-06-07T12:35:00'
            }
        }
    }

    Output (XCom key: 'activity_data_request_resolve'):
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
                'flag_update_date': 2,
                'creation_date': '2024-06-07 12:34:56',
                'update_date': '2024-06-07 12:34:56',
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'sr_reminder_activity_details': {
                'u_status': 'SR Submitted',
                'u_task_type': 'New User Account',
                'u_sr_no': 'REQ123456',
                'u_description': 'Initial request',
                'u_role': 'Developer',
                'u_organization': 'IT',
                'u_platform': 'Windows',
                'u_manager_email': 'manager@company.com',
                'u_user_email': 'user@company.com',
                'u_user_name': 'John Doe'
            },
            'approver_info': {
                'approver_names': ['John Approver', 'Jane Approver'],
                'approver_emails': ['approver1@company.com', 'approver2@company.com']
            },
            'sr_reminder_communication_result': {
                'status': 'Sent',
                'recipients': ['user@company.com', 'manager@company.com', 'approver1@company.com', 'approver2@company.com'],
                'timestamp': '2024-06-07T12:35:00'
            },
            'sr_reminder_update_result_flag_2': {
                'status': 'Updated',
                'remarks': 'SR Reminder transferred to FO for manual intervention',
                'u_status': 'Completed',
                'timestamp': '2024-06-07T12:36:00'
            }
        }
    }
    """
    ti = context['ti']
    activity_data = ti.xcom_pull(task_ids='service_request_reminder_task_group.send_email_communication_sr_reminder_date_flag_2', key='activity_data_request_resolve')
    details = activity_data['data'].get('sr_reminder_activity_details', {})
    identifier = details.get('u_sr_no')
    worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
    db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)
    query = f"""
        UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
        SET u_remarks = 'SR Reminder transferred to FO for manual intervention',
            u_status = 'Completed'
        WHERE u_sr_no = '{identifier}'
    """
    try:
        result = db_hook.execute_query(query)
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            hook.error(f"Failed to update activity data: {error_msg}")
            activity_data['data']['sr_reminder_update_result_flag_2'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return {"success": False, "error": error_msg}
        hook.info(f"Updated remarks and status for SR: {identifier}")
        activity_data['data']['sr_reminder_update_result_flag_2'] = {
            'status': 'Updated',
            'remarks': 'SR Reminder transferred to FO for manual intervention',
            'u_status': 'Completed',
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": True, "data": {'remarks': 'SR Reminder transferred to FO for manual intervention', 'u_status': 'Completed'}}
    except Exception as e:
        error_msg = f"Error occurred while updating remarks and status: {str(e)}"
        hook.error(error_msg)
        activity_data['data']['sr_reminder_update_result_flag_2'] = {
            'status': 'Failed',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        return {"success": False, "error": error_msg}

def update_activity_info_service_request_cancel(**context) -> Dict[str, Any]:
    """
    Updates the activity table when a service request is cancelled/rejected.
    This function is called after check_wo_status_task when the SR status indicates rejection.

    Input (from check_wo_status task's activity_data):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'wo_status_data': {
                'flag_wo': 'cancel',
                'sr_status': 'Rejected',
                'sr_no': 'REQ123456'
            }
        }
    }

    Output (extends activity_data with activity_update_sr_cancel while preserving all previous data):
    {
        'data': {
            // ... all previous data ...
            'activity_update_sr_cancel': {
                'status': 'Success',
                'message': 'Activity updated successfully'
            }
        }
    }
    """
    try:
        ti = context['ti']
        activity_data = ti.xcom_pull(task_ids='check_wo_status', key='activity_data_request_resolve')
        if not activity_data or 'data' not in activity_data:
            error_msg = "No activity data found in XCom from check_wo_status task"
            logger.error(error_msg)
            raise ValueError(error_msg)

        worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
        if not worklog_id:
            logger.error("No worklog ID found in XCom")
            return {
                "success": False,
                "error": "No worklog ID found in XCom"
            }

        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting activity update for service request cancellation")

        identifier = activity_data['data']['request_resolve_initial_data']['identifier']
        sr_no = activity_data['data']['request_resolve_initial_data']['sr_no']

        oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
        db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

        query = f"""
            UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY 
            SET u_status = 'SR approval Rejected.',
                u_remarks = 'SR rejected in ITSM. Please check for REQ# {sr_no}'
            WHERE u_identifier = '{identifier}'
        """
        result = db_hook.execute_query(query)

        if not result.get('success'):
            error_msg = f"Failed to update activity: {result.get('message', 'Unknown error')}"
            hook.error(f"Failed to update activity for identifier: {identifier}")
            # Update activity_data with error information before raising exception
            activity_data['data']['activity_update_sr_cancel'] = {
                'status': 'Failed',
                'error': error_msg
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

        hook.info(f"Successfully updated activity for identifier: {identifier}")

        # Add update result to activity data
        activity_data['data']['activity_update_sr_cancel'] = {
            'status': 'Success',
            'message': 'Activity updated successfully'
        }

        # Push updated activity data back to XCom
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

        return activity_data

    except Exception as e:
        error_msg = f"Error in update_activity_info_service_request_cancel: {str(e)}"
        logger.error(error_msg)
        # Ensure activity_data is updated with error information if it exists
        if 'activity_data' in locals() and activity_data and 'data' in activity_data:
            activity_data['data']['activity_update_sr_cancel'] = {
                'status': 'Failed',
                'error': error_msg
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

def send_communication_service_request_cancel(**context) -> Dict[str, Any]:
    """
    Send email notification when a service request is cancelled/rejected.

    This function:
    1. Gets worklog_id from XCom
    2. Gets activity data from update_activity_info_service_request_cancel task
    3. Prepares email content using template from mapping
    4. Sends email notification using notify hook

    Input (from update_activity_info_service_request_cancel task's activity_data_request_resolve):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'wo_status_data': {
                'flag_wo': 'cancel',
                'sr_status': 'Rejected',
                'sr_no': 'REQ123456'
            },
            'activity_update_sr_cancel': {
                'status': 'Success',
                'message': 'Activity updated successfully'
            }
        }
    }

    Output (extends activity_data with communication_result_sr_cancel):
    {
        'data': {
            // ... all previous data ...
            'communication_result_sr_cancel': {
                'status': 'Sent',
                'recipients': ['user@company.com'],
                'timestamp': '2024-06-07T12:35:00',
                'details': 'Service request cancellation notification sent successfully'
            }
        }
    }

    In case of errors:
    {
        'data': {
            // ... all previous data ...
            'communication_result_sr_cancel': {
                'status': 'Failed',
                'error': 'Failed to send notification: Connection timeout',
                'timestamp': '2024-06-07T12:35:00'
            }
        }
    }
    """
    try:
        ti = context['ti']
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
            hook.info("Starting service request cancellation notification")
        except Exception as e:
            logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
            return {
                "success": False,
                "error": f"Error initializing worklog hook: {str(e)}"
            }

        # Get activity data from previous task
        activity_data = ti.xcom_pull(task_ids='wo_cancel_task_group.update_activity_info_service_request_cancel', key='activity_data_request_resolve')
        if not activity_data or 'data' not in activity_data:
            error_msg = "No activity data found from previous task"
            hook.error(error_msg)
            raise ValueError(error_msg)

        # Get required data from activity_data
        request_resolve_data = activity_data['data'].get('request_resolve_initial_data', {})
        user_email = request_resolve_data.get('user_email')
        identifier = request_resolve_data.get('identifier')
        sr_no = request_resolve_data.get('sr_no')
        task_type = request_resolve_data.get('task_type')

        if not user_email:
            hook.warning("No recipient email address found")
            activity_data['data']['communication_result_sr_cancel'] = {
                'status': 'Not Sent',
                'recipients': [],
                'timestamp': datetime.now().isoformat(),
                'details': 'No recipient email address found'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return activity_data

        # Get mapping hook for email template
        mapping_hook = MappingHook()
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="service_request_cancel_notification_email_template"
        )

        if not mapping_elements:
            hook.error("No mapping element found for service request cancellation notification template")
            raise ValueError("No mapping element found for service request cancellation notification template")

        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Prepare template variables
        template_vars = {
            'identifier': identifier,
            'sr_no': sr_no,
            'task_type': task_type
        }

        # Render email template
        rendered_content = Template(template_content).render(**template_vars)
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        fo_email = os.environ.get("OSCAR_ENABLE_FO_EMAIL", "ericsson.tmus.operate.services.-.fo@t-mobile.com")
        subject = f"Access Management | {task_type} |Process Id:{identifier}"

        try:
            # Send email using notify hook
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": email_group,
                "notifier_id": [user_email, fo_email]
            })

            hook.info(f"Sent service request cancellation notification to {user_email}")

            # Update activity_data with success information
            activity_data['data']['communication_result_sr_cancel'] = {
                'status': 'Sent',
                'recipients': [user_email],
                'timestamp': datetime.now().isoformat(),
                'details': 'Service request cancellation notification sent successfully',
                'result': result
            }

            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return activity_data

        except httpx.HTTPError as e:
            error_msg = f"Failed to send service request cancellation notification: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['communication_result_sr_cancel'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat(),
                'details': 'Failed to send service request cancellation notification'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending service request cancellation notification: {str(e)}"
        hook.error(error_msg)
        # Ensure activity_data is updated with error information if it exists
        if 'activity_data' in locals() and activity_data and 'data' in activity_data:
            activity_data['data']['communication_result_sr_cancel'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat(),
                'details': 'Failed to send service request cancellation notification'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

def update_activity_info_service_request_wo_fetch_fail(**context) -> Dict[str, Any]:
    """
    Updates the activity table when a work order fetch fails.
    This function is called after check_wo_status_task when the WO fetch indicates failure.

    Input (from check_wo_status task's activity_data):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'wo_status_data': {
                'flag_wo': 'false',
                'sr_status': 'Assigned',
                'sr_no': 'REQ123456',
                'error_flag': True,
                'error_message': 'Failed to fetch work order'
            }
        }
    }

    Output (extends activity_data with activity_update_sr_wo_fetch_fail while preserving all previous data):
    {
        'data': {
            // ... all previous data ...
            'activity_update_sr_wo_fetch_fail': {
                'status': 'Success',
                'message': 'Activity updated successfully'
            }
        }
    }
    """
    try:
        ti = context['ti']
        activity_data = ti.xcom_pull(task_ids='check_wo_status', key='activity_data_request_resolve')
        if not activity_data or 'data' not in activity_data:
            error_msg = "No activity data found in XCom from check_wo_status task"
            logger.error(error_msg)
            raise ValueError(error_msg)

        worklog_id = ti.xcom_pull(task_ids='create_worklog', key='worklog_id')
        if not worklog_id:
            logger.error("No worklog ID found in XCom")
            return {
                "success": False,
                "error": "No worklog ID found in XCom"
            }

        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Starting activity update for work order fetch failure")

        identifier = activity_data['data']['request_resolve_initial_data']['identifier']
        sr_no = activity_data['data']['request_resolve_initial_data']['sr_no']
        error_message = activity_data['data']['wo_status_data'].get('error_message', 'Unknown error')

        oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
        db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

        query = f"""
            UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY 
            SET u_status = 'Error in ITSM Process',
                u_remarks = 'Error in WO Fetch process for REQ# {sr_no}. {error_message}'
            WHERE u_identifier = '{identifier}'
        """
        result = db_hook.execute_query(query)

        if not result.get('success'):
            error_msg = f"Failed to update activity: {result.get('message', 'Unknown error')}"
            hook.error(f"Failed to update activity for identifier: {identifier}")
            # Update activity_data with error information before raising exception
            activity_data['data']['activity_update_sr_wo_fetch_fail'] = {
                'status': 'Failed',
                'error': error_msg
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

        hook.info(f"Successfully updated activity for identifier: {identifier}")

        # Add update result to activity data
        activity_data['data']['activity_update_sr_wo_fetch_fail'] = {
            'status': 'Success',
            'message': 'Activity updated successfully'
        }

        # Push updated activity data back to XCom
        ti.xcom_push(key='activity_data_request_resolve', value=activity_data)

        return activity_data

    except Exception as e:
        error_msg = f"Error in update_activity_info_service_request_wo_fetch_fail: {str(e)}"
        logger.error(error_msg)
        # Ensure activity_data is updated with error information if it exists
        if 'activity_data' in locals() and activity_data and 'data' in activity_data:
            activity_data['data']['activity_update_sr_wo_fetch_fail'] = {
                'status': 'Failed',
                'error': error_msg
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

def send_communication_service_request_request_wo_fetch_fail(**context) -> Dict[str, Any]:
    """
    Send email notification when a work order fetch fails.

    This function:
    1. Gets worklog_id from XCom
    2. Gets activity data from update_activity_info_service_request_wo_fetch_fail task
    3. Prepares email content using template from mapping
    4. Sends email notification using notify hook

    Input (from update_activity_info_service_request_wo_fetch_fail task's activity_data_request_resolve):
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
                'sr_no': 'REQ123456',
                'user_email': 'user@company.com'
            },
            'wo_status_data': {
                'flag_wo': 'false',
                'sr_status': 'Assigned',
                'sr_no': 'REQ123456',
                'error_flag': True,
                'error_message': 'Failed to fetch work order'
            },
            'activity_update_sr_wo_fetch_fail': {
                'status': 'Success',
                'message': 'Activity updated successfully'
            }
        }
    }

    Output (extends activity_data with communication_result_sr_wo_fetch_fail):
    {
        'data': {
            // ... all previous data ...
            'communication_result_sr_wo_fetch_fail': {
                'status': 'Sent',
                'recipients': ['user@company.com'],
                'timestamp': '2024-06-07T12:35:00',
                'details': 'Work order fetch failure notification sent successfully'
            }
        }
    }

    In case of errors:
    {
        'data': {
            // ... all previous data ...
            'communication_result_sr_wo_fetch_fail': {
                'status': 'Failed',
                'error': 'Failed to send notification: Connection timeout',
                'timestamp': '2024-06-07T12:35:00'
            }
        }
    }
    """
    try:
        ti = context['ti']
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
            hook.info("Starting work order fetch failure notification")
        except Exception as e:
            logger.error(f"[Worklog: {worklog_id}] Error initializing worklog hook: {str(e)}")
            return {
                "success": False,
                "error": f"Error initializing worklog hook: {str(e)}"
            }

        # Get activity data from previous task
        activity_data = ti.xcom_pull(task_ids='wo_fetch_fail_task_group.update_activity_info_service_request_wo_fetch_fail', key='activity_data_request_resolve')
        if not activity_data or 'data' not in activity_data:
            error_msg = "No activity data found from previous task"
            hook.error(error_msg)
            raise ValueError(error_msg)

        # Get required data from activity_data
        request_resolve_data = activity_data['data'].get('request_resolve_initial_data', {})
        user_email = request_resolve_data.get('user_email')
        identifier = request_resolve_data.get('identifier')
        sr_no = request_resolve_data.get('sr_no')
        task_type = request_resolve_data.get('task_type')
        error_message = activity_data['data']['wo_status_data'].get('error_message', 'Unknown error')

        if not user_email:
            hook.warning("No recipient email address found")
            activity_data['data']['communication_result_sr_wo_fetch_fail'] = {
                'status': 'Not Sent',
                'recipients': [],
                'timestamp': datetime.now().isoformat(),
                'details': 'No recipient email address found'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return activity_data

        # Get mapping hook for email template
        mapping_hook = MappingHook()
        mapping_elements = mapping_hook.list_mapping_elements(
            mapping_name="access_management_request_resolve",
            mapping_namespace_name="access_management_enable",
            mapping_key="service_request_wo_fetch_fail_notification_email_template"
        )

        if not mapping_elements:
            hook.error("No mapping element found for work order fetch failure notification template")
            raise ValueError("No mapping element found for work order fetch failure notification template")

        template_element = mapping_elements[0]
        if not isinstance(template_element, dict):
            raise ValueError("Invalid mapping element format")

        template_content = template_element.get("value")
        if not template_content:
            raise ValueError("No template content found in mapping element")

        # Prepare template variables
        template_vars = {
            'identifier': identifier,
            'sr_no': sr_no,
            'task_type': task_type,
            'error_message': error_message
        }

        # Render email template
        rendered_content = Template(template_content).render(**template_vars)
        subject = f"Access Management | Error Fetch WO | Process Id: {identifier}"
        notifier_name = os.environ.get("OSCAR_ENABLE_NOTFIER_NAME", "oscar_notifier_email")
        email_group = os.environ.get("OSCAR_ENABLE_EMAIL_GROUP", "am_email_group")
        fo_email = os.environ.get("OSCAR_ENABLE_FO_EMAIL", "ericsson.tmus.operate.services.-.fo@t-mobile.com")

        try:
            # Send email using notify hook
            notify_hook = NotifyHook()
            result = notify_hook.send_notification({
                "name": notifier_name,
                "subject": subject,
                "message": rendered_content,
                "cc_notifier_id": email_group,
                "notifier_id": [user_email, fo_email]
            })

            hook.info(f"Sent work order fetch failure notification to {user_email}")

            # Update activity_data with success information
            activity_data['data']['communication_result_sr_wo_fetch_fail'] = {
                'status': 'Sent',
                'recipients': [user_email],
                'timestamp': datetime.now().isoformat(),
                'details': 'Work order fetch failure notification sent successfully',
                'result': result
            }

            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            return activity_data

        except httpx.HTTPError as e:
            error_msg = f"Failed to send work order fetch failure notification: {str(e)}"
            hook.error(error_msg)
            # Update activity_data with error information
            activity_data['data']['communication_result_sr_wo_fetch_fail'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat(),
                'details': 'Failed to send work order fetch failure notification'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
            raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Error sending work order fetch failure notification: {str(e)}"
        hook.error(error_msg)
        # Ensure activity_data is updated with error information if it exists
        if 'activity_data' in locals() and activity_data and 'data' in activity_data:
            activity_data['data']['communication_result_sr_wo_fetch_fail'] = {
                'status': 'Failed',
                'error': error_msg,
                'timestamp': datetime.now().isoformat(),
                'details': 'Failed to send work order fetch failure notification'
            }
            ti.xcom_push(key='activity_data_request_resolve', value=activity_data)
        raise ValueError(error_msg)

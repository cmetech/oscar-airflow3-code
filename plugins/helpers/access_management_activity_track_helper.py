from datetime import datetime
from typing import Dict, Any
import os
import re
import logging
from hooks.worklog_hook import WorkLogHook
from hooks.access_management_db_hook import AccessManagementSQLHook
import json

logger = logging.getLogger(__name__)

def update_or_create_request_activity(**context) -> Dict[str, Any]:
    """
    Updates or creates a record in the EXT_ACCESS_MANAGEMENT_ACTIVITY table based on the email subject and configuration.
    This function handles all activity tracking for access management requests.

    Args:
        **context: Airflow context containing configuration and task instance

    Returns:
        Dict containing success status and activity details
    """
    try:
        oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")

        # Get the worklog ID from XCom
        try:
            worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
            if not worklog_id:
                logger.error("No worklog ID found in XCom")
                return {
                    "success": False,
                    "error": "No worklog ID found in XCom"
                }
        except Exception as e:
            logger.error(f"Error retrieving worklog ID: {str(e)}")
            return {
                "success": False,
                "error": f"Error retrieving worklog ID: {str(e)}"
            }

        # Initialize hooks
        try:
            hook = WorkLogHook()
            hook.set_worklog_id(worklog_id)
            db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)
        except Exception as e:
            logger.error(f"Error initializing hooks: {str(e)}")
            return {
                "success": False,
                "error": f"Error initializing hooks: {str(e)}"
            }

        # Get configuration from context
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

        # Check if email_subject exists in conf
        if 'email_subject' not in conf:
            hook.error("No email subject found in configuration")
            logger.error("No email subject found in configuration")
            return {
                "success": False,
                "error": "No email subject found in configuration"
            }

        email_subject = conf['email_subject']
        if not email_subject:
            hook.error("Email subject is empty")
            logger.error("Email subject is empty")
            return {
                "success": False,
                "error": "Email subject is empty"
            }

        hook.info(f"Processing request with subject: {email_subject}")
        logger.info(f"Processing request with subject: {email_subject}")

        # Generate a unique identifier for new requests
        unique_number = None
        process_id = None
        try:
            # Generate timestamp in format yyyyMMddHHmmssSSS this format is nedded for AD scripts to work
            timestamp_uid = datetime.now().strftime('%Y%m%d%H%M%S%f')[:17]  # Get first 17 digits for yyyyMMddHHmmssSSS
            unique_number = timestamp_uid
            current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"Error generating unique identifier: {str(e)}")
            return {
                "success": False,
                "error": f"Error generating unique identifier: {str(e)}"
            }

        # Initialize variables
        is_update = False
        connection_id = None
        u_task_type = ""
        activity_state = None

        hook.info("Email subject: " + email_subject)
        logger.info("Email subject: " + email_subject)

        # Access Management Request : Update Request
        if "Process Id:" in email_subject:
            try:
                # Match exactly 17 digits for the process ID
                process_id_match = re.search(r'Process Id: (\d{17})', email_subject)
                if not process_id_match:
                    hook.error("Invalid process ID format in email subject - expected 17 digits")
                    logger.error("Invalid process ID format in email subject - expected 17 digits")
                    return {
                        "success": False,
                        "error": "Invalid process ID format in email subject - expected 17 digits"
                    }
                process_id = process_id_match.group(1)
                is_update = True

                hook.info(f"Process ID found: {process_id}")
                logger.info(f"Process ID found: {process_id}")
                hook.info(f"Updating existing activity record with process ID: {process_id}")
                logger.info(f"Updating existing activity record with process ID: {process_id}")

                result = _handle_update_request(hook, db_hook, process_id, email_subject, conf, current_timestamp)
                if not result.get('success'):
                    logger.error(f"Error processing update request: {result.get('error')}")
                    return result

                u_task_type = result.get('task_type')
                try:
                    hook.add_metadata([
                        {"key": "task_type", "value": u_task_type},
                        {"key": "process_id", "value": process_id},
                        {"key": "email_subject", "value": email_subject},
                        {"key": "dag_config", "value": json.dumps(conf)}
                    ])
                except Exception as metadata_error:
                    logger.warning(f"Failed to add metadata to worklog, but continuing with main process: {str(metadata_error)}")
                    # Continue with main process - metadata failure should not impact the core functionality
                activity_state = result.get('activity_state')

            except Exception as e:
                hook.error(f"Error processing update request: {str(e)}")
                logger.error(f"Error processing update request: {str(e)}")
                return {
                    "success": False,
                    "error": f"Error processing update request: {str(e)}"
                }

        # Access Management Request : New Request
        else:
            try:
                hook.info("No process ID found, treating as new request")
                logger.info("No process ID found, treating as new request")
                process_id = unique_number

                result = _handle_new_request(hook, db_hook, unique_number, email_subject, conf, current_timestamp)
                if not result.get('success'):
                    return result

                u_task_type = result.get('task_type')
                try:
                    hook.add_metadata([
                        {"key": "task_type", "value": u_task_type},
                        {"key": "process_id", "value": process_id},
                        {"key": "email_subject", "value": email_subject},
                        {"key": "dag_config", "value": json.dumps(conf)}
                    ])
                except Exception as metadata_error:
                    logger.warning(f"Failed to add metadata to worklog, but continuing with main process: {str(metadata_error)}")
                    # Continue with main process - metadata failure should not impact the core functionality
                activity_state = result.get('activity_state')

            except Exception as e:
                hook.error(f"Error processing new request: {str(e)}")
                logger.error(f"Error processing new request: {str(e)}")
                return {
                    "success": False,
                    "error": f"Error processing new request: {str(e)}"
                }

        # Create and push activity data to XCom
        activity_data = {
            'data': {
                'process_id': process_id,
                'worklog_id': worklog_id
            }
        }
        context['ti'].xcom_push(key='activity_data', value=activity_data)

        return {
            "success": True,
            "data": {
                "email_subject": email_subject,
                "connection_id": connection_id,
                "process_id": process_id,
                "task_type": u_task_type,
                "activity_state": activity_state,
                "is_update": is_update,
                "activity_processed": True,
                "process_time": datetime.now().isoformat()
            }
        }

    except Exception as e:
        hook.error(f"Error in update_or_create_request_activity: {str(e)}")
        logger.error(f"Error in update_or_create_request_activity: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


def _handle_update_request(hook: WorkLogHook, db_hook: AccessManagementSQLHook, process_id: str, email_subject: str, conf: dict, current_timestamp: str) -> Dict[str, Any]:
    """
    Handles updating an existing record in EXT_ACCESS_MANAGEMENT_ACTIVITY table.
    Contains the exact same update logic as before.
    """
    try:
        update_fields = []
        update_values = []
        u_task_type = ""
        activity_state = "Data Updated"

        if "User Account Password Reset".lower() in email_subject.lower() or "User Password Reset".lower() in email_subject.lower():
            u_task_type = "User Account Password Reset"

            if conf.get('user_ad_id'):
                update_fields.append("u_user_adid")
                update_values.append(f"'{conf.get('user_ad_id')}'")

            if conf.get('user_full_name'):
                update_fields.append("u_user_name")
                update_values.append(f"'{conf.get('user_full_name')}'")

            if conf.get('user_email_id'):
                update_fields.append("u_user_email")
                update_values.append(f"'{conf.get('user_email_id')}'")

            if conf.get('official_mobile_no'):
                update_fields.append("u_mobile_no")
                update_values.append(f"'{conf.get('official_mobile_no')}'")

            if conf.get('organization'):
                if "Tmobile" in conf.get('organization', ''):
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('T-Mobile')}'")
                else:
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('organization')}'")

            if conf.get('environment'):
                update_fields.append("u_env")
                update_values.append(f"'{conf.get('environment')}'")

            if conf.get('comments'):
                update_fields.append("u_comments")
                update_values.append(f"'{conf.get('comments')}'")

        elif "Service Account Password Reset".lower() in email_subject.lower() or "ServiceAc Passwd Reset".lower() in email_subject.lower() or "ServiceAc Password Reset".lower() in email_subject.lower():
            u_task_type = "ServiceAc Passwd Reset"

            if conf.get('service_account_full_name'):
                update_fields.append("u_svc_name")
                update_values.append(f"'{conf.get('service_account_full_name')}'")

            if conf.get('service_account_owner_name'):
                update_fields.append("u_user_name")
                update_values.append(f"'{conf.get('service_account_owner_name')}'")

            if conf.get('service_account_owner_email_id'):
                update_fields.append("u_user_email")
                update_values.append(f"'{conf.get('service_account_owner_email_id')}'")

            if conf.get('service_account_owner_uprising_id'):
                update_fields.append("u_user_adid")
                update_values.append(f"'{conf.get('service_account_owner_uprising_id')}'")

            if conf.get('organization'):
                if "Tmobile" in conf.get('organization', ''):
                    update_fields.append("u_organization")
                    update_values.append("'T-Mobile'")
                else:
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('organization')}'")

            if conf.get('environment'):
                update_fields.append("u_env")
                update_values.append(f"'{conf.get('environment')}'")

            if conf.get('owner_group_email'):
                update_fields.append("u_svc_owner_email")
                update_values.append(f"'{conf.get('owner_group_email')}'")

            if conf.get('business_justification'):
                update_fields.append("u_business_justification")
                update_values.append(f"'{conf.get('business_justification')}'")

            if conf.get('platform'):
                platform_raw = conf.get('platform', '')
                match = re.match(r'(.*)\s(\S+)$', platform_raw.strip())
                if match:
                    u_platform = match.group(1).strip()
                else:
                    u_platform = platform_raw.strip()
                update_fields.append("u_platform")
                update_values.append(f"'{u_platform}'")

        elif "New User Account Creation".lower() in email_subject.lower() or "New User Account".lower() in email_subject.lower():
            u_task_type = "New User Account"

            if conf.get('user_full_name'):
                update_fields.append("u_user_name")
                update_values.append(f"'{conf.get('user_full_name')}'")

            if conf.get('user_ntid_signum'):
                update_fields.append("u_user_ntid_signum")
                update_values.append(f"'{conf.get('user_ntid_signum')}'")

            if conf.get('user_email_id'):
                update_fields.append("u_user_email")
                update_values.append(f"'{conf.get('user_email_id')}'")

            if conf.get('organization'):
                if "Tmobile" in conf.get('organization', ''):
                    update_fields.append("u_organization")
                    update_values.append("'T-Mobile'")
                else:
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('organization')}'")

            if conf.get('manager_ad_id'):
                update_fields.append("u_manager_adid")
                update_values.append(f"'{conf.get('manager_ad_id')}'")
                # Get manager info from EXT_AM_ITSM_USER_LIST
                u_manager_name, u_manager_email = db_hook.get_info_by_adid(conf.get('manager_ad_id'))
                update_fields.append("u_manager_name")
                update_values.append(f"'{u_manager_name}'")
                update_fields.append("u_manager_email")
                update_values.append(f"'{u_manager_email}'")

            if conf.get('team_name'):
                update_fields.append("u_team_name")
                update_values.append(f"'{conf.get('team_name')}'")

            if conf.get('business_justification'):
                update_fields.append("u_business_justification")
                update_values.append(f"'{conf.get('business_justification')}'")

            if conf.get('reference_ad_id'):
                update_fields.append("u_ref_adid")
                update_values.append(f"'{conf.get('reference_ad_id')}'")

            if conf.get('environment'):
                update_fields.append("u_env")
                update_values.append(f"'{conf.get('environment')}'")

            if conf.get('description'):
                update_fields.append("u_description")
                update_values.append(f"'{conf.get('description')}'")
                # Get role and platform from EXT_AM_ITSM_ROLE_PLATFORM_MAPPING
                u_role, u_platform = db_hook.get_role_platform(conf.get('description'), conf.get('organization', ''))
                update_fields.append("u_role")
                update_values.append(f"'{u_role}'")
                update_fields.append("u_platform")
                update_values.append(f"'{u_platform}'")

            if conf.get('official_mobile_no'):
                update_fields.append("u_mobile_no")
                update_values.append(f"'{conf.get('official_mobile_no')}'")

        elif "New Service Account Creation".lower() in email_subject.lower() or "New Service Account".lower() in email_subject.lower():
            u_task_type = "New Service Account"

            if conf.get('service_account_full_name'):
                update_fields.append("u_svc_name")
                update_values.append(f"'{conf.get('service_account_full_name')}'")

            if conf.get('service_account_owner_name'):
                update_fields.append("u_user_name")
                update_values.append(f"'{conf.get('service_account_owner_name')}'")

            if conf.get('service_account_owner_email_id'):
                update_fields.append("u_user_email")
                update_values.append(f"'{conf.get('service_account_owner_email_id')}'")

            if conf.get('official_mobile_no'):
                update_fields.append("u_mobile_no")
                update_values.append(f"'{conf.get('official_mobile_no')}'")

            if conf.get('service_account_owner_uprising_id'):
                update_fields.append("u_user_adid")
                update_values.append(f"'{conf.get('service_account_owner_uprising_id')}'")

            if conf.get('owner_group_email'):
                update_fields.append("u_svc_owner_email")
                update_values.append(f"'{conf.get('owner_group_email')}'")

            if conf.get('organization'):
                if "Tmobile" in conf.get('organization', ''):
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('T-Mobile')}'")
                else:
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('organization')}'")

            if conf.get('manager_ad_id'):
                update_fields.append("u_manager_adid")
                update_values.append(f"'{conf.get('manager_ad_id')}'")
                # Get manager info from EXT_AM_ITSM_USER_LIST
                u_manager_name, u_manager_email = db_hook.get_info_by_adid(conf.get('manager_ad_id'))
                update_fields.append("u_manager_name")
                update_values.append(f"'{u_manager_name}'")
                update_fields.append("u_manager_email")
                update_values.append(f"'{u_manager_email}'")

            if conf.get('owner_ntid'):
                update_fields.append("u_svc_owner_ntid_signum")
                update_values.append(f"'{conf.get('owner_ntid')}'")

            if conf.get('environment'):
                update_fields.append("u_env")
                update_values.append(f"'{conf.get('environment')}'")

            if conf.get('team_name'):
                update_fields.append("u_team_name")
                update_values.append(f"'{conf.get('team_name')}'")

            if conf.get('business_justification'):
                update_fields.append("u_business_justification")
                update_values.append(f"'{conf.get('business_justification')}'")

            if conf.get('description'):
                update_fields.append("u_description")
                update_values.append(f"'{conf.get('description')}'")

            if conf.get('intended_use'):
                update_fields.append("u_intended_use")
                update_values.append(f"'{conf.get('intended_use')}'")

        elif "Service Account Modification".lower() in email_subject.lower() or "Service Account Modify".lower() in email_subject.lower():
            u_task_type = "Service Account Modify"

            if conf.get('service_account_full_name'):
                update_fields.append("u_svc_name")
                update_values.append(f"'{conf.get('service_account_full_name')}'")

            if conf.get('service_account_owner_name'):
                update_fields.append("u_user_name")
                update_values.append(f"'{conf.get('service_account_owner_name')}'")

            if conf.get('service_account_owner_email_id'):
                update_fields.append("u_user_email")
                update_values.append(f"'{conf.get('service_account_owner_email_id')}'")

            if conf.get('service_account_owner_uprising_id'):
                update_fields.append("u_user_adid")
                update_values.append(f"'{conf.get('service_account_owner_uprising_id')}'")

            # Manmager details
            if conf.get('manager_ad_id'):
                update_fields.append("u_manager_adid")
                update_values.append(f"'{conf.get('manager_ad_id')}'")
                # Get manager info from EXT_AM_ITSM_USER_LIST
                u_manager_name, u_manager_email = db_hook.get_info_by_adid(conf.get('manager_ad_id'))

                if u_manager_name:
                    update_fields.append("u_manager_name")
                    update_values.append(f"'{u_manager_name}'")
                if u_manager_email:
                    update_fields.append("u_manager_email")
                    update_values.append(f"'{u_manager_email}'")

            if conf.get('organization'):
                if "Tmobile" in conf.get('organization', ''):
                    update_fields.append("u_organization")
                    update_values.append("'T-Mobile'")
                else:
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('organization')}'")

            if conf.get('environment'):
                update_fields.append("u_env")
                update_values.append(f"'{conf.get('environment')}'")

            if conf.get('owner_group_email'):
                update_fields.append("u_svc_owner_email")
                update_values.append(f"'{conf.get('owner_group_email')}'")

            if conf.get('team_name'):
                update_fields.append("u_team_name")
                update_values.append(f"'{conf.get('team_name')}'")

            if conf.get('business_justification'):
                update_fields.append("u_business_justification")
                update_values.append(f"'{conf.get('business_justification')}'")

            if conf.get('environment'):
                update_fields.append("u_env")
                update_values.append(f"'{conf.get('environment')}'")

            if conf.get('action'):
                update_fields.append("u_modification_action")
                update_values.append(f"'{conf.get('action')}'")

            if conf.get('description'):
                update_fields.append("u_description")
                update_values.append(f"'{conf.get('description')}'")

            # Get role and platform from EXT_AM_ITSM_ROLE_PLATFORM_MAPPING
            u_role, u_platform = db_hook.get_role_platform(conf.get('description'), conf.get('organization', ''))
            if u_role:
                update_fields.append("u_role")
                update_values.append(f"'{u_role}'")
            if u_platform:
                update_fields.append("u_platform")
                update_values.append(f"'{u_platform}'")

            if conf.get('intended_use'):
                update_fields.append("u_intended_use")
                update_values.append(f"'{conf.get('intended_use')}'")

        elif "User Account Modification".lower() in email_subject.lower() or "User Account Modify".lower() in email_subject.lower():
            u_task_type = "User Account Modify"

            if conf.get('user_full_name'):
                update_fields.append("u_user_name")
                update_values.append(f"'{conf.get('user_full_name')}'")

            if conf.get('user_email_id'):
                update_fields.append("u_user_email")
                update_values.append(f"'{conf.get('user_email_id')}'")

            if conf.get('user_uprising_id'):
                update_fields.append("u_user_adid")
                update_values.append(f"'{conf.get('user_uprising_id')}'")

            if conf.get('organization'):
                if "Tmobile" in conf.get('organization', ''):
                    update_fields.append("u_organization")
                    update_values.append("'T-Mobile'")
                else:
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('organization')}'")

            if conf.get('manager_name'):
                update_fields.append("u_manager_name")
                update_values.append(f"'{conf.get('manager_name')}'")

            if conf.get('manager_email_id'):
                update_fields.append("u_manager_email")
                update_values.append(f"'{conf.get('manager_email_id')}'")

            if conf.get('manager_ad_id'):
                update_fields.append("u_manager_adid")
                update_values.append(f"'{conf.get('manager_ad_id')}'")
                # Get manager info from EXT_AM_ITSM_USER_LIST if name/email not provided
                if not conf.get('manager_name') or not conf.get('manager_email_id'):
                    u_manager_name, u_manager_email = db_hook.get_info_by_adid(conf.get('manager_ad_id'))
                    if not conf.get('manager_name'):
                        update_fields.append("u_manager_name")
                        update_values.append(f"'{u_manager_name}'")
                    if not conf.get('manager_email_id'):
                        update_fields.append("u_manager_email")
                        update_values.append(f"'{u_manager_email}'")

            if conf.get('reference_ad_id'):
                update_fields.append("u_ref_adid")
                update_values.append(f"'{conf.get('reference_ad_id')}'")

            if conf.get('environment'):
                update_fields.append("u_env")
                update_values.append(f"'{conf.get('environment')}'")

            if conf.get('team_name'):
                update_fields.append("u_team_name")
                update_values.append(f"'{conf.get('team_name')}'")

            if conf.get('business_justification'):
                update_fields.append("u_business_justification")
                update_values.append(f"'{conf.get('business_justification')}'")

            if conf.get('description'):
                update_fields.append("u_description")
                update_values.append(f"'{conf.get('description')}'")

            if conf.get('action'):
                update_fields.append("u_modification_action")
                update_values.append(f"'{conf.get('action')}'")

            if conf.get('official_mobile_no'):
                update_fields.append("u_mobile_no")
                update_values.append(f"'{conf.get('official_mobile_no')}'")

            # Get role and platform from EXT_AM_ITSM_ROLE_PLATFORM_MAPPING
            u_role, u_platform = db_hook.get_role_platform(conf.get('description'), conf.get('organization', ''))
            if u_role:
                update_fields.append("u_role")
                update_values.append(f"'{u_role}'")
            if u_platform:
                update_fields.append("u_platform")
                update_values.append(f"'{u_platform}'")

        elif "Disable Service Account".lower() in email_subject.lower():
            u_task_type = "Disable Service Account"

            if conf.get('service_account_full_name'):
                update_fields.append("u_svc_name")
                update_values.append(f"'{conf.get('service_account_full_name')}'")

            if conf.get('service_account_owner_name'):
                update_fields.append("u_user_name")
                update_values.append(f"'{conf.get('service_account_owner_name')}'")

            if conf.get('organization'):
                if "Tmobile" in conf.get('organization', ''):
                    update_fields.append("u_organization")
                    update_values.append("'T-Mobile'")
                else:
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('organization')}'")

            if conf.get('service_account_owner_email_id'):
                update_fields.append("u_user_name")
                update_values.append(f"'{conf.get('service_account_owner_email_id')}'")

            if conf.get('service_account_owner_ad_id'):
                update_fields.append("u_user_adid")
                update_values.append(f"'{conf.get('service_account_owner_ad_id')}'")

            if conf.get('manager_ad_id'):
                update_fields.append("u_manager_adid")
                update_values.append(f"'{conf.get('manager_ad_id')}'")
                # Get manager info from EXT_AM_ITSM_USER_LIST
                u_manager_name, u_manager_email = db_hook.get_info_by_adid(conf.get('manager_ad_id'))
                update_fields.append("u_manager_name")
                update_values.append(f"'{u_manager_name}'")
                update_fields.append("u_manager_email")
                update_values.append(f"'{u_manager_email}'")

            if conf.get('environment'):
                update_fields.append("u_env")
                update_values.append(f"'{conf.get('environment')}'")

            if conf.get('business_justification'):
                update_fields.append("u_business_justification")
                update_values.append(f"'{conf.get('business_justification')}'")

        elif "Disable User Account".lower() in email_subject.lower():
            u_task_type = "Disable User Account"

            if conf.get('user_full_name'):
                update_fields.append("u_user_name")
                update_values.append(f"'{conf.get('user_full_name')}'")

            if conf.get('user_email_id'):
                update_fields.append("u_user_email")
                update_values.append(f"'{conf.get('user_email_id')}'")

            if conf.get('user_email_id'):
                login_id = db_hook.get_login_id_by_email(conf.get('user_email_id'))
                if login_id:
                    update_fields.append("u_user_adid")
                    update_values.append(f"'{login_id}'")

            if conf.get('organization'):
                if "Tmobile" in conf.get('organization', ''):
                    update_fields.append("u_organization")
                    update_values.append("'T-Mobile'")
                else:
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('organization')}'")

            if conf.get('service_request_number'):
                update_fields.append("u_sr_no")
                update_values.append(f"'{conf.get('service_request_number')}'")

            if conf.get('service_owner_ad_id_prod'):
                update_fields.append("u_svc_owner_adid")
                update_values.append(f"'{conf.get('service_owner_ad_id_prod')}'")

            if conf.get('service_owner_ad_id_lab'):
                update_fields.append("u_svc_owner_ntid_signum")
                update_values.append(f"'{conf.get('service_owner_ad_id_lab')}'")

        else:  # Other Requests
            u_task_type = "Other Requests"
            if conf.get('user_ad_id'):
                update_fields.append("u_user_adid")
                update_values.append(f"'{conf.get('user_ad_id')}'")

            if conf.get('user_full_name'):
                update_fields.append("u_user_name")
                update_values.append(f"'{conf.get('user_full_name')}'")

            if conf.get('user_email_id'):
                update_fields.append("u_user_email")
                update_values.append(f"'{conf.get('user_email_id')}'")

            if conf.get('organization'):
                if "Tmobile" in conf.get('organization', ''):
                    update_fields.append("u_organization")
                    update_values.append("'T-Mobile'")
                else:
                    update_fields.append("u_organization")
                    update_values.append(f"'{conf.get('organization')}'")

            if conf.get('manager_ad_id'):
                update_fields.append("u_manager_adid")
                update_values.append(f"'{conf.get('manager_ad_id')}'")

            if conf.get('business_justification'):
                update_fields.append("u_business_justification")
                update_values.append(f"'{conf.get('business_justification')}'")

        # Always update these fields
        update_fields.extend(["u_status", "u_task_type", "updated_on", "update_by"])
        update_values.extend(["'Data updated'", f"'{u_task_type}'", f"'{current_timestamp}'", "'airflow'"])

        if update_fields:
            logger.info(f"update_fields: {update_fields} update_values: {update_values}")

            query = f"""
                UPDATE EXT_ACCESS_MANAGEMENT_ACTIVITY
                SET {', '.join([f"{field} = {value}" for field, value in zip(update_fields, update_values)])}
                WHERE u_identifier = '{process_id}'
            """

            logger.debug(f"Update query execution for process id: {process_id} task_type: {u_task_type} Query: {query}")
            hook.debug(f"Update query execution for process id: {process_id} task_type: {u_task_type} Query: {query}")

            hook.info(f"Updating {u_task_type} activity record")
            logger.info(f"Updating {u_task_type} activity record")
            try:
                result = db_hook.update_activity(query, process_id)
                if not result.get('success'):
                    hook.error(f"Database update failed: {result.get('error')}")
                    logger.error(f"Database update failed: {result.get('error')}")
                    return {
                        "success": False,
                        "error": result.get('error', 'Unknown error updating activity')
                    }
            except Exception as e:
                hook.error(f"Database operation failed: {str(e)}")
                logger.error(f"Database operation failed: {str(e)}")
                return {
                    "success": False,
                    "error": f"Database operation failed: {str(e)}"
                }

        return {
            "success": True,
            "task_type": u_task_type,
            "activity_state": activity_state
        }

    except Exception as e:
        hook.error(f"Error processing update request: {str(e)}")
        logger.error(f"Error processing update request: {str(e)}")
        return {
            "success": False,
            "error": f"Error processing update request: {str(e)}"
        }

def _handle_new_request(hook: WorkLogHook, db_hook: AccessManagementSQLHook, unique_number: str, email_subject: str, conf: dict, current_timestamp: str) -> Dict[str, Any]:
    """
    Handles creating a new record in EXT_ACCESS_MANAGEMENT_ACTIVITY table.
    Contains the exact same creation logic as before.
    """
    try:
        hook.info("No process ID found, treating as new request")
        logger.info("No process ID found, treating as new request")
        hook.info(f"Email subject received: '{email_subject}'")
        logger.info(f"Email subject received: '{email_subject}'")
        hook.info(f"Full conf data: {conf}")
        logger.info(f"Full conf data: {conf}")

        insert_fields = []
        insert_values = []
        u_task_type = ""
        activity_state = "Data Inserted"

        if "User Account Password Reset".lower() in email_subject.lower() or "User Password Reset".lower() in email_subject.lower():
            logger.info("Processing User Account Password Reset request")
            u_task_type = "User Account Password Reset"
            u_user_adid = conf.get('user_ad_id', '')
            u_user_name = conf.get('user_full_name', '')
            u_user_email = conf.get('user_email_id', '')
            u_mobile_no = conf.get('official_mobile_no', '')

            if "Tmobile" in conf.get('organization', ''):
                u_organization = "T-Mobile"
            else:
                u_organization = conf.get('organization', '')

            u_env = conf.get('environment', '')
            u_comments = conf.get('comments', '')

            logger.debug(f"insert_fields: {insert_fields} insert_values: {insert_values}")

            insert_fields = ["u_user_name", "u_user_adid", "u_user_email", "u_mobile_no", "u_env",
                             "u_comments", "u_status", "u_identifier", "u_task_type", "u_organization",
                             "created_on", "updated_on", "created_by"]
            insert_values = [f"'{u_user_name}'", f"'{u_user_adid}'", f"'{u_user_email}'", f"'{u_mobile_no}'",
                             f"'{u_env}'", f"'{u_comments}'", "'Data Inserted'", f"'{unique_number}'",
                             f"'{u_task_type}'", f"'{u_organization}'", f"'{current_timestamp}'", f"'{current_timestamp}'", "'oscar-automation'"]

        elif "Service Account Password Reset".lower() in email_subject.lower() or "ServiceAc Passwd Reset".lower() in email_subject.lower() or "ServiceAc Password Reset".lower() in email_subject.lower():
            logger.info("Processing Service Account Password Reset request")
            u_task_type = "ServiceAc Passwd Reset"
            u_svc_name = conf.get('service_account_full_name', '')
            u_user_name = conf.get('service_account_owner_name', '')
            u_user_email = conf.get('service_account_owner_email_id', '')
            u_user_adid = conf.get('service_account_owner_uprising_id', '')

            if "Tmobile" in conf.get('organization', ''):
                u_organization = conf.get('T-Mobile', '')
            else:
                u_organization = conf.get('organization', '')

            u_env = conf.get('environment', '')
            u_svc_owner_email = conf.get('owner_group_email', '')
            u_business_justification = conf.get('business_justification', '')

            platform_raw = conf.get('platform', '')
            match = re.match(r'(.*)\s(\S+)$', platform_raw.strip())
            if match:
                u_platform = match.group(1).strip()
            else:
                u_platform = platform_raw.strip()

            logger.debug(f"insert_fields: {insert_fields} insert_values: {insert_values}")

            insert_fields = ["u_svc_name", "u_user_name", "u_user_email", "u_user_adid", "u_organization",
                             "u_env", "u_svc_owner_email", "u_business_justification", "u_platform",
                             "u_status", "u_identifier", "u_task_type", "created_on", "updated_on", "created_by"]
            insert_values = [f"'{u_svc_name}'", f"'{u_user_name}'", f"'{u_user_email}'", f"'{u_user_adid}'",
                             f"'{u_organization}'", f"'{u_env}'", f"'{u_svc_owner_email}'",
                             f"'{u_business_justification}'", f"'{u_platform}'", "'Data Inserted'",
                             f"'{unique_number}'", f"'{u_task_type}'", f"'{current_timestamp}'", f"'{current_timestamp}'", "'oscar-automation'"]

        elif "New User Account Creation".lower() in email_subject.lower() or "New User Account".lower() in email_subject.lower():
            logger.info("Processing New User Account Creation request")

            u_task_type = "New User Account"
            u_user_name = conf.get('user_full_name', '')
            u_user_ntid_sinum = conf.get('user_ntid_signum', '')
            u_user_email = conf.get('user_email_id', '')
            u_manager_adid = conf.get('manager_ad_id', '')

            if "Tmobile" in conf.get('organization', ''):
                u_organization = conf.get('T-Mobile', '')
            else:
                u_organization = conf.get('organization', '')

            # Get manager info from EXT_AM_ITSM_USER_LIST
            if u_manager_adid:
                u_manager_name, u_manager_email = db_hook.get_info_by_adid(u_manager_adid)
            else:
                u_manager_name = ''
                u_manager_email = ''

            u_team_name = conf.get('team_name', '')
            u_business_justification = conf.get('business_justification', '')
            u_ref_adid = conf.get('reference_ad_id', '')
            u_env = conf.get('environment', '')
            u_description = conf.get('description', '')

            # Get role and platform from EXT_AM_ITSM_ROLE_PLATFORM_MAPPING
            u_role, u_platform = db_hook.get_role_platform(u_description, u_organization)
            u_mobile_no = conf.get('official_mobile_no', '')

            logger.debug(f"insert_fields: {insert_fields} insert_values: {insert_values}")

            insert_fields = ["u_user_name", "u_user_ntid_signum", "u_user_email", "u_manager_name", "u_manager_email",
                             "u_manager_adid", "u_team_name", "u_business_justification", "u_ref_adid", "u_env",
                             "u_description", "u_role", "u_platform", "u_mobile_no", "u_status", "u_identifier",
                             "u_task_type", "u_organization", "created_on", "updated_on", "created_by"]
            insert_values = [f"'{u_user_name}'", f"'{u_user_ntid_sinum}'", f"'{u_user_email}'", f"'{u_manager_name}'",
                             f"'{u_manager_email}'", f"'{u_manager_adid}'", f"'{u_team_name}'",
                             f"'{u_business_justification}'", f"'{u_ref_adid}'", f"'{u_env}'", f"'{u_description}'",
                             f"'{u_role}'", f"'{u_platform}'", f"'{u_mobile_no}'", "'Data Inserted'",
                             f"'{unique_number}'", f"'{u_task_type}'", f"'{u_organization}'",
                             f"'{current_timestamp}'", f"'{current_timestamp}'", "'oscar-automation'"]

        elif "New Service Account Creation".lower() in email_subject.lower() or "New Service Account".lower() in email_subject.lower():
            logger.info("Processing New Service Account Creation request")

            u_task_type = "New Service Account"
            u_svc_name = conf.get('service_account_full_name', '')
            u_user_name = conf.get('service_account_owner_name', '')
            u_user_email = conf.get('service_account_owner_email_id', '')
            u_user_adid = conf.get('service_account_owner_uprising_id', '')
            u_svc_owner_email = conf.get('owner_group_email', '')

            if "Tmobile" in conf.get('organization', ''):
                u_organization = conf.get('T-Mobile', '')
            else:
                u_organization = conf.get('organization', '')

            u_manager_adid = conf.get('manager_ad_id', '')

            if u_manager_adid:
                u_manager_name, u_manager_email = db_hook.get_info_by_adid(u_manager_adid)
            else:
                u_manager_name = ''
                u_manager_email = ''

            u_user_ntid_sinum = conf.get('owner_ntid', '')
            u_team_name = conf.get('team_name', '')
            u_business_justification = conf.get('business_justification', '')
            u_env = conf.get('environment', '')
            u_description = conf.get('description', '')
            u_role, u_platform = db_hook.get_role_platform(u_description, u_organization)
            u_mobile_no = conf.get('official_mobile_no', '')

            logger.debug(f"insert_fields: {insert_fields} insert_values: {insert_values}")

            insert_fields = ["u_svc_name", "u_user_name", "u_user_email", "u_user_adid", "u_svc_owner_email",
                             "u_organization", "u_manager_name", "u_manager_email", "u_manager_adid",
                             "u_user_ntid_signum", "u_team_name", "u_business_justification", "u_env",
                             "u_description", "u_role", "u_platform", "u_mobile_no", "u_status", "u_identifier",
                             "u_task_type", "created_on", "updated_on", "created_by"]
            insert_values = [f"'{u_svc_name}'", f"'{u_user_name}'", f"'{u_user_email}'", f"'{u_user_adid}'",
                             f"'{u_svc_owner_email}'", f"'{u_organization}'", f"'{u_manager_name}'",
                             f"'{u_manager_email}'", f"'{u_manager_adid}'", f"'{u_user_ntid_sinum}'",
                             f"'{u_team_name}'", f"'{u_business_justification}'", f"'{u_env}'",
                             f"'{u_description}'", f"'{u_role}'", f"'{u_platform}'", f"'{u_mobile_no}'",
                             "'Data Inserted'", f"'{unique_number}'", f"'{u_task_type}'",
                             f"'{current_timestamp}'", f"'{current_timestamp}'", "'oscar-automation'"]

        elif "User Account Modification".lower() in email_subject.lower() or "User Account Modify".lower() in email_subject.lower():
            logger.info("Processing User Account Modification request")

            u_task_type = "User Account Modify"
            u_user_name = conf.get('user_full_name', '')
            u_user_email = conf.get('user_email_id', '')
            u_user_adid = conf.get('user_uprising_id', '')

            if "Tmobile" in conf.get('organization', ''):
                u_organization = conf.get('T-Mobile', '')
            else:
                u_organization = conf.get('organization', '')

            u_manager_name = conf.get('manager_name', '')
            u_manager_email = conf.get('manager_email_id', '')

            u_manager_adid = conf.get('manager_ad_id', '')

            # Get manager info from EXT_AM_ITSM_USER_LIST if name/email not provided
            if not conf.get('manager_name') or not conf.get('manager_email_id') and u_manager_adid:
                u_manager_name, u_manager_email = db_hook.get_info_by_adid(conf.get('manager_ad_id'))

            u_ref_adid = conf.get('reference_ad_id', '')
            u_env = conf.get('environment', '')
            u_team_name = conf.get('team_name', '')
            u_business_justification = conf.get('business_justification', '')
            u_description = conf.get('description', '')
            u_modification_action = conf.get('action', '')
            u_mobile_no = conf.get('official_mobile_no', '')

            # Get role and platform from EXT_AM_ITSM_ROLE_PLATFORM_MAPPING
            u_role, u_platform = db_hook.get_role_platform(u_description, u_organization)

            logger.debug(f"insert_fields: {insert_fields} insert_values: {insert_values}")

            insert_fields = ["u_user_name", "u_user_email", "u_user_adid", "u_organization", "u_manager_name",
                             "u_manager_email", "u_manager_adid", "u_ref_adid", "u_env", "u_team_name",
                             "u_business_justification", "u_description", "u_modification_action", "u_mobile_no",
                             "u_platform", "u_role", "u_status", "u_identifier", "u_task_type", "created_on", "updated_on", "created_by"]
            insert_values = [f"'{u_user_name}'", f"'{u_user_email}'", f"'{u_user_adid}'", f"'{u_organization}'",
                             f"'{u_manager_name}'", f"'{u_manager_email}'", f"'{u_manager_adid}'",
                             f"'{u_ref_adid}'", f"'{u_env}'", f"'{u_team_name}'", f"'{u_business_justification}'",
                             f"'{u_description}'", f"'{u_modification_action}'", f"'{u_mobile_no}'",
                             f"'{u_platform}'", f"'{u_role}'", "'Data Inserted'", f"'{unique_number}'",
                             f"'{u_task_type}'", f"'{current_timestamp}'", f"'{current_timestamp}'", "'oscar-automation'"]

        elif "Service Account Modification".lower() in email_subject.lower() or "Service Account Modify".lower() in email_subject.lower():
            logger.info("Processing Service Account Modification request")

            u_task_type = "Service Account Modify"
            u_svc_name = conf.get('service_account_full_name', '')
            u_user_name = conf.get('service_account_owner_name', '')
            u_user_email = conf.get('service_account_owner_email_id', '')
            u_user_adid = conf.get('service_account_owner_uprising_id', '')

            if "Tmobile" in conf.get('organization', ''):
                u_organization = conf.get('T-Mobile', '')
            else:
                u_organization = conf.get('organization', '')

            u_manager_adid = conf.get('manager_ad_id', '')

            if u_manager_adid:
                u_manager_name, u_manager_email = db_hook.get_info_by_adid(u_manager_adid)
            else:
                u_manager_name = ''
                u_manager_email = ''

            u_env = conf.get('environment', '')
            u_team_name = conf.get('team_name', '')
            u_business_justification = conf.get('business_justification', '')
            u_description = conf.get('description', '')
            u_modification_action = conf.get('action', '')
            u_intended_use = conf.get('intended_use', '')

            # Get role and platform from EXT_AM_ITSM_ROLE_PLATFORM_MAPPING
            u_role, u_platform = db_hook.get_role_platform(u_description, u_organization)

            logger.debug(f"insert_fields: {insert_fields} insert_values: {insert_values}")

            insert_fields = ["u_svc_name", "u_user_name", "u_user_email", "u_user_adid", "u_organization",
                             "u_manager_name", "u_manager_email", "u_manager_adid", "u_env", "u_team_name",
                             "u_business_justification", "u_description", "u_modification_action", "u_intended_use",
                             "u_role", "u_platform", "u_status", "u_identifier", "u_task_type", "created_on", "updated_on", "created_by"]
            insert_values = [f"'{u_svc_name}'", f"'{u_user_name}'", f"'{u_user_email}'", f"'{u_user_adid}'",
                             f"'{u_organization}'", f"'{u_manager_name}'", f"'{u_manager_email}'",
                             f"'{u_manager_adid}'", f"'{u_env}'", f"'{u_team_name}'", f"'{u_business_justification}'",
                             f"'{u_description}'", f"'{u_modification_action}'", f"'{u_intended_use}'",
                             f"'{u_role}'", f"'{u_platform}'", "'Data Inserted'", f"'{unique_number}'",
                             f"'{u_task_type}'", f"'{current_timestamp}'", f"'{current_timestamp}'", "'oscar-automation'"]

        elif "Disable Service Account".lower() in email_subject.lower():
            logger.info("Processing Disable Service Account request")

            u_task_type = "Disable Service Account"
            u_svc_name = conf.get('service_account_full_name', '')
            u_user_name = conf.get('service_account_owner_name', '')

            if "Tmobile" in conf.get('organization', ''):
                u_organization = conf.get('T-Mobile', '')
            else:
                u_organization = conf.get('organization', '')

            u_user_email = conf.get('service_account_owner_email_id', '')
            u_user_adid = conf.get('service_account_owner_ad_id', '')
            u_manager_adid = conf.get('manager_ad_id', '')

            if u_manager_adid:
                u_manager_name, u_manager_email = db_hook.get_info_by_adid(u_manager_adid)
            else:
                u_manager_name = ''
                u_manager_email = ''

            u_env = conf.get('environment', '')
            u_business_justification = conf.get('business_justification', '')

            # Get login ID using the new hook method
            if u_user_email:
                u_login_id = db_hook.get_login_id_by_email(u_user_email)
            else:
                u_login_id = ''

            logger.debug(f"insert_fields: {insert_fields} insert_values: {insert_values}")

            insert_fields = ["u_svc_name", "u_user_name", "u_organization", "u_user_email", "u_user_adid",
                             "u_manager_name", "u_manager_email", "u_manager_adid", "u_env", "u_business_justification",
                             "u_login_id", "u_status", "u_identifier", "u_task_type", "created_on", "updated_on", "created_by"]
            insert_values = [f"'{u_svc_name}'", f"'{u_user_name}'", f"'{u_organization}'", f"'{u_user_email}'",
                             f"'{u_user_adid}'", f"'{u_manager_name}'", f"'{u_manager_email}'",
                             f"'{u_manager_adid}'", f"'{u_env}'", f"'{u_business_justification}'",
                             f"'{u_login_id}'", "'Data Inserted'", f"'{unique_number}'", f"'{u_task_type}'",
                             f"'{current_timestamp}'", f"'{current_timestamp}'", "'oscar-automation'"]

        elif "Disable User Account".lower() in email_subject.lower():
            logger.info("Processing Disable User Account request")

            u_task_type = "Disable User Account"
            u_user_name = conf.get('user_full_name', '')
            u_user_email = conf.get('user_email_id', '')

            # Get login ID if service_account_owner_name exists
            if conf.get('user_email_id'):
                u_user_adid = db_hook.get_login_id_by_email(conf.get('user_email_id'))
            else:
                u_user_adid = ''

            if "Tmobile" in conf.get('organization', ''):
                u_organization = 'T-Mobile'
            else:
                u_organization = conf.get('organization', '')

            u_sr_no = conf.get('service_request_number', '')
            u_svc_owner_adid = conf.get('service_owner_ad_id_prod', '')
            u_svc_owner_ntid_signum = conf.get('service_owner_ad_id_lab', '')

            logger.debug(f"insert_fields: {insert_fields} insert_values: {insert_values}")

            insert_fields = ["u_user_name", "u_user_email", "u_user_adid", "u_organization", 
                             "u_sr_no", "u_svc_owner_adid", "u_svc_owner_ntid_signum",
                             "u_status", "u_identifier", "u_task_type", "created_on", "updated_on", "created_by"]
            insert_values = [f"'{u_user_name}'", f"'{u_user_email}'", f"'{u_user_adid}'", f"'{u_organization}'",
                             f"'{u_sr_no}'", f"'{u_svc_owner_adid}'", f"'{u_svc_owner_ntid_signum}'",
                             "'Data Inserted'", f"'{unique_number}'", f"'{u_task_type}'",
                             f"'{current_timestamp}'", f"'{current_timestamp}'", "'oscar-automation'"]

        else:  # Other Requests
            logger.info("Processing Other Requests")

            u_task_type = "Other Requests"
            u_user_adid = conf.get('user_ad_id', '')
            u_user_name = conf.get('user_full_name', '')
            u_user_email = conf.get('user_email_id', '')

            if "Tmobile" in conf.get('organization', ''):
                u_organization = conf.get('T-Mobile', '')
            else:
                u_organization = conf.get('organization', '')

            u_manager_adid = conf.get('manager_ad_id', '')

            if u_manager_adid:
                u_manager_name, u_manager_email = db_hook.get_info_by_adid(u_manager_adid)
            else:
                u_manager_name = ''
                u_manager_email = ''

            u_business_justification = conf.get('business_justification', '')

            logger.debug(f"insert_fields: {insert_fields} insert_values: {insert_values}")

            insert_fields = ["u_task_type", "u_user_name", "u_user_adid", "u_user_email", "u_organization",
                             "u_manager_adid", "u_manager_name", "u_manager_email", "u_business_justification",
                             "u_status", "u_identifier", "created_on", "updated_on", "created_by"]
            insert_values = [f"'{u_task_type}'", f"'{u_user_name}'", f"'{u_user_adid}'", f"'{u_user_email}'",
                             f"'{u_organization}'", f"'{u_manager_adid}'", f"'{u_manager_name}'",
                             f"'{u_manager_email}'", f"'{u_business_justification}'", "'Data Inserted'",
                             f"'{unique_number}'", f"'{current_timestamp}'", f"'{current_timestamp}'", "'oscar-automation'"]

        if insert_fields:
            query = f"""
                INSERT INTO EXT_ACCESS_MANAGEMENT_ACTIVITY
                ({', '.join(insert_fields)})
                VALUES
                ({', '.join(insert_values)});
            """
            logger.debug(f"Query execution for new request with task type: {u_task_type} : {query}")
            hook.info(f"Query execution for new request with task type: {u_task_type} : {query}")

            hook.info(f"Creating new {u_task_type} activity record")
            try:
                result = db_hook.create_activity(query)
                logger.info(f"Database create result: {result}")
                if not result.get('success'):
                    hook.error(f"Database create failed: {result.get('error')}")
                    return {
                        "success": False,
                        "error": result.get('error', 'Unknown error creating activity')
                    }
            except Exception as e:
                hook.error(f"Database operation failed: {str(e)}")
                return {
                    "success": False,
                    "error": f"Database operation failed: {str(e)}"
                }

        return {
            "success": True,
            "task_type": u_task_type,
            "activity_state": activity_state
        }

    except Exception as e:
        hook.error(f"Error processing new request: {str(e)}")
        return {
            "success": False,
            "error": f"Error processing new request: {str(e)}"
        }

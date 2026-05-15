from airflow.sdk.bases.hook import BaseHook
import os
import logging
from typing import Dict, Any, List, Optional, Union, cast, TypedDict, Literal
import json
from enum import Enum
from helpers.worklog_helper import SyncWorkLogManager
import httpx

logger = logging.getLogger(__name__)

# Define the enums here to avoid import issues


class WorkLogStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    ARCHIVED = "ARCHIVED"


class WorkLogType(str, Enum):
    DB = "DB"
    ELASTIC = "ELASTIC"


class SeverityLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# Define a type for the payload
class WorkLogCreatePayload(TypedDict, total=False):
    name: str
    type: str
    status: str
    description: Optional[str]
    metadata: Optional[List[Dict[str, str]]]


class WorkLogHook(BaseHook):
    """
    Hook for interacting with the WorkLog API through the middleware.

    This hook provides methods for creating, updating, and closing worklogs,
    as well as adding entries with different severity levels.

    :param conn_id: The connection ID to retrieve connection information.
                    If provided, connection info will be retrieved from Airflow's
                    connection store. Otherwise, environment variables or defaults will be used.
    :type conn_id: str
    :param worklog_id: Optional worklog ID to use for all operations
    :type worklog_id: str
    """

    conn_name_attr = 'worklog_conn_id'
    default_conn_name = 'worklog_default'
    conn_type = 'worklog'
    hook_name = 'WorkLog'

    def __init__(self, conn_id: Optional[str] = None, worklog_id: Optional[str] = None):
        super().__init__()
        self.conn_id = conn_id
        self.worklog_id = worklog_id
        self.host: str = ""
        self.port: int = 0
        self.protocol: str = ""
        self.verify_ssl: bool = False
        self.base_url: str = ""

        # Set up connection parameters
        self._setup_connection()

        logger.info(f"Initialized WorkLogHook with base URL: {self.base_url}")

        # If worklog_id is provided, verify it exists and is open
        if self.worklog_id:
            try:
                worklog = self.get_worklog(self.worklog_id)
                if worklog.get('status') != 'OPEN':
                    logger.warning(f"Worklog {self.worklog_id} is not in OPEN state (status: {worklog.get('status')})")
            except Exception as e:
                logger.warning(f"Could not verify worklog {self.worklog_id}: {str(e)}")

    def _setup_connection(self):
        """Set up connection parameters from Airflow connection or environment variables."""
        if self.conn_id:
            conn = self.get_connection(self.conn_id)
            self.host = conn.host or "middleware"
            self.port = conn.port or 5200
            self.protocol = conn.schema or "https"
            # Extract SSL verification from extras if available
            extras = conn.extra_dejson
            self.verify_ssl = extras.get("verify_ssl", False) if extras else False
        else:
            # Use environment variables or defaults
            self.host = os.environ.get("MIDDLEWARE_HOST", "middleware")
            self.port = int(os.environ.get("MIDDLEWARE_PORT", "5200"))
            self.protocol = os.environ.get("MIDDLEWARE_PROTOCOL", "https")
            self.verify_ssl = os.environ.get("SSL_VERIFY", "false").lower() == "true"

        self.base_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/worklogs"
        # Setup headers for internal service authentication
        self.api_headers = {"Content-Type": "application/json", "X-Internal-Service": "airflow"}

    def set_worklog_id(self, worklog_id: str):
        """
        Set the current worklog ID for subsequent operations.

        :param worklog_id: The worklog ID to use
        """
        self.worklog_id = worklog_id
        logger.info(f"Set current worklog ID to: {worklog_id}")

        # Verify the worklog exists and is open
        try:
            worklog = self.get_worklog(worklog_id)
            if worklog.get('status') != 'OPEN':
                logger.warning(f"Worklog {worklog_id} is not in OPEN state (status: {worklog.get('status')})")
        except Exception as e:
            logger.warning(f"Could not verify worklog {worklog_id}: {str(e)}")

    def create_worklog(self, name: str, description: Optional[str] = None,
                       worklog_type: Union[WorkLogType, str] = WorkLogType.DB,
                       metadata: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
        """
        Create a new worklog and set it as the current worklog.

        :param name: Name of the worklog
        :param description: Optional description
        :param worklog_type: Type of worklog (DB or ELASTIC)
        :param metadata: Optional list of key-value pairs as metadata
        :return: Dict containing the created worklog details
        """
        try:
            # Prepare the payload
            payload: WorkLogCreatePayload = {
                "name": name,
                "type": worklog_type.value if isinstance(worklog_type, WorkLogType) else worklog_type,
                "status": WorkLogStatus.OPEN.value,  # Explicitly set status to OPEN
            }

            if description is not None:
                payload["description"] = description

            if metadata is not None:
                payload["metadata"] = metadata

            logger.debug(f"Creating worklog with payload: {json.dumps(payload)}")

            with httpx.Client(verify=self.verify_ssl) as client:
                response = client.post(
                    self.base_url,
                    json=payload,
                    headers=self.api_headers,
                    timeout=30.0
                )

                if response.status_code != 201:
                    logger.error(f"Failed to create worklog: {response.text}")
                    raise Exception(f"Failed to create worklog: {response.text}")

                worklog_data = response.json()

                # Set the current worklog ID
                self.worklog_id = worklog_data["id"]
                logger.info(f"Created worklog with ID: {self.worklog_id}")

                return worklog_data

        except Exception as e:
            logger.error(f"Error creating worklog: {str(e)}")
            raise

    def get_worklog(self, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get worklog details.

        :param worklog_id: ID of the worklog to retrieve (defaults to current worklog)
        :return: Dict containing the worklog details
        """
        id_to_use = worklog_id or self.worklog_id

        if not id_to_use:
            raise ValueError("No worklog ID provided and no current worklog set")

        try:
            with httpx.Client(verify=self.verify_ssl) as client:
                response = client.get(
                    f"{self.base_url}/{id_to_use}",
                    headers=self.api_headers,
                    timeout=30.0
                )

                if response.status_code != 200:
                    logger.error(f"Failed to get worklog: {response.text}")
                    raise Exception(f"Failed to get worklog: {response.text}")

                return response.json()

        except Exception as e:
            logger.error(f"Error getting worklog: {str(e)}")
            raise

    def close_worklog(self, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Close a worklog.

        :param worklog_id: ID of the worklog to close (defaults to current worklog)
        :return: Dict containing the updated worklog details
        """
        id_to_use = worklog_id or self.worklog_id

        if not id_to_use:
            raise ValueError("No worklog ID provided and no current worklog set")

        try:
            logger.debug(f"Closing worklog with ID: {id_to_use}")

            with httpx.Client(verify=self.verify_ssl) as client:
                # Get the current worklog to ensure it exists
                get_response = client.get(
                    f"{self.base_url}/{id_to_use}",
                    headers=self.api_headers,
                    timeout=30.0
                )

                if get_response.status_code != 200:
                    logger.error(f"Failed to get worklog for closing: {get_response.text}")
                    raise Exception(f"Failed to get worklog for closing: {get_response.text}")

                # Use POST to close the worklog
                response = client.post(
                    f"{self.base_url}/{id_to_use}/close",
                    headers=self.api_headers,
                    timeout=30.0
                )

                if response.status_code not in [200, 201, 204]:
                    logger.error(f"Failed to close worklog: {response.text}")
                    raise Exception(f"Failed to close worklog: {response.text}")

                # If we're closing the current worklog, clear the ID
                if id_to_use == self.worklog_id:
                    self.worklog_id = None

                # If the response has no content (204), get the worklog details
                if response.status_code == 204 or not response.text:
                    get_response = client.get(
                        f"{self.base_url}/{id_to_use}",
                        headers=self.api_headers,
                        timeout=30.0
                    )
                    return get_response.json()

                return response.json()

        except Exception as e:
            logger.error(f"Error closing worklog: {str(e)}")
            raise

    def add_entry(self, message: str, severity: Union[SeverityLevel, str] = SeverityLevel.INFO,
                  worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Add an entry to a worklog.

        :param message: Content of the entry
        :param severity: Severity level of the entry
        :param worklog_id: ID of the worklog to add the entry to (defaults to current worklog)
        :return: Dict containing the created entry details
        """
        id_to_use = worklog_id or self.worklog_id

        if not id_to_use:
            raise ValueError("No worklog ID provided and no current worklog set")

        try:
            payload = {
                "message": message,
                "severity": severity.value if isinstance(severity, SeverityLevel) else severity
            }

            logger.debug(f"Adding entry to worklog {id_to_use} with payload: {json.dumps(payload)}")

            with httpx.Client(verify=self.verify_ssl) as client:
                # First, check if the worklog is in OPEN state
                get_response = client.get(
                    f"{self.base_url}/{id_to_use}",
                    headers=self.api_headers,
                    timeout=30.0
                )

                if get_response.status_code != 200:
                    logger.error(f"Failed to get worklog status: {get_response.text}")
                    raise Exception(f"Failed to get worklog status: {get_response.text}")

                worklog_data = get_response.json()
                if worklog_data.get("status") != "OPEN":
                    logger.error(f"Cannot add entry to worklog {id_to_use} - status is {worklog_data.get('status')}")
                    raise Exception(f"Cannot add entry to worklog that is not open (status: {worklog_data.get('status')})")

                # Now add the entry
                response = client.post(
                    f"{self.base_url}/{id_to_use}/entries",
                    json=payload,
                    headers=self.api_headers,
                    timeout=30.0
                )

                if response.status_code not in [200, 201]:
                    logger.error(f"Failed to add entry: {response.text}")
                    raise Exception(f"Failed to add entry: {response.text}")

                logger.debug(f"Successfully added entry to worklog {id_to_use}")
                return response.json()

        except Exception as e:
            logger.error(f"Error adding entry: {str(e)}")
            raise

    # Convenience methods for different severity levels
    def debug(self, message: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """Add a DEBUG level entry to the worklog"""
        return self.add_entry(message, SeverityLevel.DEBUG, worklog_id)

    def info(self, message: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """Add an INFO level entry to the worklog"""
        return self.add_entry(message, SeverityLevel.INFO, worklog_id)

    def warning(self, message: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """Add a WARNING level entry to the worklog"""
        return self.add_entry(message, SeverityLevel.WARNING, worklog_id)

    def error(self, message: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """Add an ERROR level entry to the worklog"""
        return self.add_entry(message, SeverityLevel.ERROR, worklog_id)

    def critical(self, message: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """Add a CRITICAL level entry to the worklog"""
        return self.add_entry(message, SeverityLevel.CRITICAL, worklog_id)

    def find_open_worklog(self, metadata_filters: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Find an open worklog that matches the given metadata filters.

        :param metadata_filters: Dictionary of metadata key-value pairs to filter by
        :return: ID of the first matching open worklog, or None if none found
        """
        try:
            # Build query parameters for filtering
            params = {"status": "OPEN"}

            if metadata_filters:
                # Convert metadata filters to the format expected by the API
                for key, value in metadata_filters.items():
                    params[f"metadata.{key}"] = value

            with httpx.Client(verify=self.verify_ssl) as client:
                response = client.get(
                    self.base_url,
                    params=params,
                    headers=self.api_headers,
                    timeout=30.0
                )

                if response.status_code != 200:
                    logger.error(f"Failed to search for worklogs: {response.text}")
                    raise Exception(f"Failed to search for worklogs: {response.text}")

                data = response.json()

                # Check if we found any matching worklogs
                if data.get("items") and len(data["items"]) > 0:
                    worklog_id = data["items"][0]["id"]
                    logger.info(f"Found open worklog with ID: {worklog_id}")

                    # Set as current worklog
                    self.worklog_id = worklog_id
                    return worklog_id
                else:
                    logger.info("No matching open worklogs found")
                    return None

        except Exception as e:
            logger.error(f"Error searching for worklogs: {str(e)}")
            raise

    def add_metadata(self, metadata_items: Union[List[Dict[str, str]], Dict[str, str]], worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Add or update multiple metadata key-value pairs to the worklog.

        This method follows the "client responsibility" pattern:
        1. Gets current worklog metadata
        2. Combines existing metadata with new metadata (updates existing keys, adds new keys)
        3. Sends complete desired metadata list to middleware
        4. Middleware handles deletion of unwanted metadata and sends to taskmanager

        Behavior:
        - If a key exists in current metadata, its value will be updated
        - If a key doesn't exist, it will be added
        - Existing metadata keys not mentioned in metadata_items will be preserved
        - The final result is exactly the combined metadata list

        :param metadata_items: List of metadata key-value pairs or single key-value pair
                              Examples: 
                              - [{"key": "priority", "value": "high"}, {"key": "ticket_id", "value": "INC-123"}]
                              - {"key": "priority", "value": "high"}
        :param worklog_id: ID of the worklog to update (defaults to current worklog)
        :return: Dict containing the updated worklog details
        """
        id_to_use = worklog_id or self.worklog_id

        if not id_to_use:
            raise ValueError("No worklog ID provided and no current worklog set")

        # Convert single dict to list for consistent processing
        if isinstance(metadata_items, dict):
            metadata_items = [metadata_items]

        if not metadata_items:
            raise ValueError("No metadata items provided")

        try:
            logger.debug(f"Adding metadata to worklog {id_to_use}: {metadata_items}")

            with httpx.Client(verify=self.verify_ssl) as client:
                # First, get the current worklog to check if it's open and get existing metadata
                get_response = client.get(
                    f"{self.base_url}/{id_to_use}",
                    headers=self.api_headers,
                    timeout=30.0
                )

                if get_response.status_code != 200:
                    logger.error(f"Failed to get worklog: {get_response.text}")
                    raise Exception(f"Failed to get worklog: {get_response.text}")

                worklog_data = get_response.json()
                if worklog_data.get("status") != "OPEN":
                    logger.error(f"Cannot add metadata to worklog {id_to_use} - status is {worklog_data.get('status')}")
                    raise Exception(f"Cannot add metadata to worklog that is not open (status: {worklog_data.get('status')})")

                # Get existing metadata
                existing_metadata = worklog_data.get("metadata", [])

                logger.info(f"Existing metadata: {existing_metadata}")

                # Combine existing metadata with new metadata
                combined_metadata = existing_metadata.copy()  # Start with existing metadata

                # Process each new metadata item
                for metadata_item in metadata_items:
                    key = metadata_item.get("key")
                    value = metadata_item.get("value")

                    if not key or not value:
                        logger.warning(f"Skipping invalid metadata item: {metadata_item}")
                        continue

                    # Check if key already exists and update it, otherwise add new
                    key_exists = False
                    for meta_item in combined_metadata:
                        if meta_item.get("key") == key:
                            meta_item["value"] = value  # Update existing key (latest value wins)
                            key_exists = True
                            logger.debug(f"Updated existing metadata: {key}={value}")
                            break

                    if not key_exists:
                        combined_metadata.append({"key": key, "value": value})  # Append new key
                        logger.debug(f"Added new metadata: {key}={value}")

                # Send complete combined metadata
                logger.info(f"Adding combined metadata to worklog {id_to_use}")
                update_payload = {"metadata": combined_metadata}

                logger.info(f"Adding combined metadata to worklog {id_to_use} with payload: {json.dumps(update_payload)}")

                response = client.put(
                    f"{self.base_url}/{id_to_use}",
                    json=update_payload,
                    headers=self.api_headers,
                    timeout=30.0
                )

                if response.status_code != 200:
                    logger.error(f"Failed to add combined metadata: {response.text}")
                    raise Exception(f"Failed to add combined metadata: {response.text}")

                logger.info(f"Successfully added/updated {len(metadata_items)} metadata items to worklog {id_to_use}")
                return response.json()

        except Exception as e:
            logger.error(f"Error adding metadata: {str(e)}")
            raise

import os
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway
from typing import ClassVar, Optional, Dict, List, Any, Union, ContextManager, Iterator, AsyncIterator
from urllib.parse import quote, unquote_plus
import redis
import ssl
import httpx
import logging
import json
import uuid
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime
from enum import Enum

logger = logging.getLogger('oscar-taskmanager')


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


class SyncWorkLogManager:
    """
    A synchronous version of the WorkLogManager for use in fabric tasks.
    Provides methods for creating, updating, and closing worklogs, as well as
    adding entries with different severity levels.
    """

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 protocol: Optional[str] = None, verify_ssl: Optional[bool] = None):
        """
        Initialize the SyncWorkLogManager with connection details.

        Args:
            host: Middleware host (defaults to MIDDLEWARE_HOST env var)
            port: Middleware port (defaults to MIDDLEWARE_PORT env var)
            protocol: HTTP protocol (defaults to MIDDLEWARE_PROTOCOL env var or 'https')
            verify_ssl: Whether to verify SSL certificates (defaults to SSL_VERIFY env var or False)
        """
        self.host = host or os.environ.get("MIDDLEWARE_HOST", "middleware")
        self.port = port or int(os.environ.get("MIDDLEWARE_PORT", 5200))
        self.protocol = protocol or os.environ.get("MIDDLEWARE_PROTOCOL", "https")
        self.verify_ssl = verify_ssl if verify_ssl is not None else (
            os.environ.get("SSL_VERIFY", "false").lower() == "true"
        )

        self.base_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/worklogs"
        self.current_worklog_id: Optional[str] = None
        self.current_worklog: Optional[Dict[str, Any]] = None

        logger.debug(f"[SyncWorkLogManager] Initialized with base URL: {self.base_url}")

    def create(self, name: str, description: Optional[str] = None,
               worklog_type: Union[WorkLogType, str] = WorkLogType.DB,
               metadata: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
        """
        Create a new worklog.

        Args:
            name: Name of the worklog
            description: Optional description
            worklog_type: Type of worklog (DB or ELASTIC)
            metadata: Optional list of key-value pairs as metadata

        Returns:
            Dict containing the created worklog details
        """
        try:
            # Prepare the payload
            payload: Dict[str, Any] = {
                "name": name,
                "type": worklog_type.value if isinstance(worklog_type, WorkLogType) else worklog_type,
                "status": WorkLogStatus.OPEN.value,  # Explicitly set status to OPEN
            }

            if description is not None:
                payload["description"] = description

            if metadata is not None:
                payload["metadata"] = metadata

            logger.debug(f"[SyncWorkLogManager] Creating worklog with payload: {json.dumps(payload)}")

            with httpx.Client(verify=self.verify_ssl) as client:
                response = client.post(
                    self.base_url,
                    json=payload,
                    timeout=30.0
                )

                if response.status_code != 201:
                    logger.error(f"[SyncWorkLogManager] Failed to create worklog: {response.text}")
                    raise Exception(f"Failed to create worklog: {response.text}")

                worklog_data = response.json()
                self.current_worklog_id = worklog_data["id"]
                self.current_worklog = worklog_data

                logger.info(f"[SyncWorkLogManager] Created worklog with ID: {self.current_worklog_id}")
                return worklog_data

        except Exception as e:
            logger.error(f"[SyncWorkLogManager] Error creating worklog: {str(e)}")
            raise

    def get(self, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get a worklog by ID.

        Args:
            worklog_id: ID of the worklog to retrieve (defaults to current worklog)

        Returns:
            Dict containing the worklog details
        """
        try:
            id_to_use = worklog_id or self.current_worklog_id

            if not id_to_use:
                raise ValueError("No worklog ID provided and no current worklog set")

            logger.debug(f"[SyncWorkLogManager] Getting worklog with ID: {id_to_use}")

            with httpx.Client(verify=self.verify_ssl) as client:
                response = client.get(
                    f"{self.base_url}/{id_to_use}",
                    timeout=30.0
                )

                if response.status_code != 200:
                    logger.error(f"[SyncWorkLogManager] Failed to get worklog: {response.text}")
                    raise Exception(f"Failed to get worklog: {response.text}")

                worklog = response.json()

                if not worklog_id:  # If using current worklog, update the cached version
                    self.current_worklog = worklog

                return worklog

        except Exception as e:
            logger.error(f"[SyncWorkLogManager] Error getting worklog: {str(e)}")
            raise

    def update(self, worklog_id: Optional[str] = None, name: Optional[str] = None,
               description: Optional[str] = None, metadata: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
        """
        Update a worklog.

        Args:
            worklog_id: ID of the worklog to update (defaults to current worklog)
            name: New name for the worklog
            description: New description for the worklog
            metadata: New metadata for the worklog

        Returns:
            Dict containing the updated worklog details
        """
        try:
            id_to_use = worklog_id or self.current_worklog_id

            if not id_to_use:
                raise ValueError("No worklog ID provided and no current worklog set")

            payload: Dict[str, Any] = {}
            if name is not None:
                payload["name"] = name
            if description is not None:
                payload["description"] = description
            if metadata is not None:
                payload["metadata"] = metadata

            if not payload:
                logger.warning("[SyncWorkLogManager] No update parameters provided")
                return self.get(id_to_use)

            logger.debug(f"[SyncWorkLogManager] Updating worklog {id_to_use} with payload: {json.dumps(payload)}")

            with httpx.Client(verify=self.verify_ssl) as client:
                response = client.put(
                    f"{self.base_url}/{id_to_use}",
                    json=payload,
                    timeout=30.0
                )

                if response.status_code != 200:
                    logger.error(f"[SyncWorkLogManager] Failed to update worklog: {response.text}")
                    raise Exception(f"Failed to update worklog: {response.text}")

                worklog = response.json()

                if not worklog_id or worklog_id == self.current_worklog_id:
                    self.current_worklog = worklog

                return worklog

        except Exception as e:
            logger.error(f"[SyncWorkLogManager] Error updating worklog: {str(e)}")
            raise

    def close(self, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Close a worklog.

        Args:
            worklog_id: ID of the worklog to close (defaults to current worklog)

        Returns:
            Dict containing the closed worklog details
        """
        try:
            id_to_use = worklog_id or self.current_worklog_id

            if not id_to_use:
                raise ValueError("No worklog ID provided and no current worklog set")

            logger.debug(f"[SyncWorkLogManager] Closing worklog with ID: {id_to_use}")

            with httpx.Client(verify=self.verify_ssl) as client:
                response = client.post(
                    f"{self.base_url}/{id_to_use}/close",
                    timeout=30.0
                )

                if response.status_code != 200:
                    logger.error(f"[SyncWorkLogManager] Failed to close worklog: {response.text}")
                    raise Exception(f"Failed to close worklog: {response.text}")

                worklog = response.json()

                if not worklog_id or worklog_id == self.current_worklog_id:
                    self.current_worklog = worklog

                return worklog

        except Exception as e:
            logger.error(f"[SyncWorkLogManager] Error closing worklog: {str(e)}")
            raise

    def archive(self, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Archive a worklog.

        Args:
            worklog_id: ID of the worklog to archive (defaults to current worklog)

        Returns:
            Dict containing the archived worklog details
        """
        try:
            id_to_use = worklog_id or self.current_worklog_id

            if not id_to_use:
                raise ValueError("No worklog ID provided and no current worklog set")

            logger.debug(f"[SyncWorkLogManager] Archiving worklog with ID: {id_to_use}")

            with httpx.Client(verify=self.verify_ssl) as client:
                response = client.post(
                    f"{self.base_url}/{id_to_use}/archive",
                    timeout=30.0
                )

                if response.status_code != 200:
                    logger.error(f"[SyncWorkLogManager] Failed to archive worklog: {response.text}")
                    raise Exception(f"Failed to archive worklog: {response.text}")

                worklog = response.json()

                if not worklog_id or worklog_id == self.current_worklog_id:
                    self.current_worklog = worklog

                return worklog

        except Exception as e:
            logger.error(f"[SyncWorkLogManager] Error archiving worklog: {str(e)}")
            raise

    def add_entry(self, message: str, severity: Union[SeverityLevel, str] = SeverityLevel.INFO,
                  worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Add an entry to a worklog.

        Args:
            message: Content of the entry
            severity: Severity level of the entry
            worklog_id: ID of the worklog to add the entry to (defaults to current worklog)

        Returns:
            Dict containing the created entry details
        """
        try:
            id_to_use = worklog_id or self.current_worklog_id

            if not id_to_use:
                raise ValueError("No worklog ID provided and no current worklog set")

            payload = {
                "message": message,
                "severity": severity.value if isinstance(severity, SeverityLevel) else severity
            }

            logger.debug(f"[SyncWorkLogManager] Adding entry to worklog {id_to_use} with payload: {json.dumps(payload)}")

            with httpx.Client(verify=self.verify_ssl) as client:
                # First, check if the worklog is in OPEN state
                get_response = client.get(
                    f"{self.base_url}/{id_to_use}",
                    timeout=30.0
                )

                if get_response.status_code != 200:
                    logger.error(f"[SyncWorkLogManager] Failed to get worklog status: {get_response.text}")
                    raise Exception(f"Failed to get worklog status: {get_response.text}")

                worklog_data = get_response.json()
                if worklog_data.get("status") != "OPEN":
                    logger.error(f"[SyncWorkLogManager] Cannot add entry to worklog {id_to_use} - status is {worklog_data.get('status')}")
                    raise Exception(f"Cannot add entry to worklog that is not open (status: {worklog_data.get('status')})")

                # Now add the entry
                response = client.post(
                    f"{self.base_url}/{id_to_use}/entries",
                    json=payload,
                    timeout=30.0
                )

                if response.status_code not in [200, 201]:
                    logger.error(f"[SyncWorkLogManager] Failed to add entry: {response.text}")
                    raise Exception(f"Failed to add entry: {response.text}")

                # Log success instead of treating it as an error
                logger.debug(f"[SyncWorkLogManager] Successfully added entry to worklog {id_to_use}")
                return response.json()

        except Exception as e:
            logger.error(f"[SyncWorkLogManager] Error adding entry: {str(e)}")
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

    @contextmanager
    def open(self, name: str, description: Optional[str] = None,
             worklog_type: Union[WorkLogType, str] = WorkLogType.DB,
             metadata: Optional[List[Dict[str, str]]] = None) -> Iterator["SyncWorkLogManager"]:
        """
        Context manager for creating, using, and automatically closing a worklog.

        Args:
            name: Name of the worklog
            description: Optional description
            worklog_type: Type of worklog (DB or ELASTIC)
            metadata: Optional list of key-value pairs as metadata

        Yields:
            The SyncWorkLogManager instance with an active worklog
        """
        try:
            # Create the worklog
            self.create(name, description, worklog_type, metadata)

            # Verify the worklog was created and is in OPEN state
            if not self.current_worklog_id:
                raise ValueError("Failed to create worklog - no ID returned")

            logger.info(f"[SyncWorkLogManager] Successfully opened worklog {self.current_worklog_id}")

            # Yield self for use in the with block
            yield self

        except Exception as e:
            logger.error(f"[SyncWorkLogManager] Error in worklog context manager: {str(e)}")
            raise
        finally:
            # Close the worklog if we have one
            if self.current_worklog_id:
                try:
                    self.close()
                    logger.info(f"[SyncWorkLogManager] Successfully closed worklog {self.current_worklog_id}")
                except Exception as e:
                    logger.error(f"[SyncWorkLogManager] Error closing worklog in context manager: {str(e)}")

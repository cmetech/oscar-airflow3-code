import os
import logging
from typing import List, Optional, Dict, Any
import httpx
from hooks.oscar_hook import OscarHook
from hooks.worklog_hook import WorkLogHook  # For type hints and optional worklog integration

logger = logging.getLogger(__name__)


class InventoryHook(OscarHook):
    """
    Hook for interacting with the Inventory API through the middleware.

    This hook provides methods for retrieving server details and managing
    maintenance modes for servers and environments.
    """

    def __init__(self, conn_id: Optional[str] = None):
        super().__init__(conn_id)
        self.base_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/inventory"
        # Setup headers for internal service authentication
        self.headers = {"Content-Type": "application/json", "X-Internal-Service": "airflow"}
        logger.info(f"Initialized InventoryHook with base URL: {self.base_url}")

    def get_server(self, server_identifier: str) -> Dict[str, Any]:
        """
        Get a server by UUID, IP address, or hostname.

        Args:
            server_identifier: The identifier of the server (UUID, IP address, or hostname)

        Returns:
            Dict containing the server details

        Raises:
            Exception: If server not found or error occurs
        """
        logger.info(f"Retrieving server with identifier: {server_identifier}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.get(
                    f"{self.base_url}/servers/lookup/{server_identifier}",
                    headers=self.headers
                )
                response.raise_for_status()
                server = response.json()
                logger.info(f"Successfully retrieved server: {server.get('hostname', 'unknown')}")
                return server
        except Exception as e:
            logger.error(f"Error retrieving server {server_identifier}: {str(e)}")
            raise

    def list_servers(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List servers with optional filtering.

        Args:
            filters: Optional dictionary of filter parameters
                - datacenter_id: Filter by datacenter ID
                - environment_id: Filter by environment ID
                - status: Filter by status
                - is_under_maintenance: Filter by maintenance status
                - hostname: Filter by hostname (partial match)

        Returns:
            List of server dictionaries
        """
        params = filters or {}
        logger.info(f"Listing servers with filters: {params}")

        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.get(
                    f"{self.base_url}/servers",
                    params=params,
                    headers=self.headers
                )
                response.raise_for_status()
                servers = response.json()

                # Handle both list response and paginated response
                if isinstance(servers, dict) and 'items' in servers:
                    servers = servers['items']

                logger.info(f"Successfully retrieved {len(servers)} servers")
                return servers
        except Exception as e:
            logger.error(f"Error listing servers: {str(e)}")
            raise

    def enable_server_maintenance(self, server_id: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Enable maintenance mode for a specific server.

        Args:
            server_id: The UUID of the server
            worklog_id: Optional worklog ID for tracking this operation

        Returns:
            Dict containing the updated server details
        """
        logger.info(f"Enabling maintenance mode for server: {server_id}")

        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Enabling maintenance mode for server ID: {server_id}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")

        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.patch(
                    f"{self.base_url}/servers/{server_id}/maintenance/enable",
                    headers=self.headers
                )
                response.raise_for_status()
                server = response.json()
                logger.info(f"Successfully enabled maintenance mode for server: {server.get('hostname', 'unknown')}")

                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Maintenance mode enabled successfully for server: {server.get('hostname')}")
                    except:
                        pass

                return server
        except Exception as e:
            logger.error(f"Error enabling maintenance mode for server {server_id}: {str(e)}")

            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to enable maintenance mode for server {server_id}: {str(e)}")
                except:
                    pass

            raise

    def enable_server(self, server_id: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Enable a server by setting its status to 'Active'.
        
        Args:
            server_id: The UUID of the server
            worklog_id: Optional worklog ID for tracking this operation
            
        Returns:
            Dict containing the updated server details
        """
        logger.info(f"Enabling server: {server_id}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Enabling server ID: {server_id}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                # Use PATCH to update server status
                update_data = {"status": "Active"}
                response = client.patch(
                    f"{self.base_url}/servers/{server_id}",
                    json=update_data,
                    headers=self.headers
                )
                response.raise_for_status()
                server = response.json()
                logger.info(f"Successfully enabled server: {server.get('hostname', 'unknown')} (status: {server.get('status')})")
                
                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Server enabled successfully: {server.get('hostname')} - Status: {server.get('status')}")
                    except:
                        pass
                
                return server
        except Exception as e:
            logger.error(f"Error enabling server {server_id}: {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to enable server {server_id}: {str(e)}")
                except:
                    pass
            
            raise
    
    def disable_server(self, server_id: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Disable a server by setting its status to 'Inactive'.
        
        Args:
            server_id: The UUID of the server
            worklog_id: Optional worklog ID for tracking this operation
            
        Returns:
            Dict containing the updated server details
        """
        logger.info(f"Disabling server: {server_id}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Disabling server ID: {server_id}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                # Use PATCH to update server status
                update_data = {"status": "Inactive"}
                response = client.patch(
                    f"{self.base_url}/servers/{server_id}",
                    json=update_data,
                    headers=self.headers
                )
                response.raise_for_status()
                server = response.json()
                logger.info(f"Successfully disabled server: {server.get('hostname', 'unknown')} (status: {server.get('status')})")
                
                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Server disabled successfully: {server.get('hostname')} - Status: {server.get('status')}")
                    except:
                        pass
                
                return server
        except Exception as e:
            logger.error(f"Error disabling server {server_id}: {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to disable server {server_id}: {str(e)}")
                except:
                    pass
            
            raise
    
    def disable_server_maintenance(self, server_id: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Disable maintenance mode for a specific server.

        Args:
            server_id: The UUID of the server
            worklog_id: Optional worklog ID for tracking this operation

        Returns:
            Dict containing the updated server details
        """
        logger.info(f"Disabling maintenance mode for server: {server_id}")

        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Disabling maintenance mode for server ID: {server_id}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")

        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.patch(
                    f"{self.base_url}/servers/{server_id}/maintenance/disable",
                    headers=self.headers
                )
                response.raise_for_status()
                server = response.json()
                logger.info(f"Successfully disabled maintenance mode for server: {server.get('hostname', 'unknown')}")

                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Maintenance mode disabled successfully for server: {server.get('hostname')}")
                    except:
                        pass

                return server
        except Exception as e:
            logger.error(f"Error disabling maintenance mode for server {server_id}: {str(e)}")

            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to disable maintenance mode for server {server_id}: {str(e)}")
                except:
                    pass

            raise

    def enable_environment_maintenance(self, environment_id: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Enable maintenance mode for an entire environment.

        Args:
            environment_id: The UUID of the environment
            worklog_id: Optional worklog ID for tracking this operation

        Returns:
            Dict containing the response details
        """
        logger.info(f"Enabling maintenance mode for environment: {environment_id}")

        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Enabling maintenance mode for environment ID: {environment_id}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")

        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    f"{self.base_url}/environments/{environment_id}/maintenance/enable",
                    headers=self.headers
                )
                response.raise_for_status()
                result = response.json()
                logger.info(f"Successfully enabled maintenance mode for environment: {environment_id}")

                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Maintenance mode enabled successfully for environment: {environment_id}")
                    except:
                        pass

                return result
        except Exception as e:
            logger.error(f"Error enabling maintenance mode for environment {environment_id}: {str(e)}")

            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to enable maintenance mode for environment {environment_id}: {str(e)}")
                except:
                    pass

            raise

    def disable_environment_maintenance(self, environment_id: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Disable maintenance mode for an entire environment.

        Args:
            environment_id: The UUID of the environment
            worklog_id: Optional worklog ID for tracking this operation

        Returns:
            Dict containing the response details
        """
        logger.info(f"Disabling maintenance mode for environment: {environment_id}")

        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Disabling maintenance mode for environment ID: {environment_id}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")

        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    f"{self.base_url}/environments/{environment_id}/maintenance/disable",
                    headers=self.headers
                )
                response.raise_for_status()
                result = response.json()
                logger.info(f"Successfully disabled maintenance mode for environment: {environment_id}")

                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Maintenance mode disabled successfully for environment: {environment_id}")
                    except:
                        pass

                return result
        except Exception as e:
            logger.error(f"Error disabling maintenance mode for environment {environment_id}: {str(e)}")

            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to disable maintenance mode for environment {environment_id}: {str(e)}")
                except:
                    pass

            raise
    
    def enable_environment(self, environment_id: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Enable an environment (set its status to active/enabled).
        
        Args:
            environment_id: The UUID of the environment
            worklog_id: Optional worklog ID for tracking this operation
            
        Returns:
            Dict containing the updated environment details
        """
        logger.info(f"Enabling environment: {environment_id}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Enabling environment ID: {environment_id}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    f"{self.base_url}/environments/{environment_id}/enable",
                    headers=self.headers
                )
                response.raise_for_status()
                result = response.json()
                logger.info(f"Successfully enabled environment: {environment_id}")
                
                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Environment enabled successfully: {environment_id}")
                    except:
                        pass
                
                return result
        except Exception as e:
            logger.error(f"Error enabling environment {environment_id}: {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to enable environment {environment_id}: {str(e)}")
                except:
                    pass
            
            raise
    
    def disable_environment(self, environment_id: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Disable an environment (set its status to inactive/disabled).
        
        Args:
            environment_id: The UUID of the environment
            worklog_id: Optional worklog ID for tracking this operation
            
        Returns:
            Dict containing the updated environment details
        """
        logger.info(f"Disabling environment: {environment_id}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Disabling environment ID: {environment_id}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    f"{self.base_url}/environments/{environment_id}/disable",
                    headers=self.headers
                )
                response.raise_for_status()
                result = response.json()
                logger.info(f"Successfully disabled environment: {environment_id}")
                
                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Environment disabled successfully: {environment_id}")
                    except:
                        pass
                
                return result
        except Exception as e:
            logger.error(f"Error disabling environment {environment_id}: {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to disable environment {environment_id}: {str(e)}")
                except:
                    pass
            
            raise

    # Helper methods for specific lookups
    def get_server_by_uuid(self, server_uuid: str) -> Dict[str, Any]:
        """
        Get a server by its UUID.

        Args:
            server_uuid: The UUID of the server

        Returns:
            Dict containing the server details
        """
        return self.get_server(server_uuid)

    def get_server_by_hostname(self, hostname: str) -> Dict[str, Any]:
        """
        Get a server by its hostname.

        Args:
            hostname: The hostname of the server

        Returns:
            Dict containing the server details
        """
        return self.get_server(hostname)

    def get_server_by_ip(self, ip_address: str) -> Dict[str, Any]:
        """
        Get a server by its IP address.

        Args:
            ip_address: The IP address of the server

        Returns:
            Dict containing the server details
        """
        return self.get_server(ip_address)
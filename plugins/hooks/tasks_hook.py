import os
import logging
import json
import httpx
from typing import Optional, Dict, Any, List
from hooks.oscar_hook import OscarHook
from hooks.worklog_hook import WorkLogHook  # For optional worklog integration

logger = logging.getLogger(__name__)


class TasksHook(OscarHook):
    """
    Hook for managing and triggering tasks via the middleware API.

    This hook provides methods for:
      - Retrieving task details by ID or name
      - Enabling/disabling tasks
      - Listing tasks with filtering
      - Triggering task execution
      - Triggering workflow execution

    All endpoints use middleware connection details provided via Airflow connection (if available)
    or default to environment variables:
      - MIDDLEWARE_HOST (default: "middleware")
      - MIDDLEWARE_PORT (default: "5200")
      - MIDDLEWARE_PROTOCOL (default: "https")
      - SSL_VERIFY (default: "false")
      - MIDDLEWARE_APIKEY (if available)
    """
    # conn_name_attr = "tasks_conn_id"  # override if needed
    # default_conn_name = "tasks_default"
    # hook_name = "TasksHook"

    def __init__(self, conn_id: Optional[str] = None) -> None:
        super().__init__(conn_id)
        # Build the base URL for the middleware API.
        self.middleware_base_url = f"{self.protocol}://{self.host}:{self.port}/api/v1"
        # Setup headers – include internal service identifier for middleware authentication.
        self.headers = {"Content-Type": "application/json", "X-Internal-Service": "airflow"}
        api_key = os.environ.get("MIDDLEWARE_APIKEY")
        if api_key:
            self.headers["X-API-Key"] = api_key

        logger.info(f"Initialized TasksHook with base URL: {self.middleware_base_url}")

    def trigger_task(
        self,
        task_id_or_name: str,
        prompts: Optional[List[Dict[str, Any]]] = None,
        user_data: Optional[Dict[str, Any]] = None,
        worklog_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Trigger a task via the middleware API.

        :param task_id_or_name: The ID or name of the task to trigger.
        :param prompts: Optional list of prompt dictionaries.
        :param user_data: Optional dictionary of additional user data.
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict with the response from the API.
        """
        prompts = prompts or []
        payload = {
            "prompts": prompts,
            "user_data": user_data
        }
        url = f"{self.middleware_base_url}/tasks/run/{task_id_or_name}"
        logger.info(f"Triggering task at URL: {url} with payload: {json.dumps(payload)}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Triggering task: {task_id_or_name}")
                if prompts:
                    worklog_hook.debug(f"Task prompts: {json.dumps(prompts)}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")

        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
                response = client.post(url, headers=self.headers, json=payload)
                response.raise_for_status()
                result = response.json()
                logger.info(f"Task triggered successfully. Response: {result}")
                
                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Successfully triggered task: {task_id_or_name}")
                        if result.get('task_id'):
                            worklog_hook.debug(f"Task execution ID: {result.get('task_id')}")
                    except:
                        pass
                
                return result
        except Exception as e:
            logger.error(f"Error triggering task {task_id_or_name}: {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to trigger task {task_id_or_name}: {str(e)}")
                except:
                    pass
            
            raise

    def trigger_workflow(
        self,
        workflow_id: str,
        workflow_payload: Optional[Dict[str, Any]] = None,
        worklog_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Trigger a workflow via the middleware API.

        :param workflow_id: The ID of the workflow (DAG) to trigger.
        :param workflow_payload: Optional payload for the workflow run.
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict with the response from the API.
        """
        workflow_payload = workflow_payload or {}
        url = f"{self.middleware_base_url}/workflows/{workflow_id}"
        logger.info(f"Triggering workflow at URL: {url} with payload: {json.dumps(workflow_payload)}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Triggering workflow: {workflow_id}")
                if workflow_payload:
                    worklog_hook.debug(f"Workflow payload: {json.dumps(workflow_payload)}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")

        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
                response = client.post(url, headers=self.headers, json=workflow_payload)
                response.raise_for_status()
                result = response.json()
                logger.info(f"Workflow triggered successfully. Response: {result}")
                
                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Successfully triggered workflow: {workflow_id}")
                        if result.get('run_id'):
                            worklog_hook.debug(f"Workflow run ID: {result.get('run_id')}")
                    except:
                        pass
                
                return result
        except Exception as e:
            logger.error(f"Error triggering workflow {workflow_id}: {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to trigger workflow {workflow_id}: {str(e)}")
                except:
                    pass
            
            raise

    def get_task(self, task_id_or_name: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get task details by ID or name.

        :param task_id_or_name: The UUID or name of the task to retrieve
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict containing the task details
        :raises: Exception if task not found or error occurs
        """
        logger.info(f"Retrieving task with identifier: {task_id_or_name}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Retrieving task: {task_id_or_name}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        # First, try to get the task using the query endpoint which expects an array of task names
        url = f"{self.middleware_base_url}/tasks/query"
        payload = [task_id_or_name]  # API expects an array of task names
        
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
                response = client.post(url, headers=self.headers, json=payload)
                
                if response.status_code == 200:
                    tasks = response.json()
                    if isinstance(tasks, list) and len(tasks) > 0:
                        logger.info(f"Successfully retrieved task: {tasks[0].get('name', 'unknown')}")
                        
                        # Log success to worklog
                        if worklog_id:
                            try:
                                worklog_hook.info(f"Successfully retrieved task: {tasks[0].get('name', 'unknown')} (ID: {tasks[0].get('id', 'unknown')})")
                            except:
                                pass
                        
                        return tasks[0]
                else:
                    # Log the error response for debugging
                    logger.error(f"Failed to query task. Status code: {response.status_code}, Response: {response.text}")
                
                # If not found by name, it might be a UUID, try the list endpoint with filtering
                logger.debug(f"Task not found by name, attempting to retrieve by ID")
                # Note: The middleware doesn't expose a direct GET /tasks/{id} endpoint,
                # so we'll use the list endpoint with filtering if needed
                
                raise Exception(f"Task with identifier '{task_id_or_name}' not found")
                
        except Exception as e:
            logger.error(f"Error retrieving task {task_id_or_name}: {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to retrieve task {task_id_or_name}: {str(e)}")
                except:
                    pass
            
            raise

    def list_tasks(
        self, 
        page: int = 1, 
        per_page: int = 10, 
        order: str = "desc",
        column: str = "succeeded",
        filter_dict: Optional[Dict[str, Any]] = None,
        worklog_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List tasks with pagination and optional filtering.

        :param page: Page number (default: 1)
        :param per_page: Number of items per page (default: 10)
        :param order: Sort order ('asc' or 'desc', default: 'desc')
        :param column: Column to sort by (default: 'succeeded')
        :param filter_dict: Optional dictionary for filtering (e.g., {"status": "enabled"})
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict containing paginated task list and metadata
        """
        logger.info(f"Listing tasks - page: {page}, per_page: {per_page}, filter: {filter_dict}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Listing tasks - page: {page}, per_page: {per_page}, filter: {filter_dict}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        url = f"{self.middleware_base_url}/tasks/"
        params = {
            "page": page,
            "perPage": per_page,
            "order": order,
            "column": column
        }
        
        if filter_dict:
            params["filter"] = json.dumps(filter_dict)
        
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
                response = client.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                result = response.json()
                logger.info(f"Successfully retrieved {len(result.get('records', []))} tasks")
                
                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Successfully retrieved {len(result.get('records', []))} tasks (page {page} of {result.get('total_pages', 0)})")
                    except:
                        pass
                
                return result
        except Exception as e:
            logger.error(f"Error listing tasks: {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to list tasks: {str(e)}")
                except:
                    pass
            
            raise

    def enable_task(self, task_id: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Enable a task by its ID.

        :param task_id: The UUID of the task to enable
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict containing the response from the API
        """
        logger.info(f"Enabling task with ID: {task_id}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Enabling task ID: {task_id}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        url = f"{self.middleware_base_url}/tasks/enable/{task_id}"
        
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
                response = client.post(url, headers=self.headers)
                response.raise_for_status()
                result = response.json()
                logger.info(f"Successfully enabled task: {task_id}")
                
                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Successfully enabled task: {task_id}")
                    except:
                        pass
                
                return result
        except Exception as e:
            logger.error(f"Error enabling task {task_id}: {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to enable task {task_id}: {str(e)}")
                except:
                    pass
            
            raise

    def disable_task(self, task_id: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Disable a task by its ID.

        :param task_id: The UUID of the task to disable
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict containing the response from the API
        """
        logger.info(f"Disabling task with ID: {task_id}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Disabling task ID: {task_id}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        url = f"{self.middleware_base_url}/tasks/disable/{task_id}"
        
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
                response = client.post(url, headers=self.headers)
                response.raise_for_status()
                result = response.json()
                logger.info(f"Successfully disabled task: {task_id}")
                
                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Successfully disabled task: {task_id}")
                    except:
                        pass
                
                return result
        except Exception as e:
            logger.error(f"Error disabling task {task_id}: {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to disable task {task_id}: {str(e)}")
                except:
                    pass
            
            raise

    def enable_tasks(self, task_ids: List[str], worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Enable multiple tasks by their IDs.

        :param task_ids: List of task UUIDs to enable
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict containing summary of the operation
        """
        logger.info(f"Enabling {len(task_ids)} tasks")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Enabling {len(task_ids)} tasks in bulk")
                worklog_hook.debug(f"Task IDs: {', '.join(task_ids)}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        results = {
            "successful": [],
            "failed": [],
            "total": len(task_ids)
        }
        
        # Enable each task individually since the API doesn't support bulk operations
        for task_id in task_ids:
            try:
                # Use the single task enable method (without worklog to avoid spam)
                result = self.enable_task(task_id)
                results["successful"].append(task_id)
                logger.debug(f"Successfully enabled task: {task_id}")
            except Exception as e:
                results["failed"].append({"task_id": task_id, "error": str(e)})
                logger.error(f"Failed to enable task {task_id}: {str(e)}")
        
        # Log summary to worklog
        if worklog_id:
            try:
                if results["failed"]:
                    worklog_hook.warning(f"Bulk enable completed with errors: {len(results['successful'])} succeeded, {len(results['failed'])} failed")
                else:
                    worklog_hook.info(f"Successfully enabled all {len(results['successful'])} tasks in bulk")
            except:
                pass
        
        # Raise exception if any tasks failed
        if results["failed"]:
            error_msg = f"Failed to enable {len(results['failed'])} out of {results['total']} tasks"
            logger.error(error_msg)
            
            if worklog_id:
                try:
                    worklog_hook.error(error_msg)
                except:
                    pass
            
            raise Exception(error_msg)
        
        return results

    def disable_tasks(self, task_ids: List[str], worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Disable multiple tasks by their IDs.

        :param task_ids: List of task UUIDs to disable
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict containing summary of the operation
        """
        logger.info(f"Disabling {len(task_ids)} tasks")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Disabling {len(task_ids)} tasks in bulk")
                worklog_hook.debug(f"Task IDs: {', '.join(task_ids)}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        results = {
            "successful": [],
            "failed": [],
            "total": len(task_ids)
        }
        
        # Disable each task individually since the API doesn't support bulk operations
        for task_id in task_ids:
            try:
                # Use the single task disable method (without worklog to avoid spam)
                result = self.disable_task(task_id)
                results["successful"].append(task_id)
                logger.debug(f"Successfully disabled task: {task_id}")
            except Exception as e:
                results["failed"].append({"task_id": task_id, "error": str(e)})
                logger.error(f"Failed to disable task {task_id}: {str(e)}")
        
        # Log summary to worklog
        if worklog_id:
            try:
                if results["failed"]:
                    worklog_hook.warning(f"Bulk disable completed with errors: {len(results['successful'])} succeeded, {len(results['failed'])} failed")
                else:
                    worklog_hook.info(f"Successfully disabled all {len(results['successful'])} tasks in bulk")
            except:
                pass
        
        # Raise exception if any tasks failed
        if results["failed"]:
            error_msg = f"Failed to disable {len(results['failed'])} out of {results['total']} tasks"
            logger.error(error_msg)
            
            if worklog_id:
                try:
                    worklog_hook.error(error_msg)
                except:
                    pass
            
            raise Exception(error_msg)
        
        return results

    def get_task_by_name(self, task_name: str, worklog_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Helper method to get a task specifically by name.

        :param task_name: The name of the task to retrieve
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict containing the task details, or None if not found
        """
        try:
            return self.get_task(task_name, worklog_id)
        except Exception as e:
            if "not found" in str(e).lower():
                return None
            raise

    def get_task_by_id(self, task_id: str, worklog_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Helper method to get a task specifically by ID.
        Note: This uses the generic get_task method as the API doesn't 
        distinguish between ID and name lookups.

        :param task_id: The UUID of the task to retrieve
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict containing the task details, or None if not found
        """
        try:
            return self.get_task(task_id, worklog_id)
        except Exception as e:
            if "not found" in str(e).lower():
                return None
            raise
    
    def search_tasks(self, pattern: str, worklog_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for tasks using regex or glob patterns.
        
        This method allows searching for tasks using flexible pattern matching,
        supporting both glob patterns (with * and ?) and regular expressions.
        
        Pattern Examples:
            - "*.EAST" - matches all tasks ending with .EAST
            - "backup.*" - matches all tasks starting with backup.
            - ".*test.*" - matches all tasks containing test
            - "^task:.*deploy$" - regex matching tasks starting with 'task:' and ending with 'deploy'
            - "log_*_daily" - matches tasks like log_cleanup_daily, log_rotate_daily
        
        :param pattern: The regex or glob pattern to match task names against
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: List of task dictionaries matching the pattern
        :raises: Exception if the search fails or pattern is invalid
        """
        logger.info(f"Searching for tasks with pattern: {pattern}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Searching for tasks with pattern: {pattern}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        url = f"{self.middleware_base_url}/tasks/search"
        payload = {"pattern": pattern}
        
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
                response = client.post(url, headers=self.headers, json=payload)
                
                if response.status_code == 404:
                    # No tasks found matching the pattern
                    logger.info(f"No tasks found matching pattern: {pattern}")
                    
                    if worklog_id:
                        try:
                            worklog_hook.info(f"No tasks found matching pattern: {pattern}")
                        except:
                            pass
                    
                    return []
                
                response.raise_for_status()
                tasks = response.json()
                
                logger.info(f"Found {len(tasks)} tasks matching pattern: {pattern}")
                
                # Log success to worklog
                if worklog_id:
                    try:
                        worklog_hook.info(f"Found {len(tasks)} tasks matching pattern: {pattern}")
                        if len(tasks) > 0 and len(tasks) <= 10:
                            # Log task names if not too many
                            task_names = [task.get('name', 'unknown') for task in tasks]
                            worklog_hook.debug(f"Matching tasks: {', '.join(task_names)}")
                    except:
                        pass
                
                return tasks
                
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error searching tasks with pattern '{pattern}': {e.response.status_code}"
            logger.error(error_msg)
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(error_msg)
                except:
                    pass
            
            raise Exception(error_msg)
            
        except Exception as e:
            logger.error(f"Error searching tasks with pattern '{pattern}': {str(e)}")
            
            # Log error to worklog
            if worklog_id:
                try:
                    worklog_hook.error(f"Failed to search tasks with pattern '{pattern}': {str(e)}")
                except:
                    pass
            
            raise
    
    def enable_tasks_by_pattern(self, pattern: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Enable all tasks matching a pattern.
        
        This is a convenience method that searches for tasks matching the pattern
        and then enables all of them.
        
        :param pattern: The regex or glob pattern to match task names
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict containing summary of the operation
        """
        logger.info(f"Enabling tasks matching pattern: {pattern}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Enabling tasks matching pattern: {pattern}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        # Search for tasks matching the pattern
        matching_tasks = self.search_tasks(pattern)
        
        if not matching_tasks:
            logger.info(f"No tasks found matching pattern: {pattern}")
            return {"message": "No tasks found to enable", "pattern": pattern, "count": 0}
        
        # Extract task IDs
        task_ids = [task.get('id') for task in matching_tasks if task.get('id')]
        
        logger.info(f"Found {len(task_ids)} tasks to enable")
        
        # Enable the tasks
        return self.enable_tasks(task_ids, worklog_id)
    
    def disable_tasks_by_pattern(self, pattern: str, worklog_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Disable all tasks matching a pattern.
        
        This is a convenience method that searches for tasks matching the pattern
        and then disables all of them.
        
        :param pattern: The regex or glob pattern to match task names
        :param worklog_id: Optional worklog ID for tracking this operation
        :return: Dict containing summary of the operation
        """
        logger.info(f"Disabling tasks matching pattern: {pattern}")
        
        # Log to worklog if provided
        if worklog_id:
            try:
                worklog_hook = WorkLogHook()
                worklog_hook.set_worklog_id(worklog_id)
                worklog_hook.info(f"Disabling tasks matching pattern: {pattern}")
            except Exception as e:
                logger.warning(f"Could not log to worklog: {str(e)}")
        
        # Search for tasks matching the pattern
        matching_tasks = self.search_tasks(pattern)
        
        if not matching_tasks:
            logger.info(f"No tasks found matching pattern: {pattern}")
            return {"message": "No tasks found to disable", "pattern": pattern, "count": 0}
        
        # Extract task IDs
        task_ids = [task.get('id') for task in matching_tasks if task.get('id')]
        
        logger.info(f"Found {len(task_ids)} tasks to disable")
        
        # Disable the tasks
        return self.disable_tasks(task_ids, worklog_id)

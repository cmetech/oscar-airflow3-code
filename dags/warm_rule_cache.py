"""
Warm Rule Cache DAG

This DAG warms the rule cache across all uvicorn workers in taskmanager.
It makes multiple HTTP calls to ensure all workers have the latest rules
in their in-memory cache.

This DAG is triggered asynchronously after a successful maintenance enable
to ensure all workers have the new suppression rules cached.

The DAG is designed to:
1. Be fast and non-blocking
2. Not disrupt the main maintenance flow
3. Ensure all workers have consistent cache state

Configuration (via dag_run.conf):
    - cr_number: CR number for tracking (default: 'unknown')
    - workers: Number of uvicorn workers (default: 4, from TASKMANAGER_UVICORN_WORKERS env)
    - multiplier: Safety multiplier for number of calls (default: 3.0)
    - delay: Delay in seconds between calls (default: 0.4)
    - namespace: Specific namespace to warm (default: 'notifier_autocaller')
    - cache_all_rule_ns: If True, warm ALL namespaces (default: False)
    
Namespace Precedence:
    1. If cache_all_rule_ns=True → warms ALL namespaces (namespace param ignored)
    2. If namespace provided → warms that specific namespace
    3. Default → warms 'notifier_autocaller' namespace only

Example triggers:
    # Default: warm notifier_autocaller only
    {"conf": {"cr_number": "CR123"}}
    
    # Warm specific namespace
    {"conf": {"cr_number": "CR123", "namespace": "my_namespace"}}
    
    # Warm ALL namespaces
    {"conf": {"cr_number": "CR123", "cache_all_rule_ns": true}}
"""

import logging
import os
import time
import requests
from datetime import timedelta
from typing import Dict, Any, List
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from concurrent.futures import ThreadPoolExecutor, as_completed

from hooks.worklog_hook import WorkLogHook  # type: ignore

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Configuration
DEFAULT_WORKERS = 4
DEFAULT_MULTIPLIER = 3.0  # 3x safety margin = 60 calls for 4 workers
DEFAULT_TIMEOUT = 10
DEFAULT_DELAY = 0.4
DEFAULT_NAMESPACE = "notifier_autocaller"


def warm_cache_single_call(middleware_url: str, call_id: int, timeout: int, namespace: str = None) -> Dict:
    """Make a single warm cache call via middleware API."""
    try:
        # Build payload with optional namespace for targeted warming
        payload = {"namespace": namespace} if namespace else None
        
        response = requests.post(
            f"{middleware_url}/api/v1/cache/rule-cache/warm",
            headers={"Connection": "close", "Content-Type": "application/json"},
            json=payload,
            timeout=timeout,
            verify=False
        )
        return {
            "call_id": call_id,
            "status_code": response.status_code,
            "success": response.status_code == 200,
            "namespace": namespace
        }
    except requests.exceptions.Timeout:
        return {
            "call_id": call_id,
            "status_code": 0,
            "success": False,
            "error": "timeout"
        }
    except Exception as e:
        return {
            "call_id": call_id,
            "status_code": 0,
            "success": False,
            "error": str(e)
        }


def warm_all_workers(
    middleware_url: str,
    workers: int,
    multiplier: float,
    timeout: int,
    delay: float,
    namespace: str = None,
    worklog_hook: WorkLogHook = None
) -> Dict:
    """
    Warm cache for all workers by making multiple HTTP calls.
    
    Uses sequential calls with delay to maximize load balancer distribution
    across all workers.
    
    Args:
        namespace: Optional namespace to warm. If provided, only warms that namespace.
                   If None, warms all namespaces.
    """
    calls_needed = int(workers * 5 * multiplier)
    
    results = {
        "total_calls": calls_needed,
        "successful_calls": 0,
        "failed_calls": 0,
        "errors": [],
        "namespace": namespace
    }
    
    ns_info = f" for namespace '{namespace}'" if namespace else " for all namespaces"
    if worklog_hook:
        worklog_hook.info(f"Starting cache warm: {calls_needed} calls for {workers} workers{ns_info}")
    
    # Sequential execution with delay for better distribution
    for i in range(1, calls_needed + 1):
        result = warm_cache_single_call(middleware_url, i, timeout, namespace)
        
        if result["success"]:
            results["successful_calls"] += 1
        else:
            results["failed_calls"] += 1
            if "error" in result:
                results["errors"].append(result["error"])
        
        # Small delay to allow load balancer to rotate
        if delay > 0 and i < calls_needed:
            time.sleep(delay)
    
    if worklog_hook:
        worklog_hook.info(
            f"Cache warm completed: {results['successful_calls']}/{calls_needed} successful"
        )
    
    return results


def warm_rule_cache(**context):
    """
    Main task function to warm rule cache across all workers.
    
    This function:
    1. Gets configuration from DAG conf or defaults
    2. Makes multiple calls to middleware cache warm endpoint
    3. Logs results to worklog
    """
    # Get DAG run configuration
    dag_conf = context.get('dag_run').conf if context.get('dag_run') else {}
    
    # Get parent worklog ID if passed from triggering DAG
    parent_worklog_id = dag_conf.get('parent_worklog_id')
    cr_number = dag_conf.get('cr_number', 'unknown')
    
    # Configuration
    workers = int(dag_conf.get('workers', DEFAULT_WORKERS))
    multiplier = float(dag_conf.get('multiplier', DEFAULT_MULTIPLIER))
    timeout = int(dag_conf.get('timeout', DEFAULT_TIMEOUT))
    delay = float(dag_conf.get('delay', DEFAULT_DELAY))
    
    # Namespace logic with precedence:
    # 1. If cache_all_rule_ns=True → namespace=None (warm all namespaces)
    # 2. If specific namespace provided → use that namespace
    # 3. Default → notifier_autocaller
    cache_all_rule_ns = dag_conf.get('cache_all_rule_ns', False)
    if cache_all_rule_ns:
        # cache_all_rule_ns takes precedence - warm all namespaces
        namespace = None
    else:
        # Use specific namespace or default
        namespace = dag_conf.get('namespace', DEFAULT_NAMESPACE)
    
    # Get middleware URL from environment
    middleware_host = os.getenv('OSCAR_MIDDLEWARE_HOST', 'https://middleware:5200')
    
    # Create worklog
    worklog_hook = WorkLogHook()
    
    try:
        worklog = worklog_hook.create_worklog(
            name=f"Warm Rule Cache - {cr_number}",
            description=f"Warming rule cache across {workers} workers after maintenance enable",
            metadata=[
                {"key": "cr_number", "value": cr_number},
                {"key": "workers", "value": str(workers)},
                {"key": "parent_worklog_id", "value": parent_worklog_id or "none"}
            ]
        )
        worklog_id = worklog['id']
        logger.info(f"Created worklog {worklog_id} for cache warming")
        
    except Exception as e:
        logger.error(f"Failed to create worklog: {e}")
        worklog_hook = None
        worklog_id = None
    
    try:
        ns_display = namespace if namespace else "ALL NAMESPACES"
        if worklog_hook:
            worklog_hook.info(f"Starting rule cache warm for CR: {cr_number}")
            worklog_hook.info(f"Configuration: workers={workers}, multiplier={multiplier}, delay={delay}s, namespace={ns_display}, cache_all_rule_ns={cache_all_rule_ns}")
            worklog_hook.info(f"Middleware URL: {middleware_host}")
        
        # Execute cache warming
        results = warm_all_workers(
            middleware_url=middleware_host,
            workers=workers,
            multiplier=multiplier,
            timeout=timeout,
            delay=delay,
            namespace=namespace,
            worklog_hook=worklog_hook
        )
        
        # Determine status
        total_calls = results["total_calls"]
        successful = results["successful_calls"]
        failed = results["failed_calls"]
        
        if failed == 0:
            status = "success"
            message = f"All {total_calls} cache warm calls successful"
        elif successful > 0:
            status = "partial_success"
            message = f"{successful}/{total_calls} cache warm calls successful"
        else:
            status = "failed"
            message = f"All {total_calls} cache warm calls failed"
        
        if worklog_hook:
            if status == "success":
                worklog_hook.info(message)
            elif status == "partial_success":
                worklog_hook.warning(message)
            else:
                worklog_hook.error(message)
            
            if results["errors"]:
                unique_errors = list(set(results["errors"][:5]))  # First 5 unique errors
                worklog_hook.warning(f"Errors encountered: {unique_errors}")
        
        logger.info(f"Cache warm completed: {message}")
        
        return {
            "status": status,
            "message": message,
            "results": results,
            "cr_number": cr_number,
            "worklog_id": worklog_id
        }
        
    except Exception as e:
        error_msg = f"Cache warm failed with error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        if worklog_hook:
            worklog_hook.error(error_msg)
        
        return {
            "status": "error",
            "message": error_msg,
            "cr_number": cr_number,
            "worklog_id": worklog_id
        }
        
    finally:
        # Close worklog
        if worklog_hook and worklog_id:
            try:
                worklog_hook.close_worklog(worklog_id)
                logger.info(f"Closed worklog {worklog_id}")
            except Exception as e:
                logger.error(f"Failed to close worklog: {e}")


# Create the DAG
with DAG(
    dag_id="warm_rule_cache",
    default_args=default_args,
    description="Warm rule cache across all uvicorn workers after maintenance enable",
    schedule=None,  # Only triggered by other DAGs or manually
    start_date=pendulum.today('UTC').add(days=-1),
    tags=["cache", "maintenance", "internal"],
    catchup=False,
    max_active_runs=3,  # Allow multiple concurrent runs
) as dag:
    
    warm_cache_task = PythonOperator(
        task_id="warm_rule_cache",
        python_callable=warm_rule_cache,
    )

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import httpx
import asyncio
import logging
import os
from typing import List, Dict, Any

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_pending_requests() -> List[Dict[str, Any]]:
    """
    Query the MySQL database for pending access management requests.
    Returns a list of dictionaries containing request identifiers.
    """
    AM_CONN_ID: str = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
    mysql_hook = MySqlHook(mysql_conn_id=AM_CONN_ID)

    # Specific query for access management requests
    query = """
        SELECT u_identifier
        FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
        WHERE u_status IN ('SR Submitted', 'Awaiting Response')
    """
    try:
        # Execute query and fetch results
        results = mysql_hook.get_records(query)
        logger.info(f"Found {len(results)} pending requests")
        return [{'u_identifier': row[0]} for row in results]
    except Exception as e:
        logger.error(f"Error querying database: {str(e)}")
        raise

async def trigger_workflow_for_request(request: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Trigger the workflow for a single request using the middleware API.
    """
    # Get middleware configuration

    MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
    MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))
    workflow_id : str = os.environ.get("ACCESS_MANAGEMENT_REQUEST_RESOLUTION_WORKFLOW_ID", "access_management_request_resolve")

    u_identifier = request['u_identifier']
    api_url = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/workflows/{workflow_id}"

    # Generate a unique DAG run ID
    current_time = datetime.now().isoformat()
    dag_run_id = f"access_management_{u_identifier}_{current_time}"

    # Prepare the payload
    payload = {
        "dag_run_id": dag_run_id,
        "conf": {
            "u_identifier": u_identifier
        },
        "note": f"Access management request for {u_identifier}"
    }

    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(api_url, json=payload)
            response.raise_for_status()
            result = response.json()

            logger.info(f"Successfully triggered workflow for u_identifier: {u_identifier}")
            return {
                "u_identifier": u_identifier,
                "status": "success",
                "dag_run_id": result.get('dag_run_id')
            }
    except Exception as e:
        logger.error(f"Failed to trigger workflow for u_identifier {u_identifier}: {str(e)}")
        return {
            "u_identifier": u_identifier,
            "status": "error",
            "error": str(e)
        }

def process_requests(**context) -> Dict[str, Any]:
    """
    Main function to process all pending requests.
    """
    try:
        # Get pending requests
        requests = get_pending_requests()

        if not requests:
            logger.info("No pending requests found")
            return {
                "status": "success",
                "message": "No pending requests found",
                "results": [],
                "summary": {
                    "total_processed": 0,
                    "total_success": 0,
                    "total_errors": 0
                }
            }

        # Process each request
        results = []
        total_processed = 0
        total_success = 0
        total_errors = 0

        for request in requests:
            total_processed += 1
            result = asyncio.run(trigger_workflow_for_request(request, **context))

            if result["status"] == "success":
                total_success += 1
            else:
                total_errors += 1

            results.append(result)

        logger.info(f"Processed {total_processed} requests: {total_success} successful, {total_errors} failed")
        return {
            "status": "success",
            "message": f"Processed {total_processed} access management requests: {total_success} successful, {total_errors} failed",
            "results": results,
            "summary": {
                "total_processed": total_processed,
                "total_success": total_success,
                "total_errors": total_errors
            }
        }

    except Exception as e:
        logger.error(f"Error processing requests: {str(e)}")
        raise

# Create the DAG
with DAG(
    'access_management_request_resolution_trigger',
    default_args=default_args,
    description='DAG to trigger access management request resolution workflow based on pending requests',
    schedule="*/10 * * * *",  # Run every 10 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['access_management'],
) as dag:

    process_requests_task = PythonOperator(
        task_id='process_access_management_requests',
        python_callable=process_requests,
    )

    # Task dependencies
    process_requests_task

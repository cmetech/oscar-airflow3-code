from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timezone
import httpx

default_args = {
    'start_date': datetime(2023, 1, 1),
    # You can add other default arguments here
}

with DAG(
    'oscar_chat_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    @task
    def run_oscar_chat_pipeline(**context):
        logger = context['task_instance'].log

        # Access the DAG's configuration parameters
        dag_run_conf = context['dag_run'].conf
        user_id = dag_run_conf.get('user_id')
        user_message = dag_run_conf.get('message')
        user_info = dag_run_conf.get('user_info', {})
        timestamp = dag_run_conf.get('timestamp')

        logger.info(f"DAG run configuration: {dag_run_conf}")
        logger.info(f"User ID: {user_id}")
        logger.info(f"User Message: {user_message}")
        logger.info(f"User Info: {user_info}")
        logger.info(f"Timestamp: {timestamp}")

        first_name = user_info.get('first_name', 'User')

        # Handle missing timestamp
        if not timestamp:
            timestamp = datetime.now(timezone.utc).isoformat()  # ISO 8601 format
            logger.info(f"No timestamp provided. Using current time: {timestamp}")

        # Craft the response message using the user's first name and timestamp
        response_message = f"Hi, {first_name}! You sent: '{user_message}' at {timestamp}."

        logger.info(f"Response message: {response_message}")

        # Use the response message as the result
        result = response_message

        return result

    @task
    def send_result(result, **context):
        logger = context['task_instance'].log
        user_id = context['dag_run'].conf.get('user_id')
        callback_url = context['dag_run'].conf.get('callback_url')

        logger.info(f"Preparing to send result to callback URL: {callback_url}")
        logger.info(f"Result to be sent: {result}")

        payload = {
            "user_id": user_id,
            "message": result
        }
        headers = {
            "Content-Type": "application/json",
            "X-Internal-Service": "airflow"
        }

        try:
            logger.info(f"Sending POST request to {callback_url} with payload: {payload}")
            logger.info(f"Headers: {headers}")
            response = httpx.post(
                callback_url,
                json=payload,
                headers=headers,
                verify=False,  # Disable SSL verification in development
                timeout=30.0   # 30 second timeout
            )
            response.raise_for_status()
            logger.info(f"Successfully sent result to {callback_url}. Response status: {response.status_code}")
            logger.info(f"Callback response body: {response.text}")
        except httpx.HTTPStatusError as exc:
            logger.error(f"HTTP error occurred: {exc.response.status_code} - {exc.response.text}")
            raise
        except httpx.ConnectError as exc:
            logger.error(f"Connection error - cannot reach {callback_url}: {exc}")
            raise
        except httpx.TimeoutException as exc:
            logger.error(f"Timeout error - callback to {callback_url} timed out: {exc}")
            raise
        except httpx.RequestError as exc:
            logger.error(f"Request error occurred: {exc}")
            raise
        except Exception as exc:
            logger.error(f"An unexpected error occurred: {type(exc).__name__}: {exc}")
            raise

    # Define the task dependencies
    result = run_oscar_chat_pipeline()
    send_result(result)

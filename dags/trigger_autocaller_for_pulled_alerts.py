from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk.bases.hook import BaseHook

import oracledb
import uuid
import os
import logging
import requests
import time

from hooks.worklog_hook import WorkLogHook, SeverityLevel, WorkLogType

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.today('UTC').add(days=-1),
    "retries": 1,
}

# Generate a unique task ID for this run
task_id = f"WORKLOG-TEST-{uuid.uuid4().hex[:8]}"


def create_worklog(**context):
    """Create a new worklog and add initial entries"""
    hook = WorkLogHook()

    buildmode = os.getenv('BUILD_MODE', 'production')

    # Create metadata for the worklog
    metadata = [
        {"key": "task_id", "value": task_id},
        {"key": "environment", "value": buildmode},
        {"key": "initiated_by", "value": "airflow"}
    ]

    # Create the worklog
    worklog = hook.create_worklog(
        name="Process alerts from Netcool and trigger autocaller",
        description="Worklog for automation of autocaller for generated alerts",
        worklog_type=WorkLogType.DB,
        metadata=metadata
    )

    logger.info(f"Created worklog with ID: {worklog['id']}")

    # Add some initial entries
    hook.info("Starting the worklog test workflow for process_alert_initiate_autocaller")

    # Store the worklog ID in XCom for later tasks
    context['ti'].xcom_push(key='worklog_id', value=worklog['id'])

    return worklog['id']

def close_worklog(**context):
    """Close the worklog and add final entries"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    # Add a final entry
    hook.info(f"Workflow completed, closing worklog for worklog id: {worklog_id}")

    # Close the worklog
    closed_worklog = hook.close_worklog()

    logger.info(f"Closed worklog with ID: {closed_worklog['id']}")

    return closed_worklog['id']

def fetch_and_acknowledge_alerts(**context):
    """Maintain worklog and fetch alerts from Netcool Oracle DB."""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    oracle_conn: str = os.getenv("ORACLE_NETCOOL_DB_OSCAR", "oracle__netcool_db_oscar")

    """Fetch alert details from Netcool Oracle DB and acknowledge them."""
    oracle_conn = BaseHook.get_connection(oracle_conn)
    hook.info("Connecting to Oracle Netcool DB")

    if not all([oracle_conn.host, oracle_conn.schema, oracle_conn.login, oracle_conn.password]):
        hook.error("Oracle connection details are incomplete")
        raise ValueError("Oracle connection details are incomplete")

    dsn = oracledb.makedsn(oracle_conn.host, oracle_conn.port or 1521, service_name=oracle_conn.schema)

    def serialize_oracle_data(value):
        """Handle LOB data safely."""
        if isinstance(value, oracledb.LOB):
            try:
                return value.read() or ''
            except Exception:
                return ''
        return value

    if not dsn:
        hook.error("Oracle DSN is invalid")
        raise ValueError("Oracle DSN is invalid")

    with oracledb.connect(user=oracle_conn.login, password=oracle_conn.password, dsn=dsn) as connection:
        with connection.cursor() as cursor:
            query = """select * from alerts.status where (Severity = 5 OR Severity = 4 OR Severity = 3) and TicketNumber != '' 
            and upper(Manager) like '.*PROD.*' and upper(Acknowledger) NOT LIKE 'ENABLE'  and Summary NOT IN 
            ('Catalina::GlobalRequestProcessor::http-bio-8459::processingTime  in BSCS JMX Monitor 3451' ,
            'Catalina::GlobalRequestProcessor::http-bio-8461::processingTime  in BSCS JMX Monitor 3453' ,
            'Catalina::GlobalRequestProcessor::http-bio-8462::processingTime  in BSCS JMX Monitor 3454'  ,
            'Catalina::GlobalRequestProcessor::http-bio-8460::processingTime  in BSCS JMX Monitor 3452'  ,
            'java.lang::GarbageCollector::G1 Young Generation::CollectionCount  in BSCS JMX Monitor' ,
            'java.lang::GarbageCollector::G1 Young Generation::CollectionTime  in BSCS JMX Monitor' ,
            'java.lang::Threading::CurrentThreadCpuTime  in BSCS JMX Monitor 3352' ,
            'java.lang::Threading::CurrentThreadCpuTime  in BSCS JMX Monitor 3353'  ,
            'java.lang::Threading::CurrentThreadCpuTime  in BSCS JMX Monitor 3354'  ,
            'java.lang::Threading::ThreadCount  in BSCS JMX Monitor')"""
            cursor.execute(query)
            rows = cursor.fetchall()
            hook.info(f"Found {len(rows)} alerts in Netcool")
            if not rows:
                logging.info("No alerts found in Netcool")
                hook.info("No alerts found in Netcool")
                return []

            columns = [desc[0] for desc in cursor.description]
            processed_alerts = []

            for row in rows:
                raw_alert = dict(zip(columns, map(serialize_oracle_data, row)))
                hook.info(f"Processing alert: {raw_alert}")

                # Log the data types we receive for debugging
                hook.info("Data types received from Netcool:")
                for field in ["LASTOCCURRENCE", "FIRSTOCCURRENCE", "TICKETNUMBER", "CLASS",
                              "TICKETQUENAME", "SEVERITY", "NODE", "DATACENTER", "HOSTNAME",
                              "SUMMARY", "TTNumber"]:
                    if field in raw_alert:
                        hook.info(f"{field}: {type(raw_alert[field]).__name__}")

                # Safe conversion of values with type checking
                try:
                    processed_alert = {
                        "LASTOCCURRENCE": str(raw_alert.get("LASTOCCURRENCE", "Unknown")),
                        "FIRSTOCCURRENCE": str(raw_alert.get("FIRSTOCCURRENCE", "Unknown")),
                        "TICKETNUMBER": str(raw_alert.get("TICKETNUMBER", "")).strip(),
                        "CLASS": str(raw_alert.get("CLASS", "")),
                        "TICKETQUENAME": str(raw_alert.get("TICKETQUENAME", "")),
                        "SEVERITY": str(raw_alert.get("SEVERITY", "")),  # Convert to string for consistency
                        "NODE": str(raw_alert.get("NODE", "")),
                        "DATACENTER": str(raw_alert.get("DATACENTER", "")),
                        "HOSTNAME": str(raw_alert.get("HOSTNAME", "")),
                        "SUMMARY": str(raw_alert.get("SUMMARY", "")),
                        "REFERENCE": str(raw_alert.get("TTNumber", "")),  # Safe conversion of TTNumber
                    }

                    # Validate required fields
                    required_fields = ["TICKETNUMBER", "SEVERITY", "NODE", "SUMMARY"]
                    missing_fields = [field for field in required_fields if not processed_alert[field]]
                    if missing_fields:
                        hook.error(f"Alert missing required fields: {missing_fields}")
                        continue

                    processed_alerts.append(processed_alert)

                except Exception as e:
                    hook.error(f"Error processing alert data: {str(e)}")
                    continue

            if processed_alerts:
                ticket_number = [(alert["TICKETNUMBER"],) for alert in processed_alerts]
                update_query = "UPDATE alerts.status SET Acknowledger = 'ENABLE' WHERE TicketNumber = :1"
                cursor.executemany(update_query, ticket_number)
                connection.commit()
                logging.info(f"Acknowledged {len(ticket_number)} alerts.")
                hook.info(f"Acknowledged {len(ticket_number)} alerts.")
            else:
                logging.info("No processed alerts to be consumed")
                hook.info("No processed alerts to be consumed")

    context['ti'].xcom_push(key='alerts', value=processed_alerts)

def trigger_autocaller_dags(**context):
    """Maintain worklog and fetch alerts from Netcool Oracle DB."""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    """Dynamically trigger the autocaller DAG for each alert."""
    alerts = context["ti"].xcom_pull(task_ids="fetch_and_acknowledge_alerts", key="alerts")

    if not alerts:
        logging.info("No alerts to process.")
        return

    hook.info(f"Triggering autocaller for {len(alerts)} alerts")

    triggered_dag_run_ids = []

    for alert in alerts:
        # Validate required alert fields
        required_fields = ["TICKETNUMBER", "SEVERITY", "NODE", "SUMMARY"]
        missing_fields = [field for field in required_fields if not alert.get(field)]
        if missing_fields:
            hook.error(f"Alert missing required fields: {missing_fields}")
            logging.error(f"Alert missing required fields: {missing_fields}")
            continue

        hook.info(f"Triggering autocaller for alert: {alert}")

        conf = {
            "worklog_id": worklog_id,
            "alert": alert
        }

        dag_run_id = f"autocaller_{alert['TICKETNUMBER']}_{int(time.time())}"

        payload = {"conf": conf, "dag_run_id": dag_run_id}

        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        MIDDLEWARE_HOST: str = os.environ.get("MIDDLEWARE_HOST", "middleware")
        MIDDLEWARE_PORT: int = int(os.environ.get("MIDDLEWARE_PORT", 5200))

        airflow_dag_url_autocaller = f'https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/workflows/process_alert_initiate_autocaller'

        hook.info("Triggering send autocaller email dag throgh airflow webserver")

        # Trigger autocaller dag through airflow webserver api call
        response = requests.post(airflow_dag_url_autocaller, json=payload, headers=headers, verify=False)

        if response.status_code in [200, 201]:
            logging.info(f"Triggered process_alert_initiate_autocaller DAG for alert {alert}")
            hook.info(f"Triggered process_alert_initiate_autocaller DAG for ticket {alert}")
            triggered_dag_run_ids.append(dag_run_id)
        else:
            logging.error(f"Failed to trigger email DAG: {response.text}")
            hook.error(f"Failed to Trigger process_alert_initiate_autocaller DAG for ticket {alert}")
            raise Exception("Failed to trigger process_alert_initiate_autocaller DAG")

    # Store all triggered DAG run IDs
    context['ti'].xcom_push(key="triggered_dag_run_ids", value=triggered_dag_run_ids)

    for id in triggered_dag_run_ids:
        hook.info(f"child dag process_alert_initiate_autocaller with dag_run_id {id} Triggered")

with DAG(
    "trigger_autocaller_for_alerts",
    default_args=default_args,
    description="Queries Netcool every 5 minutes and triggers the autocaller notification DAG",
    schedule="*/5 * * * *",  # Runs every 5 minutes
    catchup=False,
) as dag:

    create_worklog_task = PythonOperator(
        task_id="create_worklog",
        python_callable=create_worklog,
    )

    fetch_alert_task = PythonOperator(
        task_id="fetch_and_acknowledge_alerts",
        python_callable=fetch_and_acknowledge_alerts,
        dag=dag,
    )

    trigger_autocaller_task = PythonOperator(
        task_id="trigger_autocaller_dags",
        python_callable=trigger_autocaller_dags,
        dag=dag,
    )

    close_worklog_task = PythonOperator(
        task_id="close_worklog",
        python_callable=close_worklog,
    )

create_worklog_task >> fetch_alert_task >> trigger_autocaller_task >> close_worklog_task

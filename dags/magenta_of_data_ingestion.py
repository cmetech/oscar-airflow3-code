"""
Elasticsearch Data Ingestion DAG

This DAG ingests data from Oracle and SQL Server databases into Elasticsearch indices.
It runs every 20 minutes and processes:
- Oracle: Order errors and order payments
- SQL Server: ITSM incidents and changes

The DAG uses parallel task groups for efficient processing and maintains a worklog
for tracking progress and errors.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import uuid

from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

import oracledb
import pyodbc
from elasticsearch8 import Elasticsearch, helpers

# Import our custom hook
from hooks.worklog_hook import WorkLogHook, WorkLogType

logger = logging.getLogger(__name__)

# Try to import timezone handling
try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from pytz import timezone as ZoneInfo
    except ImportError:
        raise ImportError("You must have either zoneinfo (Python 3.9+) or pytz installed.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Configuration from environment variables
ORACLE_CONN_ID = os.getenv('ORACLE_ORDER_CONN_ID', 'oracle_order_db')
SQLSERVER_CONN_ID = os.getenv('SQLSERVER_ITSM_CONN_ID', 'sqlserver_itsm_db')
ELASTICSEARCH_CONN_ID = os.getenv('ELASTICSEARCH_CONN_ID', 'elasticsearch_main')
ES_BATCH_SIZE = int(os.getenv('ES_BATCH_SIZE', '2000'))
INGEST_INTERVAL_MINUTES = int(os.getenv('INGEST_INTERVAL_MINUTES', '20'))

# Elasticsearch index names
INDEX_ORDER_ERRORS = "order_errors_index"
INDEX_ITSM_DATA = "itsm_data_index"
INDEX_ITSM_CHANGE = "itsm_change_index"
INDEX_ORDER_PAYMENT = "order_payment"

# SQL Queries
ORACLE_ORDER_ERRORS_QUERY = """
SELECT oh.orderid,
       ordertype,
       creationdate,
       channelid,
       orderstatus,
       oh.customerid,
       oh.activityid,
       interactionid,
       dealercode,
       tm.ordererrorid,
       tm.errorcode,
       tm.createddate,
       tm.ordererrorstate,
       tm.closeddate,
       tm.retrystatus,
       tm.operationname,
       tm.shortdescription,
       tm.longdescription,
       tm.backendsystem,
       tm.interfacename,
       tm.resolutionmethod,
       tm.resolutionaction,
       tm.systemdecision
  FROM plprodser1.order_header oh, plprodser1.tmom_ordererror tm
 WHERE oh.orderid  = tm.orderid (+)
   AND creationdate >= sysdate - interval '""" + str(INGEST_INTERVAL_MINUTES) + """' minute
   AND oh.orderid LIKE 'C%'
"""

ORACLE_ORDER_PAYMENT_QUERY = """
SELECT oh.orderid,
       oh.ordertype,
       oh.creationdate,
       oh.customerid,
       oh.channelid,
       paymentmethodcode,
       authorizationid,
       paymentdate,
       amount
  FROM plprodser1.order_header oh, plprodser1.order_payment@aux_to_om op
 WHERE oh.orderid = op.orderid
   AND oh.creationdate >= sysdate - interval '""" + str(INGEST_INTERVAL_MINUTES) + """' minute
"""

SQLSERVER_INCIDENT_QUERY = """
SELECT
Incident_Number
,Last_Name
,First_Name
,Description
,Service
,Status
,DATEADD(ss, Next_Target_Date, '19700101') as Next_Target_Date1
,Priority
,DATEADD(ss, Reported_Date, '19700101') as Reported
,DATEADD(ss, Submit_Date, '19700101') as Submit
,DATEADD(ss, Last_Modified_Date, '19700101') as Modified
,DATEADD(ss, Last_Resolved_Date, '19700101') as Resolved
,DATEADD(ss, Responded_Date, '19700101') as Responded
,DATEADD(ss, Closed_Date, '19700101') as Closed
,DATEADD(ss, ERI_LastEscalatedDate, '19700101') as LastEscalated
,DateADD(ss, ERI_FirstEscalatedDate, '19700101') as FirstEscalated
,Assigned_Group
,Assignee
,SLM_Status_Resolution
,SLM_Status_Response
,Sel_B2B_Reference
,Chr_Source_Object_ID
,Vendor_Group001
,Submitter
,Sel_Environment
,ERI_ReleaseNumber
,Assigned_Support_Organization
,Service_Type
,Last_Modified_By
,Urgency
,CI
,Hours
,Impact
,Resolution_Method
,Service_Impacting
,Status_Reason
,TicketType
,MTTR
,ERI_TMOPendingTime
,ERI_TMOSpentTime
,ERI_EricssonPendingTime
,Assignee_Location
,ERI_ReopenCount
,ERI_EscalationCount
,ERI_ProblemTicket
,Group_Transfers
,Issue_Category
,ERI_OverAllPendingTime
,ERI_KnownError
,ERI_Team
,ERI_PRRELATEFLAG
,Problem_Rel_Flag
FROM [ARSystem].[dbo].[HPD_Help_Desk]
WHERE Submit_Date >= DATEDIFF(SECOND,{d '1970-01-01'}, DATEADD(MINUTE, -""" + str(INGEST_INTERVAL_MINUTES) + """, GETDATE()))
  AND Sel_Environment = 0
  AND Sel_B2B_Reference = 2001
"""

SQLSERVER_CHANGE_QUERY = """
SELECT
Infrastructure_Change_ID,
Description,
ASORG,
ASCHG,
ASGRP,
ASGRPID,
ServiceCI,
Scheduled_Service_Impact_Start,
Submitter,
Scheduled_Service_Impact_End_D,
Submit_Date,
Last_Modified_By,
Change_Request_Status,
Sel_Environment,
CAB_Manager___Change_Co_ord__,
DateADD(hh,-8,DATEADD(ss, Scheduled_Start_Date, '19700101')) as Scheduled_Start_Date1,
DateADD(hh,-8,DATEADD(ss, Scheduled_End_Date,'19700101')) as Scheduled_End_Date1,
DateADD(hh,-8,DATEADD(ss, Actual_Start_Date, '19700101')) as Actual_Start_Date1,
DateADD(hh,-8,DATEADD(ss, Actual_End_Date, '19700101')) as Actual_End_Date1,
DateADD(hh,-8,DATEADD(ss, Requested_Start_Date,'19700101')) as Requested_Start_Date1,
DateADD(hh,-8,DATEADD(ss, Requested_End_Date,'19700101')) as Requested_End_Date1,
Support_Organization2,
Status_Reason,
Risk_Level,
Change_Type,
Urgency,
Impact,
Priority,
Location_Company,
Vendor_Organization,
Vendor_Group,
Change_Request_Number,
Completed_Date,
Change_Timing
FROM [ARSystem].[dbo].[CHG_Infrastructure_Change]
WHERE Scheduled_Start_Date >= DATEDIFF(SECOND,{d '1970-01-01'}, DATEADD(MINUTE, -""" + str(INGEST_INTERVAL_MINUTES) + """, GETDATE()))
  AND Change_Request_Status IN (10, 11)
"""


# Helper Functions
def map_oracle_type_to_es(oracle_type_name: str) -> str:
    """Map Oracle data types to Elasticsearch field types."""
    oracle_type_name = oracle_type_name.upper()
    if oracle_type_name in ("NUMBER", "INTEGER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return "float"
    elif oracle_type_name in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR", "CLOB", "NCLOB"):
        return "text"
    elif oracle_type_name == "DATE":
        return "date"
    elif oracle_type_name.startswith("TIMESTAMP"):
        return "date"
    else:
        return "text"


def generate_es_mapping_from_oracle(cursor) -> Dict[str, Any]:
    """Generate Elasticsearch mapping from Oracle cursor description."""
    mapping = {"mappings": {"properties": {}}}
    for desc in cursor.description:
        column_name = desc[0].lower()

        if column_name in ("creationdate", "createddate"):
            es_type = "date"
            mapping["mappings"]["properties"][column_name] = {"type": es_type}
            continue

        oracle_type_name = desc[1].name
        es_type = map_oracle_type_to_es(oracle_type_name)

        if es_type == "text":
            mapping["mappings"]["properties"][column_name] = {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            }
        else:
            mapping["mappings"]["properties"][column_name] = {"type": es_type}

    return mapping


def convert_to_pst(dt) -> Optional[str]:
    """Convert datetime to PST timezone ISO format."""
    if dt is None:
        return None
    pst_zone = ZoneInfo("America/Los_Angeles")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pst_zone)
    return dt.isoformat()


def get_elasticsearch_client() -> Elasticsearch:
    """Create and return an Elasticsearch client using Airflow connection."""
    es_conn = BaseHook.get_connection(ELASTICSEARCH_CONN_ID)

    # Build the Elasticsearch URL
    protocol = 'https' if es_conn.schema and es_conn.schema.lower() == 'https' else 'http'
    host = es_conn.host
    port = es_conn.port or 9200

    es_url = f"{protocol}://{host}:{port}"

    # Get CA certificate path from extras if available
    extras = es_conn.extra_dejson
    ca_certs = extras.get('ca_certs') if extras else None

    # Create Elasticsearch client
    es_config = {
        "hosts": [es_url],
        "basic_auth": (es_conn.login, es_conn.password) if es_conn.login else None,
        "verify_certs": extras.get('verify_certs', False) if extras else False,
        "ssl_show_warn": False
    }

    if ca_certs:
        es_config["ca_certs"] = ca_certs

    return Elasticsearch(**es_config)


# Task definitions
@task
def create_worklog(**context) -> str:
    """Create a new worklog and add initial entries."""
    hook = WorkLogHook()

    # Generate unique run ID
    run_id = f"ES-INGEST-{uuid.uuid4().hex[:8]}"

    # Create metadata for the worklog
    metadata = [
        {"key": "run_id", "value": run_id},
        {"key": "dag_run_id", "value": context['dag_run'].run_id},
        {"key": "environment", "value": os.getenv('BUILD_MODE', 'production')},
        {"key": "initiated_by", "value": "airflow"},
        {"key": "interval_minutes", "value": str(INGEST_INTERVAL_MINUTES)}
    ]

    # Create the worklog
    worklog = hook.create_worklog(
        name="Elasticsearch Data Ingestion",
        description=f"Ingest data from Oracle and SQL Server to Elasticsearch (last {INGEST_INTERVAL_MINUTES} minutes)",
        worklog_type=WorkLogType.ELASTIC,
        metadata=metadata
    )

    logger.info(f"Created worklog with ID: {worklog['id']}")

    # Add initial entries
    hook.info("Starting Elasticsearch data ingestion workflow")
    hook.info(f"Processing data from the last {INGEST_INTERVAL_MINUTES} minutes")
    hook.debug(f"Oracle connection: {ORACLE_CONN_ID}")
    hook.debug(f"SQL Server connection: {SQLSERVER_CONN_ID}")
    hook.debug(f"Elasticsearch connection: {ELASTICSEARCH_CONN_ID}")

    # Store the worklog ID in XCom for later tasks
    context['ti'].xcom_push(key='worklog_id', value=worklog['id'])

    return worklog['id']


@task
def close_worklog(**context) -> str:
    """Close the worklog and add final entries."""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    if not worklog_id:
        logger.warning("No worklog ID found in XCom")
        return "No worklog to close"

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    # Add summary entries
    hook.info("Elasticsearch data ingestion workflow completed")

    # Get task instance states to report summary
    dag_run = context['dag_run']
    task_instances = dag_run.get_task_instances()

    failed_tasks = [ti.task_id for ti in task_instances if ti.state == 'failed']
    success_tasks = [ti.task_id for ti in task_instances if ti.state == 'success']

    hook.info(f"Tasks completed successfully: {len(success_tasks)}")
    if failed_tasks:
        hook.error(f"Tasks failed: {len(failed_tasks)} - {', '.join(failed_tasks)}")

    # Close the worklog
    hook.info("Closing worklog")
    closed_worklog = hook.close_worklog()

    logger.info(f"Closed worklog with ID: {closed_worklog['id']}")

    return closed_worklog['id']


@task
def ingest_oracle_order_errors(**context) -> Dict[str, Any]:
    """Ingest order errors from Oracle to Elasticsearch."""
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    hook.info("Starting Oracle order errors ingestion")

    try:
        # Get Oracle connection
        oracle_conn = BaseHook.get_connection(ORACLE_CONN_ID)
        hook.debug(f"Connecting to Oracle database: {oracle_conn.host}")

        # Create DSN
        dsn = oracledb.makedsn(
            oracle_conn.host,
            oracle_conn.port or 1521,
            service_name=oracle_conn.schema
        )

        # Connect to Oracle
        with oracledb.connect(
            user=oracle_conn.login,
            password=oracle_conn.password,
            dsn=dsn
        ) as connection:
            with connection.cursor() as cursor:
                hook.info("Executing order errors query")
                cursor.execute(ORACLE_ORDER_ERRORS_QUERY)

                # Generate mapping and fetch data
                columns = [col[0].lower() for col in cursor.description]
                mapping = generate_es_mapping_from_oracle(cursor)

                rows = []
                for row in cursor:
                    doc = dict(zip(columns, row))
                    # Convert datetime fields
                    for key, value in doc.items():
                        if isinstance(value, datetime):
                            doc[key] = convert_to_pst(value)
                    rows.append(doc)

                hook.info(f"Fetched {len(rows)} order error records from Oracle")

        if not rows:
            hook.warning("No order error data to ingest")
            return {"index": INDEX_ORDER_ERRORS, "count": 0, "status": "no_data"}

        # Get Elasticsearch client
        es = get_elasticsearch_client()

        # Ensure index exists
        if not es.indices.exists(index=INDEX_ORDER_ERRORS):
            hook.info(f"Creating index '{INDEX_ORDER_ERRORS}'")
            es.indices.create(index=INDEX_ORDER_ERRORS, body=mapping)

        # Prepare bulk actions
        actions = [
            {
                "_index": INDEX_ORDER_ERRORS,
                "_id": row["orderid"],
                "_source": row
            }
            for row in rows
        ]

        # Bulk ingest
        hook.info(f"Ingesting {len(actions)} records to Elasticsearch")
        success, failed = helpers.bulk(
            es,
            actions,
            chunk_size=ES_BATCH_SIZE,
            request_timeout=60,
            raise_on_error=False
        )

        hook.info(f"Successfully ingested {success} order error records")
        if failed:
            hook.error(f"Failed to ingest {len(failed)} records")
            for error in failed[:5]:  # Log first 5 errors
                hook.error(f"Error: {error}")

        return {
            "index": INDEX_ORDER_ERRORS,
            "count": success,
            "failed": len(failed) if failed else 0,
            "status": "success"
        }

    except Exception as e:
        hook.error(f"Error in order errors ingestion: {str(e)}")
        logger.exception("Order errors ingestion failed")
        raise


@task
def ingest_oracle_order_payments(**context) -> Dict[str, Any]:
    """Ingest order payments from Oracle to Elasticsearch."""
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    hook.info("Starting Oracle order payments ingestion")

    try:
        # Get Oracle connection
        oracle_conn = BaseHook.get_connection(ORACLE_CONN_ID)
        hook.debug(f"Connecting to Oracle database: {oracle_conn.host}")

        # Create DSN
        dsn = oracledb.makedsn(
            oracle_conn.host,
            oracle_conn.port or 1521,
            service_name=oracle_conn.schema
        )

        # Connect to Oracle
        with oracledb.connect(
            user=oracle_conn.login,
            password=oracle_conn.password,
            dsn=dsn
        ) as connection:
            with connection.cursor() as cursor:
                hook.info("Executing order payments query")
                cursor.execute(ORACLE_ORDER_PAYMENT_QUERY)

                # Generate mapping and fetch data
                columns = [col[0].lower() for col in cursor.description]
                mapping = generate_es_mapping_from_oracle(cursor)

                rows = []
                for row in cursor:
                    doc = dict(zip(columns, row))
                    # Convert datetime fields
                    for key, value in doc.items():
                        if isinstance(value, datetime):
                            doc[key] = convert_to_pst(value)
                    rows.append(doc)

                hook.info(f"Fetched {len(rows)} order payment records from Oracle")

        if not rows:
            hook.warning("No order payment data to ingest")
            return {"index": INDEX_ORDER_PAYMENT, "count": 0, "status": "no_data"}

        # Get Elasticsearch client
        es = get_elasticsearch_client()

        # Ensure index exists
        if not es.indices.exists(index=INDEX_ORDER_PAYMENT):
            hook.info(f"Creating index '{INDEX_ORDER_PAYMENT}'")
            es.indices.create(index=INDEX_ORDER_PAYMENT, body=mapping)

        # Prepare bulk actions
        actions = [
            {
                "_index": INDEX_ORDER_PAYMENT,
                "_id": row["orderid"],
                "_source": row
            }
            for row in rows
        ]

        # Bulk ingest
        hook.info(f"Ingesting {len(actions)} records to Elasticsearch")
        success, failed = helpers.bulk(
            es,
            actions,
            chunk_size=ES_BATCH_SIZE,
            request_timeout=60,
            raise_on_error=False
        )

        hook.info(f"Successfully ingested {success} order payment records")
        if failed:
            hook.error(f"Failed to ingest {len(failed)} records")
            for error in failed[:5]:  # Log first 5 errors
                hook.error(f"Error: {error}")

        return {
            "index": INDEX_ORDER_PAYMENT,
            "count": success,
            "failed": len(failed) if failed else 0,
            "status": "success"
        }

    except Exception as e:
        hook.error(f"Error in order payments ingestion: {str(e)}")
        logger.exception("Order payments ingestion failed")
        raise


@task
def ingest_sqlserver_incidents(**context) -> Dict[str, Any]:
    """Ingest ITSM incidents from SQL Server to Elasticsearch."""
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    hook.info("Starting SQL Server ITSM incidents ingestion")

    try:
        # Get SQL Server connection
        sql_conn = BaseHook.get_connection(SQLSERVER_CONN_ID)
        hook.debug(f"Connecting to SQL Server: {sql_conn.host}")

        # Build connection string
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={sql_conn.host};"
            f"DATABASE={sql_conn.schema};"
            f"UID={sql_conn.login};"
            f"PWD={sql_conn.password}"
        )

        # Add port if specified
        if sql_conn.port:
            conn_str = conn_str.replace(f"SERVER={sql_conn.host};", f"SERVER={sql_conn.host},{sql_conn.port};")

        # Connect to SQL Server
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            hook.info("Executing ITSM incidents query")
            cursor.execute(SQLSERVER_INCIDENT_QUERY)

            # Fetch data
            columns = [col[0].lower() for col in cursor.description]
            rows = []

            for row in cursor.fetchall():
                doc = dict(zip(columns, row))
                # Convert datetime fields
                for key, value in doc.items():
                    if isinstance(value, datetime):
                        doc[key] = convert_to_pst(value)
                rows.append(doc)

            hook.info(f"Fetched {len(rows)} ITSM incident records from SQL Server")

        if not rows:
            hook.warning("No ITSM incident data to ingest")
            return {"index": INDEX_ITSM_DATA, "count": 0, "status": "no_data"}

        # Get Elasticsearch client
        es = get_elasticsearch_client()

        # Ensure index exists
        if not es.indices.exists(index=INDEX_ITSM_DATA):
            hook.info(f"Creating index '{INDEX_ITSM_DATA}'")
            es.indices.create(index=INDEX_ITSM_DATA)

        # Prepare bulk actions
        actions = [
            {
                "_index": INDEX_ITSM_DATA,
                "_id": row["incident_number"],
                "_source": row
            }
            for row in rows
        ]

        # Bulk ingest
        hook.info(f"Ingesting {len(actions)} records to Elasticsearch")
        success, failed = helpers.bulk(
            es,
            actions,
            chunk_size=ES_BATCH_SIZE,
            request_timeout=60,
            raise_on_error=False
        )

        hook.info(f"Successfully ingested {success} ITSM incident records")
        if failed:
            hook.error(f"Failed to ingest {len(failed)} records")
            for error in failed[:5]:  # Log first 5 errors
                hook.error(f"Error: {error}")

        return {
            "index": INDEX_ITSM_DATA,
            "count": success,
            "failed": len(failed) if failed else 0,
            "status": "success"
        }

    except Exception as e:
        hook.error(f"Error in ITSM incidents ingestion: {str(e)}")
        logger.exception("ITSM incidents ingestion failed")
        raise


@task
def ingest_sqlserver_changes(**context) -> Dict[str, Any]:
    """Ingest ITSM changes from SQL Server to Elasticsearch."""
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    hook.info("Starting SQL Server ITSM changes ingestion")

    try:
        # Get SQL Server connection
        sql_conn = BaseHook.get_connection(SQLSERVER_CONN_ID)
        hook.debug(f"Connecting to SQL Server: {sql_conn.host}")

        # Build connection string
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={sql_conn.host};"
            f"DATABASE={sql_conn.schema};"
            f"UID={sql_conn.login};"
            f"PWD={sql_conn.password}"
        )

        # Add port if specified
        if sql_conn.port:
            conn_str = conn_str.replace(f"SERVER={sql_conn.host};", f"SERVER={sql_conn.host},{sql_conn.port};")

        # Connect to SQL Server
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            hook.info("Executing ITSM changes query")
            cursor.execute(SQLSERVER_CHANGE_QUERY)

            # Fetch data
            columns = [col[0].lower() for col in cursor.description]
            rows = []

            for row in cursor.fetchall():
                doc = dict(zip(columns, row))
                # Convert datetime fields
                for key, value in doc.items():
                    if isinstance(value, datetime):
                        doc[key] = convert_to_pst(value)
                rows.append(doc)

            hook.info(f"Fetched {len(rows)} ITSM change records from SQL Server")

        if not rows:
            hook.warning("No ITSM change data to ingest")
            return {"index": INDEX_ITSM_CHANGE, "count": 0, "status": "no_data"}

        # Get Elasticsearch client
        es = get_elasticsearch_client()

        # Ensure index exists
        if not es.indices.exists(index=INDEX_ITSM_CHANGE):
            hook.info(f"Creating index '{INDEX_ITSM_CHANGE}'")
            es.indices.create(index=INDEX_ITSM_CHANGE)

        # Prepare bulk actions
        actions = [
            {
                "_index": INDEX_ITSM_CHANGE,
                "_id": row["infrastructure_change_id"],
                "_source": row
            }
            for row in rows
        ]

        # Bulk ingest
        hook.info(f"Ingesting {len(actions)} records to Elasticsearch")
        success, failed = helpers.bulk(
            es,
            actions,
            chunk_size=ES_BATCH_SIZE,
            request_timeout=60,
            raise_on_error=False
        )

        hook.info(f"Successfully ingested {success} ITSM change records")
        if failed:
            hook.error(f"Failed to ingest {len(failed)} records")
            for error in failed[:5]:  # Log first 5 errors
                hook.error(f"Error: {error}")

        return {
            "index": INDEX_ITSM_CHANGE,
            "count": success,
            "failed": len(failed) if failed else 0,
            "status": "success"
        }

    except Exception as e:
        hook.error(f"Error in ITSM changes ingestion: {str(e)}")
        logger.exception("ITSM changes ingestion failed")
        raise


# Create the DAG
with DAG(
    'magenta_of_data_ingestion',
    default_args=default_args,
    description='Magenta Order Fallout - Ingest data from Oracle and SQL Server databases to Elasticsearch',
    schedule=f'*/{INGEST_INTERVAL_MINUTES} * * * *',  # Run every N minutes
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['magenta', 'elasticsearch', 'ingestion', 'oracle', 'sqlserver'],
) as dag:

    # Create worklog task
    task_create_worklog = create_worklog()

    # Oracle ingestion tasks (run in parallel)
    with TaskGroup(group_id='oracle_ingestion') as oracle_group:
        task_order_errors = ingest_oracle_order_errors()
        task_order_payments = ingest_oracle_order_payments()

    # SQL Server ingestion tasks (run in parallel)
    with TaskGroup(group_id='sqlserver_ingestion') as sqlserver_group:
        task_itsm_incidents = ingest_sqlserver_incidents()
        task_itsm_changes = ingest_sqlserver_changes()

    # Close worklog task (always runs)
    task_close_worklog = close_worklog()
    task_close_worklog.trigger_rule = TriggerRule.ALL_DONE

    # Define task dependencies
    task_create_worklog >> [oracle_group, sqlserver_group] >> task_close_worklog

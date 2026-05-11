import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import uuid
from hooks.worklog_hook import WorkLogHook, WorkLogType
from hooks.access_management_db_hook import AccessManagementSQLHook

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Find the absolute path to the 'oscar' folder, then append 'excel_ingest'
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
OSCAR_DIR = os.path.abspath(os.path.join(DAG_DIR, '..', '..', '..'))  # up from dags/ to oscar/
DEFAULT_EXCEL_DIR = os.path.join(OSCAR_DIR, 'excel_ingest')
EXCEL_DIR = '/opt/airflow/dags/excel_ingest'

task_id = f"WORKLOG-EXCEL-INGEST-{uuid.uuid4().hex[:8]}"

# Valid excel_type values for dag_run.conf:
#   platform_role_mapping, approver_role, user_report, shift_roster
EXCEL_TYPE_CONFIGS = {
    'platform_role_mapping': {
        'filename': 'Platform_Role_Mapping.xlsx',
        'sheet': 'Sheet1',
        'table': 'EXT_AM_ITSM_ROLE_PLATFORM_MAPPING',
        'mapping': {
            'Role Description': 'u_role_description',
            'Company': 'u_company',
            'Platform': 'u_platform',
            'Role Display Name': 'u_role',
        },
    },
    'approver_role': {
        'filename': 'Approver_Roles.xlsx',
        'sheet': 'Report',
        'table': 'EXT_AM_ITSM_APPROVER_ROLE',
        'mapping': {
            'Company': 'u_company',
            'Platform': 'u_platform',
            'Role Description': 'u_role_des',
            'Approver List': 'u_approver_list',
            'Approver Names': 'u_approver_name',
            'Display Name': 'u_display_name',
        },
    },
    'user_report': {
        'filename': 'UserReportITSM_ENV.xlsx',
        'sheet': 'Report',
        'table': 'EXT_AM_ITSM_USER_LIST',
        'mapping': {
            'Login ID': 'u_login_id',
            'First Name*': 'u_first_name',
            'Last Name*+': 'u_last_name',
            'Full Name': 'u_full_name',
            'Email Address': 'u_email',
            'Environment': 'u_env',
        },
    },
    'shift_roster': {
        'filename': 'ShiftRoster.xlsx',
        'sheet': 'Sheet1',
        'table': 'EXT_AM_SHIFT_ROSTER_TABLE',
        'mapping': {
            'Name': 'u_name',
            'Email': 'u_email',
            'Date': 'u_current_date',
            'Shift Time': 'u_full_time',
        },
    },
}

# Connection ID for DB (set via Airflow connection/environment)
DB_CONN_ID = os.getenv('OSCAR_DB_EXT_CONNECTION_ID', 'oscar_db_ext')

def create_worklog(**context):
    hook = WorkLogHook()
    buildmode = os.getenv('BUILD_MODE', 'production')
    metadata = [
        {"key": "task_id", "value": task_id},
        {"key": "environment", "value": buildmode},
        {"key": "initiated_by", "value": "airflow"}
    ]
    worklog = hook.create_worklog(
        name="Excel to DB Ingest Worklog",
        description="Worklog for Excel to DB ingestion DAG",
        worklog_type=WorkLogType.DB,
        metadata=metadata
    )
    logger.info(f"Created worklog with ID: {worklog['id']}")
    hook.info("Starting Excel to DB ingestion workflow")
    context['ti'].xcom_push(key='worklog_id', value=worklog['id'])
    return worklog['id']

def close_worklog(**context):
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    hook.info(f"Workflow completed, closing worklog for worklog id: {worklog_id}")
    closed_worklog = hook.close_worklog()
    logger.info(f"Closed worklog with ID: {closed_worklog['id']}")
    return closed_worklog['id']

def ingest_excel_to_db(**context):
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    db_hook = AccessManagementSQLHook(DB_CONN_ID, worklog_id)
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    # Check for base64 content mode
    if conf.get('content_b54') and conf.get('excel_content') and conf.get('excel_type'):
        excel_type = conf['excel_type']
        config = EXCEL_TYPE_CONFIGS.get(excel_type)
        if not config:
            msg = f"Unknown excel_type '{excel_type}'. Valid types: {list(EXCEL_TYPE_CONFIGS.keys())}"
            worklog_hook.error(msg)
            logger.error(msg)
            raise ValueError(msg)
        import base64
        import io
        worklog_hook.info(f"Reading Excel for type '{excel_type}' from base64 content in conf")
        logger.info(f"Reading Excel for type '{excel_type}' from base64 content in conf")
        excel_bytes = base64.b64decode(conf['excel_content'])
        excel_file = io.BytesIO(excel_bytes)
        df = pd.read_excel(excel_file, sheet_name=config['sheet'])
    else:
        excel_type = conf.get('excel_type', 'shift_roster')  # Default to 'approver_role' if not provided
        if not excel_type:
            msg = "No excel_type provided in dag_run.conf. Defaulting to 'approver_role'."
            worklog_hook.info(msg)
            logger.info(msg)
            excel_type = 'approver_role'
        config = EXCEL_TYPE_CONFIGS.get(excel_type)
        if not config:
            msg = f"Unknown excel_type '{excel_type}'. Valid types: {list(EXCEL_TYPE_CONFIGS.keys())}"
            worklog_hook.error(msg)
            logger.error(msg)
            raise ValueError(msg)
        # Robust file search logic
        filename = config['filename']
        possible_paths = [
            os.path.join(EXCEL_DIR, filename),
            os.path.join('.', 'excel_ingest', filename),
            filename
        ]
        file_path = None
        for path in possible_paths:
            logger.info(f"Trying to read Excel file at: {path}")
            if os.path.exists(path):
                file_path = path
                break
        if not file_path:
            msg = f"Excel file '{filename}' not found in any of: {possible_paths}"
            worklog_hook.error(msg)
            logger.error(msg)
            raise FileNotFoundError(msg)
        worklog_hook.info(f"Reading Excel for type '{excel_type}' from file: {file_path}")
        logger.info(f"Reading Excel for type '{excel_type}' from file: {file_path}")
        df = pd.read_excel(file_path, sheet_name=config['sheet'])

    df = df.rename(columns=config['mapping'])
    df = df[list(config['mapping'].values())]
    df = df.where(pd.notnull(df), None)
    # --- Create table if not exists ---
    table = config['table']
    columns_defs = ', '.join([
        f"{col} DATE" if 'date' in col.lower() else f"{col} VARCHAR(255)" for col in df.columns
    ])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table} ({columns_defs})"
    db_hook.hook.run(create_table_sql)
    worklog_hook.info(f"Ensured table {table} exists with columns: {', '.join(df.columns)}")
    # --- Truncate table before insert ---
    db_hook.hook.run(f"DELETE FROM {table}")
    worklog_hook.info(f"Cleared all existing data from {table} before inserting new rows.")
    # --- Insert data ---
    data_tuples = [tuple(x) for x in df.values]
    db_hook.hook.insert_rows(table, data_tuples, target_fields=list(df.columns), commit_every=1000)
    worklog_hook.info(f"Inserted {len(data_tuples)} rows for {config['filename']} into {table}")
    logger.info(f"Successfully ingested {len(df)} rows from {config['filename']} into {table}")

with DAG(
    'excel_to_db_ingest',
    default_args=default_args,
    description='Ingest a single Excel file into DB table as per mapping, based on excel_type conf',
    schedule=None,
    catchup=False,
) as dag:
    create_worklog_task = PythonOperator(
        task_id='create_worklog',
        python_callable=create_worklog,
    )
    ingest_task = PythonOperator(
        task_id='ingest_excel_to_db',
        python_callable=ingest_excel_to_db,
    )
    close_worklog_task = PythonOperator(
        task_id='close_worklog',
        python_callable=close_worklog,
        trigger_rule='all_done',
    )
    create_worklog_task >> ingest_task >> close_worklog_task
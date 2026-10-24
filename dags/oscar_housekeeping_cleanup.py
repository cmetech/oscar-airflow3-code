import os
import logging
import json
import shutil
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.sdk.bases.hook import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.standard.operators.empty import EmptyOperator

from hooks.worklog_hook import WorkLogHook, WorkLogType  # type: ignore
from helpers import oscar_housekeeping_config as housekeeping_config
import mysql.connector
from airflow.sdk.observability import stats


logger = logging.getLogger(__name__)

POLICY_DEFAULTS = {
    'task_history': ('ENABLE_TASK_HISTORY_CLEANUP', 'TASK_HISTORY_DAYS_TO_KEEP'),
    'alert_history': ('ENABLE_ALERT_HISTORY_CLEANUP', 'ALERT_HISTORY_DAYS_TO_KEEP'),
    'notification_audit': ('ENABLE_NOTIFICATION_AUDIT_CLEANUP', 'NOTIFICATION_AUDIT_DAYS_TO_KEEP'),
    'ticketing_audit': ('ENABLE_TICKETING_AUDIT_CLEANUP', 'TICKETING_AUDIT_DAYS_TO_KEEP'),
    'user_audit': ('ENABLE_USER_AUDIT_CLEANUP', 'USER_AUDIT_DAYS_TO_KEEP'),
    'alert_audit': ('ENABLE_ALERT_AUDIT_CLEANUP', 'ALERT_AUDIT_DAYS_TO_KEEP'),
}


def resolved_policies(overrides):
    """Resolve validated database overrides against the file defaults."""
    if not isinstance(overrides, dict) or set(overrides) - set(POLICY_DEFAULTS):
        raise ValueError('Invalid housekeeping policy overrides')
    policies = {}
    for family, (enabled_key, days_key) in POLICY_DEFAULTS.items():
        default = {'enabled': bool(getattr(housekeeping_config, enabled_key)),
                   'retention_days': int(getattr(housekeeping_config, days_key))}
        override = overrides.get(family, {})
        if (not isinstance(override, dict) or set(override) - set(default)
                or any(key not in default for key in override)):
            raise ValueError(f'Invalid housekeeping policy: {family}')
        merged = {**default, **override}
        if type(merged['enabled']) is not bool or type(merged['retention_days']) is not int or not 1 <= merged['retention_days'] <= 3650:
            raise ValueError(f'Invalid housekeeping policy: {family}')
        policies[family] = merged
    return policies


@task
def load_housekeeping_policy():
    """One policy snapshot is shared by every task in this DAG run."""
    db = _connect_oscar_db(os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db'))
    try:
        cursor = db.cursor()
        try:
            cursor.execute("SELECT overrides, revision FROM SYS_HousekeepingSettings WHERE id='default'")
            row = cursor.fetchone()
        except mysql.connector.Error as exc:
            if exc.errno != 1146:  # rollout before the settings migration
                raise
            row = None
        finally:
            cursor.close()
        overrides = row[0] if row else {}
        revision = int(row[1]) if row else 0
        policies = resolved_policies(json.loads(overrides) if isinstance(overrides, str) else overrides)
        for family, policy in policies.items():
            stats.gauge(f'oscar_housekeeping.policy_enabled.{family}',
                        int(policy['enabled'] and housekeeping_config.ENABLE_DATABASE_CLEANUP))
        logger.info('Housekeeping policy snapshot revision=%s policies=%s', revision, policies)
        return {'revision': revision, 'policies': policies}
    finally:
        db.close()


def policy_for(context, family):
    snapshot = context['ti'].xcom_pull(task_ids='load_housekeeping_policy')
    policies = snapshot.get('policies') if isinstance(snapshot, dict) else None
    if not isinstance(policies, dict) or family not in policies:
        raise RuntimeError('Housekeeping policy snapshot unavailable')
    return policies[family]


def bounded_eligible_count(cursor, table, predicate, params, limit):
    """Count at most limit indexed candidates so a large backlog cannot monopolize MySQL."""
    cursor.execute(
        f'SELECT COUNT(*) FROM (SELECT 1 FROM {table} WHERE {predicate} LIMIT %s) AS eligible',
        (*params, limit),
    )
    return cursor.fetchone()[0]


def _cleanup_success(context):
    task_id = context['task_instance'].task_id
    keys = {
        'clean_task_history': ('task_history', 'task_history_cleanup_result'),
        'clean_alert_history_records': ('alert_history', 'alert_history_cleanup_result'),
        'clean_notification_audit_history': ('notification_audit', 'notification_audit_cleanup_result'),
        'clean_ticketing_audit_history': ('ticketing_audit', 'ticketing_audit_cleanup_result'),
        'clean_user_audit_history': ('user_audit', 'user_audit_cleanup_result'),
        'clean_alert_audit': ('alert_audit', 'alert_audit_cleanup_result'),
    }
    if task_id not in keys:
        return
    family, result_key = keys[task_id]
    result = context['ti'].xcom_pull(task_ids=task_id, key=result_key)
    if result is None:
        result = context['ti'].xcom_pull(task_ids=task_id, key='return_value') or {}
    if not isinstance(result, dict) or not result or result.get('dry_run'):
        return
    rows = result.get('rows_deleted')
    if rows is None:
        rows = next((value for key, value in result.items()
                     if key.startswith('total_') and key.endswith('_deleted') and key != 'total_summary_deleted'), 0)
    stats.incr(f'oscar_housekeeping.runs_success.{family}')
    if rows:
        stats.incr(f'oscar_housekeeping.rows_deleted.{family}', int(rows))
    stats.gauge(f'oscar_housekeeping.last_success_epoch.{family}', int(datetime.now(timezone.utc).timestamp()))
    if 'eligible_backlog' in result:
        stats.gauge(f'oscar_housekeeping.eligible_backlog.{family}', int(result['eligible_backlog']))
    stats.gauge(f'oscar_housekeeping.duration_seconds.{family}', float(context['ti'].duration or 0))


def _cleanup_failure(context):
    family = {'clean_task_history': 'task_history', 'clean_alert_history_records': 'alert_history',
              'clean_notification_audit_history': 'notification_audit',
              'clean_ticketing_audit_history': 'ticketing_audit',
              'clean_user_audit_history': 'user_audit', 'clean_alert_audit': 'alert_audit'}.get(
                  context['task_instance'].task_id)
    if family in POLICY_DEFAULTS:
        stats.incr(f'oscar_housekeeping.runs_failure.{family}')


def _dag_failure(context):
    stats.incr('oscar_housekeeping.dag_failure.all')


def _dag_success(context):
    stats.incr('oscar_housekeeping.dag_success.all')


def _connect_oscar_db(connection_id: str):
    """
    Open the OSCAR database: the Airflow connection when it exists, otherwise
    the DB password secret mounted in every Airflow container.

    No install step creates the `oscar_db` connection, so without the fallback
    every database cleanup task fails and the tables grow until the disk fills.
    The secret is a container path, so this works wherever OSCAR is installed.
    A connection that exists but cannot connect is an error, not a fallback.
    """
    try:
        conn = BaseHook.get_connection(connection_id)
    except Exception as lookup_error:
        password_file = os.getenv('OSCAR_DB_PASSWORD_FILE', '/run/secrets/airflow_db_password')
        try:
            with open(password_file) as fh:
                password = fh.read().strip()
        except OSError as secret_error:
            raise RuntimeError(
                f"No Airflow connection '{connection_id}' ({lookup_error}) and no DB password "
                f"secret at {password_file} ({secret_error}); cannot reach the OSCAR database"
            ) from secret_error
        host = os.getenv('OSCAR_DB_HOST', 'db')
        user = os.getenv('OSCAR_DB_USER', 'oscar')
        database = os.getenv('OSCAR_DB_NAME', 'oscar')
        logger.warning(
            f"Airflow connection '{connection_id}' not found; connecting as {user}@{host}/{database} "
            f"with the password from {password_file}"
        )
        return mysql.connector.connect(
            host=host,
            port=int(os.getenv('OSCAR_DB_PORT', '3306')),
            user=user,
            password=password,
            database=database
        )
    return mysql.connector.connect(
        host=conn.host,
        port=conn.port or 3306,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )


def create_timeout_checker(task_name: str, max_minutes: int):
    """
    Create a timeout checker function for database cleanup tasks.
    
    This provides an additional safety layer beyond Airflow's execution_timeout
    to ensure cleanup tasks don't run forever and impact application performance.
    
    Args:
        task_name: Name of the task for logging
        max_minutes: Maximum execution time in minutes
        
    Returns:
        Function that returns True if timeout exceeded, False otherwise
    """
    import time
    start_time = time.time()
    max_seconds = max_minutes * 60
    
    def check_timeout() -> bool:
        elapsed = time.time() - start_time
        if elapsed > max_seconds:
            logger.warning(f"[{task_name}] Timeout reached: {elapsed:.2f}s > {max_seconds}s")
            return True
        return False
    
    return check_timeout


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=housekeeping_config.TASK_TIMEOUT_MINUTES),  # Hard timeout per task
}


@task
def create_worklog(**context):
    """Create a new worklog and push its ID via XCom."""
    hook = WorkLogHook()

    buildmode = os.getenv('BUILD_MODE', 'production')

    task_id = context.get('task').task_id if context and context.get('task') else 'create_worklog'

    metadata = [
        {"key": "task_id", "value": task_id},
        {"key": "environment", "value": buildmode},
        {"key": "initiated_by", "value": "airflow"},
    ]

    # The worklog is a report, not a gate: every cleanup task depends on this
    # one, so a worklog API failure here must not stop the disk cleanup. The
    # cleanup tasks already run without a worklog (wl_write ignores errors).
    try:
        worklog = hook.create_worklog(
            name="Oscar Housekeeping Worklog",
            description="Worklog for housekeeping logs cleanup",
            worklog_type=WorkLogType.DB,
            metadata=metadata,
        )
    except Exception as e:
        logger.error(f"Worklog could not be created; cleanup continues without a worklog: {e}")
        return None

    logger.info(f"Created worklog with ID: {worklog['id']}")

    context['ti'].xcom_push(key='worklog_id', value=worklog['id'])

    try:
        hook.info("Starting housekeeping logs cleanup workflow")
    except Exception as e:
        logger.error(f"Could not add the start entry to worklog {worklog['id']}; cleanup continues: {e}")

    return worklog['id']


def parse_size(size_str):
    """Parse size string like '100MB' to bytes"""
    size_str = str(size_str).upper()
    if size_str.endswith('MB'):
        return int(size_str[:-2]) * 1024 * 1024
    elif size_str.endswith('GB'):
        return int(size_str[:-2]) * 1024 * 1024 * 1024
    elif size_str.endswith('KB'):
        return int(size_str[:-2]) * 1024
    else:
        return int(size_str)


def wl_write(worklog_hook, message: str, level: str = "INFO") -> None:
    """Common worklog writer. Call from tasks after initializing worklog_hook."""
    if not worklog_hook:
        return
    try:
        lvl = str(level).upper()
        if lvl == "DEBUG":
            worklog_hook.debug(message)
        elif lvl == "INFO":
            worklog_hook.info(message)
        elif lvl in ("WARN", "WARNING"):
            worklog_hook.warning(message)
        elif lvl == "ERROR":
            worklog_hook.error(message)
        elif lvl == "CRITICAL":
            worklog_hook.critical(message)
        else:
            worklog_hook.info(f"[{lvl}] {message}")
    except Exception:
        pass

def format_bytes(num_bytes):
    """Format bytes into a human-readable string (KB, MB, GB)."""
    if num_bytes is None:
        return "0 B"
    num_bytes = float(num_bytes)
    if num_bytes >= 1024 * 1024 * 1024:
        return f"{num_bytes / (1024 * 1024 * 1024):.2f} GB"
    elif num_bytes >= 1024 * 1024:
        return f"{num_bytes / (1024 * 1024):.2f} MB"
    elif num_bytes >= 1024:
        return f"{num_bytes / 1024:.2f} KB"
    return f"{num_bytes:.0f} B"


def build_task_report(task_name: str, cleanup_type: str, result: dict) -> dict:
    """
    Build a structured cleanup report from a task's result dict.
    
    Args:
        task_name: Human-readable task name (e.g. "Airflow Logs")
        cleanup_type: "file" or "db"
        result: The result dict produced by the cleanup task
        
    Returns:
        A dict with standardized report fields, suitable for JSON serialization
        into worklog metadata.
    """
    report = {
        "task": task_name,
        "type": cleanup_type,
        "status": "completed",
    }

    if isinstance(result, str):
        # Task returned early with a skip message
        report["status"] = "skipped"
        report["reason"] = result
        return report

    if not isinstance(result, dict):
        report["status"] = "skipped"
        report["reason"] = str(result) if result else "No result returned"
        return report

    # Dry run flag
    if result.get("dry_run"):
        report["status"] = "dry_run"

    # File-based cleanup stats
    if cleanup_type == "file":
        report["files_deleted"] = result.get("files_deleted", 0)
        report["dirs_deleted"] = result.get("dirs_deleted", 0)
        space_bytes = result.get("space_freed_bytes", 0)
        report["space_freed"] = format_bytes(space_bytes)
        report["space_freed_bytes"] = space_bytes
        if result.get("timeout_reached"):
            report["warning"] = "Timeout reached - may need another run"
        if result.get("limit_reached"):
            report["warning"] = "Directory limit reached - may need another run"

    # DB-based cleanup stats
    elif cleanup_type == "db":
        # Collect all row-deletion fields into a single rows_deleted total
        rows_deleted = 0
        row_details = {}
        for key, val in result.items():
            if key.startswith("total_") and key.endswith("_deleted") and isinstance(val, (int, float)):
                row_details[key] = val
                rows_deleted += val
            elif key == "rows_deleted" and isinstance(val, (int, float)):
                rows_deleted = val
                row_details["rows_deleted"] = val

        report["rows_deleted"] = rows_deleted
        if len(row_details) > 1:
            report["rows_detail"] = row_details

        # Space freed (GB-based from table swap / optimize)
        space_freed_gb = result.get("space_freed_gb")
        if space_freed_gb is not None and space_freed_gb > 0:
            report["space_freed"] = f"{space_freed_gb} GB"
            report["space_freed_gb"] = space_freed_gb
        else:
            report["space_freed"] = "N/A (InnoDB reuse)"

        if result.get("original_size_gb") is not None:
            report["original_size_gb"] = result["original_size_gb"]
        if result.get("new_size_gb") is not None:
            report["new_size_gb"] = result["new_size_gb"]
        if result.get("mode"):
            report["mode"] = result["mode"]
        if result.get("optimization_completed") is not None:
            report["optimization_completed"] = result["optimization_completed"]

    # Common fields
    if result.get("execution_time_seconds") is not None:
        report["execution_time_seconds"] = result["execution_time_seconds"]
    elif result.get("total_execution_seconds") is not None:
        report["execution_time_seconds"] = result["total_execution_seconds"]
    if result.get("days_to_keep") is not None:
        report["days_to_keep"] = result["days_to_keep"]
    if result.get("summary"):
        report["summary"] = result["summary"]

    return report


# Mapping of XCom keys to (task_name, cleanup_type) for report building
TASK_REPORT_REGISTRY = [
    ("cleanup_result", "clean_airflow_logs", "Airflow Logs", "file"),
    ("task_history_cleanup_result", "clean_task_history", "Task History (TM_History + TM_StageHistory)", "db"),
    ("alert_cleanup_result", "clean_alert_history", "Legacy Alerts (AM_Alert)", "db"),
    ("alert_history_cleanup_result", "clean_alert_history_records", "Alert History (AM_AlertHistory)", "db"),
    ("alert_audit_cleanup_result", "clean_alert_audit", "Alert Audit (AM_AlertAudit)", "db"),
    ("notification_audit_cleanup_result", "clean_notification_audit_history", "Notification Audit (NTF_Notifications_Audit)", "db"),
    ("ticketing_audit_cleanup_result", "clean_ticketing_audit_history", "Ticketing Audit (TKT_Ticketing_Audit)", "db"),
    ("user_audit_cleanup_result", "clean_user_audit_history", "User Audit (UA_User_Audit)", "db"),
]


@task
def clean_airflow_logs(**context):
    """Clean Airflow logs older than specified days - INDEPENDENT OPERATION
    
    Performance optimizations:
    - Uses subprocess 'du' for fast size calculation
    - Batch processing with limits
    - Timeout protection
    - Minimal logging for performance
    """
    import subprocess
    import time
    
    # Worklog setup
    worklog_hook = None
    try:
        worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id') if context else None
        worklog_hook = WorkLogHook()
        if worklog_id:
            worklog_hook.set_worklog_id(worklog_id)
    except Exception:
        worklog_hook = None

    def wl(message: str, level: str = "INFO") -> None:
        wl_write(worklog_hook, message, level)

    # Master kill switch check for Airflow logs cleanup
    if not housekeeping_config.ENABLE_AIRFLOW_LOGS_CLEANUP:
        logger.info("Airflow logs cleanup disabled by master flag (ENABLE_AIRFLOW_LOGS_CLEANUP=False)")
        wl("Airflow logs cleanup skipped - master flag disabled", "WARNING")
        return "Airflow logs cleanup disabled by master flag"

    if not housekeeping_config.AIRFLOW_LOGS_CLEANUP_ENABLED:
        logger.info("Airflow logs cleanup disabled")
        wl("Airflow logs cleanup disabled by configuration", "INFO")
        return "Airflow logs cleanup disabled"

    # Configuration
    days_to_keep = housekeeping_config.AIRFLOW_LOGS_DAYS_TO_KEEP
    max_file_size = housekeeping_config.AIRFLOW_LOGS_MAX_FILE_SIZE
    airflow_logs_dir = Path('/opt/airflow/logs')
    
    # PERFORMANCE LIMITS - Prevent infinite execution (configurable)
    MAX_EXECUTION_TIME_SECONDS = housekeeping_config.AIRFLOW_LOGS_MAX_EXECUTION_TIME_SECONDS
    MAX_DIRS_TO_PROCESS = housekeeping_config.AIRFLOW_LOGS_MAX_DIRS_TO_PROCESS
    BATCH_PROGRESS_INTERVAL = housekeeping_config.AIRFLOW_LOGS_BATCH_PROGRESS_INTERVAL
    
    start_time = time.time()
    
    wl(f"Starting Airflow logs cleanup: days_to_keep={days_to_keep}, max_time={MAX_EXECUTION_TIME_SECONDS}s, max_dirs={MAX_DIRS_TO_PROCESS}", "INFO")

    if not airflow_logs_dir.exists():
        wl("Airflow logs directory does not exist", "WARNING")
        return "Airflow logs directory does not exist"

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
    min_age = datetime.now(timezone.utc) - timedelta(hours=housekeeping_config.MIN_FILE_AGE_HOURS)
    
    # Statistics
    stats = {
        "dirs_processed": 0,
        "dirs_deleted": 0,
        "files_deleted": 0,
        "space_freed_bytes": 0,
        "skipped_large": 0,
        "skipped_recent": 0,
        "timeout_reached": False,
        "limit_reached": False
    }
    
    def get_dir_size_fast(path: Path) -> int:
        """Fast directory size calculation using subprocess du command"""
        try:
            result = subprocess.run(
                ['du', '-sb', str(path)],
                capture_output=True,
                text=True,
                timeout=10  # Timeout for individual du command
            )
            if result.returncode == 0:
                size_str = result.stdout.split()[0]
                return int(size_str)
        except Exception as e:
            logger.debug(f"du command failed for {path}, using fallback: {e}")
        
        # Fallback to stat-based estimation (fast, less accurate)
        try:
            return path.stat().st_size * 100  # Rough estimate
        except Exception:
            return 0
    
    def should_continue() -> bool:
        """Check if we should continue processing"""
        elapsed = time.time() - start_time
        if elapsed > MAX_EXECUTION_TIME_SECONDS:
            stats["timeout_reached"] = True
            return False
        if stats["dirs_processed"] >= MAX_DIRS_TO_PROCESS:
            stats["limit_reached"] = True
            return False
        return True
    
    try:
        # ===== Clean DAG run directories =====
        dag_dirs = [d for d in airflow_logs_dir.iterdir() if d.is_dir() and d.name.startswith('dag_id=')]
        wl(f"Found {len(dag_dirs)} DAG directories to scan", "INFO")
        
        for dag_dir in dag_dirs:
            if not should_continue():
                break
            
            try:
                run_dirs = [d for d in dag_dir.iterdir() if d.is_dir() and d.name.startswith('run_id=')]
                
                for run_dir in run_dirs:
                    if not should_continue():
                        break
                    
                    stats["dirs_processed"] += 1
                    
                    # Log progress periodically
                    if stats["dirs_processed"] % BATCH_PROGRESS_INTERVAL == 0:
                        elapsed = int(time.time() - start_time)
                        wl(f"Progress: {stats['dirs_processed']} dirs processed, {stats['dirs_deleted']} deleted, {elapsed}s elapsed", "INFO")
                    
                    try:
                        # Extract timestamp from run_id
                        run_name = run_dir.name
                        timestamp_str = None
                        
                        if 'scheduled__' in run_name:
                            timestamp_str = run_name.split('scheduled__')[1]
                        elif 'manual__' in run_name:
                            timestamp_str = run_name.split('manual__')[1]
                        else:
                            continue
                        
                        run_time = datetime.fromisoformat(timestamp_str.replace('+00:00', '+00:00'))
                        
                        # Check if old enough
                        if run_time >= cutoff_date:
                            continue
                        
                        # Check modification time
                        dir_mtime = datetime.fromtimestamp(run_dir.stat().st_mtime, tz=timezone.utc)
                        if dir_mtime >= min_age:
                            stats["skipped_recent"] += 1
                            continue
                        
                        # Fast size check using du command
                        dir_size = get_dir_size_fast(run_dir)
                        
                        # Size limit check
                        # if dir_size > parse_size(max_file_size):
                        #    stats["skipped_large"] += 1
                        #    if stats["dirs_processed"] % BATCH_PROGRESS_INTERVAL == 0:
                        #        logger.debug(f"Skipping large dir: {run_dir.name} ({dir_size / 1024 / 1024:.1f} MB)")
                        #    continue
                        
                        # DELETE IT
                        shutil.rmtree(run_dir)
                        stats["dirs_deleted"] += 1
                        stats["space_freed_bytes"] += dir_size
                        
                    except (ValueError, AttributeError, OSError) as e:
                        logger.debug(f"Could not process {run_dir.name}: {e}")
                        continue
                        
            except Exception as e:
                logger.warning(f"Error processing DAG directory {dag_dir.name}: {e}")
                continue
        
        # ===== Clean scheduler directories =====
        scheduler_dir = airflow_logs_dir / 'scheduler'
        if scheduler_dir.exists() and should_continue():
            wl("Processing scheduler logs", "INFO")
            
            try:
                date_dirs = [d for d in scheduler_dir.iterdir() 
                           if d.is_dir() and d.name != 'latest' and re.match(r'\d{4}-\d{2}-\d{2}', d.name)]
                
                for date_dir in date_dirs:
                    if not should_continue():
                        break
                    
                    stats["dirs_processed"] += 1
                    
                    try:
                        dir_date = datetime.strptime(date_dir.name, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                        
                        if dir_date >= cutoff_date:
                            continue
                        
                        dir_mtime = datetime.fromtimestamp(date_dir.stat().st_mtime, tz=timezone.utc)
                        if dir_mtime >= min_age:
                            stats["skipped_recent"] += 1
                            continue
                        
                        dir_size = get_dir_size_fast(date_dir)
                        
                        # if dir_size > parse_size(max_file_size):
                        #    stats["skipped_large"] += 1
                        #    continue
                        
                        # DELETE IT
                        shutil.rmtree(date_dir)
                        stats["dirs_deleted"] += 1
                        stats["space_freed_bytes"] += dir_size
                        
                    except (ValueError, OSError) as e:
                        logger.debug(f"Could not process scheduler dir {date_dir.name}: {e}")
                        continue
                        
            except Exception as e:
                logger.warning(f"Error processing scheduler directory: {e}")
        
        # ===== Clean DAG processor logs =====
        dag_processor_dir = airflow_logs_dir / 'dag_processor_manager'
        if dag_processor_dir.exists() and should_continue():
            wl("Processing DAG processor logs", "INFO")
            
            try:
                log_files = list(dag_processor_dir.rglob('*.log'))
                
                for log_file in log_files:
                    if not should_continue():
                        break
                    
                    stats["dirs_processed"] += 1
                    
                    try:
                        mtime = datetime.fromtimestamp(log_file.stat().st_mtime, tz=timezone.utc)
                        
                        if mtime >= cutoff_date or mtime >= min_age:
                            continue
                        
                        file_size = log_file.stat().st_size
                        
                        if file_size > parse_size(max_file_size):
                            stats["skipped_large"] += 1
                            continue
                        
                        # DELETE IT
                        log_file.unlink()
                        stats["files_deleted"] += 1
                        stats["space_freed_bytes"] += file_size
                        
                    except OSError as e:
                        logger.debug(f"Could not process log file {log_file.name}: {e}")
                        continue
                        
            except Exception as e:
                logger.warning(f"Error processing DAG processor logs: {e}")
        
        # Final statistics
        elapsed_time = int(time.time() - start_time)
        space_freed_mb = round(stats["space_freed_bytes"] / 1024 / 1024, 2)
        
        result = {
            "dirs_processed": stats["dirs_processed"],
            "dirs_deleted": stats["dirs_deleted"],
            "files_deleted": stats["files_deleted"],
            "space_freed_bytes": stats["space_freed_bytes"],
            "space_freed_mb": space_freed_mb,
            "skipped_large": stats["skipped_large"],
            "skipped_recent": stats["skipped_recent"],
            "timeout_reached": stats["timeout_reached"],
            "limit_reached": stats["limit_reached"],
            "execution_time_seconds": elapsed_time,
            "days_to_keep": days_to_keep,
            "summary": f"Processed {stats['dirs_processed']} items, deleted {stats['dirs_deleted']} dirs + {stats['files_deleted']} files, freed {space_freed_mb} MB in {elapsed_time}s"
        }
        
        # Add warnings if limits were reached
        if stats["timeout_reached"]:
            result["warning"] = f"Cleanup stopped after {elapsed_time}s timeout - may need another run"
            wl(f"WARNING: Timeout reached after {elapsed_time}s", "WARNING")
        elif stats["limit_reached"]:
            result["warning"] = f"Cleanup stopped after processing {stats['dirs_processed']} items - may need another run"
            wl(f"WARNING: Directory limit reached", "WARNING")
        
        wl(f"Airflow logs cleanup completed: {result['summary']}", "INFO")
        wl(json.dumps({"cleanup_stats": result}), "INFO")
        context['ti'].xcom_push(key='cleanup_result', value=result)
        
        return result

    except Exception as e:
        elapsed = int(time.time() - start_time)
        logger.error(f"Error cleaning Airflow logs after {elapsed}s: {e}")
        wl(f"Error during Airflow logs cleanup: {e}", "ERROR")
        raise


@task
def clean_task_history(**context):
    """Retire terminal TM_History rows and child stages in committed batches."""
    import time
    
    # Master kill switch check for database cleanup
    if not housekeeping_config.ENABLE_DATABASE_CLEANUP:
        logger.info("Task history cleanup skipped - master database flag disabled (ENABLE_DATABASE_CLEANUP=False)")
        return "Database cleanup disabled by master flag"
    
    # Granular task-specific check
    policy = policy_for(context, 'task_history')
    if not policy['enabled']:
        logger.info("Task history cleanup skipped - task-specific flag disabled (ENABLE_TASK_HISTORY_CLEANUP=False)")
        return "Task history cleanup disabled by task-specific flag"

    connection_id = os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db')
    days_to_keep = policy['retention_days']
    batch_size = int(getattr(housekeeping_config, 'TASK_HISTORY_BATCH_SIZE', getattr(housekeeping_config, 'BATCH_SIZE', 1000)))
    max_iterations = int(getattr(housekeeping_config, 'TASK_HISTORY_MAX_ITERATIONS', getattr(housekeeping_config, 'MAX_ITERATIONS', 100)))
    table_swap_mode = getattr(housekeeping_config, 'TASK_HISTORY_TABLE_SWAP_MODE', False)
    if table_swap_mode:
        raise RuntimeError('Task history table swap is unsafe with retirement triggers and foreign keys')

    db = _connect_oscar_db(connection_id)
    cursor = db.cursor()

    # Setup worklog hook
    worklog_hook = None
    try:
        worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id') if context else None
        worklog_hook = WorkLogHook()
        if worklog_id:
            worklog_hook.set_worklog_id(worklog_id)
    except Exception:
        worklog_hook = None

    def wl(message: str, level: str = "INFO") -> None:
        wl_write(worklog_hook, message, level)

    try:
        dry_run = getattr(housekeeping_config, 'DRY_RUN_MODE', False)
        maintenance_optimization = getattr(housekeeping_config, 'TASK_HISTORY_MAINTENANCE_OPTIMIZATION', False)
        cleanup_mode = "TABLE SWAP" if table_swap_mode else ("NORMAL DELETE + OPTIMIZE" if maintenance_optimization else "NORMAL DELETE")
        mode_str = f"DRY RUN ({cleanup_mode})" if dry_run else f"LIVE MODE ({cleanup_mode})"
        wl(f"Starting TaskHistory cleanup [{mode_str}]: days_to_keep={days_to_keep}", "INFO")
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        start_time = time.time()

        # PRE-FLIGHT CHECK: Count what will be deleted and get table sizes
        cursor.execute("""
            SELECT ROUND(data_length / 1024 / 1024 / 1024, 2) as size_gb
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = 'TM_History'
        """)
        history_size_gb = cursor.fetchone()[0] or 0
        
        cursor.execute("""
            SELECT ROUND(data_length / 1024 / 1024 / 1024, 2) as size_gb
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = 'TM_StageHistory'
        """)
        stage_size_gb = cursor.fetchone()[0] or 0
        
        wl(f"Current sizes: TM_History={history_size_gb} GB, TM_StageHistory={stage_size_gb} GB", "INFO")

        # A full COUNT over years of history is avoidable. This is a lower
        # bound once the run's deletion capacity has been reached.
        count_limit = batch_size * max_iterations + 1
        history_to_delete = bounded_eligible_count(
            cursor, 'TM_History',
            "created_at < %s AND state IN ('SUCCESS','FAILURE','REVOKED','EXPIRED')",
            (cutoff_date_str,), count_limit,
        )
        rows_to_keep = None  # The retained range need not be scanned for cleanup.
        wl(f"Eligible task history rows: at least {history_to_delete:,}", "INFO")
        
        if history_to_delete == 0:
            wl("No records to delete - cleanup not needed", "INFO")
            return {"total_tasks_deleted": 0, "rows_deleted": 0, "eligible_backlog": 0,
                    "dry_run": dry_run, "summary": "No records older than retention period"}

        if dry_run:
            result = {
                "dry_run": True,
                "mode": cleanup_mode,
                "rows_to_delete": history_to_delete,
                "rows_to_keep": rows_to_keep,
                "backlog_count_capped": history_to_delete == count_limit,
                "summary": f"[DRY RUN - {cleanup_mode}] Would delete {history_to_delete:,} records"
            }
            wl(f"[DRY RUN] {result['summary']}", "INFO")
            context['ti'].xcom_push(key='task_history_cleanup_result', value=result)
            return result

        # ========================================
        # BRANCH: TABLE SWAP vs NORMAL DELETE
        # ========================================
        if table_swap_mode:
            # ========================================
            # TABLE SWAP MODE - Actually reclaim disk space
            # ========================================
            wl("Using TABLE SWAP mode (disk space will be freed)", "INFO")
            
            # Use READ UNCOMMITTED to avoid locking during copy
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            
            # Step 1: Create new tables
            wl("Step 1: Creating new tables...", "INFO")
            cursor.execute("DROP TABLE IF EXISTS TM_StageHistory_new")
            cursor.execute("DROP TABLE IF EXISTS TM_History_new")
            cursor.execute("DROP TABLE IF EXISTS TM_StageHistory_old")
            cursor.execute("DROP TABLE IF EXISTS TM_History_old")
            db.commit()
            
            cursor.execute("CREATE TABLE TM_History_new LIKE TM_History")
            cursor.execute("CREATE TABLE TM_StageHistory_new LIKE TM_StageHistory")
            db.commit()
            
            # Step 2: Copy recent TM_History data
            wl(f"Step 2: Copying {rows_to_keep:,} recent TM_History records...", "INFO")
            copy_batch = 50000
            copied = 0
            last_id = None
            
            while True:
                if last_id is None:
                    cursor.execute("""
                        INSERT INTO TM_History_new
                        SELECT * FROM TM_History WHERE created_at >= %s ORDER BY id LIMIT %s
                    """, (cutoff_date_str, copy_batch))
                else:
                    cursor.execute("""
                        INSERT INTO TM_History_new
                        SELECT * FROM TM_History WHERE created_at >= %s AND id > %s ORDER BY id LIMIT %s
                    """, (cutoff_date_str, last_id, copy_batch))
                
                rows_copied = cursor.rowcount
                copied += rows_copied
                db.commit()
                
                if rows_copied == 0:
                    break
                
                cursor.execute("SELECT MAX(id) FROM TM_History_new")
                last_id = cursor.fetchone()[0]
                
                if copied % 100000 < copy_batch:
                    wl(f"Copied {copied:,} TM_History records...", "INFO")
            
            wl(f"Copied {copied:,} TM_History records", "INFO")
            
            # Step 3: Copy related TM_StageHistory data
            wl("Step 3: Copying related TM_StageHistory records...", "INFO")
            cursor.execute("""
                INSERT INTO TM_StageHistory_new
                SELECT tsh.* FROM TM_StageHistory tsh
                INNER JOIN TM_History_new th ON tsh.task_history_id = th.id
            """)
            stage_copied = cursor.rowcount
            db.commit()
            wl(f"Copied {stage_copied:,} TM_StageHistory records", "INFO")
            
            # Reset isolation level
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            
            # Step 4: Atomic table swap
            wl("Step 4: Performing atomic table swap (RENAME)...", "INFO")
            wl("WARNING: Writes may fail during this millisecond operation", "WARNING")
            
            # Must rename in correct order due to FK constraints
            # First rename child table, then parent
            cursor.execute("""
                RENAME TABLE 
                    TM_StageHistory TO TM_StageHistory_old,
                    TM_StageHistory_new TO TM_StageHistory,
                    TM_History TO TM_History_old,
                    TM_History_new TO TM_History
            """)
            db.commit()
            wl("Table swap completed", "INFO")
            
            # Step 5: Drop old tables
            wl("Step 5: Dropping old tables to reclaim space...", "INFO")
            cursor.execute("DROP TABLE TM_StageHistory_old")
            cursor.execute("DROP TABLE TM_History_old")
            db.commit()
            wl("Old tables dropped - disk space reclaimed!", "INFO")
            
            # Get new sizes
            cursor.execute("""
                SELECT ROUND(data_length / 1024 / 1024 / 1024, 2)
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = 'TM_History'
            """)
            new_history_size = cursor.fetchone()[0] or 0
            
            cursor.execute("""
                SELECT ROUND(data_length / 1024 / 1024 / 1024, 2)
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = 'TM_StageHistory'
            """)
            new_stage_size = cursor.fetchone()[0] or 0
            
            space_freed = (history_size_gb + stage_size_gb) - (new_history_size + new_stage_size)
            elapsed_time = int(time.time() - start_time)
            
            result = {
                "dry_run": False,
                "mode": "TABLE SWAP",
                "original_size_gb": history_size_gb + stage_size_gb,
                "new_size_gb": new_history_size + new_stage_size,
                "space_freed_gb": round(space_freed, 2),
                "rows_deleted": history_to_delete,
                "execution_time_seconds": elapsed_time,
                "summary": f"Freed {round(space_freed, 2)} GB in {elapsed_time}s via table swap"
            }
            wl(f"TaskHistory cleanup completed: {result['summary']}", "INFO")
            wl(json.dumps({"cleanup_stats": result}), "INFO")
            context['ti'].xcom_push(key='task_history_cleanup_result', value=result)
            return result

        # ========================================
        # NORMAL DELETE MODE - Batch delete
        # ========================================
        wl("Using NORMAL DELETE mode (space reused, not freed to OS)", "INFO")
        
        # Timeout protection (in addition to Airflow's execution_timeout)
        task_timeout = getattr(housekeeping_config, 'TASK_HISTORY_TIMEOUT_MINUTES', 60)
        check_timeout = create_timeout_checker("clean_task_history", task_timeout)

        iteration_count = 0
        total_tasks_deleted = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                print(f"Reached maximum iterations ({max_iterations}). Breaking loop after deleting {total_tasks_deleted} total TaskHistory records.")
                wl(f"Stopped: max_iterations limit reached ({max_iterations})", "WARNING")
                break
            
            # Timeout check (safety layer)
            if check_timeout():
                print(f"Timeout reached. Stopping after {iteration_count} iterations and {total_tasks_deleted} deletions.")
                wl(f"Stopped: timeout limit reached ({task_timeout} minutes)", "WARNING")
                break

            # Two-step DELETE (MySQL doesn't support LIMIT with multi-table DELETE)
            # STEP 1: Select IDs to delete (with LIMIT)
            # STEP 2: Delete those IDs from both tables
            
            # STEP 1: Get IDs to delete
            select_ids_query = """
            SELECT th.id
            FROM TM_History th
            WHERE th.created_at < %s
              AND th.state IN ('SUCCESS','FAILURE','REVOKED','EXPIRED')
            ORDER BY th.created_at, th.id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """
            cursor.execute(select_ids_query, (cutoff_date_str, batch_size))
            ids_to_delete = [row[0] for row in cursor.fetchall()]
            
            task_history_deleted = 0
            if ids_to_delete:
                # STEP 2: Delete children first, then parent (separate queries for FK safety)
                placeholders = ','.join(['%s'] * len(ids_to_delete))
                
                # STEP 2A: Delete children (TM_StageHistory)
                delete_stages_query = f"""
                DELETE FROM TM_StageHistory
                WHERE task_history_id IN ({placeholders})
                """
                cursor.execute(delete_stages_query, ids_to_delete)
                
                # STEP 2B: Delete parent (TM_History) - now safe, no children exist
                delete_history_query = f"""
                DELETE FROM TM_History
                WHERE id IN ({placeholders})
                """
                cursor.execute(delete_history_query, ids_to_delete)
                task_history_deleted = cursor.rowcount

            total_tasks_deleted += task_history_deleted

            if task_history_deleted == 0:
                print(f"No TaskHistory records were deleted in this iteration. Breaking loop after {iteration_count} iterations.")
                break

            if dry_run:
                db.rollback()  # 🔒 DRY RUN: Rollback instead of commit
                print(f"[DRY RUN] Would delete {task_history_deleted} records (TM_History + TM_StageHistory). Total so far: {total_tasks_deleted}")
                wl(f"[DRY RUN] Iteration {iteration_count}/{max_iterations}: Would delete {task_history_deleted} records. Total: {total_tasks_deleted}", "INFO")
            else:
                db.commit()  # ✅ LIVE MODE: Actually commit deletions
                print(f"Iteration {iteration_count}/{max_iterations}: Deleted {task_history_deleted} records (TM_History + associated TM_StageHistory). Total deleted so far: {total_tasks_deleted}")
                wl(f"Iteration {iteration_count}/{max_iterations}: Deleted {task_history_deleted} records (TM_History + TM_StageHistory). Total so far: {total_tasks_deleted}", "INFO")

        # The TM_StageHistory.task_history_id foreign key enforces this
        # relationship on each batch commit; a full-table orphan scan would
        # read the entire stage-history table on every production run.
        wl("Stage-history integrity enforced by database foreign key", "INFO")
        
        # Finalize and push result to XCom
        result = {
            "dry_run": dry_run,
            "total_tasks_deleted": total_tasks_deleted,
            "rows_deleted": total_tasks_deleted,
            "eligible_backlog": max(0, history_to_delete - total_tasks_deleted),
            "backlog_count_capped": history_to_delete == count_limit,
            "days_to_keep": days_to_keep,
            "batch_size": batch_size,
            "max_iterations": max_iterations,
            "orphan_check_passed": True,
            "summary": f"{'[DRY RUN] Would delete' if dry_run else 'Deleted'} {total_tasks_deleted} TaskHistory records",
        }
        wl(f"TaskHistory cleanup completed: {result['summary']}", "INFO")
        wl(json.dumps({"cleanup_stats": result}), "INFO")

        # MAINTENANCE OPTIMIZATION: Run OPTIMIZE TABLE after normal delete to reclaim disk space
        if not dry_run and not table_swap_mode and maintenance_optimization and total_tasks_deleted > 0:
            optimize_start = time.time()
            elapsed_so_far = int(optimize_start - start_time)
            wl(f"Running OPTIMIZE TABLE to reclaim disk space (delete took {elapsed_so_far}s)...", "INFO")
            wl(f"Task timeout: {task_timeout}min, elapsed: {elapsed_so_far}s — OPTIMIZE starting", "INFO")
            try:
                cursor.execute("OPTIMIZE TABLE TM_History")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE TM_StageHistory")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                # Refresh information_schema stats so table sizes are accurate
                cursor.execute("ANALYZE TABLE TM_History")
                cursor.fetchall()
                cursor.execute("ANALYZE TABLE TM_StageHistory")
                cursor.fetchall()
                db.commit()
                optimize_duration = int(time.time() - optimize_start)
                total_elapsed = int(time.time() - start_time)
                wl(f"OPTIMIZE completed in {optimize_duration}s (total task time: {total_elapsed}s) - disk space reclaimed!", "INFO")
                result["optimization_completed"] = True
                result["optimization_duration_seconds"] = optimize_duration
                result["total_execution_seconds"] = total_elapsed
            except Exception as e:
                wl(f"OPTIMIZE TABLE failed (non-fatal) after {int(time.time() - optimize_start)}s: {e}", "ERROR")
                result["optimization_completed"] = False
                result["optimization_error"] = str(e)

        context['ti'].xcom_push(key='task_history_cleanup_result', value=result)
        return result

    except Exception as e:
        db.rollback()
        print(f"An error occurred: {str(e)}")
        wl(f"Error during TaskHistory cleanup: {str(e)}", "ERROR")
        raise
    finally:
        cursor.close()
        db.close()


@task
def clean_alert_history(**context):
    """
    ⚠️ LEGACY TASK - DISABLED BY DEFAULT ⚠️
    
    Cleans legacy AM_Alert tables that are NO LONGER USED in current OSCAR architecture.
    These models have been REMOVED from oscar-alertmanager (see core/db.py).
    
    Current architecture uses AM_AlertHistory tables (cleaned by clean_alert_history_records task).
    
    To enable this task (if you have legacy data to clean):
        Set ENABLE_ALERT_CLEANUP = True in oscar_housekeeping_config.py
    
    Legacy tables cleaned:
        - AM_Alert, AM_AlertLabel, AM_AlertAnnotation
        - AM_AlertGroup, AM_CommonLabel, AM_GroupLabel, AM_CommonAnnotation
    """
    # Master kill switch check for database cleanup
    if not housekeeping_config.ENABLE_DATABASE_CLEANUP:
        logger.info("Alert cleanup skipped - master database flag disabled (ENABLE_DATABASE_CLEANUP=False)")
        return "Database cleanup disabled by master flag"
    
    # Granular task-specific check
    if not housekeeping_config.ENABLE_ALERT_CLEANUP:
        logger.info("Alert cleanup skipped - LEGACY task disabled by default (ENABLE_ALERT_CLEANUP=False)")
        return "LEGACY task disabled - AM_Alert tables no longer used"

    connection_id = os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db')
    days_to_keep = int(getattr(housekeeping_config, 'ALERTS_DAYS_TO_KEEP', 7))
    batch_size = int(getattr(housekeeping_config, 'ALERTS_BATCH_SIZE', 1000))

    db = _connect_oscar_db(connection_id)
    cursor = db.cursor()

    # Setup worklog hook
    worklog_hook = None
    try:
        worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id') if context else None
        worklog_hook = WorkLogHook()
        if worklog_id:
            worklog_hook.set_worklog_id(worklog_id)
    except Exception:
        worklog_hook = None

    def wl(message: str, level: str = "INFO") -> None:
        wl_write(worklog_hook, message, level)

    try:
        dry_run = getattr(housekeeping_config, 'DRY_RUN_MODE', False)
        mode_str = "DRY RUN (no commits)" if dry_run else "LIVE MODE (will delete)"
        wl(f"Starting Alert cleanup [{mode_str}]: days_to_keep={days_to_keep}, batch_size={batch_size}", "INFO")
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        # Timeout protection (in addition to Airflow's execution_timeout)
        task_timeout = getattr(housekeeping_config, 'TASK_TIMEOUT_MINUTES', 60)
        check_timeout = create_timeout_checker("clean_alert_history", task_timeout)

        # First loop - Delete alerts and their associated records
        max_iterations = int(getattr(housekeeping_config, 'ALERTS_MAX_ITERATIONS', 100))
        iteration_count = 0
        total_alerts_deleted = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                print(f"Reached maximum iterations ({max_iterations}). Breaking loop after deleting {total_alerts_deleted} total Alerts.")
                wl(f"Stopped: max_iterations limit reached ({max_iterations})", "WARNING")
                break
            
            # Timeout check (safety layer)
            if check_timeout():
                print(f"Timeout reached. Stopping after {iteration_count} iterations and {total_alerts_deleted} deletions.")
                wl(f"Stopped: timeout limit reached ({task_timeout} minutes)", "WARNING")
                break

            # Two-step DELETE (MySQL doesn't support LIMIT with multi-table DELETE)
            # STEP 1: Select IDs to delete (with LIMIT)
            # STEP 2: Delete those IDs from all tables
            
            # STEP 1: Get IDs to delete
            select_ids_query = """
            SELECT a.ID
            FROM AM_Alert a
            WHERE a.endsAt < %s
            LIMIT %s
            """
            cursor.execute(select_ids_query, (cutoff_date_str, batch_size))
            ids_to_delete = [row[0] for row in cursor.fetchall()]
            
            alerts_deleted = 0
            if ids_to_delete:
                # STEP 2: Delete children first, then parent (separate queries for FK safety)
                placeholders = ','.join(['%s'] * len(ids_to_delete))
                
                # STEP 2A: Delete children (AM_AlertLabel)
                delete_labels_query = f"""
                DELETE FROM AM_AlertLabel
                WHERE AlertID IN ({placeholders})
                """
                cursor.execute(delete_labels_query, ids_to_delete)
                
                # STEP 2B: Delete children (AM_AlertAnnotation)
                delete_annotations_query = f"""
                DELETE FROM AM_AlertAnnotation
                WHERE AlertID IN ({placeholders})
                """
                cursor.execute(delete_annotations_query, ids_to_delete)
                
                # STEP 2C: Delete parent (AM_Alert) - now safe, no children exist
                delete_alerts_query = f"""
                DELETE FROM AM_Alert
                WHERE ID IN ({placeholders})
                """
                cursor.execute(delete_alerts_query, ids_to_delete)
                alerts_deleted = cursor.rowcount

            total_alerts_deleted += alerts_deleted

            if alerts_deleted == 0:
                print(f"No Alerts were deleted in this iteration. Breaking loop after {iteration_count} iterations.")
                break

            if dry_run:
                db.rollback()  # 🔒 DRY RUN: Rollback instead of commit
                print(f"[DRY RUN] Would delete {alerts_deleted} Alerts (+ labels/annotations). Total so far: {total_alerts_deleted}")
                wl(f"[DRY RUN] Iteration {iteration_count}/{max_iterations}: Would delete {alerts_deleted} Alerts. Total: {total_alerts_deleted}", "INFO")
            else:
                db.commit()  # ✅ LIVE MODE: Actually commit deletions
                print(f"Iteration {iteration_count}/{max_iterations}: Deleted {alerts_deleted} Alerts (+ labels/annotations). Total deleted so far: {total_alerts_deleted}")
                wl(f"Iteration {iteration_count}/{max_iterations}: Deleted {alerts_deleted} Alerts. Total so far: {total_alerts_deleted}", "INFO")

        # After deleting alerts, clean up empty alert groups and associated records
        max_iterations = int(os.getenv('OSCAR_ALERT_MAX_ITERATIONS', 100))
        iteration_count = 0
        total_groups_deleted = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                print(f"Reached maximum iterations ({max_iterations}). Breaking loop after deleting {total_groups_deleted} total AlertGroups.")
                wl(f"Stopped: max_iterations limit reached ({max_iterations})", "WARNING")
                break
            
            # Timeout check (safety layer)
            if check_timeout():
                print(f"Timeout reached. Stopping after {iteration_count} iterations and {total_groups_deleted} deletions.")
                wl(f"Stopped: timeout limit reached ({task_timeout} minutes)", "WARNING")
                break

            # Two-step DELETE (MySQL doesn't support LIMIT with multi-table DELETE)
            # STEP 1: Select IDs to delete (with LIMIT)
            # STEP 2: Delete those IDs from all tables
            
            # STEP 1: Get IDs to delete
            select_ids_query = """
            SELECT ag.ID
            FROM AM_AlertGroup ag
            WHERE ag.ID NOT IN (SELECT DISTINCT alertGroupID FROM AM_Alert)
            LIMIT %s
            """
            cursor.execute(select_ids_query, (batch_size,))
            ids_to_delete = [row[0] for row in cursor.fetchall()]
            
            alert_groups_deleted = 0
            if ids_to_delete:
                # STEP 2: Delete children first, then parent (separate queries for FK safety)
                placeholders = ','.join(['%s'] * len(ids_to_delete))
                
                # STEP 2A: Delete children (AM_CommonLabel)
                delete_common_labels_query = f"""
                DELETE FROM AM_CommonLabel
                WHERE alertGroupID IN ({placeholders})
                """
                cursor.execute(delete_common_labels_query, ids_to_delete)
                
                # STEP 2B: Delete children (AM_GroupLabel)
                delete_group_labels_query = f"""
                DELETE FROM AM_GroupLabel
                WHERE alertGroupID IN ({placeholders})
                """
                cursor.execute(delete_group_labels_query, ids_to_delete)
                
                # STEP 2C: Delete children (AM_CommonAnnotation)
                delete_common_annotations_query = f"""
                DELETE FROM AM_CommonAnnotation
                WHERE alertGroupID IN ({placeholders})
                """
                cursor.execute(delete_common_annotations_query, ids_to_delete)
                
                # STEP 2D: Delete parent (AM_AlertGroup) - now safe, no children exist
                delete_alert_groups_query = f"""
                DELETE FROM AM_AlertGroup
                WHERE ID IN ({placeholders})
                """
                cursor.execute(delete_alert_groups_query, ids_to_delete)
                alert_groups_deleted = cursor.rowcount

            total_groups_deleted += alert_groups_deleted

            if alert_groups_deleted == 0:
                print(f"No AlertGroups were deleted in this iteration. Breaking loop after {iteration_count} iterations.")
                break

            if dry_run:
                db.rollback()  # 🔒 DRY RUN: Rollback instead of commit
                print(f"[DRY RUN] Would delete {alert_groups_deleted} AlertGroups (+ labels/annotations). Total so far: {total_groups_deleted}")
                wl(f"[DRY RUN] Iteration {iteration_count}/{max_iterations}: Would delete {alert_groups_deleted} AlertGroups. Total: {total_groups_deleted}", "INFO")
            else:
                db.commit()  # ✅ LIVE MODE: Actually commit deletions
                print(f"Iteration {iteration_count}/{max_iterations}: Deleted {alert_groups_deleted} AlertGroups (+ labels/annotations). Total deleted so far: {total_groups_deleted}")
                wl(f"Iteration {iteration_count}/{max_iterations}: Deleted {alert_groups_deleted} AlertGroups. Total so far: {total_groups_deleted}", "INFO")

        # Finalize and push result to XCom
        result_alerts = {
            "dry_run": dry_run,
            "total_alerts_deleted": total_alerts_deleted,
            "total_alert_groups_deleted": total_groups_deleted,
            "days_to_keep": days_to_keep,
            "batch_size": batch_size,
            "max_iterations": max_iterations,
            "summary": f"{'[DRY RUN] Would delete' if dry_run else 'Deleted'} {total_alerts_deleted} Alerts and {total_groups_deleted} AlertGroups",
        }
        wl(f"Alert cleanup completed: {result_alerts['summary']}", "INFO")
        wl(json.dumps({"cleanup_stats": result_alerts}), "INFO")

        # MAINTENANCE OPTIMIZATION: Run OPTIMIZE TABLE after delete to reclaim disk space
        maintenance_optimization = getattr(housekeeping_config, 'ALERTS_MAINTENANCE_OPTIMIZATION', False)
        if not dry_run and maintenance_optimization and (total_alerts_deleted > 0 or total_groups_deleted > 0):
            wl("Running OPTIMIZE TABLE to reclaim disk space (ALERTS_MAINTENANCE_OPTIMIZATION=True)...", "INFO")
            try:
                cursor.execute("OPTIMIZE TABLE AM_Alert")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE AM_AlertLabel")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE AM_AlertAnnotation")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE AM_AlertGroup")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE AM_CommonLabel")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE AM_GroupLabel")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE AM_CommonAnnotation")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                # Refresh information_schema stats so table sizes are accurate
                for _tbl in ['AM_Alert','AM_AlertLabel','AM_AlertAnnotation','AM_AlertGroup','AM_CommonLabel','AM_GroupLabel','AM_CommonAnnotation']:
                    cursor.execute(f"ANALYZE TABLE {_tbl}")
                    cursor.fetchall()
                db.commit()
                wl("Table optimization completed - disk space reclaimed!", "INFO")
                result_alerts["optimization_completed"] = True
            except Exception as e:
                wl(f"OPTIMIZE TABLE failed (non-fatal): {e}", "ERROR")
                result_alerts["optimization_completed"] = False
                result_alerts["optimization_error"] = str(e)

        context['ti'].xcom_push(key='alert_cleanup_result', value=result_alerts)

    except Exception as e:
        db.rollback()
        print(f"An error occurred: {str(e)}")
        wl(f"Error during Alert cleanup: {str(e)}", "ERROR")
        raise
    finally:
        cursor.close()
        db.close()

@task
def clean_alert_history_records(**context):
    # Master kill switch check for database cleanup
    if not housekeeping_config.ENABLE_DATABASE_CLEANUP:
        logger.info("Alert history cleanup skipped - master database flag disabled (ENABLE_DATABASE_CLEANUP=False)")
        return "Database cleanup disabled by master flag"
    
    # Granular task-specific check
    policy = policy_for(context, 'alert_history')
    if not policy['enabled']:
        logger.info("Alert history cleanup skipped - task-specific flag disabled (ENABLE_ALERT_HISTORY_CLEANUP=False)")
        return "Alert history cleanup disabled by task-specific flag"

    connection_id = os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db')
    days_to_keep = policy['retention_days']
    batch_size = int(getattr(housekeeping_config, 'ALERT_HISTORY_BATCH_SIZE', 1000))

    db = _connect_oscar_db(connection_id)
    cursor = db.cursor()

    # Setup worklog hook
    worklog_hook = None
    try:
        worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id') if context else None
        worklog_hook = WorkLogHook()
        if worklog_id:
            worklog_hook.set_worklog_id(worklog_id)
    except Exception:
        worklog_hook = None

    def wl(message: str, level: str = "INFO") -> None:
        wl_write(worklog_hook, message, level)

    try:
        dry_run = getattr(housekeeping_config, 'DRY_RUN_MODE', False)
        mode_str = "DRY RUN (no commits)" if dry_run else "LIVE MODE (will delete)"
        wl(f"Starting AlertHistory cleanup [{mode_str}]: days_to_keep={days_to_keep}, batch_size={batch_size}", "INFO")
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        start_time = time.time()
        count_limit = batch_size * int(getattr(housekeeping_config, 'ALERT_HISTORY_MAX_ITERATIONS', 100)) + 1
        eligible_at_start = bounded_eligible_count(
            cursor, 'AM_AlertHistory', 'resolved_at < %s', (cutoff_date_str,), count_limit)
        if dry_run:
            result = {'dry_run': True, 'rows_to_delete': eligible_at_start, 'rows_deleted': 0,
                      'eligible_backlog': eligible_at_start, 'days_to_keep': days_to_keep,
                      'backlog_count_capped': eligible_at_start == count_limit,
                      'execution_time_seconds': time.time() - start_time}
            context['ti'].xcom_push(key='alert_history_cleanup_result', value=result)
            return result

        # Timeout protection (in addition to Airflow's execution_timeout)
        task_timeout = getattr(housekeeping_config, 'TASK_TIMEOUT_MINUTES', 60)
        check_timeout = create_timeout_checker("clean_alert_history_records", task_timeout)

        # Clean AlertHistory records in batches
        max_iterations = int(getattr(housekeeping_config, 'ALERT_HISTORY_MAX_ITERATIONS', 100))
        iteration_count = 0
        total_history_deleted = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                print(f"Reached maximum iterations ({max_iterations}). Breaking loop after deleting {total_history_deleted} total AlertHistory records.")
                wl(f"Stopped: max_iterations limit reached ({max_iterations})", "WARNING")
                break
            
            # Timeout check (safety layer)
            if check_timeout():
                print(f"Timeout reached. Stopping after {iteration_count} iterations and {total_history_deleted} deletions.")
                wl(f"Stopped: timeout limit reached ({task_timeout} minutes)", "WARNING")
                break

            # Lock closed episodes before deleting their children; a concurrent
            # reopen or receipt must finish before this eligibility decision.
            select_ids_query = """
            SELECT ah.ID
            FROM AM_AlertHistory ah
            WHERE ah.resolved_at < %s
            ORDER BY ah.resolved_at, ah.ID
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """
            cursor.execute(select_ids_query, (cutoff_date_str, batch_size))
            ids_to_delete = [row[0] for row in cursor.fetchall()]
            
            history_deleted = 0
            if ids_to_delete:
                # STEP 2: Delete children first, then parent (separate queries for FK safety)
                placeholders = ','.join(['%s'] * len(ids_to_delete))
                
                # STEP 2A: Delete children (AM_AlertHistoryLabel)
                delete_labels_query = f"""
                DELETE FROM AM_AlertHistoryLabel
                WHERE AlertHistoryID IN ({placeholders})
                """
                cursor.execute(delete_labels_query, ids_to_delete)
                
                # STEP 2B: Delete children (AM_AlertHistoryAnnotation)
                delete_annotations_query = f"""
                DELETE FROM AM_AlertHistoryAnnotation
                WHERE AlertHistoryID IN ({placeholders})
                """
                cursor.execute(delete_annotations_query, ids_to_delete)
                
                # STEP 2C: Delete parent (AM_AlertHistory) - now safe, no children exist
                delete_history_query = f"""
                DELETE FROM AM_AlertHistory
                WHERE ID IN ({placeholders})
                """
                cursor.execute(delete_history_query, ids_to_delete)
                history_deleted = cursor.rowcount

            total_history_deleted += history_deleted

            if history_deleted == 0:
                print(f"No AlertHistory records were deleted in this iteration. Breaking loop after {iteration_count} iterations.")
                break

            if dry_run:
                db.rollback()  # 🔒 DRY RUN: Rollback instead of commit
                print(f"[DRY RUN] Would delete {history_deleted} AlertHistory records (+ labels/annotations). Total so far: {total_history_deleted}")
                wl(f"[DRY RUN] Iteration {iteration_count}/{max_iterations}: Would delete {history_deleted} AlertHistory. Total: {total_history_deleted}", "INFO")
            else:
                db.commit()  # ✅ LIVE MODE: Actually commit deletions
                print(f"Iteration {iteration_count}/{max_iterations}: Deleted {history_deleted} AlertHistory records (+ labels/annotations). Total deleted so far: {total_history_deleted}")
                wl(f"Iteration {iteration_count}/{max_iterations}: Deleted {history_deleted} AlertHistory. Total so far: {total_history_deleted}", "INFO")

        # Finalize and push result to XCom
        result_history = {
            "dry_run": dry_run,
            "total_alert_history_deleted": total_history_deleted,
            "rows_deleted": total_history_deleted,
            "eligible_backlog": max(0, eligible_at_start - total_history_deleted),
            "backlog_count_capped": eligible_at_start == count_limit,
            "execution_time_seconds": time.time() - start_time,
            "days_to_keep": days_to_keep,
            "batch_size": batch_size,
            "max_iterations": max_iterations,
            "summary": f"{'[DRY RUN] Would delete' if dry_run else 'Deleted'} {total_history_deleted} AlertHistory records",
        }
        wl(f"AlertHistory cleanup completed: {result_history['summary']}", "INFO")
        wl(json.dumps({"cleanup_stats": result_history}), "INFO")

        # MAINTENANCE OPTIMIZATION: Run OPTIMIZE TABLE after delete to reclaim disk space
        maintenance_optimization = getattr(housekeeping_config, 'ALERT_HISTORY_MAINTENANCE_OPTIMIZATION', False)
        if not dry_run and maintenance_optimization and total_history_deleted > 0:
            wl("Running OPTIMIZE TABLE to reclaim disk space (ALERT_HISTORY_MAINTENANCE_OPTIMIZATION=True)...", "INFO")
            try:
                cursor.execute("OPTIMIZE TABLE AM_AlertHistory")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE AM_AlertHistoryLabel")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE AM_AlertHistoryAnnotation")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                _history_tables = ['AM_AlertHistory', 'AM_AlertHistoryLabel', 'AM_AlertHistoryAnnotation']
                # Long label values live in AM_AlertHistoryLabelAnchor. Its orphans are deleted by the
                # alertmanager's own nightly sweep (oscar-scheduler label_anchor_prune, 00:45 ->
                # tm.alerts.prune_label_anchors) -- the single owner of that cleanup. Housekeeping only
                # reclaims the table's disk space, and only on installs that have the table.
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_name = 'AM_AlertHistoryLabelAnchor'")
                if cursor.fetchone()[0]:
                    cursor.execute("OPTIMIZE TABLE AM_AlertHistoryLabelAnchor")
                    cursor.fetchall()  # Must consume OPTIMIZE result set
                    _history_tables.append('AM_AlertHistoryLabelAnchor')
                # Refresh information_schema stats so table sizes are accurate
                for _tbl in _history_tables:
                    cursor.execute(f"ANALYZE TABLE {_tbl}")
                    cursor.fetchall()
                db.commit()
                wl("Table optimization completed - disk space reclaimed!", "INFO")
                result_history["optimization_completed"] = True
            except Exception as e:
                wl(f"OPTIMIZE TABLE failed (non-fatal): {e}", "ERROR")
                result_history["optimization_completed"] = False
                result_history["optimization_error"] = str(e)

        context['ti'].xcom_push(key='alert_history_cleanup_result', value=result_history)
        return result_history

    except Exception as e:
        db.rollback()
        print(f"An error occurred: {str(e)}")
        wl(f"Error during AlertHistory cleanup: {str(e)}", "ERROR")
        raise
    finally:
        cursor.close()
        db.close()


@task
def clean_notification_audit_history(**context):
    """Retire terminal notification attempts in committed batches."""
    import time
    
    # Master kill switch check for database cleanup
    if not housekeeping_config.ENABLE_DATABASE_CLEANUP:
        logger.info("Notification audit cleanup skipped - master database flag disabled (ENABLE_DATABASE_CLEANUP=False)")
        return "Database cleanup disabled by master flag"
    
    # Granular task-specific check
    policy = policy_for(context, 'notification_audit')
    if not policy['enabled']:
        logger.info("Notification audit cleanup skipped - task-specific flag disabled (ENABLE_NOTIFICATION_AUDIT_CLEANUP=False)")
        return "Notification audit cleanup disabled by task-specific flag"

    connection_id = os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db')
    days_to_keep = policy['retention_days']
    batch_size = int(getattr(housekeeping_config, 'NOTIFICATION_AUDIT_BATCH_SIZE', 1000))
    max_iterations = int(getattr(housekeeping_config, 'NOTIFICATION_AUDIT_MAX_ITERATIONS', 100))
    table_swap_mode = getattr(housekeeping_config, 'NOTIFICATION_AUDIT_TABLE_SWAP_MODE', False)
    if table_swap_mode:
        raise RuntimeError('Notification audit table swap bypasses summary triggers')

    db = _connect_oscar_db(connection_id)
    cursor = db.cursor()

    # Setup worklog hook
    worklog_hook = None
    try:
        worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id') if context else None
        worklog_hook = WorkLogHook()
        if worklog_id:
            worklog_hook.set_worklog_id(worklog_id)
    except Exception:
        worklog_hook = None

    def wl(message: str, level: str = "INFO") -> None:
        wl_write(worklog_hook, message, level)

    try:
        dry_run = getattr(housekeeping_config, 'DRY_RUN_MODE', False)
        maintenance_optimization = getattr(housekeeping_config, 'NOTIFICATION_AUDIT_MAINTENANCE_OPTIMIZATION', False)
        cleanup_mode = "TABLE SWAP" if table_swap_mode else ("NORMAL DELETE + OPTIMIZE" if maintenance_optimization else "NORMAL DELETE")
        mode_str = f"DRY RUN ({cleanup_mode})" if dry_run else f"LIVE MODE ({cleanup_mode})"
        wl(f"Starting NotificationAudit cleanup [{mode_str}]: days_to_keep={days_to_keep}", "INFO")
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        start_time = time.time()

        # Get current table size
        cursor.execute("""
            SELECT ROUND(data_length / 1024 / 1024 / 1024, 2) as size_gb, table_rows
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = 'NTF_Notifications_Audit'
        """)
        row = cursor.fetchone()
        original_size_gb = row[0] if row else 0
        original_rows = row[1] if row else 0
        wl(f"Current NTF_Notifications_Audit: {original_size_gb} GB, ~{original_rows:,} rows", "INFO")

        rows_to_keep = None  # No retained-range scan needed for batch deletion.
        count_limit = batch_size * max_iterations + 1
        rows_to_delete = bounded_eligible_count(
            cursor, 'NTF_Notifications_Audit',
            "created_at < %s AND status IN ('sent','failed','suppressed','cancelled')",
            (cutoff_date_str,), count_limit)
        wl(f"Eligible notification audit rows: at least {rows_to_delete:,}", "INFO")

        if rows_to_delete <= 0:
            wl("No old records to delete", "INFO")
            return {"summary": "No old records to delete", "dry_run": dry_run,
                    "rows_deleted": 0, "eligible_backlog": 0}

        if dry_run:
            result = {
                "dry_run": True,
                "mode": cleanup_mode,
                "rows_to_delete": rows_to_delete,
                "backlog_count_capped": rows_to_delete == count_limit,
                "summary": f"[DRY RUN - {cleanup_mode}] Would delete {rows_to_delete:,} records"
            }
            wl(f"[DRY RUN] {result['summary']}", "INFO")
            context['ti'].xcom_push(key='notification_audit_cleanup_result', value=result)
            return result

        # ========================================
        # BRANCH: TABLE SWAP vs NORMAL DELETE
        # ========================================
        if table_swap_mode:
            # TABLE SWAP MODE
            wl("Using TABLE SWAP mode (disk space will be freed)", "INFO")
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            
            # Create new table
            cursor.execute("DROP TABLE IF EXISTS NTF_Notifications_Audit_new")
            cursor.execute("DROP TABLE IF EXISTS NTF_Notifications_Audit_old")
            db.commit()
            cursor.execute("CREATE TABLE NTF_Notifications_Audit_new LIKE NTF_Notifications_Audit")
            db.commit()
            
            # Copy recent data
            wl(f"Copying {rows_to_keep:,} recent records...", "INFO")
            copy_batch = 50000
            copied = 0
            last_id = None
            
            while True:
                if last_id is None:
                    cursor.execute("""
                        INSERT INTO NTF_Notifications_Audit_new
                        SELECT * FROM NTF_Notifications_Audit WHERE created_at >= %s ORDER BY id LIMIT %s
                    """, (cutoff_date_str, copy_batch))
                else:
                    cursor.execute("""
                        INSERT INTO NTF_Notifications_Audit_new
                        SELECT * FROM NTF_Notifications_Audit WHERE created_at >= %s AND id > %s ORDER BY id LIMIT %s
                    """, (cutoff_date_str, last_id, copy_batch))
                
                rows_copied = cursor.rowcount
                copied += rows_copied
                db.commit()
                if rows_copied == 0:
                    break
                cursor.execute("SELECT MAX(id) FROM NTF_Notifications_Audit_new")
                last_id = cursor.fetchone()[0]
                if copied % 100000 < copy_batch:
                    wl(f"Copied {copied:,} records...", "INFO")
            
            wl(f"Copied {copied:,} records", "INFO")
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            
            # Atomic swap
            wl("Performing atomic table swap...", "INFO")
            cursor.execute("""
                RENAME TABLE 
                    NTF_Notifications_Audit TO NTF_Notifications_Audit_old,
                    NTF_Notifications_Audit_new TO NTF_Notifications_Audit
            """)
            db.commit()
            
            # Drop old table
            cursor.execute("DROP TABLE NTF_Notifications_Audit_old")
            db.commit()
            wl("Old table dropped - disk space reclaimed!", "INFO")
            
            # Clean summary table
            cursor.execute("DELETE FROM NTF_Notifications_Audit_Summary WHERE summary_date < %s", (cutoff_date.date(),))
            summary_deleted = cursor.rowcount
            db.commit()
            
            # Get new size
            cursor.execute("""
                SELECT ROUND(data_length / 1024 / 1024 / 1024, 2)
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = 'NTF_Notifications_Audit'
            """)
            new_size_gb = cursor.fetchone()[0] or 0
            space_freed = original_size_gb - new_size_gb
            elapsed_time = int(time.time() - start_time)
            
            result = {
                "dry_run": False,
                "mode": "TABLE SWAP",
                "original_size_gb": original_size_gb,
                "new_size_gb": new_size_gb,
                "space_freed_gb": round(space_freed, 2),
                "rows_deleted": rows_to_delete,
                "summary_deleted": summary_deleted,
                "execution_time_seconds": elapsed_time,
                "summary": f"Freed {round(space_freed, 2)} GB in {elapsed_time}s via table swap"
            }
            wl(f"NotificationAudit cleanup completed: {result['summary']}", "INFO")
            wl(json.dumps({"cleanup_stats": result}), "INFO")
            context['ti'].xcom_push(key='notification_audit_cleanup_result', value=result)
            return result

        # ========================================
        # NORMAL DELETE MODE
        # ========================================
        wl("Using NORMAL DELETE mode (space reused, not freed to OS)", "INFO")
        
        task_timeout = getattr(housekeeping_config, 'NOTIFICATION_AUDIT_TIMEOUT_MINUTES', 60)
        check_timeout = create_timeout_checker("clean_notification_audit_history", task_timeout)
        iteration_count = 0
        total_audit_deleted = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                print(f"Reached maximum iterations ({max_iterations}). Breaking loop after deleting {total_audit_deleted} total NotificationAudit records.")
                wl(f"Stopped: max_iterations limit reached ({max_iterations})", "WARNING")
                break
            
            # Timeout check (safety layer)
            if check_timeout():
                print(f"Timeout reached. Stopping after {iteration_count} iterations and {total_audit_deleted} deletions.")
                wl(f"Stopped: timeout limit reached ({task_timeout} minutes)", "WARNING")
                break

            # Direct DELETE (OPTIMIZED: 1 query instead of 2)
            delete_audit_query = """
            DELETE FROM NTF_Notifications_Audit 
            WHERE created_at < %s 
              AND status IN ('sent','failed','suppressed','cancelled')
            ORDER BY created_at, id
            LIMIT %s
            """
            cursor.execute(delete_audit_query, (cutoff_date_str, batch_size))
            audit_deleted = cursor.rowcount

            total_audit_deleted += audit_deleted

            if audit_deleted == 0:
                print(f"No more NotificationAudit records found after {iteration_count} iterations. Total records deleted: {total_audit_deleted}")
                break

            if dry_run:
                db.rollback()  # 🔒 DRY RUN: Rollback instead of commit
                print(f"[DRY RUN] Would delete {audit_deleted} NotificationAudit records. Total so far: {total_audit_deleted}")
                wl(f"[DRY RUN] Iteration {iteration_count}/{max_iterations}: Would delete {audit_deleted} NotificationAudit records. Total: {total_audit_deleted}", "INFO")
            else:
                db.commit()  # ✅ LIVE MODE: Actually commit deletions
                print(f"Iteration {iteration_count}/{max_iterations}: Deleted {audit_deleted} NotificationAudit records. Total deleted so far: {total_audit_deleted}")
                wl(f"Iteration {iteration_count}/{max_iterations}: Deleted {audit_deleted} NotificationAudit records. Total so far: {total_audit_deleted}", "INFO")

        # ========================================
        # STEP 2: Clean NTF_Notifications_Audit_Summary
        # ========================================
        # The summary table aggregates daily stats from the main audit table
        # Once we've deleted old audit records, we should also delete old summary records
        # to prevent indefinite growth
        wl(f"Cleaning notification audit summary records older than {days_to_keep} days", "INFO")
        
        delete_summary_query = """
        DELETE FROM NTF_Notifications_Audit_Summary 
        WHERE summary_date < %s
          AND NOT EXISTS (
            SELECT 1 FROM NTF_Notifications_Audit a
            WHERE a.created_at >= summary_date
              AND a.created_at < summary_date + INTERVAL 1 DAY
          )
        """
        cursor.execute(delete_summary_query, (cutoff_date.date(),))
        summary_deleted = cursor.rowcount
        
        if dry_run:
            db.rollback()  # 🔒 DRY RUN: Rollback summary deletion too
            print(f"[DRY RUN] Would delete {summary_deleted} old summary records")
            wl(f"[DRY RUN] Would delete {summary_deleted} NotificationAuditSummary records", "INFO")
        else:
            db.commit()  # ✅ LIVE MODE: Commit summary deletion
            print(f"Deleted {summary_deleted} old summary records")
            wl(f"Deleted {summary_deleted} NotificationAuditSummary records", "INFO")

        # Finalize and push result to XCom
        result_audit = {
            "dry_run": dry_run,
            "total_notification_audit_deleted": total_audit_deleted,
            "rows_deleted": total_audit_deleted,
            "eligible_backlog": max(0, rows_to_delete - total_audit_deleted),
            "backlog_count_capped": rows_to_delete == count_limit,
            "execution_time_seconds": time.time() - start_time,
            "total_summary_deleted": summary_deleted,
            "days_to_keep": days_to_keep,
            "batch_size": batch_size,
            "max_iterations": max_iterations,
            "summary": f"{'[DRY RUN] Would delete' if dry_run else 'Deleted'} {total_audit_deleted} audit records and {summary_deleted} summary records",
        }
        wl(f"NotificationAudit cleanup completed: {result_audit['summary']}", "INFO")
        wl(json.dumps({"cleanup_stats": result_audit}), "INFO")

        # MAINTENANCE OPTIMIZATION: Run OPTIMIZE TABLE after normal delete to reclaim disk space
        if not dry_run and not table_swap_mode and maintenance_optimization and total_audit_deleted > 0:
            optimize_start = time.time()
            elapsed_so_far = int(optimize_start - start_time)
            wl(f"Running OPTIMIZE TABLE to reclaim disk space (delete took {elapsed_so_far}s)...", "INFO")
            wl(f"Task timeout: {task_timeout}min, elapsed: {elapsed_so_far}s — OPTIMIZE starting", "INFO")
            try:
                cursor.execute("OPTIMIZE TABLE NTF_Notifications_Audit")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE NTF_Notifications_Audit_Summary")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                # Refresh information_schema stats so table sizes are accurate
                cursor.execute("ANALYZE TABLE NTF_Notifications_Audit")
                cursor.fetchall()
                cursor.execute("ANALYZE TABLE NTF_Notifications_Audit_Summary")
                cursor.fetchall()
                db.commit()
                optimize_duration = int(time.time() - optimize_start)
                total_elapsed = int(time.time() - start_time)
                wl(f"OPTIMIZE completed in {optimize_duration}s (total task time: {total_elapsed}s) - disk space reclaimed!", "INFO")
                result_audit["optimization_completed"] = True
                result_audit["optimization_duration_seconds"] = optimize_duration
                result_audit["total_execution_seconds"] = total_elapsed
            except Exception as e:
                wl(f"OPTIMIZE TABLE failed (non-fatal) after {int(time.time() - optimize_start)}s: {e}", "ERROR")
                result_audit["optimization_completed"] = False
                result_audit["optimization_error"] = str(e)

        context['ti'].xcom_push(key='notification_audit_cleanup_result', value=result_audit)
        return result_audit

    except Exception as e:
        db.rollback()
        print(f"An error occurred: {str(e)}")
        wl(f"Error during NotificationAudit cleanup: {str(e)}", "ERROR")
        raise
    finally:
        cursor.close()
        db.close()

@task
def clean_ticketing_audit_history(**context):
    """Retire terminal ticketing attempts in committed batches."""
    import time
    
    # Master kill switch check for database cleanup
    if not housekeeping_config.ENABLE_DATABASE_CLEANUP:
        logger.info("Ticketing audit cleanup skipped - master database flag disabled (ENABLE_DATABASE_CLEANUP=False)")
        return "Database cleanup disabled by master flag"
    
    # Granular task-specific check
    policy = policy_for(context, 'ticketing_audit')
    if not policy['enabled']:
        logger.info("Ticketing audit cleanup skipped - task-specific flag disabled (ENABLE_TICKETING_AUDIT_CLEANUP=False)")
        return "Ticketing audit cleanup disabled by task-specific flag"

    connection_id = os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db')
    days_to_keep = policy['retention_days']
    batch_size = int(getattr(housekeeping_config, 'TICKETING_AUDIT_BATCH_SIZE', 1000))
    max_iterations = int(getattr(housekeeping_config, 'TICKETING_AUDIT_MAX_ITERATIONS', 100))
    table_swap_mode = getattr(housekeeping_config, 'TICKETING_AUDIT_TABLE_SWAP_MODE', False)
    if table_swap_mode:
        raise RuntimeError('Ticketing audit table swap bypasses summary triggers')

    db = _connect_oscar_db(connection_id)
    cursor = db.cursor()

    # Setup worklog hook
    worklog_hook = None
    try:
        worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id') if context else None
        worklog_hook = WorkLogHook()
        if worklog_id:
            worklog_hook.set_worklog_id(worklog_id)
    except Exception:
        worklog_hook = None

    def wl(message: str, level: str = "INFO") -> None:
        wl_write(worklog_hook, message, level)

    try:
        dry_run = getattr(housekeeping_config, 'DRY_RUN_MODE', False)
        maintenance_optimization = getattr(housekeeping_config, 'TICKETING_AUDIT_MAINTENANCE_OPTIMIZATION', False)
        cleanup_mode = "TABLE SWAP" if table_swap_mode else ("NORMAL DELETE + OPTIMIZE" if maintenance_optimization else "NORMAL DELETE")
        mode_str = f"DRY RUN ({cleanup_mode})" if dry_run else f"LIVE MODE ({cleanup_mode})"
        wl(f"Starting TicketingAudit cleanup [{mode_str}]: days_to_keep={days_to_keep}", "INFO")
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        start_time = time.time()

        # Get current table size
        cursor.execute("""
            SELECT ROUND(data_length / 1024 / 1024 / 1024, 2) as size_gb, table_rows
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = 'TKT_Ticketing_Audit'
        """)
        row = cursor.fetchone()
        original_size_gb = row[0] if row else 0
        original_rows = row[1] if row else 0
        wl(f"Current TKT_Ticketing_Audit: {original_size_gb} GB, ~{original_rows:,} rows", "INFO")

        # Count rows to keep/delete (use actual COUNT, not stale information_schema estimate)
        rows_to_keep = None  # No retained-range scan needed for batch deletion.
        count_limit = batch_size * max_iterations + 1
        rows_to_delete = bounded_eligible_count(
            cursor, 'TKT_Ticketing_Audit',
            "created_at < %s AND status IN ('success','failure','timeout','invalid_response','suppressed')",
            (cutoff_date_str,), count_limit)
        wl(f"Eligible ticketing audit rows: at least {rows_to_delete:,}", "INFO")

        if rows_to_delete <= 0:
            wl("No old records to delete", "INFO")
            return {"summary": "No old records to delete", "dry_run": dry_run,
                    "rows_deleted": 0, "eligible_backlog": 0}

        if dry_run:
            result = {
                "dry_run": True,
                "mode": cleanup_mode,
                "rows_to_delete": rows_to_delete,
                "backlog_count_capped": rows_to_delete == count_limit,
                "summary": f"[DRY RUN - {cleanup_mode}] Would delete {rows_to_delete:,} records"
            }
            wl(f"[DRY RUN] {result['summary']}", "INFO")
            context['ti'].xcom_push(key='ticketing_audit_cleanup_result', value=result)
            return result

        # ========================================
        # BRANCH: TABLE SWAP vs NORMAL DELETE
        # ========================================
        if table_swap_mode:
            # TABLE SWAP MODE
            wl("Using TABLE SWAP mode (disk space will be freed)", "INFO")
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            
            # Create new table
            cursor.execute("DROP TABLE IF EXISTS TKT_Ticketing_Audit_new")
            cursor.execute("DROP TABLE IF EXISTS TKT_Ticketing_Audit_old")
            db.commit()
            cursor.execute("CREATE TABLE TKT_Ticketing_Audit_new LIKE TKT_Ticketing_Audit")
            db.commit()
            
            # Copy recent data
            wl(f"Copying {rows_to_keep:,} recent records...", "INFO")
            copy_batch = 50000
            copied = 0
            last_id = None
            
            while True:
                if last_id is None:
                    cursor.execute("""
                        INSERT INTO TKT_Ticketing_Audit_new
                        SELECT * FROM TKT_Ticketing_Audit WHERE created_at >= %s ORDER BY id LIMIT %s
                    """, (cutoff_date_str, copy_batch))
                else:
                    cursor.execute("""
                        INSERT INTO TKT_Ticketing_Audit_new
                        SELECT * FROM TKT_Ticketing_Audit WHERE created_at >= %s AND id > %s ORDER BY id LIMIT %s
                    """, (cutoff_date_str, last_id, copy_batch))
                
                rows_copied = cursor.rowcount
                copied += rows_copied
                db.commit()
                if rows_copied == 0:
                    break
                cursor.execute("SELECT MAX(id) FROM TKT_Ticketing_Audit_new")
                last_id = cursor.fetchone()[0]
                if copied % 100000 < copy_batch:
                    wl(f"Copied {copied:,} records...", "INFO")
            
            wl(f"Copied {copied:,} records", "INFO")
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            
            # Atomic swap
            wl("Performing atomic table swap...", "INFO")
            cursor.execute("""
                RENAME TABLE 
                    TKT_Ticketing_Audit TO TKT_Ticketing_Audit_old,
                    TKT_Ticketing_Audit_new TO TKT_Ticketing_Audit
            """)
            db.commit()
            
            # Drop old table
            cursor.execute("DROP TABLE TKT_Ticketing_Audit_old")
            db.commit()
            wl("Old table dropped - disk space reclaimed!", "INFO")
            
            # Clean summary table
            cursor.execute("DELETE FROM TKT_Ticketing_Audit_Summary WHERE summary_date < %s", (cutoff_date.date(),))
            summary_deleted = cursor.rowcount
            db.commit()
            
            # Get new size
            cursor.execute("""
                SELECT ROUND(data_length / 1024 / 1024 / 1024, 2)
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = 'TKT_Ticketing_Audit'
            """)
            new_size_gb = cursor.fetchone()[0] or 0
            space_freed = original_size_gb - new_size_gb
            elapsed_time = int(time.time() - start_time)
            
            result = {
                "dry_run": False,
                "mode": "TABLE SWAP",
                "original_size_gb": original_size_gb,
                "new_size_gb": new_size_gb,
                "space_freed_gb": round(space_freed, 2),
                "rows_deleted": rows_to_delete,
                "summary_deleted": summary_deleted,
                "execution_time_seconds": elapsed_time,
                "summary": f"Freed {round(space_freed, 2)} GB in {elapsed_time}s via table swap"
            }
            wl(f"TicketingAudit cleanup completed: {result['summary']}", "INFO")
            wl(json.dumps({"cleanup_stats": result}), "INFO")
            context['ti'].xcom_push(key='ticketing_audit_cleanup_result', value=result)
            return result

        # ========================================
        # NORMAL DELETE MODE
        # ========================================
        wl("Using NORMAL DELETE mode (space reused, not freed to OS)", "INFO")
        
        task_timeout = getattr(housekeeping_config, 'TICKETING_AUDIT_TIMEOUT_MINUTES', 60)
        check_timeout = create_timeout_checker("clean_ticketing_audit_history", task_timeout)
        iteration_count = 0
        total_audit_deleted = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                print(f"Reached maximum iterations ({max_iterations}). Breaking loop after deleting {total_audit_deleted} total TicketingAudit records.")
                wl(f"Stopped: max_iterations limit reached ({max_iterations})", "WARNING")
                break
            
            # Timeout check (safety layer)
            if check_timeout():
                print(f"Timeout reached. Stopping after {iteration_count} iterations and {total_audit_deleted} deletions.")
                wl(f"Stopped: timeout limit reached ({task_timeout} minutes)", "WARNING")
                break

            # Direct DELETE (OPTIMIZED: 1 query per batch)
            # Uses idx_created_at index for efficient range scan
            delete_audit_query = """
            DELETE FROM TKT_Ticketing_Audit 
            WHERE created_at < %s 
              AND status IN ('success','failure','timeout','invalid_response','suppressed')
            ORDER BY created_at, id
            LIMIT %s
            """
            cursor.execute(delete_audit_query, (cutoff_date_str, batch_size))
            audit_deleted = cursor.rowcount

            total_audit_deleted += audit_deleted

            if audit_deleted == 0:
                print(f"No more TicketingAudit records found after {iteration_count} iterations. Total records deleted: {total_audit_deleted}")
                break

            if dry_run:
                db.rollback()  # 🔒 DRY RUN: Rollback instead of commit
                print(f"[DRY RUN] Would delete {audit_deleted} TicketingAudit records. Total so far: {total_audit_deleted}")
                wl(f"[DRY RUN] Iteration {iteration_count}/{max_iterations}: Would delete {audit_deleted} TicketingAudit records. Total: {total_audit_deleted}", "INFO")
            else:
                db.commit()  # ✅ LIVE MODE: Actually commit deletions
                print(f"Iteration {iteration_count}/{max_iterations}: Deleted {audit_deleted} TicketingAudit records. Total deleted so far: {total_audit_deleted}")
                wl(f"Iteration {iteration_count}/{max_iterations}: Deleted {audit_deleted} TicketingAudit records. Total so far: {total_audit_deleted}", "INFO")

        # ========================================
        # STEP 2: Clean TKT_Ticketing_Audit_Summary
        # ========================================
        # The summary table aggregates daily stats from the main audit table
        # Once we've deleted old audit records, we should also delete old summary records
        # to prevent indefinite growth
        wl(f"Cleaning ticketing audit summary records older than {days_to_keep} days", "INFO")
        
        delete_summary_query = """
        DELETE FROM TKT_Ticketing_Audit_Summary 
        WHERE summary_date < %s
          AND NOT EXISTS (
            SELECT 1 FROM TKT_Ticketing_Audit a
            WHERE a.created_at >= summary_date
              AND a.created_at < summary_date + INTERVAL 1 DAY
          )
        """
        cursor.execute(delete_summary_query, (cutoff_date.date(),))
        summary_deleted = cursor.rowcount
        
        if dry_run:
            db.rollback()  # 🔒 DRY RUN: Rollback summary deletion too
            print(f"[DRY RUN] Would delete {summary_deleted} old summary records")
            wl(f"[DRY RUN] Would delete {summary_deleted} TicketingAuditSummary records", "INFO")
        else:
            db.commit()  # ✅ LIVE MODE: Commit summary deletion
            print(f"Deleted {summary_deleted} old summary records")
            wl(f"Deleted {summary_deleted} TicketingAuditSummary records", "INFO")

        # Finalize and push result to XCom
        result_audit = {
            "dry_run": dry_run,
            "total_ticketing_audit_deleted": total_audit_deleted,
            "rows_deleted": total_audit_deleted,
            "eligible_backlog": max(0, rows_to_delete - total_audit_deleted),
            "backlog_count_capped": rows_to_delete == count_limit,
            "execution_time_seconds": time.time() - start_time,
            "total_summary_deleted": summary_deleted,
            "days_to_keep": days_to_keep,
            "batch_size": batch_size,
            "max_iterations": max_iterations,
            "summary": f"{'[DRY RUN] Would delete' if dry_run else 'Deleted'} {total_audit_deleted} audit records and {summary_deleted} summary records",
        }
        wl(f"TicketingAudit cleanup completed: {result_audit['summary']}", "INFO")
        wl(json.dumps({"cleanup_stats": result_audit}), "INFO")

        # MAINTENANCE OPTIMIZATION: Run OPTIMIZE TABLE after normal delete to reclaim disk space
        if not dry_run and not table_swap_mode and maintenance_optimization and total_audit_deleted > 0:
            optimize_start = time.time()
            elapsed_so_far = int(optimize_start - start_time)
            wl(f"Running OPTIMIZE TABLE to reclaim disk space (delete took {elapsed_so_far}s)...", "INFO")
            wl(f"Task timeout: {task_timeout}min, elapsed: {elapsed_so_far}s — OPTIMIZE starting", "INFO")
            try:
                cursor.execute("OPTIMIZE TABLE TKT_Ticketing_Audit")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                cursor.execute("OPTIMIZE TABLE TKT_Ticketing_Audit_Summary")
                cursor.fetchall()  # Must consume OPTIMIZE result set
                # Refresh information_schema stats so table sizes are accurate
                cursor.execute("ANALYZE TABLE TKT_Ticketing_Audit")
                cursor.fetchall()
                cursor.execute("ANALYZE TABLE TKT_Ticketing_Audit_Summary")
                cursor.fetchall()
                db.commit()
                optimize_duration = int(time.time() - optimize_start)
                total_elapsed = int(time.time() - start_time)
                wl(f"OPTIMIZE completed in {optimize_duration}s (total task time: {total_elapsed}s) - disk space reclaimed!", "INFO")
                result_audit["optimization_completed"] = True
                result_audit["optimization_duration_seconds"] = optimize_duration
                result_audit["total_execution_seconds"] = total_elapsed
            except Exception as e:
                wl(f"OPTIMIZE TABLE failed (non-fatal) after {int(time.time() - optimize_start)}s: {e}", "ERROR")
                result_audit["optimization_completed"] = False
                result_audit["optimization_error"] = str(e)

        context['ti'].xcom_push(key='ticketing_audit_cleanup_result', value=result_audit)
        return result_audit

    except Exception as e:
        db.rollback()
        print(f"An error occurred: {str(e)}")
        wl(f"Error during TicketingAudit cleanup: {str(e)}", "ERROR")
        raise
    finally:
        cursor.close()
        db.close()


@task
def clean_user_audit_history(**context):
    """Retire user audit events in committed batches."""
    import time
    
    # Master kill switch check for database cleanup
    if not housekeeping_config.ENABLE_DATABASE_CLEANUP:
        logger.info("User audit cleanup skipped - master database flag disabled (ENABLE_DATABASE_CLEANUP=False)")
        return "Database cleanup disabled by master flag"
    
    # Granular task-specific check
    policy = policy_for(context, 'user_audit')
    if not policy['enabled']:
        logger.info("User audit cleanup skipped - task-specific flag disabled (ENABLE_USER_AUDIT_CLEANUP=False)")
        return "User audit cleanup disabled by task-specific flag"

    connection_id = os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db')
    days_to_keep = policy['retention_days']
    table_swap_mode = getattr(housekeeping_config, 'USER_AUDIT_TABLE_SWAP_MODE', False)
    if table_swap_mode:
        raise RuntimeError('User audit table swap bypasses summary triggers')
    batch_size = int(getattr(housekeeping_config, 'USER_AUDIT_BATCH_SIZE', 5000))
    max_iterations = int(getattr(housekeeping_config, 'USER_AUDIT_MAX_ITERATIONS', 500))

    db = _connect_oscar_db(connection_id)
    cursor = db.cursor()

    # Setup worklog hook
    worklog_hook = None
    try:
        worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id') if context else None
        worklog_hook = WorkLogHook()
        if worklog_id:
            worklog_hook.set_worklog_id(worklog_id)
    except Exception:
        worklog_hook = None

    def wl(message: str, level: str = "INFO") -> None:
        wl_write(worklog_hook, message, level)

    try:
        dry_run = getattr(housekeeping_config, 'DRY_RUN_MODE', False)
        maintenance_optimization = getattr(housekeeping_config, 'USER_AUDIT_MAINTENANCE_OPTIMIZATION', False)
        cleanup_mode = "TABLE SWAP" if table_swap_mode else ("NORMAL DELETE + OPTIMIZE" if maintenance_optimization else "NORMAL DELETE")
        mode_str = f"DRY RUN ({cleanup_mode})" if dry_run else f"LIVE MODE ({cleanup_mode})"
        wl(f"Starting UserAudit cleanup [{mode_str}]: days_to_keep={days_to_keep}", "INFO")
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        
        start_time = time.time()

        # ========================================
        # STEP 0: Get current table size and row count
        # ========================================
        cursor.execute("""
            SELECT 
                ROUND(data_length / 1024 / 1024 / 1024, 2) as size_gb,
                table_rows
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = 'UA_User_Audit'
        """)
        row = cursor.fetchone()
        original_size_gb = row[0] if row else 0
        original_rows = row[1] if row else 0
        wl(f"Current UA_User_Audit: {original_size_gb} GB, ~{original_rows:,} rows", "INFO")
        
        rows_to_keep = None  # The retained range need not be scanned.
        count_limit = batch_size * max_iterations + 1
        rows_to_delete = bounded_eligible_count(
            cursor, 'UA_User_Audit', 'created_at < %s', (cutoff_date_str,), count_limit)
        wl(f"Eligible user audit rows: at least {rows_to_delete:,}", "INFO")
        
        if rows_to_delete <= 0:
            wl("No old records to delete", "INFO")
            result = {"summary": "No old records to delete", "dry_run": dry_run,
                      "rows_deleted": 0, "eligible_backlog": 0}
            context['ti'].xcom_push(key='user_audit_cleanup_result', value=result)
            return result

        if dry_run:
            # DRY RUN - just report what would happen
            result = {
                "dry_run": True,
                "mode": cleanup_mode,
                "original_size_gb": original_size_gb,
                "estimated_new_size_gb": original_size_gb,
                "estimated_space_freed_gb": 0,
                "rows_to_keep": rows_to_keep,
                "rows_to_delete": rows_to_delete,
                "backlog_count_capped": rows_to_delete == count_limit,
                "summary": f"[DRY RUN - {cleanup_mode}] Would delete at least {rows_to_delete:,} old records (space reused, not freed)",
            }
            wl(f"[DRY RUN] {result['summary']}", "INFO")
            context['ti'].xcom_push(key='user_audit_cleanup_result', value=result)
            return result

        # ========================================
        # BRANCH: TABLE SWAP vs NORMAL DELETE
        # ========================================
        if not table_swap_mode:
            # ========================================
            # NORMAL DELETE MODE - Delete by day ranges
            # ========================================
            wl("Using NORMAL DELETE mode (space reused, not freed to OS)", "INFO")
            
            task_timeout = getattr(housekeeping_config, 'USER_AUDIT_TIMEOUT_MINUTES', 60)
            check_timeout = create_timeout_checker("clean_user_audit_history", task_timeout)
            total_deleted = 0
            
            # Find the oldest record date
            cursor.execute("SELECT DATE(MIN(created_at)) FROM UA_User_Audit")
            min_date_result = cursor.fetchone()[0]
            
            if min_date_result is None:
                wl("No records found to delete", "INFO")
            else:
                # Delete day by day, starting from oldest
                current_date = min_date_result
                cutoff_date_only = cutoff_date.date()
                
                while current_date < cutoff_date_only:
                    if check_timeout():
                        wl(f"Stopped: timeout limit reached", "WARNING")
                        break
                    
                    next_date = current_date + timedelta(days=1)
                    
                    # Delete all records for this specific day in batches
                    day_deleted = 0
                    iteration = 0
                    while iteration < max_iterations:
                        iteration += 1
                        delete_query = """
                            DELETE FROM UA_User_Audit 
                            WHERE created_at >= %s AND created_at < %s 
                            LIMIT %s
                        """
                        cursor.execute(delete_query, (current_date.strftime('%Y-%m-%d'), next_date.strftime('%Y-%m-%d'), batch_size))
                        deleted = cursor.rowcount
                        day_deleted += deleted
                        db.commit()
                        
                        if deleted < batch_size:
                            break
                    
                    if day_deleted > 0:
                        total_deleted += day_deleted
                        wl(f"Deleted {day_deleted:,} records for {current_date} (total: {total_deleted:,})", "INFO")
                    
                    current_date = next_date
            
            # Clean summary table
            cursor.execute("""DELETE FROM UA_User_Audit_Summary WHERE summary_date < %s
                AND NOT EXISTS (SELECT 1 FROM UA_User_Audit a
                    WHERE a.created_at >= summary_date
                      AND a.created_at < summary_date + INTERVAL 1 DAY)""", (cutoff_date.date(),))
            summary_deleted = cursor.rowcount
            db.commit()
            
            remaining = bounded_eligible_count(
                cursor, 'UA_User_Audit', 'created_at < %s', (cutoff_date_str,), 1001)
            elapsed_time = int(time.time() - start_time)
            result = {
                "dry_run": False,
                "mode": "NORMAL DELETE",
                "rows_deleted": total_deleted,
                "eligible_backlog": remaining,
                "backlog_count_capped": remaining == 1001,
                "summary_deleted": summary_deleted,
                "execution_time_seconds": elapsed_time,
                "summary": f"Deleted {total_deleted:,} records in {elapsed_time}s (space reused, not freed)"
            }
            wl(f"UserAudit cleanup completed: {result['summary']}", "INFO")
            wl(json.dumps({"cleanup_stats": result}), "INFO")
            
            # ========================================
            # MAINTENANCE OPTIMIZATION: Run OPTIMIZE TABLE after normal delete to reclaim disk space
            # ========================================
            if maintenance_optimization and total_deleted > 0:
                optimize_start = time.time()
                elapsed_so_far = int(optimize_start - start_time)
                wl(f"Running OPTIMIZE TABLE to reclaim disk space (delete took {elapsed_so_far}s)...", "INFO")
                wl(f"Task timeout: {task_timeout}min, elapsed: {elapsed_so_far}s — OPTIMIZE starting", "INFO")
                try:
                    cursor.execute("OPTIMIZE TABLE UA_User_Audit")
                    cursor.fetchall()  # Must consume OPTIMIZE result set
                    cursor.execute("OPTIMIZE TABLE UA_User_Audit_Summary")
                    cursor.fetchall()  # Must consume OPTIMIZE result set
                    # Refresh information_schema stats so table sizes are accurate
                    cursor.execute("ANALYZE TABLE UA_User_Audit")
                    cursor.fetchall()
                    cursor.execute("ANALYZE TABLE UA_User_Audit_Summary")
                    cursor.fetchall()
                    db.commit()
                    optimize_duration = int(time.time() - optimize_start)
                    total_elapsed = int(time.time() - start_time)
                    wl(f"OPTIMIZE completed in {optimize_duration}s (total task time: {total_elapsed}s) - disk space reclaimed!", "INFO")
                    
                    result["optimization_completed"] = True
                    result["optimization_duration_seconds"] = optimize_duration
                    result["total_execution_seconds"] = total_elapsed
                    result["summary"] = f"Deleted {total_deleted:,} records in {elapsed_time}s + OPTIMIZE in {optimize_duration}s (space freed)"
                except Exception as e:
                    wl(f"OPTIMIZE TABLE failed (non-fatal) after {int(time.time() - optimize_start)}s: {e}", "ERROR")
                    result["optimization_completed"] = False
                    result["optimization_error"] = str(e)
            
            context['ti'].xcom_push(key='user_audit_cleanup_result', value=result)
            return result

        # ========================================
        # TABLE SWAP MODE - Actually reclaim disk space
        # ========================================
        wl("Using TABLE SWAP mode (disk space will be freed)", "INFO")

        # ========================================
        # STEP 1: Create new table with same structure
        # ========================================
        wl("Step 1: Creating new table UA_User_Audit_new...", "INFO")
        
        # Drop if exists from previous failed run
        cursor.execute("DROP TABLE IF EXISTS UA_User_Audit_new")
        cursor.execute("DROP TABLE IF EXISTS UA_User_Audit_old")
        db.commit()
        
        # Create new table with same structure
        cursor.execute("CREATE TABLE UA_User_Audit_new LIKE UA_User_Audit")
        db.commit()
        wl("Created UA_User_Audit_new", "INFO")

        # ========================================
        # STEP 2: Copy recent data to new table (in batches)
        # ========================================
        wl(f"Step 2: Copying {rows_to_keep:,} recent records to new table...", "INFO")
        
        # Use READ UNCOMMITTED to avoid locking the source table during copy
        # This is safe because we're only reading data that won't be modified
        # (old audit records are never updated, only new ones are inserted)
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        
        batch_size = 50000  # Copy in batches to avoid long transactions
        copied = 0
        last_id = None
        
        while True:
            if last_id is None:
                # First batch - get oldest record to keep
                copy_query = """
                    INSERT INTO UA_User_Audit_new
                    SELECT * FROM UA_User_Audit 
                    WHERE created_at >= %s
                    ORDER BY id
                    LIMIT %s
                """
                cursor.execute(copy_query, (cutoff_date_str, batch_size))
            else:
                # Subsequent batches - continue from last_id
                copy_query = """
                    INSERT INTO UA_User_Audit_new
                    SELECT * FROM UA_User_Audit 
                    WHERE created_at >= %s AND id > %s
                    ORDER BY id
                    LIMIT %s
                """
                cursor.execute(copy_query, (cutoff_date_str, last_id, batch_size))
            
            rows_copied = cursor.rowcount
            copied += rows_copied
            db.commit()
            
            if rows_copied == 0:
                break
            
            # Get last ID for next batch
            cursor.execute("SELECT MAX(id) FROM UA_User_Audit_new")
            last_id = cursor.fetchone()[0]
            
            # Log progress every 100k rows
            if copied % 100000 < batch_size:
                wl(f"Copied {copied:,} / {rows_to_keep:,} records...", "INFO")
        
        wl(f"Copied {copied:,} records to new table", "INFO")
        
        # Reset isolation level back to default
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")

        # ========================================
        # STEP 3: Atomic table swap (RENAME)
        # ========================================
        wl("Step 3: Performing atomic table swap (RENAME)...", "INFO")
        wl("WARNING: Writes may fail during this millisecond operation", "WARNING")
        
        # Atomic rename - this is the critical moment (milliseconds)
        cursor.execute("""
            RENAME TABLE 
                UA_User_Audit TO UA_User_Audit_old,
                UA_User_Audit_new TO UA_User_Audit
        """)
        db.commit()
        wl("Table swap completed successfully", "INFO")

        # ========================================
        # STEP 4: Drop old table (instant space reclaim)
        # ========================================
        wl("Step 4: Dropping old table to reclaim disk space...", "INFO")
        cursor.execute("DROP TABLE UA_User_Audit_old")
        db.commit()
        wl("Old table dropped - disk space reclaimed!", "INFO")

        # ========================================
        # STEP 5: Clean UA_User_Audit_Summary
        # ========================================
        wl(f"Step 5: Cleaning summary records older than {days_to_keep} days...", "INFO")
        cursor.execute("DELETE FROM UA_User_Audit_Summary WHERE summary_date < %s", (cutoff_date.date(),))
        summary_deleted = cursor.rowcount
        db.commit()
        wl(f"Deleted {summary_deleted} old summary records", "INFO")

        # ========================================
        # STEP 6: Get new table size
        # ========================================
        cursor.execute("""
            SELECT ROUND(data_length / 1024 / 1024 / 1024, 2) as size_gb
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() AND table_name = 'UA_User_Audit'
        """)
        new_size_gb = cursor.fetchone()[0] or 0
        space_freed = original_size_gb - new_size_gb
        
        elapsed_time = int(time.time() - start_time)
        
        result = {
            "dry_run": False,
            "original_size_gb": original_size_gb,
            "new_size_gb": new_size_gb,
            "space_freed_gb": round(space_freed, 2),
            "rows_deleted": rows_to_delete,
            "rows_kept": copied,
            "summary_deleted": summary_deleted,
            "execution_time_seconds": elapsed_time,
            "summary": f"Freed {round(space_freed, 2)} GB ({original_size_gb} GB → {new_size_gb} GB) in {elapsed_time}s"
        }
        
        wl(f"UserAudit cleanup completed: {result['summary']}", "INFO")
        wl(json.dumps({"cleanup_stats": result}), "INFO")
        context['ti'].xcom_push(key='user_audit_cleanup_result', value=result)
        
        return result

    except Exception as e:
        db.rollback()
        # Cleanup on failure
        try:
            cursor.execute("DROP TABLE IF EXISTS UA_User_Audit_new")
            db.commit()
        except:
            pass
        print(f"An error occurred: {str(e)}")
        wl(f"Error during UserAudit cleanup: {str(e)}", "ERROR")
        raise
    finally:
        cursor.close()
        db.close()


@task
def clean_alert_audit(**context):
    """Retire recorded alert activity after its independent evidence window."""
    if not housekeeping_config.ENABLE_DATABASE_CLEANUP:
        return 'Database cleanup disabled by master flag'
    policy = policy_for(context, 'alert_audit')
    if not policy['enabled']:
        return 'Alert audit cleanup disabled'
    db = _connect_oscar_db(os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db'))
    cursor = db.cursor()
    started = datetime.now(timezone.utc)
    cutoff = started - timedelta(days=policy['retention_days'])
    batch_size = int(housekeeping_config.ALERT_AUDIT_BATCH_SIZE)
    max_iterations = int(housekeeping_config.ALERT_AUDIT_MAX_ITERATIONS)
    try:
        count_limit = batch_size * max_iterations + 1
        eligible = bounded_eligible_count(
            cursor, 'AM_AlertAudit', 'recorded_at < %s',
            (cutoff.replace(tzinfo=None),), count_limit)
        if housekeeping_config.DRY_RUN_MODE:
            result = {'dry_run': True, 'rows_to_delete': eligible, 'rows_deleted': 0,
                      'eligible_backlog': eligible, 'backlog_count_capped': eligible == count_limit,
                      'days_to_keep': policy['retention_days']}
        else:
            deleted = 0
            for _ in range(max_iterations):
                cursor.execute('DELETE FROM AM_AlertAudit WHERE recorded_at < %s LIMIT %s',
                               (cutoff.replace(tzinfo=None), batch_size))
                batch = cursor.rowcount
                db.commit()
                deleted += batch
                if batch < batch_size:
                    break
            result = {'dry_run': False, 'rows_deleted': deleted,
                      'eligible_backlog': max(0, eligible - deleted),
                      'backlog_count_capped': eligible == count_limit,
                      'days_to_keep': policy['retention_days']}
        result['execution_time_seconds'] = (datetime.now(timezone.utc) - started).total_seconds()
        logger.info('Alert audit cleanup: %s', result)
        context['ti'].xcom_push(key='alert_audit_cleanup_result', value=result)
        return result
    except Exception:
        db.rollback()
        raise
    finally:
        cursor.close()
        db.close()


@task
def close_worklog(**context):
    """Close the worklog created earlier, persist cleanup reports as metadata, and add a final entry."""
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    # ========================================
    # Collect cleanup reports from all tasks and persist as worklog metadata
    # ========================================
    ti = context['ti']
    metadata_items = []
    total_rows_deleted = 0
    total_files_deleted = 0
    total_dirs_deleted = 0
    total_space_freed_bytes = 0
    total_space_freed_gb = 0.0
    tasks_completed = 0
    tasks_skipped = 0
    tasks_failed = 0

    for xcom_key, airflow_task_id, task_name, cleanup_type in TASK_REPORT_REGISTRY:
        try:
            instance = context['dag_run'].get_task_instance(airflow_task_id)
            task_state = getattr(instance, 'state', None)
            result = ti.xcom_pull(task_ids=airflow_task_id, key=xcom_key)
            if task_state in ('failed', 'upstream_failed'):
                report = {'task': task_name, 'type': cleanup_type, 'status': 'error',
                          'reason': f'Airflow task state: {task_state}'}
            else:
                report = build_task_report(task_name, cleanup_type, result)
        except Exception as e:
            logger.warning(f"Could not pull XCom for {airflow_task_id}/{xcom_key}: {e}")
            report = {
                "task": task_name,
                "type": cleanup_type,
                "status": "error",
                "reason": f"Failed to retrieve result: {e}",
            }

        # Aggregate totals
        status = report.get("status", "unknown")
        if status in ("completed", "dry_run"):
            tasks_completed += 1
            if cleanup_type == "file":
                total_files_deleted += report.get("files_deleted", 0)
                total_dirs_deleted += report.get("dirs_deleted", 0)
                total_space_freed_bytes += report.get("space_freed_bytes", 0)
            elif cleanup_type == "db":
                total_rows_deleted += report.get("rows_deleted", 0)
                total_space_freed_gb += report.get("space_freed_gb", 0) or 0
        elif status == "skipped":
            tasks_skipped += 1
        else:
            tasks_failed += 1

        # Store per-task report as metadata (key prefix: "report__" for easy identification)
        metadata_items.append({
            "key": f"report__{airflow_task_id}",
            "value": json.dumps(report),
        })

    # Build overall summary
    overall_summary = {
        "tasks_completed": tasks_completed,
        "tasks_skipped": tasks_skipped,
        "tasks_failed": tasks_failed,
        "total_rows_deleted": total_rows_deleted,
        "total_files_deleted": total_files_deleted,
        "total_dirs_deleted": total_dirs_deleted,
        "total_space_freed_files": format_bytes(total_space_freed_bytes),
        "total_space_freed_db_gb": round(total_space_freed_gb, 2),
    }
    metadata_items.append({
        "key": "report__overall_summary",
        "value": json.dumps(overall_summary),
    })

    # The summary always reaches the task log, with or without a worklog.
    logger.info(f"Housekeeping summary: {json.dumps(overall_summary)}")
    if not worklog_id:
        logger.error("No worklog for this run (creation failed); summary is in this task log only")
        return None

    # Persist all reports as worklog metadata
    try:
        hook.add_metadata(metadata_items)
        hook.info(f"Cleanup reports persisted to worklog metadata: {tasks_completed} completed, {tasks_skipped} skipped, {tasks_failed} failed")
        logger.info(f"Persisted {len(metadata_items)} cleanup report metadata entries to worklog {worklog_id}")
    except Exception as e:
        logger.error(f"Failed to persist cleanup reports as metadata: {e}")

    # A worklog API failure must not mark a completed cleanup as failed.
    try:
        hook.info(f"Workflow completed, closing worklog for worklog id: {worklog_id}")
        closed_worklog = hook.close_worklog()
    except Exception as e:
        logger.error(f"Could not close worklog {worklog_id}; cleanup itself is complete: {e}")
        return worklog_id
    logger.info(f"Closed worklog with ID: {closed_worklog['id']}")

    return closed_worklog['id']


@task(trigger_rule=TriggerRule.ALL_DONE)
def enforce_housekeeping_result(**context):
    """The report can complete while a cleanup failed; keep the DAG red."""
    checked = [task_id for _, task_id, _, _ in TASK_REPORT_REGISTRY]
    checked.extend(('load_housekeeping_policy', 'close_worklog'))
    failed = [task_id for task_id in checked
              if getattr(context['dag_run'].get_task_instance(task_id), 'state', None)
              in ('failed', 'upstream_failed')]
    if failed:
        raise RuntimeError('Housekeeping failed: ' + ', '.join(failed))


HOUSEKEEPING_GUIDE_MD = """
## OSCAR Housekeeping — user guide

Runs **daily at 11:00 UTC** to clean database history. Airflow log deletion is disabled in this DAG:
the independent `airflow-log-groomer` service deletes `.log` files unmodified for more than **3 days**
and removes empty directories, checking every **15 minutes**. Override its retention with
`AIRFLOW_LOG_RETENTION_DAYS` in `conf/env/workflow.env`; this does not change database retention.
Set per-family retention and enabled state in **Settings → Application → Housekeeping**.
Database overrides are read once at the start of each run. Missing overrides use
`plugins/helpers/oscar_housekeeping_config.py` (on the box: `/opt/oscar/app/oscar-workflow/airflow/`).
The file is read fresh on every run. Trigger the DAG from this page or the workflow page.

| Mode | Settings | What a run does | Gives disk back? | Use when |
|---|---|---|---|---|
| Dry run | `DRY_RUN_MODE = True` | counts what it would delete, changes nothing | no | previewing |
| **Daily delete** (default) | `DRY_RUN_MODE = False`, `X_TABLE_SWAP_MODE = False`, `X_MAINTENANCE_OPTIMIZATION = False` | deletes rows older than retention; MySQL reuses the room | no, stops growth | every day |
| Delete + optimize | `X_MAINTENANCE_OPTIMIZATION = True` | deletes, then rebuilds the table | yes | **avoid on big tables**: needs free disk equal to the table size |

Retention defaults are task history 7 days, alert history 30 days, notification audit
30 days, ticketing audit 40 days, user audit 7 days, and alert audit 90 days.
Table swap modes are blocked because they can lose concurrent writes and bypass triggers.
**If a run fails:** open the failed task's log and the worklog report.
"""


with DAG(
    dag_id='oscar_housekeeping_cleanup',
    doc_md=HOUSEKEEPING_GUIDE_MD,
    default_args=default_args,
    description='Database housekeeping; Airflow log retention is managed by airflow-log-groomer',
    schedule=getattr(housekeeping_config, 'DAG_SCHEDULE', '0 11 * * *'),  # Daily at 3 AM PST (11 AM UTC)
    dagrun_timeout=timedelta(minutes=getattr(housekeeping_config, 'DAG_TIMEOUT_MINUTES', 120)),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # One run at a time: overlapping runs delete the same rows and deadlock
    # each other (a manual trigger while the scheduled run is active, or the
    # scheduled run Airflow starts on unpause).
    max_active_runs=1,
    on_failure_callback=_dag_failure,
    on_success_callback=_dag_success,
    tags=['housekeeping', 'logs', 'worklog'],
):
    create_worklog_task = create_worklog()
    policy_task = load_housekeeping_policy()

    # Apply per-task retries using config
    clean_logs_task = clean_airflow_logs.override(
        retries=housekeeping_config.MAX_RETRIES,
        retry_delay=timedelta(minutes=housekeeping_config.RETRY_DELAY_MINUTES),
    )()
    clean_task_history_task = clean_task_history.override(
        on_success_callback=_cleanup_success,
        on_failure_callback=_cleanup_failure,
        retries=housekeeping_config.MAX_RETRIES,
        retry_delay=timedelta(minutes=housekeeping_config.RETRY_DELAY_MINUTES),
        execution_timeout=timedelta(minutes=getattr(housekeeping_config, 'TASK_HISTORY_TIMEOUT_MINUTES', 60)),
    )()
    clean_alert_history_task = clean_alert_history.override(
        retries=housekeeping_config.MAX_RETRIES,
        retry_delay=timedelta(minutes=housekeeping_config.RETRY_DELAY_MINUTES),
        execution_timeout=timedelta(minutes=getattr(housekeeping_config, 'TASK_TIMEOUT_MINUTES', 60)),
    )()
    clean_alert_history_records_task = clean_alert_history_records.override(
        on_success_callback=_cleanup_success,
        on_failure_callback=_cleanup_failure,
        retries=housekeeping_config.MAX_RETRIES,
        retry_delay=timedelta(minutes=housekeeping_config.RETRY_DELAY_MINUTES),
        execution_timeout=timedelta(minutes=getattr(housekeeping_config, 'TASK_TIMEOUT_MINUTES', 60)),
    )()
    clean_notification_audit_history_task = clean_notification_audit_history.override(
        on_success_callback=_cleanup_success,
        on_failure_callback=_cleanup_failure,
        retries=housekeeping_config.MAX_RETRIES,
        retry_delay=timedelta(minutes=housekeeping_config.RETRY_DELAY_MINUTES),
        execution_timeout=timedelta(minutes=getattr(housekeeping_config, 'NOTIFICATION_AUDIT_TIMEOUT_MINUTES', 60)),
    )()
    clean_ticketing_audit_history_task = clean_ticketing_audit_history.override(
        on_success_callback=_cleanup_success,
        on_failure_callback=_cleanup_failure,
        retries=housekeeping_config.MAX_RETRIES,
        retry_delay=timedelta(minutes=housekeeping_config.RETRY_DELAY_MINUTES),
        execution_timeout=timedelta(minutes=getattr(housekeeping_config, 'TICKETING_AUDIT_TIMEOUT_MINUTES', 60)),
    )()
    clean_user_audit_history_task = clean_user_audit_history.override(
        on_success_callback=_cleanup_success,
        on_failure_callback=_cleanup_failure,
        retries=housekeeping_config.MAX_RETRIES,
        retry_delay=timedelta(minutes=housekeeping_config.RETRY_DELAY_MINUTES),
        execution_timeout=timedelta(minutes=getattr(housekeeping_config, 'USER_AUDIT_TIMEOUT_MINUTES', 60)),
    )()
    clean_alert_audit_task = clean_alert_audit.override(
        on_success_callback=_cleanup_success,
        on_failure_callback=_cleanup_failure,
        retries=housekeeping_config.MAX_RETRIES,
        retry_delay=timedelta(minutes=housekeeping_config.RETRY_DELAY_MINUTES),
    )()

    close_worklog_task = close_worklog()
    close_worklog_task.trigger_rule = TriggerRule.ALL_DONE

    # Barrier to ensure downstream runs regardless of individual cleanup task results
    join_cleanup = EmptyOperator(task_id="join_cleanup", trigger_rule=TriggerRule.ALL_DONE)

    # Run all cleanup tasks in parallel after creating the worklog, then join and close
    create_worklog_task >> policy_task >> [
        clean_logs_task,
        clean_task_history_task,
        clean_alert_history_task,
        clean_alert_history_records_task,
        clean_notification_audit_history_task,
        clean_ticketing_audit_history_task,
        clean_user_audit_history_task,
        clean_alert_audit_task,
    ] >> join_cleanup >> close_worklog_task >> enforce_housekeeping_result()

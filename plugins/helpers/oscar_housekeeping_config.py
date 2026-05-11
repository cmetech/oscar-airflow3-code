"""
Configuration file for OSCAR Housekeeping DAG
Contains all configurable parameters for cleanup operations

This file should be placed in airflow/plugins/helpers/ for proper organization
"""

import os

# ============================================================================
# MASTER CONTROL FLAGS - Emergency Kill Switches (Per Category)
# ============================================================================

# Master flag to enable/disable Airflow logs cleanup
# Set to False to completely skip Airflow logs cleanup in case of emergency
ENABLE_AIRFLOW_LOGS_CLEANUP = True

# Master flag to enable/disable OSCAR database cleanup (TaskHistory, Alerts, Notifications)
# Set to False to completely skip all database cleanup operations in case of emergency
ENABLE_DATABASE_CLEANUP = True

# DRY RUN MODE - Test without deleting
# When True: Queries run but NO commits (shows what WOULD be deleted)
# When False: Normal operation (actually deletes data)
# ⚠️ IMPORTANT: Test with True first, verify results, then set to False
DRY_RUN_MODE = False  # ✅ LIVE MODE - DAG deletes + optimizes for real every run

# ============================================================================
# GRANULAR TASK-SPECIFIC FLAGS - Individual Task Control
# ============================================================================

# Task History Cleanup - TM_TaskHistory table
ENABLE_TASK_HISTORY_CLEANUP = True

# Alert Cleanup - AM_Alert, AM_AlertLabel, AM_AlertAnnotation, AM_AlertGroup tables
# ⚠️ LEGACY: These tables are no longer used (removed from oscar-alertmanager models)
# Current architecture uses AM_AlertHistory instead. Disable by default.
ENABLE_ALERT_CLEANUP = False

# Alert History Cleanup - AM_AlertHistory, AM_AlertHistoryLabel, AM_AlertHistoryAnnotation tables
ENABLE_ALERT_HISTORY_CLEANUP = True

# Notification Audit Cleanup - NTF_Notifications_Audit table
ENABLE_NOTIFICATION_AUDIT_CLEANUP = True

# Ticketing Audit Cleanup - TKT_Ticketing_Audit + TKT_Ticketing_Audit_Summary tables
ENABLE_TICKETING_AUDIT_CLEANUP = True

# User Audit Cleanup - UA_User_Audit + UA_User_Audit_Summary tables (106GB+ - LARGEST TABLE)
ENABLE_USER_AUDIT_CLEANUP = True

# ============================================================================
# PROCESS-SPECIFIC CLEANING CONSTANTS - Each cleaning operation is independent
# ============================================================================

# Airflow Logs Cleaning
AIRFLOW_LOGS_CLEANUP_ENABLED = True
AIRFLOW_LOGS_DAYS_TO_KEEP = 7
AIRFLOW_LOGS_MAX_FILE_SIZE = '200MB'           # Don't delete files larger than this
AIRFLOW_LOGS_SAFE_EXTENSIONS = ['.log', '.log.gz']  # Only clean these extensions
AIRFLOW_LOGS_EXCLUDE_PATTERNS = ['current', 'active', 'running']  # Don't delete files with these names

# Airflow Logs Performance Settings - Prevent infinite execution
AIRFLOW_LOGS_MAX_EXECUTION_TIME_SECONDS = 1800  # 30 minutes max execution time
AIRFLOW_LOGS_MAX_DIRS_TO_PROCESS = 5000         # Stop after processing this many directories
AIRFLOW_LOGS_BATCH_PROGRESS_INTERVAL = 100      # Log progress every N items

# ============================================================================
# APPLICATION LOGS CLEANUP SETTINGS - Clean /home/splunk/logs subdirectories
# ============================================================================
# These are the main disk space consumers based on actual usage:
# - middleware: 16GB
# - taskmanager: 9.5GB  
# - scheduler: 146MB
# - trapreceiver: 596MB
# - monitor: 505MB
# - datastore_middleware: 390MB
# - email_dropped.log files: 1.7GB+

# Master flag for application logs cleanup
ENABLE_APPLICATION_LOGS_CLEANUP = True

# Base path for all application logs (from HOST_LOGGING_DIR env var)
# In prod: /data/oscar/logs, default fallback: /home/splunk/logs
APPLICATION_LOGS_BASE_PATH = os.environ.get('HOST_LOGGING_DIR', '/data/oscar/logs')

# Middleware Logs (16GB - largest consumer)
MIDDLEWARE_LOGS_CLEANUP_ENABLED = True
MIDDLEWARE_LOGS_DAYS_TO_KEEP = 3  # Aggressive cleanup due to size
MIDDLEWARE_LOGS_MAX_EXECUTION_TIME_SECONDS = 600  # 10 minutes
MIDDLEWARE_LOGS_MAX_FILES_TO_PROCESS = 10000

# TaskManager Logs (9.5GB - second largest)
TASKMANAGER_LOGS_CLEANUP_ENABLED = True
TASKMANAGER_LOGS_DAYS_TO_KEEP = 3  # Aggressive cleanup due to size
TASKMANAGER_LOGS_MAX_EXECUTION_TIME_SECONDS = 600  # 10 minutes
TASKMANAGER_LOGS_MAX_FILES_TO_PROCESS = 10000

# Scheduler Logs (146MB)
SCHEDULER_LOGS_CLEANUP_ENABLED = True
SCHEDULER_LOGS_DAYS_TO_KEEP = 7
SCHEDULER_LOGS_MAX_EXECUTION_TIME_SECONDS = 300  # 5 minutes
SCHEDULER_LOGS_MAX_FILES_TO_PROCESS = 5000

# TrapReceiver Logs (596MB)
TRAPRECEIVER_LOGS_CLEANUP_ENABLED = True
TRAPRECEIVER_LOGS_DAYS_TO_KEEP = 5
TRAPRECEIVER_LOGS_MAX_EXECUTION_TIME_SECONDS = 300  # 5 minutes
TRAPRECEIVER_LOGS_MAX_FILES_TO_PROCESS = 5000

# Monitor Logs (505MB)
MONITOR_LOGS_CLEANUP_ENABLED = True
MONITOR_LOGS_DAYS_TO_KEEP = 7
MONITOR_LOGS_MAX_EXECUTION_TIME_SECONDS = 300  # 5 minutes
MONITOR_LOGS_MAX_FILES_TO_PROCESS = 5000

# Datastore Middleware Logs (390MB)
DATASTORE_MIDDLEWARE_LOGS_CLEANUP_ENABLED = True
DATASTORE_MIDDLEWARE_LOGS_DAYS_TO_KEEP = 7
DATASTORE_MIDDLEWARE_LOGS_MAX_EXECUTION_TIME_SECONDS = 300  # 5 minutes
DATASTORE_MIDDLEWARE_LOGS_MAX_FILES_TO_PROCESS = 5000

# Email Dropped Logs (1.7GB+ - rotated log files like email_dropped.log.2026-02-11_23)
EMAIL_DROPPED_LOGS_CLEANUP_ENABLED = True
EMAIL_DROPPED_LOGS_DAYS_TO_KEEP = 3  # Aggressive cleanup
EMAIL_DROPPED_LOGS_MAX_EXECUTION_TIME_SECONDS = 300  # 5 minutes
EMAIL_DROPPED_LOGS_MAX_FILES_TO_PROCESS = 1000

# Traps Not Mapped Logs (similar rotated pattern)
TRAPS_NOT_MAPPED_LOGS_CLEANUP_ENABLED = True
TRAPS_NOT_MAPPED_LOGS_DAYS_TO_KEEP = 7
TRAPS_NOT_MAPPED_LOGS_MAX_EXECUTION_TIME_SECONDS = 300  # 5 minutes
TRAPS_NOT_MAPPED_LOGS_MAX_FILES_TO_PROCESS = 1000

# Generic rotated log pattern cleanup (catches *.log.YYYY-MM-DD* patterns)
ROTATED_LOGS_CLEANUP_ENABLED = True
ROTATED_LOGS_DAYS_TO_KEEP = 5
ROTATED_LOGS_MAX_EXECUTION_TIME_SECONDS = 600  # 10 minutes
ROTATED_LOGS_MAX_FILES_TO_PROCESS = 10000

# OSCAR Service Logs Cleaning - Process-Specific
OSCAR_SERVICE_LOGS_CLEANUP_ENABLED = True
OSCAR_SERVICE_LOGS_SAFE_EXTENSIONS = ['.log']  # Only .log files (no .gz, .out, .err found)

# OSCAR Service Log Paths (uses HOST_LOGGING_DIR env var)
OSCAR_SERVICE_LOG_PATHS = [
    APPLICATION_LOGS_BASE_PATH         # From HOST_LOGGING_DIR env var
]

# OSCAR Service Log Patterns (REAL patterns found)
OSCAR_SERVICE_LOG_PATTERNS = [
    'email_dropped.log*',              # email_dropped.log and rotated versions
    'traps_not_mapped.log*',           # traps_not_mapped.log and rotated versions
    '*.log'                            # Any other .log files
]

# OSCAR Middleware
OSCAR_MIDDLEWARE_LOGS_DAYS_TO_KEEP = 7
OSCAR_MIDDLEWARE_LOGS_MAX_SIZE = '50MB'
OSCAR_MIDDLEWARE_LOGS_EXCLUDE = ['current.log', 'active.log', 'middleware.log']

# OSCAR Datastore
OSCAR_DATASTORE_LOGS_DAYS_TO_KEEP = 10
OSCAR_DATASTORE_LOGS_MAX_SIZE = '200MB'
OSCAR_DATASTORE_LOGS_EXCLUDE = ['current.log', 'database.log', 'query.log']

# OSCAR Frontend
OSCAR_FRONTEND_LOGS_DAYS_TO_KEEP = 5
OSCAR_FRONTEND_LOGS_MAX_SIZE = '25MB'
OSCAR_FRONTEND_LOGS_EXCLUDE = ['current.log', 'access.log', 'error.log']

# OSCAR Metricstore
OSCAR_METRICSTORE_LOGS_DAYS_TO_KEEP = 7
OSCAR_METRICSTORE_LOGS_MAX_SIZE = '75MB'
OSCAR_METRICSTORE_LOGS_EXCLUDE = ['current.log', 'metrics.log', 'performance.log']

# Docker Cleanup - Independent Operation
DOCKER_CLEANUP_ENABLED = True
DOCKER_SYSTEM_PRUNE_ENABLED = True
DOCKER_BUILDER_PRUNE_ENABLED = True
DOCKER_NETWORK_PRUNE_ENABLED = True
DOCKER_VOLUME_PRUNE_ENABLED = True
DOCKER_CONTAINER_PRUNE_ENABLED = True

# Docker Log Cleanup - Specific log file management
DOCKER_LOGS_DAYS_TO_KEEP = 7          # Keep logs for 7 days
DOCKER_LOGS_MAX_SIZE = '100MB'         # Truncate logs larger than 100MB
DOCKER_LOGS_CLEANUP_ENABLED = True     # Enable Docker log cleanup

# Temp Files Cleaning - Independent Operation
TEMP_FILES_CLEANUP_ENABLED = True
TEMP_FILES_DAYS_TO_KEEP = 7
TEMP_FILES_MAX_SIZE = '10MB'
TEMP_FILES_SAFE_EXTENSIONS = ['.tmp', '.temp', '.cache']
TEMP_FILES_EXCLUDE_PATTERNS = ['current', 'active', 'lock', '.pid']

# Database Cleanup - Independent Operation
DATABASE_CLEANUP_ENABLED = True
DATABASE_OPTIMIZATION_ENABLED = True
DATABASE_MAINTENANCE_ENABLED = True
DATABASE_BACKUP_BEFORE_CLEANUP = True

# Backup Files Cleaning - Independent Operation
BACKUP_FILES_CLEANUP_ENABLED = True
BACKUP_FILES_DAYS_TO_KEEP = 30
BACKUP_FILES_MAX_SIZE = '1GB'
BACKUP_FILES_SAFE_EXTENSIONS = ['.sql', '.tar.gz', '.zip', '.bak']
BACKUP_FILES_EXCLUDE_PATTERNS = ['latest', 'current', 'recent']

# Different retention periods for different backup types
BACKUP_IMAGES_DAYS_TO_KEEP = 90        # Keep image backups longer (3 months)
BACKUP_IMAGES_MAX_SIZE = '5GB'         # Image backups can be larger
BACKUP_SSL_DAYS_TO_KEEP = 365         # Keep SSL backups much longer (1 year)
BACKUP_SSL_MAX_SIZE = '200MB'          # SSL backups are typically small

# Cache Files Cleaning - Independent Operation
CACHE_FILES_CLEANUP_ENABLED = True
CACHE_FILES_DAYS_TO_KEEP = 3
CACHE_FILES_MAX_SIZE = '200MB'
CACHE_FILES_SAFE_EXTENSIONS = ['.cache', '.tmp', '.temp']
CACHE_FILES_EXCLUDE_PATTERNS = ['current', 'active', 'lock']

# ============================================================================
# SAFETY SETTINGS - Prevent deletion of fresh/active files
# ============================================================================

# File Activity Detection
CHECK_FILE_ACTIVITY = True              # Check if file is actively being written to
CHECK_FILE_LOCKS = True                 # Check if file is locked
CHECK_FILE_PERMISSIONS = True           # Check file permissions before deletion
CHECK_FILE_OWNERSHIP = True             # Check file ownership

# Size-Based Safety
MAX_SAFE_DELETE_SIZE = '200MB'         # Maximum file size to delete without confirmation
MIN_FREE_SPACE_REQUIRED = '5GB'        # Minimum free space required before cleanup
MAX_DELETION_PERCENTAGE = 80           # Maximum percentage of files to delete in one run

# Time-Based Safety
MIN_FILE_AGE_HOURS = 1                 # Minimum age of file before considering deletion
SAFE_DELETION_WINDOW_HOURS = 2         # Only delete during low-activity hours (2-4 AM)

# Process Safety
DRY_RUN_ENABLED = False                # Enable dry-run mode (no actual deletion)
CONFIRMATION_REQUIRED = False          # Require confirmation before deletion
BACKUP_BEFORE_DELETE = False           # Backup files before deletion
SAFE_MODE = True                       # Enable safe mode (extra checks)

# ============================================================================
# VICTORIAMETRICS SETTINGS
# ============================================================================

VICTORIAMETRICS_ENABLED = True
VICTORIAMETRICS_URL = os.getenv('VMDB_URL', 'http://vmdb:8428')
VICTORIAMETRICS_RETENTION_DAYS = 30
VICTORIAMETRICS_CLEANUP_ENABLED = True
VICTORIAMETRICS_BATCH_SIZE = 1000

# ============================================================================
# STALE PROCESS METRICS CLEANUP SETTINGS
# ============================================================================
# Cleans oscar:up metrics for hosts that are disabled in inventory
# This prevents false ProcessFailed alerts for disabled hosts
# 
# How it works:
# 1. Fetches list of disabled hosts from inventory API
# 2. For each disabled host, deletes oscar:up metrics from VMDB
# 3. Also deletes from Pushgateway if configured
#
# Why this is needed:
# - When a host is disabled, check_processes task stops running
# - But old metrics persist in Pushgateway indefinitely
# - VMAgent keeps scraping stale metrics, refreshing timestamps
# - This causes false ProcessFailed alerts for disabled hosts

ENABLE_STALE_METRICS_CLEANUP = True

# Inventory API settings
INVENTORY_API_URL = os.getenv('MIDDLEWARE_BASE_URL', 'http://middleware:8000')

# Pushgateway settings (for deleting pushed metrics)
PUSHGATEWAY_URL = os.getenv('PUSHGATEWAY_URL', 'http://pushgateway:9091')
PUSHGATEWAY_CLEANUP_ENABLED = True  # Also delete from Pushgateway

# Metric selectors to clean for disabled hosts
# These are the metrics pushed by check_processes task
STALE_METRICS_SELECTORS = [
    'oscar:up',  # Process monitoring metric
]

# Job name used when pushing metrics (for Pushgateway deletion)
STALE_METRICS_JOB_NAME = 'check_processes'

# ============================================================================
# EMAIL NOTIFICATION SETTINGS
# ============================================================================

ENABLE_EMAIL_NOTIFICATIONS = False
EMAIL_ON_SUCCESS = True
EMAIL_ON_FAILURE = True
EMAIL_ON_RETRY = False
EMAIL_RECIPIENTS = ['admin@oscar.local']

# ============================================================================
# PERFORMANCE AND SAFETY SETTINGS
# ============================================================================

# Batch Processing
BATCH_SIZE = 1000                      # Batch size for database operations
MAX_ITERATIONS = 100                   # Maximum iterations for cleanup loops
TIMEOUT_SECONDS = 300                  # Timeout for cleanup operations
MAX_CONCURRENT_TASKS = 5               # Maximum concurrent cleanup tasks

# Safety Settings
DRY_RUN_ENABLED = False                # Enable dry-run mode (no actual deletion)
CONFIRMATION_REQUIRED = False          # Require confirmation before deletion
BACKUP_BEFORE_DELETE = False           # Backup files before deletion
SAFE_MODE = True                       # Enable safe mode (extra checks)

# File Size Limits
MAX_FILE_SIZE_TO_DELETE = '1GB'        # Maximum file size to delete without confirmation
MIN_FREE_SPACE_REQUIRED = '5GB'        # Minimum free space required before cleanup
MAX_DELETION_PERCENTAGE = 80           # Maximum percentage of files to delete in one run

# ============================================================================
# MONITORING AND METRICS SETTINGS
# ============================================================================

ENABLE_METRICS_COLLECTION = True       # Collect cleanup metrics
METRICS_RETENTION_DAYS = 90           # Keep metrics for 90 days
ENABLE_CLEANUP_REPORTING = True        # Enable detailed cleanup reporting
LOG_CLEANUP_STATISTICS = True          # Log detailed cleanup statistics

# ============================================================================
# SCHEDULING AND EXECUTION SETTINGS
# ============================================================================

# DAG Schedule
DAG_SCHEDULE = '0 11 * * *'            # Run daily at 3 AM PST (11 AM UTC)
DAG_START_DATE = '2023-01-01'         # DAG start date
DAG_CATCHUP = False                    # Don't catch up on missed runs

# Task Timeouts
TASK_TIMEOUT_MINUTES = 60              # Default individual task timeout (1 hour)
DAG_TIMEOUT_MINUTES = 120              # Overall DAG timeout (2 hours)

# Per-task timeout overrides (minutes)
# With daily cleanup + optimization, each task should finish well within 60 min
# These are safety limits, not expected durations
TASK_HISTORY_TIMEOUT_MINUTES = 60      # TM_History + TM_StageHistory cleanup + optimize
NOTIFICATION_AUDIT_TIMEOUT_MINUTES = 60  # NTF_Notifications_Audit cleanup + optimize
TICKETING_AUDIT_TIMEOUT_MINUTES = 60   # TKT_Ticketing_Audit cleanup + optimize
USER_AUDIT_TIMEOUT_MINUTES = 60        # UA_User_Audit cleanup + optimize

# Retry Settings
MAX_RETRIES = 3                        # Maximum retry attempts
RETRY_DELAY_MINUTES = 5                # Delay between retries

# ============================================================================
# MISSING CONSTANTS - Still referenced in the DAG
# ============================================================================

# Database Settings
OSCAR_DB_CONNECTION_ID = 'oscar_db'    # Database connection ID

# Task History Cleanup Settings (TM_History + TM_StageHistory) - 44GB combined
TASK_HISTORY_DAYS_TO_KEEP = 7
TASK_HISTORY_BATCH_SIZE = 2000          # ~230K rows/day × 7 days = 1.6M rows max; 2000×400 = 800K capacity per run
TASK_HISTORY_MAX_ITERATIONS = 600      # 2000×600=1.2M capacity; clears 242K backlog + daily 230K growth
TASK_HISTORY_TABLE_SWAP_MODE = False  # Set True only for emergency disk reclaim
TASK_HISTORY_MAINTENANCE_OPTIMIZATION = True  # Run OPTIMIZE TABLE after normal delete (reclaims disk space safely)

# Alert Cleanup Settings (AM_Alert) - ⚠️ LEGACY ⚠️
# These tables (AM_Alert, AM_AlertGroup, etc.) are no longer used in current architecture.
# See: oscar-alertmanager/src/app/core/db.py - models have been removed
# Only configure if you have legacy data that needs cleanup
ALERTS_DAYS_TO_KEEP = 7
ALERTS_BATCH_SIZE = 1000
ALERTS_MAX_ITERATIONS = 100
ALERTS_MAINTENANCE_OPTIMIZATION = False  # Run OPTIMIZE TABLE after delete (reclaims disk space safely)

# Alert History Cleanup Settings (AM_AlertHistory)
# IMPORTANT: This cleanup is CONSERVATIVE and preserves critical data:
# - Keeps ALL alerts with ticket_id (audit trail)
# - Keeps ALL acknowledged alerts (manual review happened)
# - Keeps the LATEST episode for each fingerprint (most recent occurrence)
# - Only deletes: old, unticketed, unacknowledged, non-latest episodes
#
# NOTE: We do NOT filter by status='resolved' because:
# - After Redis TTL expires (24h-7d), alerts are no longer actively tracked
# - Alertmanager may never send "resolved" for one-shot or deleted alerts
# - Truly active alerts will have recent last_occurrence (protected by age check)
# - Orphaned "firing" status alerts are just stale database records
ALERT_HISTORY_DAYS_TO_KEEP = 20
ALERT_HISTORY_BATCH_SIZE = 1000
ALERT_HISTORY_MAX_ITERATIONS = 100
ALERT_HISTORY_MAINTENANCE_OPTIMIZATION = False  # Run OPTIMIZE TABLE after delete (reclaims disk space safely)

# Notification Audit Cleanup Settings (NTF_Notifications_Audit) - 8GB
NOTIFICATION_AUDIT_DAYS_TO_KEEP = 30
NOTIFICATION_AUDIT_BATCH_SIZE = 2000   # ~100K rows/day; 2000×200 = 400K capacity per run
NOTIFICATION_AUDIT_MAX_ITERATIONS = 200  # Must handle 100K+ rows/day of notification audit data
NOTIFICATION_AUDIT_TABLE_SWAP_MODE = False  # Set True only for emergency disk reclaim
NOTIFICATION_AUDIT_MAINTENANCE_OPTIMIZATION = True  # Run OPTIMIZE TABLE after normal delete (reclaims disk space safely)

# Ticketing Audit Cleanup Settings (TKT_Ticketing_Audit)
# Tracks all ticket creation, updates, and failures for audit and compliance
# Keep longer than other audit tables due to compliance and SLA tracking needs
TICKETING_AUDIT_DAYS_TO_KEEP = 40  # 40 days for compliance/audit requirements
TICKETING_AUDIT_BATCH_SIZE = 1000      # ~80 rows/day; tiny table, default batch is fine
TICKETING_AUDIT_MAX_ITERATIONS = 100   # More than enough for ~80 rows/day
TICKETING_AUDIT_TABLE_SWAP_MODE = False  # Regular maintenance mode (no disk reclaim)
TICKETING_AUDIT_MAINTENANCE_OPTIMIZATION = True  # Run OPTIMIZE TABLE after normal delete (reclaims disk space safely)

# User Audit Cleanup Settings (UA_User_Audit) - LARGEST TABLE
# Tracks all user actions: login, logout, create, read, update, delete, navigate
# This table grows VERY fast due to UI navigation tracking
# REQUIRES INDEX: CREATE INDEX idx_ua_created_at ON UA_User_Audit(created_at);
USER_AUDIT_DAYS_TO_KEEP = 7  # Aggressive cleanup - table grows fast
USER_AUDIT_BATCH_SIZE = 5000  # Larger batches for faster cleanup
USER_AUDIT_MAX_ITERATIONS = 500  # More iterations needed due to table size
USER_AUDIT_TABLE_SWAP_MODE = False  # Set True only for emergency disk reclaim
USER_AUDIT_MAINTENANCE_OPTIMIZATION = True  # Run OPTIMIZE TABLE after normal delete (reclaims disk space safely)

# ============================================================================
# TABLE SWAP MODE & MAINTENANCE OPTIMIZATION EXPLANATION
# ============================================================================
# Each table has TWO independent flags:
#
# *_TABLE_SWAP_MODE (bool):
#   False (DEFAULT) = Normal DELETE (batch delete old rows)
#         Fast with proper indexes, no risk, space reused by InnoDB.
#         Use for daily maintenance - prevents table growth.
#   True  = Table swap (CREATE new → COPY recent → RENAME → DROP old)
#         Actually reclaims disk space to OS. Use ONLY for emergency cleanup.
#         Risk: ~10-100ms window where writes may fail during RENAME.
#
# *_MAINTENANCE_OPTIMIZATION (bool):
#   False (DEFAULT) = No post-delete optimization
#   True  = Run OPTIMIZE TABLE after normal delete mode
#         Rebuilds table and reclaims disk space safely (no write failures)
#         Slower than plain delete but faster than table swap
#         NOTE: Only applies to NORMAL DELETE mode (swap already reclaims space)
#
# MODES SUMMARY:
#   swap=False, optimize=False → Fastest, space reused internally by InnoDB
#   swap=False, optimize=True  → Normal delete + OPTIMIZE TABLE (safe disk reclaim)
#   swap=True                  → Table swap (emergency disk reclaim, brief risk)
#
# RECOMMENDED WORKFLOW:
# 1. Daily maintenance: swap=False, optimize=False (default)
# 2. Scheduled maintenance window: swap=False, optimize=True (safe reclaim)
# 3. Emergency only: swap=True (if disk is full)
#
# REQUIRED INDEXES for fast DELETE operations:
#   CREATE INDEX idx_ua_created_at ON UA_User_Audit(created_at);
#   CREATE INDEX idx_tm_created_at ON TM_History(created_at);
#   CREATE INDEX idx_ntf_created_at ON NTF_Notifications_Audit(created_at);
#   CREATE INDEX idx_tkt_created_at ON TKT_Ticketing_Audit(created_at);

# Metric Data Settings
METRIC_DATA_DAYS_TO_KEEP = 30
METRIC_DATA_CLEANUP_ENABLED = True
METRIC_DATA_BATCH_SIZE = 1000

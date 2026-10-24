"""
Configuration file for OSCAR DB Housekeeping DAG
Contains all configurable parameters for cleanup operations

This file should be placed in airflow/plugins/helpers/ for proper organization
"""

# ============================================================================
# MASTER CONTROL FLAGS - Emergency Kill Switches (Per Category)
# ============================================================================

# Master flag to enable/disable OSCAR database cleanup (TaskHistory, Alerts, Notifications)
# Set to False to completely skip all database cleanup operations in case of emergency
ENABLE_DATABASE_CLEANUP = True

# DRY RUN MODE - Test without deleting
# When True: Queries run but NO commits (shows what WOULD be deleted)
# When False: Normal operation (actually deletes data)
# ⚠️ IMPORTANT: Test with True first, verify results, then set to False
DRY_RUN_MODE = False  # LIVE MODE - the DAG commits bounded deletes

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
TASK_HISTORY_TABLE_SWAP_MODE = False  # Unsupported: the DAG rejects table swaps
TASK_HISTORY_MAINTENANCE_OPTIMIZATION = False  # OPTIMIZE rebuilds the table and needs substantial free disk

# Alert Cleanup Settings (AM_Alert) - ⚠️ LEGACY ⚠️
# These tables (AM_Alert, AM_AlertGroup, etc.) are no longer used in current architecture.
# See: oscar-alertmanager/src/app/core/db.py - models have been removed
# Only configure if you have legacy data that needs cleanup
ALERTS_DAYS_TO_KEEP = 7
ALERTS_BATCH_SIZE = 1000
ALERTS_MAX_ITERATIONS = 100
ALERTS_MAINTENANCE_OPTIMIZATION = False  # Run OPTIMIZE TABLE after delete (reclaims disk space safely)

# Alert History Cleanup Settings (AM_AlertHistory)
# Only episodes with resolved_at before the cutoff are eligible. Open or
# unresolved episodes remain even if their first occurrence is old.
ALERT_HISTORY_DAYS_TO_KEEP = 30
ALERT_HISTORY_BATCH_SIZE = 1000
ALERT_HISTORY_MAX_ITERATIONS = 100
ALERT_HISTORY_MAINTENANCE_OPTIMIZATION = False  # Run OPTIMIZE TABLE after delete (reclaims disk space safely)

# Alert activity audit is evidence separate from the mutable alert episode.
ENABLE_ALERT_AUDIT_CLEANUP = True
ALERT_AUDIT_DAYS_TO_KEEP = 90
ALERT_AUDIT_BATCH_SIZE = 1000
ALERT_AUDIT_MAX_ITERATIONS = 100

# Notification Audit Cleanup Settings (NTF_Notifications_Audit) - 8GB
NOTIFICATION_AUDIT_DAYS_TO_KEEP = 30
NOTIFICATION_AUDIT_BATCH_SIZE = 2000   # ~100K rows/day; 2000×200 = 400K capacity per run
NOTIFICATION_AUDIT_MAX_ITERATIONS = 200  # Must handle 100K+ rows/day of notification audit data
NOTIFICATION_AUDIT_TABLE_SWAP_MODE = False  # Unsupported: the DAG rejects table swaps
NOTIFICATION_AUDIT_MAINTENANCE_OPTIMIZATION = False  # OPTIMIZE rebuilds the table and needs substantial free disk

# Ticketing Audit Cleanup Settings (TKT_Ticketing_Audit)
# Tracks all ticket creation, updates, and failures for audit and compliance
# Keep longer than other audit tables due to compliance and SLA tracking needs
TICKETING_AUDIT_DAYS_TO_KEEP = 40  # 40 days for compliance/audit requirements
TICKETING_AUDIT_BATCH_SIZE = 1000      # ~80 rows/day; tiny table, default batch is fine
TICKETING_AUDIT_MAX_ITERATIONS = 100   # More than enough for ~80 rows/day
TICKETING_AUDIT_TABLE_SWAP_MODE = False  # Unsupported: the DAG rejects table swaps
TICKETING_AUDIT_MAINTENANCE_OPTIMIZATION = False  # OPTIMIZE rebuilds the table and needs substantial free disk

# User Audit Cleanup Settings (UA_User_Audit) - LARGEST TABLE
# Tracks all user actions: login, logout, create, read, update, delete, navigate
# This table grows VERY fast due to UI navigation tracking
# REQUIRES INDEX: CREATE INDEX idx_ua_created_at ON UA_User_Audit(created_at);
USER_AUDIT_DAYS_TO_KEEP = 7  # Aggressive cleanup - table grows fast
USER_AUDIT_BATCH_SIZE = 5000  # Larger batches for faster cleanup
USER_AUDIT_MAX_ITERATIONS = 500  # More iterations needed due to table size
USER_AUDIT_TABLE_SWAP_MODE = False  # Unsupported: the DAG rejects table swaps
USER_AUDIT_MAINTENANCE_OPTIMIZATION = False  # OPTIMIZE rebuilds the table and needs substantial free disk

# ============================================================================
# MAINTENANCE OPTIMIZATION
# ============================================================================
# Batch DELETE is the production default; InnoDB reuses released pages.
# Setting any *_TABLE_SWAP_MODE flag to True fails its task. A table swap can
# lose concurrent writes or detach triggers, so it is never a recovery mode.
# OPTIMIZE TABLE rebuilds the table, uses substantial temporary disk, and
# should be scheduled only after checking capacity and operational impact.
#
# REQUIRED INDEXES for fast DELETE operations:
#   CREATE INDEX idx_ua_created_at ON UA_User_Audit(created_at);
#   CREATE INDEX idx_tm_created_at ON TM_History(created_at);
#   CREATE INDEX idx_ntf_created_at ON NTF_Notifications_Audit(created_at);
#   CREATE INDEX idx_tkt_created_at ON TKT_Ticketing_Audit(created_at);

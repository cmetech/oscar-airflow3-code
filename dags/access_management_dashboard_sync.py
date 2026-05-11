#!/usr/bin/env python3
"""
Access Management Dashboard Sync DAG

This DAG syncs data from EXT_ACCESS_MANAGEMENT_ACTIVITY table to Elasticsearch
every 10 minutes for the Kibana dashboard. It maintains state based on u_identifier:
- Creates new entries for new identifiers
- Updates existing entries for existing identifiers
- Tracks metrics for completed requests and total requests
"""

import logging
import os
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy.orm import Session

# Import existing hooks
from hooks.worklog_hook import WorkLogHook
from hooks.access_management_db_hook import AccessManagementSQLHook
from hooks.elasticsearch_hook import ElasticsearchHook
from hooks.prometheus_metrics_hook import PrometheusMetricsHook

logger = logging.getLogger(__name__)

# Configuration from environment variables
SYNC_INTERVAL_HOURS = int(os.environ.get("ACCESS_MANAGEMENT_SYNC_INTERVAL_HOURS", "24"))
DAG_SCHEDULE_INTERVAL = os.environ.get("ACCESS_MANAGEMENT_DAG_SCHEDULE_INTERVAL", "*/10 * * * *")
ELASTICSEARCH_INDEX_ACCESS_MANAGEMENT = os.environ.get("ACCESS_MANAGEMENT_ES_INDEX", "access-management-dashboard")

# Elasticsearch mapping for access-management-dashboard index
ES_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "u_identifier": {"type": "keyword"},
            "u_task_type": {"type": "keyword"},
            "u_user_name": {"type": "text"},
            "u_user_adid": {"type": "keyword"},
            "u_user_email": {"type": "keyword"},
            "u_mobile_no": {"type": "keyword"},
            "u_env": {"type": "keyword"},
            "u_status": {"type": "keyword"},
            "u_organization": {"type": "keyword"},
            "created_on": {"type": "date", "format": "strict_date_optional_time"},
            "updated_on": {"type": "date", "format": "strict_date_optional_time"},
            "created_by": {"type": "keyword"},
            "u_svc_name": {"type": "text"},
            "u_manager_adid": {"type": "keyword"},
            "u_manager_name": {"type": "text"},
            "u_manager_email": {"type": "keyword"},
            "u_team_name": {"type": "keyword"},
            "u_business_justification": {"type": "text"},
            "u_ref_adid": {"type": "keyword"},
            "u_description": {"type": "text"},
            "u_role": {"type": "keyword"},
            "u_platform": {"type": "keyword"},
            "u_user_ntid_signum": {"type": "keyword"},
            "u_svc_owner_email": {"type": "keyword"},
            "u_modification_action": {"type": "keyword"},
            "u_intended_use": {"type": "text"},
            "u_svc_owner_adid": {"type": "keyword"},
            "u_svc_owner_ntid_signum": {"type": "keyword"},
            "u_new_password": {"type": "keyword"},
            "u_remarks": {"type": "text"},
            "update_by": {"type": "keyword"},
            "u_sr_no": {"type": "keyword"},
            "u_wo_no": {"type": "keyword"},
            "u_comments": {"type": "text"},
            "@timestamp": {"type": "date"},
            "type": {"type": "keyword"}
        }
    }
}

def sync_access_management_data(**context) -> Dict[str, Any]:
    """
    Sync data from EXT_ACCESS_MANAGEMENT_ACTIVITY table to Elasticsearch.

    This function:
    1. Queries the EXT_ACCESS_MANAGEMENT_ACTIVITY table for last N hours (configurable)
    2. Transforms the data into Elasticsearch format
    3. Uses upsert operations to either create new entries or update existing ones
    4. Maintains state based on u_identifier
    5. Tracks metrics for completed requests and total requests
    """
    try:
        # Get worklog ID from XCom (created by create_worklog task)
        worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
        hook = WorkLogHook()

        if worklog_id:
            hook.set_worklog_id(worklog_id)
            hook.info(f"Using worklog ID: {worklog_id}")
        else:
            hook.warning("No worklog ID found in XCom, continuing without worklog tracking")

        # Initialize Prometheus metrics hook
        metrics_hook = PrometheusMetricsHook()

        # Get database connection
        oscr_connection_id = os.environ.get("OSCAR_DB_EXT_CONNECTION_ID", "oscar_db_ext")
        db_hook = AccessManagementSQLHook(connection_id=oscr_connection_id, worklog_id=worklog_id)

        # Query last N hours of activity data (configurable)
        query = f"""
            SELECT
                u_identifier,
                u_task_type,
                u_user_name,
                u_user_adid,
                u_user_email,
                u_mobile_no,
                u_env,
                u_status,
                u_organization,
                created_on,
                updated_on,
                created_by,
                u_svc_name,
                u_manager_adid,
                u_manager_name,
                u_manager_email,
                u_team_name,
                u_business_justification,
                u_ref_adid,
                u_description,
                u_role,
                u_platform,
                u_user_ntid_signum,
                u_svc_owner_email,
                u_modification_action,
                u_intended_use,
                u_svc_owner_adid,
                u_svc_owner_ntid_signum,
                u_new_password,
                u_remarks,
                update_by,
                u_sr_no,
                u_wo_no,
                u_comments
            FROM EXT_ACCESS_MANAGEMENT_ACTIVITY
            WHERE created_on >= DATE_SUB(NOW(), INTERVAL {SYNC_INTERVAL_HOURS} HOUR)
            ORDER BY created_on DESC
        """

        hook.info(f"Querying EXT_ACCESS_MANAGEMENT_ACTIVITY table for last {SYNC_INTERVAL_HOURS} hours")
        result = db_hook.get_records(query)

        if not result:
            hook.warning("No recent activity data found")
            # Set time-series metrics to 0 when no data
            dag_run_id = context['dag_run'].run_id if 'dag_run' in context else "unknown"
            metrics_hook.set_gauge("access_management_total_requests", 0, {
                "status": "all",
                "dag_run": dag_run_id
            })
            metrics_hook.set_gauge("access_management_completed_requests", 0, {
                "status": "completed",
                "dag_run": dag_run_id
            })
            metrics_hook.set_gauge("access_management_active_requests", 0, {
                "status": "active",
                "dag_run": dag_run_id
            })
            metrics_hook.set_gauge("access_management_new_records", 0, {
                "dag_run": dag_run_id
            })
            metrics_hook.set_gauge("access_management_updated_records", 0, {
                "dag_run": dag_run_id
            })
            metrics_hook.set_gauge("access_management_completion_rate_percent", 0, {
                "dag_run": dag_run_id
            })
            metrics_hook.set_gauge("access_management_active_rate_percent", 0, {
                "dag_run": dag_run_id
            })

            # Push metrics even when no data
            metrics_hook.push_metrics(job_name="access_management_dashboard_sync")

            return {"success": True, "records_processed": 0}

        # Initialize Elasticsearch hook (supports clusters and multiple connections)
        es_hook = ElasticsearchHook()

        # Check if index exists, create with mapping if not
        index_name = ELASTICSEARCH_INDEX_ACCESS_MANAGEMENT
        if not es_hook.check_index_exists(index_name):
            hook.info(f"Creating index '{index_name}' with optimized mapping")
            es_hook.create_index(index_name, mapping=ES_INDEX_MAPPING)
        else:
            hook.debug(f"Index '{index_name}' already exists")

        # Process each record
        records_processed = 0
        new_records = 0
        updated_records = 0
        completed_requests = 0
        active_requests = 0
        total_requests = 0
        data_list = []

        for row in result:
            try:
                # Extract data from row
                activity_data = {
                    "u_identifier": row[0],
                    "u_task_type": row[1],
                    "u_user_name": row[2],
                    "u_user_adid": row[3],
                    "u_user_email": row[4],
                    "u_mobile_no": row[5],
                    "u_env": row[6],
                    "u_status": row[7],
                    "u_organization": row[8],
                    "created_on": row[9].isoformat() if row[9] else None,
                    "updated_on": row[10].isoformat() if row[10] else None,
                    "created_by": row[11],
                    "u_svc_name": row[12],
                    "u_manager_adid": row[13],
                    "u_manager_name": row[14],
                    "u_manager_email": row[15],
                    "u_team_name": row[16],
                    "u_business_justification": row[17],
                    "u_ref_adid": row[18],
                    "u_description": row[19],
                    "u_role": row[20],
                    "u_platform": row[21],
                    "u_user_ntid_signum": row[22],
                    "u_svc_owner_email": row[23],
                    "u_modification_action": row[24],
                    "u_intended_use": row[25],
                    "u_svc_owner_adid": row[26],
                    "u_svc_owner_ntid_signum": row[27],
                    "u_new_password": row[28],
                    "u_remarks": row[29],
                    "update_by": row[30],
                    "u_sr_no": row[31],
                    "u_wo_no": row[32],
                    "u_comments": row[33],
                    "@timestamp": datetime.now(timezone.utc).isoformat(),
                    "type": "access_management_activity"
                }

                # Track metrics based on status
                total_requests += 1
                status_lower = activity_data['u_status'].lower() if activity_data['u_status'] else ''

                if status_lower == 'completed':
                    completed_requests += 1
                elif status_lower in ['sr submitted', 'pending approval', 'data inserted', 'pending']:
                    active_requests += 1
                # Other statuses are counted in total_requests but not categorized

                # Check if document already exists
                doc_id = str(activity_data['u_identifier'])
                if es_hook.check_document_exists(index_name, doc_id):
                    # Document exists - this will be an update
                    updated_records += 1
                    hook.debug(f"Updating existing document for u_identifier: {doc_id}")
                else:
                    # Document doesn't exist - this will be a new entry
                    new_records += 1
                    hook.debug(f"Creating new document for u_identifier: {doc_id}")

                # Add to data list for bulk upsert
                data_list.append(activity_data)
                records_processed += 1

            except Exception as e:
                hook.warning(f"Error processing activity record {row[0] if row else 'unknown'}: {str(e)}")
                continue

        # Bulk upsert to Elasticsearch
        if data_list:
            hook.info(f"Upserting {len(data_list)} records to Elasticsearch (new: {new_records}, updated: {updated_records})")

            try:
                bulk_result = es_hook.bulk_upsert_data(index_name, data_list, id_field="u_identifier")

                hook.info(f"Successfully upserted {bulk_result['success']} records to Elasticsearch")
                if bulk_result['failed'] > 0:
                    hook.warning(f"Failed to upsert {bulk_result['failed']} records to Elasticsearch")
                    for error in bulk_result['errors']:
                        hook.error(f"Error: {error}")

            except Exception as e:
                hook.error(f"Error during bulk upsert: {str(e)}")
                return {"success": False, "error": str(e)}

        # Send metrics to Prometheus
        try:
            # Time-series metrics for this run
            current_timestamp = datetime.now(timezone.utc)
            dag_run_id = context['dag_run'].run_id if 'dag_run' in context else "unknown"

            # Gauge metrics for current state (total counts as of this run)
            metrics_hook.set_gauge("access_management_total_requests", total_requests, {
                "status": "all",
                "dag_run": dag_run_id
            })
            metrics_hook.set_gauge("access_management_completed_requests", completed_requests, {
                "status": "completed",
                "dag_run": dag_run_id
            })
            metrics_hook.set_gauge("access_management_active_requests", active_requests, {
                "status": "active",
                "dag_run": dag_run_id
            })

            # Counter metrics for cumulative totals (these will accumulate over time)
            metrics_hook.increment_counter("access_management_requests_processed_total", total_requests, {
                "status": "all"
            })
            metrics_hook.increment_counter("access_management_requests_completed_total", completed_requests, {
                "status": "completed"
            })
            metrics_hook.increment_counter("access_management_requests_active_total", active_requests, {
                "status": "active"
            })

            # Gauge metrics for processing rates and performance
            if total_requests > 0:
                completion_rate = (completed_requests / total_requests) * 100
                active_rate = (active_requests / total_requests) * 100

                metrics_hook.set_gauge("access_management_completion_rate_percent", completion_rate, {
                    "dag_run": dag_run_id
                })
                metrics_hook.set_gauge("access_management_active_rate_percent", active_rate, {
                    "dag_run": dag_run_id
                })

            # Gauge metrics for processing statistics
            metrics_hook.set_gauge("access_management_new_records", new_records, {
                "dag_run": dag_run_id
            })
            metrics_hook.set_gauge("access_management_updated_records", updated_records, {
                "dag_run": dag_run_id
            })

            # Push all metrics to Prometheus
            success = metrics_hook.push_metrics(job_name="access_management_dashboard_sync")

            if success:
                hook.info(f"Sent time-series metrics - Total: {total_requests}, Completed: {completed_requests}, Active: {active_requests}, New: {new_records}, Updated: {updated_records}")
            else:
                hook.warning("Failed to push metrics to Prometheus")

        except Exception as e:
            hook.warning(f"Error sending metrics to Prometheus: {str(e)}")

        hook.info(f"Successfully processed {records_processed} activity records (new: {new_records}, updated: {updated_records})")
        return {
            "success": True,
            "records_processed": records_processed,
            "new_records": new_records,
            "updated_records": updated_records,
            "total_requests": total_requests,
            "completed_requests": completed_requests,
            "active_requests": active_requests
        }

    except Exception as e:
        logger.error(f"Error syncing activity data: {str(e)}")
        return {"success": False, "error": str(e)}

def create_worklog(**context) -> str:
    """
    Create a worklog for this sync operation.
    """
    hook = WorkLogHook()
    worklog = hook.create_worklog(
        name="Access Management Dashboard Sync",
        description=f"Sync data from EXT_ACCESS_MANAGEMENT_ACTIVITY to Elasticsearch for dashboard (upsert based on u_identifier, {SYNC_INTERVAL_HOURS} hours)"
    )
    worklog_id = worklog["id"]
    hook.info(f"Created worklog for dashboard sync: {worklog_id}")

    # Push worklog_id to XCom for other tasks to use
    context['ti'].xcom_push(key='worklog_id', value=worklog_id)

    return worklog_id

def close_worklog(**context) -> str:
    """
    Close the worklog for this sync operation.
    """
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    if worklog_id:
        hook = WorkLogHook()
        hook.set_worklog_id(worklog_id)
        hook.info("Closing worklog for dashboard sync")
        hook.close_worklog()
        return worklog_id
    else:
        logger.warning("No worklog ID found in XCom, skipping worklog closure")
        return "no_worklog"

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="access_management_dashboard_sync",
    default_args=default_args,
    description=f"Sync access management data to Elasticsearch for Kibana dashboard (upsert based on u_identifier, {SYNC_INTERVAL_HOURS} hours)",
    schedule=DAG_SCHEDULE_INTERVAL,  # Configurable schedule interval
    start_date=pendulum.today('UTC').add(days=-1),
    tags=["access", "management", "dashboard", "sync", "elasticsearch"],
    catchup=False,
) as dag:

    # Create worklog
    create_worklog_task = PythonOperator(
        task_id="create_worklog",
        python_callable=create_worklog,
    )

    # Sync activity data
    sync_activity_task = PythonOperator(
        task_id="sync_access_management_data",
        python_callable=sync_access_management_data,
    )

    # Close worklog
    close_worklog_task = PythonOperator(
        task_id="close_worklog",
        python_callable=close_worklog,
    )

    # Set up task dependencies
    create_worklog_task >> sync_activity_task >> close_worklog_task

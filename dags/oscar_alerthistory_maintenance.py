import os
from airflow import DAG
from airflow.decorators import task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta, timezone
import mysql.connector

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@task
def clean_alert_history():
    connection_id = os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db')
    days_to_keep = int(os.getenv('OSCAR_ALERT_HISTORY_DAYS_TO_KEEP', 7))
    batch_size = int(os.getenv('OSCAR_ALERT_HISTORY_BATCH_SIZE', 1000))

    conn = BaseHook.get_connection(connection_id)
    db = mysql.connector.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )
    cursor = db.cursor()

    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        # First loop - Delete alerts and their associated records
        max_iterations = int(os.getenv('OSCAR_ALERT_MAX_ITERATIONS', 100))
        iteration_count = 0
        total_alerts_deleted = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                print(f"Reached maximum iterations ({max_iterations}). Breaking loop after deleting {total_alerts_deleted} total Alerts.")
                break

            # Select Alert IDs to delete
            select_alert_ids_query = """
            SELECT ID FROM AM_Alert WHERE endsAt < %s LIMIT %s
            """
            cursor.execute(select_alert_ids_query, (cutoff_date_str, batch_size))
            alert_ids = cursor.fetchall()

            if not alert_ids:
                print(f"No more Alerts found after {iteration_count} iterations. Total alerts deleted: {total_alerts_deleted}")
                break

            # Flatten the list of IDs
            alert_ids = [row[0] for row in alert_ids]  # type: ignore

            # Delete associated AlertLabel records
            delete_alert_labels_query = """
            DELETE FROM AM_AlertLabel WHERE AlertID IN (%s)
            """ % ','.join(['%s'] * len(alert_ids))
            cursor.execute(delete_alert_labels_query, alert_ids)  # type: ignore
            alert_labels_deleted = cursor.rowcount

            # Delete associated AlertAnnotation records
            delete_alert_annotations_query = """
            DELETE FROM AM_AlertAnnotation WHERE AlertID IN (%s)
            """ % ','.join(['%s'] * len(alert_ids))
            cursor.execute(delete_alert_annotations_query, alert_ids)  # type: ignore
            alert_annotations_deleted = cursor.rowcount

            # Delete Alert records
            delete_alerts_query = """
            DELETE FROM AM_Alert WHERE ID IN (%s)
            """ % ','.join(['%s'] * len(alert_ids))
            cursor.execute(delete_alerts_query, alert_ids)  # type: ignore
            alerts_deleted = cursor.rowcount

            total_alerts_deleted += alerts_deleted

            if alerts_deleted == 0:
                print(f"No Alerts were deleted in this iteration. Breaking loop after {iteration_count} iterations.")
                break

            db.commit()

            print(f"Iteration {iteration_count}/{max_iterations}: Deleted {alerts_deleted} Alerts. Total deleted so far: {total_alerts_deleted}")

        # After deleting alerts, clean up empty alert groups and associated records
        max_iterations = int(os.getenv('OSCAR_ALERT_MAX_ITERATIONS', 100))
        iteration_count = 0
        total_groups_deleted = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                print(f"Reached maximum iterations ({max_iterations}). Breaking loop after deleting {total_groups_deleted} total AlertGroups.")
                break

            # Select empty AlertGroup IDs
            select_empty_alert_group_ids_query = """
            SELECT ID FROM AM_AlertGroup WHERE ID NOT IN (SELECT DISTINCT alertGroupID FROM AM_Alert)
            LIMIT %s
            """
            cursor.execute(select_empty_alert_group_ids_query, (batch_size,))
            alert_group_ids = cursor.fetchall()

            if not alert_group_ids:
                print(f"No more empty AlertGroups found after {iteration_count} iterations. Total groups deleted: {total_groups_deleted}")
                break

            # Flatten the list of IDs
            alert_group_ids = [row[0] for row in alert_group_ids]  # type: ignore

            # Delete associated commonLabel records
            delete_common_labels_query = """
            DELETE FROM AM_CommonLabel WHERE alertGroupID IN (%s)
            """ % ','.join(['%s'] * len(alert_group_ids))
            cursor.execute(delete_common_labels_query, alert_group_ids)  # type: ignore
            common_labels_deleted = cursor.rowcount

            # Delete associated groupLabels records
            delete_group_labels_query = """
            DELETE FROM AM_GroupLabel WHERE alertGroupID IN (%s)
            """ % ','.join(['%s'] * len(alert_group_ids))
            cursor.execute(delete_group_labels_query, alert_group_ids)  # type: ignore
            group_labels_deleted = cursor.rowcount

            # Delete associated commonAnnotations records
            delete_common_annotations_query = """
            DELETE FROM AM_CommonAnnotation WHERE alertGroupID IN (%s)
            """ % ','.join(['%s'] * len(alert_group_ids))
            cursor.execute(delete_common_annotations_query, alert_group_ids)  # type: ignore
            common_annotations_deleted = cursor.rowcount

            # Delete AlertGroup records
            delete_alert_groups_query = """
            DELETE FROM AM_AlertGroup WHERE ID IN (%s)
            """ % ','.join(['%s'] * len(alert_group_ids))
            cursor.execute(delete_alert_groups_query, alert_group_ids)  # type: ignore
            alert_groups_deleted = cursor.rowcount

            total_groups_deleted += alert_groups_deleted

            if alert_groups_deleted == 0:
                print(f"No AlertGroups were deleted in this iteration. Breaking loop after {iteration_count} iterations.")
                break

            print(f"Iteration {iteration_count}/{max_iterations}: Deleted {alert_groups_deleted} AlertGroups. Total deleted so far: {total_groups_deleted}")

    except Exception as e:
        db.rollback()
        print(f"An error occurred: {str(e)}")
        raise
    finally:
        cursor.close()
        db.close()


@task
def clean_alert_history_records():
    connection_id = os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db')
    days_to_keep = int(os.getenv('OSCAR_ALERT_HISTORY_DAYS_TO_KEEP', 30))  # Default to 30 days for history
    batch_size = int(os.getenv('OSCAR_ALERT_HISTORY_BATCH_SIZE', 1000))

    conn = BaseHook.get_connection(connection_id)
    db = mysql.connector.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )
    cursor = db.cursor()

    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        # Clean AlertHistory records in batches
        max_iterations = int(os.getenv('OSCAR_ALERT_HISTORY_MAX_ITERATIONS', 100))
        iteration_count = 0
        total_history_deleted = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                print(f"Reached maximum iterations ({max_iterations}). Breaking loop after deleting {total_history_deleted} total AlertHistory records.")
                break

            # Select AlertHistory IDs to delete
            # Use last_occurrence for cleanup to ensure we keep frequently occurring alerts
            # Fall back to created_at for older records without last_occurrence
            select_history_ids_query = """
            SELECT ID FROM AM_AlertHistory 
            WHERE (
                (last_occurrence IS NOT NULL AND last_occurrence < %s) 
                OR 
                (last_occurrence IS NULL AND created_at < %s)
            )
            AND (ticket_id IS NULL OR ticket_id = '')  -- Keep ticketed alerts longer
            AND (acknowledged = 0 OR acknowledged IS NULL)  -- Keep acknowledged alerts longer
            LIMIT %s
            """
            cursor.execute(select_history_ids_query, (cutoff_date_str, cutoff_date_str, batch_size))
            history_ids = cursor.fetchall()

            if not history_ids:
                print(f"No more AlertHistory records found after {iteration_count} iterations. Total records deleted: {total_history_deleted}")
                break

            # Flatten the list of IDs
            history_ids = [row[0] for row in history_ids]  # type: ignore

            # Delete associated AlertHistoryLabel records
            delete_history_labels_query = """
            DELETE FROM AM_AlertHistoryLabel WHERE AlertHistoryID IN (%s)
            """ % ','.join(['%s'] * len(history_ids))
            cursor.execute(delete_history_labels_query, history_ids)  # type: ignore
            history_labels_deleted = cursor.rowcount

            # Delete associated AlertHistoryAnnotation records
            delete_history_annotations_query = """
            DELETE FROM AM_AlertHistoryAnnotation WHERE AlertHistoryID IN (%s)
            """ % ','.join(['%s'] * len(history_ids))
            cursor.execute(delete_history_annotations_query, history_ids)  # type: ignore
            history_annotations_deleted = cursor.rowcount

            # Delete AlertHistory records
            delete_history_query = """
            DELETE FROM AM_AlertHistory WHERE ID IN (%s)
            """ % ','.join(['%s'] * len(history_ids))
            cursor.execute(delete_history_query, history_ids)  # type: ignore
            history_deleted = cursor.rowcount

            total_history_deleted += history_deleted

            if history_deleted == 0:
                print(f"No AlertHistory records were deleted in this iteration. Breaking loop after {iteration_count} iterations.")
                break

            db.commit()

            print(f"Iteration {iteration_count}/{max_iterations}: Deleted {history_deleted} AlertHistory records. Total deleted so far: {total_history_deleted}")

    except Exception as e:
        db.rollback()
        print(f"An error occurred: {str(e)}")
        raise
    finally:
        cursor.close()
        db.close()


with DAG(
    'clean_alert_history',
    default_args=default_args,
    description='Clean AlertHistory records',
    schedule='0 1 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Run both tasks
    clean_alert_history_task = clean_alert_history()
    clean_alert_history_records_task = clean_alert_history_records()

    # Set task dependencies (run alert history cleaning after alert cleaning)
    clean_alert_history_task >> clean_alert_history_records_task

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
def clean_task_history():
    connection_id = os.getenv('OSCAR_DB_CONNECTION_ID', 'oscar_db')
    days_to_keep = int(os.getenv('OSCAR_TASK_HISTORY_DAYS_TO_KEEP', 7))
    batch_size = int(os.getenv('OSCAR_TASK_HISTORY_BATCH_SIZE', 1000))
    max_iterations = int(os.getenv('OSCAR_TASK_HISTORY_MAX_ITERATIONS', 100))

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

        iteration_count = 0
        total_tasks_deleted = 0

        while True:
            iteration_count += 1
            if iteration_count > max_iterations:
                print(f"Reached maximum iterations ({max_iterations}). Breaking loop after deleting {total_tasks_deleted} total TaskHistory records.")
                break

            # Select TaskHistory IDs to delete
            select_task_history_ids_query = """
            SELECT id FROM TM_History WHERE created_at < %s LIMIT %s
            """
            cursor.execute(select_task_history_ids_query, (cutoff_date_str, batch_size))
            task_history_ids = cursor.fetchall()

            if not task_history_ids:
                print(f"No more TaskHistory records found after {iteration_count} iterations. Total records deleted: {total_tasks_deleted}")
                break

            # Flatten the list of IDs
            task_history_ids = [row[0] for row in task_history_ids]  # type: ignore

            # Delete associated TaskStageHistory records
            delete_stage_history_query = """
            DELETE FROM TM_StageHistory WHERE task_history_id IN (%s)
            """ % ','.join(['%s'] * len(task_history_ids))
            cursor.execute(delete_stage_history_query, task_history_ids)  # type: ignore
            stage_history_deleted = cursor.rowcount

            # Delete TaskHistory records
            delete_task_history_query = """
            DELETE FROM TM_History WHERE id IN (%s)
            """ % ','.join(['%s'] * len(task_history_ids))
            cursor.execute(delete_task_history_query, task_history_ids)  # type: ignore
            task_history_deleted = cursor.rowcount

            total_tasks_deleted += task_history_deleted

            if task_history_deleted == 0:
                print(f"No TaskHistory records were deleted in this iteration. Breaking loop after {iteration_count} iterations.")
                break

            db.commit()

            print(f"Iteration {iteration_count}/{max_iterations}: Deleted {task_history_deleted} TaskHistory records and {stage_history_deleted} associated TaskStageHistory records. Total deleted so far: {total_tasks_deleted}")

    except Exception as e:
        db.rollback()
        print(f"An error occurred: {str(e)}")
        raise
    finally:
        cursor.close()
        db.close()


with DAG(
    'clean_task_history',
    default_args=default_args,
    description='Clean TaskHistory records',
    schedule='0 1 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    clean_task_history()

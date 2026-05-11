from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
import logging
import uuid
import random
import time

# Import our custom hook
from hooks.worklog_hook import WorkLogHook, SeverityLevel, WorkLogType  # type: ignore

logger = logging.getLogger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Generate a unique task ID for this run
task_id = f"WORKLOG-TEST-{uuid.uuid4().hex[:8]}"


def create_worklog(**context):
    """Create a new worklog and add initial entries"""
    hook = WorkLogHook()

    # Create metadata for the worklog
    metadata = [
        {"key": "task_id", "value": task_id},
        {"key": "environment", "value": "development"},
        {"key": "initiated_by", "value": "airflow"},
        {"key": "test_type", "value": "refresh_testing"},
        {"key": "version", "value": "2.0"}
    ]

    # Create the worklog
    worklog = hook.create_worklog(
        name="Airflow WorkLog Test - Extended",
        description="Enhanced WorkLog test with continuous entries for refresh testing",
        worklog_type=WorkLogType.DB,
        metadata=metadata
    )

    logger.info(f"Created worklog with ID: {worklog['id']}")

    # Add initial batch of entries
    hook.info("Starting the enhanced worklog test workflow")
    time.sleep(1)
    hook.debug("Initializing workflow components")
    time.sleep(1)
    hook.info("Loading workflow configuration")
    time.sleep(1)
    hook.debug("Configuration validation in progress")
    time.sleep(1)
    hook.info("All systems ready for processing")

    # Store the worklog ID in XCom for later tasks
    context['ti'].xcom_push(key='worklog_id', value=worklog['id'])

    return worklog['id']


def process_data(**context):
    """Simulate data processing and add entries to the worklog"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    # Simulate some processing with progress updates
    hook.info("Starting data processing task")  # No need to pass worklog_id

    # Add initial batch of entries
    hook.debug("Initializing data processing pipeline")
    time.sleep(2)
    hook.info("Loading configuration from environment")
    time.sleep(1)
    hook.debug("Configuration loaded successfully")

    # Simulate processing steps with more entries
    total_steps = 10
    for step in range(1, total_steps + 1):
        # Add pre-step entries
        hook.debug(f"Preparing to execute step {step}")
        
        # Simulate varying work times
        work_time = random.uniform(2, 5)
        time.sleep(work_time)

        # Add a progress entry
        hook.info(f"Completed processing step {step}/{total_steps} (took {work_time:.1f}s)")

        # Add metadata entries for some steps
        if step % 3 == 0:
            hook.debug(f"Step {step} metrics: CPU: {random.randint(20, 80)}%, Memory: {random.randint(30, 70)}%")
            time.sleep(1)

        # Occasionally add a warning
        if random.random() < 0.4:
            hook.warning(f"Performance degradation detected in step {step}")
            time.sleep(1)
            hook.info(f"Applying optimization for step {step}")

        # Add substep entries for some steps
        if step % 2 == 0:
            for substep in range(1, 4):
                time.sleep(1)
                hook.debug(f"Step {step}.{substep}: Processing sub-batch {substep}")

    # Add more completion entries
    hook.info("All processing steps completed")
    time.sleep(2)
    hook.debug("Performing cleanup operations")
    time.sleep(1)
    hook.info("Data processing completed successfully")

    return worklog_id


def handle_errors(**context):
    """Simulate error handling and add error entries to the worklog"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    # Simulate an error condition
    hook.info("Starting error handling and validation phase")
    time.sleep(2)

    # Add some validation entries
    hook.debug("Validating data integrity")
    time.sleep(1)
    hook.info("Running data validation checks")
    time.sleep(2)

    # Simulate multiple error scenarios
    error_probability = random.random()
    
    if error_probability < 0.3:
        # Simulate connection error
        hook.warning("Network latency detected")
        time.sleep(1)
        error_message = "Simulated error: Connection timeout to external service"
        hook.error(error_message)
        logger.error(error_message)
        time.sleep(2)
        hook.info("Attempting to reconnect...")
        time.sleep(1)
        hook.info("Connection restored successfully")
    elif error_probability < 0.6:
        # Simulate data validation error
        hook.warning("Data inconsistency detected")
        time.sleep(1)
        hook.error("Validation failed: Missing required fields in dataset")
        time.sleep(2)
        hook.info("Applying data correction algorithms")
        time.sleep(3)
        hook.info("Data validation passed after corrections")
    else:
        # No errors scenario with additional checks
        hook.info("Running comprehensive system checks")
        time.sleep(2)
        hook.debug("Memory usage: Normal")
        time.sleep(1)
        hook.debug("CPU usage: Normal")
        time.sleep(1)
        hook.debug("Disk space: Adequate")
        time.sleep(1)
        hook.info("All system checks passed - No errors detected")

    # Add final validation entries
    time.sleep(2)
    hook.info("Error handling phase completed")

    return worklog_id


def close_worklog(**context):
    """Close the worklog and add final entries"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    # Add final summary entries
    hook.info("Beginning workflow finalization")
    time.sleep(2)
    
    # Add some summary statistics
    hook.debug("Generating workflow summary statistics")
    time.sleep(1)
    hook.info(f"Total execution time: {random.randint(120, 180)} seconds")
    time.sleep(1)
    hook.info(f"Tasks completed: {random.randint(25, 35)}")
    time.sleep(1)
    hook.info(f"Warnings encountered: {random.randint(2, 5)}")
    time.sleep(1)
    hook.info(f"Errors handled: {random.randint(0, 2)}")
    
    # Add cleanup entries
    time.sleep(2)
    hook.debug("Cleaning up temporary resources")
    time.sleep(1)
    hook.info("Releasing allocated memory")
    time.sleep(1)
    hook.info("Closing database connections")
    time.sleep(1)
    
    # Add final entry
    hook.info("All cleanup operations completed")
    time.sleep(2)
    hook.info("Workflow completed successfully, closing worklog")

    # Close the worklog
    closed_worklog = hook.close_worklog()

    logger.info(f"Closed worklog with ID: {closed_worklog['id']}")

    return closed_worklog['id']


def continuous_monitoring(**context):
    """Continuously add monitoring entries while other tasks run"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')

    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)

    # Simulate continuous monitoring for about 2 minutes
    hook.info("Starting continuous monitoring service")
    monitoring_duration = 120  # 2 minutes
    start_time = time.time()
    counter = 0

    while (time.time() - start_time) < monitoring_duration:
        counter += 1
        
        # Add various monitoring entries
        if counter % 5 == 0:
            # System metrics every 5 iterations
            cpu_usage = random.randint(15, 85)
            memory_usage = random.randint(20, 75)
            hook.debug(f"System metrics - CPU: {cpu_usage}%, Memory: {memory_usage}%")
        
        if counter % 10 == 0:
            # Network status every 10 iterations
            latency = random.randint(10, 150)
            hook.info(f"Network latency: {latency}ms")
        
        if counter % 15 == 0:
            # Health check every 15 iterations
            services = ['Database', 'Cache', 'Queue', 'API']
            service = random.choice(services)
            status = random.choice(['Healthy', 'Healthy', 'Healthy', 'Degraded'])
            if status == 'Degraded':
                hook.warning(f"{service} service showing {status} status")
            else:
                hook.debug(f"{service} service status: {status}")
        
        if counter % 20 == 0:
            # Progress update every 20 iterations
            elapsed = int(time.time() - start_time)
            remaining = monitoring_duration - elapsed
            hook.info(f"Monitoring progress: {elapsed}s elapsed, {remaining}s remaining")
        
        # Random events
        if random.random() < 0.05:  # 5% chance
            hook.warning("Spike in resource usage detected")
        
        if random.random() < 0.02:  # 2% chance
            hook.error("Temporary service interruption detected")
            time.sleep(2)
            hook.info("Service recovered automatically")
        
        # Wait before next iteration
        time.sleep(random.uniform(3, 6))

    hook.info("Continuous monitoring completed")
    return worklog_id


# Create the DAG
with DAG(
    'worklog_test',
    default_args=default_args,
    description='Enhanced Test DAG for WorkLog Hook with continuous monitoring',
    schedule=None,
    start_date=pendulum.today('UTC').add(days=-1),
    tags=['test', 'worklog', 'monitoring'],
) as dag:

    # Define the tasks
    task_create_worklog = PythonOperator(
        task_id='create_worklog',
        python_callable=create_worklog,
    )

    task_process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    task_handle_errors = PythonOperator(
        task_id='handle_errors',
        python_callable=handle_errors,
    )

    task_continuous_monitoring = PythonOperator(
        task_id='continuous_monitoring',
        python_callable=continuous_monitoring,
    )

    task_close_worklog = PythonOperator(
        task_id='close_worklog',
        python_callable=close_worklog,
        trigger_rule='none_failed_or_skipped',  # Run even if monitoring task is still running
    )

    # Define the task dependencies
    # Create worklog first
    task_create_worklog >> [task_process_data, task_continuous_monitoring]
    
    # Process data and handle errors sequentially
    task_process_data >> task_handle_errors
    
    # Close worklog after main processing is done
    task_handle_errors >> task_close_worklog

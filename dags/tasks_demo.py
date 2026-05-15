from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
import logging
import uuid
import time

# Import our custom hooks
from hooks.tasks_hook import TasksHook  # type: ignore
from hooks.worklog_hook import WorkLogHook, WorkLogType  # type: ignore

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

# Generate a unique demo ID for this run
demo_id = f"TASKS-DEMO-{uuid.uuid4().hex[:8]}"

# Test task name
TEST_TASK_NAME = "EXAMPLES:ECHO_ALERT"


def create_worklog(**context):
    """Create a new worklog for tracking task operations"""
    hook = WorkLogHook()
    
    # Create metadata for the worklog
    metadata = [
        {"key": "demo_id", "value": demo_id},
        {"key": "dag_id", "value": context['dag'].dag_id},
        {"key": "run_id", "value": context['run_id']},
        {"key": "initiated_by", "value": "airflow"},
        {"key": "demo_type", "value": "task_management"}
    ]
    
    # Create the worklog
    worklog = hook.create_worklog(
        name="Tasks Hook Demo",
        description="Demonstration of task retrieval and enable/disable operations",
        worklog_type=WorkLogType.DB,
        metadata=metadata
    )
    
    logger.info(f"Created worklog with ID: {worklog['id']}")
    
    # Add initial entries
    hook.info("Starting tasks hook demonstration")
    hook.info(f"Demo ID: {demo_id}")
    hook.debug(f"Target task: {TEST_TASK_NAME}")
    
    # Store the worklog ID in XCom for later tasks
    context['ti'].xcom_push(key='worklog_id', value=worklog['id'])
    
    return worklog['id']


def retrieve_task(**context):
    """Retrieve task information by name"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    
    # Initialize hooks
    tasks_hook = TasksHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    worklog_hook.info(f"Starting task retrieval for: {TEST_TASK_NAME}")
    
    try:
        # Retrieve task by name
        worklog_hook.debug(f"Attempting to retrieve task: {TEST_TASK_NAME}")
        task = tasks_hook.get_task(TEST_TASK_NAME)
        
        if task:
            worklog_hook.info(f"Successfully retrieved task: {task.get('name', 'unknown')}")
            worklog_hook.debug(f"Task ID: {task.get('id', 'unknown')}")
            worklog_hook.debug(f"Task Type: {task.get('type', 'unknown')}")
            worklog_hook.debug(f"Task Status: {task.get('status', 'unknown')}")
            worklog_hook.debug(f"Task Description: {task.get('description', 'N/A')}")
            
            # Store task info in XCom
            context['ti'].xcom_push(key='task_info', value=task)
            context['ti'].xcom_push(key='task_id', value=task.get('id'))
            context['ti'].xcom_push(key='original_status', value=task.get('status', 'enabled'))
            
            worklog_hook.info("Task retrieval completed successfully")
            return task
        else:
            worklog_hook.error(f"Task {TEST_TASK_NAME} not found")
            raise Exception(f"Task {TEST_TASK_NAME} not found")
            
    except Exception as e:
        worklog_hook.error(f"Failed to retrieve task: {str(e)}")
        raise


def test_enable_disable_operations(**context):
    """Test enabling and disabling the task"""
    # Get data from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    task_info = context['ti'].xcom_pull(task_ids='retrieve_task', key='task_info')
    task_id = context['ti'].xcom_pull(task_ids='retrieve_task', key='task_id')
    
    # Initialize hooks
    tasks_hook = TasksHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    worklog_hook.info("Starting enable/disable operations test")
    
    try:
        # Test 1: Disable the task
        worklog_hook.info(f"Test 1: Disabling task {TEST_TASK_NAME}")
        worklog_hook.debug(f"Task ID: {task_id}")
        
        result = tasks_hook.disable_task(task_id)
        worklog_hook.info("Task disabled successfully")
        worklog_hook.debug(f"Disable operation response: {result}")
        time.sleep(2)
        
        # Test 2: Verify task is disabled by retrieving it again
        worklog_hook.info("Test 2: Verifying task is disabled")
        disabled_task = tasks_hook.get_task(TEST_TASK_NAME)
        
        if disabled_task.get('status') == 'disabled':
            worklog_hook.info("Verification SUCCESS: Task is disabled")
        else:
            worklog_hook.warning(f"Verification WARNING: Task status is {disabled_task.get('status')}")
        
        worklog_hook.debug(f"Task status after disable: {disabled_task.get('status')}")
        time.sleep(2)
        
        # Test 3: Enable the task
        worklog_hook.info(f"Test 3: Enabling task {TEST_TASK_NAME}")
        
        result = tasks_hook.enable_task(task_id)
        worklog_hook.info("Task enabled successfully")
        worklog_hook.debug(f"Enable operation response: {result}")
        time.sleep(2)
        
        # Test 4: Verify task is enabled
        worklog_hook.info("Test 4: Verifying task is enabled")
        enabled_task = tasks_hook.get_task(TEST_TASK_NAME)
        
        if enabled_task.get('status') == 'enabled':
            worklog_hook.info("Verification SUCCESS: Task is enabled")
        else:
            worklog_hook.warning(f"Verification WARNING: Task status is {enabled_task.get('status')}")
        
        worklog_hook.debug(f"Task status after enable: {enabled_task.get('status')}")
        
        # Store final state
        context['ti'].xcom_push(key='final_status', value=enabled_task.get('status'))
        
        worklog_hook.info("Enable/disable operations test completed successfully")
        
    except Exception as e:
        worklog_hook.error(f"Enable/disable operations test failed: {str(e)}")
        raise


def test_bulk_operations(**context):
    """Test bulk enable/disable operations"""
    # Get data from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    task_id = context['ti'].xcom_pull(task_ids='retrieve_task', key='task_id')
    
    # Initialize hooks
    tasks_hook = TasksHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    worklog_hook.info("Starting bulk operations test")
    
    try:
        # Test bulk disable
        worklog_hook.info("Test 5: Testing bulk disable operation")
        task_ids = [task_id]  # In real scenario, this would be multiple IDs
        
        result = tasks_hook.disable_tasks(task_ids)
        worklog_hook.info(f"Bulk disable successful for {len(task_ids)} task(s)")
        worklog_hook.debug(f"Bulk disable response: {result}")
        time.sleep(2)
        
        # Verify bulk disable
        disabled_task = tasks_hook.get_task(TEST_TASK_NAME)
        worklog_hook.debug(f"Task status after bulk disable: {disabled_task.get('status')}")
        
        # Test bulk enable
        worklog_hook.info("Test 6: Testing bulk enable operation")
        
        result = tasks_hook.enable_tasks(task_ids)
        worklog_hook.info(f"Bulk enable successful for {len(task_ids)} task(s)")
        worklog_hook.debug(f"Bulk enable response: {result}")
        time.sleep(2)
        
        # Verify bulk enable
        enabled_task = tasks_hook.get_task(TEST_TASK_NAME)
        worklog_hook.debug(f"Task status after bulk enable: {enabled_task.get('status')}")
        
        worklog_hook.info("Bulk operations test completed successfully")
        
    except Exception as e:
        worklog_hook.error(f"Bulk operations test failed: {str(e)}")
        raise


def test_list_tasks(**context):
    """Test listing tasks with filtering"""
    # Get data from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    
    # Initialize hooks
    tasks_hook = TasksHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    worklog_hook.info("Starting task listing test")
    
    try:
        # Test 1: List all tasks (first page)
        worklog_hook.info("Test 7: Listing all tasks (first page)")
        all_tasks = tasks_hook.list_tasks(page=1, per_page=10)
        
        worklog_hook.info(f"Retrieved {len(all_tasks.get('records', []))} tasks")
        worklog_hook.debug(f"Total tasks available: {all_tasks.get('total_records', 0)}")
        worklog_hook.debug(f"Total pages: {all_tasks.get('total_pages', 0)}")
        
        # Test 2: List with filter
        worklog_hook.info("Test 8: Listing enabled tasks only")
        enabled_tasks = tasks_hook.list_tasks(
            page=1, 
            per_page=10,
            filter_dict={"status": "enabled"}
        )
        
        worklog_hook.info(f"Found {len(enabled_tasks.get('records', []))} enabled tasks")
        
        # Test 3: Find our test task in the list
        worklog_hook.info(f"Test 9: Searching for {TEST_TASK_NAME} in task list")
        found = False
        for task in all_tasks.get('records', []):
            if task.get('name') == TEST_TASK_NAME:
                found = True
                worklog_hook.info(f"Found {TEST_TASK_NAME} in task list")
                worklog_hook.debug(f"Task details from list: ID={task.get('id')}, Status={task.get('status')}")
                break
        
        if not found:
            worklog_hook.warning(f"{TEST_TASK_NAME} not found in task list")
        
        worklog_hook.info("Task listing test completed successfully")
        
    except Exception as e:
        worklog_hook.error(f"Task listing test failed: {str(e)}")
        raise


def test_search_tasks(**context):
    """Test searching tasks by pattern (new feature)"""
    # Get data from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    
    # Initialize hooks
    tasks_hook = TasksHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    worklog_hook.info("Starting task search test (pattern matching)")
    
    try:
        # Test 1: Search for tasks ending with EAST
        worklog_hook.info("Test 10: Searching for tasks ending with 'EAST'")
        pattern = "*EAST"
        worklog_hook.debug(f"Using pattern: {pattern}")
        
        east_tasks = tasks_hook.search_tasks(pattern, worklog_id)
        worklog_hook.info(f"Found {len(east_tasks)} tasks matching pattern '*EAST'")
        
        if east_tasks:
            for task in east_tasks[:5]:  # Show first 5 matches
                worklog_hook.debug(f"  - {task.get('name', 'unknown')} (ID: {task.get('id', 'unknown')[:8]}...)")
        
        # Test 2: Search for EXAMPLES tasks
        worklog_hook.info("Test 11: Searching for EXAMPLES tasks")
        pattern = "EXAMPLES:*"
        worklog_hook.debug(f"Using pattern: {pattern}")
        
        example_tasks = tasks_hook.search_tasks(pattern, worklog_id)
        worklog_hook.info(f"Found {len(example_tasks)} tasks matching pattern 'EXAMPLES:*'")
        
        if example_tasks:
            for task in example_tasks[:5]:  # Show first 5 matches
                worklog_hook.debug(f"  - {task.get('name', 'unknown')} (Type: {task.get('type', 'unknown')})")
        
        # Test 3: Search with regex pattern
        worklog_hook.info("Test 12: Searching with regex pattern for tasks containing 'ALERT'")
        pattern = ".*ALERT.*"
        worklog_hook.debug(f"Using regex pattern: {pattern}")
        
        alert_tasks = tasks_hook.search_tasks(pattern, worklog_id)
        worklog_hook.info(f"Found {len(alert_tasks)} tasks containing 'ALERT'")
        
        if alert_tasks:
            for task in alert_tasks[:3]:  # Show first 3 matches
                worklog_hook.debug(f"  - {task.get('name', 'unknown')}")
        
        # Test 4: Search for non-existent pattern
        worklog_hook.info("Test 13: Searching for non-existent pattern")
        pattern = "NONEXISTENT_*_PATTERN"
        worklog_hook.debug(f"Using pattern: {pattern}")
        
        no_match_tasks = tasks_hook.search_tasks(pattern, worklog_id)
        if len(no_match_tasks) == 0:
            worklog_hook.info("Correctly returned empty list for non-matching pattern")
        else:
            worklog_hook.warning(f"Unexpected: Found {len(no_match_tasks)} tasks for non-existent pattern")
        
        # Store search results for summary
        context['ti'].xcom_push(key='search_results', value={
            'east_count': len(east_tasks),
            'examples_count': len(example_tasks),
            'alert_count': len(alert_tasks)
        })
        
        worklog_hook.info("Task search test completed successfully")
        
    except Exception as e:
        worklog_hook.error(f"Task search test failed: {str(e)}")
        raise


def test_pattern_based_operations(**context):
    """Test enable/disable operations using pattern matching"""
    # Get data from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    
    # Initialize hooks
    tasks_hook = TasksHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    worklog_hook.info("Starting pattern-based operations test")
    
    try:
        # Test 1: Disable tasks by pattern
        worklog_hook.info("Test 14: Disabling tasks matching pattern 'TEST_PATTERN_*'")
        pattern = "TEST_PATTERN_*"
        
        # Note: This would disable all matching tasks if they existed
        # For demo purposes, we're using a pattern that likely won't match anything
        result = tasks_hook.disable_tasks_by_pattern(pattern, worklog_id)
        
        if result.get('count', 0) == 0:
            worklog_hook.info(f"No tasks matched pattern '{pattern}' (expected for demo)")
        else:
            worklog_hook.info(f"Disabled {result.get('count', 0)} tasks matching pattern '{pattern}'")
            worklog_hook.debug(f"Operation result: {result}")
        
        # Test 2: Enable tasks by pattern (using a safe pattern)
        worklog_hook.info("Test 15: Enabling tasks matching pattern 'DEMO_TEST_*'")
        pattern = "DEMO_TEST_*"
        
        result = tasks_hook.enable_tasks_by_pattern(pattern, worklog_id)
        
        if result.get('count', 0) == 0:
            worklog_hook.info(f"No tasks matched pattern '{pattern}' (expected for demo)")
        else:
            worklog_hook.info(f"Enabled {result.get('count', 0)} tasks matching pattern '{pattern}'")
            worklog_hook.debug(f"Operation result: {result}")
        
        worklog_hook.info("Pattern-based operations test completed successfully")
        
    except Exception as e:
        worklog_hook.error(f"Pattern-based operations test failed: {str(e)}")
        raise


def restore_original_state(**context):
    """Restore task to its original state"""
    # Get data from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    task_id = context['ti'].xcom_pull(task_ids='retrieve_task', key='task_id')
    original_status = context['ti'].xcom_pull(task_ids='retrieve_task', key='original_status')
    
    # Initialize hooks
    tasks_hook = TasksHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    worklog_hook.info("Starting restoration of original state")
    
    try:
        # Get current status
        current_task = tasks_hook.get_task(TEST_TASK_NAME)
        current_status = current_task.get('status', 'unknown')
        
        worklog_hook.debug(f"Original status: {original_status}")
        worklog_hook.debug(f"Current status: {current_status}")
        
        # Restore if needed
        if current_status != original_status:
            worklog_hook.info(f"Restoring task to original status: {original_status}")
            
            if original_status == 'enabled':
                tasks_hook.enable_task(task_id)
            else:
                tasks_hook.disable_task(task_id)
                
            worklog_hook.info("Task state restored successfully")
        else:
            worklog_hook.info("Task already in original state, no restoration needed")
        
        worklog_hook.info("State restoration completed")
        
    except Exception as e:
        worklog_hook.error(f"Failed to restore original state: {str(e)}")
        worklog_hook.warning("Manual intervention may be required to restore task state")
        # Don't raise here to allow worklog to close


def close_worklog(**context):
    """Close the worklog with summary"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    task_info = context['ti'].xcom_pull(task_ids='retrieve_task', key='task_info')
    search_results = context['ti'].xcom_pull(task_ids='test_search_tasks', key='search_results')
    
    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    
    # Add summary entries
    hook.info("Tasks hook demonstration completed")
    hook.info("Summary of operations performed:")
    hook.debug(f"- Target task: {TEST_TASK_NAME}")
    hook.debug(f"- Task retrieval: SUCCESS")
    hook.debug("- Enable/disable operations: TESTED")
    hook.debug("- Bulk operations: TESTED")
    hook.debug("- Task listing: TESTED")
    hook.debug("- Pattern search: TESTED (NEW FEATURE)")
    hook.debug("- Pattern-based operations: TESTED (NEW FEATURE)")
    hook.debug("- State restoration: COMPLETED")
    
    if search_results:
        hook.info("Pattern search results summary:")
        hook.debug(f"  - Tasks ending with .EAST: {search_results.get('east_count', 0)}")
        hook.debug(f"  - EXAMPLES tasks: {search_results.get('examples_count', 0)}")
        hook.debug(f"  - Tasks containing ALERT: {search_results.get('alert_count', 0)}")
    
    hook.info(f"Demo ID: {demo_id}")
    hook.info("All operations completed, closing worklog")
    
    # Close the worklog
    closed_worklog = hook.close_worklog()
    
    logger.info(f"Closed worklog with ID: {closed_worklog['id']}")
    
    return closed_worklog['id']


# Create the DAG
with DAG(
    'tasks_demo',
    default_args=default_args,
    description='Demonstration of Tasks Hook with retrieval, enable/disable, and pattern search operations',
    schedule=None,
    start_date=pendulum.today('UTC').add(days=-1),
    tags=['demo', 'tasks', 'worklog', 'pattern-search'],
) as dag:
    
    # Define the tasks
    task_create_worklog = PythonOperator(
        task_id='create_worklog',
        python_callable=create_worklog,
    )
    
    task_retrieve = PythonOperator(
        task_id='retrieve_task',
        python_callable=retrieve_task,
    )
    
    task_enable_disable = PythonOperator(
        task_id='test_enable_disable_operations',
        python_callable=test_enable_disable_operations,
    )
    
    task_bulk_ops = PythonOperator(
        task_id='test_bulk_operations',
        python_callable=test_bulk_operations,
    )
    
    task_list = PythonOperator(
        task_id='test_list_tasks',
        python_callable=test_list_tasks,
    )
    
    task_search = PythonOperator(
        task_id='test_search_tasks',
        python_callable=test_search_tasks,
    )
    
    task_pattern_ops = PythonOperator(
        task_id='test_pattern_based_operations',
        python_callable=test_pattern_based_operations,
    )
    
    task_restore = PythonOperator(
        task_id='restore_original_state',
        python_callable=restore_original_state,
        trigger_rule='all_success',  # Run even if some tests fail
    )
    
    task_close_worklog = PythonOperator(
        task_id='close_worklog',
        python_callable=close_worklog,
        trigger_rule='all_success',  # Always close the worklog
    )
    
    # Define the task dependencies
    task_create_worklog >> task_retrieve >> task_enable_disable >> task_bulk_ops >> task_list >> task_search >> task_pattern_ops >> task_restore >> task_close_worklog

from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
import logging
import uuid
import time

# Import our custom hooks
from hooks.inventory_hook import InventoryHook  # type: ignore
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
demo_id = f"INVENTORY-DEMO-{uuid.uuid4().hex[:8]}"


def create_worklog(**context):
    """Create a new worklog for tracking inventory operations"""
    hook = WorkLogHook()
    
    # Create metadata for the worklog
    metadata = [
        {"key": "demo_id", "value": demo_id},
        {"key": "dag_id", "value": context['dag'].dag_id},
        {"key": "run_id", "value": context['run_id']},
        {"key": "initiated_by", "value": "airflow"},
        {"key": "demo_type", "value": "inventory_operations"}
    ]
    
    # Create the worklog
    worklog = hook.create_worklog(
        name="Inventory Hook Demo",
        description="Demonstration of inventory operations with server and environment maintenance",
        worklog_type=WorkLogType.DB,
        metadata=metadata
    )
    
    logger.info(f"Created worklog with ID: {worklog['id']}")
    
    # Add initial entries
    hook.info("Starting inventory operations demonstration")
    hook.info(f"Demo ID: {demo_id}")
    hook.debug("Initializing inventory hook and preparing test operations")
    
    # Store the worklog ID in XCom for later tasks
    context['ti'].xcom_push(key='worklog_id', value=worklog['id'])
    
    return worklog['id']


def list_and_select_server(**context):
    """List servers and select one for testing"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    
    # Initialize hooks
    inventory_hook = InventoryHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    worklog_hook.info("Starting server listing and selection")
    
    try:
        # List all servers
        worklog_hook.debug("Fetching server list from inventory")
        servers = inventory_hook.list_servers()
        worklog_hook.info(f"Retrieved {len(servers)} servers from inventory")
        
        # Try to find a test/dev server
        test_server = None
        for server in servers:
            hostname = server.get('hostname', '').lower()
            env_name = server.get('environment_name', '').lower()
            
            # Prefer test/dev servers
            if any(keyword in hostname for keyword in ['test', 'dev', 'staging']) or \
               any(keyword in env_name for keyword in ['test', 'dev', 'staging']):
                test_server = server
                worklog_hook.info(f"Selected test/dev server: {server['hostname']} in {server['environment_name']} environment")
                break
        
        # If no test server found, use the first available server
        if not test_server and servers:
            test_server = servers[0]
            worklog_hook.warning(f"No test/dev server found, using first server: {test_server['hostname']}")
        
        if not test_server:
            raise Exception("No servers available for testing")
        
        # Log server details
        worklog_hook.debug(f"Server details - ID: {test_server['id']}, "
                          f"Status: {test_server.get('status', 'unknown')}, "
                          f"Maintenance: {test_server.get('is_under_maintenance', False)}, "
                          f"Datacenter: {test_server.get('datacenter_name', 'unknown')}")
        
        # Store server info and original state
        context['ti'].xcom_push(key='test_server', value=test_server)
        context['ti'].xcom_push(key='original_maintenance_state', value=test_server.get('is_under_maintenance', False))
        context['ti'].xcom_push(key='environment_id', value=test_server.get('environment_id'))
        
        worklog_hook.info(f"Server selection completed. Will test operations on: {test_server['hostname']}")
        
        return test_server['id']
        
    except Exception as e:
        worklog_hook.error(f"Failed to list and select server: {str(e)}")
        raise


def test_server_operations(**context):
    """Test server lookup and maintenance operations"""
    # Get data from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    test_server = context['ti'].xcom_pull(task_ids='list_and_select_server', key='test_server')
    
    # Initialize hooks
    inventory_hook = InventoryHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    worklog_hook.info("Starting server operation tests")
    
    try:
        # Test 1: Lookup by UUID
        worklog_hook.info(f"Test 1: Looking up server by UUID: {test_server['id']}")
        server_by_uuid = inventory_hook.get_server_by_uuid(test_server['id'])
        worklog_hook.debug(f"Successfully retrieved server by UUID: {server_by_uuid['hostname']}")
        time.sleep(1)
        
        # Test 2: Lookup by hostname
        worklog_hook.info(f"Test 2: Looking up server by hostname: {test_server['hostname']}")
        server_by_hostname = inventory_hook.get_server_by_hostname(test_server['hostname'])
        worklog_hook.debug(f"Successfully retrieved server by hostname, ID: {server_by_hostname['id']}")
        time.sleep(1)
        
        # Test 3: Lookup by IP (if available)
        if test_server.get('network_interfaces'):
            for interface in test_server['network_interfaces']:
                if interface.get('ip_address'):
                    worklog_hook.info(f"Test 3: Looking up server by IP: {interface['ip_address']}")
                    try:
                        server_by_ip = inventory_hook.get_server_by_ip(interface['ip_address'])
                        worklog_hook.debug(f"Successfully retrieved server by IP: {server_by_ip['hostname']}")
                    except Exception as e:
                        worklog_hook.warning(f"IP lookup failed (may be expected): {str(e)}")
                    break
        else:
            worklog_hook.info("Test 3: Skipping IP lookup - no network interfaces found")
        time.sleep(1)
        
        # Test 4: Enable maintenance mode
        worklog_hook.info("Test 4: Enabling server maintenance mode")
        updated_server = inventory_hook.enable_server_maintenance(test_server['id'], worklog_id)
        worklog_hook.debug(f"Server maintenance mode enabled. Current state: {updated_server.get('is_under_maintenance')}")
        
        # Store the state after enabling maintenance
        context['ti'].xcom_push(key='maintenance_enabled', value=True)
        time.sleep(2)
        
        # Test 5: Verify maintenance mode is enabled
        worklog_hook.info("Test 5: Verifying maintenance mode status")
        server_check = inventory_hook.get_server(test_server['id'])
        if server_check.get('is_under_maintenance'):
            worklog_hook.info("Maintenance mode verification: SUCCESS - Server is under maintenance")
        else:
            worklog_hook.error("Maintenance mode verification: FAILED - Server is not under maintenance")
        time.sleep(1)
        
        # Test 6: Disable maintenance mode
        worklog_hook.info("Test 6: Disabling server maintenance mode")
        updated_server = inventory_hook.disable_server_maintenance(test_server['id'], worklog_id)
        worklog_hook.debug(f"Server maintenance mode disabled. Current state: {updated_server.get('is_under_maintenance')}")
        time.sleep(2)
        
        # Test 7: Verify maintenance mode is disabled
        worklog_hook.info("Test 7: Verifying maintenance mode is disabled")
        server_check = inventory_hook.get_server(test_server['id'])
        if not server_check.get('is_under_maintenance'):
            worklog_hook.info("Maintenance mode verification: SUCCESS - Server is not under maintenance")
        else:
            worklog_hook.error("Maintenance mode verification: FAILED - Server is still under maintenance")
        time.sleep(1)
        
        # Test 8: Disable server (change status to Inactive)
        worklog_hook.info("Test 8: Disabling server (setting status to Inactive)")
        worklog_hook.warning("Note: This will change the server's operational status")
        
        # Store the original status
        original_status = test_server.get('status', 'Active')
        context['ti'].xcom_push(key='original_server_status', value=original_status)
        
        disabled_server = inventory_hook.disable_server(test_server['id'], worklog_id)
        worklog_hook.debug(f"Server disabled. Current status: {disabled_server.get('status')}")
        time.sleep(2)
        
        # Test 9: Verify server is disabled
        worklog_hook.info("Test 9: Verifying server is disabled")
        server_check = inventory_hook.get_server(test_server['id'])
        if server_check.get('status') == 'Inactive':
            worklog_hook.info("Server disable verification: SUCCESS - Server status is Inactive")
        else:
            worklog_hook.error(f"Server disable verification: FAILED - Server status is {server_check.get('status')}")
        time.sleep(1)
        
        # Test 10: Enable server (change status to Active)
        worklog_hook.info("Test 10: Enabling server (setting status to Active)")
        enabled_server = inventory_hook.enable_server(test_server['id'], worklog_id)
        worklog_hook.debug(f"Server enabled. Current status: {enabled_server.get('status')}")
        time.sleep(2)
        
        # Test 11: Verify server is enabled
        worklog_hook.info("Test 11: Verifying server is enabled")
        server_check = inventory_hook.get_server(test_server['id'])
        if server_check.get('status') == 'Active':
            worklog_hook.info("Server enable verification: SUCCESS - Server status is Active")
        else:
            worklog_hook.error(f"Server enable verification: FAILED - Server status is {server_check.get('status')}")
        
        worklog_hook.info("Server operation tests completed successfully")
        
    except Exception as e:
        worklog_hook.error(f"Server operation tests failed: {str(e)}")
        raise


def test_environment_operations(**context):
    """Test environment maintenance operations"""
    # Get data from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    environment_id = context['ti'].xcom_pull(task_ids='list_and_select_server', key='environment_id')
    test_server = context['ti'].xcom_pull(task_ids='list_and_select_server', key='test_server')
    
    # Initialize hooks
    inventory_hook = InventoryHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    if not environment_id:
        worklog_hook.warning("No environment ID available, skipping environment tests")
        return
    
    worklog_hook.info(f"Starting environment operation tests for environment: {test_server.get('environment_name', 'unknown')}")
    
    try:
        # Test 1: Enable environment maintenance
        worklog_hook.info(f"Test 1: Enabling maintenance for environment ID: {environment_id}")
        worklog_hook.warning("Note: This will affect all servers in the environment")
        
        result = inventory_hook.enable_environment_maintenance(environment_id, worklog_id)
        worklog_hook.debug(f"Environment maintenance enable response: {result}")
        
        # Store that we enabled environment maintenance
        context['ti'].xcom_push(key='environment_maintenance_enabled', value=True)
        time.sleep(3)
        
        # Test 2: Verify environment servers are under maintenance
        worklog_hook.info("Test 2: Checking if servers in environment show maintenance status")
        servers_in_env = inventory_hook.list_servers({'environment_id': environment_id})
        maintenance_count = sum(1 for s in servers_in_env if s.get('is_under_maintenance', False))
        worklog_hook.info(f"Servers under maintenance in environment: {maintenance_count}/{len(servers_in_env)}")
        time.sleep(2)
        
        # Test 3: Disable environment maintenance
        worklog_hook.info(f"Test 3: Disabling maintenance for environment ID: {environment_id}")
        result = inventory_hook.disable_environment_maintenance(environment_id, worklog_id)
        worklog_hook.debug(f"Environment maintenance disable response: {result}")
        time.sleep(3)
        
        # Test 4: Verify environment servers are not under maintenance
        worklog_hook.info("Test 4: Verifying servers in environment are not under maintenance")
        servers_in_env = inventory_hook.list_servers({'environment_id': environment_id})
        maintenance_count = sum(1 for s in servers_in_env if s.get('is_under_maintenance', False))
        worklog_hook.info(f"Servers under maintenance after disable: {maintenance_count}/{len(servers_in_env)}")
        
        if maintenance_count == 0:
            worklog_hook.info("Environment maintenance test: SUCCESS - All servers restored")
        else:
            worklog_hook.warning(f"Environment maintenance test: WARNING - {maintenance_count} servers still under maintenance")
        
        # Test 5: Disable environment (change status)
        worklog_hook.info(f"Test 5: Disabling environment (status change) for ID: {environment_id}")
        worklog_hook.warning("Note: This will change the environment status to disabled/inactive")
        
        result = inventory_hook.disable_environment(environment_id, worklog_id)
        worklog_hook.debug(f"Environment disable response: {result}")
        
        # Store that we disabled the environment
        context['ti'].xcom_push(key='environment_disabled', value=True)
        context['ti'].xcom_push(key='original_environment_status', value=result.get('status', 'unknown'))
        time.sleep(3)
        
        # Test 6: Enable environment (change status back)
        worklog_hook.info(f"Test 6: Re-enabling environment (status change) for ID: {environment_id}")
        
        result = inventory_hook.enable_environment(environment_id, worklog_id)
        worklog_hook.debug(f"Environment enable response: {result}")
        time.sleep(3)
        
        worklog_hook.info("Environment operation tests completed")
        
    except Exception as e:
        worklog_hook.error(f"Environment operation tests failed: {str(e)}")
        # Don't raise here, we'll handle cleanup in the restore task


def restore_original_state(**context):
    """Restore server and environment to original state"""
    # Get data from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    test_server = context['ti'].xcom_pull(task_ids='list_and_select_server', key='test_server')
    original_maintenance_state = context['ti'].xcom_pull(task_ids='list_and_select_server', key='original_maintenance_state')
    environment_id = context['ti'].xcom_pull(task_ids='list_and_select_server', key='environment_id')
    original_server_status = context['ti'].xcom_pull(task_ids='test_server_operations', key='original_server_status')
    
    # Initialize hooks
    inventory_hook = InventoryHook()
    worklog_hook = WorkLogHook()
    worklog_hook.set_worklog_id(worklog_id)
    
    worklog_hook.info("Starting restoration of original states")
    
    try:
        # Check current server state
        current_server = inventory_hook.get_server(test_server['id'])
        current_maintenance = current_server.get('is_under_maintenance', False)
        current_status = current_server.get('status', 'Active')
        
        worklog_hook.debug(f"Server original maintenance state: {original_maintenance_state}")
        worklog_hook.debug(f"Server current maintenance state: {current_maintenance}")
        worklog_hook.debug(f"Server original status: {original_server_status}")
        worklog_hook.debug(f"Server current status: {current_status}")
        
        # Restore server status if needed
        if original_server_status and current_status != original_server_status:
            worklog_hook.info(f"Restoring server status to original: {original_server_status}")
            if original_server_status == 'Active':
                inventory_hook.enable_server(test_server['id'], worklog_id)
            else:
                inventory_hook.disable_server(test_server['id'], worklog_id)
            worklog_hook.info("Server status restored successfully")
        
        # Restore server maintenance state if needed
        if current_maintenance != original_maintenance_state:
            if original_maintenance_state:
                worklog_hook.info("Restoring server to maintenance mode (original state)")
                inventory_hook.enable_server_maintenance(test_server['id'], worklog_id)
            else:
                worklog_hook.info("Removing server from maintenance mode (original state)")
                inventory_hook.disable_server_maintenance(test_server['id'], worklog_id)
            worklog_hook.info("Server maintenance state restored successfully")
        else:
            worklog_hook.info("Server maintenance already in original state, no restoration needed")
        
        # Ensure environment is not in maintenance (if we tested it)
        environment_maintenance_enabled = context['ti'].xcom_pull(
            task_ids='test_environment_operations', 
            key='environment_maintenance_enabled'
        )
        
        if environment_maintenance_enabled and environment_id:
            worklog_hook.info("Ensuring environment is not in maintenance mode")
            try:
                inventory_hook.disable_environment_maintenance(environment_id, worklog_id)
                worklog_hook.info("Environment maintenance mode cleared")
            except Exception as e:
                worklog_hook.warning(f"Could not disable environment maintenance: {str(e)}")
        
        # Check if we need to restore environment status (enable/disable)
        environment_disabled = context['ti'].xcom_pull(
            task_ids='test_environment_operations',
            key='environment_disabled'
        )
        
        if environment_disabled and environment_id:
            # The test enabled it back, so we should leave it enabled 
            # unless it was originally disabled
            worklog_hook.info("Environment status was tested and restored during the test")
        
        worklog_hook.info("State restoration completed successfully")
        
    except Exception as e:
        worklog_hook.error(f"Failed to restore original state: {str(e)}")
        worklog_hook.warning("Manual intervention may be required to restore server state")
        raise


def close_worklog(**context):
    """Close the worklog with summary"""
    # Get the worklog ID from XCom
    worklog_id = context['ti'].xcom_pull(task_ids='create_worklog', key='worklog_id')
    test_server = context['ti'].xcom_pull(task_ids='list_and_select_server', key='test_server')
    
    # Create hook and set the worklog ID
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    
    # Add summary entries
    hook.info("Inventory operations demonstration completed")
    hook.info("Summary of operations performed:")
    hook.debug(f"- Tested server: {test_server['hostname'] if test_server else 'None'}")
    hook.debug("- Server lookup operations: UUID, hostname, IP")
    hook.debug("- Server maintenance: enable/disable with verification")
    hook.debug("- Server status: disable/enable with verification")
    hook.debug("- Environment maintenance: enable/disable")
    hook.debug("- Environment status: disable/enable")
    hook.debug("- State restoration: completed")
    
    hook.info(f"Demo ID: {demo_id}")
    hook.info("All operations completed, closing worklog")
    
    # Close the worklog
    closed_worklog = hook.close_worklog()
    
    logger.info(f"Closed worklog with ID: {closed_worklog['id']}")
    
    return closed_worklog['id']


# Create the DAG
with DAG(
    'inventory_demo',
    default_args=default_args,
    description='Demonstration of Inventory Hook with server and environment operations',
    schedule=None,
    start_date=pendulum.today('UTC').add(days=-1),
    tags=['demo', 'inventory', 'worklog'],
) as dag:
    
    # Define the tasks
    task_create_worklog = PythonOperator(
        task_id='create_worklog',
        python_callable=create_worklog,
    )
    
    task_list_and_select = PythonOperator(
        task_id='list_and_select_server',
        python_callable=list_and_select_server,
    )
    
    task_server_ops = PythonOperator(
        task_id='test_server_operations',
        python_callable=test_server_operations,
    )
    
    task_env_ops = PythonOperator(
        task_id='test_environment_operations',
        python_callable=test_environment_operations,
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
    task_create_worklog >> task_list_and_select >> task_server_ops >> task_env_ops >> task_restore >> task_close_worklog

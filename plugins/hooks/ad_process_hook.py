import json
import logging
import time
from typing import Dict, Any
from fabric import Connection
from airflow.hooks.base import BaseHook
from invoke import Responder

logger = logging.getLogger(__name__)

class AD_ProcessHook(BaseHook):
    """
    Hook for executing AD-related processes on remote Windows machines.
    Handles input file creation, script execution, and output file management.

    Example usage:
    ------------
    # Input command example:
    # echo {"inputvalues": [{"USER_ADID" : "esshar01"}],"action_name" : "RESET_PASSWORD - Check Account Disabled"} > "C:\path\to\input.json"

    # Script command example:
    # "C:\path\to\check_user_disable.bat"

    # Expected output file content:
    # {
    #     "PYTHON_STATUS": "Finished",
    #     "AD_OUTPUT": "User Account is not disabled"
    # }
    """

    def __init__(
        self,
        ssh_host: str,  # e.g., 'PLPTOOSDADJMP01'
        ssh_user: str,  # e.g., 'm2m_enable_auto'
        ssh_password: str = None,  # SSH password for Windows machine
        ssh_port: int = 22,
        input_command: str = None,  # Command to create input file
        script_cmd: str = None,  # Command to execute batch script
        output_file: str = None,  # Local output file path
        target_scp_file_path: str = None,  # Target path for SCP transfer
        scp_target: str = None,  # SCP target host
        scp_password: str = None,  # SCP password
        worklog_id: str = None  # Worklog ID for logging
    ):
        super().__init__()
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_password = ssh_password
        self.ssh_port = ssh_port
        self.input_command = input_command
        self.script_cmd = script_cmd
        self.output_file = output_file
        self.target_scp_file_path = target_scp_file_path
        self.scp_target = scp_target
        self.scp_password = scp_password
        self.worklog_id = worklog_id
        self.conn = None

    def connect(self):
        """Establish SSH connection to the Windows machine"""
        self.conn = Connection(
            host=self.ssh_host,
            user=self.ssh_user,
            port=self.ssh_port,
            connect_kwargs={"password": self.ssh_password}
        )

    def execute_process(self) -> Dict[str, Any]:
        """
        Execute the complete AD process flow:
        1. Create input file
        2. Execute script
        3. Read output file
        4. Copy output file to remote location
        5. Clean up

        Returns:
        --------
        {
            'success': bool,  # True if process completed successfully
            'data': dict,     # Parsed JSON output from the script
            'error': str      # Error message if any
        }

        Example successful output:
        {
            'success': True,
            'data': {
                'PYTHON_STATUS': 'Finished',
                'AD_OUTPUT': 'User Account is not disabled'
            },
            'error': None
        }

        Example error output:
        {
            'success': False,
            'data': {
                'PYTHON_STATUS': 'Finished with error',
                'AD_OUTPUT': 'User account not found'
            },
            'error': 'User account not found'
        }
        """
        result = {'success': False, 'data': None, 'error': None}

        try:
            self.connect()

            # Step 1: Run input generation command
            if self.input_command:
                logger.info(f"[Worklog: {self.worklog_id}] Creating input file with command: {self.input_command}")
                self.conn.run(self.input_command, hide=True)
                time.sleep(20)  # Wait 20 seconds after input file creation

            # Step 2: Run the batch script
            logger.info(f"[Worklog: {self.worklog_id}] Executing script: {self.script_cmd}")
            self.conn.run(self.script_cmd, hide=False)
            time.sleep(20)  # Wait 20 seconds after script execution

            # Step 3: Read output JSON content
            logger.info(f"[Worklog: {self.worklog_id}] Reading output file: {self.output_file}")

            # Retry mechanism to check for Python status
            max_retries = 6
            retry_count = 0
            while retry_count < max_retries:
                output_result = self.conn.run(f'more "{self.output_file}"', hide=True)
                output = output_result.stdout.strip()

                try:
                    json_lines = [line for line in output.splitlines() if line.strip().startswith("{")]
                    parsed = json.loads(json_lines[0]) if json_lines else {}

                    # Copy output file if SCP is configured - do this in every iteration
                    if self.scp_target:
                        logger.info(f"[Worklog: {self.worklog_id}] Copying output file to: {self.target_scp_file_path}")

                        try:
                            if self.scp_password:
                                # Use a single responder that matches any line containing Password or password
                                password_responder = Responder(
                                    pattern=r".*[Pp]assword.*:",
                                    response=f"{self.scp_password}\n",
                                )

                                self.conn.run(
                                    f'scp -o StrictHostKeyChecking=no "{self.output_file}" {self.target_scp_file_path}',
                                    pty=True,
                                    watchers=[password_responder],
                                    hide=True
                                )
                            else:
                                self.conn.run(
                                    f'scp -o StrictHostKeyChecking=no "{self.output_file}" {self.target_scp_file_path}',
                                    hide=True
                                )

                            logger.info(f"[Worklog: {self.worklog_id}] SCP transfer completed successfully")
                        except Exception as e:
                            logger.error(f"[Worklog: {self.worklog_id}] SCP transfer failed: {str(e)}")
                            # Don't return here, just log the error and continue

                        time.sleep(20)  # Wait 20 seconds after SCP transfer

                    # Check if we got a valid Python status
                    if parsed.get('PYTHON_STATUS') in ['Finished', 'Finished with error']:
                        result['data'] = parsed
                        if parsed.get('PYTHON_STATUS') == 'Finished':
                            result['success'] = True
                        else:
                            result['success'] = False
                            result['error'] = parsed.get('AD_OUTPUT', 'Unknown error')
                        break
                    elif parsed:
                        result['data'] = parsed

                    # If we haven't got a valid status yet, wait and retry
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.info(f"[Worklog: {self.worklog_id}] Waiting for Python status... Attempt {retry_count}/{max_retries}")
                        time.sleep(20)  # Wait 20 seconds between retries

                except json.JSONDecodeError as e:
                    logger.error(f"[Worklog: {self.worklog_id}] Failed to parse output JSON: {e}")
                    result['error'] = f"JSON parsing error: {str(e)}"
                    return result
                except Exception as e:
                    logger.error(f"[Worklog: {self.worklog_id}] Failed to receive output file content as JSON: {e}")
                    result['error'] = f"Command error on remote machine or unable to fetch command output from remote machine: {str(e)}"
                    return result

            # If we've exhausted all retries without getting a valid status
            if retry_count == max_retries:
                result['success'] = False
                result['error'] = "Timeout waiting for Python status after 6 attempts"

                logger.error(f"[Worklog: {self.worklog_id}] {result['error']}")

            # Step 5: Cleanup output file
            logger.info(f"[Worklog: {self.worklog_id}] Cleaning up - deleting output file: {self.output_file}")
            self.conn.run(f'del "{self.output_file}"', hide=True)
            time.sleep(20)  # Wait 20 seconds after cleanup

        except Exception as e:
            logger.error(f"[Worklog: {self.worklog_id}] Error in AD process execution: {str(e)}")
            result['error'] = str(e)
        finally:
            if self.conn:
                self.conn.close()

        return result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

from typing import Dict, Any, Optional, Tuple, List
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook
from hooks.worklog_hook import WorkLogHook

logger = logging.getLogger(__name__)

class AccessManagementSQLHook:
    """
    Custom hook for handling access management database operations
    """

    def __init__(self, connection_id: str, worklog_id: str):
        """
        Initialize the hook with a connection ID and worklog ID

        Args:
            connection_id (str): The Airflow connection ID to use
            worklog_id (str): The worklog ID to use for logging
        """
        self.connection_id = connection_id
        self.worklog_id = worklog_id
        self.hook = MySqlHook(mysql_conn_id=connection_id)
        self.worklog_hook = WorkLogHook()
        self.worklog_hook.set_worklog_id(worklog_id)

    def get_info_by_adid(self, adid: str) -> Tuple[str, str]:
        """
        Get user information from EXT_AM_ITSM_USER_LIST table by ADID

        Args:
            adid (str): The ADID to look up

        Returns:
            Tuple[str, str]: (full_name, email) of the user
        """
        try:
            query = f"""
                SELECT u_full_name, u_email
                FROM EXT_AM_ITSM_USER_LIST
                WHERE u_login_id = '{adid}'
            """
            logger.info(f"Fetching user info for ADID: {adid}")
            self.worklog_hook.info(f"Fetching user info for ADID: {adid}")
            result = self.hook.get_records(query)

            if result and len(result) > 0:
                full_name = result[0][0] if result[0][0] else ''
                email = result[0][1] if result[0][1] else ''
                return full_name, email
            else:
                logger.warning(f"No user found for ADID: {adid}")
                self.worklog_hook.warning(f"No user found for ADID: {adid}")
                return '', ''
        except Exception as e:
            logger.error(f"Error fetching user info: {str(e)}")
            self.worklog_hook.error(f"Error fetching user info: {str(e)}")
            return '', ''

    def get_role_platform(self, description: str, organization: str) -> Tuple[str, str]:
        """
        Get role and platform from EXT_AM_ITSM_ROLE_PLATFORM_MAPPING table

        Args:
            description (str): The role description
            organization (str): The company/organization

        Returns:
            Tuple[str, str]: (role, platform) mapping
        """
        try:
            query = f"""
                SELECT u_role, u_platform 
                FROM EXT_AM_ITSM_ROLE_PLATFORM_MAPPING 
                WHERE u_role_description = '{description}' 
                AND u_company = '{organization}'
            """
            logger.info(f"Fetching role and platform for description: {description}, organization: {organization}")
            self.worklog_hook.info(f"Fetching role and platform for description: {description}, organization: {organization}")
            result = self.hook.get_records(query)

            if result and len(result) > 0:
                role = result[0][0] if result[0][0] else ''
                platform = result[0][1] if result[0][1] else ''
                return role, platform
            else:
                logger.warning(f"No role/platform mapping found for description: {description}, organization: {organization}")
                self.worklog_hook.warning(f"No role/platform mapping found for description: {description}, organization: {organization}")
                return '', ''
        except Exception as e:
            logger.error(f"Error fetching role/platform: {str(e)}")
            self.worklog_hook.error(f"Error fetching role/platform: {str(e)}")
            return '', ''

    def execute_query(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Execute a SQL query and return the results

        Args:
            query (str): The SQL query to execute

        Returns:
            Optional[Dict[str, Any]]: The query results or None if no results
        """
        try:
            logger.info(f"Executing query: {query}")
            self.worklog_hook.info(f"Executing query: {query}")
            # For INSERT/UPDATE queries, we use run() which returns the number of affected rows
            if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                affected_rows = self.hook.run(query)
                return {"success": True, "data": affected_rows}
            else:
                # For SELECT queries, we use get_records()
                result = self.hook.get_records(query)
                if result and len(result) > 0:
                    return {"success": True, "data": result[0][0] if isinstance(result[0], tuple) else result[0]}
                else:
                    return {"success": True, "data": None}
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            self.worklog_hook.error(f"Error executing query: {str(e)}")
            return {"success": False, "error": str(e)}

    def create_activity(self, query: str) -> Dict[str, Any]:
        """
        Create a new access management activity record

        Args:
            query (str): The INSERT query to execute

        Returns:
            Dict[str, Any]: The result of the operation
        """
        logger.info("Creating new access management activity record")
        self.worklog_hook.info("Creating new access management activity record")
        return self.execute_query(query)

    def update_activity(self, query: str, process_id: str) -> Dict[str, Any]:
        """
        Update an existing access management activity record

        Args:
            query (str): The UPDATE query to execute
            process_id (str): The process ID to update

        Returns:
            Dict[str, Any]: The result of the operation
        """
        logger.info(f"Updating activity record for process ID: {process_id}")
        self.worklog_hook.info(f"Updating activity record for process ID: {process_id}")
        return self.execute_query(query)

    def get_login_id_by_email(self, user_email: str) -> str:
        """
        Get login ID from EXT_AM_ITSM_USER_LIST based on user name.

        Args:
            user_name: Full name of the user

        Returns:
            str: Login ID if found, empty string otherwise
        """
        try:
            query = f"""
                SELECT u_login_id
                FROM EXT_AM_ITSM_USER_LIST
                WHERE u_email = '{user_email}'
            """
            logger.info(f"Looking up login ID for email: {user_email}")
            self.worklog_hook.info(f"Looking up login ID for email: {user_email}")
            result = self.execute_query(query)
            return result.get('data', '') if result and result.get('success') else ''
        except Exception as e:
            logger.error(f"Error fetching login ID for user email {user_email}: {str(e)}")
            self.worklog_hook.error(f"Error fetching login ID for user email {user_email}: {str(e)}")
            return ''

    def get_row_data(self, query: str) -> Dict[str, Any]:
        """
        Execute a SQL query and return all columns from the first row.

        Args:
            query (str): The SQL query to execute

        Returns:
            Dict[str, Any]: Dictionary containing success status and data array
                           Data array contains all columns from the first row
        """
        try:
            logger.info(f"Executing query: {query}")
            self.worklog_hook.info(f"Executing query: {query}")
            result = self.hook.get_records(query)

            if result and len(result) > 0:
                return {"success": True, "data": result[0]}
            else:
                return {"success": True, "data": None}
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            self.worklog_hook.error(f"Error executing query: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_records(self, query: str) -> List[Tuple]:
        """
        Execute a SQL query and return all rows.

        Args:
            query (str): The SQL query to execute

        Returns:
            List[Tuple]: List of tuples, where each tuple represents a row from the database
        """
        try:
            logger.info(f"Executing query: {query}")
            self.worklog_hook.info(f"Executing query: {query}")
            result = self.hook.get_records(query)
            return result
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            self.worklog_hook.error(f"Error executing query: {str(e)}")
            raise ValueError(f"Error executing query: {str(e)}")

    def run_query(self, query: str) -> None:
        """
        Execute a SQL query that doesn't return results (e.g., CREATE TABLE, DELETE).

        Args:
            query (str): The SQL query to execute
        """
        try:
            logger.info(f"Executing query: {query}")
            self.worklog_hook.info(f"Executing query: {query}")
            self.hook.run(query)
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            self.worklog_hook.error(f"Error executing query: {str(e)}")
            raise ValueError(f"Error executing query: {str(e)}")

    def insert_rows(self, table: str, rows: List[Tuple], target_fields: List[str], commit_every: int = 1000) -> None:
        """
        Insert multiple rows into a table.

        Args:
            table (str): The table to insert into
            rows (List[Tuple]): List of tuples containing the data to insert
            target_fields (List[str]): List of column names
            commit_every (int): Number of rows to insert before committing
        """
        try:
            logger.info(f"Inserting {len(rows)} rows into {table}")
            self.worklog_hook.info(f"Inserting {len(rows)} rows into {table}")
            self.hook.insert_rows(table, rows, target_fields=target_fields, commit_every=commit_every)
        except Exception as e:
            logger.error(f"Error inserting rows: {str(e)}")
            self.worklog_hook.error(f"Error inserting rows: {str(e)}")
            raise ValueError(f"Error inserting rows: {str(e)}")

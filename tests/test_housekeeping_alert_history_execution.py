"""Exercise the alert history task without Airflow or a database."""

import ast
from datetime import datetime, timedelta, timezone
import logging
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock


DAG_SOURCE = Path(__file__).resolve().parents[1] / 'dags/oscar_db_housekeeping_cleanup.py'


class AlertHistoryExecutionTests(unittest.TestCase):
    def test_dry_run_reaches_result_without_deleting_records(self):
        source = ast.parse(DAG_SOURCE.read_text())
        task = next(node for node in source.body if isinstance(node, ast.FunctionDef)
                    and node.name == 'clean_alert_history_records')
        task.decorator_list = []

        db = MagicMock()
        ti = MagicMock()
        ti.xcom_pull.return_value = None
        namespace = {
            'housekeeping_config': SimpleNamespace(ENABLE_DATABASE_CLEANUP=True, DRY_RUN_MODE=True),
            'policy_for': lambda context, family: {'enabled': True, 'retention_days': 30},
            '_connect_oscar_db': lambda connection_id: db,
            'WorkLogHook': MagicMock,
            'wl_write': lambda *args: None,
            'bounded_eligible_count': lambda *args: 3,
            'logger': logging.getLogger(__name__),
            'datetime': datetime, 'timedelta': timedelta, 'timezone': timezone, 'os': os,
        }
        exec(compile(ast.Module(body=[task], type_ignores=[]), str(DAG_SOURCE), 'exec'), namespace)

        result = namespace['clean_alert_history_records'](ti=ti)
        self.assertEqual(result['rows_to_delete'], 3)
        self.assertEqual(result['rows_deleted'], 0)
        ti.xcom_push.assert_called_once_with(key='alert_history_cleanup_result', value=result)
        db.cursor.return_value.execute.assert_not_called()
        db.rollback.assert_not_called()


if __name__ == '__main__':
    unittest.main()

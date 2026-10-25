"""Focused DAG behavior tests without importing the Airflow runtime."""

import ast
from datetime import datetime, timezone
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest


ROOT = Path(__file__).resolve().parents[1]
DAG_SOURCE = ROOT / 'dags/oscar_db_housekeeping_cleanup.py'
CONFIG_SOURCE = ROOT / 'plugins/helpers/oscar_housekeeping_config.py'


def dag_functions(*names):
    spec = importlib.util.spec_from_file_location('housekeeping_config_test', CONFIG_SOURCE)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    source = ast.parse(DAG_SOURCE.read_text())
    selected = []
    for node in source.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == 'POLICY_DEFAULTS'
            for target in node.targets
        ):
            selected.append(node)
        if isinstance(node, ast.FunctionDef) and node.name in names:
            node.decorator_list = []
            selected.append(node)
    namespace = {'housekeeping_config': config, 'datetime': datetime, 'timezone': timezone}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(DAG_SOURCE), 'exec'), namespace)
    return namespace


class RecordingStats:
    def __init__(self):
        self.events = []

    def incr(self, name, value=1):
        self.events.append(('incr', name, value))

    def gauge(self, name, value):
        self.events.append(('gauge', name, value))


class HousekeepingPolicyTests(unittest.TestCase):
    def test_eligible_count_is_limited_to_run_capacity(self):
        namespace = dag_functions('bounded_eligible_count')
        class Cursor:
            def execute(self, statement, params):
                self.statement = statement
                self.params = params

            def fetchone(self):
                return (101,)

        cursor = Cursor()
        count = namespace['bounded_eligible_count'](
            cursor, 'AM_AlertAudit', 'recorded_at < %s', ('cutoff',), 101)
        self.assertEqual(count, 101)
        self.assertIn('LIMIT %s', cursor.statement)
        self.assertEqual(cursor.params, ('cutoff', 101))

    def test_file_defaults_and_single_family_override(self):
        resolve = dag_functions('resolved_policies')['resolved_policies']
        defaults = resolve({})
        self.assertEqual(defaults['alert_history'], {'enabled': True, 'retention_days': 30})
        self.assertEqual(defaults['alert_audit'], {'enabled': True, 'retention_days': 90})
        changed = resolve({'alert_history': {'enabled': False, 'retention_days': 45}})
        self.assertEqual(changed['alert_history'], {'enabled': False, 'retention_days': 45})
        self.assertEqual(changed['alert_audit'], defaults['alert_audit'])
        self.assertEqual(defaults['alert_history']['retention_days'], 30)

    def test_tasks_read_one_versioned_policy_snapshot(self):
        policy_for = dag_functions('policy_for')['policy_for']
        snapshot = {'revision': 7, 'policies': {'alert_history': {'enabled': True, 'retention_days': 45}}}
        ti = SimpleNamespace(xcom_pull=lambda task_ids: snapshot)
        self.assertEqual(policy_for({'ti': ti}, 'alert_history')['retention_days'], 45)
        with self.assertRaises(RuntimeError):
            policy_for({'ti': ti}, 'alert_audit')

    def test_invalid_policy_cannot_change_retention(self):
        resolve = dag_functions('resolved_policies')['resolved_policies']
        for override in ({'alert_history': {'retention_days': 0}},
                         {'alert_history': {'retention_days': True}},
                         {'alert_audit': {'enabled': 'false'}},
                         {'unknown': {'enabled': True}}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                resolve(override)

    def test_failed_cleanup_keeps_dag_failed_after_report_closes(self):
        namespace = dag_functions('enforce_housekeeping_result')
        namespace['TASK_REPORT_REGISTRY'] = [('', 'clean_alert_audit', '', '')]
        states = {'clean_alert_audit': 'failed', 'load_housekeeping_policy': 'success',
                  'close_worklog': 'success'}
        run = SimpleNamespace(get_task_instance=lambda task_id: SimpleNamespace(state=states[task_id]))
        with self.assertRaisesRegex(RuntimeError, 'clean_alert_audit'):
            namespace['enforce_housekeeping_result'](dag_run=run)
        states['clean_alert_audit'] = 'success'
        namespace['enforce_housekeeping_result'](dag_run=run)

    def test_success_metrics_use_actual_rows_and_skip_disabled_result(self):
        namespace = dag_functions('_cleanup_success', '_cleanup_failure')
        stats = RecordingStats()
        namespace['stats'] = stats
        results = {'alert_history_cleanup_result': {'rows_deleted': 12,
                   'eligible_backlog': 3, 'dry_run': False}}
        ti = SimpleNamespace(task_id='clean_alert_history_records', duration=2.5,
                             xcom_pull=lambda task_ids, key: results.get(key))
        namespace['_cleanup_success']({'task_instance': ti, 'ti': ti})
        self.assertIn(('incr', 'oscar_housekeeping.rows_deleted.alert_history', 12), stats.events)
        self.assertIn(('gauge', 'oscar_housekeeping.eligible_backlog.alert_history', 3), stats.events)
        stats.events.clear()
        results.clear()
        namespace['_cleanup_success']({'task_instance': ti, 'ti': ti})
        self.assertEqual(stats.events, [])
        namespace['_cleanup_failure']({'task_instance': ti})
        self.assertEqual(stats.events, [('incr', 'oscar_housekeeping.runs_failure.alert_history', 1)])


if __name__ == '__main__':
    unittest.main()

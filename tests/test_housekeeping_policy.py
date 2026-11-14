"""Focused DAG behavior tests without importing the Airflow runtime."""

import ast
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import logging
import os

import pytest
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
            isinstance(target, ast.Name) and target.id in ('POLICY_DEFAULTS', 'TASK_REPORT_REGISTRY')
            for target in node.targets
        ):
            selected.append(node)
        if isinstance(node, ast.FunctionDef) and node.name in names:
            node.decorator_list = []
            selected.append(node)
    namespace = {'housekeeping_config': config, 'datetime': datetime, 'timezone': timezone,
                 'timedelta': timedelta, 'json': json, 'os': os, 'logger': logging.getLogger(__name__)}
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


EXPECTED_FAMILIES = {
    'task_history', 'alert_history', 'notification_audit', 'ticketing_audit',
    'user_audit', 'alert_audit', 'worklog', 'llm_logs', 'sli_events', 'task_history_retired',
}
NEW_FAMILIES = [
    ('worklog', 'clean_worklogs', 'WORKLOG', True, 30, ['TM_WorkLog']),
    ('llm_logs', 'clean_llm_logs', 'LLM_LOGS', True, 90, ['LLM_Spend_Logs', 'LLM_Chat_Audit']),
    ('sli_events', 'clean_sli_events', 'SLI_EVENTS', False, 90, ['MS_SLI_Events']),
    ('task_history_retired', 'clean_task_history_retired', 'TASK_HISTORY_RETIRED', False, 90, ['TM_HistoryRetired']),
]


class RetentionCursor:
    """Deterministic SQL boundary; never opens a DB or uses a SQL engine."""
    def __init__(self, missing=(), fail_on=None):
        self.statements = []
        self.missing = set(missing)
        self.fail_on = fail_on
        self.rowcount = 0
        self.selections = {}
        self.closed = False

    def execute(self, statement, params=()):
        self.statements.append((statement, tuple(params)))
        self.statement, self.params = statement, tuple(params)
        if self.fail_on and statement.startswith(self.fail_on):
            raise RuntimeError('injected SQL failure')
        self.rowcount = len(params) if statement.startswith('DELETE') else 0

    def fetchone(self):
        if 'information_schema.tables' in self.statement:
            return (int(self.params[0] not in self.missing),)
        return (3,)

    def fetchall(self):
        table = self.statement.split(' FROM ')[1].split()[0]
        times = self.selections.get(table, 0)
        self.selections[table] = times + 1
        return [('old-closed-1',), ('old-closed-2',)] if times == 0 else []

    def close(self):
        self.closed = True


class RetentionDB:
    def __init__(self, cursor):
        self.cur, self.commits, self.rollbacks, self.closed = cursor, 0, 0, False

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def retention_namespace(family, enabled=True, days=17, dry_run=False, missing=(), fail_on=None):
    ns = dag_functions('resolved_policies', 'policy_for', 'bounded_eligible_count', '_table_exists',
                       '_table_size_gb', '_retain_table', 'run_table_retention', 'wl_write',
                       'build_task_report', '_cleanup_success', '_cleanup_failure',
                       *(row[1] for row in NEW_FAMILIES))
    config = ns['housekeeping_config']
    config.DRY_RUN_MODE = dry_run
    for _, _, prefix, _, _, _ in NEW_FAMILIES:
        setattr(config, prefix + '_BATCH_SIZE', 2)
        setattr(config, prefix + '_MAX_ITERATIONS', 2)
        setattr(config, prefix + '_MAINTENANCE_OPTIMIZATION', False)
    snapshot = {'revision': 16, 'policies': ns['resolved_policies']({family: {'enabled': enabled, 'retention_days': days}})}
    pushes = {}
    ti = SimpleNamespace(xcom_pull=lambda task_ids, key=None: snapshot if task_ids == 'load_housekeeping_policy' else None,
                         xcom_push=lambda key, value: pushes.update({key: value}))
    cur = RetentionCursor(missing, fail_on)
    db = RetentionDB(cur)
    ns['_connect_oscar_db'] = lambda connection_id: db
    ns['create_timeout_checker'] = lambda *args: lambda: False
    ns['WorkLogHook'] = lambda: None
    ns['stats'] = RecordingStats()
    return ns, {'ti': ti}, db, cur, pushes


def test_resolver_accepts_exact_expected_family_contract():
    ns = dag_functions('resolved_policies')
    assert set(ns['resolved_policies']({})) == EXPECTED_FAMILIES


@pytest.mark.parametrize('family,task,prefix,enabled,days,tables', NEW_FAMILIES)
def test_new_family_file_defaults(family, task, prefix, enabled, days, tables):
    ns = dag_functions('resolved_policies')
    assert ns['resolved_policies']({})[family] == {'enabled': enabled, 'retention_days': days}


@pytest.mark.parametrize('family,task,prefix,default_enabled,days,tables', NEW_FAMILIES)
@pytest.mark.parametrize('enabled', [False, True])
def test_new_family_uses_policy_enabled_and_days(family, task, prefix, default_enabled, days, tables, enabled):
    ns, context, db, cur, pushes = retention_namespace(family, enabled)
    # Opposite file settings must never override the run's policy snapshot.
    setattr(ns['housekeeping_config'], 'ENABLE_' + prefix + '_CLEANUP', not enabled)
    result = ns[task](**context)
    if not enabled:
        assert isinstance(result, str) and 'disabled' in result.lower()
        assert not cur.statements and db.commits == 0
    else:
        assert result['days_to_keep'] == 17
        assert result['rows_deleted'] > 0
        assert [item['table'] for item in result['tables']] == tables
        assert pushes[family + '_cleanup_result'] == result
        counts = [(sql, params) for sql, params in cur.statements if sql.startswith('SELECT COUNT(*) FROM (')]
        assert all(params[-1] == 5 for _, params in counts)
        cutoff = datetime.strptime(result['tables'][0]['cutoff'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        assert abs((datetime.now(timezone.utc) - cutoff).total_seconds() - 17 * 86400) < 5
        assert db.closed and cur.closed


@pytest.mark.parametrize('family,task,prefix,enabled,days,tables', NEW_FAMILIES)
def test_new_family_dry_run_only_counts(family, task, prefix, enabled, days, tables):
    ns, context, db, cur, pushes = retention_namespace(family, dry_run=True)
    result = ns[task](**context)
    assert result['dry_run'] is True and result['rows_deleted'] == 0
    assert result['rows_to_delete'] == 3 * len(tables)
    assert db.commits == db.rollbacks == 0
    assert all(sql.startswith('SELECT') for sql, _ in cur.statements)
    if family == 'worklog':
        assert result['tables'][0]['would_delete_TM_WorkLogEntries'] == 3
        assert result['tables'][0]['would_delete_TM_WorkLogMetadata'] == 3
    if family == 'sli_events':
        assert result['tables'][0]['would_delete_MS_SLI_EventMetadata'] == 3
    report = ns['build_task_report'](task, 'db', result)
    assert report['status'] == 'dry_run' and report['rows_deleted'] == 0


@pytest.mark.parametrize('family,task,prefix,enabled,days,tables', NEW_FAMILIES)
def test_new_family_missing_tables_are_reported_skipped(family, task, prefix, enabled, days, tables):
    ns, context, db, cur, pushes = retention_namespace(family, missing=tables)
    result = ns[task](**context)
    report = ns['build_task_report'](task, 'db', result)
    assert report['status'] == 'skipped'
    assert 'table' in report['reason'].lower()
    assert db.commits == 0 and db.closed and cur.closed
    assert pushes[family + '_cleanup_result'] == result


@pytest.mark.parametrize('family,task,expected', [
    ('worklog', 'clean_worklogs', ['TM_WorkLogEntries', 'TM_WorkLogMetadata', 'TM_WorkLog']),
    ('sli_events', 'clean_sli_events', ['MS_SLI_EventMetadata', 'MS_SLI_Events']),
])
def test_fk_children_deleted_before_parent_in_one_batch(family, task, expected):
    ns, context, db, cur, _ = retention_namespace(family)
    ns[task](**context)
    deletes = [(sql, params) for sql, params in cur.statements if sql.startswith('DELETE')]
    assert [sql.split()[2] for sql, _ in deletes] == expected
    assert all(params == ('old-closed-1', 'old-closed-2') for _, params in deletes)
    assert db.commits == 1


def test_worklogs_only_select_closed_expired_rows_and_lock_before_children():
    ns, context, db, cur, _ = retention_namespace('worklog')
    ns['clean_worklogs'](**context)
    selects = [sql for sql, _ in cur.statements if sql.startswith('SELECT') and 'FROM TM_WorkLog WHERE' in sql]
    assert selects and all("status = 'CLOSED'" in sql and 'created_at < %s' in sql for sql in selects)
    batch_select = next(sql for sql in selects if sql.startswith('SELECT id'))
    assert 'LIMIT %s' in batch_select and 'FOR UPDATE' in batch_select


def test_missing_child_skipped_while_present_parent_is_retained():
    ns, context, db, cur, _ = retention_namespace('worklog', missing=['TM_WorkLogMetadata'])
    result = ns['clean_worklogs'](**context)
    assert result['rows_deleted'] == 4
    assert not any(sql.startswith('DELETE FROM TM_WorkLogMetadata') for sql, _ in cur.statements)


def test_one_missing_llm_table_does_not_hide_other_cleanup():
    ns, context, db, cur, _ = retention_namespace('llm_logs', missing=['LLM_Chat_Audit'])
    result = ns['clean_llm_logs'](**context)
    assert result['rows_deleted'] == 2
    assert result['tables'][1]['skipped'] == 'table not present'


def test_child_failure_rolls_back_and_closes_resources():
    ns, context, db, cur, _ = retention_namespace('worklog', fail_on='DELETE FROM TM_WorkLogMetadata')
    with pytest.raises(RuntimeError, match='injected SQL failure'):
        ns['clean_worklogs'](**context)
    assert db.commits == 0 and db.rollbacks == 1
    assert not any(sql.startswith('DELETE FROM TM_WorkLog WHERE') for sql, _ in cur.statements)
    assert db.closed and cur.closed


def test_master_flag_prevents_all_new_family_work():
    for family, task, *_ in NEW_FAMILIES:
        ns, context, db, cur, _ = retention_namespace(family)
        ns['housekeeping_config'].ENABLE_DATABASE_CLEANUP = False
        assert 'master flag' in ns[task](**context)
        assert cur.statements == []


@pytest.mark.parametrize('family,task,prefix,enabled,days,tables', NEW_FAMILIES)
def test_new_family_metrics_use_total_rows_and_failure_family(family, task, prefix, enabled, days, tables):
    ns, context, db, cur, pushes = retention_namespace(family)
    result = ns[task](**context)
    ti = SimpleNamespace(task_id=task, duration=1.5, xcom_pull=lambda **kw: result)
    ns['_cleanup_success']({'task_instance': ti, 'ti': ti})
    assert ('incr', 'oscar_housekeeping.rows_deleted.' + family, result['rows_deleted']) in ns['stats'].events
    ns['_cleanup_failure']({'task_instance': ti})
    assert ('incr', 'oscar_housekeeping.runs_failure.' + family, 1) in ns['stats'].events
    ns['stats'].events.clear()
    result['dry_run'] = True
    ns['_cleanup_success']({'task_instance': ti, 'ti': ti})
    assert not ns['stats'].events


def test_new_tasks_have_callbacks_reports_unique_ids_and_pre_join_edges():
    tree = ast.parse(DAG_SOURCE.read_text())
    dag = next(node for node in tree.body if isinstance(node, ast.With))
    assignments = {node.targets[0].id: node.value for node in dag.body if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)}
    task_ids = []
    for value in assignments.values():
        if isinstance(value, ast.Call):
            keywords = value.func.keywords if isinstance(value.func, ast.Call) else value.keywords
            explicit = next((kw.value.value for kw in keywords if kw.arg == 'task_id'), None)
            task_ids.append(explicit or ast.unparse(value.func).split('.')[0])
    assert len(task_ids) == len(set(task_ids))
    chain = next(node.value for node in dag.body if isinstance(node, ast.Expr) and isinstance(node.value, ast.BinOp))
    def flatten(node):
        return flatten(node.left) + flatten(node.right) if isinstance(node, ast.BinOp) else [node]
    nodes = flatten(chain)
    tasks = next(node for node in nodes if isinstance(node, ast.List))
    assert nodes.index(tasks) < next(i for i, node in enumerate(nodes) if isinstance(node, ast.Name) and node.id == 'join_cleanup')
    before_join = {node.id for node in tasks.elts}
    registry = dag_functions()['TASK_REPORT_REGISTRY']
    for family, task, *_ in NEW_FAMILIES:
        assert task + '_task' in before_join
        call = assignments[task + '_task'].func
        callbacks = {kw.arg: ast.unparse(kw.value) for kw in call.keywords}
        assert callbacks['on_success_callback'] == '_cleanup_success'
        assert callbacks['on_failure_callback'] == '_cleanup_failure'
        assert any(row[:2] == (family + '_cleanup_result', task) for row in registry)


@pytest.mark.parametrize('family,task,prefix,enabled,days,tables', NEW_FAMILIES)
def test_iteration_limit_caps_deletes_per_family(family, task, prefix, enabled, days, tables):
    ns, context, db, cur, _ = retention_namespace(family)
    cur.fetchall = lambda: [('id-1',), ('id-2',)]
    result = ns[task](**context)
    assert db.commits == 2 * len(tables)
    assert all(table['iterations'] == 2 and table['limit_reached'] for table in result['tables'])
    parent_deletes = [sql for sql, _ in cur.statements if any(sql.startswith('DELETE FROM ' + table + ' WHERE') for table in tables)]
    assert len(parent_deletes) == 2 * len(tables)


@pytest.mark.parametrize('family,task,prefix,enabled,days,tables', NEW_FAMILIES)
def test_timeout_stops_without_deletes(family, task, prefix, enabled, days, tables):
    ns, context, db, cur, _ = retention_namespace(family)
    ns['create_timeout_checker'] = lambda *args: lambda: True
    result = ns[task](**context)
    assert result['rows_deleted'] == 0 and db.commits == 0
    assert all(table['timeout_reached'] for table in result['tables'])
    assert not any(sql.startswith('DELETE') for sql, _ in cur.statements)


def test_dry_run_reports_capped_backlog():
    ns, context, db, cur, _ = retention_namespace('worklog', dry_run=True)
    original = cur.fetchone
    cur.fetchone = lambda: original() if 'information_schema' in cur.statement else (5,)
    result = ns['clean_worklogs'](**context)
    assert result['rows_to_delete'] == 5 and result['backlog_count_capped'] is True


def test_metrics_skip_disabled_and_missing_table_results():
    for family, task, *_ in NEW_FAMILIES:
        ns, context, db, cur, _ = retention_namespace(family, enabled=False)
        result = ns[task](**context)
        ti = SimpleNamespace(task_id=task, duration=0, xcom_pull=lambda **kw: result)
        ns['_cleanup_success']({'task_instance': ti, 'ti': ti})
        assert ns['stats'].events == []


def test_dry_run_marks_truncated_child_counts_when_parent_count_is_exact():
    ns, context, db, cur, _ = retention_namespace('worklog', dry_run=True)
    original = cur.fetchone
    def counts():
        if 'information_schema' in cur.statement:
            return original()
        return (1,) if 'SELECT 1 FROM TM_WorkLog WHERE' in cur.statement else (5,)
    cur.fetchone = counts
    result = ns['clean_worklogs'](**context)
    assert result['rows_to_delete'] == 1
    assert result['backlog_count_capped'] is False
    assert result['dry_run_counts_capped'] is True
    assert result['tables'][0]['child_counts_capped'] == {
        'TM_WorkLogEntries': True, 'TM_WorkLogMetadata': True,
    }
    report = ns['build_task_report']('worklogs', 'db', result)
    assert report['tables'][0]['child_counts_capped']['TM_WorkLogEntries'] is True

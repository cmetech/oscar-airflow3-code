"""Alert history housekeeping must run by default and keep up with a production close rate."""

import ast
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
DAG_SOURCE = ROOT / 'dags/oscar_db_housekeeping_cleanup.py'
CONFIG_SOURCE = ROOT / 'plugins/helpers/oscar_housekeeping_config.py'
PRODUCTION_CLOSES_PER_DAY = 138_000  # AT&T 26.4.4: 1.6 resolved alerts/s


class HousekeepingCapacityTest(unittest.TestCase):
    def test_dag_is_not_paused_on_creation(self):
        dag = next(node for node in ast.walk(ast.parse(DAG_SOURCE.read_text()))
                   if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'DAG')
        flags = {kw.arg: kw.value for kw in dag.keywords}
        self.assertIn('is_paused_upon_creation', flags)
        self.assertIs(flags['is_paused_upon_creation'].value, False)

    def test_alert_history_capacity_is_at_least_twice_production_closes(self):
        spec = importlib.util.spec_from_file_location('housekeeping_config_capacity', CONFIG_SOURCE)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        capacity = config.ALERT_HISTORY_BATCH_SIZE * config.ALERT_HISTORY_MAX_ITERATIONS
        self.assertGreaterEqual(capacity, 2 * PRODUCTION_CLOSES_PER_DAY)


if __name__ == '__main__':
    unittest.main()

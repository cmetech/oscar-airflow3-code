import ast
import json
import logging
from pathlib import Path
import re
import shlex
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ["compose:oscar-correlator/correlator", "compose:oscar-taskmanager/history_worker"]


def recovery_functions():
    # Exercise the actual functions without importing Airflow or starting a DAG.
    source = ast.parse((ROOT / "dags/oscar_platform_recovery.py").read_text())
    names = {"_matches_oscar_pattern", "_preprocess_required_containers", "_check_critical_containers_running"}
    module = ast.Module(body=[node for node in source.body if isinstance(node, ast.FunctionDef) and node.name in names], type_ignores=[])
    namespace = {"json": json, "logger": logging.getLogger(__name__), "re": re, "shlex": shlex, "COMMAND_TIMEOUT": 10, "AIRFLOW_VERSION": "3.3.2"}
    for flag in ("TRAPRECEIVER", "SMTPRECEIVER", "WEBHOOKRECEIVER", "CORRELATOR", "DECISION", "DISCOVERY"):
        namespace[flag + "_ENABLED"] = False
    exec(compile(module, "recovery-functions", "exec"), namespace)
    return namespace


class SharedRecoveryConfig(unittest.TestCase):
    def test_core_disable_flag_preserves_explicit_unrelated_projects(self):
        functions = recovery_functions()
        targets = ["compose:custom/correlator", "thirdparty/correlator", "compose:oscar-correlator/correlator", "correlator"]
        result = functions["_preprocess_required_containers"](json.dumps(targets), "oscar", "arm64")
        self.assertEqual(result, ["compose:custom/correlator", "thirdparty/correlator"])

    def test_normalized_legacy_aliases_count_once(self):
        functions = recovery_functions()
        result = functions["_preprocess_required_containers"]('["monitor", "oscar/monitor"]', "oscar", "arm64")
        self.assertEqual(result, ["oscar/monitor"])

    def test_recovery_uses_airflow_major_specific_roles(self):
        functions = recovery_functions()
        functions["AIRFLOW_VERSION"] = "2.10.3"
        targets = ["compose:oscar-workflow/airflow-webserver", "compose:oscar-workflow/airflow-apiserver", "compose:oscar-workflow/airflow-dag"]
        result = functions["_preprocess_required_containers"](json.dumps(targets), "oscar", "arm64")
        self.assertEqual(result, ["compose:oscar-workflow/airflow-webserver"])

    def test_recovery_filters_disabled_compose_roles(self):
        functions = recovery_functions()
        result = functions["_preprocess_required_containers"](json.dumps(TARGETS), "oscaraiapp", "arm64")
        self.assertEqual(result, ["compose:oscar-taskmanager/history_worker"])

    def test_recovery_distinguishes_same_image_roles_and_projects(self):
        functions = recovery_functions()
        output = "oscaraiapp/taskmanager:v1\toscar-taskmanager\tworklog_worker\tFalse\n" + "oscaraiapp/correlator:v1\ttest-correlator\tcorrelator\tFalse\n"
        conn = SimpleNamespace(run=lambda *args, **kwargs: SimpleNamespace(stdout=output, ok=True))
        missing, running = functions["_check_critical_containers_running"](conn, "/oscar", TARGETS + ["compose:oscar-taskmanager/worklog_worker", "oscaraiapp/taskmanager"])
        self.assertEqual(missing, TARGETS)
        self.assertEqual(running, ["compose:oscar-taskmanager/worklog_worker", "oscaraiapp/taskmanager"])

    def test_recovery_ignores_oneoff_containers_and_near_image_names(self):
        functions = recovery_functions()
        output = "oscar/correlator:v1\toscar-correlator\tcorrelator\tTrue\nredis-exporter:v1\toscar-exporters\tredis-exporter\tFalse\n"
        conn = SimpleNamespace(run=lambda *args, **kwargs: SimpleNamespace(stdout=output, ok=True))
        expected = ["compose:oscar-correlator/correlator", "redis"]
        missing, running = functions["_check_critical_containers_running"](conn, "/oscar", expected)
        self.assertEqual(missing, expected)
        self.assertEqual(running, [])


if __name__ == "__main__":
    unittest.main()

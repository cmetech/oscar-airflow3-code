"""Exercise workflow wire payloads without running an Airflow scheduler."""
import importlib
import json
import logging
import sys
from pathlib import Path
from types import ModuleType

import httpx
import pytest


@pytest.fixture
def clients(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "plugins"))
    for name in ("airflow", "airflow.hooks", "airflow.hooks.base", "airflow.sdk", "airflow.sdk.bases", "airflow.sdk.bases.hook"):
        module = ModuleType(name)
        module.BaseHook = object
        monkeypatch.setitem(sys.modules, name, module)
    helper = importlib.import_module("helpers.worklog_helper")
    hook = importlib.import_module("hooks.worklog_hook")
    requests = []
    response_status = {"create": 201, "update": 200}

    def respond(request):
        requests.append(request)
        status = 200 if request.method == "GET" else response_status["create"] if request.method == "POST" and not request.url.path.endswith("close") else response_status["update"]
        return httpx.Response(status, json={"id": "worklog-1", "status": "OPEN", "metadata": response_status.get("metadata", [])} if status < 400 else {"detail": response_status.get("detail", "metadata value exceeds 1024 characters")})

    client_class = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client_class(transport=httpx.MockTransport(respond), **kwargs))
    return hook.WorkLogHook(), helper.SyncWorkLogManager(), requests, response_status


def create(client, *args, **kwargs):
    method = client.create_worklog if hasattr(client, "create_worklog") else client.create
    return method(*args, **kwargs)


@pytest.mark.parametrize("index", [0, 1])
def test_existing_positional_create_stays_compatible(clients, index):
    create(clients[index], "name", "description", "DB", [{"key": "env", "value": "prod"}])
    assert json.loads(clients[2][-1].content) == {"name": "name", "description": "description", "type": "DB", "status": "OPEN", "metadata": [{"key": "env", "value": "prod"}]}


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("category", ["network-operations", ""])
def test_optional_category_forwarded_on_create(clients, index, category):
    create(clients[index], "name", category=category)
    assert json.loads(clients[2][-1].content)["category"] == category


def test_open_forwards_category_and_still_closes(clients):
    manager = clients[1]
    with manager.open("name", "description", "DB", [], category="network-operations"):
        assert manager.current_worklog_id == "worklog-1"
    assert json.loads(clients[2][0].content)["category"] == "network-operations"
    assert clients[2][-1].url.path.endswith("/worklog-1/close")


def test_existing_positional_open_stays_compatible(clients):
    with clients[1].open("name", "description", "DB", []):
        pass
    assert "category" not in json.loads(clients[2][0].content)


def test_update_omits_category_and_preserves_existing_positionals(clients):
    clients[1].update("worklog-1", "name", "description", [])
    assert json.loads(clients[2][-1].content) == {"name": "name", "description": "description", "metadata": []}


@pytest.mark.parametrize("category", [None, "", "network-operations"])
def test_category_only_update_distinguishes_explicit_clear(clients, category):
    clients[1].update("worklog-1", category=category)
    assert json.loads(clients[2][-1].content) == {"category": category}


@pytest.mark.parametrize("index", [0, 1])
def test_create_validation_error_is_propagated_without_id(clients, index):
    clients[3]["create"] = 422
    with pytest.raises(Exception, match="HTTP 422"):
        create(clients[index], "name", metadata=[{"key": "detail", "value": "x" * 1025}])
    assert getattr(clients[index], "worklog_id", None) is None
    assert getattr(clients[index], "current_worklog_id", None) is None


def test_update_validation_error_is_propagated(clients):
    clients[3]["update"] = 422
    with pytest.raises(Exception, match="HTTP 422"):
        clients[1].update("worklog-1", metadata=[{"key": "detail", "value": "x" * 1025}])
    assert clients[1].current_worklog is None


def test_hook_metadata_update_validation_error_is_propagated(clients):
    clients[3]["update"] = 422
    with pytest.raises(Exception, match="HTTP 422"):
        clients[0].add_metadata({"key": "detail", "value": "x" * 1025}, "worklog-1")


@pytest.mark.parametrize("level", [logging.INFO, logging.DEBUG])
@pytest.mark.parametrize("operation", ["hook_create", "manager_create", "manager_update", "manager_open", "hook_add"])
def test_rejected_metadata_is_absent_from_producer_logs(clients, caplog, level, operation):
    marker = "PRIVATE_PRODUCER_MARKER"
    metadata = [{"key":"detail", "value":marker+"x"*1025}]
    clients[3].update(create=422, update=422, metadata=[{"key":"existing", "value":marker}], detail=marker)
    caplog.set_level(level)
    with pytest.raises(Exception):
        if operation == "hook_create":
            clients[0].create_worklog("name", metadata=metadata)
        elif operation == "manager_create":
            clients[1].create("name", metadata=metadata)
        elif operation == "manager_update":
            clients[1].update("worklog-1", metadata=metadata)
        elif operation == "manager_open":
            with clients[1].open("name", metadata=metadata):
                pytest.fail("Rejected create must not enter the context")
        else:
            clients[0].add_metadata(metadata, "worklog-1")
    assert marker not in caplog.text
    assert "422" in caplog.text
    assert all(record.exc_info is None for record in caplog.records)
    assert getattr(clients[0], "worklog_id", None) is None
    assert clients[1].current_worklog is None and clients[1].current_worklog_id is None


@pytest.mark.parametrize("value", ["", " \n\t "])
@pytest.mark.parametrize("key", ["ticket", "new-key"])
def test_hook_metadata_merge_preserves_empty_values_and_unrelated_keys(clients, key, value):
    clients[3]["metadata"] = [{"key":"ticket", "value":"INC-1"}, {"key":"env", "value":"prod"}]
    clients[0].add_metadata({"key":key, "value":value}, "worklog-1")
    sent = json.loads(clients[2][-1].content)["metadata"]
    assert {item["key"]:item["value"] for item in sent}[key] == value
    assert {item["key"]:item["value"] for item in sent}["env"] == "prod"
    if key == "new-key":
        assert {item["key"]:item["value"] for item in sent}["ticket"] == "INC-1"


@pytest.mark.parametrize("field", ["key", "value"])
@pytest.mark.parametrize("invalid", [False, 0, None, [], {}])
def test_hook_invalid_falsy_metadata_is_forwarded_and_rejected(clients, field, invalid):
    clients[3].update(update=422, metadata=[{"key":"env", "value":"prod"}])
    item = {"key":"ticket", "value":"INC-1", field:invalid}
    with pytest.raises(Exception, match="HTTP 422"):
        clients[0].add_metadata(item, "worklog-1")
    request = clients[2][-1]
    assert request.method == "PUT"
    assert json.loads(request.content)["metadata"] == [{"key":"env", "value":"prod"}, item]


@pytest.mark.parametrize("filename,index,keys", [
    ("alert_workflow_test.py",0,["alert_name"]),
    ("alert_workflow_test.py",1,["severity"]),
    ("alert_workflow_test.py",2,["fingerprint"]),
    ("alert_workflow_test.py",3,["incident_number"]),
    ("alert_workflow_test.py",4,["ticket_id"]),
    ("alert_workflow_test.py",5,["processing_issue"]),
    ("alert_workflow_test.py",6,["error","processing_stage"]),
    ("oscar_platform_recovery.py",0,["incident_number"]),
    ("oscar_platform_recovery.py",1,["incident_severity"]),
    ("oscar_platform_recovery.py",2,["ticket_id"]),
    ("oscar_platform_recovery.py",3,["ticketing_system"]),
    ("oscar_platform_recovery.py",4,["ticket_updated","ticket_system","ticket_id"]),
    ("oscar_platform_recovery.py",5,["failed_task","failure_reason","failure_time"]),
    ("system_health_monitoring.py",0,["health_status","alerts_generated","check_duration"]),
    ("system_health_monitoring.py",1,["report_status","report_file"]),
])
def test_dag_metadata_arguments_reach_hook_in_documented_format(clients, filename, index, keys):
    # Evaluate the production argument expression without importing an Airflow DAG/scheduler.
    import ast
    from datetime import datetime
    from types import SimpleNamespace
    source = Path(__file__).resolve().parents[1] / "dags" / filename
    calls = sorted((node for node in ast.walk(ast.parse(source.read_text()))
                    if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute)
                    and node.func.attr == "add_metadata"), key=lambda node:node.lineno)
    expression = ast.Expression(body=calls[index].args[0])
    namespace = dict(alert_name="CPU", severity="warning", fingerprint="FP", incident_number="INC-2", ticket_id="T",
                     e=RuntimeError("controlled failure"), ticket_update_result={"updated":True,"system":"remedy","ticket_id":"T"},
                     task_instance=SimpleNamespace(task_id="check"), exception=RuntimeError("failure"), datetime=datetime,
                     health_status="healthy", alerts_generated=1, result={"duration":2.5}, report_status="ok",
                     context={"ds":"2026-10-10"}, create_ticket="remedy", incident_severity="major")
    items = eval(compile(expression, str(source), "eval"), namespace)
    assert isinstance(items,list) and [item["key"] for item in items] == keys
    assert all(isinstance(item["value"],str) for item in items)
    clients[3]["metadata"] = [{"key":"env", "value":"prod"}]
    clients[0].add_metadata(items, "worklog-1")
    assert json.loads(clients[2][-1].content)["metadata"] == [{"key":"env", "value":"prod"}, *items]


def test_hook_metadata_merge_accepts_nullable_existing_collection(clients):
    clients[3]["metadata"] = None
    clients[0].add_metadata({"key":"ticket", "value":""}, "worklog-1")
    assert clients[2][-1].method == "PUT"
    assert json.loads(clients[2][-1].content) == {"metadata":[{"key":"ticket", "value":""}]}

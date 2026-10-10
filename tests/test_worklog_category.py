"""Exercise workflow wire payloads without running an Airflow scheduler."""
import importlib
import json
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
        return httpx.Response(status, json={"id": "worklog-1", "status": "OPEN", "metadata": []} if status < 400 else {"detail": "metadata value exceeds 1024 characters"})

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
    with pytest.raises(Exception, match="metadata value exceeds 1024"):
        create(clients[index], "name", metadata=[{"key": "detail", "value": "x" * 1025}])
    assert getattr(clients[index], "worklog_id", None) is None
    assert getattr(clients[index], "current_worklog_id", None) is None


def test_update_validation_error_is_propagated(clients):
    clients[3]["update"] = 422
    with pytest.raises(Exception, match="metadata value exceeds 1024"):
        clients[1].update("worklog-1", metadata=[{"key": "detail", "value": "x" * 1025}])
    assert clients[1].current_worklog is None


def test_hook_metadata_update_validation_error_is_propagated(clients):
    clients[3]["update"] = 422
    with pytest.raises(Exception, match="metadata value exceeds 1024"):
        clients[0].add_metadata({"key": "detail", "value": "x" * 1025}, "worklog-1")

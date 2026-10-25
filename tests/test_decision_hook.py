import sys
import types
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "plugins"))
base = types.ModuleType("hooks.oscar_hook")


class OscarHook:
    def __init__(self, conn_id=None):
        self.conn_id = conn_id or self.default_conn_name
        self.protocol = "https"
        self.host = "middleware"
        self.port = 5200
        self.verify_ssl = False

    def get_connection(self, conn_id):
        return types.SimpleNamespace(password="airflow-key-123")


base.OscarHook = OscarHook
sys.modules["hooks.oscar_hook"] = base

from hooks.decision_hook import DecisionHook


def test_decision_hook_uses_airflow_connection_key(monkeypatch):
    seen = {}
    client_class = httpx.Client

    def handle(request):
        seen["url"] = str(request.url)
        seen["key"] = request.headers["X-API-Key"]
        return httpx.Response(200, json={"status": "selected", "choice": "check_orders"})

    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client_class(transport=httpx.MockTransport(handle)))
    answer = DecisionHook().evaluate("examples_ticket_triage", {"description": "Orders checkout 502"})
    assert answer["choice"] == "check_orders"
    assert seen == {"url": "https://middleware:5200/api/v1/decisions/evaluate/examples_ticket_triage", "key": "airflow-key-123"}


def test_decision_hook_rejects_missing_connection_key(monkeypatch):
    monkeypatch.setattr(OscarHook, "get_connection", lambda self, conn_id: types.SimpleNamespace(password=None))
    with pytest.raises(ValueError, match="API key"):
        DecisionHook()

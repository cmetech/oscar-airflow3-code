import sys
import types
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parents[1] / 'plugins'))
base = types.ModuleType('hooks.oscar_hook')


class OscarHook:
    def __init__(self, conn_id=None):
        self.protocol, self.host, self.port, self.verify_ssl = 'https', 'middleware', 5200, True


base.OscarHook = OscarHook
sys.modules['hooks.oscar_hook'] = base

from hooks.notify_hook import NotifyHook


def test_notification_payload_and_response_never_reach_logs(monkeypatch, caplog):
    real_client = httpx.Client

    def handle(request):
        assert b'private-email@example.test' in request.content
        return httpx.Response(200, json={'message': 'private-email@example.test'})

    monkeypatch.setattr(httpx, 'Client', lambda **kw: real_client(transport=httpx.MockTransport(handle)))
    caplog.set_level('INFO')
    NotifyHook().send_notification({'name': 'approved', 'recipients': 'private-email@example.test',
        'message': 'sensitive'})
    assert 'private-email@example.test' not in caplog.text
    assert 'sensitive' not in caplog.text

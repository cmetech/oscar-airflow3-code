import sys
import types
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'plugins'))
base = types.ModuleType('hooks.oscar_hook')


class OscarHook:
    def __init__(self, conn_id=None):
        self.protocol, self.host, self.port, self.verify_ssl = 'https', 'middleware', 5200, True

    def get_connection(self, conn_id):
        assert conn_id == 'ticket_automation_custom_fields'
        return types.SimpleNamespace(password='dedicated-key')


base.OscarHook = OscarHook
sys.modules['hooks.oscar_hook'] = base

from hooks.ticketing_hook import TicketingHook


def test_custom_fields_use_dedicated_key_only_for_opt_in_reads(monkeypatch):
    seen = []
    real_client = httpx.Client

    def handle(request):
        seen.append(request)
        return httpx.Response(200, json=[])

    monkeypatch.setattr(httpx, 'Client', lambda **kw: real_client(transport=httpx.MockTransport(handle)))
    hook = TicketingHook()
    hook.search_tickets(system='snow')
    hook.search_tickets(system='snow', custom_field_keys=['email'])
    assert 'X-API-Key' not in seen[0].headers
    assert seen[1].headers['X-API-Key'] == 'dedicated-key'
    assert seen[1].url.params['custom_field_keys'] == 'email'


def test_custom_fields_allow_unverified_https():
    hook = TicketingHook()
    hook.verify_ssl = False
    assert hook._read_headers(['email'])['X-API-Key'] == 'dedicated-key'


def test_custom_fields_still_require_https():
    hook = TicketingHook()
    hook.protocol = 'http'
    with pytest.raises(ValueError, match='HTTPS middleware'):
        hook._read_headers(['email'])


def test_custom_fields_accept_dedicated_ca_bundle(tmp_path):
    ca = tmp_path / 'oscar-ca.pem'
    ca.write_text('synthetic test certificate path')
    hook = TicketingHook()
    hook.verify_ssl = str(ca)
    assert hook._read_headers(['email'])['X-API-Key'] == 'dedicated-key'


def test_dedicated_key_never_follows_redirect(monkeypatch):
    seen = []
    real_client = httpx.Client
    def handle(request):
        seen.append(request)
        return httpx.Response(302, headers={'Location': 'https://elsewhere.example/'})
    monkeypatch.setattr(httpx, 'Client', lambda **kw: real_client(transport=httpx.MockTransport(handle), **kw))
    with pytest.raises(Exception):
        TicketingHook().search_tickets(system='snow', custom_field_keys=['email'])
    assert len(seen) == 1

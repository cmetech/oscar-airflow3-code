"""Airflow is not importable on the host; assert the DAG file compiles and
declares the expected task ids and no forbidden constants."""
import ast
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

DAG = Path(__file__).resolve().parents[2] / "dags" / "splunk_closed_loop.py"
TEMPLATE_DIR = Path(__file__).resolve().parents[3] / "oscar" / "conf" / "templates" / "jinja2"


def test_dag_compiles_and_declares_tasks():
    src = DAG.read_text()
    ast.parse(src)
    for tid in ("await_incident", "check_sdp", "check_occ", "check_site", "check_craigcool", "decide", "transition_ticket", "notify"):
        assert f'task_id="{tid}"' in src or f"task_id='{tid}'" in src
    assert 'dag_id="splunk_closed_loop"' in src


def test_no_inline_thresholds_or_hosts():
    src = DAG.read_text()
    for forbidden in ("0.9990", "0.97", "10.202.11.105", "172.30.1.108", "@ericsson.com"):
        assert forbidden not in src, forbidden


def test_no_sleep_calls():
    src = DAG.read_text()
    assert "time.sleep(" not in src.replace("sleep=time.sleep", "")


def test_dag_uses_templates_not_inline_body_strings():
    src = DAG.read_text()
    for tpl in ("att_splunk_cl_created.j2", "att_splunk_cl_verdict.j2", "att_splunk_cl_failure.j2"):
        assert tpl in src, tpl
    assert '"<br>".join(' not in src


def test_no_direct_step_status_indexing():
    """Regression guard (fix round 2, finding 1): the worklog-only fail-path step has no
    'status' key at all — indexing it directly is a guaranteed KeyError."""
    src = DAG.read_text()
    assert 'step["status"]' not in src
    assert "step['status']" not in src


def test_notify_guards_missing_verdict():
    """Regression guard (fix round 2, finding 2)."""
    src = DAG.read_text()
    assert "raw_verdict" in src


def test_transition_guards_unconfirmable_ref():
    """Regression guard (fix round 2, finding 3)."""
    src = DAG.read_text()
    assert "if not ref:" in src


def test_audit_reads_use_the_working_ticketing_audit_route():
    """Fix wave finding 1: TicketingHook.list_ticketing_audit builds
    /api/v1/tickets/ticketing-audit, which 404s upstream. Reads must go straight to
    the working /api/v1/ticketing-audit/ route (trailing slash required)."""
    src = DAG.read_text()
    assert "/api/v1/ticketing-audit/" in src
    assert "f\"{base.protocol}://{base.host}:{base.port}/api/v1/tickets/ticketing-audit\"" not in src
    assert "TicketingHook().list_ticketing_audit" not in src


def test_audit_reads_filter_by_fingerprint_both_server_and_client_side():
    """Fix wave finding 2: taskmanager silently drops an unsupported alert_fingerprint
    param, so the client must never trust the server-side filter alone."""
    src = DAG.read_text()
    assert "filter_model" in src
    assert 'perPage' in src
    assert 'r.get("alert_fingerprint") == cfg.fingerprint' in src


def test_marker_post_uses_trailing_slash_and_follows_redirects():
    """Fix wave finding 6: a marker POST without the trailing slash 307s, and
    raise_for_status() does not raise on a 3xx, so the marker silently vanishes."""
    src = DAG.read_text()
    assert "/api/v1/ticketing-audit/\", json=payload" in src
    assert "follow_redirects=True" in src


def _env():
    return Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=False, keep_trailing_newline=True)


def test_render_created_template():
    out = _env().get_template("att_splunk_cl_created.j2").render(
        incident_number="INC0012345", kind="Aggregate", alarm_block="Alert Severity: critical\nNode: node1")
    assert "INC0012345" in out
    assert "Aggregate KPI degraded alarm" in out
    assert "Alert Severity: critical" in out


def test_render_verdict_template():
    check_lines = "Craigcool check : SUCCESSFUL<br>Splunk Site check : SUCCESSFUL<br>Splunk OCC check : SUCCESSFUL<br>SDP success rate: SUCCESSFUL"
    out = _env().get_template("att_splunk_cl_verdict.j2").render(
        timestamp="2026-08-24T00:00:00+00:00", summary="KPI degraded", incident_number="INC0012345",
        ticket_status="Resolved", check_lines=check_lines)
    assert "Ticket ID: INC0012345" in out
    assert "Ticket Status: Resolved" in out
    assert "Craigcool check : SUCCESSFUL" in out
    assert "Splunk Site check : SUCCESSFUL" in out
    assert "Splunk OCC check : SUCCESSFUL" in out
    assert "SDP success rate: SUCCESSFUL" in out
    assert "<br>" in out


def test_render_failure_template():
    out = _env().get_template("att_splunk_cl_failure.j2").render(
        reason="transition failed", incident_number="INC0012345", ticket_ref="ref-1",
        fingerprint="fp1", detail="transition Resolved not confirmed")
    assert "[ACTION NEEDED] Splunk closed loop — transition failed" in out
    assert "Ticket: INC0012345" in out
    assert "Fingerprint: fp1" in out
    assert "Detail: transition Resolved not confirmed" in out


def test_render_failure_template_falls_back_to_ticket_ref():
    out = _env().get_template("att_splunk_cl_failure.j2").render(
        reason="no INC", incident_number=None, ticket_ref="ref-1", fingerprint="fp1", detail="timeout")
    assert "Ticket: ref-1" in out

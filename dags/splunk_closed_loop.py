"""Splunk Gy/Sy/Ro KPI closed loop.

Triggered by task att:splunk_closed_loop (conf = {alert, config}). The platform
has already created/reused the ESB ticket; this DAG waits for the INC, runs the
four health checks, decides on typed booleans, drives the ticket transitions
through the middleware ESB handler, emails, and pushes metrics.

Autoclose marker convention: the "Closed" transition step posts a
ticketing-audit marker row via _post_marker() (operation_type="close",
status_details containing "splunk_closed_loop:autoclose") rather than relying
on the ESB update's own work_info_notes surviving into status_details — this
keeps the recurrence/storm guards' audit reads (audit.count_autocloses /
audit.count_markers) independent of what the ESB handler chooses to persist
from the ticket update payload.

Spec: oscar/docs/superpowers/specs/2026-08-24-splunk-closed-loop-design.md
"""
import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx
import pendulum
from airflow import DAG
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from jinja2 import Environment, FileSystemLoader
from hooks.notify_hook import NotifyHook
from hooks.oscar_hook import OscarHook
from hooks.prometheus_metrics_hook import PrometheusMetricsHook
from hooks.ticketing_hook import TicketingHook
from hooks.worklog_hook import WorkLogHook

from splunk_closed_loop_lib import audit, checks, config, kpi, splunk_client, transitions, verdict

logger = logging.getLogger(__name__)

default_args = {"owner": "airflow", "depends_on_past": False,
                "start_date": pendulum.datetime(2026, 8, 1, tz="UTC"), "retries": 0}

METRIC = "splunk_cl"
TEMPLATE_DIR = os.environ.get("OSCAR_TEMPLATE_DIR", "/opt/airflow/templates")


def _cfg(context) -> config.RunConfig:
    return config.load(context["dag_run"].conf or {})


def _worklog(context, cfg: config.RunConfig) -> WorkLogHook:
    wl_id = context["ti"].xcom_pull(key="worklog_id", task_ids="await_incident")
    hook = WorkLogHook(worklog_id=wl_id) if wl_id else WorkLogHook()
    if not wl_id:
        res = hook.create_worklog(name=f"Splunk closed loop {cfg.kpi_type} {cfg.fingerprint}",
                                  description="Splunk Gy/Sy/Ro KPI validation",
                                  metadata=[{"key": "fingerprint", "value": cfg.fingerprint},
                                            {"key": "splunk_kpi_type", "value": cfg.kpi_type},
                                            {"key": "ticket_ref", "value": cfg.ticket_ref or ""},
                                            {"key": "automation", "value": "splunk_closed_loop"}])
        context["ti"].xcom_push(key="worklog_id", value=res.get("id") or res.get("worklog_id"))
    return hook


def _render(template_name: str, **ctx) -> str:
    """Render an OSCAR notifier template. A missing/broken template must not abort a
    run whose ticket work already succeeded — log loudly and fall back to plain text."""
    try:
        env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=False, keep_trailing_newline=True)
        return env.get_template(template_name).render(**ctx)
    except Exception as exc:  # noqa: BLE001
        logger.error("template %s failed to render: %s", template_name, exc)
        return "\n".join(f"{k}: {v}" for k, v in ctx.items())


def _audit_rows(cfg: config.RunConfig):
    """Read ticketing-audit rows directly (bypasses TicketingHook.list_ticketing_audit,
    which builds /api/v1/tickets/ticketing-audit — a route that 404s upstream; the working
    route is /api/v1/ticketing-audit/, trailing slash required or httpx will not follow the
    307). The server-side alert_fingerprint filter is unsupported by taskmanager's list
    handler (silently discarded), so filter_model is sent AND the result is filtered
    client-side — never trust the server filter alone here."""
    base = OscarHook()
    filter_model = json.dumps({"items": [{"field": "alert_fingerprint", "operator": "equals",
                                          "value": cfg.fingerprint}], "logicOperator": "and"})
    params = {"filter_model": filter_model, "sort": "created_at", "order": "desc", "perPage": 200}
    with httpx.Client(verify=base.verify_ssl, timeout=30.0, follow_redirects=True) as client:
        response = client.get(f"{base.protocol}://{base.host}:{base.port}/api/v1/ticketing-audit/",
                              params=params, headers={"X-Internal-Service": "airflow"})
        response.raise_for_status()
        res = response.json()
    rows = res.get("items") or res.get("records") or res.get("data") or []
    return [r for r in rows if r.get("alert_fingerprint") == cfg.fingerprint]


def _metric(name: str, labels: dict, value=1):
    try:
        PrometheusMetricsHook().send_counter_metric(f"{METRIC}_{name}", value, labels)
    except Exception as exc:  # noqa: BLE001 — metrics never fail the run
        logger.warning("metric %s failed: %s", name, exc)


def _post_marker(cfg: config.RunConfig, context, inc: str, operation_type: str, details: str, suffix: str) -> None:
    """Guard-marker audit row (never awaited). See plan Task 7 'VERIFIED audit contract'."""
    try:
        base = OscarHook()
        run_id = context["dag_run"].run_id
        payload = {"alert_id": str(cfg.alert.get("id") or cfg.fingerprint), "alert_fingerprint": cfg.fingerprint,
                   "incident_number": inc, "ticket_system": "esb", "operation_type": operation_type,
                   "status": "success", "status_details": details, "trace_id": f"{run_id}:{suffix}"}
        with httpx.Client(verify=base.verify_ssl, timeout=15.0, follow_redirects=True) as client:
            resp = client.post(f"{base.protocol}://{base.host}:{base.port}/api/v1/ticketing-audit/", json=payload,
                               headers={"X-Internal-Service": "airflow"})
            if not (200 <= resp.status_code < 300):
                logger.warning("audit marker %s got non-2xx status %s: %s", suffix, resp.status_code, resp.text)
            resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001 — markers never fail the run
        logger.warning("audit marker %s failed: %s", suffix, exc)


def _email(cfg: config.RunConfig, recipients: str, subject: str, body: str, attachments=None):
    payload = {"name": cfg.email.notifier, "subject": subject, "message": body, "recipients": recipients}
    if attachments:
        payload["attachments"] = [{"filename": fn, "content": base64.b64encode(txt.encode()).decode(), "content_type": "text/plain"}
                                  for fn, txt in attachments]
    NotifyHook().send_notification(payload)


def await_incident(**context):
    cfg = _cfg(context)
    wl = _worklog(context, cfg)
    wl.info(f"await_incident: fingerprint={cfg.fingerprint} ticket_ref={cfg.ticket_ref} kpi={cfg.kpi_type}")
    try:
        row = audit.wait_for(lambda: _audit_rows(cfg), lambda rows: audit.find_create(rows, cfg.ticket_ref),
                             cfg.inc_wait.deadline_s, cfg.inc_wait.poll_s)
    except TimeoutError as exc:
        wl.error(str(exc)); _metric("runs_total", {"kpi_type": cfg.kpi_type, "outcome": "error"})
        body = _render("att_splunk_cl_failure.j2", reason="no INC", incident_number=None,
                       ticket_ref=cfg.ticket_ref, fingerprint=cfg.fingerprint, detail=str(exc))
        _email(cfg, cfg.email.verdict_list, f"[ACTION NEEDED] Splunk closed loop — no INC — {cfg.ticket_ref}", body)
        raise
    if row.get("status") != "success" and not row.get("incident_number"):
        msg = f"ticket create {row.get('status')}: {row.get('status_details') or row.get('error_message')}"
        wl.error(msg); _metric("runs_total", {"kpi_type": cfg.kpi_type, "outcome": "error"})
        body = _render("att_splunk_cl_failure.j2", reason="ticket create failed", incident_number=None,
                       ticket_ref=cfg.ticket_ref, fingerprint=cfg.fingerprint, detail=msg)
        _email(cfg, cfg.email.verdict_list, f"[ACTION NEEDED] Splunk closed loop — ticket create failed — {cfg.ticket_ref}", body)
        raise RuntimeError(msg)
    inc = row["incident_number"]
    reused = row.get("status") == "suppressed"
    rows = _audit_rows(cfg)
    if cfg.storm_min_interval_s and audit.count_markers(rows, "splunk_closed_loop:verdict", cfg.storm_min_interval_s, datetime.now(timezone.utc)):
        wl.warning("storm guard: verdict already produced inside min_interval — suppressed")
        _metric("runs_total", {"kpi_type": cfg.kpi_type, "outcome": "suppressed"})
        raise RuntimeError("storm guard suppressed")
    if reused and not cfg.revalidate_on_reused_ticket:
        TicketingHook().update_ticket(cfg.ticket_ref, {"name": "splunk_closed_loop", "incident_number": inc,
                                                        "fingerprint": cfg.fingerprint,
                                                        "work_info_notes": "Alarm repeated; ticket reused, validation skipped by config"}, system="esb")
        _metric("runs_total", {"kpi_type": cfg.kpi_type, "outcome": "suppressed"})
        raise RuntimeError("reused ticket, revalidate disabled")
    prior = audit.count_autocloses(rows, cfg.recurrence_window_s, datetime.now(timezone.utc)) if cfg.recurrence_window_s else 0
    k = kpi.parse(cfg.kpi_type)
    kind = "Aggregate" if k.kind == "aggregate" else "Per-Entity"
    labels = cfg.alert.get("labels") or {}
    alarm_block = "\n".join([f"Alert Severity: {labels.get('severity', '')}", f"Node: {labels.get('node', '')}",
                             f"Node Source IP: {labels.get('source_ip', labels.get('node_alias', ''))}",
                             f"Alarm Summary: {labels.get('summary', '')}", "Alarm acknowledged by : ",
                             "Front Office Notes : ", f"Last Occurrence: {cfg.alert.get('startsAt', '')}",
                             f"Tally: {labels.get('tally', '1')}"])
    body = _render("att_splunk_cl_created.j2", incident_number=inc, kind=kind, alarm_block=alarm_block)
    _email(cfg, cfg.email.created_list, f"Ticket {inc} has been created for the Splunk {kind} KPI degraded alarm", body)
    wl.info(f"INC {inc} (reused={reused}); prior autocloses in window={prior}")
    context["ti"].xcom_push(key="incident_number", value=inc)
    context["ti"].xcom_push(key="prior_autocloses", value=prior)


def _check(name):
    def run(**context):
        cfg = _cfg(context)
        wl = _worklog(context, cfg)
        t0 = time.monotonic()
        try:
            if name == "craigcool":
                from airflow.providers.mysql.hooks.mysql import MySqlHook  # provider import kept local: only this task needs it
                hook = MySqlHook(mysql_conn_id=cfg.nmcdb.conn_id)
                since_dt = checks.craigcool_since(cfg.nmcdb.window, datetime.now(timezone.utc))
                since = since_dt.strftime("%Y-%m-%d %H:%M:%S")
                count = hook.get_first("SELECT COUNT(*) FROM nmcdb.jb_clientlink_exception_h WHERE dt > %s", parameters=(since,))[0]
                result = checks.eval_craigcool(int(count), cfg.thresholds.link_drops_max)
            else:
                k = kpi.parse(cfg.kpi_type)
                names = kpi.search_names(k, cfg.splunk.searches)
                search = {"sdp": names["sdp"], "occ": names["occ_success"], "site": names["site_result"]}[name]
                conn = BaseHook.get_connection(cfg.splunk.conn_id)
                token = conn.password
                base_url = splunk_client.base_url_from_conn(conn.host, conn.schema, conn.port, cfg.splunk.base_url)
                csv_text = splunk_client.export_csv(base_url, cfg.splunk.app_path, token, search,
                                                    cfg.splunk.timeout_s, cfg.splunk.verify_tls)
                result = {"sdp": lambda: checks.eval_sdp(csv_text, cfg.thresholds.sdp_success_min),
                          "occ": lambda: checks.eval_occ(csv_text, cfg.thresholds.occ_success_min),
                          "site": lambda: checks.eval_site(csv_text, cfg.thresholds.site_sd_error_max,
                                                           kpi.site_sd_error_index(k))}[name]()
        except Exception as exc:  # noqa: BLE001 — a failed check is a FAIL verdict, never a crash
            result = checks.errored(name, f"{type(exc).__name__}: {exc}")
        dur = time.monotonic() - t0
        (wl.info if result.ok else wl.error)(f"check {name}: ok={result.ok} value={result.value} error={result.error} rows={len(result.rows)}")
        _metric("check_result", {"check": name, "ok": str(result.ok).lower()})
        try:
            PrometheusMetricsHook().send_gauge_metric(f"{METRIC}_check_duration_seconds", dur, {"check": name})
        except Exception:  # noqa: BLE001
            pass
        context["ti"].xcom_push(key="result", value=result.__dict__)
    return run


def decide(**context):
    cfg = _cfg(context)
    wl = _worklog(context, cfg)
    ti = context["ti"]
    results = {n: checks.CheckResult(**ti.xcom_pull(key="result", task_ids=f"check_{n}")) for n in ("sdp", "occ", "site", "craigcool")}
    prior = ti.xcom_pull(key="prior_autocloses", task_ids="await_incident") or 0
    v = verdict.decide(results, cfg.sdp_gates_verdict, prior, cfg.recurrence_max_autoclose)
    wl.info(f"verdict passed={v.passed} reason={v.reason}")
    inc = ti.xcom_pull(key="incident_number", task_ids="await_incident")
    _post_marker(cfg, context, inc, "update", f"splunk_closed_loop:verdict:{'pass' if v.passed else 'fail'}", "verdict")
    ti.xcom_push(key="verdict", value=v.__dict__)


def transition_ticket(**context):
    cfg = _cfg(context)
    wl = _worklog(context, cfg)
    ti = context["ti"]
    v = verdict.Verdict(**ti.xcom_pull(key="verdict", task_ids="decide"))
    inc = ti.xcom_pull(key="incident_number", task_ids="await_incident")
    steps = transitions.plan(v.passed, v.worklogs, v.email_body, cfg.fail_path_resolves, inc, cfg.fingerprint,
                             cfg.ticket.customer_name, cfg.ticket.impact)
    hook = TicketingHook()
    for i, step in enumerate(steps):
        label = step.get("status") or "worklog-only"
        res = hook.update_ticket(cfg.ticket_ref, step, system="esb")
        ref = res.get("ref") or res.get("id")
        wl.info(f"transition {i+1}/{len(steps)} -> {label} ref={ref}")
        if not ref:
            msg = f"ticket update for status {step.get('status') or 'worklog-only'} returned no correlation ref; cannot confirm the transition"
            wl.error(msg)
            _metric("transition_total", {"status": step.get("status") or "worklog_only", "ok": "false"})
            body = _render("att_splunk_cl_failure.j2", reason="transition unconfirmable", incident_number=inc,
                           ticket_ref=cfg.ticket_ref, fingerprint=cfg.fingerprint, detail=msg)
            _email(cfg, cfg.email.verdict_list, f"[ACTION NEEDED] Splunk closed loop — transition unconfirmable — {inc}", body)
            raise RuntimeError(msg)
        try:
            row = audit.wait_for(lambda: _audit_rows(cfg), lambda rows: audit.find_update(rows, ref),
                                 cfg.inc_wait.deadline_s, cfg.inc_wait.poll_s)
        except TimeoutError as exc:
            wl.error(f"transition {label} not confirmed: {exc}")
            _metric("transition_total", {"status": step.get("status") or "worklog_only", "ok": "false"})
            body = _render("att_splunk_cl_failure.j2", reason="transition failed", incident_number=inc,
                           ticket_ref=cfg.ticket_ref, fingerprint=cfg.fingerprint, detail=str(exc))
            _email(cfg, cfg.email.verdict_list, f"[ACTION NEEDED] Splunk closed loop — transition failed — {inc}", body)
            raise
        ok = row.get("status") == "success"
        _metric("transition_total", {"status": step.get("status") or "worklog_only", "ok": str(ok).lower()})
        if not ok:
            msg = f"transition {label} failed: {row.get('status_details') or row.get('error_message')}"
            wl.error(msg)
            body = _render("att_splunk_cl_failure.j2", reason="transition failed", incident_number=inc,
                           ticket_ref=cfg.ticket_ref, fingerprint=cfg.fingerprint, detail=msg)
            _email(cfg, cfg.email.verdict_list, f"[ACTION NEEDED] Splunk closed loop — transition failed — {inc}", body)
            raise RuntimeError(msg)
        if step.get("status") == "Closed":
            _post_marker(cfg, context, inc, "close", "splunk_closed_loop:autoclose", "autoclose")


def notify(**context):
    cfg = _cfg(context)
    wl = _worklog(context, cfg)
    ti = context["ti"]
    inc = ti.xcom_pull(key="incident_number", task_ids="await_incident")
    raw_verdict = ti.xcom_pull(key="verdict", task_ids="decide")
    if not raw_verdict:
        wl.error("no verdict produced — the run failed before the decision stage; see the failing task's log")
        body = _render("att_splunk_cl_failure.j2", reason="run ended before a verdict was produced",
                       incident_number=inc, ticket_ref=cfg.ticket_ref, fingerprint=cfg.fingerprint,
                       detail="decide did not run; the ticket was left untouched by this run")
        _email(cfg, cfg.email.verdict_list, f"[ACTION NEEDED] Splunk closed loop — no verdict — {inc or cfg.ticket_ref}", body)
        _metric("runs_total", {"kpi_type": cfg.kpi_type, "outcome": "error"})
        wl.close_worklog()
        return
    v = verdict.Verdict(**raw_verdict)
    labels = cfg.alert.get("labels") or {}
    # Enable's B21 stamps `current_date` as CST "yyyy-MM-dd HH:mm:ss" (its Calendar
    # arithmetic converts into America/Chicago), not a UTC ISO string.
    stamp = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d %H:%M:%S")
    body = _render("att_splunk_cl_verdict.j2", timestamp=stamp,
                   summary=labels.get("summary", ""), incident_number=inc,
                   ticket_status="Resolved" if v.passed else "Assigned", check_lines=v.email_body)
    attachments = []
    for n in ("sdp", "occ", "site", "craigcool"):
        r = ti.xcom_pull(key="result", task_ids=f"check_{n}") or {}
        attachments.append((f"{n}_output.txt", r.get("raw") or f"error: {r.get('error')}"))
    _email(cfg, cfg.email.verdict_list, v.subject, body, attachments)
    _metric("runs_total", {"kpi_type": cfg.kpi_type, "outcome": "pass" if v.passed else "fail"})
    wl.info("notified; closing worklog")
    wl.close_worklog()


with DAG(dag_id="splunk_closed_loop", default_args=default_args, schedule=None, catchup=False,
         max_active_runs=4, tags=["att", "splunk", "closed-loop"]) as dag:
    t_await = PythonOperator(task_id="await_incident", python_callable=await_incident)
    t_check_sdp = PythonOperator(task_id="check_sdp", python_callable=_check("sdp"))
    t_check_occ = PythonOperator(task_id="check_occ", python_callable=_check("occ"))
    t_check_site = PythonOperator(task_id="check_site", python_callable=_check("site"))
    t_check_craigcool = PythonOperator(task_id="check_craigcool", python_callable=_check("craigcool"))
    t_checks = [t_check_sdp, t_check_occ, t_check_site, t_check_craigcool]
    t_decide = PythonOperator(task_id="decide", python_callable=decide)
    t_transition = PythonOperator(task_id="transition_ticket", python_callable=transition_ticket)
    t_notify = PythonOperator(task_id="notify", python_callable=notify, trigger_rule="all_done")
    t_await >> t_checks >> t_decide >> t_transition >> t_notify

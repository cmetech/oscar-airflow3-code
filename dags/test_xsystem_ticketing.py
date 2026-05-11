"""
test_xsystem_ticketing
======================
Test DAG for validating the XSystem ticketing integration via the OSCAR middleware
ticketing API.  Every call goes through:

    DAG task  →  middleware POST/PATCH/GET /api/v1/tickets?system=remedy
              →  ticketing.py handler registry
              →  RemedyHandler   (TICKETING_REMEDY_OVERRIDE_XSYS is false / absent)
              →  XSystemHandler  (TICKETING_REMEDY_OVERRIDE_XSYS=true)

The DAG deliberately uses system=remedy to prove the override flag works end-to-end.

Tasks (sequential):
  0. open_worklog           – create a single shared worklog for the entire DAG run
  1. check_routing          – inspect env var, log which handler will be used
  2. create_ticket          – POST  /api/v1/tickets?system=remedy
  3. get_ticket             – GET   /api/v1/tickets/{id}?system=remedy
  4. update_severity        – PATCH /api/v1/tickets/{id}?system=remedy  (severity + description comment)
  5. add_resolution_comment – PATCH /api/v1/tickets/{id}?system=remedy  (add_comment)
  6. verify_final_state     – GET   /api/v1/tickets/{id}?system=remedy  (confirm all changes)
  7. close_worklog          – close the shared worklog

Ticket ID is passed between tasks via XCom.
worklog_id is passed from open_worklog to all subsequent tasks via XCom.
"""

import json
import os
import uuid
import requests
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

from hooks.worklog_hook import WorkLogHook, WorkLogType  # type: ignore
from hooks.oscar_hook import OscarHook  # type: ignore

# ── DAG defaults ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

TICKETING_SYSTEM = "remedy"   # intentionally "remedy" — override routes to XSystem when flag is set
INTERNAL_HEADER = {"X-Internal-Service": "airflow", "Content-Type": "application/json"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _base_url() -> str:
    hook = OscarHook()
    return f"{hook.protocol}://{hook.host}:{hook.port}/api/v1"


def _tickets_url(ticket_id: str = "") -> str:
    base = f"{_base_url()}/tickets"
    return f"{base}/{ticket_id}" if ticket_id else base


def _ssl_verify() -> bool:
    return os.environ.get("SSL_VERIFY", "false").lower() == "true"


def _get_hook(context) -> WorkLogHook:
    """Pull worklog_id from XCom and return a WorkLogHook bound to that worklog."""
    worklog_id = context["ti"].xcom_pull(task_ids="open_worklog", key="worklog_id")
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    return hook


def _log_request(hook: WorkLogHook, method: str, url: str, payload: dict = None):
    hook.info(f"REQUEST  {method} {url}")
    if payload:
        hook.info(f"PAYLOAD  {json.dumps(payload, indent=2)}")


def _log_response(hook: WorkLogHook, resp: requests.Response):
    hook.info(f"RESPONSE HTTP {resp.status_code}")
    try:
        hook.info(f"BODY     {json.dumps(resp.json(), indent=2)}")
    except Exception:
        hook.info(f"BODY     {resp.text[:2000]}")


# ── Task 0: open_worklog ──────────────────────────────────────────────────────

def open_worklog(**context):
    """Create a single worklog for the entire DAG run and push its ID to XCom."""
    hook = WorkLogHook()

    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"XSYS-TEST-{uuid.uuid4().hex[:8].upper()}"

    metadata = [
        {"key": "run_id", "value": run_id},
        {"key": "ticketing_system", "value": TICKETING_SYSTEM},
        {"key": "handler_override_env", "value": os.environ.get("TICKETING_REMEDY_OVERRIDE_XSYS", "(not set)")},
    ]

    worklog = hook.create_worklog(
        name="[XSYS TEST] XSystem Ticketing Integration Test",
        description=(
            f"End-to-end test of XSystem ticketing via middleware ticketing API "
            f"(system={TICKETING_SYSTEM}). Run: {run_id}"
        ),
        worklog_type=WorkLogType.DB,
        metadata=metadata,
    )

    worklog_id = worklog.get("id") if isinstance(worklog, dict) else None
    hook.info(f"Worklog opened — ID: {worklog_id}")
    hook.info(f"DAG run: {run_id}")
    hook.info(f"Middleware base URL: {_base_url()}")

    context["ti"].xcom_push(key="worklog_id", value=worklog_id)
    return worklog_id


# ── Task 1: check_routing ─────────────────────────────────────────────────────

def check_routing(**context):
    hook = _get_hook(context)

    override = os.environ.get("TICKETING_REMEDY_OVERRIDE_XSYS", "").lower()
    is_xsys = override in ("true", "1", "yes")

    hook.info("=" * 60)
    hook.info("TICKETING ROUTING CHECK")
    hook.info("=" * 60)
    hook.info(f"system param sent in all requests : {TICKETING_SYSTEM!r}")
    hook.info(f"TICKETING_REMEDY_OVERRIDE_XSYS    : {override!r}")
    hook.info(f"Active handler                    : {'XSystemHandler' if is_xsys else 'RemedyHandler'}")
    hook.info("")

    if is_xsys:
        hook.info("XSystem override is ACTIVE — calls to system=remedy will hit XSystemHandler")
    else:
        hook.warning("XSystem override is NOT set — calls will go to RemedyHandler")
        hook.warning("  Set TICKETING_REMEDY_OVERRIDE_XSYS=true in middleware env to test XSystem")

    hook.info("=" * 60)

    context["ti"].xcom_push(key="is_xsystem", value=is_xsys)


# ── Task 2: create_ticket ────────────────────────────────────────────────────

def create_ticket(**context):
    hook = _get_hook(context)

    run_id = f"XSYS-TEST-{uuid.uuid4().hex[:8].upper()}"
    payload = {
        "name": f"XSystem Integration Test — {run_id}",
        "summary": f"ALERT INCIDENT REPORT: XSystem test alert triggered by DAG run {run_id}",
        "description": "This is an automated test ticket from the OSCAR xsystem test DAG.",
        "detailed_description": (
            "Test incident raised to validate the XSystem ticketing integration.\n\n"
            f"Run ID: {run_id}\n"
            "Alert Name: XSystemIntegrationTest\n"
            "Severity: critical\n"
            "Environment: production\n"
            "Node: test-node-01.example.com"
        ),
        "severity": "critical",
        "environment": "production",
        "support_group": "AS_T2_OMON_OSCAR",
        "submitter": "OSCARUser1",
        "node_name": "test-node-01.example.com",
        "instance": "10.0.0.1",
        "datacenter": "polaris",
        "alertgroup": "LinuxInfra",
    }

    url = f"{_tickets_url()}?system={TICKETING_SYSTEM}"
    _log_request(hook, "POST", url, payload)

    resp = requests.post(url, json=payload, headers=INTERNAL_HEADER, verify=_ssl_verify(), timeout=30)
    _log_response(hook, resp)
    resp.raise_for_status()

    data = resp.json()
    ticket_id = data.get("id") or data.get("incident_number") or data.get("incident_id")

    hook.info("Ticket created successfully")
    hook.info(f"  ticket_id       : {ticket_id}")
    hook.info(f"  incident_number : {data.get('incident_number')}")
    hook.info(f"  status          : {data.get('status')}")
    hook.info(f"  impact          : {data.get('impact')}")
    hook.info(f"  urgency         : {data.get('urgency')}")

    context["ti"].xcom_push(key="ticket_id", value=ticket_id)


# ── Task 3: get_ticket ───────────────────────────────────────────────────────

def get_ticket(**context):
    hook = _get_hook(context)

    ticket_id = context["ti"].xcom_pull(task_ids="create_ticket", key="ticket_id")
    if not ticket_id:
        hook.error("No ticket_id from create_ticket — cannot continue")
        raise ValueError("ticket_id missing from XCom")

    url = f"{_tickets_url(ticket_id)}?system={TICKETING_SYSTEM}"
    _log_request(hook, "GET", url)

    resp = requests.get(url, headers=INTERNAL_HEADER, verify=_ssl_verify(), timeout=30)
    _log_response(hook, resp)
    resp.raise_for_status()

    data = resp.json()
    hook.info("Ticket fetched successfully")
    hook.info(f"  id              : {data.get('id')}")
    hook.info(f"  incident_number : {data.get('incident_number')}")
    hook.info(f"  status          : {data.get('status')}")
    hook.info(f"  impact          : {data.get('impact')}")
    hook.info(f"  urgency         : {data.get('urgency')}")
    hook.info(f"  assignee        : {data.get('assignee')}")
    hook.info(f"  assigned_group  : {data.get('assigned_group')}")
    hook.info(f"  environment     : {data.get('environment')}")


# ── Task 4: update_severity ──────────────────────────────────────────────────

def update_severity(**context):
    hook = _get_hook(context)

    ticket_id = context["ti"].xcom_pull(task_ids="create_ticket", key="ticket_id")
    if not ticket_id:
        hook.error("No ticket_id — cannot update severity")
        raise ValueError("ticket_id missing from XCom")

    # Mirror exactly what the alert flow sends via _update_ticket_severity
    # severity drives impact/urgency mapping in XSystemHandler._map_to()
    payload = {
        "name": "testticket",
        "severity": "major",
        "description": "Severity updated to major — alert condition has stabilised from critical.",
        "submitter": "OSCARUser1",
        "support_group": "AS_T2_OMON_OSCAR",
    }

    url = f"{_tickets_url(ticket_id)}?system={TICKETING_SYSTEM}"
    _log_request(hook, "PATCH", url, payload)

    resp = requests.patch(url, json=payload, headers=INTERNAL_HEADER, verify=_ssl_verify(), timeout=30)
    _log_response(hook, resp)
    resp.raise_for_status()

    data = resp.json()
    hook.info("Severity updated successfully")
    hook.info(f"  impact  (expected High)  : {data.get('impact')}")
    hook.info(f"  urgency (expected Major) : {data.get('urgency')}")
    hook.info(f"  status                   : {data.get('status')}")


# ── Task 5: add_resolution_comment ───────────────────────────────────────────

def add_resolution_comment(**context):
    hook = _get_hook(context)

    ticket_id = context["ti"].xcom_pull(task_ids="create_ticket", key="ticket_id")
    if not ticket_id:
        hook.error("No ticket_id — cannot add resolution comment")
        raise ValueError("ticket_id missing from XCom")

    # Mirror exactly what the alert flow sends via _add_resolution_comment
    payload = {
        "name": "testticket",
        "add_comment": (
            "ALERT RESOLVED — condition cleared.\n\n"
            "Resolution: The alert condition has been automatically resolved. "
            "Disk utilization returned below threshold. No further action required."
        ),
        "submitter": "OSCARUser1",
    }

    url = f"{_tickets_url(ticket_id)}?system={TICKETING_SYSTEM}"
    _log_request(hook, "PATCH", url, payload)

    resp = requests.patch(url, json=payload, headers=INTERNAL_HEADER, verify=_ssl_verify(), timeout=30)
    _log_response(hook, resp)
    resp.raise_for_status()

    hook.info("Resolution comment added successfully")


# ── Task 6: verify_final_state ───────────────────────────────────────────────

def verify_final_state(**context):
    hook = _get_hook(context)

    ticket_id = context["ti"].xcom_pull(task_ids="create_ticket", key="ticket_id")
    is_xsystem = context["ti"].xcom_pull(task_ids="check_routing", key="is_xsystem")

    if not ticket_id:
        hook.error("No ticket_id — cannot verify")
        raise ValueError("ticket_id missing from XCom")

    url = f"{_tickets_url(ticket_id)}?system={TICKETING_SYSTEM}"
    _log_request(hook, "GET", url)

    resp = requests.get(url, headers=INTERNAL_HEADER, verify=_ssl_verify(), timeout=30)
    _log_response(hook, resp)
    resp.raise_for_status()

    data = resp.json()

    hook.info("=" * 60)
    hook.info("FINAL STATE VERIFICATION")
    hook.info("=" * 60)
    hook.info(f"  Handler used    : {'XSystemHandler' if is_xsystem else 'RemedyHandler'}")
    hook.info(f"  ticket_id       : {ticket_id}")
    hook.info(f"  incident_number : {data.get('incident_number')}")
    hook.info(f"  status          : {data.get('status')}")
    hook.info(f"  impact          : {data.get('impact')}  (expected: High after severity=major update)")
    hook.info(f"  urgency         : {data.get('urgency')}  (expected: Major after severity=major update)")
    hook.info(f"  assignee        : {data.get('assignee')}")
    hook.info(f"  assigned_group  : {data.get('assigned_group')}")
    hook.info(f"  environment     : {data.get('environment')}")
    hook.info("=" * 60)

    # Assertions — fail the task loudly if something is wrong
    errors = []
    if is_xsystem:
        if data.get("impact") != "High":
            errors.append(f"impact mismatch: expected 'High', got {data.get('impact')!r}")
        if data.get("urgency") != "Major":
            errors.append(f"urgency mismatch: expected 'Major', got {data.get('urgency')!r}")

    if errors:
        for e in errors:
            hook.error(f"ASSERTION FAILED: {e}")
        raise AssertionError(f"Verification failed: {errors}")

    hook.info("All assertions passed — XSystem integration is working correctly")


# ── Task 7: close_worklog ─────────────────────────────────────────────────────

def close_worklog(**context):
    """Close the shared worklog created in open_worklog."""
    worklog_id = context["ti"].xcom_pull(task_ids="open_worklog", key="worklog_id")
    hook = WorkLogHook()
    hook.set_worklog_id(worklog_id)
    hook.info(f"All tasks complete — closing worklog ID: {worklog_id}")
    hook.close_worklog()


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="test_xsystem_ticketing",
    default_args=default_args,
    description=(
        "Tests the XSystem ticketing integration end-to-end via middleware. "
        "Uses system=remedy — routes to XSystemHandler when TICKETING_REMEDY_OVERRIDE_XSYS=true."
    ),
    schedule=None,
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=["test", "xsystem", "ticketing", "remedy"],
) as dag:

    t0 = PythonOperator(task_id="open_worklog",           python_callable=open_worklog)
    t1 = PythonOperator(task_id="check_routing",          python_callable=check_routing)
    t2 = PythonOperator(task_id="create_ticket",          python_callable=create_ticket)
    t3 = PythonOperator(task_id="get_ticket",             python_callable=get_ticket)
    t4 = PythonOperator(task_id="update_severity",        python_callable=update_severity)
    t5 = PythonOperator(task_id="add_resolution_comment", python_callable=add_resolution_comment)
    t6 = PythonOperator(task_id="verify_final_state",     python_callable=verify_final_state)
    t7 = PythonOperator(task_id="close_worklog",          python_callable=close_worklog)

    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7

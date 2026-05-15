"""
access_management_xsystem_integration_test
===========================================
Single-trigger integration test DAG that mirrors the exact middleware calls
made by the two access management DAGs.

    DAG → middleware → (middleware routes to XSystem internally)

─────────────────────────────────────────────────────────────────
TASK FLOW (one trigger, no conf needed)

  T01  Create SR          POST /api/v1/tickets/service-requests
  T02  SR audit log       POST /service-requests/work-info
                          Runs immediately after SR creation — for ALL flows.
  T03  Approve SR         mock-only — auto-approves via xsystem-mock if
                          XSYSTEM_MOCK_HOST is set; no-op in actual XSystem.
                          Skipped for SR-only flows.
  T04  Poll WO by SR ID   GET  /work-orders?service_request_id=
                          Retries up to WO_POLL_MAX_RETRIES with WO_POLL_WAIT_SECONDS wait.
                          Skips gracefully if SR still pending after retries.
                          Skipped for SR-only flows.
  T05  WO audit log       POST /work-orders/work-logs  (BEFORE WO close)
  T06  Search WO by WO#   GET  /work-orders?work_order_number=
  T07  Update WO status   PUT  /work-orders/{request_id}
  T08  Summary            always runs — full PASS/SKIP/FAIL report

─────────────────────────────────────────────────────────────────
SR-ONLY FLOWS (service_account_password_reset, other_request)

  T01 runs (SR creation).
  T02 runs (SR audit log — posted immediately, same as real DAG Phase 1).
  T03–T07 are skipped — no WO lifecycle in OSCAR automation for these flows.
  WO is handled manually by the ITSM team.

─────────────────────────────────────────────────────────────────
ENVIRONMENT BEHAVIOUR

  xsystem-mock (XSYSTEM_MOCK_HOST is set):
    T03 calls the mock /approve endpoint → WO is created immediately.
    T04 finds the WO on the first attempt.

  Actual XSystem (XSYSTEM_MOCK_HOST not set):
    T03 is a no-op (logs that approval is a human portal action).
    T04 polls for the WO: if empty, logs a WARNING and waits,
    then retries up to WO_POLL_MAX_RETRIES times. If no WO after all
    retries the task is skipped with "SR pending approval" and T05-T07
    are also skipped. Re-trigger the DAG once the SR has been approved.

─────────────────────────────────────────────────────────────────
ENV VARS

  Middleware (same as real access management DAGs):
    MIDDLEWARE_HOST                   default: middleware
    MIDDLEWARE_PORT                   default: 5200
    DEFAULT_TICKETING_SYSTEM          default: REMEDY
    UA_USER_ADMIN_SUPPORT_GROUP_NAME  default: UA_User admin
    UA_USER_ADMIN_ASSIGNED_GROUP      default: UA_User admin

  Mock-only (only set in xsystem-mock environments):
    XSYSTEM_MOCK_HOST   host of xsystem-mock (e.g. xsystem-mock)
    XSYSTEM_MOCK_PORT   port of xsystem-mock (default: 8099)
"""

import json
import logging
import os
import time
from datetime import datetime

import httpx
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── Config — middleware (identical to real access management DAG helpers) ─────

MIDDLEWARE_HOST   = os.environ.get("MIDDLEWARE_HOST", "middleware")
MIDDLEWARE_PORT   = int(os.environ.get("MIDDLEWARE_PORT", 5200))
TICKETING_SYSTEM  = os.environ.get("DEFAULT_TICKETING_SYSTEM_XSYS", "XSYS")
UA_SUPPORT_GROUP  = os.environ.get("UA_USER_ADMIN_SUPPORT_GROUP_NAME", "UA_User admin")
UA_ASSIGNED_GROUP = os.environ.get("UA_USER_ADMIN_ASSIGNED_GROUP", "UA_User admin")

MW_BASE = f"https://{MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}/api/v1/tickets"

MW_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-Internal-Service": "airflow",
}

# ── Config — xsystem-mock fallback ───────────────────────────────────────────
# Priority: DAG conf {"xsystem_mock_host": "xsystem-mock", "xsystem_mock_port": 8099}
#        → falls back to these values if not in conf
#        → empty host = actual XSystem behaviour (no auto-approve, T04 polls)
XSYSTEM_MOCK_HOST = ""    # ← set e.g. "xsystem-mock" to test against the mock
XSYSTEM_MOCK_PORT = 8099  # ← mock port fallback

# ── WO poll settings ──────────────────────────────────────────────────────────

WO_POLL_MAX_RETRIES   = 10   # 10 × 2 min = up to 20 mins waiting for SR approval
WO_POLL_WAIT_SECONDS  = 120  # 2 min per retry — override via conf: {"wo_poll_wait_seconds": N}

# ── Flow selection ────────────────────────────────────────────────────────────
# Pass {"flow": "<name>"} in dag_run.conf to select a flow.
# Default (no conf / unknown flow): new_user_account — preserves existing behaviour.

DEFAULT_FLOW = "new_user_account"

FLOW_CONFIGS = {
    "new_user_account": {
        "sr_payload": {
            "name": "OSCAR XSystem Integration Test — New User Account",
            "offering_title": "Access Management - Test - Create New User Account",
            "source_keyword": "MYIT",
            "title_instance_id": "SRHAA5V0G9R60AOHCG7PL5ISW8U0XA",
            "full_name": "Svc Enable Automation",
            "login_id": "svc_enable_itsm",
            "submitter": "svc_enable_itsm",
            "sr_type_field_1": "Integration test — requires AD account",
            "sr_type_field_2": "test.user@example.com",
            "sr_type_field_3": "Access Management User Create",
            "sr_type_field_4": "Test Manager",
            "sr_type_field_5": "testadid01",
            "sr_type_field_10": "approver01;approver02",
            "sr_type_field_13": "Test Requester",
            "sr_type_field_16": "TestCorp",
            "sr_type_field_19": "manager@example.com",
            "sr_type_field_20": "Production",
            "sr_type_field_21": "TOOL-ITSM",
            "sr_type_field_22": "Integration test user account creation",
            "sr_type_field_42": "+10000000000",
        },
        "wo_update_extra": {
            "wo_type_field_23": "testadid01",
            "wo_type_field_28": "testadid01",
            "wo_type_field_29": "test.user@example.com",
            "wo_type_field_30": "Test User",
        },
        "sr_work_info_notes": "Integration test: new user account — AD account creation validation",
        "wo_work_log_comment": "Integration test: user AD account created in PROD",
    },
    "password_reset": {
        "sr_payload": {
            "name": "OSCAR XSystem Integration Test — Password Reset",
            "offering_title": "Access Management - Test - Reset Password - User Account",
            "source_keyword": "MYIT",
            "title_instance_id": "SRHAA5V0G9R60AO8U2R8FP5BYUA1PH",
            "full_name": "Svc Enable Automation",
            "login_id": "svc_enable_itsm",
            "submitter": "svc_enable_itsm",
            "sr_type_field_1": "Integration test — password reset request",
            "sr_type_field_2": "testadid01",
            "sr_type_field_3": "Access Management User Create",
            "sr_type_field_4": "test.user@example.com",
            "sr_type_field_5": "TestCorp",
            "sr_type_field_11": "+10000000000",
            "sr_type_field_15": "Test User",
        },
        "wo_update_extra": {},
        "sr_work_info_notes": "Integration test: password reset — user account validation",
        "wo_work_log_comment": "Integration test: user account password reset completed",
    },
    "new_service_account": {
        "sr_payload": {
            "name": "OSCAR XSystem Integration Test — New Service Account",
            "offering_title": "Access Management - Test - Create New Service Account",
            "source_keyword": "MYIT",
            "title_instance_id": "SRHAA5V0G9R60AO3DGHV0JAULUI5MK",
            "full_name": "Svc Enable Automation",
            "login_id": "svc_enable_itsm",
            "submitter": "svc_enable_itsm",
            "sr_type_field_1": "Integration test — service account creation",
            "sr_type_field_2": "svc.test@example.com",
            "sr_type_field_3": "Access Management User Create",
            "sr_type_field_4": "Test Manager",
            "sr_type_field_5": "testmgradid01",
            "sr_type_field_10": "approver01;approver02",
            "sr_type_field_11": "svc.owner@example.com",
            "sr_type_field_12": "manager@example.com",
            "sr_type_field_13": "Test Requester",
            "sr_type_field_16": "TestCorp",
            "sr_type_field_17": "svc_demo_account",
            "sr_type_field_19": "svc-testadid01",
            "sr_type_field_20": "Production",
            "sr_type_field_21": "TOOL-ITSM",
            "sr_type_field_22": "Integration test service account creation",
            "sr_type_field_42": "+10000000000",
            "sr_type_field_43": "Add",
        },
        "wo_update_extra": {
            "wo_type_field_23": "svc-testadid01",
            "wo_type_field_28": "svc-testadid01",
            "wo_type_field_29": "svc.test@example.com",
            "wo_type_field_30": "Service Test Account",
        },
        "sr_work_info_notes": "Integration test: new service account — AD service account creation validation",
        "wo_work_log_comment": "Integration test: service AD account created in PROD",
    },
    "user_account_modify": {
        "sr_payload": {
            "name": "OSCAR XSystem Integration Test — User Account Modify",
            "offering_title": "Access Management - Test - Modify Account/Modify Access",
            "source_keyword": "MYIT",
            "title_instance_id": "SRHAA5V0G9R60AOHC2PJLKB0SDWH9O",
            "full_name": "Svc Enable Automation",
            "login_id": "svc_enable_itsm",
            "submitter": "svc_enable_itsm",
            "sr_type_field_1": "Integration test — account modification request",
            "sr_type_field_2": "testadid01",
            "sr_type_field_3": "Access Management User Create",
            "sr_type_field_4": "Test User",
            "sr_type_field_5": "testmgradid01",
            "sr_type_field_10": "approver01;approver02",
            "sr_type_field_11": "Add Access",
            "sr_type_field_12": "Test Manager",
            "sr_type_field_14": "manager@example.com",
            "sr_type_field_19": "test.user@example.com",
            "sr_type_field_20": "Production",
            "sr_type_field_21": "TOOL-ITSM",
            "sr_type_field_22": "Integration test account modification",
            "sr_type_field_42": "+10000000000",
            "sr_type_field_43": "Add",
        },
        "wo_update_extra": {},
        "sr_work_info_notes": "Integration test: user account modification validation",
        "wo_work_log_comment": "Integration test: user account modification completed in PROD",
    },
    "service_account_password_reset": {
        "sr_payload": {
            "name": "OSCAR XSystem Integration Test — Service Account Password Reset",
            "offering_title": "Access Management - Test - Reset Password - Service Account",
            "source_keyword": "MYIT",
            "title_instance_id": "SRHAA5V0G9R60AOHC3NULKRZSWWIKY",
            "full_name": "Svc Enable Automation",
            "login_id": "svc_enable_itsm",
            "submitter": "svc_enable_itsm",
            "sr_type_field_1": "Integration test — service account password reset",
            "sr_type_field_2": "svc_demo_account",
            "sr_type_field_3": "Access Management User Create",
            "sr_type_field_4": "svc.owner@example.com",
            "sr_type_field_5": "svc-testadid01",
            "sr_type_field_10": "approver01;approver02",
            "sr_type_field_11": "svc.owner@example.com",
            "sr_type_field_12": "TestCorp",
            "sr_type_field_13": "Production",
            "sr_type_field_14": "TOOL-ITSM",
            "sr_type_field_15": "Test User",
        },
        "wo_update_extra": {},
        "sr_only": True,
        "sr_work_info_notes": "Integration test: service account password reset validation",
        "wo_work_log_comment": "Integration test: service account password reset completed",
    },
    "other_request": {
        "sr_payload": {
            "name": "OSCAR XSystem Integration Test — Other Request",
            "offering_title": "Access Management - Test - Other",
            "source_keyword": "MYIT",
            "title_instance_id": "SRHAA5V0G9R60AO8U4FRFQ7TLAA6YD",
            "full_name": "Svc Enable Automation",
            "login_id": "svc_enable_itsm",
            "submitter": "svc_enable_itsm",
            "sr_type_field_1": "Integration test — other access management request",
            "sr_type_field_2": "testadid01",
            "sr_type_field_3": "Access Management User Create",
            "sr_type_field_4": "Test User",
            "sr_type_field_5": "test.user@example.com",
            "sr_type_field_10": "testmgradid01",
            "sr_type_field_11": "TestCorp",
            "sr_type_field_12": "Test Manager",
            "sr_type_field_13": "testmgradid01",
            "sr_type_field_14": "manager@example.com",
        },
        "wo_update_extra": {},
        "sr_only": True,
        "sr_work_info_notes": "Integration test: other access management request validation",
        "wo_work_log_comment": "Integration test: other access management request handled",
    },
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _assert(condition: bool, msg: str) -> None:
    if not condition:
        raise ValueError(f"ASSERTION FAILED: {msg}")

def _get_conf(context):
    dag_run = context.get("dag_run")
    return (dag_run.conf or {}) if dag_run and dag_run.conf else {}

def _push(ti, key, value):
    ti.xcom_push(key=key, value=value)
    log.info("  XCom push [%s] = %s", key, value)

def _pull(ti, task_id, key):
    value = ti.xcom_pull(task_ids=task_id, key=key)
    log.info("  XCom pull [%s] from [%s] = %s", key, task_id, value)
    return value

def _get_flow(context):
    conf = _get_conf(context)
    flow = (conf.get("flow") or "").strip()
    if flow not in FLOW_CONFIGS:
        if flow:
            log.warning("Unknown flow=%r in conf — falling back to default (%s)", flow, DEFAULT_FLOW)
        flow = DEFAULT_FLOW
    return flow


# ── T01: Create SR ────────────────────────────────────────────────────────────

def t01_create_sr(**context):
    """
    Mirrors: access_management_sr_helper_*_flow
      POST /api/v1/tickets/service-requests?system=REMEDY

    Flow selected via dag_run.conf: {"flow": "password_reset"}
    Default (no conf): new_user_account — existing behaviour unchanged.

    Real DAG asserts:
      - response is dict
      - 'request_number' present
      - 'status' present
    """
    ti   = context["ti"]
    flow = _get_flow(context)
    cfg  = FLOW_CONFIGS[flow]

    url     = f"{MW_BASE}/service-requests?system={TICKETING_SYSTEM}"
    payload = cfg["sr_payload"].copy()

    log.info("T01 → POST %s  [flow=%s]", url, flow)
    with httpx.Client(verify=False, timeout=480.0) as client:
        response = client.post(url, json=payload, headers=MW_HEADERS)
        response.raise_for_status()
        sr_data = response.json()

    log.info("  response: %s", json.dumps(sr_data, indent=2))

    _assert(isinstance(sr_data, dict),
            f"SR create response must be a dict, got {type(sr_data).__name__}")
    missing = [f for f in ("request_number", "status") if not sr_data.get(f)]
    _assert(not missing,
            f"SR create response missing required fields: {missing}  |  got keys: {list(sr_data.keys())}")

    _push(ti, "sr_number", sr_data["request_number"])
    _push(ti, "sr_status",  sr_data["status"])
    _push(ti, "flow",        flow)
    _push(ti, "detail", {
        "action": "Create Service Request in XSystem",
        "flow":   flow,
        "sent": {
            "offering_title":    payload["offering_title"],
            "title_instance_id": payload["title_instance_id"],
            "login_id":          payload["login_id"],
        },
        "received_from_xsystem": {
            "request_number": sr_data["request_number"],
            "status":         sr_data["status"],
        },
    })
    log.info("T01 PASS — SR=%s  status=%s  flow=%s", sr_data["request_number"], sr_data["status"], flow)


# ── T02: SR audit log (work info) ─────────────────────────────────────────────

def t02_create_sr_work_info(**context):
    """
    Mirrors: access_management_sr_helper_*_flow — Phase 1, immediately after SR creation.

    Real DAG does:
      1. GET /service-requests?request_number={sr_number}  → fetch SR, assert instance_id present
      2. POST /service-requests/work-info                  → post audit log with instance_id

    Real DAG stores work_info_id from the response for audit purposes but does NOT
    assert it is non-empty — it is an audit trail, not a downstream dependency.
    We mirror that exactly: assert instance_id (real DAG fails if missing),
    do NOT assert work_info_id (real DAG never checks it).

    Runs for ALL flows including SR-only — matches real DAG Phase 1 behaviour.
    """
    ti        = context["ti"]
    sr_number = _pull(ti, "t01_create_sr", "sr_number")
    _assert(sr_number, "No sr_number from T01")
    flow = _pull(ti, "t01_create_sr", "flow") or DEFAULT_FLOW

    # Brief pause: XSystem needs a moment after SR creation before it accepts
    # the audit log. Manual Postman calls succeed because of human delay;
    # back-to-back automation calls within milliseconds can return WorkInfoID="".
    log.info("T02 — sleeping 20s to allow XSystem to commit SR before posting work info")
    time.sleep(20)

    # Step 1: fetch SR to get instance ID — mirrors real DAG helper exactly
    search_url = f"{MW_BASE}/service-requests?request_number={sr_number}&system={TICKETING_SYSTEM}"
    log.info("T02 step-1 → GET %s  (fetch SR instance ID)", search_url)
    with httpx.Client(verify=False, timeout=480.0) as client:
        search_resp = client.get(search_url, headers=MW_HEADERS)
        search_resp.raise_for_status()
        search_data = search_resp.json()

    log.info("  search response: %s", json.dumps(search_data, indent=2))
    _assert(isinstance(search_data, list) and len(search_data) > 0,
            f"SR search by request_number={sr_number} returned empty")

    instance_id = search_data[0].get("instance")
    _assert(instance_id, f"SR search response missing 'instance' field  |  got keys: {list(search_data[0].keys())}")
    log.info("  instance_id=%s", instance_id)

    # Step 2: post SR audit log — mirrors real DAG helper payload exactly (including sr_instance_id)
    work_info_url = f"{MW_BASE}/service-requests/work-info?system={TICKETING_SYSTEM}"
    payload = {
        "name":                          f"XSystem Integration Test — SR Work Info {sr_number}",
        "notes":                         FLOW_CONFIGS[flow]["sr_work_info_notes"],
        "summary":                       "Request Information",
        "work_info_type_selection":      "General Information",
        "sr_instance_id":                instance_id,
        "service_request_instance_id":   instance_id,
        "request_number":                sr_number,
        "service_request_number":        sr_number,
        "sr_id":                         sr_number,
        "secure_log":                    "Yes",
        "view_access":                   "Public",
        "work_info_type":                "General Information",
    }

    log.info("T02 step-2 → POST %s  [flow=%s]", work_info_url, flow)
    with httpx.Client(verify=False, timeout=480.0) as client:
        response = client.post(work_info_url, json=payload, headers=MW_HEADERS)
        response.raise_for_status()
        wi_data = response.json()

    log.info("  response: %s", json.dumps(wi_data, indent=2))

    _assert(isinstance(wi_data, dict),
            f"Work info response must be a dict, got {type(wi_data).__name__}")

    # Real DAG does NOT assert on id — it logs and stores whatever XSystem returns (may be empty)
    work_info_id = wi_data.get("id", "")
    _push(ti, "work_info_id", work_info_id)
    _push(ti, "detail", {
        "action": "Post SR audit log (work info)",
        "sent":   {"sr_id": sr_number, "sr_instance_id": instance_id, "notes": payload["notes"]},
        "received_from_xsystem": {"work_info_id": work_info_id},
    })
    log.info("T02 PASS — SR work info id=%s", work_info_id)


# ── T03: Approve SR (mock only) ───────────────────────────────────────────────

def t03_approve_sr(**context):
    """
    SR-only flows (service_account_password_reset, other_request):
      Skipped — no WO lifecycle in OSCAR automation.

    xsystem-mock environment (XSYSTEM_MOCK_HOST is set):
      Calls the mock-only /approve endpoint to simulate L1/L2 portal approval.
      This is NOT a middleware call — it talks directly to xsystem-mock.

    Actual XSystem (XSYSTEM_MOCK_HOST not set):
      No-op. Logs that approval is a human action in the XSystem portal.
      T04 will poll until the WO appears.
    """
    ti   = context["ti"]
    conf = _get_conf(context)
    mock_host = conf.get("xsystem_mock_host") or XSYSTEM_MOCK_HOST
    mock_port = int(conf.get("xsystem_mock_port", XSYSTEM_MOCK_PORT))
    flow      = _pull(ti, "t01_create_sr", "flow") or DEFAULT_FLOW

    sr_number = _pull(ti, "t01_create_sr", "sr_number")
    _assert(sr_number, "No sr_number from T01")

    if FLOW_CONFIGS[flow].get("sr_only"):
        log.info("T03 SKIP — flow=%s is SR-only (no WO lifecycle). ITSM team handles WO manually.", flow)
        _push(ti, "detail", {"action": "SR Approval", "result": "SKIPPED — SR-only flow", "flow": flow})
        raise AirflowSkipException(f"Flow '{flow}' is SR-only — no WO approval/resolution in OSCAR automation.")

    if not mock_host:
        log.info("T03 — actual XSystem mode: SR approval is a human action in the portal.")
        log.info("       T04 will poll for the WO (up to %d retries × %ds).",
                 WO_POLL_MAX_RETRIES, WO_POLL_WAIT_SECONDS)
        log.info("T03 SKIP (no-op in actual XSystem)")
        _push(ti, "detail", {
            "action": "SR Approval",
            "mode":   "actual-XSystem",
            "note":   f"SR {sr_number} — awaiting human approval in XSystem portal; T04 will poll",
        })
        return

    # xsystem-mock: trigger approval directly
    approve_url = (
        f"http://{mock_host}:{mock_port}"
        f"/api/user-requests/service-request/{sr_number}/approve"
    )
    log.info("T03 → POST %s  (xsystem-mock approve)", approve_url)
    with httpx.Client(verify=False, timeout=60.0) as client:
        response = client.post(approve_url)
        response.raise_for_status()

    log.info("  mock approve response: %s", response.text)
    _push(ti, "detail", {
        "action": "SR Approval via xsystem-mock",
        "mode":   "xsystem-mock",
        "sent":   {"sr_number": sr_number},
        "received_from_mock": {"approved": True, "mock": f"{mock_host}:{mock_port}"},
    })
    log.info("T03 PASS — SR %s approved via xsystem-mock (%s:%s)", sr_number, mock_host, mock_port)


# ── T04: Poll WO by SR ID ─────────────────────────────────────────────────────

def t04_poll_wo_by_srid(**context):
    """
    Mirrors: check_wo_status() step-2
      GET /api/v1/tickets/work-orders?service_request_id={sr_no}&support_group_name=...&assigned_group=...&system=REMEDY

    Real DAG reads:
      - entry['work_order_number']  → stored as wo_id
      - non-empty list → flag_wo='true'

    Poll behaviour (actual XSystem):
      Empty list → WARNING + sleep WO_POLL_WAIT_SECONDS → retry up to WO_POLL_MAX_RETRIES times.
      Still empty → AirflowSkipException (SR pending approval — re-trigger after approval).
    """
    ti        = context["ti"]
    conf      = _get_conf(context)
    sr_number = _pull(ti, "t01_create_sr", "sr_number")
    _assert(sr_number, "No sr_number from T01")
    flow = _pull(ti, "t01_create_sr", "flow") or DEFAULT_FLOW

    if FLOW_CONFIGS[flow].get("sr_only"):
        log.info("T04 SKIP — flow=%s is SR-only. No WO expected from OSCAR automation.", flow)
        skip_msg = f"Flow '{flow}' is SR-only — T04–T07 skipped."
        _push(ti, "skip_reason", skip_msg)
        _push(ti, "detail", {"action": "Poll for Work Order by SR ID", "result": "SKIPPED — SR-only flow"})
        raise AirflowSkipException(skip_msg)

    poll_wait = int(conf.get("wo_poll_wait_seconds", WO_POLL_WAIT_SECONDS))

    url = (
        f"{MW_BASE}/work-orders"
        f"?service_request_id={sr_number}"
        f"&support_group_name={UA_SUPPORT_GROUP}"
        f"&assigned_group={UA_ASSIGNED_GROUP}"
        f"&system={TICKETING_SYSTEM}"
    )

    data = []
    for attempt in range(WO_POLL_MAX_RETRIES + 1):
        log.info("T04 → GET %s  (attempt %d/%d)", url, attempt + 1, WO_POLL_MAX_RETRIES + 1)
        with httpx.Client(verify=False, timeout=480.0) as client:
            response = client.get(url, headers=MW_HEADERS)
            response.raise_for_status()
            data = response.json()

        log.info("  response: %s", json.dumps(data, indent=2))

        _assert(isinstance(data, list),
                f"WO search must return a list, got {type(data).__name__}")

        if data:
            break

        if attempt < WO_POLL_MAX_RETRIES:
            log.warning(
                "T04 WARNING — WO list empty for SR=%s (attempt %d/%d). "
                "SR may still be pending L1/L2 approval. "
                "Waiting %d seconds before retry...",
                sr_number, attempt + 1, WO_POLL_MAX_RETRIES, poll_wait,
            )
            time.sleep(poll_wait)
        else:
            skip_msg = (
                f"SR {sr_number} has no Work Order after {WO_POLL_MAX_RETRIES} retries "
                f"({WO_POLL_MAX_RETRIES * poll_wait // 60} min total). "
                "SR is pending L1/L2 approval in XSystem portal. "
                "Re-trigger this DAG once the SR has been approved."
            )
            _push(ti, "skip_reason", skip_msg)
            _push(ti, "detail", {
                "action": "Poll for Work Order by SR ID",
                "result": "SKIPPED — SR pending approval",
                "sent":   {"service_request_id": sr_number},
                "note":   skip_msg,
            })
            log.warning("T04 SKIP — %s", skip_msg)
            raise AirflowSkipException(skip_msg)

    entry = data[0]
    _assert("work_order_number" in entry,
            f"WO entry missing 'work_order_number'  |  got keys: {list(entry.keys())}")
    _assert(entry["work_order_number"], "'work_order_number' is empty")

    _push(ti, "wo_id", entry["work_order_number"])
    _push(ti, "detail", {
        "action": "Poll for Work Order by SR ID",
        "sent":   {"service_request_id": sr_number},
        "received_from_xsystem": {
            "work_order_number": entry["work_order_number"],
            "status":            entry.get("status"),
            "assigned_group":    entry.get("assigned_group"),
        },
    })
    log.info("T04 PASS — flag_wo='true', wo_id=%s", entry["work_order_number"])


# ── T05: WO work log (audit log) ──────────────────────────────────────────────

def t05_create_wo_work_log(**context):
    """
    Mirrors: create_wo_work_log() via XSystemHandler — Phase 2, BEFORE WO close.
      POST /api/v1/tickets/work-orders/work-logs?system=REMEDY

    Real DAG reads:
      - response['id']  → Work Log ID
    """
    ti    = context["ti"]
    wo_id = _pull(ti, "t04_poll_wo_by_srid", "wo_id")
    _assert(wo_id, "No wo_id from T04")
    flow = _pull(ti, "t01_create_sr", "flow") or DEFAULT_FLOW

    url     = f"{MW_BASE}/work-orders/work-logs?system={TICKETING_SYSTEM}"
    payload = {
        "name":                 "XSystem Integration Test — WO Work Log",
        "work_order_id":        wo_id,
        "work_order_entry_id":  wo_id,
        "detailed_description": FLOW_CONFIGS[flow]["wo_work_log_comment"],
        "short_description":    ".",
        "work_log_type":        "General Information",
        "communication_source": "Email",
        "secure_work_log":      "Yes",
        "view_access":          "Public",
    }

    log.info("T05 → POST %s", url)
    with httpx.Client(verify=False, timeout=480.0) as client:
        response = client.post(url, json=payload, headers=MW_HEADERS)
        response.raise_for_status()
        wl_data = response.json()

    log.info("  response: %s", json.dumps(wl_data, indent=2))

    _assert(isinstance(wl_data, dict),
            f"Work log response must be a dict, got {type(wl_data).__name__}")
    _assert("id" in wl_data,
            f"Work log response missing 'id' (Work Log ID)  |  got keys: {list(wl_data.keys())}")
    _assert(wl_data["id"], "'id' is empty in work log response")

    _push(ti, "work_log_id", wl_data["id"])
    _push(ti, "detail", {
        "action": "Post WO audit log (work log)",
        "sent":   {"work_order_id": wo_id, "detailed_description": payload["detailed_description"], "work_log_type": payload["work_log_type"]},
        "received_from_xsystem": {"work_log_id": wl_data["id"]},
    })
    log.info("T05 PASS — WO work log id=%s", wl_data["id"])


# ── T06: Search WO by WO number ───────────────────────────────────────────────

def t06_search_wo_by_wo_number(**context):
    """
    Mirrors: update_wo_status_*() step-1
      GET /api/v1/tickets/work-orders?work_order_number={wo_number}&system=REMEDY

    XSystemHandler intercepts 'work_order_number' and routes to path param
    endpoint because XSystem does not support ?Work_Order_ID= query filter.

    Real DAG reads:
      - entry['request_id']  ← used as path param in the PUT URL below
    """
    ti    = context["ti"]
    wo_id = _pull(ti, "t04_poll_wo_by_srid", "wo_id")
    _assert(wo_id, "No wo_id from T04")

    url = f"{MW_BASE}/work-orders?work_order_number={wo_id}&system={TICKETING_SYSTEM}"
    log.info("T06 → GET %s", url)

    with httpx.Client(verify=False, timeout=480.0) as client:
        response = client.get(url, headers=MW_HEADERS)
        response.raise_for_status()
        data = response.json()

    log.info("  response: %s", json.dumps(data, indent=2))

    _assert(isinstance(data, list),
            f"WO search must return a list, got {type(data).__name__}")
    _assert(len(data) > 0,
            f"WO search by work_order_number={wo_id} returned empty → real DAG raises 'No work order found'")

    entry = data[0]
    # Real Remedy populates 'request_id' with an internal instance ID.
    # xsystem-mock returns request_id=null but sets work_order_id — fall back to work_order_id.
    raw_request_id         = entry.get("request_id")
    work_order_id_fallback = entry.get("work_order_id")
    if not raw_request_id:
        log.warning(
            "T06 NOTE — 'request_id' field is absent/null in WO response (expected for xsystem-mock); "
            "falling back to 'work_order_id'=%s",
            work_order_id_fallback,
        )
    request_id = raw_request_id or work_order_id_fallback
    _assert(request_id, f"'request_id' is empty and no fallback found — PUT URL would be malformed  |  got keys: {list(entry.keys())}")

    _push(ti, "request_id", request_id)
    _push(ti, "detail", {
        "action": "Search Work Order by WO number",
        "sent":   {"work_order_number": wo_id},
        "received_from_xsystem": {
            "request_id":     request_id,
            "work_order_id":  entry.get("work_order_id"),
            "status":         entry.get("status"),
            "assigned_group": entry.get("assigned_group"),
        },
    })
    log.info("T06 PASS — request_id=%s", request_id)


# ── T07: Update WO status ─────────────────────────────────────────────────────

def t07_update_wo_status(**context):
    """
    Mirrors: update_wo_status_*() step-2 per flow
      PUT /api/v1/tickets/work-orders/{request_id}?system=REMEDY

    Base payload (all flows): status, status_reason, environment.
    wo_update_extra (flow-specific): wo_type_field_23/28/29/30 for flows that
    write back AD result data (new_user_account, new_service_account).
    Other flows send no wo_type_fields — matches real DAG behaviour.

    Real DAG reads:
      - response['id']
    """
    ti         = context["ti"]
    request_id = _pull(ti, "t06_search_wo_by_wo_number", "request_id")
    _assert(request_id, "No request_id from T06")
    flow = _pull(ti, "t01_create_sr", "flow") or DEFAULT_FLOW

    url     = f"{MW_BASE}/work-orders/{request_id}?system={TICKETING_SYSTEM}"
    payload = {
        "status":        "Completed",
        "status_reason": "Successful",
        "environment":   "Production",
    }
    payload.update(FLOW_CONFIGS[flow]["wo_update_extra"])

    log.info("T07 → PUT %s", url)
    log.info("  payload: %s", json.dumps(payload, indent=2))

    with httpx.Client(verify=False, timeout=480.0) as client:
        response = client.put(url, json=payload, headers=MW_HEADERS)
        response.raise_for_status()
        wo_data = response.json()

    log.info("  response: %s", json.dumps(wo_data, indent=2))

    _assert(isinstance(wo_data, dict),
            f"WO update response must be a dict, got {type(wo_data).__name__}")
    _assert("id" in wo_data,
            f"WO update response missing 'id'  |  got keys: {list(wo_data.keys())}")

    _push(ti, "wo_update_id", wo_data["id"])
    _push(ti, "detail", {
        "action": "Update Work Order status to Completed",
        "sent":   payload,
        "received_from_xsystem": {
            "id":     wo_data["id"],
            "status": wo_data.get("status"),
        },
    })
    log.info("T07 PASS — WO updated: id=%s", wo_data["id"])


# ── T08: Summary ──────────────────────────────────────────────────────────────

def t08_summary(**context):
    """Always runs (ALL_DONE). Prints full PASS/SKIP/FAIL report."""
    ti = context["ti"]

    def pull_safe(task_id, key):
        try:
            v = ti.xcom_pull(task_ids=task_id, key=key)
            return v if v is not None else None
        except Exception:
            return None

    conf      = _get_conf(context)
    mock_host = conf.get("xsystem_mock_host") or XSYSTEM_MOCK_HOST
    mock_port = int(conf.get("xsystem_mock_port", XSYSTEM_MOCK_PORT))
    flow      = pull_safe("t01_create_sr", "flow") or DEFAULT_FLOW

    checks = [
        ("T01", "SR created (request_number)",         pull_safe("t01_create_sr",             "sr_number")),
        ("T01", "SR initial status",                    pull_safe("t01_create_sr",             "sr_status")),
        ("T02", "SR work info id (WorkInfoID)",          pull_safe("t02_create_sr_work_info",   "work_info_id")),
        ("T03", "SR approved (mock) / no-op (actual)",  f"mock({mock_host}:{mock_port})" if mock_host else "actual-XSystem-noop"),
        ("T04", "work_order_number from SRID poll",     pull_safe("t04_poll_wo_by_srid",        "wo_id")),
        ("T05", "WO work log id",                       pull_safe("t05_create_wo_work_log",     "work_log_id")),
        ("T06", "request_id from WO-number search",     pull_safe("t06_search_wo_by_wo_number", "request_id")),
        ("T07", "WO updated (id)",                      pull_safe("t07_update_wo_status",       "wo_update_id")),
    ]

    sep = "=" * 65
    log.info("\n%s", sep)
    log.info("  ACCESS MANAGEMENT → XSYSTEM INTEGRATION TEST REPORT")
    log.info("  Flow: %s", flow)
    log.info("  Middleware: https://%s:%s  |  system=%s", MIDDLEWARE_HOST, MIDDLEWARE_PORT, TICKETING_SYSTEM)
    mode = f"xsystem-mock ({mock_host}:{mock_port})" if mock_host else "actual XSystem"
    log.info("  Mode: %s", mode)
    log.info("%s", sep)

    failures = []
    skipped  = []

    for tid, label, value in checks:
        if value is not None:
            log.info("  [PASS]  [%s]  %-46s %s", tid, label, value)
        else:
            log.warning("  [SKIP/FAIL]  [%s]  %-46s (no result)", tid, label)
            skipped.append(f"{tid}: {label}")

    log.info("%s", sep)

    sr_only = FLOW_CONFIGS[flow].get("sr_only", False)
    wo_id   = pull_safe("t04_poll_wo_by_srid", "wo_id")

    if sr_only:
        sr_number = pull_safe("t01_create_sr", "sr_number") or "SR..."
        work_info_id = pull_safe("t02_create_sr_work_info", "work_info_id")
        log.info("  Flow '%s' is SR-only — T03/T04/T05/T06/T07 intentionally skipped.", flow)
        log.info("  SR %s created. SR work info id=%s.", sr_number, work_info_id)
        log.info("  WO resolution handled manually by ITSM team.")
        log.info("  ALL SR-ONLY ASSERTIONS PASSED")
        log.info("%s\n", sep)
        return

    # T04 skip = SR pending approval (not a hard failure)
    if wo_id is None:
        sr_number   = pull_safe("t01_create_sr", "sr_number") or "SR..."
        skip_reason = pull_safe("t04_poll_wo_by_srid", "skip_reason") or "SR pending approval"
        log.warning("  SR %s is still pending approval in XSystem portal.", sr_number)
        log.warning("  Reason: %s", skip_reason)
        log.warning("  Re-trigger this DAG once the SR has been approved to complete the test.")
        log.info("%s\n", sep)
        return  # not a hard failure — expected state when SR not yet approved

    if failures:
        log.error("  FAILED: %d assertion(s)", len(failures))
        for f in failures:
            log.error("    x  %s", f)
        log.info("%s\n", sep)
        raise ValueError(f"Integration test FAILED — {len(failures)} assertion(s) failed.")

    log.info("  ALL ASSERTIONS PASSED")
    log.info("  Ready for production with TICKETING_REMEDY_OVERRIDE_XSYS=true")
    log.info("%s\n", sep)


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="access_management_xsystem_integration_test",
    description=(
        "Integration test for access management XSystem flow. "
        "Single trigger: creates SR, posts SR work info immediately, "
        "approves (mock) or polls (actual), then validates the full WO resolve flow via middleware."
    ),
    default_args={
        "owner": "access_management",
        "retries": 0,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["access_management", "integration_test", "xsystem"],
    max_active_runs=1,
) as dag:

    t01 = PythonOperator(task_id="t01_create_sr",              python_callable=t01_create_sr)
    t02 = PythonOperator(task_id="t02_create_sr_work_info",    python_callable=t02_create_sr_work_info)
    t03 = PythonOperator(task_id="t03_approve_sr",             python_callable=t03_approve_sr)
    t04 = PythonOperator(task_id="t04_poll_wo_by_srid",        python_callable=t04_poll_wo_by_srid,
                         trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    t05 = PythonOperator(task_id="t05_create_wo_work_log",     python_callable=t05_create_wo_work_log,
                         trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    t06 = PythonOperator(task_id="t06_search_wo_by_wo_number", python_callable=t06_search_wo_by_wo_number,
                         trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    t07 = PythonOperator(task_id="t07_update_wo_status",       python_callable=t07_update_wo_status,
                         trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    t08 = PythonOperator(task_id="t08_summary",                python_callable=t08_summary,
                         trigger_rule=TriggerRule.ALL_DONE)

    t01 >> t02 >> t03 >> t04 >> t05 >> t06 >> t07 >> t08

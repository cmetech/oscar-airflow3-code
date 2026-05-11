# Access Management Automation — Architecture Context

## Purpose

OSCAR automates account provisioning (create/modify/disable user and service accounts, reset passwords) by bridging internal AD operations with an external ITSM ticketing system (XSystem, replacing Remedy). Two Airflow DAGs handle the full lifecycle: one raises the ticket (SR), the other resolves it after human approval creates a Work Order.

---

## System Components

| Component | Location | Role |
|---|---|---|
| `access_management_sr_management` DAG | `airflow/dags/` | Phase 1 — parse email, check AD, create SR |
| `access_management_request_resolve` DAG | `airflow/dags/` | Phase 2 — detect approved WO, action AD, close WO |
| oscar-middleware ticketing router | `oscar-middleware/src/app/routers/ticketing.py` | Translates Oscar payloads → handler calls |
| `TicketingSystemHandler` (ABC) | `oscar-middleware/src/app/core/ticketing_handlers.py` | Strategy pattern base |
| `XSystemHandler` | same file | Concrete impl — talks to XSystem REST API |
| `RemedyHandler` | same file | Legacy concrete impl — BMC Remedy |
| xsystem-mock | `/home/splunk/xsystem-mock/` | Fake XSystem for local dev/test (port 8099) |
| Integration test DAG | `airflow/dags/access_management_xsystem_integration_test.py` | End-to-end smoke test |

---

## Event Types Handled

| Event | SR Management group | Request Resolve group |
|---|---|---|
| New user account | `new_user_account_group` | `new_user_account_task_group` |
| New service account | `new_service_account_group` | `new_service_account_task_group` |
| User account modification | `user_account_modification_group` | `user_account_modification_task_group` |
| Password reset | `user_password_reset_group` | `password_reset_task_group` |
| Other / service account password reset | `other_requests_group` | _(catch-all)_ |
| Service account modification | stub (not yet supported) | — |
| Disable service account | stub | — |
| Disable user account | stub | — |

---

## Full Data Flow

```
Email → SR Management DAG → oscar-middleware → XSystemHandler → XSystem API
                                                                       ↓
                                                               SR created (e.g. SR000000000020)
                                                                       ↓
              [Human approves SR in XSystem portal → WO created]
                                                                       ↓
         Request Resolve DAG → oscar-middleware → XSystemHandler → XSystem API
                                                                       ↓
                                                           WO fetched, AD actioned
                                                           WO comment posted, WO closed
```

### Phase 1 — SR Management DAG (per active event group)

1. `update_or_create_request_activity` — upsert activity record in Oscar DB
2. Branch on event type from email subject
3. `fetch_data_activity_info_*` — load activity data from DB
4. `ad_connect_*` / `ad_process_*` — query AD (does user exist? is account disabled?)
5. Branch on AD result (success / already-exists / error)
6. `sr_creation_*` — **POST `/api/v1/tickets/service-requests`** → middleware → XSystem → SR created
7. Branch on SR result (success / failure)
8. `sr_worklog_update_*` — **POST `/api/v1/tickets/service-requests/work-info`** → SR audit log posted
9. `sr_creation_activity_update_*` — update DB activity with SR number
10. Email notification to approvers / requestor

### Phase 2 — Request Resolve DAG

1. `check_activity_request_resolve` — load activity record from DB
2. `branch_on_sr_submitted` — if status is "SR Submitted" → check WO; otherwise → send reminders
3. `check_wo_status` — **GET `/api/v1/tickets/work-orders?service_request_id={sr}`** → search for linked WO
4. Branch on WO result:
   - `flag_wo='true'` → WO found and approved → proceed
   - `flag_wo='errored'` → WO fetch failed → notify
   - `flag_wo='cancel'` → SR cancelled → notify
   - else → send SR reminder emails (based on `flag_update_date`)
5. `task_type_request_resolve_branch` — route to correct event-type group
6. Per event-type group:
   - `fetch_activity_info_*` — reload activity
   - `ad_process_*` — **execute** the action in AD (create user, reset password, modify, create service account)
   - Branch: `finished` / `non-finished` / `decryption-failed`
   - `rs_update_activity_*` — store result (new password, new AD ID, etc.) in DB
   - Email notifications (shift-wise FO, user notification)
   - `update_wo_comment_*` — **POST `/api/v1/tickets/work-orders/work-logs`** → WO audit log posted
   - Branch on WO comment status (success / failure)
   - `update_wo_status_*` — **PUT `/api/v1/tickets/work-orders/{wo_id}`** with `status=Completed`
   - Branch on WO status update (success / failure)
   - Extra steps (e.g. `insert_new_user_data_into_user_list` for new accounts)

---

## Ticketing Abstraction Layer

### Strategy Pattern

```
TicketingSystemHandler (ABC)
├── XSystemHandler     — active: XSystem REST API
├── RemedyHandler      — legacy: BMC Remedy ITSM REST API
├── StubHandler        — Redis-backed fake (testing)
└── ElasticHandler     — Elastic Cases (incidents only)
```

### Handler Selection (ticketing.py router)

```python
_xsys_handler = XSystemHandler()
handlers = {
    "remedy": _xsys_handler if settings.TICKETING_REMEDY_OVERRIDE_XSYS else RemedyHandler(),
    "xsys":   _xsys_handler,
}
```

**`TICKETING_REMEDY_OVERRIDE_XSYS=true`** — DAGs that pass `system=REMEDY` (the default) transparently hit `XSystemHandler`. Zero DAG code changes needed.

### Oscar Model → XSystem Field Mapping

| Oscar field | XSystem field | Operation |
|---|---|---|
| `offering_title` | `OfferingTitle` | Create SR |
| `source_keyword` | `SourceKeyword` | Create SR |
| `title_instance_id` | `TitleInstanceID` | Create SR |
| `full_name` | `FullName` | Create SR |
| `login_id` | `LoginID` + `Submitter` | Create SR |
| `sr_type_field_1..42` | `SRTypeField1..42` | Create SR |
| `sr_id` | `SRID` (path) | SR audit log |
| `notes` | `Notes` | SR audit log |
| `work_info_type` | `WorkInfoType` | SR audit log |
| `status` | `Status` | Update WO |
| `status_reason` | `Status Reason` | Update WO |
| `environment` | `ERI:Environment` | Update WO |
| `wo_type_field_23..30` | `WOTypeField23..30` | Update WO |
| `detailed_description` | `Detailed Description` | WO work log |
| `work_order_id` | `Work Order ID` (body) + path param | WO work log |
| `service_request_id` | `SRID` (query param) | Search WO |
| `work_order_number` | `Work_Order_ID` (query/path) | Search WO |

---

## XSystem API Endpoints

| Operation | Oscar middleware endpoint | XSystem endpoint | Method |
|---|---|---|---|
| Create SR | `POST /tickets/service-requests` | `POST /api/user-requests/create?fields=RequestNumber` | POST |
| Get SR | `GET /tickets/service-requests/{id}` | `GET /api/user-requests/service-request/{SR_ID}` | GET |
| Search SR | internal | `GET /api/user-requests/service-request?RequestNumber=...` | GET |
| SR audit log | `POST /tickets/service-requests/work-info` | `POST /api/user-requests/service-request/audit-log/{SR_ID}` | POST |
| Search WO by SR | `GET /tickets/work-orders?service_request_id=` | `GET /api/user-requests/work-orders?SRID=...` | GET |
| Get WO | `GET /tickets/work-orders/{id}` | `GET /api/user-requests/work-orders/{WO_ID}` | GET |
| Update WO | `PUT /tickets/work-orders/{id}` | `PUT /api/user-requests/work-orders/update/{WO_ID}` | PUT (204) |
| WO audit log | `POST /tickets/work-orders/work-logs` | `POST /api/user-requests/work-orders/audit-log/{WO_ID}` | POST |
| Auth | internal | `POST /api/login/` | POST (form) |

**XSystem quirks handled in the handler:**
- GET WO by path (`/work-orders/{id}`) returns a **list**, not a single object — handler normalises with `isinstance(response_data, list)` check
- PUT WO returns **204 No Content** — handler does `raise_for_status()` then fetches updated WO with a separate GET
- Auth token is cached in Redis; `XSystemHandler._get_auth_token()` uses `ConnectionManager` for credentials

---

## Integration Test DAG (`access_management_xsystem_integration_test`)

Single-trigger DAG that exercises every middleware operation the two production DAGs perform.

### Task Flow

| Task | Operation | Middleware call |
|---|---|---|
| T01 `t01_create_sr` | Create SR | `POST /tickets/service-requests` |
| T02 `t02_approve_sr` | Approve SR | Mock: direct `/approve` on xsystem-mock. Actual XSystem: no-op, T03 polls |
| T03 `t03_poll_wo_by_srid` | Poll for WO | `GET /tickets/work-orders?service_request_id=` (retries 2× × 10 min) |
| T04 `t04_search_wo_by_wo_number` | Search WO by number | `GET /tickets/work-orders?work_order_number=` |
| T05 `t05_update_wo_status` | Close WO | `PUT /tickets/work-orders/{id}` |
| T06 `t06_create_sr_work_info` | SR audit log | `POST /tickets/service-requests/work-info` |
| T07 `t07_create_wo_work_log` | WO audit log | `POST /tickets/work-orders/work-logs` |
| T08 `t08_summary` | Report | Always runs (`ALL_DONE`), prints full PASS/SKIP/FAIL |

### Operating Modes

**xsystem-mock** (`XSYSTEM_MOCK_HOST` set or passed via DAG conf):
- T02 calls `/api/user-requests/service-request/{sr}/approve` directly on mock
- T03 finds WO immediately on first attempt

**Actual XSystem** (`XSYSTEM_MOCK_HOST` not set):
- T02 is a no-op (approval is a human portal action)
- T03 polls up to 2 times with 10-minute waits; skips gracefully with `AirflowSkipException` if no WO found
- Re-trigger DAG once SR approved in portal

### XCom Keys (per task)

| Task | Key | Value |
|---|---|---|
| t01 | `sr_number` | SR request number (e.g. `SR000000000020`) |
| t01 | `sr_status` | SR initial status |
| t03 | `wo_id` | WO number (e.g. `WO000000000045`) |
| t04 | `request_id` | WO instance ID (falls back to `work_order_id` for xsystem-mock) |
| t05 | `wo_update_id` | Updated WO id |
| t06 | `work_info_id` | SR WorkInfoID |
| t07 | `work_log_id` | WO Work Log ID |

---

## Key Config

| Variable | Where set | Effect |
|---|---|---|
| `TICKETING_REMEDY_OVERRIDE_XSYS=true` | `oscar/overrides.env` | Routes all `system=REMEDY` calls to XSystemHandler |
| `DEFAULT_TICKETING_SYSTEM=REMEDY` | `oscar/overrides.env` | Default system query param in DAG calls |
| `TICKETING_HOST` | `oscar/overrides.env` | XSystem (or mock) host |
| `TICKETING_PORT` | `oscar/overrides.env` | XSystem (or mock) port (8008=remedy-mock, 8099=xsystem-mock) |
| `ENABLE_TICKETING_MIDDLEWARE=true` | `oscar/overrides.env` | Enables ticketing router in middleware |
| `XSYSTEM_MOCK_HOST` | Integration test DAG header | Set to `xsystem-mock` to enable auto-approve in T02 |

---

## Validated Operations (Dry Run, April 2026)

All 8 XSystemHandler operations verified against XSystem API doc and integration test:

| # | Operation | Status |
|---|---|---|
| 1 | Create SR | PASS |
| 2 | SR audit log | PASS |
| 3 | Search WO by SR ID | PASS |
| 4 | WO audit log | PASS |
| 5 | Update WO status (204 handling) | PASS |
| 6 | Get WO (list normalisation) | PASS |
| 7 | Get SR | PASS |
| 8 | Auth token (Redis cache) | PASS |

No breaking issues. Ready for production with `TICKETING_REMEDY_OVERRIDE_XSYS=true`.

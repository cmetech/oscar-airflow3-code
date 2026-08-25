import pytest

from splunk_closed_loop_lib import audit

ROWS = [
    {"operation_type": "create", "status": "success", "incident_number": "INC9", "trace_id": "ref-c", "alert_fingerprint": "fp", "created_at": "2026-08-24T01:00:00Z"},
    {"operation_type": "update", "status": "success", "incident_number": "INC9", "trace_id": "ref-u1", "alert_fingerprint": "fp", "created_at": "2026-08-24T01:05:00Z"},
    # a DAG-originated transition: the state consumer inserts it as operation_type='unknown'
    {"operation_type": "unknown", "status": "success", "incident_number": "INC9", "trace_id": "ref-u2", "alert_fingerprint": "fp", "created_at": "2026-08-24T01:06:00Z"},
]


def test_find_create_success():
    assert audit.find_create(ROWS, "ref-c")["incident_number"] == "INC9"


def test_find_create_ignores_pending():
    row = {"operation_type": "create", "status": "pending", "trace_id": "x"}
    assert audit.find_create([row], "x") is None


def test_find_create_returns_failure_row():
    row = audit.find_create(
        [{"operation_type": "create", "status": "failure", "incident_number": None, "trace_id": "x"}], "x")
    assert row["status"] == "failure"


def test_find_create_with_ref_ignores_a_stale_row_for_another_ticket():
    rows = [
        {"operation_type": "create", "status": "success", "incident_number": "INC-OLD", "trace_id": "ref-old", "created_at": "2026-08-01T01:00:00Z"},
        {"operation_type": "create", "status": "success", "incident_number": "INC-NEW", "trace_id": "ref-new", "created_at": "2026-08-24T01:00:00Z"},
    ]
    assert audit.find_create(rows, "ref-new")["incident_number"] == "INC-NEW"


def test_find_create_with_ref_waits_until_incident_number_arrives():
    rows = [{"operation_type": "create", "status": "success", "incident_number": None, "trace_id": "ref-new", "created_at": "2026-08-24T01:00:00Z"}]
    assert audit.find_create(rows, "ref-new") is None


def test_find_create_with_ref_returns_terminal_failure():
    rows = [{"operation_type": "create", "status": "failure", "incident_number": None, "trace_id": "ref-new", "error_message": "esb down", "created_at": "2026-08-24T01:00:00Z"}]
    assert audit.find_create(rows, "ref-new")["status"] == "failure"


def test_find_create_with_ref_never_falls_back_to_a_different_ref():
    rows = [{"operation_type": "create", "status": "success", "incident_number": "INC-OLD", "trace_id": "ref-old", "created_at": "2026-08-01T01:00:00Z"}]
    assert audit.find_create(rows, "ref-new") is None


def test_find_create_requires_a_ref():
    rows = [
        {"operation_type": "create", "status": "success", "incident_number": "INC-OLD", "trace_id": "a", "created_at": "2026-08-01T01:00:00Z"},
        {"operation_type": "create", "status": "success", "incident_number": "INC-NEW", "trace_id": "b", "created_at": "2026-08-24T01:00:00Z"},
    ]
    with pytest.raises(ValueError, match="find_create requires the create ref"):
        audit.find_create(rows, None)
    with pytest.raises(ValueError, match="find_create requires the create ref"):
        audit.find_create(rows, "")


def test_find_create_matches_by_incident_number_on_ttl_reuse():
    """On a TTL-reuse the alert's ticket_id label carries the INC (the ITSM state
    consumer swaps it in), not the create trace_id — find_create must still locate
    its own create row via incident_number so the reused-ticket path doesn't poll to
    the deadline and false-alarm on a perfectly healthy ticket."""
    rows = [{"operation_type": "create", "status": "success", "incident_number": "INC-REUSED",
            "trace_id": "ref-original", "created_at": "2026-08-24T01:00:00Z"}]
    assert audit.find_create(rows, "INC-REUSED")["trace_id"] == "ref-original"


def test_find_update_by_ref():
    assert audit.find_update(ROWS, "ref-u1")["status"] == "success"
    assert audit.find_update(ROWS, "ref-nope") is None


def test_find_update_accepts_consumer_inserted_unknown_row():
    assert audit.find_update(ROWS, "ref-u2")["status"] == "success"


def test_find_update_rejects_falsy_ref_even_if_a_row_lacks_trace_id():
    assert audit.find_update([{"operation_type": "update", "status": "success", "trace_id": None}], None) is None
    assert audit.find_update(ROWS, "") is None


def test_wait_for_polls_until_predicate():
    calls = {"n": 0}

    def fetch():
        calls["n"] += 1
        return ROWS if calls["n"] >= 3 else []

    slept = []
    row = audit.wait_for(fetch, lambda rows: audit.find_create(rows, "ref-c"), deadline_s=100, poll_s=10, sleep=slept.append)
    assert row["incident_number"] == "INC9" and calls["n"] == 3 and slept == [10, 10]


def test_wait_for_times_out():
    with pytest.raises(TimeoutError):
        audit.wait_for(lambda: [], lambda rows: audit.find_create(rows, "ref-c"), deadline_s=25, poll_s=10, sleep=lambda s: None)


def test_count_autocloses_in_window():
    rows = [
        {"operation_type": "close", "status": "success", "status_details": "splunk_closed_loop:autoclose", "created_at": "2026-08-24T01:00:00Z"},
        {"operation_type": "close", "status": "success", "status_details": "splunk_closed_loop:autoclose", "created_at": "2026-08-23T20:00:00Z"},
        {"operation_type": "update", "status": "success", "status_details": "splunk_closed_loop:verdict:pass", "created_at": "2026-08-24T01:30:00Z"},
    ]
    from datetime import datetime, timezone
    now = datetime(2026, 8, 24, 2, 0, tzinfo=timezone.utc)
    assert audit.count_autocloses(rows, window_s=7200, now=now) == 1
    assert audit.count_markers(rows, "splunk_closed_loop:verdict", window_s=900, now=datetime(2026, 8, 24, 1, 40, tzinfo=timezone.utc)) == 1
    assert audit.count_markers(rows, "splunk_closed_loop:verdict", window_s=300, now=datetime(2026, 8, 24, 1, 40, tzinfo=timezone.utc)) == 0

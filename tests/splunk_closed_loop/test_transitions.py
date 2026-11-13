from splunk_closed_loop_lib import transitions

WL = {"craigcool": "CraigCool_Check_Results: SUCCESSFUL", "site": "Splunk_Site_Traffic_Status: SUCCESSFUL",
      "occ": "Splunk_OCC_Traffic_Status: SUCCESSFUL", "sdp": "SDP_Success_Status: SUCCESSFUL"}
COMMON = dict(incident_number="INC1", fingerprint="fp1", customer_name="ATTM-PPS-HOC-US", impact="4-Minor/Localized")


def test_pass_path_is_four_steps_ending_closed():
    steps = transitions.plan(True, WL, "<body>", False, **COMMON)
    assert [s["status"] for s in steps] == ["Assigned", "In Progress", "Resolved", "Closed"]
    assert steps[0]["work_info_notes"] == WL["craigcool"] and steps[1]["work_info_notes"] == WL["craigcool"]
    r = steps[2]
    assert r["status_reason"] == "No Further Action Required" and r["resolution"] == "<body>"
    assert r["resolution_category_tier_1"] == "No Fault Found" and r["resolution_category_tier_2"] == "Resolved"
    assert steps[3]["work_info_notes"] == "Ticket closed as all checks are successful"
    for s in steps:
        assert s["incident_number"] == "INC1" and s["fingerprint"] == "fp1" and s["name"] == "splunk_closed_loop"
        assert s["company"] == "ATTM-PPS-HOC-US" and s["impact"] == "4-Minor/Localized"


def test_fail_path_stops_at_in_progress():
    steps = transitions.plan(False, WL, "<body>", False, **COMMON)
    assert [s.get("status") for s in steps] == ["Assigned", "In Progress", None]
    assert [s["work_info_notes"] for s in steps] == [WL["craigcool"], WL["site"], WL["occ"]]
    assert all("resolution" not in s for s in steps)


def test_fail_path_faithful_resolves_without_category():
    steps = transitions.plan(False, WL, "<body>", True, **COMMON)
    assert [s["status"] for s in steps] == ["Assigned", "In Progress", "Resolved"]
    assert "resolution_category_tier_1" not in steps[2] and "status_reason" not in steps[2]


# --- E-1: notify may only say "Resolved / SUCCESSFUL" when the ticket actually got there ---
PASS_VERDICT = {"passed": True}
FAIL_VERDICT = {"passed": False}


def test_notify_gate_reports_failure_when_the_transition_never_ran():
    g = transitions.notify_gate(PASS_VERDICT, None)
    assert g["mode"] == "transition_failed"
    assert g["outcome"] == "error"
    assert g["ticket_status"] != "Resolved"


def test_notify_gate_reports_failure_on_a_partial_transition():
    steps = transitions.plan(True, WL, "<body>", False, **COMMON)
    rec = transitions.start_record(steps)
    rec.update({"done": 2, "last_status": "In Progress"})
    g = transitions.notify_gate(PASS_VERDICT, rec)
    assert g["mode"] == "transition_failed"
    assert g["ticket_status"] == "In Progress"
    assert g["done"] == 2 and g["planned"] == len(steps)


def test_notify_gate_allows_the_all_clear_only_when_every_step_confirmed():
    steps = transitions.plan(True, WL, "<body>", False, **COMMON)
    rec = transitions.start_record(steps)
    rec.update({"done": len(steps), "ok": True, "last_status": "Closed"})
    g = transitions.notify_gate(PASS_VERDICT, rec)
    # N-7: report the status the ticket actually reached, not a hard-coded one.
    assert g == {"mode": "verdict", "outcome": "pass", "ticket_status": "Closed"}


def test_notify_gate_fail_verdict_with_a_complete_transition_is_a_fail_not_an_error():
    steps = transitions.plan(False, WL, "<body>", False, **COMMON)
    rec = transitions.start_record(steps)
    rec.update({"done": len(steps), "ok": True, "last_status": "In Progress"})
    g = transitions.notify_gate(FAIL_VERDICT, rec)
    assert g["mode"] == "verdict" and g["outcome"] == "fail" and g["ticket_status"] == "In Progress"


def test_notify_gate_missing_verdict_still_wins_over_transition_state():
    assert transitions.notify_gate(None, {"ok": True})["mode"] == "no_verdict"


def test_start_record_starts_not_ok():
    rec = transitions.start_record(transitions.plan(True, WL, "<b>", False, **COMMON))
    assert rec["ok"] is False and rec["done"] == 0 and rec["planned"] == 4


def test_notify_gate_fail_path_that_resolves_reports_resolved():
    steps = transitions.plan(False, WL, "<body>", True, **COMMON)
    rec = transitions.start_record(steps)
    rec.update({"done": len(steps), "ok": True, "last_status": "Resolved"})
    assert transitions.notify_gate(FAIL_VERDICT, rec)["ticket_status"] == "Resolved"

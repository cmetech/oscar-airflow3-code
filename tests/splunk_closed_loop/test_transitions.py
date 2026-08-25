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

import itertools

import pytest

from splunk_closed_loop_lib import checks, verdict


def _r(name, ok, error=None):
    return checks.CheckResult(name=name, ok=ok, value="", rows=[], raw="", error=error)


def _results(sdp=True, occ=True, site=True, cc=True):
    return {"sdp": _r("sdp", sdp), "occ": _r("occ", occ), "site": _r("site", site), "craigcool": _r("craigcool", cc)}


@pytest.mark.parametrize("sdp,occ,site,cc", list(itertools.product([True, False], repeat=4)))
def test_truth_table_with_sdp_gating(sdp, occ, site, cc):
    v = verdict.decide(_results(sdp, occ, site, cc), sdp_gates=True, prior_autocloses=0, max_autoclose=1)
    assert v.passed is (sdp and occ and site and cc)


@pytest.mark.parametrize("sdp", [True, False])
def test_sdp_ignored_when_not_gating(sdp):
    v = verdict.decide(_results(sdp=sdp), sdp_gates=False, prior_autocloses=0, max_autoclose=1)
    assert v.passed is True


def test_error_counts_as_fail():
    res = _results()
    res["occ"] = _r("occ", False, error="timeout after 900s")
    v = verdict.decide(res, sdp_gates=True, prior_autocloses=0, max_autoclose=1)
    assert v.passed is False and "timeout" in v.reason


def test_recurrence_escalates_instead_of_autoclose():
    v = verdict.decide(_results(), sdp_gates=True, prior_autocloses=1, max_autoclose=1)
    assert v.passed is False and "recurrence" in v.reason.lower()


def test_rendered_text_from_booleans():
    v = verdict.decide(_results(cc=False), sdp_gates=True, prior_autocloses=0, max_autoclose=1)
    assert v.subject == "Splunk Traffic Alarm validation result = Failed"
    # Enable B21 spacing is reproduced literally — note the DOUBLE space after
    # "Craigcool check :" and the single space on the other two.
    assert "Craigcool check :  FAILED" in v.email_body
    assert "Splunk OCC check : SUCCESSFUL" in v.email_body
    assert v.worklogs["craigcool"].startswith("CraigCool_Check_Results: FAILED")
    v2 = verdict.decide(_results(), sdp_gates=True, prior_autocloses=0, max_autoclose=1)
    assert v2.subject == "Splunk Traffic Alarm validation result = SUCCESSFUL"


def test_worklog_detail_is_capped():
    res = _results(occ=False)
    res["occ"] = checks.CheckResult(name="occ", ok=False, value="", rows=[f"row{i}" for i in range(35)], raw="")
    v = verdict.decide(res, sdp_gates=True, prior_autocloses=0, max_autoclose=1)
    wl = v.worklogs["occ"]
    assert wl.startswith("Splunk_OCC_Traffic_Status: FAILED")
    assert "row0" in wl and "row19" in wl
    assert "row20" not in wl
    assert "and 15 more row(s)" in wl


def test_worklog_detail_uncapped_when_under_limit():
    res = _results(site=False)
    res["site"] = checks.CheckResult(name="site", ok=False, value="", rows=["a", "b"], raw="")
    wl = verdict.decide(res, sdp_gates=True, prior_autocloses=0, max_autoclose=1).worklogs["site"]
    assert "a" in wl and "b" in wl and "more row(s)" not in wl


def test_email_body_matches_enable_b21_shape():
    """B21 builds exactly THREE check lines, <br>-joined, in this order.
    SDP is deliberately absent — it ships as an attachment (B32)."""
    v = verdict.decide(_results(), sdp_gates=False, prior_autocloses=0, max_autoclose=1)
    assert v.email_body == ("Craigcool check :  SUCCESSFUL"
                            "<br> Splunk Site check : SUCCESSFUL"
                            "<br> Splunk OCC check : SUCCESSFUL")
    assert "SDP" not in v.email_body


def test_pass_body_cannot_claim_success_for_a_failed_check():
    """B21's PASS branch hardcodes 'Successful' on all three lines. Ours is computed,
    so the body can never contradict the per-check result."""
    v = verdict.decide(_results(cc=False), sdp_gates=False, prior_autocloses=0, max_autoclose=1)
    assert "Craigcool check :  FAILED" in v.email_body
    assert v.passed is False


def test_sdp_absent_from_body_but_present_in_worklogs():
    """SDP is still measured and reported — just not in the email body."""
    v = verdict.decide(_results(sdp=False), sdp_gates=False, prior_autocloses=0, max_autoclose=1)
    assert "SDP" not in v.email_body
    assert v.worklogs["sdp"].startswith("SDP_Success_Status: FAILED")
    assert v.passed is True, "SDP does not gate — B21 gates on OCC, Site, CraigCool"

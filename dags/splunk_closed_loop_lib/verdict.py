"""Decision on typed booleans; prose rendered FROM the decision, never read back."""
from dataclasses import dataclass
from typing import Dict

from .checks import CheckResult

_WORKLOG_KEY = {"craigcool": "CraigCool_Check_Results", "site": "Splunk_Site_Traffic_Status",
                "occ": "Splunk_OCC_Traffic_Status", "sdp": "SDP_Success_Status"}

MAX_DETAIL_ROWS = 20


@dataclass(frozen=True)
class Verdict:
    passed: bool
    reason: str
    subject: str
    email_body: str
    worklogs: Dict[str, str]


def _status(r: CheckResult) -> str:
    return "SUCCESSFUL" if r.ok else "FAILED"


def decide(results: Dict[str, CheckResult], sdp_gates: bool, prior_autocloses: int, max_autoclose: int) -> Verdict:
    gating = ["occ", "site", "craigcool"] + (["sdp"] if sdp_gates else [])
    failed = [n for n in gating if not results[n].ok]
    errors = [f"{n}: {results[n].error}" for n in gating if results[n].error]
    passed = not failed
    reason = "all checks passed"
    if failed:
        reason = "failed: " + ", ".join(failed) + (("; errors: " + "; ".join(errors)) if errors else "")
    if passed and prior_autocloses >= max_autoclose:
        passed = False
        reason = f"recurrence guard: {prior_autocloses} auto-close(s) already in window (max {max_autoclose}) — escalated to human"
    worklogs = {}
    for n, r in results.items():
        shown_rows = r.rows[:MAX_DETAIL_ROWS]
        remaining = len(r.rows) - len(shown_rows)
        if remaining > 0:
            shown_rows = shown_rows + [f"… and {remaining} more row(s)"]
        detail = ("\n".join(shown_rows) if shown_rows else "") + (f"\nerror: {r.error}" if r.error else "")
        worklogs[n] = f"{_WORKLOG_KEY[n]}: {_status(r)}" + (f"\n{detail}" if detail else "")
    # Enable's Splunk_Calculation (B21) builds Email_Body with exactly THREE check
    # lines — Craigcool, Splunk Site, Splunk OCC. SDP is deliberately absent from the
    # body; it travels as the SDP_Success_Rate_Status attachment instead (B32).
    # Label text and spacing are reproduced literally, including the double space
    # after "Craigcool check :".
    #
    # One deliberate improvement: B21's PASS branch hardcodes the string "Successful"
    # on all three lines rather than reading the per-check status, so a body could
    # claim success for a check that had failed. Every line here is computed from the
    # check result, so the body cannot contradict the verdict.
    lines = ["Craigcool check :  " + _status(results["craigcool"]),
             "Splunk Site check : " + _status(results["site"]),
             "Splunk OCC check : " + _status(results["occ"])]
    body = "<br> ".join(lines)
    subject = "Splunk Traffic Alarm validation result = " + ("SUCCESSFUL" if passed else "Failed")
    return Verdict(passed=passed, reason=reason, subject=subject, email_body=body, worklogs=worklogs)

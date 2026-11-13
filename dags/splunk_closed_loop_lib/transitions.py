"""Ticket transition plans — the exact PATCH bodies, from Enable B23-B29 INPUTS (2026-08-23)."""
from typing import Dict, List


def _base(status: str, note: str, incident_number: str, fingerprint: str, customer_name: str, impact: str) -> Dict[str, str]:
    return {"name": "splunk_closed_loop", "status": status, "work_info_notes": note,
            "incident_number": incident_number, "fingerprint": fingerprint, "company": customer_name, "impact": impact}


def plan(passed: bool, worklogs: Dict[str, str], email_body: str, fail_path_resolves: bool,
         incident_number: str, fingerprint: str, customer_name: str, impact: str) -> List[Dict[str, str]]:
    b = lambda status, note: _base(status, note, incident_number, fingerprint, customer_name, impact)  # noqa: E731
    if passed:
        resolved = b("Resolved", worklogs["craigcool"])
        resolved.update({"status_reason": "No Further Action Required", "resolution": email_body,
                         "resolution_category_tier_1": "No Fault Found", "resolution_category_tier_2": "Resolved"})
        return [b("Assigned", worklogs["craigcool"]), b("In Progress", worklogs["craigcool"]), resolved,
                b("Closed", "Ticket closed as all checks are successful")]
    steps = [b("Assigned", worklogs["craigcool"]), b("In Progress", worklogs["site"])]
    if fail_path_resolves:
        steps.append(b("Resolved", worklogs["occ"]))
    else:
        # Worklog-only update: no redundant repeated "In Progress" status (the ESB
        # handler omits Status entirely when the caller doesn't supply one).
        third = b("In Progress", worklogs["occ"])
        del third["status"]
        steps.append(third)
    return steps


# E-1: notify must be gated on what the TICKET actually did, not on the verdict alone.
# transition_ticket is trigger_rule-independent of notify (notify is all_done), so a
# verdict that passed followed by a failed/never-run transition previously produced an
# "all clear / Resolved" email for an incident still sitting in Assigned or In Progress.
def start_record(steps: List[Dict[str, str]]) -> Dict:
    return {"ok": False, "planned": len(steps), "done": 0, "last_status": None}


def notify_gate(raw_verdict, record) -> Dict:
    """Decide what notify may claim. `record` absent means transition_ticket never ran
    or died mid-flight — that is a failure, never an all-clear."""
    if not raw_verdict:
        return {"mode": "no_verdict", "outcome": "error", "ticket_status": None}
    rec = record or {}
    if not rec.get("ok"):
        return {"mode": "transition_failed", "outcome": "error",
                "ticket_status": rec.get("last_status"),
                "done": rec.get("done", 0), "planned": rec.get("planned")}
    passed = bool(raw_verdict.get("passed"))
    # Only here — with every planned step confirmed — may the verdict wording be used.
    # N-7: the status is the one the ticket actually reached (Closed on PASS, In Progress
    # on FAIL), not Enable B21's hard-coded "Resolved"/"Assigned" — every email must be true.
    return {"mode": "verdict", "outcome": "pass" if passed else "fail",
            "ticket_status": rec.get("last_status")}

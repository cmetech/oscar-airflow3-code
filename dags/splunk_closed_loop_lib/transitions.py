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

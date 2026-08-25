"""Read-only helpers over middleware GET /ticketing-audit rows. HTTP is injected."""
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

AUTOCLOSE_MARK = "splunk_closed_loop:autoclose"
VERDICT_MARK = "splunk_closed_loop:verdict"
_TERMINAL = ("success", "failure", "timeout", "invalid_response")


def find_create(rows: List[Dict], ref: str) -> Optional[Dict]:
    # Never fall back to another row: the audit table holds every create row this
    # fingerprint ever had (including earlier TTL windows' tickets), and picking a
    # different ref would drive the DAG against a stale/unrelated incident. Without a
    # ref we cannot know which ticket is ours, so a ref is mandatory here.
    if not ref:
        raise ValueError("find_create requires the create ref")
    # On a TTL-reuse the alert's ticket_id label carries the INC, not the create ref (the
    # ITSM state consumer swaps it in) — so match on either the create trace_id or the
    # incident_number; both are strong keys unique to this ticket.
    for r in rows:
        if r.get("operation_type") == "create" and (r.get("trace_id") == ref or r.get("incident_number") == ref):
            if r.get("incident_number") or r.get("status") in ("failure", "timeout", "invalid_response"):
                return r
    return None


def find_update(rows: List[Dict], ref: str) -> Optional[Dict]:
    if not ref:
        # A falsy ref must never match a row that merely lacks a trace_id — that would let an
        # unrelated audit row "confirm" a transition that was never actually correlated.
        return None
    # DAG-originated transitions are inserted by the ITSM state consumer as operation_type='unknown'
    for r in rows:
        if r.get("operation_type") in ("update", "unknown") and r.get("trace_id") == ref and r.get("status") in _TERMINAL:
            return r
    return None


def wait_for(fetch: Callable[[], List[Dict]], predicate: Callable[[List[Dict]], Optional[Dict]],
             deadline_s: int, poll_s: int, sleep: Callable[[float], None] = time.sleep) -> Dict:
    waited = 0
    while True:
        row = predicate(fetch())
        if row is not None:
            return row
        if waited + poll_s > deadline_s:
            raise TimeoutError(f"no matching ticketing audit row after {deadline_s}s")
        sleep(poll_s)
        waited += poll_s


def _ts(row: Dict) -> Optional[datetime]:
    raw = row.get("created_at")
    if not raw:
        return None
    dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def count_markers(rows: List[Dict], marker: str, window_s: int, now: datetime) -> int:
    n = 0
    for r in rows:
        if marker in str(r.get("status_details", "")):
            ts = _ts(r)
            if ts and 0 <= (now - ts).total_seconds() <= window_s:
                n += 1
    return n


def count_autocloses(rows: List[Dict], window_s: int, now: datetime) -> int:
    return count_markers([r for r in rows if r.get("operation_type") == "close" and r.get("status") == "success"],
                         AUTOCLOSE_MARK, window_s, now)

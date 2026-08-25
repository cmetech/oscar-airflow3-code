"""The four health checks as pure evaluators. They NEVER raise: every failure
mode is a CheckResult(ok=False, error=...). Thresholds are passed in from the
task YAML — no constants here."""
import csv
import io
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

RATE_HEADERS = ("success_rate", "successrate", "success rate", "rate")
SITE_ERROR_HEADERS = ("sd_error", "sderror", "sd error")

# A legitimate success rate can sit slightly above 1.0 because success counts can
# exceed attempt counts in the source feed (witnessed 1.0030989448830516 in the
# captured SDP sample, 190330 successes against 189742 attempts). A percent-scaled
# feed instead reads values like 97 or 99.12. Nothing legitimate lands between —
# so only a value at or above this threshold is suspect enough to refuse to guess.
PERCENT_SCALE_SUSPECT = 2.0

_DURATION_RE = re.compile(r"^(\d+)([mh])$")


@dataclass
class CheckResult:
    name: str
    ok: bool
    value: str = ""
    rows: List[str] = field(default_factory=list)
    raw: str = ""
    error: Optional[str] = None


def errored(name: str, error: str) -> CheckResult:
    return CheckResult(name=name, ok=False, error=error)


def _rows(csv_text: str):
    reader = csv.reader(io.StringIO(csv_text))
    rows = [r for r in reader if r and any(c.strip() for c in r)]
    if len(rows) < 2:
        raise ValueError("CSV has no data rows")
    return rows[0], rows[1:]


def _line(row) -> str:
    out = io.StringIO()
    csv.writer(out, quoting=csv.QUOTE_ALL, lineterminator="").writerow(row)
    return out.getvalue()


def _validate_col(header, candidates, index: int) -> Optional[int]:
    """The index is authoritative — real feeds carry prefixed, repeated column names
    (Ro_voice_success_rate, Ro_Initial_Sum_sd_error AND Ro_Sum_sd_error in the same
    row) so *searching* the header for a candidate is unsafe: it can silently land on
    the wrong one (e.g. the leftmost `*_sd_error` match at index 6 instead of the
    index-9 column we actually need). Instead we trust the fixed index and use the
    header only to VALIDATE that it points at the right kind of column — matching by
    exact name or by suffix (case-insensitive, stripped/unquoted) to tolerate the
    real prefixed headers. Returns None (=> caller errors loudly) when the index is
    out of range or its header does not match any candidate."""
    if index >= len(header):
        return None
    cell = header[index].strip().strip('"').strip().lower()
    for candidate in candidates:
        if cell == candidate or cell.endswith(candidate):
            return index
    return None


def _eval_min_rate(name: str, csv_text: str, fallback_col: int, min_rate: float) -> CheckResult:
    try:
        header, data = _rows(csv_text)
        col = _validate_col(header, RATE_HEADERS, fallback_col)
        if col is None:
            return errored(name, f"index {fallback_col} header does not match a rate column (looked for {RATE_HEADERS})")
        flagged = []
        worst = None
        for row in data:
            try:
                rate = float(row[col].strip().strip('"'))
            except (ValueError, IndexError):
                return errored(name, f"unparseable value {row[col] if col < len(row) else '<missing>'!r} in row {_line(row)}")
            if rate >= PERCENT_SCALE_SUSPECT and min_rate <= 1.0:
                return errored(name, f"rate {rate} looks percent-scaled (>= {PERCENT_SCALE_SUSPECT}) but the threshold {min_rate} is a fraction — refusing to guess")
            if worst is None or rate < worst:
                worst = rate
            if rate < min_rate:
                flagged.append(_line(row))
        return CheckResult(name=name, ok=not flagged, value=str(worst), rows=flagged, raw=csv_text)
    except Exception as exc:  # noqa: BLE001 — never raise out of a check
        return errored(name, f"{type(exc).__name__}: {exc}")


def eval_sdp(csv_text: str, min_rate: float) -> CheckResult:
    return _eval_min_rate("sdp", csv_text, 2, min_rate)


def eval_occ(csv_text: str, min_rate: float) -> CheckResult:
    return _eval_min_rate("occ", csv_text, 2, min_rate)


def eval_site(csv_text: str, max_sd_error: float, sd_error_col: int) -> CheckResult:
    name = "site"
    try:
        header, data = _rows(csv_text)
        col = _validate_col(header, SITE_ERROR_HEADERS, sd_error_col)
        if col is None:
            return errored(name, f"index {sd_error_col} header does not match an sd_error column (looked for {SITE_ERROR_HEADERS})")
        flagged = []
        worst = None
        for row in data:
            try:
                sd = float(row[col].strip().strip('"'))
            except (ValueError, IndexError):
                return errored(name, f"unparseable sd_error in row {_line(row)}")
            if worst is None or sd > worst:
                worst = sd
            if sd > max_sd_error:
                flagged.append(_line(row))
        return CheckResult(name=name, ok=not flagged, value=str(worst), rows=flagged, raw=csv_text)
    except Exception as exc:  # noqa: BLE001
        return errored(name, f"{type(exc).__name__}: {exc}")


def eval_craigcool(row_count: int, max_rows: int) -> CheckResult:
    # < closes Enable's ==50 dead zone (spec defect 1)
    ok = row_count < max_rows
    return CheckResult(name="craigcool", ok=ok, value=str(row_count), rows=[], raw=str(row_count))


def craigcool_since(window: str, now: datetime) -> datetime:
    """Resolve the NMCDB lookback window from cfg.nmcdb.window (the YAML `today_cst`
    default, or a duration like `30m` / `2h`). Anything else is a config error and
    must fail loud rather than silently defaulting to the container's own timezone."""
    if window == "today_cst":
        cst_now = now.astimezone(ZoneInfo("America/Chicago"))
        return cst_now.replace(hour=0, minute=0, second=0, microsecond=0)
    m = _DURATION_RE.match(window.strip()) if isinstance(window, str) else None
    if not m:
        raise ValueError(f"unrecognised nmcdb.window value {window!r}: expected 'today_cst' or '<N>m'/'<N>h'")
    amount, unit = int(m.group(1)), m.group(2)
    delta = timedelta(minutes=amount) if unit == "m" else timedelta(hours=amount)
    # CST, exactly as the `today_cst` branch above and for the same reason: nmcdb.dt
    # is CST wall-clock and the caller strftime()s this value, dropping the offset.
    # Returning UTC here put the threshold 5-6 hours in the FUTURE, so
    # `COUNT(*) WHERE dt > <threshold>` was always 0 and the CraigCool gating check
    # could never fail -- a real outage would auto-close with link drops unmeasured.
    # Found by adversarial review 2026-08-26 (F-1, BLOCKER).
    cst_now = now.astimezone(ZoneInfo("America/Chicago"))
    return cst_now - delta

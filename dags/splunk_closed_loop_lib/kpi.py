"""splunk_kpi_type (stamped by ericsson-splunk-mapping.json) -> protocol/service/outcome."""
from dataclasses import dataclass
from typing import Dict, Optional

_PROTO = {"gy": ("Gy", "Data"), "sy": ("Sy", "Data"), "ro_messaging": ("Ro", "Messaging"), "ro_voice": ("Ro", "Voice")}


@dataclass(frozen=True)
class Kpi:
    kind: str        # aggregate | perentity
    protocol: str    # Gy | Sy | Ro
    service: str     # Data | Messaging | Voice
    outcome: Optional[str]  # errors | success | None
    family: str      # e.g. gy_initial, ro_voice_sum_rc_errors


def parse(kpi_type: str) -> Kpi:
    kind, _, rest = kpi_type.partition("_")
    if kind not in ("aggregate", "perentity") or not rest:
        raise ValueError(f"unknown splunk_kpi_type {kpi_type!r}")
    proto_key = next((p for p in sorted(_PROTO, key=len, reverse=True) if rest.startswith(p + "_")), None)
    if proto_key is None:
        raise ValueError(f"unknown protocol in splunk_kpi_type {kpi_type!r}")
    protocol, service = _PROTO[proto_key]
    outcome = "errors" if rest.endswith("sum_rc_errors") else "success" if rest.endswith("sum_rc_success") else None
    return Kpi(kind=kind, protocol=protocol, service=service, outcome=outcome, family=rest)


def _protocol_token(k: Kpi) -> str:
    # Enable's saved searches: occ_Gy_*, occ_Sy_*, occ_Ro_messaging_*, occ_Ro_voice_*
    return k.protocol if k.protocol != "Ro" else f"Ro_{k.service.lower()}"


def search_names(k: Kpi, searches: Dict[str, str]) -> Dict[str, str]:
    proto = _protocol_token(k)
    outcome = k.outcome or "success"
    return {
        "sdp": searches["sdp"],
        "occ_success": searches["occ_success"].format(protocol=proto),
        "site_result": searches["site_result"].format(protocol=proto, outcome=outcome),
    }

# Enable reads the site CSV's sd_error column by hardcoded index, and the index is
# PER PROTOCOL -- Splunk_Calculation (B21) uses temp102[9] for Gy, [12] for Sy, [6]
# for Ro Messaging and [9] for Ro Voice. The debug labels in that block are unreliable
# (Ro Voice's says "Sy_sum_ci_error"); the index is authoritative. Reading one index
# for every protocol evaluates the wrong column for Sy and Ro Messaging.
# Found by port-fidelity audit 2026-08-26 (F-3, HIGH).
_SITE_SD_ERROR_INDEX = {"Gy": 9, "Sy": 12, "Ro_messaging": 6, "Ro_voice": 9}


def site_sd_error_index(k: Kpi) -> int:
    """The sd_error column index for this alarm's protocol, as Enable indexes it."""
    token = _protocol_token(k)
    try:
        return _SITE_SD_ERROR_INDEX[token]
    except KeyError:  # pragma: no cover - guarded by parse()
        raise ValueError(f"no site sd_error column index known for protocol {token!r}")

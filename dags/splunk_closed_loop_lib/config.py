from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class IncWait:
    poll_s: int = 10
    deadline_s: int = 600


@dataclass(frozen=True)
class Thresholds:
    link_drops_max: int = 50
    site_sd_error_max: float = 1.0
    occ_success_min: float = 0.9990
    sdp_success_min: float = 0.97


@dataclass(frozen=True)
class SplunkCfg:
    base_url: str
    app_path: str
    conn_id: str
    timeout_s: int = 900
    verify_tls: bool = False
    searches: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class NmcdbCfg:
    conn_id: str
    window: str = "today_cst"


@dataclass(frozen=True)
class TicketCfg:
    customer_name: str = "ATTM-PPS-HOC-US"
    impact: str = "4-Minor/Localized"


@dataclass(frozen=True)
class EmailCfg:
    notifier: str = "SMTP"
    created_list: str = ""
    verdict_list: str = ""


@dataclass(frozen=True)
class RunConfig:
    alert: Dict[str, Any]
    fingerprint: str
    ticket_ref: Optional[str]
    kpi_type: str
    dag_id: str
    inc_wait: IncWait
    thresholds: Thresholds
    sdp_gates_verdict: bool
    fail_path_resolves: bool
    revalidate_on_reused_ticket: bool
    storm_min_interval_s: int
    recurrence_window_s: int
    recurrence_max_autoclose: int
    splunk: SplunkCfg
    nmcdb: NmcdbCfg
    ticket: TicketCfg
    email: EmailCfg


def _as_bool(value: Any, default: bool) -> bool:
    """Parse a boolean that may arrive as a real bool or as a string.

    The task DB stores every kwarg as a string, so `fail_path_resolves: false`
    in the YAML reaches the DAG as "False" -- and bare bool("False") is True,
    which would turn the FAIL path into a ticket-resolving path. Anything not
    recognisable is an error, never a guess: mis-reading one of these flags
    auto-closes a real outage.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "yes", "y", "on", "1"):
            return True
        if text in ("false", "no", "n", "off", "0"):
            return False
    raise ValueError(f"cannot read {value!r} as a boolean")


def _labels(alert: Dict[str, Any]) -> Dict[str, str]:
    return dict(alert.get("labels") or {})


def load(conf: Dict[str, Any]) -> RunConfig:
    alert = conf.get("alert") or {}
    cfg = conf.get("config") or {}
    labels = _labels(alert)
    fingerprint = labels.get("oscar_fingerprint") or labels.get("fingerprint") or alert.get("fingerprint")
    if not fingerprint:
        raise ValueError("alert has no fingerprint")
    kpi_type = labels.get("splunk_kpi_type", "")
    if not kpi_type:
        raise ValueError("alert has no splunk_kpi_type label — rule/mapping mismatch")
    splunk = cfg.get("splunk") or {}
    for required in ("base_url", "app_path", "conn_id"):
        if not splunk.get(required):
            raise ValueError(f"config.splunk.{required} missing")
    nmcdb = cfg.get("nmcdb") or {}
    if not nmcdb.get("conn_id"):
        raise ValueError("config.nmcdb.conn_id missing")
    _inc_wait = cfg.get("inc_wait") or {}
    th = cfg.get("thresholds") or {}
    occ_success_min = float(th.get("occ_success_min", 0.9990))
    sdp_success_min = float(th.get("sdp_success_min", 0.97))

    # Validate rate thresholds are fractions [0, 1.0]. A percent-authored value
    # (e.g. 99.90 instead of 0.9990) silently disables checks.py's percent-scale guard.
    if not (0 < occ_success_min <= 1.0):
        raise ValueError(f"config.thresholds.occ_success_min must be a fraction between 0 and 1 (e.g. 0.9990 for 99.90%), got {occ_success_min}")
    if not (0 < sdp_success_min <= 1.0):
        raise ValueError(f"config.thresholds.sdp_success_min must be a fraction between 0 and 1 (e.g. 0.97 for 97%), got {sdp_success_min}")

    return RunConfig(
        alert=alert,
        fingerprint=fingerprint,
        # conf["config"]["ticket_ref"] is the create row's trace_id, pinned by the task
        # and immutable. The alert's ticket_id label is NOT stable — it goes absent ->
        # create ref -> incident number as the async ESB handshake completes — so the
        # explicit ref wins whenever the task supplied one.
        ticket_ref=cfg.get("ticket_ref") or labels.get("ticket_id") or None,
        kpi_type=kpi_type,
        dag_id=cfg.get("dag_id", "splunk_closed_loop"),
        inc_wait=IncWait(
            # ints, not the strings the task DB hands back: these drive the
            # INC wait loop's sleep and deadline arithmetic.
            poll_s=int(_inc_wait.get("poll_s", 10)),
            deadline_s=int(_inc_wait.get("deadline_s", 600)),
        ),
        thresholds=Thresholds(
            link_drops_max=int(th.get("link_drops_max", 50)),
            site_sd_error_max=float(th.get("site_sd_error_max", 1)),
            occ_success_min=occ_success_min,
            sdp_success_min=sdp_success_min,
        ),
        sdp_gates_verdict=_as_bool(cfg.get("sdp_gates_verdict"), True),
        fail_path_resolves=_as_bool(cfg.get("fail_path_resolves"), False),
        revalidate_on_reused_ticket=_as_bool(cfg.get("revalidate_on_reused_ticket"), True),
        storm_min_interval_s=int((cfg.get("storm_guard") or {}).get("min_interval_s", 0)),
        recurrence_window_s=int((cfg.get("recurrence") or {}).get("window_s", 0)),
        recurrence_max_autoclose=int((cfg.get("recurrence") or {}).get("max_autoclose", 1)),
        splunk=SplunkCfg(base_url=splunk["base_url"].rstrip("/"), app_path=splunk["app_path"],
                         conn_id=splunk["conn_id"], timeout_s=int(splunk.get("timeout_s", 900)),
                         verify_tls=_as_bool(splunk.get("verify_tls"), False), searches=dict(splunk.get("searches") or {})),
        nmcdb=NmcdbCfg(conn_id=nmcdb["conn_id"], window=nmcdb.get("window", "today_cst")),
        ticket=TicketCfg(**(cfg.get("ticket") or {})),
        email=EmailCfg(**(cfg.get("email") or {})),
    )

import pytest

from splunk_closed_loop_lib import config, kpi

CONF = {
    "alert": {"labels": {"oscar_fingerprint": "fp1", "splunk_kpi_type": "perentity_ro_voice_sum_rc_errors",
                         "ticket_id": "ref-1", "incident_number": ""}},
    "config": {
        "dag_id": "splunk_closed_loop",
        "inc_wait": {"poll_s": 10, "deadline_s": 600},
        "thresholds": {"link_drops_max": 50, "site_sd_error_max": 1, "occ_success_min": 0.999, "sdp_success_min": 0.97},
        "sdp_gates_verdict": True, "fail_path_resolves": False, "revalidate_on_reused_ticket": True,
        "storm_guard": {"min_interval_s": 900}, "recurrence": {"window_s": 7200, "max_autoclose": 1},
        "splunk": {"base_url": "https://s:8089", "app_path": "/x/export", "conn_id": "splunk_kpi", "timeout_s": 900,
                   "verify_tls": False, "searches": {"sdp": "SDP: End to End Call Processing Success Rate",
                                                     "occ_success": "occ_{protocol}_success_rate_by_occ",
                                                     "site_result": "occ_{protocol}_result_code_by_site_{outcome}"}},
        "nmcdb": {"conn_id": "nmcdb", "window": "today_cst"},
        "ticket": {"customer_name": "ATTM-PPS-HOC-US", "impact": "4-Minor/Localized"},
        "email": {"notifier": "SMTP", "created_list": "l1", "verdict_list": "l2"},
    },
}


def test_load_config_typed():
    c = config.load(CONF)
    assert c.fingerprint == "fp1" and c.ticket_ref == "ref-1" and c.kpi_type == "perentity_ro_voice_sum_rc_errors"
    assert c.thresholds.link_drops_max == 50 and c.thresholds.occ_success_min == pytest.approx(0.999)
    assert c.inc_wait.deadline_s == 600 and c.storm_min_interval_s == 900 and c.recurrence_max_autoclose == 1
    assert c.splunk.searches["sdp"].startswith("SDP") and c.email.verdict_list == "l2"


def test_load_config_missing_required_raises():
    bad = {"alert": {"labels": {"oscar_fingerprint": "fp"}}, "config": {}}
    with pytest.raises(ValueError, match="splunk_kpi_type"):
        config.load(bad)


@pytest.mark.parametrize("kpi_type,kind,proto,service,outcome", [
    ("aggregate_gy_initial", "aggregate", "Gy", "Data", None),
    ("aggregate_sy_success_rate", "aggregate", "Sy", "Data", None),
    ("perentity_gy_sum_rc_errors", "perentity", "Gy", "Data", "errors"),
    ("perentity_sy_sum_rc_success", "perentity", "Sy", "Data", "success"),
    ("perentity_ro_messaging_sum_rc_errors", "perentity", "Ro", "Messaging", "errors"),
    ("aggregate_ro_voice_request_total", "aggregate", "Ro", "Voice", None),
])
def test_parse_kpi(kpi_type, kind, proto, service, outcome):
    k = kpi.parse(kpi_type)
    assert (k.kind, k.protocol, k.service, k.outcome) == (kind, proto, service, outcome)


def test_parse_kpi_unknown_raises():
    with pytest.raises(ValueError):
        kpi.parse("aggregate_xx_thing")


def test_search_names_templating():
    k = kpi.parse("perentity_ro_messaging_sum_rc_errors")
    names = kpi.search_names(k, CONF["config"]["splunk"]["searches"])
    assert names == {"sdp": "SDP: End to End Call Processing Success Rate",
                     "occ_success": "occ_Ro_messaging_success_rate_by_occ",
                     "site_result": "occ_Ro_messaging_result_code_by_site_errors"}


def test_search_names_default_outcome_is_success():
    k = kpi.parse("aggregate_gy_initial")
    names = kpi.search_names(k, CONF["config"]["splunk"]["searches"])
    assert names["occ_success"] == "occ_Gy_success_rate_by_occ"
    assert names["site_result"] == "occ_Gy_result_code_by_site_success"


def test_occ_success_min_percent_value_raises():
    """Reject percent-authored occ_success_min (e.g. 99.90 instead of 0.9990)."""
    bad = dict(CONF)
    bad["config"] = dict(CONF["config"])
    bad["config"]["thresholds"] = dict(CONF["config"]["thresholds"])
    bad["config"]["thresholds"]["occ_success_min"] = 99.90
    with pytest.raises(ValueError, match="occ_success_min must be a fraction.*got 99.9"):
        config.load(bad)


def test_sdp_success_min_percent_value_raises():
    """Reject percent-authored sdp_success_min (e.g. 97 instead of 0.97)."""
    bad = dict(CONF)
    bad["config"] = dict(CONF["config"])
    bad["config"]["thresholds"] = dict(CONF["config"]["thresholds"])
    bad["config"]["thresholds"]["sdp_success_min"] = 97
    with pytest.raises(ValueError, match="sdp_success_min must be a fraction.*got 97"):
        config.load(bad)


def test_rate_thresholds_accept_1_0_boundary():
    """Fractional threshold of 1.0 (100% success) is valid."""
    valid = dict(CONF)
    valid["config"] = dict(CONF["config"])
    valid["config"]["thresholds"] = dict(CONF["config"]["thresholds"])
    valid["config"]["thresholds"]["occ_success_min"] = 1.0
    valid["config"]["thresholds"]["sdp_success_min"] = 1.0
    c = config.load(valid)
    assert c.thresholds.occ_success_min == 1.0
    assert c.thresholds.sdp_success_min == 1.0

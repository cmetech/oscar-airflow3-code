from pathlib import Path

import pytest

from splunk_closed_loop_lib import checks

FX = Path(__file__).parent / "fixtures"

# sdp.csv / occ.csv / site.csv are verbatim rows captured from real Splunk output —
# see att_splunk_closed_loop.md, section "Splunk Response Samples" (captured 2023-05).


def test_sdp_passes_on_real_captured_rows():
    # Real SDP sample: rates are 1 and 1.0030989448830516 — both pass a 0.97
    # threshold. The second is the regression guard for Fix 1 (percent-scale guard
    # must not misfire on a legitimate rate slightly above 1.0).
    r = checks.eval_sdp((FX / "sdp.csv").read_text(), 0.97)
    assert r.name == "sdp" and r.ok is True and r.error is None
    assert r.rows == []


def test_occ_passes_on_real_captured_rows():
    r = checks.eval_occ((FX / "occ.csv").read_text(), 0.97)
    assert r.ok is True and r.error is None


def test_site_passes_on_real_captured_rows():
    # Both real sample rows carry sd_error 0 at index 9 (Ro_Sum_sd_error).
    r = checks.eval_site((FX / "site.csv").read_text(), 1, 9)
    assert r.ok is True and r.error is None


def test_sdp_passes_when_all_above():
    r = checks.eval_sdp('"host","t","rate"\n"a","x","0.98"\n', 0.97)
    assert r.ok is True and r.rows == []


def test_sdp_flags_low_rate_as_fail_not_error():
    r = checks.eval_sdp('"_time",host,"success_rate"\n"t",plasdp01,"0.5"\n', 0.97)
    assert r.ok is False and r.error is None
    assert "plasdp01" in r.rows[0]


def test_site_flags_sd_error_above_max():
    csv_text = (
        '"site","t","a","b","c","d","e","f","g","sd_error"\n'
        '"s1","x","","","","","","","","0.4"\n'
        '"s2","x","","","","","","","","2.7"\n'
    )
    r = checks.eval_site(csv_text, 1, 9)
    assert r.ok is False and len(r.rows) == 1 and "s2" in r.rows[0]


def test_site_boundary_equal_is_ok():
    r = checks.eval_site('"site","t","a","b","c","d","e","f","g","sd_error"\n"s","x","","","","","","","","1.0"\n', 1, 9)
    assert r.ok is True


@pytest.mark.parametrize("count,ok", [(0, True), (49, True), (50, False), (51, False)])
def test_craigcool_boundary(count, ok):
    assert checks.eval_craigcool(count, 50).ok is ok


def test_empty_csv_is_error_not_pass():
    r = checks.eval_occ("", 0.999)
    assert r.ok is False and r.error is not None


def test_unparseable_value_is_error_not_pass():
    r = checks.eval_sdp('"h","t","rate"\n"a","x","n/a"\n', 0.97)
    assert r.ok is False and "n/a" in (r.error or "")


def test_errored_helper():
    r = checks.errored("occ", "timeout")
    assert r.ok is False and r.error == "timeout" and r.name == "occ"


def test_sdp_column_index_is_authoritative_not_searched():
    # The real feed's rate column is fixed at index 2 (fallback_col=2 for eval_sdp).
    # If the header at that index doesn't look like a rate column, the check must
    # error loudly rather than search the row for one elsewhere — searching is what
    # risks landing on the wrong repeated/prefixed column in the real data.
    csv_text = '"host","success_rate","time"\n"a","0.50","x"\n'
    r = checks.eval_sdp(csv_text, 0.97)
    assert r.ok is False and r.error is not None
    assert "does not match a rate column" in r.error


def test_sdp_errors_when_index_header_unrecognised():
    csv_text = '"h1","h2","h3"\n"a","x","0.98"\n'
    r = checks.eval_sdp(csv_text, 0.97)
    assert r.ok is False and r.error is not None


def test_occ_accepts_prefixed_rate_header_at_the_authoritative_index():
    # Real OCC header is "Ro_voice_success_rate" (prefixed), not a bare "success_rate".
    # Index validation must match by suffix.
    csv_text = '"_time","occ_server","Ro_voice_success_rate"\n"t","srv","0.50"\n'
    r = checks.eval_occ(csv_text, 0.97)
    assert r.ok is False and r.error is None
    assert "srv" in r.rows[0]


def test_site_uses_index_9_ro_sum_sd_error_not_index_6_ro_initial_sum_sd_error():
    # Real site header has TWO *_sd_error columns: Ro_Initial_Sum_sd_error at index 6
    # and Ro_Sum_sd_error at index 9. A search-based resolver would pick the leftmost
    # (index 6, wrong). Craft a row where the two values straddle the threshold so a
    # wrong-column read flips the verdict: index 6 = 0.2 (would pass), index 9 = 5.0
    # (must fail).
    header = (
        '"_time",resultCode,site,"rc_site","Ro_Initial_Sum","Ro_Initial_Sum_error",'
        '"Ro_Initial_Sum_sd_error","Ro_Sum","Ro_Sum_error","Ro_Sum_sd_error"\n'
    )
    row = '"t",1,akr,"1_akr",0,0,0.2,0,0,5.0\n'
    r = checks.eval_site(header + row, 1, 9)
    assert r.ok is False and r.error is None
    assert "akr" in r.rows[0]


def test_site_errors_when_index_9_header_does_not_end_in_sd_error():
    csv_text = '"a","b","c","d","e","f","g","h","i","not_the_right_column"\n"1","2","3","4","5","6","7","8","9","10"\n'
    r = checks.eval_site(csv_text, 1, 9)
    assert r.ok is False and r.error is not None
    assert "does not match an sd_error column" in r.error


def test_percent_scaled_rate_errors_instead_of_passing():
    """A rate of 99.12 against a fractional threshold (e.g. 0.97) must never be
    silently accepted — guessing the scale is how a real outage gets auto-closed."""
    csv_text = '"host","time","success_rate"\n"a","x","99.12"\n'
    r = checks.eval_sdp(csv_text, 0.97)
    assert r.ok is False
    assert "percent-scaled" in r.error


def test_rate_just_above_one_does_not_misfire_the_percent_scale_guard():
    # Regression guard for Fix 1: the captured SDP sample legitimately reports
    # 1.0030989448830516 (190330 successes against 189742 attempts). That must be
    # evaluated as a normal rate, not rejected as percent-scaled.
    csv_text = '"_time",host,"success_rate"\n"t",plasdp60ahp,"1.0030989448830516"\n'
    r = checks.eval_sdp(csv_text, 0.97)
    assert r.ok is True and r.error is None


def test_sdp_value_carries_worst_measured_rate_not_the_threshold():
    csv_text = '"_time",host,"success_rate"\n"a","x","0.99"\n"b","x","0.80"\n'
    r = checks.eval_sdp(csv_text, 0.97)
    assert r.value == "0.8"


def test_site_value_carries_worst_measured_sd_error():
    csv_text = (
        '"site","t","a","b","c","d","e","f","g","sd_error"\n'
        '"s1","x","","","","","","","","0.4"\n'
        '"s2","x","","","","","","","","2.7"\n'
    )
    r = checks.eval_site(csv_text, 1, 9)
    assert r.value == "2.7"


def test_craigcool_since_today_cst_is_midnight_chicago():
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    now = datetime(2026, 8, 24, 18, 30, tzinfo=timezone.utc)
    since = checks.craigcool_since("today_cst", now)
    assert since == datetime(2026, 8, 24, 0, 0, tzinfo=ZoneInfo("America/Chicago"))


def test_craigcool_since_duration_minutes():
    from datetime import datetime, timedelta, timezone
    now = datetime(2026, 8, 24, 18, 30, tzinfo=timezone.utc)
    since = checks.craigcool_since("30m", now)
    assert since == now - timedelta(minutes=30)


def test_craigcool_since_duration_hours():
    from datetime import datetime, timedelta, timezone
    now = datetime(2026, 8, 24, 18, 30, tzinfo=timezone.utc)
    since = checks.craigcool_since("2h", now)
    assert since == now - timedelta(hours=2)


def test_craigcool_since_rejects_unrecognised_window():
    from datetime import datetime, timezone
    with pytest.raises(ValueError, match="unrecognised nmcdb.window"):
        checks.craigcool_since("bogus", datetime(2026, 8, 24, tzinfo=timezone.utc))


# --- F-1 (BLOCKER, adversarial review 2026-08-26) -----------------------------
# nmcdb.dt is CST wall-clock and the caller strftime()s this value, dropping the
# offset. A UTC return put the threshold 5-6h in the FUTURE, so COUNT(*) was always
# 0 and the CraigCool gating check could never fail.

def test_duration_window_is_cst_not_utc():
    from datetime import datetime, timezone
    now = datetime(2026, 8, 26, 2, 0, 0, tzinfo=timezone.utc)   # 21:00 CST on the 25th
    got = checks.craigcool_since("15m", now).strftime("%Y-%m-%d %H:%M:%S")
    assert got == "2026-08-25 20:45:00", got


def test_every_window_lands_in_the_past_in_cst_terms():
    """The threshold must always be behind CST 'now', or the query matches nothing."""
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    now = datetime(2026, 8, 26, 2, 0, 0, tzinfo=timezone.utc)
    cst_now = now.astimezone(ZoneInfo("America/Chicago")).replace(tzinfo=None)
    for window in ("15m", "30m", "2h", "today_cst"):
        since = checks.craigcool_since(window, now).replace(tzinfo=None)
        assert since < cst_now, f"{window} -> {since} is not before CST now {cst_now}"

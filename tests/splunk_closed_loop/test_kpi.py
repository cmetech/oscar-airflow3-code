from splunk_closed_loop_lib.kpi import parse, search_names, site_sd_error_index



# --- F-3 (HIGH, port-fidelity audit 2026-08-26) -------------------------------
# Enable's Splunk_Calculation reads the site sd_error column by hardcoded index and
# the index is PER PROTOCOL: Gy temp102[9], Sy temp102[12], Ro Messaging temp102[6],
# Ro Voice temp102[9]. We previously used 9 for all four, so Sy and Ro Messaging
# evaluated the wrong column -- a wrong verdict, or a permanent validation error and
# no auto-close for those protocols.

def test_site_sd_error_index_is_per_protocol_as_enable_indexes_it():
    cases = {
        "perentity_gy_sum_rc_errors": 9,
        "perentity_sy_sum_rc_errors": 12,
        "perentity_ro_messaging_sum_rc_errors": 6,
        "perentity_ro_voice_sum_rc_errors": 9,
    }
    for kpi_type, expected in cases.items():
        got = site_sd_error_index(parse(kpi_type))
        assert got == expected, f"{kpi_type}: got {got}, Enable uses {expected}"


def test_every_perentity_type_has_a_site_column_index():
    """No per-entity alarm may reach the site check without a known column."""
    for proto in ("gy", "sy", "ro_messaging", "ro_voice"):
        for outcome in ("errors", "success"):
            k = parse(f"perentity_{proto}_sum_rc_{outcome}")
            assert isinstance(site_sd_error_index(k), int)

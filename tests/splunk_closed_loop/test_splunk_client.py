import httpx

from splunk_closed_loop_lib import splunk_client


def test_export_csv_posts_saved_search():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["url"] = str(req.url); seen["auth"] = req.headers["authorization"]; seen["body"] = req.content.decode()
        return httpx.Response(200, text='"h","t","r"\n"a","x","0.99"\n')

    out = splunk_client.export_csv("https://s:8089", "/x/export", "TOK", 'occ_Gy_success_rate_by_occ', 30, False,
                                   transport=httpx.MockTransport(handler))
    assert out.startswith('"h"')
    assert seen["url"] == "https://s:8089/x/export" and seen["auth"] == "Bearer TOK"
    assert "output_mode=csv" in seen["body"] and "savedsearch" in seen["body"] and "occ_Gy_success_rate_by_occ" in seen["body"]


def test_export_csv_raises_on_http_error():
    import pytest
    def handler(req): return httpx.Response(503, text="down")
    with pytest.raises(httpx.HTTPStatusError):
        splunk_client.export_csv("https://s:8089", "/x", "T", "s", 30, False, transport=httpx.MockTransport(handler))


# --- base_url_from_conn: OSCAR stores host/port/scheme as separate fields ---

def test_base_url_built_from_separate_conn_fields():
    assert splunk_client.base_url_from_conn("dev-sim-splunk", "http", 8089, "https://fallback:8089") == "http://dev-sim-splunk:8089"


def test_base_url_defaults_to_https_when_schema_absent():
    assert splunk_client.base_url_from_conn("10.202.11.105", None, 8089, "https://fallback:8089") == "https://10.202.11.105:8089"
    assert splunk_client.base_url_from_conn("10.202.11.105", "", 8089, "https://fallback:8089") == "https://10.202.11.105:8089"


def test_base_url_omits_port_when_absent():
    assert splunk_client.base_url_from_conn("splunk.example", "https", None, "https://fallback:8089") == "https://splunk.example"


def test_base_url_passes_through_a_host_that_already_has_a_scheme():
    assert splunk_client.base_url_from_conn("http://already:8089", None, None, "https://fallback:8089") == "http://already:8089"


def test_base_url_falls_back_to_yaml_when_conn_has_no_host():
    assert splunk_client.base_url_from_conn(None, "https", 8089, "https://fallback:8089") == "https://fallback:8089"
    assert splunk_client.base_url_from_conn("", None, None, "https://fallback:8089") == "https://fallback:8089"

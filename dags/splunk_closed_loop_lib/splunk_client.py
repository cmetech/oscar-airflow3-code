"""Splunk saved-search export (the 3 Enable Splunk_REST_API_Call blocks)."""
from typing import Optional

import httpx


def export_csv(base_url: str, app_path: str, token: str, search_name: str, timeout_s: int, verify_tls: bool,
               transport: Optional[httpx.BaseTransport] = None) -> str:
    url = f"{base_url.rstrip('/')}{app_path}"
    data = {"output_mode": "csv", "search": f'savedsearch "{search_name}"'}
    with httpx.Client(verify=verify_tls, timeout=timeout_s, transport=transport) as client:
        resp = client.post(url, data=data, headers={"Authorization": f"Bearer {token}"})
        resp.raise_for_status()
        return resp.text


def base_url_from_conn(host, schema, port, fallback: str) -> str:
    """Build the Splunk base URL from an OSCAR connection.

    OSCAR stores connections as separate fields -- bare host, numeric port, and
    the protocol in `schema` (the same convention OscarHook uses) -- because the
    middleware serialises them into a `conn_type://login:password@host:port`
    URI. A host carrying its own scheme would produce a double-scheme URI that
    Airflow refuses to parse, so callers must not store one; if one shows up
    anyway it is already a complete base URL and is passed straight through.

    With no connection host at all, the YAML's splunk.base_url is used.
    """
    if not host:
        return fallback
    host = str(host)
    if "://" in host:
        return host
    scheme = (schema or "https").strip() or "https"
    return f"{scheme}://{host}:{port}" if port else f"{scheme}://{host}"

import os
from typing import Optional
from airflow.hooks.base import BaseHook


class OscarHook(BaseHook):
    """
    Base hook for accessing middleware connections.

    This hook provides a standardized connection setup that can be reused by all Oscar hooks.
    It initializes the following attributes from an Airflow connection (if available) or
    falls back to environment variables:
      - host: e.g. from MIDDLEWARE_HOST (default: "middleware")
      - port: e.g. from MIDDLEWARE_PORT (default: "5200")
      - protocol: e.g. from MIDDLEWARE_PROTOCOL (default: "https")
      - verify_ssl: either from connection extra 'verify_ssl' or from SSL_VERIFY env var.

    Subclasses should set:
      - conn_name_attr: used when calling get_connection
      - default_conn_name: the connection ID to use in absence of one provided at instantiation.
    """
    conn_name_attr = None       # Subclasses should override if desired.
    default_conn_name = None    # Subclasses may set a default connection name.

    def __init__(self, conn_id: Optional[str] = None) -> None:
        super().__init__()
        self.conn_id = conn_id or self.default_conn_name
        self._setup_connection()

    def _setup_connection(self) -> None:
        if self.conn_id:
            conn = self.get_connection(self.conn_id)
            self.host = conn.host or os.environ.get("MIDDLEWARE_HOST", "middleware")
            self.port = conn.port or os.environ.get("MIDDLEWARE_PORT", "5200")
            self.protocol = conn.schema or os.environ.get("MIDDLEWARE_PROTOCOL", "https")
            extras = conn.extra_dejson if conn.extra_dejson else {}
            self.verify_ssl = extras.get("verify_ssl", False)
        else:
            self.host = os.environ.get("MIDDLEWARE_HOST", "middleware")
            self.port = os.environ.get("MIDDLEWARE_PORT", "5200")
            self.protocol = os.environ.get("MIDDLEWARE_PROTOCOL", "https")
            self.verify_ssl = os.environ.get("SSL_VERIFY", "false").lower() == "true"

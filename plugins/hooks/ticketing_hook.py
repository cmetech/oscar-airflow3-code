import os
import logging
from typing import Any, Dict, List, Optional

import httpx
from hooks.oscar_hook import OscarHook


logger = logging.getLogger(__name__)


class TicketingHook(OscarHook):
    """
    Hook for interacting with the middleware ticketing APIs under `/api/v1/tickets`.

    Covers:
        - Tickets: create, get, search, update, update-by-identifier
        - Service Requests: CRUD-like operations and search
        - Work Orders: get, update, search, work logs create
        - Work Info (SR work info): create
        - Ticketing Audit: list, statistics, export, enhanced export, detail

    Connection configuration is inherited from OscarHook and taken from
    Airflow connection or environment variables:
        - MIDDLEWARE_HOST (default: "middleware")
        - MIDDLEWARE_PORT (default: "5200")
        - MIDDLEWARE_PROTOCOL (default: "https")
        - SSL_VERIFY (default: "false")
        - MIDDLEWARE_APIKEY (optional header X-API-Key)
    """

    def __init__(self, conn_id: Optional[str] = None) -> None:
        super().__init__(conn_id)
        self.base_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/tickets"
        self.headers = {"Content-Type": "application/json", "X-Internal-Service": "airflow"}
        api_key = os.environ.get("MIDDLEWARE_APIKEY")
        if api_key:
            self.headers["X-API-Key"] = api_key
        logger.info(f"Initialized TicketingHook with base URL: {self.base_url}")

    # -----------------------
    # Ticket endpoints
    # -----------------------
    def create_ticket(self, ticket: Dict[str, Any], system: Optional[str] = None) -> Dict[str, Any]:
        params = {"system": system} if system else None
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.post(self.base_url, headers=self.headers, params=params, json=ticket)
            response.raise_for_status()
            return response.json()

    def get_ticket(self, ticket_id: str, system: Optional[str] = None) -> Dict[str, Any]:
        params = {"system": system} if system else None
        url = f"{self.base_url}/{ticket_id}"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()

    def search_tickets(
        self,
        filters: Optional[Dict[str, Any]] = None,
        page: int = 1,
        per_page: int = 20,
        system: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"page": page, "per_page": per_page}
        if system:
            params["system"] = system
        if filters:
            # Flatten into query params as middleware expects individual keys
            for k, v in filters.items():
                if v is not None:
                    params[k] = v
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.get(self.base_url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()

    def update_ticket(self, ticket_id: str, ticket_update: Dict[str, Any], system: Optional[str] = None) -> Dict[str, Any]:
        params = {"system": system} if system else None
        url = f"{self.base_url}/{ticket_id}"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.patch(url, headers=self.headers, params=params, json=ticket_update)
            response.raise_for_status()
            return response.json()

    def update_ticket_by_identifier(
        self,
        ticket_update: Dict[str, Any],
        ticket_id: Optional[str] = None,
        incident_no: Optional[str] = None,
        system: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not ticket_id and not incident_no:
            raise ValueError("Either ticket_id or incident_no must be provided")
        params: Dict[str, Any] = {}
        if ticket_id:
            params["ticket_id"] = ticket_id
        if incident_no:
            params["incident_no"] = incident_no
        if system:
            params["system"] = system
        url = f"{self.base_url}/update-by-identifier"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.patch(url, headers=self.headers, params=params, json=ticket_update)
            response.raise_for_status()
            return response.json()

    # -----------------------
    # Service Request endpoints
    # -----------------------
    def create_service_request(self, service_request: Dict[str, Any], system: Optional[str] = None) -> Dict[str, Any]:
        params = {"system": system} if system else None
        url = f"{self.base_url}/service-requests"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.post(url, headers=self.headers, params=params, json=service_request)
            response.raise_for_status()
            return response.json()

    def get_service_request(self, request_id: str, system: Optional[str] = None) -> Dict[str, Any]:
        params = {"system": system} if system else None
        url = f"{self.base_url}/service-requests/{request_id}"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()

    def update_service_request(self, request_id: str, service_request: Dict[str, Any], system: Optional[str] = None) -> Dict[str, Any]:
        params = {"system": system} if system else None
        url = f"{self.base_url}/service-requests/{request_id}"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.put(url, headers=self.headers, params=params, json=service_request)
            response.raise_for_status()
            return response.json()

    def search_service_requests(
        self,
        filters: Optional[Dict[str, Any]] = None,
        page: int = 1,
        per_page: int = 20,
        system: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"page": page, "per_page": per_page}
        if system:
            params["system"] = system
        if filters:
            for k, v in filters.items():
                if v is not None:
                    params[k] = v
        url = f"{self.base_url}/service-requests"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()

    def create_sr_work_info(self, work_info: Dict[str, Any], system: Optional[str] = None) -> Dict[str, Any]:
        params = {"system": system} if system else None
        url = f"{self.base_url}/service-requests/work-info"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.post(url, headers=self.headers, params=params, json=work_info)
            response.raise_for_status()
            return response.json()

    # -----------------------
    # Work Order endpoints
    # -----------------------
    def get_work_order(self, work_order_id: str, system: Optional[str] = None) -> Dict[str, Any]:
        params = {"system": system} if system else None
        url = f"{self.base_url}/work-orders/{work_order_id}"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()

    def update_work_order(self, work_order_id: str, work_order: Dict[str, Any], system: Optional[str] = None) -> Dict[str, Any]:
        params = {"system": system} if system else None
        url = f"{self.base_url}/work-orders/{work_order_id}"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.put(url, headers=self.headers, params=params, json=work_order)
            response.raise_for_status()
            return response.json()

    def search_work_orders(
        self,
        filters: Optional[Dict[str, Any]] = None,
        page: int = 1,
        per_page: int = 20,
        system: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"page": page, "per_page": per_page}
        if system:
            params["system"] = system
        if filters:
            for k, v in filters.items():
                if v is not None:
                    params[k] = v
        url = f"{self.base_url}/work-orders"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()

    def create_work_log(self, work_log: Dict[str, Any], system: Optional[str] = None) -> Dict[str, Any]:
        params = {"system": system} if system else None
        url = f"{self.base_url}/work-orders/work-logs"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.post(url, headers=self.headers, params=params, json=work_log)
            response.raise_for_status()
            return response.json()

    # -----------------------
    # Ticketing Audit endpoints (proxy to taskmanager via middleware)
    # -----------------------
    def list_ticketing_audit(
        self,
        query: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        params = query or {}
        url = f"{self.base_url}/ticketing-audit"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()

    def ticketing_audit_statistics(self, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = query or {}
        url = f"{self.base_url}/ticketing-audit/statistics"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()

    def export_ticketing_audit(self, query: Optional[Dict[str, Any]] = None, enhanced: bool = False) -> bytes:
        params = query or {}
        path = "export-enhanced" if enhanced else "export"
        url = f"{self.base_url}/ticketing-audit/{path}"
        timeout = 120.0 if enhanced else 60.0
        with httpx.Client(verify=self.verify_ssl, timeout=timeout, follow_redirects=True) as client:
            response = client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.content

    def get_ticketing_audit_detail(self, audit_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/ticketing-audit/{audit_id}"
        with httpx.Client(verify=self.verify_ssl, timeout=30.0, follow_redirects=True) as client:
            response = client.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()

"""Evaluate published OSCAR Decisions through Middleware."""

import re

import httpx

from hooks.oscar_hook import OscarHook


class DecisionHook(OscarHook):
    default_conn_name = "oscar_decisions"

    def __init__(self, conn_id=None):
        super().__init__(conn_id)
        self.api_key = self.get_connection(self.conn_id).password
        if not isinstance(self.api_key, str) or not self.api_key or len(self.api_key) > 255:
            raise ValueError("Decision API key is missing from the Airflow connection")

    def evaluate(self, definition_id, input, *, revision=None, allowed_choices=None):
        if not isinstance(definition_id, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", definition_id):
            raise ValueError("Decision definition id is invalid")
        if not isinstance(input, dict):
            raise ValueError("Decision input must be an object")
        payload = {"input": input}
        if revision is not None:
            payload["revision"] = revision
        if allowed_choices is not None:
            payload["allowed_choices"] = allowed_choices
        url = f"{self.protocol}://{self.host}:{self.port}/api/v1/decisions/evaluate/{definition_id}"
        with httpx.Client(verify=self.verify_ssl, timeout=5.0) as client:
            response = client.post(url, json=payload, headers={"X-API-Key": self.api_key})
            response.raise_for_status()
            return response.json()

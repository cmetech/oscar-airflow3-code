import os
import logging
from typing import Optional, Dict, Any
import httpx
from hooks.oscar_hook import OscarHook

logger = logging.getLogger(__name__)


class RuleHook(OscarHook):
    """
    Hook for interacting with the Rules API through the middleware.

    This hook provides methods for evaluating a rule by name, using a properties dictionary.
    """
    # conn_name_attr = 'rules_conn_id'
    # default_conn_name = 'rules_default'
    # conn_type = 'rules_api'
    # hook_name = 'Rules'

    def __init__(self, conn_id: Optional[str] = None, namespace: Optional[str] = None):
        """
        :param conn_id: The Airflow connection ID to retrieve connection info.
        :param namespace: The default namespace to use in evaluation requests.
        """
        self.namespace = namespace
        super().__init__(conn_id)
        # Build the base URL for the rules API.
        self.base_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/rules"
        # Setup headers for internal service authentication
        self.headers = {"Content-Type": "application/json", "X-Internal-Service": "airflow"}
        logger.info(f"Initialized RuleHook with base URL: {self.base_url}")

    def get_rule(self, rule_name: str, namespace: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get a rule by name from the middleware.

        :param rule_name: The rule name to retrieve.
        :param namespace: The namespace of the rule (defaults to self.namespace).
        :return: The rule data if found, None otherwise.
        """
        namespace = namespace or self.namespace
        url = f"{self.base_url}?namespace={namespace}"

        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.get(url, headers=self.headers)
                response.raise_for_status()

                rules_data = response.json()
                rules = rules_data.get("rules", [])

                # Find the specific rule by name
                for rule in rules:
                    if rule.get("name") == rule_name:
                        logger.info(f"Successfully retrieved rule '{rule_name}'")
                        return rule

                logger.info(f"Rule '{rule_name}' not found in namespace '{namespace}'")
                return None

        except httpx.HTTPStatusError as exc:
            logger.error(f"Error getting rule '{rule_name}': {exc.response.text}")
            return None
        except httpx.RequestError as exc:
            logger.error(f"Network error while getting rule: {exc}")
            return None

    def create_rule(self, rule_data: Dict[str, Any], namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new rule in the middleware.

        :param rule_data: The rule data dictionary.
        :param namespace: The namespace for the rule (defaults to self.namespace).
        :return: The created rule data.
        """
        namespace = namespace or self.namespace

        # Ensure namespace is set in rule data
        rule_data["namespace"] = namespace

        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(self.base_url, json=rule_data, headers=self.headers)
                response.raise_for_status()
                logger.info(f"Successfully created rule '{rule_data.get('name')}'")
                return response.json()
        except httpx.HTTPStatusError as exc:
            logger.error(f"Error creating rule '{rule_data.get('name')}': {exc.response.text}")
            raise
        except httpx.RequestError as exc:
            logger.error(f"Network error while creating rule: {exc}")
            raise

    def update_rule(self, rule_name: str, rule_data: Dict[str, Any], namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Update an existing rule in the middleware.

        :param rule_name: The name of the rule to update.
        :param rule_data: The updated rule data.
        :param namespace: The namespace of the rule (defaults to self.namespace).
        :return: The updated rule data.
        """
        namespace = namespace or self.namespace
        url = f"{self.base_url}/{rule_name}?namespace={namespace}"

        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.put(url, json=rule_data, headers=self.headers)
                response.raise_for_status()
                logger.info(f"Successfully updated rule '{rule_name}'")
                return response.json()
        except httpx.HTTPStatusError as exc:
            logger.error(f"Error updating rule '{rule_name}': {exc.response.text}")
            raise
        except httpx.RequestError as exc:
            logger.error(f"Network error while updating rule: {exc}")
            raise

    def evaluate_rules(self, name: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate a rule by sending a POST request to the middleware's /rules/evaluate endpoint.

        This method assembles a payload in the following format:

        {
          "namespace": "default",
          "name": "string",
          "properties": {}
        }

        :param name: The rule name to evaluate.
        :param properties: A dictionary of properties that the rule will evaluate against.
        :return: The evaluation result as a dictionary.
        """
        payload = {
            "namespace": self.namespace,
            "name": name,
            "properties": properties
        }
        url = f"{self.base_url}/evaluate"
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(url, json=payload, headers=self.headers)
                response.raise_for_status()
                logger.info(f"Successfully evaluated rule '{name}' at {url}")
                return response.json()
        except httpx.HTTPStatusError as exc:
            logger.error(f"Error evaluating rule: {exc.response.text}")
            raise
        except httpx.RequestError as exc:
            logger.error(f"Network error while evaluating rule: {exc}")
            raise

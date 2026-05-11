import os
import logging
from typing import List, Optional, Dict, Any
import httpx
from hooks.oscar_hook import OscarHook

logger = logging.getLogger(__name__)


class MappingHook(OscarHook):
    """
    Hook for interacting with the Mapping Data API.
    """

    def __init__(self, conn_id: Optional[str] = None):
        super().__init__(conn_id)
        self.base_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/mapping-data"
        # Setup headers for internal service authentication
        self.headers = {"Content-Type": "application/json", "X-Internal-Service": "airflow"}

    def list_mapping_elements(
        self,
        mapping_name: Optional[str] = None,
        mapping_namespace_name: Optional[str] = None,
        mapping_key: Optional[str] = None,
        fields: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List mapping elements with optional filtering.

        Args:
            mapping_name: Filter by mapping name
            mapping_namespace_name: Filter by namespace name
            mapping_key: Filter by mapping key
            fields: Comma-separated list of fields to include in response

        Returns:
            List of mapping elements
        """
        params = {}
        if mapping_name:
            params["mapping_name"] = mapping_name
        if mapping_namespace_name:
            params["mapping_namespace_name"] = mapping_namespace_name
        if mapping_key:
            params["mapping_key"] = mapping_key
        if fields:
            params["filter"] = fields

        logger.info(f"Listing mapping elements with params: {params}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.get(
                    f"{self.base_url}/mapping/element", params=params, headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Successfully listed mapping elements. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error listing mapping elements: {str(e)}")
            raise e

    def add_mapping(
        self,
        name: str,
        mapping_namespace_name: str,
        mapping_key: str,
        mapping_value: str,
        comment: Optional[str] = None,
        additional_ref: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Add a new mapping.

        Args:
            name: Name of the mapping
            mapping_namespace_name: Name of the mapping namespace
            mapping_key: Key for the mapping
            mapping_value: Value for the mapping
            comment: Optional comment for the mapping
            additional_ref: Optional additional reference

        Returns:
            Created mapping data
        """
        mapping_data = {
            "name": name,
            "mapping_namespace_name": mapping_namespace_name,
            "mapping_key": mapping_key,
            "mapping_value": mapping_value,
        }
        if comment:
            mapping_data["comment"] = comment
        if additional_ref:
            mapping_data["additional_ref"] = additional_ref

        logger.info(f"Adding new mapping with data: {mapping_data}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(
                    f"{self.base_url}/mapping", json=mapping_data, headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Successfully added mapping. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error adding mapping: {str(e)}")
            raise e

    def update_mapping(
        self, mapping_id: str, mapping_value: str, comment: Optional[str] = None, additional_ref: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a mapping.

        Args:
            mapping_id: ID of the mapping to update
            mapping_value: New value for the mapping
            comment: Optional new comment
            additional_ref: Optional new additional reference

        Returns:
            Updated mapping data
        """
        update_data = {"mapping_value": mapping_value}
        if comment is not None:
            update_data["comment"] = comment
        if additional_ref is not None:
            update_data["additional_ref"] = additional_ref

        logger.info(f"Updating mapping {mapping_id} with data: {update_data}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.patch(
                    f"{self.base_url}/mapping/{mapping_id}",
                    json=update_data,
                    headers=self.headers,
                )
                response.raise_for_status()
                logger.info(f"Successfully updated mapping. Response: {response.text}")
                return response.json()
        except Exception as e:
            logger.error(f"Error updating mapping: {str(e)}")
            raise e

    def delete_mapping(self, mapping_id: str) -> None:
        """
        Delete a mapping.

        Args:
            mapping_id: ID of the mapping to delete
        """
        logger.info(f"Deleting mapping: {mapping_id}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.delete(
                    f"{self.base_url}/mapping/{mapping_id}", headers=self.headers
                )
                response.raise_for_status()
                logger.info(f"Successfully deleted mapping: {mapping_id}")
        except Exception as e:
            logger.error(f"Error deleting mapping: {str(e)}")
            raise e

    def get_mapping(
        self,
        mapping_name: str,
        mapping_namespace_name: str,
    ) -> dict:
        """
        Retrieve the mapping object for the specified mapping_name and mapping_namespace_name.
        Returns the first mapping object found, or an empty dict if not found.
        """
        params = {
            "name": mapping_name,
            "mapping_namespace_name": mapping_namespace_name,
        }
        logger.info(f"Retrieving mapping with params: {params}")
        try:
            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.get(f"{self.base_url}/mapping", params=params, headers=self.headers)
                response.raise_for_status()
                logger.info(f"Successfully retrieved mapping. Response: {response.text}")
                data = response.json()
                if isinstance(data, list) and data:
                    return data[0]
                return {}
        except Exception as e:
            logger.error(f"Error retrieving mapping: {str(e)}")
            return {}

    def list_mapping_elements_by_mapping_name(
        self, mapping_name: str, mapping_namespace_name: str
    ) -> List[Dict[str, Any]]:
        """
        List all mapping elements for a given mapping name and namespace.
        This is equivalent to calling list_mapping_elements with just the mapping name and namespace.
        """
        logger.info(f"Listing all mapping elements for mapping_name: {mapping_name}, namespace: {mapping_namespace_name}")
        return self.list_mapping_elements(
            mapping_name=mapping_name,
            mapping_namespace_name=mapping_namespace_name,
            mapping_key=None
        )

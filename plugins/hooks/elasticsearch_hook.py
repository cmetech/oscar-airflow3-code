#!/usr/bin/env python3
"""
Elasticsearch Hook for Airflow

This hook provides a standardized way to connect to Elasticsearch and post data
using Airflow connections. It supports both single connections and clusters.
"""

import os
import logging
import json
import httpx
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone

from airflow.hooks.base import BaseHook
from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class ElasticsearchHook:
    """
    Hook for connecting to Elasticsearch and posting data.
    
    This hook supports:
    - Single Elasticsearch connections (when connection_id is provided)
    - Elasticsearch clusters (multiple connections when no connection_id provided)
    - Connection by type (elasticsearch)
    - Connection by specific connection ID
    """

    def __init__(self, connection_id: Optional[str] = None, connection_type: str = "elasticsearch"):
        """
        Initialize the Elasticsearch hook.

        Args:
            connection_id: Optional specific connection ID. If not provided, will get ALL connections by type as a cluster.
            connection_type: Connection type to search for (default: "elasticsearch")
        """
        self.connection_id = connection_id
        self.connection_type = connection_type
        self.client = None
        self._configure_client()

    def _get_elasticsearch_connections(self) -> List[Dict[str, Any]]:
        """
        Get Elasticsearch connections from Airflow.

        Behavior depends on how the hook was initialized:
        - No parameters (cluster mode): Gets ALL elasticsearch connections by type from middleware API
        - connection_id provided (single connection mode): Gets specific connection by ID from Airflow

        Returns:
            List of connection dictionaries (1 item for single connection, multiple items for cluster)
        """
        try:
            if self.connection_id:
                # Get specific connection by ID - single connection mode
                conn = BaseHook.get_connection(self.connection_id)
                return [{
                    'conn_id': conn.conn_id,
                    'host': conn.host,
                    'port': conn.port or 9200,
                    'login': conn.login,
                    'password': conn.password,
                    'schema': conn.schema,
                    'extra': conn.extra_dejson
                }]
            else:
                # CLUSTER MODE: Get ALL elasticsearch connections as a cluster
                # Call the middleware API to get connections by type
                connections = self._get_connections_by_type()

                if not connections:
                    raise Exception("No Elasticsearch connections found for cluster mode")

                return connections

        except Exception as e:
            logger.error(f"Failed to get Elasticsearch connections: {str(e)}")
            raise

    def _get_connections_by_type(self) -> List[Dict[str, Any]]:
        """
        Get connections by type from the middleware API.
        This replicates the ConnectionManager.get_connections() logic.

        Returns:
            List of connection dictionaries
        """
        try:
            # Get middleware host and port from environment
            middleware_host = os.environ.get("MIDDLEWARE_HOST", "middleware")
            middleware_port = int(os.environ.get("MIDDLEWARE_PORT", "5200"))
            ssl_verify = os.environ.get("SSL_VERIFY", "false").lower() in ("true", "1", "t")

            base_url = f"https://{middleware_host}:{middleware_port}"

            # Build query parameters for the API call
            params = {
                "limit": 100,
                "offset": 0,
                "conn_type": self.connection_type,
                "include_credentials": "true"
            }

            logger.debug(f"[ElasticsearchHook] Getting connections by type: {self.connection_type}")
            logger.debug(f"[ElasticsearchHook] API URL: {base_url}/api/v1/workflows/connections")
            logger.debug(f"[ElasticsearchHook] Request parameters: {params}")

            # Make the API request
            with httpx.Client(verify=ssl_verify) as client:
                response = client.get(f"{base_url}/api/v1/workflows/connections", params=params)
                response.raise_for_status()

                data = response.json()
                connections = data.get("connections", [])

                logger.debug(f"[ElasticsearchHook] Retrieved {len(connections)} connections")

                # Transform the connections to match our expected format
                transformed_connections = []
                for conn in connections:
                    transformed_connections.append({
                        'conn_id': conn.get('conn_id', ''),
                        'host': conn.get('host', ''),
                        'port': conn.get('port', 9200),
                        'login': conn.get('login', ''),
                        'password': conn.get('password', ''),
                        'schema': conn.get('conn_type', 'http'),
                        'extra': conn.get('extra', {})
                    })

                return transformed_connections

        except httpx.HTTPError as exc:
            logger.error(f"[ElasticsearchHook] HTTP error occurred: {exc}")
            raise Exception(f"Failed to get connections from middleware API: {exc}")
        except Exception as exc:
            logger.error(f"[ElasticsearchHook] Unexpected error occurred: {exc}")
            raise Exception(f"Failed to get connections from middleware API: {exc}")

    def _configure_client(self) -> None:
        """Configure the Elasticsearch client using Airflow connections."""
        try:
            # Get connections
            connections = self._get_elasticsearch_connections()

            if not connections:
                raise Exception("No Elasticsearch connections found")

            # Build hosts list for cluster support (replicating metrics_to_elasticsearch logic)
            hosts = []
            ssl_verify = os.environ.get('SSL_VERIFY', 'false').lower() in ('true', '1', 't')

            # Create auth tuple if credentials exist (from FIRST connection only - cluster logic)
            auth = None
            login = connections[0].get('login')
            password = connections[0].get('password')

            if login and password:
                auth = (login, password)
                logger.debug("[NT_ELASTIC_SEND] Using basic authentication")
            else:
                logger.warning("[NT_ELASTIC_SEND] No authentication credentials found")

            # Process each connection as they are part of one cluster (replicating task logic)
            for conn in connections:
                host = conn['host']
                port = conn['port']
                conn_type = conn.get('schema', 'http')
                protocol = 'https' if conn_type == 'elasticsearch' or conn_type == 'https' else 'http'

                if not host:
                    logger.warning("[NT_ELASTIC_CONFIG] Skipping connection with missing host")
                    continue

                # Build base URL WITHOUT embedded auth (Elasticsearch client handles auth separately)
                base_url = f"{protocol}://{host}:{port}"
                hosts.append(base_url)

            if not hosts:
                raise Exception("No valid Elasticsearch hosts found")

            # Create Elasticsearch client configuration
            es_config = {
                "hosts": hosts,
                "verify_certs": ssl_verify,
                "ssl_show_warn": False
            }

            # Add auth if available (Elasticsearch client handles this separately)
            if auth:
                es_config["basic_auth"] = auth
            
            # Create the client
            self.client = Elasticsearch(**es_config)
            
            # Test the connection
            if not self.client.ping():
                raise Exception(f"Cannot connect to Elasticsearch at {hosts[0]}")
            
            logger.info(f"Successfully connected to Elasticsearch cluster with {len(hosts)} hosts")
            
        except Exception as e:
            logger.error(f"Failed to configure Elasticsearch client: {str(e)}")
            raise

    def post_data(self, index: str, data: Dict[str, Any], doc_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Post data to Elasticsearch.
        
        Args:
            index: The Elasticsearch index name
            data: The data to post (will be converted to JSON)
            doc_id: Optional document ID. If not provided, Elasticsearch will generate one.
            
        Returns:
            Dict containing the response from Elasticsearch
        """
        try:
            if not self.client:
                self._configure_client()
            
            # Add timestamp if not present
            if "@timestamp" not in data:
                data["@timestamp"] = datetime.now(timezone.utc).isoformat()
            
            # Post the document (using modern document= parameter)
            response = self.client.index(
                index=index,
                id=doc_id,
                document=data
            )
            
            logger.debug(f"Successfully posted data to Elasticsearch: {response}")
            return response
            
        except Exception as e:
            logger.error(f"Failed to post data to Elasticsearch: {str(e)}")
            raise

    def upsert_data(self, index: str, data: Dict[str, Any], doc_id: str) -> Dict[str, Any]:
        """
        Upsert data to Elasticsearch (create if not exists, update if exists).
        
        Args:
            index: The Elasticsearch index name
            data: The data to upsert
            doc_id: Document ID (required for upsert)
            
        Returns:
            Dict containing the response from Elasticsearch
        """
        try:
            if not self.client:
                self._configure_client()
            
            # Add timestamp if not present
            if "@timestamp" not in data:
                data["@timestamp"] = datetime.now(timezone.utc).isoformat()
            
            # Use update with upsert to create or update document (using modern doc= parameter)
            response = self.client.update(
                index=index,
                id=doc_id,
                doc=data,
                doc_as_upsert=True
            )
            
            logger.debug(f"Successfully upserted data to Elasticsearch: {response}")
            return response
            
        except Exception as e:
            logger.error(f"Failed to upsert data to Elasticsearch: {str(e)}")
            raise

    def bulk_upsert_data(self, index: str, data_list: List[Dict[str, Any]], id_field: str = "u_identifier") -> Dict[str, Any]:
        """
        Bulk upsert multiple documents to Elasticsearch using standard elasticsearch library.
        
        Args:
            index: The Elasticsearch index name
            data_list: List of data dictionaries to upsert
            id_field: Field name to use as document ID (default: u_identifier)
            
        Returns:
            Dict containing the bulk response from Elasticsearch
        """
        try:
            if not self.client:
                self._configure_client()
            
            # Import helpers for bulk operations
            from elasticsearch import helpers
            
            # Prepare bulk actions for upsert
            actions = []
            for data in data_list:
                # Get document ID from the specified field
                doc_id = data.get(id_field)
                if not doc_id:
                    logger.warning(f"No {id_field} found in data, skipping: {data}")
                    continue
                
                # Add timestamp if not present
                if "@timestamp" not in data:
                    data["@timestamp"] = datetime.now(timezone.utc).isoformat()
                
                # Create upsert action
                action = {
                    "_index": index,
                    "_id": str(doc_id),
                    "_source": data,
                    "_op_type": "index"  # This creates or updates (upsert behavior)
                }
                actions.append(action)
            
            if not actions:
                logger.warning("No valid documents to upsert")
                return {"success": 0, "failed": 0, "errors": []}
            
            # Use helpers.bulk for efficient bulk operations
            logger.info(f"Upserting {len(actions)} documents to Elasticsearch")
            success, failed = helpers.bulk(
                self.client,
                actions,
                chunk_size=1000,
                request_timeout=60,
                raise_on_error=False
            )
            
            # Process results
            errors = []
            if failed:
                for error in failed[:5]:  # Return first 5 errors
                    errors.append(str(error))
            
            result = {
                "success": success,
                "failed": len(failed) if failed else 0,
                "errors": errors
            }
            
            logger.info(f"Bulk upserted {success} documents to Elasticsearch, {len(failed) if failed else 0} failed")
            return result
            
        except Exception as e:
            logger.error(f"Failed to bulk upsert data to Elasticsearch: {str(e)}")
            raise

    def check_document_exists(self, index: str, doc_id: str) -> bool:
        """
        Check if a document exists in Elasticsearch.
        
        Args:
            index: The index name
            doc_id: The document ID to check
            
        Returns:
            True if document exists, False otherwise
        """
        try:
            if not self.client:
                self._configure_client()
            
            return self.client.exists(index=index, id=doc_id)
            
        except Exception as e:
            logger.error(f"Failed to check if document {doc_id} exists in index {index}: {str(e)}")
            return False

    def get_document(self, index: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a document from Elasticsearch.
        
        Args:
            index: The index name
            doc_id: The document ID to get
            
        Returns:
            Document data if exists, None otherwise
        """
        try:
            if not self.client:
                self._configure_client()
            
            response = self.client.get(index=index, id=doc_id)
            return response.get("_source")
            
        except Exception as e:
            logger.error(f"Failed to get document {doc_id} from index {index}: {str(e)}")
            return None

    def bulk_post_data(self, index: str, data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Post multiple documents to Elasticsearch using bulk API.
        
        Args:
            index: The Elasticsearch index name
            data_list: List of data dictionaries to post
            
        Returns:
            Dict containing the bulk response from Elasticsearch
        """
        try:
            if not self.client:
                self._configure_client()
            
            # Prepare bulk actions
            actions = []
            for data in data_list:
                # Add timestamp if not present
                if "@timestamp" not in data:
                    data["@timestamp"] = datetime.now(timezone.utc).isoformat()
                
                action = {
                    "_index": index,
                    "_source": data
                }
                actions.append(action)
            
            # Use bulk API
            from elasticsearch import helpers
            success, failed = helpers.bulk(
                self.client,
                actions,
                chunk_size=1000,
                request_timeout=60,
                raise_on_error=False
            )
            
            result = {
                "success": success,
                "failed": len(failed) if failed else 0,
                "errors": failed[:5] if failed else []  # Return first 5 errors
            }
            
            logger.info(f"Bulk posted {success} documents to Elasticsearch, {len(failed) if failed else 0} failed")
            return result
            
        except Exception as e:
            logger.error(f"Failed to bulk post data to Elasticsearch: {str(e)}")
            raise

    def check_index_exists(self, index: str) -> bool:
        """
        Check if an index exists in Elasticsearch.
        
        Args:
            index: The index name to check
            
        Returns:
            True if index exists, False otherwise
        """
        try:
            if not self.client:
                self._configure_client()
            
            return self.client.indices.exists(index=index)
            
        except Exception as e:
            logger.error(f"Failed to check if index {index} exists: {str(e)}")
            return False

    def create_index(self, index: str, mapping: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create an index in Elasticsearch.
        
        Args:
            index: The index name to create
            mapping: Optional mapping configuration with outer 'mappings' structure
            
        Returns:
            True if index was created successfully, False otherwise
        """
        try:
            if not self.client:
                self._configure_client()
            
            if self.check_index_exists(index):
                logger.info(f"Index {index} already exists")
                return True
            
            # Create the index with mapping
            if mapping:
                # Extract the inner mapping definition
                inner_mapping = mapping.get('mappings', {})
                self.client.indices.create(index=index, mappings=inner_mapping)
            else:
                self.client.indices.create(index=index)
            
            logger.info(f"Successfully created index {index}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create index {index}: {str(e)}")
            raise  # Re-raise to get full error in DAG

    def get_document_count(self, index: str) -> int:
        """
        Get the document count for an index.
        
        Args:
            index: The index name
            
        Returns:
            Number of documents in the index
        """
        try:
            if not self.client:
                self._configure_client()
            
            response = self.client.count(index=index)
            return response.get("count", 0)
            
        except Exception as e:
            logger.error(f"Failed to get document count for index {index}: {str(e)}")
            return 0

    def search(self, index: str, query: Dict[str, Any], size: int = 10) -> Dict[str, Any]:
        """
        Search for documents in an index.
        
        Args:
            index: The index name to search
            query: The search query
            size: Number of results to return
            
        Returns:
            Search results from Elasticsearch
        """
        try:
            if not self.client:
                self._configure_client()
            
            response = self.client.search(
                index=index,
                query=query,
                size=size
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to search index {index}: {str(e)}")
            raise
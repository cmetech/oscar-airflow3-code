import logging
import httpx
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timezone
import base64
import json

logger = logging.getLogger(__name__)


class CacheHook:
    """
    Hook for interacting with the cache service via middleware APIs.
    Provides methods for storing and retrieving both binary and non-binary data.
    """

    def __init__(self, middleware_host: str = "middleware", middleware_port: str = "5200"):
        """
        Initialize the CacheHook.

        Args:
            middleware_host: Hostname of the middleware service
            middleware_port: Port of the middleware service
        """
        self.middleware_url = f"https://{middleware_host}:{middleware_port}/api/v1"
        # Setup headers for internal service authentication
        self.headers = {"Content-Type": "application/json", "X-Internal-Service": "airflow"}
        self.client = httpx.AsyncClient(verify=False, headers=self.headers)

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.client.aclose()

    async def store_item(self, key: str, value: Any, expiry_seconds: Optional[int] = None) -> Dict[str, Any]:
        """
        Store a JSON-serializable item in the cache.

        Args:
            key: The unique identifier for the cached item
            value: The value to cache (must be JSON-serializable)
            expiry_seconds: Time in seconds until the item expires (None for no expiry)

        Returns:
            Dict containing the cache key and status
        """
        try:
            payload = {"key": key, "value": value, "expiry_seconds": expiry_seconds}
            response = await self.client.post(f"{self.middleware_url}/cache/items", json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error storing item in cache: {str(e)}")
            raise

    async def get_item(self, key: str) -> Dict[str, Any]:
        """
        Retrieve a cached item by key.

        Args:
            key: The unique identifier of the cached item

        Returns:
            Dict containing the cached item and metadata
        """
        try:
            response = await self.client.get(f"{self.middleware_url}/cache/items/{key}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error retrieving item from cache: {str(e)}")
            raise

    async def delete_item(self, key: str) -> None:
        """
        Delete a cached item by key.

        Args:
            key: The unique identifier of the cached item
        """
        try:
            response = await self.client.delete(f"{self.middleware_url}/cache/items/{key}")
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Error deleting item from cache: {str(e)}")
            raise

    async def store_binary(
        self,
        key: Optional[str] = None,
        content_type: str = "application/octet-stream",
        data: str = "",
        expiry_seconds: Optional[int] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Store binary data in the cache.

        Args:
            key: The unique identifier for the cached binary (generated if not provided)
            content_type: The MIME type of the binary data
            data: Base64 encoded binary data
            expiry_seconds: Time in seconds until the item expires (None for no expiry)
            metadata: Additional metadata to store with the binary data

        Returns:
            Dict containing the cache key and metadata
        """
        try:
            payload = {
                "key": key,
                "content_type": content_type,
                "data": data,
                "expiry_seconds": expiry_seconds,
                "metadata": metadata or {},
            }
            response = await self.client.post(f"{self.middleware_url}/cache/binary", json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error storing binary in cache: {str(e)}")
            raise

    async def get_binary_metadata(self, key: str) -> Dict[str, Any]:
        """
        Get metadata for binary data without retrieving the actual content.

        Args:
            key: The unique identifier of the cached binary item

        Returns:
            Dict containing metadata about the binary data
        """
        try:
            response = await self.client.get(f"{self.middleware_url}/cache/binary/{key}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error retrieving binary metadata from cache: {str(e)}")
            raise

    async def get_binary_content(self, key: str) -> Dict[str, Any]:
        """
        Retrieve binary data content by key.

        Args:
            key: The unique identifier of the cached binary item

        Returns:
            Dict containing the binary data content and metadata
        """
        try:
            response = await self.client.get(f"{self.middleware_url}/cache/binary/{key}/content")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error retrieving binary content from cache: {str(e)}")
            raise

    async def list_keys(self, pattern: str = "*", limit: int = 100) -> Dict[str, Any]:
        """
        List cache keys matching a pattern.

        Args:
            pattern: Redis key pattern to match (default: "*" for all keys)
            limit: Maximum number of keys to return (default: 100)

        Returns:
            Dict containing list of matching keys and count
        """
        try:
            response = await self.client.get(
                f"{self.middleware_url}/cache/keys", params={"pattern": pattern, "limit": limit}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error listing cache keys: {str(e)}")
            raise

    async def delete_keys(self, pattern: str, confirm: bool = False) -> Dict[str, Any]:
        """
        Delete cache keys matching a pattern.

        Args:
            pattern: Redis key pattern to match
            confirm: Confirmation flag (must be True to delete)

        Returns:
            Dict containing count of deleted keys
        """
        try:
            response = await self.client.delete(
                f"{self.middleware_url}/cache/keys", params={"pattern": pattern, "confirm": confirm}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error deleting cache keys: {str(e)}")
            raise

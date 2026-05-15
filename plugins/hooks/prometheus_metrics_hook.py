#!/usr/bin/env python3
"""
Prometheus Metrics Hook for Airflow

This hook provides a standardized way to send metrics to Prometheus using Airflow connections.
It uses the middleware cache API for storing metric state and follows Airflow best practices.
"""

import os
import logging
import json
import httpx
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone
from airflow.sdk.bases.hook import BaseHook
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway

logger = logging.getLogger(__name__)


class PrometheusMetricsHook(BaseHook):
    """
    Hook for sending metrics to Prometheus using Airflow connections.

    This hook supports:
    - Sending gauge and counter metrics to Prometheus pushgateway
    - Storing metric state in middleware cache (instead of Redis)
    - Following Airflow best practices for connection management
    """

    conn_name_attr = 'prometheus_conn_id'
    default_conn_name = 'prometheus_default'
    conn_type = 'prometheus'
    hook_name = 'PrometheusMetrics'

    def __init__(self, conn_id: Optional[str] = None):
        """
        Initialize the PrometheusMetrics hook.

        Args:
            conn_id: Optional specific connection ID. If not provided, will use default.
        """
        super().__init__()
        self.conn_id = conn_id
        self.pushgateway_url = ""
        self.middleware_url = ""
        self.verify_ssl = False
        self.registry = CollectorRegistry()
        self.metrics = {}
        self._setup_connection()

    def _setup_connection(self):
        """Set up connection parameters from Airflow connection or environment variables."""
        if self.conn_id:
            try:
                conn = self.get_connection(self.conn_id)
                self.pushgateway_url = conn.host or os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")
                # Extract additional config from extras
                extras = conn.extra_dejson
                self.verify_ssl = extras.get("verify_ssl", False) if extras else False
            except Exception as e:
                logger.warning(f"Could not get connection {self.conn_id}: {str(e)}")
                self.pushgateway_url = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")
                self.verify_ssl = False
        else:
            self.pushgateway_url = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")
            self.verify_ssl = os.environ.get("SSL_VERIFY", "false").lower() == "true"

        # Set up middleware URL for cache operations
        middleware_host = os.environ.get("MIDDLEWARE_HOST", "middleware")
        middleware_port = os.environ.get("MIDDLEWARE_PORT", "5200")
        self.middleware_url = f"https://{middleware_host}:{middleware_port}/api/v1"

        logger.info(f"Initialized PrometheusMetricsHook with pushgateway: {self.pushgateway_url}")

    def _get_cache_key(self, metric_name: str, labels: Dict[str, str]) -> str:
        """
        Generate a cache key for a metric with its labels.

        Args:
            metric_name: Name of the metric
            labels: Dictionary of labels

        Returns:
            String key for cache
        """
        # Sort labels by key to ensure consistent key generation
        sorted_labels = sorted(labels.items())
        labels_str = ",".join([f"{k}={v}" for k, v in sorted_labels])
        return f"prometheus_metrics:{metric_name}:{labels_str}"

    def _store_metric_state(self, metric_name: str, labels: Dict[str, str], value: Union[int, float]) -> bool:
        """
        Store metric state in middleware cache.

        Args:
            metric_name: Name of the metric
            labels: Dictionary of labels
            value: Metric value

        Returns:
            True if successful, False otherwise
        """
        try:
            cache_key = self._get_cache_key(metric_name, labels)
            payload = {
                "key": cache_key,
                "value": {
                    "metric_name": metric_name,
                    "labels": labels,
                    "value": value,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                },
                "expiry_seconds": 3600  # 1 hour expiry
            }

            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.post(f"{self.middleware_url}/cache/items", json=payload)
                response.raise_for_status()
                logger.debug(f"Stored metric state for {cache_key}")
                return True

        except Exception as e:
            logger.error(f"Error storing metric state for {metric_name}: {str(e)}")
            return False

    def _get_metric_state(self, metric_name: str, labels: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Retrieve metric state from middleware cache.

        Args:
            metric_name: Name of the metric
            labels: Dictionary of labels

        Returns:
            Metric state dictionary or None if not found
        """
        try:
            cache_key = self._get_cache_key(metric_name, labels)

            with httpx.Client(verify=self.verify_ssl, timeout=30.0) as client:
                response = client.get(f"{self.middleware_url}/cache/items/{cache_key}")
                if response.status_code == 200:
                    data = response.json()
                    return data.get("value")
                else:
                    return None

        except Exception as e:
            logger.debug(f"Error retrieving metric state for {metric_name}: {str(e)}")
            return None

    def _ensure_metric_exists(self, metric_name: str, metric_type: str, labels: Dict[str, str], description: str = ""):
        """
        Ensure a metric exists in the registry.

        Args:
            metric_name: Name of the metric
            metric_type: Type of metric ('gauge' or 'counter')
            labels: Dictionary of labels
            description: Metric description
        """
        if metric_name not in self.metrics:
            label_names = list(labels.keys()) if labels else []

            if metric_type == "gauge":
                self.metrics[metric_name] = Gauge(
                    metric_name, description, label_names, registry=self.registry
                )
            elif metric_type == "counter":
                self.metrics[metric_name] = Counter(
                    metric_name, description, label_names, registry=self.registry
                )

    def send_gauge_metric(
        self, metric_name: str, value: Union[int, float],
        labels: Optional[Dict[str, str]] = None,
        description: str = ""
    ) -> bool:
        """
        Send a gauge metric to Prometheus.

        Args:
            metric_name: Name of the metric
            value: Metric value
            labels: Optional dictionary of labels
            description: Optional metric description

        Returns:
            True if successful, False otherwise
        """
        try:
            labels = labels or {}

            # Store metric state in cache
            self._store_metric_state(metric_name, labels, value)

            # Ensure metric exists in registry
            self._ensure_metric_exists(metric_name, "gauge", labels, description)

            # Set the metric value
            self.metrics[metric_name].labels(**labels).set(value)

            logger.debug(f"Sent gauge metric {metric_name}={value} with labels {labels}")
            return True

        except Exception as e:
            logger.error(f"Error sending gauge metric {metric_name}: {str(e)}")
            return False

    def send_counter_metric(
        self, metric_name: str, value: Union[int, float] = 1,
        labels: Optional[Dict[str, str]] = None,
        description: str = ""
    ) -> bool:
        """
        Send a counter metric to Prometheus.

        Args:
            metric_name: Name of the metric
            value: Metric value (default: 1)
            labels: Optional dictionary of labels
            description: Optional metric description

        Returns:
            True if successful, False otherwise
        """
        try:
            labels = labels or {}

            # Get current counter value from cache
            current_state = self._get_metric_state(metric_name, labels)
            current_value = current_state.get("value", 0) if current_state else 0

            # Increment the counter
            new_value = current_value + value

            # Store updated metric state in cache
            self._store_metric_state(metric_name, labels, new_value)

            # Ensure metric exists in registry
            self._ensure_metric_exists(metric_name, "counter", labels, description)

            # Set the counter value (not increment, as we want the total)
            self.metrics[metric_name].labels(**labels)._value._value = new_value

            logger.debug(f"Sent counter metric {metric_name}={new_value} with labels {labels}")
            return True

        except Exception as e:
            logger.error(f"Error sending counter metric {metric_name}: {str(e)}")
            return False

    def push_metrics(self, job_name: str = "airflow", grouping_key: Optional[Dict[str, str]] = None) -> bool:
        """
        Push all collected metrics to Prometheus pushgateway.

        Args:
            job_name: The job name to use for the metrics
            grouping_key: Optional dictionary with grouping key labels

        Returns:
            True if successful, False otherwise
        """
        try:
            grouping_key = grouping_key or {}

            logger.debug(f"Pushing metrics to Pushgateway with job={job_name}, grouping_key={grouping_key}")

            # Push to gateway using prometheus_client
            push_to_gateway(
                self.pushgateway_url,
                job=job_name,
                grouping_key=grouping_key,
                registry=self.registry
            )

            logger.debug(f"Successfully pushed metrics to Pushgateway")
            return True

        except Exception as e:
            logger.error(f"Error pushing metrics to Pushgateway: {str(e)}")
            return False

    def get_metric_value(
        self, metric_name: str,
        labels: Optional[Dict[str, str]] = None
    ) -> Optional[Union[int, float]]:
        """
        Get the current value of a metric from cache.

        Args:
            metric_name: Name of the metric
            labels: Optional dictionary of labels

        Returns:
            Metric value or None if not found
        """
        labels = labels or {}
        state = self._get_metric_state(metric_name, labels)
        return state.get("value") if state else None

    def increment_counter(
        self, metric_name: str, increment: Union[int, float] = 1,
        labels: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Increment a counter metric.

        Args:
            metric_name: Name of the metric
            increment: Amount to increment (default: 1)
            labels: Optional dictionary of labels

        Returns:
            True if successful, False otherwise
        """
        return self.send_counter_metric(metric_name, increment, labels)

    def set_gauge(self, metric_name: str, value: Union[int, float],
                  labels: Optional[Dict[str, str]] = None) -> bool:
        """
        Set a gauge metric to a specific value.

        Args:
            metric_name: Name of the metric
            value: Metric value
            labels: Optional dictionary of labels

        Returns:
            True if successful, False otherwise
        """
        return self.send_gauge_metric(metric_name, value, labels)

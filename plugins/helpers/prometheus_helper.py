import os
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway

class PrometheusMetricsSender:
    def __init__(self, pushgateway_url=None):
        self.pushgateway_url = pushgateway_url or os.environ.get(
            "PUSHGATEWAY_URL", "http://pushgateway:9091")
        self.registry = CollectorRegistry()
        self.metrics = {}

    def add_gauge_metric(self, metric):
        if metric["name"] not in self.metrics:
            label_names = list(metric["labels"].keys())
            self.metrics[metric["name"]] = Gauge(
                metric["name"], metric["description"], label_names, registry=self.registry)
        self.metrics[metric["name"]].labels(
            **metric["labels"]).set(metric["value"])

    def add_counter_metric(self, metric):
        if metric["name"] not in self.metrics:
            label_names = list(metric["labels"].keys())
            self.metrics[metric["name"]] = Counter(
                metric["name"], metric["description"], label_names, registry=self.registry)
        self.metrics[metric["name"]].labels(
            **metric["labels"]).inc(metric["value"])

    def push_metrics(self, job_name="oscar_taskmanager"):
        push_to_gateway(self.pushgateway_url, job=job_name, registry=self.registry)
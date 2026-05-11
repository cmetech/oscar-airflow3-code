from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from helpers.prometheus_helper import PrometheusMetricsSender

class PushMetricsToPrometheusOperator(BaseOperator):
    def __init__(self, metrics, job_name="airflow", pushgateway_conn_id="pushgateway_default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metrics = metrics
        self.job_name = job_name
        self.pushgateway_conn_id = pushgateway_conn_id

    def execute(self, context):
        connection = BaseHook.get_connection(self.pushgateway_conn_id)
        pushgateway_url = f"http://{connection.host}:{connection.port}"

        sender = PrometheusMetricsSender(pushgateway_url=pushgateway_url)
        for metric in self.metrics:
            if metric["type"] == "gauge":
                sender.add_gauge_metric(metric)
            elif metric["type"] == "counter":
                sender.add_counter_metric(metric)
        sender.push_metrics(job_name=self.job_name)
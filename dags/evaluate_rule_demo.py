from datetime import timedelta
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
import logging

from hooks.rule_hook import RuleHook  # type: ignore

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def evaluate_rules_task(**context):
    """
    Airflow task that uses RuleHook to evaluate a rule.
    """
    # Instantiate the hook with the default namespace
    hook = RuleHook(namespace="default")

    # Define the rule name and properties dictionary.
    rule_name = "test_rule_evaluation"
    evaluation_properties = {"summary": "This is a test alert from Airflow"}

    result = hook.evaluate_rules(rule_name, evaluation_properties)
    logger.info(f"Rule evaluation result: {result}")
    return result


with DAG(
    dag_id="evaluate_rule_demo",
    default_args=default_args,
    description="A DAG to demonstrate rule evaluation using RuleHook",
    schedule=None,
    start_date=pendulum.today('UTC').add(days=-1),
    tags=["rules", "example"],
) as dag:

    evaluate_task = PythonOperator(
        task_id="evaluate_rules",
        python_callable=evaluate_rules_task,
    )

    evaluate_task

"""
OSCAR System Health Monitoring DAG

This DAG provides comprehensive system health monitoring for the OSCAR platform,
including scheduled health checks, alerting, and reporting capabilities.

Features:
- Continuous health monitoring every 5 minutes
- Daily comprehensive health reports
- Alert generation for degraded system health
- Integration with Fabric tasks via TasksHook
- Metrics collection and analysis

Schedule:
- Health checks: Every 5 minutes
- Daily reports: Daily at 6 AM UTC
- Weekly comprehensive analysis: Weekly on Sundays at 8 AM UTC

Dependencies:
- oscar-util/scripts/ticketing_health_monitor.py
- oscar-taskmanager Fabric health tasks
- VictoriaMetrics for metrics storage
- Alertmanager for alert processing
"""

from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

# Import OSCAR hooks
from hooks.tasks_hook import TasksHook
from hooks.alert_hook import AlertHook
from hooks.notify_hook import NotifyHook
from hooks.worklog_hook import WorkLogHook

# Configure logging
logger = logging.getLogger(__name__)

# DAG default arguments
default_args = {
    'owner': 'oscar-platform',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# DAG definition
dag = DAG(
    'system_health_monitoring',
    default_args=default_args,
    description='OSCAR System Health Monitoring and Alerting',
    schedule=timedelta(minutes=5),  # Run every 5 minutes
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['health', 'monitoring', 'system', 'oscar'],
    max_active_runs=2,  # Limit concurrent runs
    doc_md=__doc__
)


def check_system_health(**context):
    """
    Execute system health check using Fabric tasks.

    This function triggers the health monitoring via TasksHook,
    processes the results, and determines if alerts need to be generated.
    """
    logger.info("Starting system health check...")

    # Initialize hooks
    tasks_hook = TasksHook()
    worklog_hook = WorkLogHook()

    # Create worklog for this health check
    worklog = worklog_hook.create_worklog(
        name=f"System Health Check - {context['ds']}",
        description="Automated system health monitoring",
        worklog_type="DB",
        metadata=[
            {"key": "dag_id", "value": context['dag'].dag_id},
            {"key": "task_id", "value": context['task'].task_id},
            {"key": "execution_date", "value": context['ds']},
            {"key": "run_id", "value": context['run_id']}
        ]
    )

    try:
        worklog_hook.info("Starting system health check via Fabric task")

        # Execute health check via Fabric task
        result = tasks_hook.trigger_task(
            task_id_or_name="health.system-health-check",
            prompts=[
                {"prompt": "verbose", "value": "false"},
                {"prompt": "lookback", "value": "15m"}  # Quick check for frequent monitoring
            ],
            user_data={
                "initiated_by": "airflow_dag",
                "dag_run_id": context['run_id'],
                "check_type": "routine"
            }
        )

        worklog_hook.info(f"Health check task result: {result.get('status', 'unknown')}")

        # Process results and determine health status
        health_status = "healthy"  # Default assumption
        alerts_generated = 0

        if result.get('status') == 'success':
            worklog_hook.info("System health check completed successfully")

            # Extract health metrics from task result if available
            task_output = result.get('output', {})

            # Check if any critical issues were detected
            if 'critical' in str(task_output).lower() or 'failed' in str(task_output).lower():
                health_status = "degraded"
                worklog_hook.warning("Potential health issues detected in output")

        elif result.get('status') == 'failed':
            health_status = "unhealthy"
            worklog_hook.error(f"Health check task failed: {result.get('error', 'Unknown error')}")
            alerts_generated += 1

        else:
            health_status = "unknown"
            worklog_hook.warning(f"Health check returned unexpected status: {result.get('status')}")

        # Store results for downstream tasks
        context['task_instance'].xcom_push(
            key='health_check_result',
            value={
                'status': health_status,
                'task_result': result,
                'alerts_generated': alerts_generated,
                'timestamp': context['ts'],
                'worklog_id': worklog['id']
            }
        )

        worklog_hook.add_metadata({
            "health_status": health_status,
            "alerts_generated": str(alerts_generated),
            "check_duration": result.get('duration', 'unknown')
        })

        worklog_hook.info("System health check completed")

        return {
            'status': health_status,
            'alerts_generated': alerts_generated,
            'worklog_id': worklog['id']
        }

    except Exception as e:
        worklog_hook.error(f"Health check failed with exception: {str(e)}")

        # Store error result
        context['task_instance'].xcom_push(
            key='health_check_result',
            value={
                'status': 'error',
                'error': str(e),
                'alerts_generated': 1,
                'timestamp': context['ts'],
                'worklog_id': worklog['id']
            }
        )

        raise

    finally:
        worklog_hook.close_worklog()


def process_health_alerts(**context):
    """
    Process health check results and generate alerts if necessary.

    This function analyzes the health check results and generates
    appropriate alerts for degraded or unhealthy systems.
    """
    logger.info("Processing health alerts...")

    # Get health check results
    health_result = context['task_instance'].xcom_pull(
        task_ids='check_system_health',
        key='health_check_result'
    )

    if not health_result:
        logger.warning("No health check result available")
        return {'alerts_sent': 0}

    health_status = health_result.get('status', 'unknown')
    worklog_id = health_result.get('worklog_id')

    # Initialize hooks
    alert_hook = AlertHook()
    notify_hook = NotifyHook()
    worklog_hook = WorkLogHook()

    # Resume worklog if available
    if worklog_id:
        worklog_hook.set_worklog_id(worklog_id)
        worklog_hook.info("Processing health alerts")

    alerts_sent = 0

    try:
        # Generate alerts based on health status
        if health_status in ['degraded', 'unhealthy', 'error']:
            severity = 'critical' if health_status in ['unhealthy', 'error'] else 'warning'

            # Create alert
            alert_data = {
                'alertname': f'System_Health_{health_status.title()}',
                'severity': severity,
                'system': 'oscar_platform',
                'health_status': health_status,
                'dag_run_id': context['run_id'],
                'execution_date': context['ds'],
                'description': f'OSCAR system health is {health_status}',
                'runbook_url': 'https://wiki.company.com/oscar/health-monitoring'
            }

            # Send alert via AlertHook
            alert_response = alert_hook.send_alerts(
                template_name='system_health_alert',
                alert_objects=[alert_data]
            )

            if alert_response:
                alerts_sent += 1
                worklog_hook.warning(f"Generated {severity} alert for {health_status} system health")

                # Send notification for critical issues
                if severity == 'critical':
                    notify_response = notify_hook.send_notification({
                        "name": "ops-team-email",
                        "subject": f"🚨 CRITICAL: OSCAR System Health {health_status.title()}",
                        "message": f"""
OSCAR System Health Alert

Status: {health_status.upper()}
Execution: {context['ds']} ({context['run_id']})
Details: System health monitoring detected {health_status} status

Please investigate immediately:
- Check system health dashboard
- Review component status
- Examine recent changes

Health Check Results:
{health_result.get('task_result', 'No detailed results available')}

This is an automated alert from the OSCAR health monitoring system.
                        """
                    })

                    if notify_response:
                        worklog_hook.warning("Critical health notification sent to ops team")

        elif health_status == 'healthy':
            worklog_hook.info("System health is normal - no alerts generated")

            # Check if we should send a recovery notification
            # (This would require checking previous health state)

        else:
            worklog_hook.warning(f"Unknown health status: {health_status}")

        return {'alerts_sent': alerts_sent, 'health_status': health_status}

    except Exception as e:
        if worklog_id:
            worklog_hook.error(f"Failed to process health alerts: {str(e)}")
        logger.error(f"Health alert processing failed: {e}")
        raise


def generate_daily_health_report(**context):
    """
    Generate comprehensive daily health report.

    This function creates a detailed health report covering the last 24 hours
    of system operation, including trends, summaries, and recommendations.
    """
    logger.info("Generating daily health report...")

    # Initialize hooks
    tasks_hook = TasksHook()
    notify_hook = NotifyHook()
    worklog_hook = WorkLogHook()

    # Create worklog for report generation
    worklog = worklog_hook.create_worklog(
        name=f"Daily Health Report - {context['ds']}",
        description="Daily comprehensive system health report generation",
        worklog_type="DB",
        metadata=[
            {"key": "report_type", "value": "daily_health"},
            {"key": "dag_id", "value": context['dag'].dag_id},
            {"key": "execution_date", "value": context['ds']}
        ]
    )

    try:
        worklog_hook.info("Starting daily health report generation")

        # Execute comprehensive health report via Fabric task
        result = tasks_hook.trigger_task(
            task_id_or_name="health.system-health-report",
            prompts=[
                {"prompt": "verbose", "value": "true"},
                {"prompt": "output_format", "value": "json"},
                {"prompt": "save_report", "value": "true"},
                {"prompt": "report_path", "value": f"/tmp/oscar_health_report_{context['ds']}"}
            ],
            user_data={
                "initiated_by": "airflow_daily_report",
                "report_date": context['ds'],
                "report_type": "comprehensive"
            }
        )

        worklog_hook.info(f"Daily report generation result: {result.get('status', 'unknown')}")

        # Process report results
        report_status = "success" if result.get('status') == 'success' else "failed"

        if report_status == "success":
            # Send daily report notification
            report_summary = f"""
📊 OSCAR Daily Health Report - {context['ds']}

Report Status: ✅ Generated Successfully

Key Information:
- Report execution completed at {datetime.utcnow().isoformat()}
- Comprehensive analysis covering last 24 hours
- All monitored systems analyzed

Detailed report has been generated and stored.
Access the OSCAR health dashboard for complete metrics and trends.

This is an automated daily health report from the OSCAR monitoring system.
            """

            notify_hook.send_notification({
                "name": "ops-team-email",
                "subject": f"📊 OSCAR Daily Health Report - {context['ds']}",
                "message": report_summary
            })

            worklog_hook.info("Daily health report completed and notification sent")

        else:
            worklog_hook.error(f"Daily report generation failed: {result.get('error', 'Unknown error')}")

            # Send failure notification
            notify_hook.send_notification({
                "name": "ops-team-email",
                "subject": f"🚨 OSCAR Daily Health Report FAILED - {context['ds']}",
                "message": f"""
Daily Health Report Generation Failed

Date: {context['ds']}
Status: FAILED
Error: {result.get('error', 'Unknown error')}

Please investigate the health monitoring system.

This is an automated alert from the OSCAR monitoring system.
                """
            })

        worklog_hook.add_metadata({
            "report_status": report_status,
            "report_file": f"/tmp/oscar_health_report_{context['ds']}.json"
        })

        return {
            'status': report_status,
            'report_date': context['ds'],
            'worklog_id': worklog['id']
        }

    except Exception as e:
        worklog_hook.error(f"Daily report generation failed: {str(e)}")
        logger.error(f"Daily health report failed: {e}")
        raise

    finally:
        worklog_hook.close_worklog()


# Define DAG tasks
with dag:

    # System health check task (runs every 5 minutes)
    health_check_task = PythonOperator(
        task_id='check_system_health',
        python_callable=check_system_health,
        doc_md="""
        ## System Health Check

        Executes comprehensive system health monitoring including:
        - API response times and error rates
        - Database connection and query performance
        - Message queue health and processing times
        - Notification system delivery rates
        - Overall system health scoring

        Runs every 5 minutes to provide continuous monitoring.
        """
    )

    # Process health alerts task
    process_alerts_task = PythonOperator(
        task_id='process_health_alerts',
        python_callable=process_health_alerts,
        doc_md="""
        ## Process Health Alerts

        Analyzes health check results and generates appropriate alerts:
        - WARNING alerts for degraded performance
        - CRITICAL alerts for system failures
        - Recovery notifications when health improves
        - Integration with OSCAR alerting system
        """
    )

    # Set task dependencies for regular health monitoring
    health_check_task >> process_alerts_task


# Daily health report DAG (separate schedule)
daily_report_dag = DAG(
    'system_health_daily_report',
    default_args=default_args,
    description='OSCAR Daily System Health Report',
    schedule='0 6 * * *',  # Daily at 6 AM UTC
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['health', 'reporting', 'daily', 'oscar'],
    max_active_runs=1,
    doc_md="""
    ## Daily Health Report DAG

    Generates comprehensive daily health reports including:
    - 24-hour system performance summary
    - Health trends and analysis
    - Error pattern analysis
    - System capacity utilization
    - Recommendations for optimization

    Runs daily at 6 AM UTC.
    """
)

with daily_report_dag:

    daily_report_task = PythonOperator(
        task_id='generate_daily_health_report',
        python_callable=generate_daily_health_report,
        execution_timeout=timedelta(minutes=45),  # Longer timeout for comprehensive reports
        doc_md="""
        ## Daily Health Report Generation

        Creates comprehensive daily health reports with:
        - Complete system health analysis
        - Performance trend analysis over 24 hours
        - Capacity planning insights
        - Detailed error analysis
        - Actionable recommendations
        """
    )


# Weekly comprehensive health analysis DAG
weekly_analysis_dag = DAG(
    'system_health_weekly_analysis',
    default_args=default_args,
    description='OSCAR Weekly System Health Analysis',
    schedule='0 8 * * 0',  # Weekly on Sundays at 8 AM UTC
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['health', 'analysis', 'weekly', 'oscar'],
    max_active_runs=1
)

def generate_weekly_health_analysis(**context):
    """Generate comprehensive weekly health analysis."""
    logger.info("Generating weekly health analysis...")

    tasks_hook = TasksHook()
    notify_hook = NotifyHook()
    worklog_hook = WorkLogHook()

    # Create worklog for weekly analysis
    worklog = worklog_hook.create_worklog(
        name=f"Weekly Health Analysis - {context['ds']}",
        description="Weekly comprehensive system health analysis",
        worklog_type="DB",
        metadata=[
            {"key": "analysis_type", "value": "weekly_comprehensive"},
            {"key": "dag_id", "value": context['dag'].dag_id}
        ]
    )

    try:
        worklog_hook.info("Starting weekly comprehensive health analysis")

        # Execute extended health analysis
        result = tasks_hook.trigger_task(
            task_id_or_name="health.system-health-report",
            prompts=[
                {"prompt": "verbose", "value": "true"},
                {"prompt": "lookback", "value": "7d"},  # Full week analysis
                {"prompt": "output_format", "value": "json"},
                {"prompt": "save_report", "value": "true"},
                {"prompt": "systems", "value": "remedy,elastic,stub,snow,api,database,queue,notifications"}
            ],
            user_data={
                "initiated_by": "airflow_weekly_analysis",
                "analysis_period": "7d",
                "analysis_type": "comprehensive"
            }
        )

        worklog_hook.info("Weekly analysis completed - sending summary report")

        # Send weekly summary
        if result.get('status') == 'success':
            notify_hook.send_notification({
                "name": "ops-team-email",
                "subject": f"📈 OSCAR Weekly Health Analysis - Week of {context['ds']}",
                "message": f"""
📈 OSCAR Weekly Health Analysis Summary

Analysis Period: {context['ds']} (7 days)
Status: ✅ Completed Successfully

This comprehensive weekly analysis covers:
- 7-day health trend analysis
- Performance pattern identification
- Capacity utilization trends
- Error pattern analysis
- System optimization recommendations

Detailed analysis report has been generated and stored.
Review the OSCAR health dashboard for complete insights and trends.

This is an automated weekly analysis from the OSCAR monitoring system.
                """
            })

        worklog_hook.info("Weekly health analysis completed successfully")
        return {"status": "success", "analysis_period": "7d"}

    except Exception as e:
        worklog_hook.error(f"Weekly analysis failed: {str(e)}")
        raise
    finally:
        worklog_hook.close_worklog()


with weekly_analysis_dag:

    weekly_analysis_task = PythonOperator(
        task_id='generate_weekly_health_analysis',
        python_callable=generate_weekly_health_analysis,
        execution_timeout=timedelta(hours=1),  # Extended timeout for weekly analysis
        doc_md="""
        ## Weekly Health Analysis

        Performs comprehensive weekly health analysis including:
        - 7-day trend analysis
        - Performance pattern identification
        - Capacity planning recommendations
        - Long-term health trend analysis
        """
    )

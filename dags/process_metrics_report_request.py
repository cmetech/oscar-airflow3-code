import logging
import re
import json
import base64
import os
import io
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple, Union
from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
import requests
import pandas as pd
from urllib.parse import quote
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from jinja2 import Environment, FileSystemLoader

# Import hooks
from hooks.worklog_hook import WorkLogHook  # type: ignore
from hooks.notify_hook import NotifyHook  # type: ignore

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Email validation regex
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

# Default values
DEFAULT_DAYS = 7
DEFAULT_FORMAT = "long"
DEFAULT_INTERVAL = "15min"
MAX_ATTACHMENT_SIZE = 1024 * 1024  # 1MB
OSCARADMIN_GROUP_ID = "oscaradmin_group"  # Notifier ID for OSCAR admin group

# Set up Jinja2 template environment
TEMPLATE_DIR = "/opt/airflow/templates/email/html"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def validate_email_address(email: str) -> bool:
    """Validate email address format."""
    return EMAIL_REGEX.match(email.strip()) is not None


def filter_recipient_emails(emails: List[str], inbox_email: Optional[str] = None) -> List[str]:
    """
    Filter email list to exclude the OSCAR inbox address.
    
    This prevents error notifications from being sent back to the inbox
    that is being processed, avoiding potential loops.
    
    Args:
        emails: List of email addresses
        inbox_email: The inbox email to exclude (from to_addr)
        
    Returns:
        Filtered list of email addresses
    """
    filtered = []
    inbox_lower = inbox_email.lower() if inbox_email else None
    
    for email in emails:
        email_clean = email.strip()
        email_lower = email_clean.lower()
        
        # Skip the inbox email if provided
        if inbox_lower and email_lower == inbox_lower:
            continue
        # Skip common OSCAR inbox patterns
        if email_lower.startswith('oscar@'):
            continue
            
        filtered.append(email_clean)
    return filtered


def get_oscaradmin_emails(notify_hook: NotifyHook, worklog_hook: Optional[WorkLogHook] = None) -> List[str]:
    """
    Retrieve email addresses from the oscaradmin_group notifier.
    
    This function attempts to fetch the oscaradmin_group notifier configuration
    and extract email addresses for CC'ing on error notifications.
    
    Args:
        notify_hook: The NotifyHook instance
        worklog_hook: Optional WorkLogHook for logging
        
    Returns:
        List of email addresses from the admin group, or empty list if not found/error
    """
    try:
        # Get the oscaradmin_group notifier by name
        notifier_data = notify_hook.get_notifier(OSCARADMIN_GROUP_ID, by_name=True)
        
        # Check if it's an email notifier
        if notifier_data.get("type") != "email":
            if worklog_hook:
                worklog_hook.warning(f"Notifier '{OSCARADMIN_GROUP_ID}' is not an email notifier")
            return []
        
        # Extract email addresses - they can be in different places depending on the notifier type
        admin_emails = []
        
        # First check for direct email_addresses field (as seen in the log)
        if "email_addresses" in notifier_data:
            email_addresses = notifier_data["email_addresses"]
            if isinstance(email_addresses, list):
                admin_emails = [email.strip() for email in email_addresses if email.strip()]
            elif isinstance(email_addresses, str):
                admin_emails = [email.strip() for email in email_addresses.split(',') if email.strip()]
        
        # If not found, check in config.recipients (alternative structure)
        elif "config" in notifier_data:
            config = notifier_data["config"]
            recipients = config.get("recipients", [])
            if isinstance(recipients, str):
                admin_emails = [email.strip() for email in recipients.split(',') if email.strip()]
            elif isinstance(recipients, list):
                admin_emails = [email.strip() for email in recipients if email.strip()]
        
        if worklog_hook and admin_emails:
            worklog_hook.info(f"Found {len(admin_emails)} admin emails from '{OSCARADMIN_GROUP_ID}' notifier")
        
        return admin_emails
        
    except Exception as e:
        logger.warning(f"Failed to retrieve oscaradmin_group emails: {str(e)}")
        if worklog_hook:
            worklog_hook.warning(f"Could not retrieve admin emails from '{OSCARADMIN_GROUP_ID}': {str(e)}")
        return []


def strip_quotes(value: str) -> str:
    """Strip matching quotes from a string."""
    value = value.strip()
    if len(value) >= 2:
        if value[0] == "'" and value[-1] == "'":
            return value[1:-1].strip()
        elif value[0] == '"' and value[-1] == '"':
            return value[1:-1].strip()
    return value


def render_email_template(template_name: str, context: Dict[str, Any]) -> str:
    """
    Render an email template using Jinja2.
    
    Args:
        template_name: Name of the template file (without path)
        context: Dictionary of variables to pass to the template
        
    Returns:
        Rendered HTML content
    """
    try:
        template = jinja_env.get_template(template_name)
        return template.render(**context)
    except Exception as e:
        logger.error(f"Failed to render template {template_name}: {str(e)}")
        # Fallback to a simple error message
        return f"<html><body><h2>Error</h2><p>Failed to render email template: {str(e)}</p></body></html>"


def parse_list_from_body(text: str, key: str) -> List[str]:
    """Parse a list of values from email body for a given key."""
    values = []
    
    logger.debug(f"parse_list_from_body: Looking for key '{key}'")
    
    # Helper function to clean values (remove leading dashes, asterisks, etc.)
    def clean_value(val: str) -> str:
        val = val.strip()
        # Remove leading list markers (-, *, •, etc.)
        while val and val[0] in '-*•·':
            val = val[1:].strip()
        return strip_quotes(val)
    
    # Pattern for inline comma-separated values
    inline_pattern = rf'^\s*{key}\s*:\s*([^\n]+)$'
    inline_match = re.search(inline_pattern, text, re.IGNORECASE | re.MULTILINE)
    
    if inline_match:
        value_text = inline_match.group(1).strip()
        logger.debug(f"Found inline match for '{key}': '{value_text}'")
        if value_text and not value_text.endswith(':'):
            # Check if it's a single value with a dash (like "- 192.168.0.12")
            if value_text.startswith(('-', '*', '•', '·')):
                # Single value with list marker
                cleaned = clean_value(value_text)
                if cleaned:
                    values = [cleaned]
                    logger.debug(f"Returning single value with list marker removed: {values}")
                    return values
            else:
                # Comma-separated values
                values = [clean_value(v) for v in value_text.split(',') if v.strip()]
                values = [v for v in values if v]  # Remove empty values
                if values:
                    logger.debug(f"Returning comma-separated values: {values}")
                    return values
    
    # Pattern for list format - handle flexible indentation
    # This pattern looks for the key followed by optional whitespace and colon,
    # then captures any number of list items (with - or *) that have more indentation than the key
    list_pattern = rf'^(\s*){key}\s*:\s*$'
    key_match = re.search(list_pattern, text, re.IGNORECASE | re.MULTILINE)
    
    if key_match:
        # Get the indentation level of the key
        key_indent = len(key_match.group(1))
        key_line_end = key_match.end()
        logger.debug(f"Found list-style key '{key}' with indent level {key_indent}")
        
        # Look for list items after the key that have more indentation
        remaining_text = text[key_line_end:]
        # Also look for items without list markers but with indentation
        item_pattern = rf'^(\s+)(.+)$'
        
        for match in re.finditer(item_pattern, remaining_text, re.MULTILINE):
            item_indent = len(match.group(1))
            item_text = match.group(2).strip()
            
            # Only include items that are indented more than the key
            if item_indent > key_indent and item_text:
                # Stop if we hit another key (contains :)
                if ':' in item_text and not item_text.startswith(('-', '*', '•', '·')):
                    logger.debug(f"Stopping - found another key: '{item_text}'")
                    break
                    
                value = clean_value(item_text)
                if value:
                    values.append(value)
                    logger.debug(f"Added list item: '{value}'")
            elif item_indent <= key_indent and item_text:
                # Stop if we hit a line that's not indented enough
                logger.debug(f"Stopping - found line with insufficient indent ({item_indent} <= {key_indent})")
                break
    
    logger.debug(f"parse_list_from_body returning: {values}")
    return values


def parse_single_value(text: str, key: str) -> Optional[str]:
    """Parse a single value from email body for a given key."""
    pattern = rf'{key}\s*:\s*([^\n]+)'
    match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
    if match:
        return strip_quotes(match.group(1).strip())
    return None


def parse_metrics_subject(subject: str) -> Optional[str]:
    """Parse metrics type from email subject."""
    subject = subject.strip().upper()
    
    # Pattern: "METRICS REPORT: CPU"
    pattern = r'METRICS\s*REPORT\s*:\s*(\w+)'
    match = re.search(pattern, subject, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    
    return None


def parse_metrics_body(body: str) -> Dict[str, Any]:
    """Parse metrics parameters from email body."""
    params = {}
    
    if body is None:
        logger.warning("Email body is None, returning empty params")
        return params
    
    logger.debug(f"Starting to parse email body of length {len(body)}")
    logger.debug(f"Email body content:\n{body}")
    
    # Parse metrics
    metrics_value = parse_single_value(body, 'metrics') or parse_single_value(body, 'metric')
    if metrics_value:
        params['metrics'] = metrics_value.lower()
        logger.debug(f"Parsed metrics: {params['metrics']}")
    
    # Parse servers
    servers = parse_list_from_body(body, 'servers') or parse_list_from_body(body, 'server')
    logger.debug(f"parse_list_from_body returned: {servers}")
    
    if not servers:
        server_value = parse_single_value(body, 'servers') or parse_single_value(body, 'server')
        logger.debug(f"parse_single_value returned: {server_value}")
        if server_value:
            servers = [strip_quotes(s.strip()) for s in server_value.split(',') if s.strip()]
            logger.debug(f"Servers after splitting single value: {servers}")
    
    if servers:
        # Log what we parsed for debugging
        logger.info(f"Parsed servers from email body: {servers}")
        params['servers'] = servers
    
    # Parse format
    format_value = parse_single_value(body, 'format') or parse_single_value(body, 'report_format')
    if format_value:
        params['format'] = format_value.lower()
    
    # Parse days
    days_value = parse_single_value(body, 'days')
    if days_value:
        days_match = re.search(r'(\d+)', days_value)
        if days_match:
            params['days'] = int(days_match.group(1))
    
    # Parse start/end dates
    start_date = parse_single_value(body, 'start_date') or parse_single_value(body, 'start')
    if start_date:
        params['start_date'] = start_date
    
    end_date = parse_single_value(body, 'end_date') or parse_single_value(body, 'end')
    if end_date:
        params['end_date'] = end_date
    
    # Parse interval
    interval_value = parse_single_value(body, 'interval') or parse_single_value(body, 'time_interval')
    if interval_value:
        params['interval'] = interval_value
    
    # Parse notification emails
    notify_emails = parse_list_from_body(body, 'notify') or parse_list_from_body(body, 'notifications')
    if not notify_emails:
        notify_value = parse_single_value(body, 'notify') or parse_single_value(body, 'notifications')
        if notify_value:
            notify_emails = [strip_quotes(e.strip()) for e in notify_value.split(',') if e.strip()]
    if notify_emails:
        params['notify_emails'] = notify_emails
    
    # Parse notifier ID
    notifier_id = parse_single_value(body, 'notifier_id') or parse_single_value(body, 'notifier')
    if notifier_id:
        params['notifier_id'] = notifier_id
    
    return params


def get_export_query(metric: str, instance: str) -> Union[str, List[str]]:
    """Generate Victoria Metrics export query for a specific metric."""
    # Use meta_ipaddress label to match the actual metrics
    ip_filter = f'meta_ipaddress="{instance}"'
    
    if metric == "cpu":
        return f'node_cpu_seconds_total{{{ip_filter},mode="idle"}}'
    elif metric == "memory":
        # Return list of queries for memory metrics - each needs its own match[] param
        return [
            f"node_memory_MemFree_bytes{{{ip_filter}}}",
            f"node_memory_Cached_bytes{{{ip_filter}}}",
            f"node_memory_Buffers_bytes{{{ip_filter}}}",
            f"node_memory_MemTotal_bytes{{{ip_filter}}}"
        ]
    elif metric == "swap":
        # Return list of queries for swap metrics - each needs its own match[] param
        return [
            f"node_memory_SwapTotal_bytes{{{ip_filter}}}",
            f"node_memory_SwapFree_bytes{{{ip_filter}}}"
        ]
    else:
        raise ValueError(f"Unsupported metric: {metric}")


def fetch_metrics_from_vm(
    victoria_metrics_url: str,
    query: str,
    start_timestamp: int,
    end_timestamp: int,
    worklog_hook: Optional[WorkLogHook] = None
) -> Dict[str, Any]:
    """Fetch metrics data from Victoria Metrics using CSV export."""
    try:
        # Build export URL - use CSV export like CLI and task manager
        export_url = f"{victoria_metrics_url}/api/v1/export/csv"
        
        params = {
            "match[]": query,
            "start": str(start_timestamp),
            "end": str(end_timestamp),
            "format": "__name__,__value__,__timestamp__:unix_s,__labels__"
        }
        
        logger.info(f"Victoria Metrics URL: {export_url}")
        logger.info(f"Query: {query}")
        logger.info(f"Params: {params}")
        
        if worklog_hook:
            worklog_hook.info(f"Fetching metrics from: {export_url}")
            worklog_hook.info(f"Query: {query}")
            worklog_hook.info(f"Full params: {params}")
        
        # Make request with SSL verification disabled for internal calls
        response = requests.get(export_url, params=params, verify=False, timeout=300)
        logger.info(f"Response status code: {response.status_code}")
        
        response.raise_for_status()
        
        # Log response content for debugging
        logger.debug(f"Response text length: {len(response.text)}")
        if len(response.text) > 0:
            logger.debug(f"First 500 chars of response: {response.text[:500]}")
        else:
            logger.warning("Empty response from Victoria Metrics")
        
        csv_content = response.text
        
        if worklog_hook:
            worklog_hook.info(f"Fetched CSV data, size: {len(csv_content)} bytes")
        
        return {"status": "success", "csv_content": csv_content}
        
    except requests.exceptions.RequestException as re:
        error_msg = f"Request failed: {str(re)}"
        logger.error(error_msg)
        logger.error(f"Request URL was: {export_url}")
        if worklog_hook:
            worklog_hook.error(error_msg)
        return {"status": "error", "message": error_msg}
    except Exception as e:
        error_msg = f"Failed to fetch metrics: {str(e)}"
        logger.error(error_msg, exc_info=True)
        if worklog_hook:
            worklog_hook.error(error_msg)
        return {"status": "error", "message": error_msg}


def process_raw_metrics_with_instance(csv_content: str, metric_name: str, instance: str) -> List[Dict]:
    """Process raw metrics CSV data with a specific instance."""
    return process_raw_metrics(csv_content, metric_name, instance)


def process_raw_metrics(csv_content: str, metric_name: str, instance: str = None) -> List[Dict]:
    """Process raw metrics CSV data and calculate utilization percentages."""
    logger.debug(f"=== process_raw_metrics called for metric: {metric_name} ===")
    
    if not csv_content or not csv_content.strip():
        logger.warning(f"No CSV content provided for metric: {metric_name}")
        return []
    
    logger.debug(f"Processing CSV content, size: {len(csv_content)} bytes")
    logger.debug(f"First 500 chars of CSV: {csv_content[:500]}")
    
    try:
        # Parse CSV content - use simple approach like CLI and Tasks
        import io
        
        df = pd.read_csv(io.StringIO(csv_content), header=None)
        
        if df.empty:
            logger.warning(f"Empty DataFrame parsed from CSV for {metric_name}")
            return []
        
        # Always assume 4 columns like CLI and Tasks
        df.columns = ["metric", "value", "timestamp", "labels"]
        
        logger.debug(f"Parsed {len(df)} rows from CSV")
        logger.debug(f"DataFrame head:\n{df.head()}")
        
        # Convert timestamp to datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        
        # Use the provided instance if labels are not available
        default_instance = instance
        if not default_instance and ("labels" not in df.columns or df["labels"].isna().all()):
            # Try to extract instance from the original query
            import re
            match = re.search(r'meta_ipaddress="([^"]+)"', csv_content)
            if match:
                default_instance = match.group(1)
            else:
                default_instance = "unknown"
            logger.debug(f"Extracted instance from query: {default_instance}")
        
        # Calculate utilization based on metric type
        if metric_name == "cpu":
            return calculate_cpu_utilization_df(df, default_instance)
        elif metric_name == "memory":
            return calculate_memory_utilization_df(df, default_instance)
        elif metric_name == "swap":
            return calculate_swap_utilization_df(df, default_instance)
        else:
            logger.error(f"Unknown metric type: {metric_name}")
            return []
            
    except Exception as e:
        logger.error(f"Error processing {metric_name} metrics: {str(e)}", exc_info=True)
        return []


def calculate_cpu_utilization_df(df: pd.DataFrame, default_instance: str = None) -> List[Dict]:
    """Calculate CPU utilization from idle time metrics."""
    logger.debug(f"Calculating CPU utilization from {len(df)} rows")
    
    if df.empty:
        return []
    
    # Sort by timestamp
    df = df.sort_values("timestamp")
    
    # Calculate the difference in counter value between consecutive points
    df["value_diff"] = df["value"].diff()
    
    # Calculate the difference in time (in seconds) between consecutive points
    df["time_diff_sec"] = df["timestamp"].diff().dt.total_seconds()
    
    # Handle counter resets and division by zero
    valid_rows = (df["time_diff_sec"] > 0) & (df["value_diff"] >= 0)
    df_valid = df[valid_rows].copy()
    
    if df_valid.empty:
        logger.warning("Not enough valid data points to calculate CPU rate")
        return []
    
    # Calculate the idle rate (idle seconds per second)
    df_valid["idle_rate"] = df_valid["value_diff"] / df_valid["time_diff_sec"]
    
    # Calculate utilization: 100 * (1 - idle_rate)
    df_valid["cpu_utilization"] = (1 - df_valid["idle_rate"]) * 100
    
    # Clamp values between 0 and 100
    df_valid["cpu_utilization"] = df_valid["cpu_utilization"].clip(0, 100)
    
    # Extract instance from labels if available, otherwise use default
    if "labels" in df_valid.columns and not df_valid["labels"].isna().all() and df_valid["labels"].dtype == 'object':
        try:
            df_valid["instance"] = df_valid["labels"].str.extract(r'meta_ipaddress="([^"]+)"')[0]
            # Fill any NaN values with default instance
            df_valid["instance"] = df_valid["instance"].fillna(default_instance or "unknown")
        except Exception as e:
            logger.debug(f"Failed to extract instance from labels: {e}")
            df_valid["instance"] = default_instance or "unknown"
    else:
        df_valid["instance"] = default_instance or "unknown"
    
    # Convert to list of dicts
    result = []
    for _, row in df_valid.iterrows():
        result.append({
            "timestamp": row["timestamp"],
            "instance": row["instance"],
            "metric": "cpu_utilization",
            "value": round(row["cpu_utilization"], 2)
        })
    
    logger.info(f"Processed {len(result)} CPU data points")
    return result


def calculate_memory_utilization_df(df: pd.DataFrame, default_instance: str = None) -> List[Dict]:
    """Calculate memory utilization from memory metrics."""
    logger.debug(f"Calculating memory utilization from {len(df)} rows")
    
    if df.empty:
        return []
    
    # Extract instance from labels if available, otherwise use default
    if "labels" in df.columns and not df["labels"].isna().all() and df["labels"].dtype == 'object':
        try:
            df["instance"] = df["labels"].str.extract(r'meta_ipaddress="([^"]+)"')[0]
            # Fill any NaN values with default instance
            df["instance"] = df["instance"].fillna(default_instance or "unknown")
        except Exception as e:
            logger.debug(f"Failed to extract instance from labels: {e}")
            df["instance"] = default_instance or "unknown"
    else:
        df["instance"] = default_instance or "unknown"
    
    # Create a pivot table using metric names as columns
    pivot_df = df.pivot_table(
        index=["timestamp", "instance"], 
        columns="metric", 
        values="value"
    ).reset_index()
    
    # Calculate memory utilization
    pivot_df["memory_utilization"] = 100 * (
        1 - (
            (pivot_df["node_memory_MemFree_bytes"] +
             pivot_df["node_memory_Cached_bytes"] +
             pivot_df["node_memory_Buffers_bytes"]) /
            pivot_df["node_memory_MemTotal_bytes"]
        )
    )
    
    # Convert to list of dicts
    result = []
    for _, row in pivot_df.iterrows():
        result.append({
            "timestamp": row["timestamp"],
            "instance": row["instance"],
            "metric": "memory_utilization",
            "value": round(row["memory_utilization"], 2)
        })
    
    logger.info(f"Processed {len(result)} memory data points")
    return result


def calculate_swap_utilization_df(df: pd.DataFrame, default_instance: str = None) -> List[Dict]:
    """Calculate swap utilization from swap metrics."""
    logger.debug(f"Calculating swap utilization from {len(df)} rows")
    
    if df.empty:
        return []
    
    # Extract instance from labels if available, otherwise use default
    if "labels" in df.columns and not df["labels"].isna().all() and df["labels"].dtype == 'object':
        try:
            df["instance"] = df["labels"].str.extract(r'meta_ipaddress="([^"]+)"')[0]
            # Fill any NaN values with default instance
            df["instance"] = df["instance"].fillna(default_instance or "unknown")
        except Exception as e:
            logger.debug(f"Failed to extract instance from labels: {e}")
            df["instance"] = default_instance or "unknown"
    else:
        df["instance"] = default_instance or "unknown"
    
    # Create a pivot table using metric names as columns
    pivot_df = df.pivot_table(
        index=["timestamp", "instance"], 
        columns="metric", 
        values="value"
    ).reset_index()
    
    # Calculate swap utilization
    # Handle case where swap total is 0 (no swap configured)
    pivot_df["swap_utilization"] = 0.0
    mask = pivot_df["node_memory_SwapTotal_bytes"] > 0
    pivot_df.loc[mask, "swap_utilization"] = 100 * (
        (pivot_df.loc[mask, "node_memory_SwapTotal_bytes"] - 
         pivot_df.loc[mask, "node_memory_SwapFree_bytes"]) /
        pivot_df.loc[mask, "node_memory_SwapTotal_bytes"]
    )
    
    # Convert to list of dicts
    result = []
    for _, row in pivot_df.iterrows():
        result.append({
            "timestamp": row["timestamp"],
            "instance": row["instance"],
            "metric": "swap_utilization",
            "value": round(row["swap_utilization"], 2)
        })
    
    logger.info(f"Processed {len(result)} swap data points")
    return result


def create_long_format_df(all_metrics: List[Dict]) -> pd.DataFrame:
    """Create long format DataFrame with all metrics."""
    df = pd.DataFrame(all_metrics)
    if df.empty:
        return df
    
    # Sort by timestamp and instance
    df = df.sort_values(['timestamp', 'instance', 'metric'])
    
    # Format timestamp
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Round values
    df['value'] = df['value'].round(2)
    
    return df[['timestamp', 'instance', 'metric', 'value']]


def create_wide_format_df(all_metrics: List[Dict]) -> pd.DataFrame:
    """Create wide format DataFrame with metrics as columns."""
    df = pd.DataFrame(all_metrics)
    if df.empty:
        return df
    
    # Pivot the data
    pivot_df = df.pivot_table(
        index='timestamp',
        columns=['instance', 'metric'],
        values='value',
        aggfunc='mean'
    )
    
    # Flatten column names
    pivot_df.columns = [f"{instance}_{metric}" for instance, metric in pivot_df.columns]
    
    # Reset index and format timestamp
    pivot_df = pivot_df.reset_index()
    pivot_df['timestamp'] = pivot_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Round values
    numeric_cols = pivot_df.select_dtypes(include=['float64']).columns
    pivot_df[numeric_cols] = pivot_df[numeric_cols].round(2)
    
    return pivot_df


def dataframe_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to XLSX bytes in memory."""
    # Create a BytesIO buffer
    buffer = io.BytesIO()
    
    # Write DataFrame to Excel
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Metrics Report')
        
        # Get the workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets['Metrics Report']
        
        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            
            adjusted_width = min(max_length + 2, 50)  # Cap at 50 chars
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    # Get the bytes
    buffer.seek(0)
    return buffer.read()


def create_compact_format_df(all_metrics: List[Dict]) -> pd.DataFrame:
    """Create compact format DataFrame with aggregated statistics."""
    df = pd.DataFrame(all_metrics)
    if df.empty:
        return df
    
    # Group by instance and metric
    grouped = df.groupby(['instance', 'metric'])['value']
    
    # Calculate statistics
    stats_df = grouped.agg(['min', 'max', 'mean', 'std']).round(2)
    stats_df = stats_df.reset_index()
    
    # Add percentiles
    percentiles = df.groupby(['instance', 'metric'])['value'].quantile([0.5, 0.95]).round(2)
    percentiles = percentiles.unstack()
    percentiles.columns = ['p50', 'p95']
    percentiles = percentiles.reset_index()
    
    # Merge statistics
    final_df = pd.merge(stats_df, percentiles, on=['instance', 'metric'])
    
    # Reorder columns
    final_df = final_df[['instance', 'metric', 'min', 'max', 'mean', 'std', 'p50', 'p95']]
    
    return final_df


def send_notification_email(
    notify_hook: NotifyHook,
    to_addresses: List[str],
    subject: str,
    body: str,
    attachment_data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
    worklog_hook: Optional[WorkLogHook] = None,
    notifier_id: str = "mail_notifier"
) -> bool:
    """Send notification email using NotifyHook."""
    try:
        # Validate email addresses
        valid_emails = [email for email in to_addresses if validate_email_address(email)]
        if not valid_emails:
            logger.error(f"No valid email addresses found in: {to_addresses}")
            return False
        
        # Prepare notification data
        notification_data = {
            "name": notifier_id,
            "recipients": ",".join(valid_emails),
            "subject": subject,
            "message": body
        }
        
        # Add attachments if provided
        if attachment_data:
            attachments = []
            
            # Handle single attachment (backward compatibility)
            if isinstance(attachment_data, dict):
                attachment_data = [attachment_data]
            
            # Process each attachment
            for attachment in attachment_data:
                if attachment.get("content"):
                    content = attachment["content"]
                    # Base64 encode the content if it's a string
                    if isinstance(content, str):
                        encoded_content = base64.b64encode(content.encode()).decode()
                    elif isinstance(content, bytes):
                        encoded_content = base64.b64encode(content).decode()
                    else:
                        continue
                    
                    attachments.append({
                        "filename": attachment.get("filename", "report.csv"),
                        "content": encoded_content,
                        "content_type": attachment.get("content_type", "text/csv")
                    })
            
            if attachments:
                notification_data["attachments"] = attachments
        
        result = notify_hook.send_notification(notification_data)
        
        if worklog_hook:
            worklog_hook.info(f"Sent notification to {', '.join(valid_emails)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to send notification email: {e}")
        if worklog_hook:
            worklog_hook.error(f"Failed to send notification: {e}")
        return False


def process_metrics_report_request(**context):
    """
    Process incoming metrics report request email.
    
    This function:
    1. Extracts email data from the DAG run context
    2. Parses the subject and body for report parameters
    3. Validates all inputs
    4. Fetches metrics from Victoria Metrics
    5. Generates CSV report in requested format
    6. Sends report via email
    """
    # Initialize variables
    worklog_hook = None
    worklog_id = None
    routing_worklog_id = None
    result = None
    
    try:
        # Get email data from context
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        email_data = conf.get("email_data", {})
        
        # Get routing worklog ID if passed
        routing_worklog_id = conf.get("worklog_id")
        
        if not email_data:
            logger.error("No email data provided in DAG run configuration")
            return None
        
        # Extract email fields
        subject = email_data.get("subject", "(No Subject)")
        from_addr = email_data.get("from", "(No From Address)")
        to_addr = email_data.get("to", "(No To Address)")
        cc_addr = email_data.get("cc", "")
        date = email_data.get("date", str(datetime.now(timezone.utc)))
        body = email_data.get("body") or ""  # Handle None values
        
        # Initialize hooks
        worklog_hook = WorkLogHook()
        notify_hook = NotifyHook()
        
        # Create worklog
        worklog = worklog_hook.create_worklog(
            name="Metrics Report Processing",
            description=f"Processing metrics report request from {from_addr}"
        )
        worklog_id = worklog["id"]
        
        # Get admin emails for error notifications
        admin_emails = get_oscaradmin_emails(notify_hook, worklog_hook)
        
        # Log email details
        worklog_hook.info("Processing metrics report request email")
        worklog_hook.info(f"Subject: {subject}")
        worklog_hook.info(f"From: {from_addr}")
        worklog_hook.info(f"To: {to_addr}")
        if cc_addr:
            worklog_hook.info(f"CC: {cc_addr}")
        worklog_hook.info(f"Date: {date}")
        
        # Initialize notification list
        notification_emails = [from_addr]
        
        # Add to/cc addresses
        if to_addr:
            to_emails = [e.strip() for e in to_addr.split(',') if e.strip()]
            notification_emails.extend(to_emails)
        if cc_addr:
            cc_emails = [e.strip() for e in cc_addr.split(',') if e.strip()]
            notification_emails.extend(cc_emails)
        
        # Parse body for parameters
        worklog_hook.info("Parsing email body for parameters")
        logger.info(f"Email body to parse:\n{body}")
        body_params = parse_metrics_body(body)
        logger.info(f"Parsed body params: {body_params}")
        
        # Get metrics from subject if not in body
        metrics = body_params.get('metrics')
        if not metrics:
            subject_metrics = parse_metrics_subject(subject)
            if subject_metrics:
                metrics = subject_metrics
                worklog_hook.info(f"Metrics found in subject: {metrics}")
        
        if not metrics:
            metrics = "all"
            worklog_hook.info("No metrics specified, defaulting to 'all'")
        
        # Get other parameters with defaults
        servers = body_params.get('servers', [])
        format_type = body_params.get('format', DEFAULT_FORMAT)
        days = body_params.get('days', DEFAULT_DAYS)
        interval = body_params.get('interval', DEFAULT_INTERVAL)
        start_date = body_params.get('start_date')
        end_date = body_params.get('end_date')
        notifier_id = body_params.get('notifier_id', 'mail_notifier')
        
        # Add notify emails from body
        if body_params.get('notify_emails'):
            notification_emails.extend(body_params['notify_emails'])
        
        # Remove duplicates and validate
        notification_emails = list(set(email for email in notification_emails if validate_email_address(email)))
        
        # Validate required fields
        logger.info(f"Servers before validation: {servers}")
        if not servers:
            error_msg = "Server list is required for metrics report"
            worklog_hook.error(error_msg)
            
            # Filter out the inbox email to avoid loops
            error_recipients = filter_recipient_emails(notification_emails, to_addr)
            
            # Add admin emails for CC
            if admin_emails:
                # Combine with error recipients, removing duplicates
                all_error_recipients = list(set(error_recipients + admin_emails))
            else:
                all_error_recipients = error_recipients
            
            # Render the error template
            email_body = render_email_template("metrics_report_missing_servers.j2", {
                "error_msg": error_msg,
                "subject": subject,
                "from_addr": from_addr,
                "date": date
            })
            
            send_notification_email(
                notify_hook,
                all_error_recipients,
                "Metrics Report Failed - Missing Servers",
                email_body,
                worklog_hook=worklog_hook,
                notifier_id=notifier_id
            )
            
            result = {"status": "error", "message": error_msg}
            return result
        
        # Calculate time range
        if start_date and end_date:
            # Parse provided dates
            try:
                # Try RFC3339 format
                start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            except:
                # Try Unix timestamp
                try:
                    start_dt = datetime.fromtimestamp(int(start_date), tz=timezone.utc)
                    end_dt = datetime.fromtimestamp(int(end_date), tz=timezone.utc)
                except:
                    raise ValueError("Invalid date format. Use RFC3339 or Unix timestamp")
        else:
            # Use days parameter
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(days=days)
        
        start_timestamp = int(start_dt.timestamp())
        end_timestamp = int(end_dt.timestamp())
        
        worklog_hook.info(f"Time range: {start_dt} to {end_dt}")
        worklog_hook.info(f"Format: {format_type}, Interval: {interval}")
        
        # Log all final parameters for debugging
        logger.info("=== Final Parameters ===")
        logger.info(f"Servers: {servers}")
        logger.info(f"Metrics: {metrics}")
        logger.info(f"Format: {format_type}")
        logger.info(f"Days: {days}")
        logger.info(f"Start timestamp: {start_timestamp}")
        logger.info(f"End timestamp: {end_timestamp}")
        logger.info("=======================")
        
        # Get Victoria Metrics configuration
        # Use direct connection to Victoria Metrics service instead of going through middleware
        vmdb_host = os.getenv('VMDB_HOST', 'vmdb')
        vmdb_port = os.getenv('VMDB_PORT', '8428')
        victoria_metrics_url = f"http://{vmdb_host}:{vmdb_port}"
        
        # Determine which metrics to fetch
        if metrics == "all":
            metric_types = ["cpu", "memory", "swap"]
        else:
            metric_types = [m.strip() for m in metrics.split(",") if m.strip()]
        
        # Fetch all metrics
        all_metrics_data = []
        
        # Process each server individually
        for server in servers:
            # Clean server name - remove any leading dash or asterisk that might have been included
            server_clean = server.strip()
            if server_clean.startswith('-') or server_clean.startswith('*'):
                server_clean = server_clean[1:].strip()
            
            worklog_hook.info(f"Processing server: original='{server}', cleaned='{server_clean}'")
            
            for metric_type in metric_types:
                worklog_hook.info(f"Fetching {metric_type} metrics for {server_clean}...")
                
                try:
                    # Generate query for this specific server
                    queries = get_export_query(metric_type, server_clean)
                    
                    # Handle both single query and list of queries
                    if isinstance(queries, str):
                        queries = [queries]
                    
                    # Collect CSV data from all queries
                    all_csv_data = []
                    
                    for query in queries:
                        # Fetch CSV data from Victoria Metrics
                        result = fetch_metrics_from_vm(
                            victoria_metrics_url,
                            query,
                            start_timestamp,
                            end_timestamp,
                            worklog_hook
                        )
                        
                        if result["status"] == "success":
                            csv_content = result.get("csv_content", "")
                            if csv_content:
                                all_csv_data.append(csv_content)
                            else:
                                worklog_hook.warning(f"Empty CSV content for query: {query}")
                        else:
                            worklog_hook.warning(f"Failed to fetch metrics for query '{query}': {result.get('message')}")
                    
                    if all_csv_data:
                        # Combine all CSV data
                        combined_csv = "\n".join(all_csv_data)
                        
                        # Process raw CSV data, passing server instance
                        processed = process_raw_metrics_with_instance(combined_csv, metric_type, server_clean)
                        if processed:
                            all_metrics_data.extend(processed)
                            worklog_hook.info(f"Processed {len(processed)} {metric_type} data points for {server_clean}")
                        else:
                            worklog_hook.warning(f"No data points processed for {metric_type} metrics for {server_clean}")
                    else:
                        worklog_hook.warning(f"No CSV data retrieved for {metric_type} metrics for {server_clean}")
                        
                except Exception as e:
                    worklog_hook.error(f"Error fetching {metric_type} for {server_clean}: {str(e)}")
        
        if not all_metrics_data:
            error_msg = "No metrics data could be retrieved"
            worklog_hook.error(error_msg)
            
            # Filter out the inbox email to avoid loops
            error_recipients = filter_recipient_emails(notification_emails, to_addr)
            
            # Add admin emails for CC
            if admin_emails:
                # Combine with error recipients, removing duplicates
                all_error_recipients = list(set(error_recipients + admin_emails))
            else:
                all_error_recipients = error_recipients
            
            # Render the error template
            email_body = render_email_template("metrics_report_no_data.j2", {
                "error_msg": error_msg,
                "servers": servers,
                "metrics": metrics,
                "start_dt": start_dt,
                "end_dt": end_dt
            })
            
            send_notification_email(
                notify_hook,
                all_error_recipients,
                "Metrics Report Failed - No Data",
                email_body,
                worklog_hook=worklog_hook,
                notifier_id=notifier_id
            )
            
            result = {"status": "error", "message": error_msg}
            return result
        
        # Generate report based on format
        worklog_hook.info(f"Generating {format_type} format report...")
        
        try:
            if format_type == "long":
                df = create_long_format_df(all_metrics_data)
            elif format_type == "wide":
                df = create_wide_format_df(all_metrics_data)
            elif format_type == "compact":
                df = create_compact_format_df(all_metrics_data)
            else:
                # Default to long format
                df = create_long_format_df(all_metrics_data)
            
            # Convert to CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Convert to XLSX
            xlsx_content = dataframe_to_xlsx_bytes(df)
            
            worklog_hook.info(f"Generated CSV report with {len(df)} rows")
            worklog_hook.info(f"Generated XLSX report with {len(xlsx_content)} bytes")
            
            # Prepare email content using template
            metrics_display = ', '.join(metric_types).upper()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Calculate report size in KB
            csv_size_kb = round(len(csv_content) / 1024, 1)
            xlsx_size_kb = round(len(xlsx_content) / 1024, 1)
            total_size_kb = round((len(csv_content) + len(xlsx_content)) / 1024, 1)
            
            # Render the success template
            report_summary = render_email_template("metrics_report_success.j2", {
                "servers": servers,
                "metrics": metrics_display,
                "format_type": format_type,
                "start_time": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                "end_time": end_dt.strftime('%Y-%m-%d %H:%M:%S'),
                "report_size_kb": total_size_kb,
                "data_points": len(df),
                "worklog_id": worklog_id,
                "csv_filename": f"metrics_report_{timestamp}.csv",
                "xlsx_filename": f"metrics_report_{timestamp}.xlsx"
            })
            
            # Prepare attachments - both CSV and XLSX
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            attachment_data = [
                {
                    "filename": f"metrics_report_{timestamp}.csv",
                    "content": csv_content,
                    "content_type": "text/csv"
                },
                {
                    "filename": f"metrics_report_{timestamp}.xlsx",
                    "content": xlsx_content,
                    "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                }
            ]
            
            send_notification_email(
                notify_hook,
                notification_emails,
                f"Metrics Report ({', '.join(metric_types).upper()}) - {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
                report_summary,
                attachment_data=attachment_data,
                worklog_hook=worklog_hook,
                notifier_id=notifier_id
            )
            
            worklog_hook.info("Metrics report processed and sent successfully")
            result = {"status": "success", "message": "Report generated and sent", "rows": len(df)}
            
        except Exception as e:
            error_msg = f"Failed to generate report: {str(e)}"
            worklog_hook.error(error_msg)
            
            # Filter out the inbox email to avoid loops
            error_recipients = filter_recipient_emails(notification_emails, to_addr)
            
            # Add admin emails for CC
            if admin_emails:
                # Combine with error recipients, removing duplicates
                all_error_recipients = list(set(error_recipients + admin_emails))
            else:
                all_error_recipients = error_recipients
            
            # Render the error template
            email_body = render_email_template("metrics_report_generation_error.j2", {
                "error_msg": error_msg,
                "servers": servers,
                "metrics": metrics,
                "start_dt": start_dt,
                "end_dt": end_dt
            })
            
            send_notification_email(
                notify_hook,
                all_error_recipients,
                "Metrics Report Failed - Generation Error",
                email_body,
                worklog_hook=worklog_hook,
                notifier_id=notifier_id
            )
            
            result = {"status": "error", "message": error_msg}
            return result
        
        return result
        
    except Exception as e:
        error_msg = f"Unexpected error processing metrics report: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        if worklog_hook:
            try:
                worklog_hook.error(error_msg)
            except:
                pass
        
        # Try to send error notification
        if 'notify_hook' in locals() and 'notification_emails' in locals() and notification_emails:
            try:
                # Filter out the inbox email to avoid loops
                error_recipients = filter_recipient_emails(
                    notification_emails, 
                    to_addr if 'to_addr' in locals() else None
                )
                
                # Add admin emails for CC if available
                if 'admin_emails' in locals() and admin_emails:
                    # Combine with error recipients, removing duplicates
                    all_error_recipients = list(set(error_recipients + admin_emails))
                else:
                    all_error_recipients = error_recipients
                
                # Render the error template
                email_body = render_email_template("metrics_report_system_error.j2", {
                    "error_msg": error_msg,
                    "subject": subject if 'subject' in locals() else None,
                    "from_addr": from_addr if 'from_addr' in locals() else None,
                    "date": date if 'date' in locals() else None
                })
                
                send_notification_email(
                    notify_hook,
                    all_error_recipients,
                    "Metrics Report Failed - System Error",
                    email_body,
                    worklog_hook=worklog_hook if 'worklog_hook' in locals() else None,
                    notifier_id=notifier_id if 'notifier_id' in locals() else 'email'
                )
            except:
                logger.error("Failed to send error notification")
        
        result = {"status": "error", "message": error_msg}
        return result
        
    finally:
        # Always close the worklog
        if worklog_hook and worklog_id:
            try:
                worklog_hook.close_worklog(worklog_id)
                logger.info(f"Closed worklog {worklog_id}")
            except Exception as e:
                logger.error(f"Failed to close worklog {worklog_id}: {e}")
        
        # Handle routing worklog
        if worklog_hook and routing_worklog_id:
            try:
                routing_worklog_hook = WorkLogHook(worklog_id=routing_worklog_id)
                
                if result and result.get("status") == "success":
                    routing_worklog_hook.info(
                        f"Email routed successfully to process_metrics_report_request. "
                        f"Report generated with {result.get('rows', 0)} rows. "
                        f"See worklog {worklog_id} for details."
                    )
                else:
                    error_msg = result.get("message", "Unknown error") if result else "Unknown error"
                    routing_worklog_hook.error(
                        f"Email routed to process_metrics_report_request but processing failed: {error_msg}. "
                        f"See worklog {worklog_id} for details."
                    )
                
                routing_worklog_hook.close_worklog()
                logger.info(f"Closed routing worklog {routing_worklog_id}")
                
            except Exception as e:
                logger.error(f"Failed to handle routing worklog {routing_worklog_id}: {e}")
        
        if result:
            return result
        else:
            return {"status": "error", "message": "Unknown error occurred"}


# Create the DAG
with DAG(
    dag_id="process_metrics_report_request",
    default_args=default_args,
    description="Process metrics report requests from email",
    schedule=None,  # Triggered by route_email_request
    start_date=pendulum.today('UTC').add(days=-1),
    tags=["email", "metrics", "reporting"],
    catchup=False
) as dag:
    
    # Define the task
    process_metrics_task = PythonOperator(
        task_id="process_metrics_report",
        python_callable=process_metrics_report_request,
    )

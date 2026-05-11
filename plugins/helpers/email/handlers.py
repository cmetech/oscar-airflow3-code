import re
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Tuple, List
import shlex
from helpers.email.extractors import extract_ip_or_hostname  # type: ignore

logger = logging.getLogger(__name__)


class BaseEmailHandler(ABC):
    """
    Base class for handling different types of email alerts.
    Subclasses should implement parse_subject and parse_body.
    """

    alert_type: Optional[str] = None

    def __init__(self, email_data: Dict[str, Any]):
        self.email_data = email_data
        self.subject = email_data.get("subject", "")
        self.body = email_data.get("body", "")

    @abstractmethod
    def parse_subject(self) -> Dict[str, Any]:
        """Parses the email subject and returns a dictionary of extracted fields."""
        # Basic subject parsing, can be overridden
        return {"raw_subject": self.subject}

    @abstractmethod
    def parse_body(self) -> Dict[str, Any]:
        """Parses the email body and returns a dictionary of extracted fields."""
        # Basic body parsing, can be overridden
        return {"raw_body": self.body}

    def _apply_common_transformations(self, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Applies common transformations like severity mapping, IP/host enrichment, etc.
        This logic is taken from the original create_alert_object function.
        """
        # Apply severity mapping if present.
        if "severity" in parsed_data:
            severity_value = parsed_data["severity"]
            try:
                severity_num = int(severity_value)
                if severity_num == 3:
                    mapped_severity = "critical"
                elif severity_num == 2:
                    mapped_severity = "major"
                elif severity_num == 1:
                    mapped_severity = "warning"
                else:
                    mapped_severity = "major"  # Default for out-of-range numbers
            except ValueError:
                # Not an integer; use lowercased value.
                severity_str = str(severity_value).lower()
                if severity_str in {"critical", "major", "warning", "info", "resolved"}:
                    mapped_severity = severity_str
                else:
                    mapped_severity = "major"  # Default for unrecognized strings
            parsed_data["severity"] = mapped_severity
        else:
            # if no severity is found in parsed_data, let's try to get it from common alertmanager label
            if "alertmanager_severity" in parsed_data:  # common for alertmanager
                parsed_data["severity"] = parsed_data["alertmanager_severity"]
            # else:
            # parsed_data["severity"] = "major" # Default if not present at all - decided against for now

        # Check for IP/host based information with enhanced extraction logic
        # Priority order:
        # 1. Existing parsed fields (from key-value pairs)
        # 2. Extracted from subject (from parse_subject)
        # 3. Extracted from body (from parse_body)
        
        # First check existing parsed fields
        ip_keys = ["sourceip", "ip_address", "hostname", "host", "ipaddress", "instance"]
        ip_value = None
        hostname_value = None
        
        for key in ip_keys:
            if key in parsed_data:
                value = parsed_data[key]
                if value:
                    # Determine if it's an IP or hostname
                    extracted = extract_ip_or_hostname(str(value))
                    if extracted["ip_address"]:
                        ip_value = extracted["ip_address"]
                    if extracted["hostname"]:
                        hostname_value = extracted["hostname"]
                    if not ip_value and not hostname_value:
                        # Fallback to original value if extraction failed
                        ip_value = value
                break
        
        # Check for extracted values from subject (higher priority)
        if "_extracted_from_subject" in parsed_data:
            subject_extracted = parsed_data.pop("_extracted_from_subject", {})
            if subject_extracted.get("ip_address") and not ip_value:
                ip_value = subject_extracted["ip_address"]
                logger.info(f"[EMAIL_HANDLER] Using IP from subject: {ip_value}")
            if subject_extracted.get("hostname") and not hostname_value:
                hostname_value = subject_extracted["hostname"]
                logger.info(f"[EMAIL_HANDLER] Using hostname from subject: {hostname_value}")
        
        # Check for extracted values from body (lower priority)
        if "_extracted_from_body" in parsed_data:
            body_extracted = parsed_data.pop("_extracted_from_body", {})
            if body_extracted.get("ip_address") and not ip_value:
                ip_value = body_extracted["ip_address"]
                logger.info(f"[EMAIL_HANDLER] Using IP from body: {ip_value}")
            if body_extracted.get("hostname") and not hostname_value:
                hostname_value = body_extracted["hostname"]
                logger.info(f"[EMAIL_HANDLER] Using hostname from body: {hostname_value}")
        
        # Set the meta fields based on what we found
        if ip_value:
            parsed_data["meta_ipaddress"] = ip_value
            if not parsed_data.get("instance"):
                parsed_data["instance"] = ip_value
                
        if hostname_value:
            parsed_data["meta_hostname"] = hostname_value
            if not parsed_data.get("instance"):
                parsed_data["instance"] = hostname_value
                
        # If we have both IP and hostname but no instance, prefer IP
        if ip_value and hostname_value and not parsed_data.get("instance"):
            parsed_data["instance"] = ip_value

        # If datacenter exists, add meta_datacenter.
        if "datacenter" in parsed_data:
            parsed_data["meta_datacenter"] = parsed_data["datacenter"]

        # If environment exists, add meta_environment.
        if "environment" in parsed_data:
            parsed_data["meta_environment"] = parsed_data["environment"]

        # Add meta_component if alert_type is defined for the handler.
        if self.alert_type:
            parsed_data["meta_component"] = self.alert_type

        # Ensure alertname exists
        alertname_keys = [
            "alertname",
            "alarmid",
            "alarm_id",
            "id",
            "alert_name",
            "alert_id",
            "alertid",
            "alarmname",
            "rulename",
            "rule_name",
        ]
        current_alertname = None
        for key in alertname_keys:
            if key in parsed_data:
                current_alertname = parsed_data.pop(key)  # Remove the original specific key
                break

        parsed_data["alertname"] = current_alertname if current_alertname else "unknown_alertname_from_handler"

        return parsed_data

    def _build_summary_and_description(self, parsed_data: Dict[str, Any]) -> Tuple[str, str]:
        """
        Builds the summary and description for the alert.
        """
        alertname = parsed_data.get("alertname", "unknown_alertname")

        # Use a more specific prefix if self.alert_type is available
        prefix = f"{self.alert_type.capitalize()} Audit Alert" if self.alert_type else "Audit Alert"
        summary = f"{prefix} for {alertname}"

        description = parsed_data.pop("description", None)  # Try to get from parsed_data first
        if not description:  # If not in parsed_data, use body, then summary
            description = self.body if self.body else summary

        return summary, description

    def create_alert_object_from_email(self) -> Dict[str, Any]:
        """
        Orchestrates the parsing of subject and body, applies common transformations,
        and prepares the data structure for the mapping pipeline.
        This method does NOT apply the mapping pipeline itself.
        """
        parsed_subject = self.parse_subject()
        parsed_body = self.parse_body()

        # Merge parsed data, body takes precedence in case of key conflicts
        # but we want to keep raw subject/body if they exist from parsing.
        labels = {**parsed_subject, **parsed_body}

        # Preserve raw fields if they were specifically set by parsers
        if "raw_subject" not in labels and "raw_subject" in parsed_subject:
            labels["raw_subject_details"] = parsed_subject["raw_subject"]
        if "raw_body" not in labels and "raw_body" in parsed_body:
            labels["raw_body_details"] = parsed_body["raw_body"]

        # Remove the original raw subject/body keys if they made it to the top level and were not specific parser outputs
        if "raw_subject" in labels and "raw_subject" not in parsed_subject:  # came from default base
            del labels["raw_subject"]
        if "raw_body" in labels and "raw_body" not in parsed_body:  # came from default base
            del labels["raw_body"]

        labels = self._apply_common_transformations(labels)
        summary, description = self._build_summary_and_description(labels)

        # The final object structure to be returned for further processing by the mapping pipeline
        return {
            "labels": labels,
            "summary": summary,
            "description": description,
            "original_email_data": self.email_data,  # For reference
        }


class DefaultEmailHandler(BaseEmailHandler):
    """
    Default handler for emails that don't match a specific type.
    Parses the body for key=value pairs.
    """

    alert_type = "default"  # Or None, to signify it's the fallback
    log_prefix = "[DefaultEmailHandler]"

    def parse_subject(self) -> Dict[str, Any]:
        logger.info(f"{self.log_prefix} Parsing subject: {self.subject}")
        parsed = {"raw_subject": self.subject}
        
        # Extract IP and hostname from subject
        if self.subject:
            extracted = extract_ip_or_hostname(self.subject)
            if extracted["ip_address"] or extracted["hostname"]:
                parsed["_extracted_from_subject"] = extracted
                logger.info(f"{self.log_prefix} Extracted from subject - IP: {extracted['ip_address']}, Hostname: {extracted['hostname']}")
        
        return parsed

    def parse_body(self) -> Dict[str, Any]:
        logger.info(f"{self.log_prefix} Parsing body (first 100 chars): {self.body[:100]}")
        parsed = {}
        
        # First, parse key-value pairs
        lexer = shlex.shlex(self.body, posix=True)
        lexer.whitespace_split = True
        lexer.commenters = ""
        lexer.quotes = '"'
        lexer.escape = ""
        lexer.whitespace = " \r\n\t"

        tokens = list(lexer)
        key_pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9_:-]*$")

        for token in tokens:
            if "=" in token:
                key, value = token.split("=", 1)
                key_stripped = key.strip()
                if key_pattern.match(key_stripped):
                    key_normalized = key_stripped.lower()
                    value_normalized = value.strip()
                    parsed[key_normalized] = value_normalized
        
        logger.info(f"{self.log_prefix} Parsed body labels: {parsed}")
        
        # Extract IP and hostname from body
        if self.body:
            extracted = extract_ip_or_hostname(self.body)
            if extracted["ip_address"] or extracted["hostname"]:
                parsed["_extracted_from_body"] = extracted
                logger.info(f"{self.log_prefix} Extracted from body - IP: {extracted['ip_address']}, Hostname: {extracted['hostname']}")
        
        return parsed

    def _apply_common_transformations(self, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        parsed_data = super()._apply_common_transformations(parsed_data)
        logger.info(f"{self.log_prefix} Final parsed_data after common transformations: {parsed_data}")
        return parsed_data


# Placeholder for Cohesity specific parsing functions until we integrate them
def cohesity_parse_subject_standalone(subject: str) -> Dict[str, Any]:
    """
    Standalone Cohesity email subject parser.
    (Content will be copied from process_cohesity_email_alerts.py)
    """
    parsed_subject_info = {"raw_subject": subject}
    subject_lower = subject.lower()
    parsed_subject_info["alert_severity_from_subject"] = "INFO"
    parsed_subject_info["alert_type_from_subject"] = "UnknownCohesityType"  # Default specific to Cohesity context

    # Extract cluster_name (first word before space or colon)
    cluster_name_match = re.match(r"([A-Za-z0-9_-]+)[ :]", subject, re.IGNORECASE)
    if cluster_name_match:
        parsed_subject_info["cluster_name"] = cluster_name_match.group(1).strip()

    severity_match = re.search(r"cohesity alert\\s*-\\s*(critical|warning|info|error)", subject_lower, re.IGNORECASE)
    if severity_match:
        parsed_subject_info["alert_severity_from_subject"] = severity_match.group(1).upper()

    if "cluster health" in subject_lower:
        parsed_subject_info["alert_type_from_subject"] = "Cluster Health"
    elif "protection group" in subject_lower or "job" in subject_lower:
        parsed_subject_info["alert_type_from_subject"] = "Protection Group/Job"
        if "failed" in subject_lower:
            parsed_subject_info["job_status_from_subject"] = "Failed"
        elif "succeeded" in subject_lower or "successful" in subject_lower:
            parsed_subject_info["job_status_from_subject"] = "Successful"

        pg_name_match = re.search(r"protection group\\s*\\\'(.*?)\\\'", subject, re.IGNORECASE)
        if pg_name_match:
            parsed_subject_info["protection_group_name_from_subject"] = pg_name_match.group(1)
        else:
            job_name_match = re.search(r"job\\s*\\\'(.*?)\\\'", subject, re.IGNORECASE)
            if job_name_match:
                parsed_subject_info["job_name_from_subject"] = job_name_match.group(1)
    elif "replication" in subject_lower:
        parsed_subject_info["alert_type_from_subject"] = "Replication"
    elif "storage" in subject_lower or "capacity" in subject_lower:
        parsed_subject_info["alert_type_from_subject"] = "Storage/Capacity"

    logger.debug(f"Cohesity Standalone Subject Parse: {parsed_subject_info}")
    return parsed_subject_info


def cohesity_parse_body_standalone(body: str) -> Dict[str, Any]:
    """
    Standalone Cohesity email body parser.
    (Content will be copied from process_cohesity_email_alerts.py)
    """
    parsed_body_info = {"raw_body": body}
    lines = body.splitlines()
    key_value_pairs: Dict[str, str] = {}
    details_lines: List[str] = []

    known_keys = [
        "Alert",
        "Severity",
        "Cluster Name",
        "Cluster ID",
        "Timestamp",
        "Job Name",
        "Protection Group Name",
        "Object Name",
        "Status",
        "Error Message",
        "Details",
        "Recommended Action",
        "Source Name",
        "Target Name",
        "Start Time",
        "End Time",
        "Duration",
        # Add any other common Cohesity keys
        # "Alert URL",  # <-- Removed to prevent parsing alert_url
    ]
    specific_kv_pattern = re.compile(
        r"^\\s*(" + "|".join(re.escape(k) for k in known_keys) + r")\\s*:\\s*(.*)", re.IGNORECASE
    )

    multi_line_detail_key = None
    current_multi_line_value = []

    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            if multi_line_detail_key and current_multi_line_value:
                key_value_pairs[multi_line_detail_key] = "\\n".join(current_multi_line_value).strip()
                multi_line_detail_key = None
                current_multi_line_value = []
            continue

        match = specific_kv_pattern.match(line_stripped)
        if match:
            if multi_line_detail_key and current_multi_line_value:
                key_value_pairs[multi_line_detail_key] = "\\n".join(current_multi_line_value).strip()
                current_multi_line_value = []

            key_raw = match.group(1).strip()
            value = match.group(2).strip()
            key_normalized = key_raw.lower().replace(" ", "_").replace("-", "_")
            key_value_pairs[key_normalized] = value

            if key_raw.lower() in [
                "details",
                "error message",
                "recommended action",
                "summary",
                "description",
            ]:  # Added description
                multi_line_detail_key = key_normalized
                current_multi_line_value = [value]
            else:
                multi_line_detail_key = None
        elif multi_line_detail_key:
            current_multi_line_value.append(line_stripped)
        else:
            if line_stripped:  # Collect non-empty, non-matching lines as details
                details_lines.append(line_stripped)

    if multi_line_detail_key and current_multi_line_value:
        key_value_pairs[multi_line_detail_key] = "\\n".join(current_multi_line_value).strip()

    if details_lines:  # Add unmatched lines if any
        parsed_body_info["unmatched_cohesity_details"] = "\\n".join(details_lines)

    parsed_body_info.update(key_value_pairs)
    logger.debug(f"Cohesity Standalone Body Parse KVs: {key_value_pairs}")
    return parsed_body_info


class CohesityEmailHandler(BaseEmailHandler):
    alert_type = "cohesity"

    def parse_subject(self) -> Dict[str, Any]:
        # Use the standalone Cohesity subject parsing logic
        parsed = cohesity_parse_subject_standalone(self.subject)
        
        # Also extract IP and hostname from subject
        if self.subject:
            extracted = extract_ip_or_hostname(self.subject)
            if extracted["ip_address"] or extracted["hostname"]:
                parsed["_extracted_from_subject"] = extracted
                logger.info(f"[CohesityEmailHandler] Extracted from subject - IP: {extracted['ip_address']}, Hostname: {extracted['hostname']}")
        
        return parsed

    def parse_body(self) -> Dict[str, Any]:
        # Use the standalone Cohesity body parsing logic
        parsed = cohesity_parse_body_standalone(self.body)
        
        # Also extract IP and hostname from body
        if self.body:
            extracted = extract_ip_or_hostname(self.body)
            if extracted["ip_address"] or extracted["hostname"]:
                parsed["_extracted_from_body"] = extracted
                logger.info(f"[CohesityEmailHandler] Extracted from body - IP: {extracted['ip_address']}, Hostname: {extracted['hostname']}")
        
        return parsed

    def _apply_common_transformations(self, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        handler_prefix = "[CohesityEmailHandler]"

        # Special handling for UnregisteredCluster in subject
        if "unregisteredcluster" in (self.subject or "").lower():
            # 1. Parse cluster_name from subject
            cluster_name = None
            cluster_name_match = re.match(r"([A-Za-z0-9_-]+)[ :]", self.subject, re.IGNORECASE)
            if cluster_name_match:
                cluster_name = cluster_name_match.group(1).strip()
            else:
                cluster_name = "--"

            # 2. Parse DESCRIPTION and CAUSE from body
            body_text = self.body or ""
            description = None
            cause = None
            desc_match = re.search(r"DESCRIPTION\s*:\s*(.*)", body_text, re.IGNORECASE)
            if desc_match:
                description = desc_match.group(1).strip()
            cause_match = re.search(r"CAUSE\s*:\s*(.*)", body_text, re.IGNORECASE)
            if cause_match:
                cause = cause_match.group(1).strip()

            # 3. Set fields
            parsed_data["meta_datacenter"] = "--"
            parsed_data["meta_environment"] = "--"
            parsed_data["meta_hostname"] = "--"
            parsed_data["meta_ipaddress"] = "--"
            parsed_data["instance"] = cluster_name
            parsed_data["summary"] = description or ""
            parsed_data["description"] = cause or ""
            parsed_data["alertname"] = "COHESITY_UNREGISTERED_CLUSTER"
            parsed_data["meta_component"] = "cohesity"
            parsed_data["cluster_name"] = cluster_name
            logger.info(f"{handler_prefix} UnregisteredCluster special case applied: {parsed_data}")
            # Remove raw fields and unmatched details
            parsed_data.pop("raw_body", None)
            parsed_data.pop("raw_subject", None)
            parsed_data.pop("unmatched_cohesity_details", None)
            return parsed_data

        # Normalize keys
        if "alert_severity_from_subject" in parsed_data:
            parsed_data["severity"] = parsed_data.pop("alert_severity_from_subject")
            logger.info(f"{handler_prefix} Normalized 'alert_severity_from_subject' to 'severity'")
        if "alert_type_from_subject" in parsed_data:
            parsed_data["type"] = parsed_data.pop("alert_type_from_subject")
            logger.info(f"{handler_prefix} Normalized 'alert_type_from_subject' to 'type'")

        # Call base transformations
        parsed_data = super()._apply_common_transformations(parsed_data)

        # --- Additional Cohesity-specific parsing for ProtectionGroupFailed ---
        body_text = self.body or ""
        logger.info(f"{handler_prefix} Parsing body: {body_text}")

        # 1. Hostname extraction
        match = re.search(r"Cohesity\s+service\s+on\s+host\s+([a-zA-Z0-9_.-]+)", body_text, re.IGNORECASE)
        if match:
            hostname = match.group(1).strip()
            parsed_data["meta_hostname"] = hostname
            parsed_data["meta_ipaddress"] = hostname
            logger.info(f"{handler_prefix} Parsed hostname: {hostname}")

            # Set meta_datacenter if hostname starts with 'pl'
            if hostname.startswith("pl"):
                parsed_data["meta_datacenter"] = "Polaris"
                logger.info(f"{handler_prefix} Set meta_datacenter to Polaris")
            else:
                parsed_data["meta_datacenter"] = "--"
                logger.info(f"{handler_prefix} Set meta_datacenter to --")
            # Set meta_environment based on second segment
            segments = re.split(r"[._-]", hostname)
            if len(segments) > 1:
                second = segments[1]
                if re.match(r"^p\d+", second, re.IGNORECASE):
                    parsed_data["meta_environment"] = "Production"
                    logger.info(f"{handler_prefix} Set meta_environment to Production")
                elif re.match(r"^l\d+", second, re.IGNORECASE):
                    parsed_data["meta_environment"] = "Lab"
                    logger.info(f"{handler_prefix} Set meta_environment to Lab")
                else:
                    parsed_data["meta_environment"] = "--"
                    logger.info(f"{handler_prefix} Set meta_environment to --")
            # Set instance as meta_hostname:cluster_name if both are present
            if parsed_data.get("cluster_name"):
                parsed_data["instance"] = f'{hostname}:{parsed_data["cluster_name"]}'
                logger.info(f"{handler_prefix} Set instance to {parsed_data['instance']}")
            else:
                parsed_data["instance"] = hostname
                logger.info(f"{handler_prefix} Set instance to {hostname}")
        else:
            logger.info(f"{handler_prefix} No hostname found in body for enrichment.")
            parsed_data["meta_datacenter"] = "--"
            parsed_data["meta_environment"] = "--"
            parsed_data["instance"] = "--"
            parsed_data["meta_hostname"] = "--"
            parsed_data["meta_ipaddress"] = "--"

        # 2. Cluster name and cluster id extraction from body (override if found)
        match_name = re.search(r"Cluster name is ([A-Za-z0-9_-]+)", body_text, re.IGNORECASE)
        if match_name:
            cluster_name_body = match_name.group(1).strip()
            parsed_data["cluster_name"] = cluster_name_body
            logger.info(f"{handler_prefix} Parsed cluster_name from body: {cluster_name_body}")

        match_id = re.search(r"Cluster Id is ([0-9]+)", body_text, re.IGNORECASE)
        if match_id:
            cluster_id_body = match_id.group(1).strip()
            parsed_data["cluster_id"] = cluster_id_body
            logger.info(f"{handler_prefix} Parsed cluster_id from body: {cluster_id_body}")

        # 3. Set alertname for ProtectionGroupFailed
        parsed_data["alertname"] = "COHESITY_BACKUP_ALERT"

        # Remove raw fields and unmatched details
        parsed_data.pop("raw_body", None)
        parsed_data.pop("raw_subject", None)
        parsed_data.pop("unmatched_cohesity_details", None)

        logger.info(f"{handler_prefix} Final parsed_data: {parsed_data}")
        return parsed_data


class KeyValueEmailHandler(DefaultEmailHandler):  # Inherits Default's parsing
    """
    Handler for "kv" (key-value) alert type.
    Parses email body for key=value pairs using the same logic as DefaultEmailHandler.
    
    Note: This handler also supports the legacy "elastic" alert type for backward compatibility.
    """

    alert_type = "kv"
    log_prefix = "[KeyValueEmailHandler]"


class SimpleEmailHandler(BaseEmailHandler):
    """
    Handler that processes emails with subject and optional body.
    - Preserves labels from rule match
    - Adds entire body as description label
    - Adds subject as summary label
    """

    alert_type = "simple"
    log_prefix = "[SimpleEmailHandler]"

    def parse_subject(self) -> Dict[str, Any]:
        logger.info(f"{self.log_prefix} Parsing subject: {self.subject}")
        parsed = {"summary": self.subject}
        
        # Extract IP and hostname from subject
        if self.subject:
            extracted = extract_ip_or_hostname(self.subject)
            if extracted["ip_address"] or extracted["hostname"]:
                parsed["_extracted_from_subject"] = extracted
                logger.info(f"{self.log_prefix} Extracted from subject - IP: {extracted['ip_address']}, Hostname: {extracted['hostname']}")
        
        return parsed

    def parse_body(self) -> Dict[str, Any]:
        logger.info(f"{self.log_prefix} Parsing body (first 100 chars): {self.body[:100] if self.body else 'No body'}")
        parsed = {}
        
        if self.body:
            parsed["description"] = self.body
            
            # Extract IP and hostname from body
            extracted = extract_ip_or_hostname(self.body)
            if extracted["ip_address"] or extracted["hostname"]:
                parsed["_extracted_from_body"] = extracted
                logger.info(f"{self.log_prefix} Extracted from body - IP: {extracted['ip_address']}, Hostname: {extracted['hostname']}")
        
        return parsed

    def _apply_common_transformations(self, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        # Preserve any existing labels from rule match
        if "labels" in self.email_data:
            for key, value in self.email_data["labels"].items():
                if key not in parsed_data:
                    parsed_data[key] = value
                    logger.info(f"{self.log_prefix} Preserved label from rule match: {key}={value}")

        parsed_data = super()._apply_common_transformations(parsed_data)
        logger.info(f"{self.log_prefix} Final parsed_data after common transformations: {parsed_data}")
        return parsed_data

    def _build_summary_and_description(self, parsed_data: Dict[str, Any]) -> Tuple[str, str]:
        """
        Uses the subject as summary and body as description.
        """
        summary = parsed_data.get("summary", self.subject)
        description = parsed_data.get("description", self.body if self.body else summary)

        return summary, description


# Factory function to get the appropriate handler
def get_email_handler(alert_type_str: Optional[str], email_data: Dict[str, Any]) -> BaseEmailHandler:
    """
    Factory function to instantiate the correct email handler based on alert_type.
    """
    # Ensure alert_type_str is None if it's an empty string or similar falsy value,
    # but allow "0" or other stringified falsy values if they are legitimate types.
    effective_alert_type = (
        alert_type_str.lower().strip() if isinstance(alert_type_str, str) and alert_type_str.strip() else None
    )

    if effective_alert_type == "cohesity":
        logger.info(f"Using CohesityEmailHandler for alert type: {alert_type_str}")
        return CohesityEmailHandler(email_data)
    elif effective_alert_type in ["kv", "elastic"]:  # Support both "kv" and "elastic" for backward compatibility
        logger.info(f"Using KeyValueEmailHandler for alert type: {alert_type_str}")
        return KeyValueEmailHandler(email_data)
    elif effective_alert_type == "simple":
        logger.info(f"Using SimpleEmailHandler for alert type: {alert_type_str}")
        return SimpleEmailHandler(email_data)
    # Add more handlers here with elif conditions
    # elif effective_alert_type == "some_other_type":
    #     return SomeOtherTypeHandler(email_data)
    else:
        logger.info(f"Using DefaultEmailHandler for alert type: {alert_type_str} (or no type specified)")
        return DefaultEmailHandler(email_data)

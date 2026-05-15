import logging
import base64
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List
from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from helpers.schema.email_processing import AttachmentStorageType  # type: ignore
from helpers.email_helper import retrieve_stored_attachment  # type: ignore
from helpers.utils import normalize_boolean  # type: ignore
from helpers.email.handlers import get_email_handler  # type: ignore
from hooks.worklog_hook import WorkLogHook  # type: ignore
from hooks.rule_hook import RuleHook  # type: ignore
from hooks.alert_hook import AlertHook  # type: ignore
from hooks.mapping_hook import MappingHook  # type: ignore
import re

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# --- Mapping pipeline helpers for email alerts ---
def get_add_labels_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="add~"
        )
        add_labels = [(e["key"][4:], e["value"]) for e in elements if e.get("key", "").startswith("add~")]
        if add_labels:
            logger.info(f"[EMAIL_ALERT_MAPPING] Found {len(add_labels)} add labels for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_MAPPING] No add labels found for {alert_type}")
        return add_labels
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_MAPPING] Error getting add labels for {alert_type}: {e}")
        return []


def get_replace_map_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="replace~"
        )
        replace_map = {e["key"][8:].lower(): e["value"] for e in elements if e.get("key", "").startswith("replace~")}
        if replace_map:
            logger.info(f"[EMAIL_ALERT_MAPPING] Found {len(replace_map)} replace mappings for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_MAPPING] No replace mappings found for {alert_type}")
        return replace_map
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_MAPPING] Error getting replace map for {alert_type}: {e}")
        return {}


def get_merge_elements_for_alert(alert_type, mapping_hook, namespace):
    try:
        merge = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="merge~"
        )
        merge_remove = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="merge_remove~"
        )
        merge_update = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="update~"
        )
        merge_elements = merge + merge_remove + merge_update
        if merge_elements:
            logger.info(f"[EMAIL_ALERT_MAPPING] Found {len(merge_elements)} merge elements for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_MAPPING] No merge elements found for {alert_type}")
        return merge_elements
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_MAPPING] Error getting merge elements for {alert_type}: {e}")
        return []


def get_remove_patterns_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="remove~"
        )
        remove_patterns = [e["key"][7:] for e in elements if e.get("key", "").startswith("remove~")]
        if remove_patterns:
            logger.info(f"[EMAIL_ALERT_MAPPING] Found {len(remove_patterns)} remove patterns for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_MAPPING] No remove patterns found for {alert_type}")
        return remove_patterns
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_MAPPING] Error getting remove patterns for {alert_type}: {e}")
        return []


def get_substr_elements_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="substr~"
        )
        substr_elements = [e for e in elements if e.get("key", "").startswith("substr~")]
        if substr_elements:
            logger.info(f"[EMAIL_ALERT_SUBSTR] Found {len(substr_elements)} substr elements for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_SUBSTR] No substr elements found for {alert_type}")
        return substr_elements
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_SUBSTR] Error getting substr elements for {alert_type}: {e}")
        return []


def get_split_elements_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="split~"
        )
        split_elements = [e for e in elements if e.get("key", "").startswith("split~")]
        if split_elements:
            logger.info(f"[EMAIL_ALERT_SPLIT] Found {len(split_elements)} split elements for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_SPLIT] No split elements found for {alert_type}")
        return split_elements
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_SPLIT] Error getting split elements for {alert_type}: {e}")
        return []


def get_eval_elements_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="eval~"
        )
        eval_elements = [e for e in elements if e.get("key", "").startswith("eval~")]
        if eval_elements:
            logger.info(f"[EMAIL_ALERT_EVAL] Found {len(eval_elements)} eval elements for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_EVAL] No eval elements found for {alert_type}")
        return eval_elements
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_EVAL] Error getting eval elements for {alert_type}: {e}")
        return []


def process_substr_labels(parsed, substr_elements):
    label_data_lc = {k.lower(): k for k in parsed.keys()}
    for element in substr_elements:
        try:
            key = element.get("key", "")
            value_str = element.get("value", "")
            if not key.startswith("substr~") or not value_str:
                continue

            target_label = key[len("substr~"):]
            if not target_label:
                continue

            # Resolve templates in value
            try:
                tmpl_result, _ = _process_template_string(value_str, parsed)
                if tmpl_result is not None:
                    value_str = tmpl_result
            except Exception:
                pass

            # Parse value: source_label|start|length  (pipe delimiter)
            parts = value_str.split("|")
            if len(parts) < 2:
                logger.warning(f"[EMAIL_ALERT_SUBSTR] Invalid substr format (need source|start[|length]): {value_str}")
                continue

            source_label = parts[0].strip()
            try:
                start_pos = int(parts[1].strip())
            except ValueError:
                logger.warning(f"[EMAIL_ALERT_SUBSTR] Invalid start position: {parts[1]}")
                continue

            length = None
            if len(parts) >= 3 and parts[2].strip():
                try:
                    length = int(parts[2].strip())
                except ValueError:
                    logger.warning(f"[EMAIL_ALERT_SUBSTR] Invalid length: {parts[2]}")

            # Find source value (case-insensitive)
            source_lc = source_label.lower()
            if source_lc not in label_data_lc:
                logger.debug(f"[EMAIL_ALERT_SUBSTR] Source label '{source_label}' not found")
                continue

            orig_key = label_data_lc[source_lc]
            source_value = str(parsed[orig_key])

            # Extract substring
            if length is not None:
                result = source_value[start_pos:start_pos + length]
            else:
                result = source_value[start_pos:]

            parsed[target_label] = result
            label_data_lc[target_label.lower()] = target_label
            logger.info(
                f"[EMAIL_ALERT_SUBSTR] Extracted '{target_label}'='{result}' "
                f"from '{orig_key}' [{start_pos}:{start_pos + length if length else ''}]"
            )

        except Exception as e:
            logger.error(f"[EMAIL_ALERT_SUBSTR] Error processing element: {e}")
    return parsed


def process_split_labels(parsed, split_elements):
    label_data_lc = {k.lower(): k for k in parsed.keys()}
    for element in split_elements:
        try:
            key = element.get("key", "")
            value_str = element.get("value", "")
            if not key.startswith("split~") or not value_str:
                continue

            target_label = key[len("split~"):]
            if not target_label:
                continue

            # Resolve templates in value
            try:
                tmpl_result, _ = _process_template_string(value_str, parsed)
                if tmpl_result is not None:
                    value_str = tmpl_result
            except Exception:
                pass

            # Parse value: source_label|delimiter|index  (pipe delimiter, maxsplit=2)
            parts = value_str.split("|", 2)
            if len(parts) < 3:
                logger.warning(f"[EMAIL_ALERT_SPLIT] Invalid split format (need source|delimiter|index): {value_str}")
                continue

            source_label = parts[0].strip()
            delimiter = parts[1]  # Don't strip - delimiter may be whitespace
            index_str = parts[2].strip()

            # Find source value (case-insensitive)
            source_lc = source_label.lower()
            if source_lc not in label_data_lc:
                logger.debug(f"[EMAIL_ALERT_SPLIT] Source label '{source_label}' not found")
                continue

            orig_key = label_data_lc[source_lc]
            source_value = str(parsed[orig_key])

            # Split the value
            split_parts = source_value.split(delimiter)

            if index_str == "*":
                # Explode all parts as target_0, target_1, ...
                for i, part in enumerate(split_parts):
                    label_name = f"{target_label}_{i}"
                    parsed[label_name] = part.strip()
                    label_data_lc[label_name.lower()] = label_name
                logger.info(
                    f"[EMAIL_ALERT_SPLIT] Split '{orig_key}' by '{delimiter}' -> "
                    f"{len(split_parts)} parts as {target_label}_0..{target_label}_{len(split_parts)-1}"
                )
            else:
                try:
                    idx = int(index_str)
                except ValueError:
                    logger.warning(f"[EMAIL_ALERT_SPLIT] Invalid index: {index_str}")
                    continue

                if abs(idx) < len(split_parts):
                    result = split_parts[idx].strip()
                    parsed[target_label] = result
                    label_data_lc[target_label.lower()] = target_label
                    logger.info(
                        f"[EMAIL_ALERT_SPLIT] Extracted '{target_label}'='{result}' "
                        f"from '{orig_key}' split by '{delimiter}' index [{idx}]"
                    )
                else:
                    logger.warning(
                        f"[EMAIL_ALERT_SPLIT] Index {idx} out of range for "
                        f"split of '{orig_key}' ({len(split_parts)} parts)"
                    )

        except Exception as e:
            logger.error(f"[EMAIL_ALERT_SPLIT] Error processing element: {e}")
    return parsed


def process_eval_labels(parsed, eval_elements):
    label_data_lc = {k.lower(): k for k in parsed.keys()}

    # Safe evaluation functions (no access to builtins, filesystem, imports)
    safe_funcs = {
        "int": lambda x: int(float(x)) if isinstance(x, str) else int(x),
        "str": str,
        "float": float,
        "abs": abs,
        "lower": lambda x: str(x).lower(),
        "upper": lambda x: str(x).upper(),
        "len": lambda x: len(str(x)),
        "strip": lambda x: str(x).strip(),
        "lstrip": lambda x: str(x).lstrip(),
        "rstrip": lambda x: str(x).rstrip(),
    }

    for element in eval_elements:
        try:
            key = element.get("key", "")
            value_str = element.get("value", "")
            if not key.startswith("eval~") or not value_str:
                continue

            target_label = key[len("eval~"):]
            if not target_label:
                continue

            # Resolve templates in the expression
            expr = value_str
            try:
                tmpl_result, _ = _process_template_string(expr, parsed)
                if tmpl_result is not None:
                    expr = tmpl_result
            except Exception:
                pass

            # SECURITY: Reject dunder patterns before eval
            if "__" in expr:
                logger.warning(
                    f"[EMAIL_ALERT_EVAL] Expression rejected (contains dunder): {expr}"
                )
                continue

            # Evaluate in restricted sandbox
            try:
                result = eval(expr, {"__builtins__": {}}, safe_funcs)  # noqa: S307
                parsed[target_label] = str(result)
                logger.info(
                    f"[EMAIL_ALERT_EVAL] Evaluated '{target_label}'='{result}' "
                    f"from expression '{value_str}'"
                )
            except Exception as e:
                logger.warning(
                    f"[EMAIL_ALERT_EVAL] Failed to evaluate expression '{expr}' "
                    f"(from '{value_str}'): {e}"
                )

        except Exception as e:
            logger.error(f"[EMAIL_ALERT_EVAL] Error processing element: {e}")
    return parsed


def get_extract_elements_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="extract~"
        )
        extract_elements = [e for e in elements if e.get("key", "").startswith("extract~")]
        if extract_elements:
            logger.info(f"[EMAIL_ALERT_EXTRACT] Found {len(extract_elements)} extract elements for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_EXTRACT] No extract elements found for {alert_type}")
        return extract_elements
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_EXTRACT] Error getting extract elements for {alert_type}: {e}")
        return []


def get_regreplace_elements_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="regreplace~"
        )
        regreplace_elements = [e for e in elements if e.get("key", "").startswith("regreplace~")]
        if regreplace_elements:
            logger.info(f"[EMAIL_ALERT_REGREPLACE] Found {len(regreplace_elements)} regreplace elements for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_REGREPLACE] No regreplace elements found for {alert_type}")
        return regreplace_elements
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_REGREPLACE] Error getting regreplace elements for {alert_type}: {e}")
        return []


def get_cut_elements_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="cut~"
        )
        cut_elements = [e for e in elements if e.get("key", "").startswith("cut~")]
        if cut_elements:
            logger.info(f"[EMAIL_ALERT_CUT] Found {len(cut_elements)} cut elements for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_CUT] No cut elements found for {alert_type}")
        return cut_elements
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_CUT] Error getting cut elements for {alert_type}: {e}")
        return []


def get_hash_elements_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="hash~"
        )
        hash_elements = [e for e in elements if e.get("key", "").startswith("hash~")]
        if hash_elements:
            logger.info(f"[EMAIL_ALERT_HASH] Found {len(hash_elements)} hash elements for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_HASH] No hash elements found for {alert_type}")
        return hash_elements
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_HASH] Error getting hash elements for {alert_type}: {e}")
        return []


def get_keep_patterns_for_alert(alert_type, mapping_hook, namespace):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="keep~"
        )
        keep_patterns = [e["key"][5:] for e in elements if e.get("key", "").startswith("keep~")]
        if keep_patterns:
            logger.info(f"[EMAIL_ALERT_KEEP] Found {len(keep_patterns)} keep patterns for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_KEEP] No keep patterns found for {alert_type}")
        return keep_patterns
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_KEEP] Error getting keep patterns for {alert_type}: {e}")
        return []


def process_extract_labels(parsed, extract_elements):
    label_data_lc = {k.lower(): k for k in parsed.keys()}
    for element in extract_elements:
        try:
            key = element.get("key", "")
            value_str = element.get("value", "")
            if not key.startswith("extract~") or not value_str:
                continue

            target_label = key[len("extract~"):]
            if not target_label:
                continue

            # Parse value: source_label:regex_pattern (first colon is delimiter)
            colon_idx = value_str.find(":")
            if colon_idx < 0:
                logger.warning(f"[EMAIL_ALERT_EXTRACT] Invalid extract value format (no colon): {value_str}")
                continue

            source_label = value_str[:colon_idx].strip()
            regex_pattern = value_str[colon_idx + 1:].strip()

            if not source_label or not regex_pattern:
                continue

            # Find source value (case-insensitive)
            source_lc = source_label.lower()
            if source_lc not in label_data_lc:
                logger.debug(f"[EMAIL_ALERT_EXTRACT] Source label '{source_label}' not found")
                continue

            orig_key = label_data_lc[source_lc]
            source_value = str(parsed[orig_key])

            # Apply regex
            try:
                match = re.search(regex_pattern, source_value, re.IGNORECASE)
                if match and match.group(1):
                    parsed[target_label] = match.group(1)
                    label_data_lc[target_label.lower()] = target_label
                    logger.info(
                        f"[EMAIL_ALERT_EXTRACT] Extracted '{target_label}'='{match.group(1)}' "
                        f"from '{orig_key}' using pattern '{regex_pattern}'"
                    )
                else:
                    logger.debug(
                        f"[EMAIL_ALERT_EXTRACT] No match for pattern '{regex_pattern}' "
                        f"on value '{source_value[:100]}'"
                    )
            except re.error as e:
                logger.warning(f"[EMAIL_ALERT_EXTRACT] Invalid regex pattern '{regex_pattern}': {e}")

        except Exception as e:
            logger.error(f"[EMAIL_ALERT_EXTRACT] Error processing element: {e}")
    return parsed


def process_regreplace_labels(parsed, regreplace_elements):
    label_data_lc = {k.lower(): k for k in parsed.keys()}
    for element in regreplace_elements:
        try:
            key = element.get("key", "")
            value_str = element.get("value", "")
            if not key.startswith("regreplace~") or not value_str:
                continue

            target_label = key[len("regreplace~"):]
            if not target_label:
                continue

            # Parse value: regex_pattern:::replacement_string
            delimiter = ":::"
            delim_idx = value_str.find(delimiter)
            if delim_idx < 0:
                logger.warning(f"[EMAIL_ALERT_REGREPLACE] Invalid format (no ::: delimiter): {value_str}")
                continue

            regex_pattern = value_str[:delim_idx]
            replacement = value_str[delim_idx + len(delimiter):]

            if not regex_pattern:
                continue

            # Find target value (case-insensitive)
            target_lc = target_label.lower()
            if target_lc not in label_data_lc:
                logger.debug(f"[EMAIL_ALERT_REGREPLACE] Target label '{target_label}' not found")
                continue

            orig_key = label_data_lc[target_lc]
            current_value = str(parsed[orig_key])

            # Resolve {{template}} variables in replacement string
            try:
                template_result, _ = _process_template_string(replacement, parsed)
                if template_result is not None:
                    replacement = template_result
            except Exception:
                pass  # Use original replacement if template fails

            # Apply regex substitution
            try:
                new_value = re.sub(regex_pattern, replacement, current_value, flags=re.IGNORECASE)
                if new_value != current_value:
                    parsed[orig_key] = new_value
                    logger.info(
                        f"[EMAIL_ALERT_REGREPLACE] Replaced in '{orig_key}': "
                        f"'{current_value[:50]}' -> '{new_value[:50]}'"
                    )
            except re.error as e:
                logger.warning(f"[EMAIL_ALERT_REGREPLACE] Invalid regex pattern '{regex_pattern}': {e}")

        except Exception as e:
            logger.error(f"[EMAIL_ALERT_REGREPLACE] Error processing element: {e}")
    return parsed


def process_cut_labels(parsed, cut_elements):
    label_data_lc = {k.lower(): k for k in parsed.keys()}
    for element in cut_elements:
        try:
            key = element.get("key", "")
            value_str = element.get("value", "")
            if not key.startswith("cut~") or not value_str:
                continue

            target_label = key[len("cut~"):]
            if not target_label:
                continue

            # Parse value: source_label:regex_pattern[:capture_label]
            parts = value_str.split(":", 2)
            if len(parts) < 2:
                logger.warning(f"[EMAIL_ALERT_CUT] Invalid cut format (need source:pattern): {value_str}")
                continue

            source_label = parts[0].strip()
            regex_pattern = parts[1].strip()

            if not source_label or not regex_pattern:
                continue

            # Optional capture label (3rd part)
            capture_label = None
            if len(parts) >= 3:
                capture_label = parts[2].strip() or None

            # Find source value (case-insensitive)
            source_lc = source_label.lower()
            if source_lc not in label_data_lc:
                logger.debug(f"[EMAIL_ALERT_CUT] Source label '{source_label}' not found")
                continue

            orig_key = label_data_lc[source_lc]
            source_value = str(parsed[orig_key])

            # Apply regex search
            try:
                match = re.search(regex_pattern, source_value, re.IGNORECASE)

                if match:
                    matched_text = match.group(0)

                    # Remove matched text from source, store in target
                    parsed[target_label] = (
                        source_value[:match.start()] + source_value[match.end():]
                    )
                    label_data_lc[target_label.lower()] = target_label

                    # Optionally capture the removed text
                    if capture_label:
                        # Use group(1) if capture group exists, otherwise group(0)
                        try:
                            captured = match.group(1)
                        except IndexError:
                            captured = matched_text
                        parsed[capture_label] = captured
                        label_data_lc[capture_label.lower()] = capture_label

                    logger.info(
                        f"[EMAIL_ALERT_CUT] Removed '{matched_text}' from '{orig_key}' "
                        f"-> '{target_label}'"
                    )
                else:
                    logger.debug(
                        f"[EMAIL_ALERT_CUT] No match for pattern '{regex_pattern}' "
                        f"on source '{source_label}'"
                    )
            except re.error as e:
                logger.warning(f"[EMAIL_ALERT_CUT] Invalid regex pattern '{regex_pattern}': {e}")

        except Exception as e:
            logger.error(f"[EMAIL_ALERT_CUT] Error processing element: {e}")
    return parsed


def process_hash_labels(parsed, hash_elements):
    """
    Process hash~ mapping elements.
    Canonical format is byte-identical across all 4 pipelines:
    - Fields sorted case-insensitively
    - Each part: field_lc=value (missing fields get field_lc=)
    - Pipe-delimited
    - SHA-256 first 16 hex chars
    """
    label_data_lc = {k.lower(): k for k in parsed.keys()}
    for element in hash_elements:
        try:
            key = element.get("key", "")
            value_str = element.get("value", "")
            if not key.startswith("hash~") or not value_str:
                continue

            target_label = key[len("hash~"):]
            if not target_label:
                continue

            # Parse field list
            field_names = [f.strip() for f in value_str.split(",") if f.strip()]
            if not field_names:
                continue

            # Build canonical string from sorted fields (case-insensitive sort)
            parts = []
            for field_name in sorted(field_names, key=str.lower):
                field_lc = field_name.lower()
                if field_lc in label_data_lc:
                    orig_key = label_data_lc[field_lc]
                    parts.append(f"{field_lc}={parsed[orig_key]}")
                else:
                    parts.append(f"{field_lc}=")

            canonical = "|".join(parts)
            hash_value = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]

            parsed[target_label] = hash_value
            label_data_lc[target_label.lower()] = target_label
            logger.info(
                f"[EMAIL_ALERT_HASH] Created hash label "
                f"'{target_label}'='{hash_value}' from fields: {field_names}"
            )

        except Exception as e:
            logger.error(f"[EMAIL_ALERT_HASH] Error processing element: {e}")
    return parsed


def process_keep_labels(parsed, keep_patterns):
    """
    Process keep~ patterns - retain only labels matching whitelist regex patterns.
    If no keep_patterns, return parsed unchanged (empty list means keep all).
    """
    if not keep_patterns:
        return parsed

    keys_to_keep = set()
    for key in parsed.keys():
        for pattern in keep_patterns:
            try:
                if re.search(pattern, key, re.IGNORECASE):
                    keys_to_keep.add(key)
                    break
            except re.error as e:
                logger.warning(f"[EMAIL_ALERT_KEEP] Invalid regex pattern '{pattern}': {e}")

    # Remove all keys NOT in the keep set
    keys_to_remove = set(parsed.keys()) - keys_to_keep
    for key in keys_to_remove:
        if key in parsed:
            del parsed[key]

    logger.debug(f"[EMAIL_ALERT_KEEP] Kept {len(keys_to_keep)} keys, removed {len(keys_to_remove)} keys")
    if keys_to_remove:
        logger.debug(f"[EMAIL_ALERT_KEEP] Removed keys: {list(keys_to_remove)}")

    return parsed


def process_add_labels(parsed, add_labels):
    for key, value in add_labels:
        # Only add the label if it doesn't already exist
        if key not in parsed:
            parsed[key] = value
            logger.info(f"[EMAIL_ALERT_MAPPING] Added label {key}={value}")
        else:
            logger.debug(f"[EMAIL_ALERT_MAPPING] Skipped adding label {key} - already exists with value {parsed[key]}")
    return parsed


def process_replace_labels(parsed, replace_map):
    for key, value in list(parsed.items()):
        lookup_key = key.lower()
        if lookup_key in replace_map:
            new_key = replace_map[lookup_key]
            parsed[new_key] = value
            del parsed[key]
    return parsed


def process_merge_labels(parsed, merge_elements):
    parsed_lc = {k.lower(): k for k in parsed.keys()}
    for element in merge_elements:
        key = element.get("key", "")
        value_str = element.get("value", "")
        if not key or not value_str:
            continue
        command_type = None
        target_label = None
        if key.startswith("merge_remove~"):
            command_type = "merge_remove~"
            target_label = key[len("merge_remove~") :].strip()
        elif key.startswith("merge~"):
            command_type = "merge~"
            target_label = key[len("merge~") :].strip()
        elif key.startswith("update~"):
            command_type = "update~"
            target_label = key[len("update~") :].strip()
        else:
            continue
        target_label_lc = target_label.lower()

        # Check if value_str contains template placeholders
        template_pattern = re.compile(r"\{\{([^}]+)\}\}")
        has_template = template_pattern.search(value_str)

        merged_value_part = None
        found_source_keys = []

        if has_template:
            # Process as template string
            merged_value_part = value_str
            # Find all placeholders in the template
            placeholders = template_pattern.findall(value_str)

            for placeholder in placeholders:
                placeholder_lc = placeholder.strip().lower()
                if placeholder_lc in parsed_lc:
                    orig_key = parsed_lc[placeholder_lc]
                    replacement_value = str(parsed[orig_key])
                    # Replace the placeholder with its value (case-insensitive)
                    merged_value_part = re.sub(
                        r"\{\{\s*" + re.escape(placeholder) + r"\s*\}\}",
                        replacement_value,
                        merged_value_part,
                        flags=re.IGNORECASE,
                    )
                    found_source_keys.append(orig_key)
        else:
            # Process as comma-separated list (existing logic)
            source_labels = [label.strip() for label in value_str.split(",") if label.strip()]
            found_source_values = []
            for source_label in source_labels:
                source_label_lc = source_label.lower()
                if source_label_lc in parsed_lc:
                    orig_key = parsed_lc[source_label_lc]
                    found_source_values.append(str(parsed[orig_key]))
                    found_source_keys.append(orig_key)
            if not found_source_values:
                continue
            merged_value_part = "~".join(found_source_values)

        if merged_value_part:
            logger.info(f"[EMAIL_ALERT_MAPPING] Parsed_LC {parsed_lc} with {target_label_lc}")
            if target_label_lc in parsed_lc:
                logger.info(f"[EMAIL_ALERT_MAPPING] Merging {target_label} with {merged_value_part}")
                orig_target_key = parsed_lc[target_label_lc]
                existing_value = str(parsed[orig_target_key])
                if command_type == "update~":
                    parsed[orig_target_key] = merged_value_part
                else:
                    parsed[orig_target_key] = f"{existing_value}~{merged_value_part}"
            else:
                parsed[target_label] = merged_value_part
            if command_type == "merge_remove~":
                for source_key_to_remove in found_source_keys:
                    if source_key_to_remove in parsed:
                        del parsed[source_key_to_remove]
    return parsed


def process_remove_labels(parsed, remove_patterns):
    keys_to_remove = set()
    for key in parsed.keys():
        for pattern in remove_patterns:
            try:
                if re.search(pattern, key, re.IGNORECASE):
                    keys_to_remove.add(key)
                    break
            except re.error as e:
                logger.warning(f"[EMAIL_ALERT_MAPPING] Invalid regex pattern '{pattern}': {e}")
    for key in keys_to_remove:
        if key in parsed:
            del parsed[key]
    return parsed


# --- Lookup mapping helpers for email alerts ---
def _process_template_string(template_str, data_dict):
    """
    Process a template string by replacing {{variable}} placeholders with values from data dictionary.

    Args:
        template_str: String that may contain {{variable}} placeholders
        data_dict: Dictionary containing variable values for substitution

    Returns:
        Tuple of (processed_string, list_of_found_keys).
        processed_string is None if no templates found.
    """
    try:
        template_pattern = re.compile(r"\{\{([^}]+)\}\}")
        if not template_pattern.search(template_str):
            return None, []

        result = template_str
        found_keys = []
        placeholders = template_pattern.findall(template_str)
        data_dict_lc = {k.lower(): k for k in data_dict.keys()}

        for placeholder in placeholders:
            placeholder_key = placeholder.strip()
            placeholder_lc = placeholder_key.lower()

            if placeholder_lc in data_dict_lc:
                orig_key = data_dict_lc[placeholder_lc]
                value = str(data_dict[orig_key])
                result = re.sub(
                    r"\{\{\s*" + re.escape(placeholder) + r"\s*\}\}",
                    value, result, flags=re.IGNORECASE
                )
                found_keys.append(orig_key)

        return result, found_keys
    except Exception:
        return None, []


def _parse_lookup_value(raw_value):
    """
    Parse a lookup~ element's value string.

    Format: <mapping_name>[@<namespace>]:<mapping_key>[:flat]

    The optional @namespace qualifier allows cross-namespace lookups.
    If omitted, the caller's default namespace is used.

    Returns:
        Tuple of (mapping_name, mapping_key, flatten, namespace) or None on parse error.
        namespace is None when not specified.
    """
    flatten = False
    if raw_value.endswith(":flat"):
        flatten = True
        raw_value = raw_value[:-5]

    parts = raw_value.split(":", 1)
    if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
        return None

    mapping_name_part = parts[0].strip()
    mapping_key = parts[1].strip()

    # Extract optional @namespace from mapping_name_part
    namespace = None
    if "@" in mapping_name_part:
        name_ns = mapping_name_part.split("@", 1)
        mapping_name = name_ns[0].strip()
        namespace = name_ns[1].strip()
        if not mapping_name or not namespace:
            return None
    else:
        mapping_name = mapping_name_part

    return (mapping_name, mapping_key, flatten, namespace)


def _flatten_json(prefix, obj, max_depth=3, max_keys=20, _current_depth=0):
    """
    Flatten a JSON object into dot-path key-value pairs.

    Args:
        prefix: Base prefix for flattened keys
        obj: JSON-parsed object to flatten
        max_depth: Maximum nesting depth (hard cap)
        max_keys: Maximum total flattened keys (hard cap)
        _current_depth: Internal recursion tracker

    Returns:
        List of (key, value) tuples
    """
    results = []
    if not isinstance(obj, dict):
        results.append((prefix, str(obj)))
        return results

    for key, value in obj.items():
        if len(results) >= max_keys:
            dropped = len(obj) - len([r for r in results if r[0].startswith(prefix)])
            if dropped > 0:
                logger.warning(
                    f"[EMAIL_ALERT_LOOKUP_FLATTEN] Max keys cap ({max_keys}) reached for '{prefix}', dropped {dropped} remaining keys"
                )
            break

        flat_key = f"{prefix}.{key}"

        if isinstance(value, dict):
            if _current_depth + 1 >= max_depth:
                logger.warning(
                    f"[EMAIL_ALERT_LOOKUP_FLATTEN] Depth cap reached at '{flat_key}', stringifying remaining value"
                )
                results.append((flat_key, json.dumps(value, ensure_ascii=False)))
            else:
                sub_results = _flatten_json(flat_key, value, max_depth, max_keys - len(results), _current_depth + 1)
                results.extend(sub_results)
        elif isinstance(value, list):
            results.append((flat_key, json.dumps(value, ensure_ascii=False)))
        else:
            results.append((flat_key, str(value)))

    return results


def get_lookup_elements_for_alert(alert_type, mapping_hook, namespace):
    """
    Get lookup~ mapping elements for this alert type.

    Args:
        alert_type: The alert type for mapping lookup
        mapping_hook: The mapping hook instance
        namespace: The namespace for mappings

    Returns:
        list: List of lookup mapping elements
    """
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=alert_type, mapping_namespace_name=namespace, mapping_key="lookup~"
        )
        lookup_elements = [e for e in elements if e.get("key", "").startswith("lookup~")]
        if lookup_elements:
            logger.info(f"[EMAIL_ALERT_LOOKUP] Found {len(lookup_elements)} lookup elements for {alert_type}")
        else:
            logger.info(f"[EMAIL_ALERT_LOOKUP] No lookup elements found for {alert_type}")
        return lookup_elements
    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_LOOKUP] Error getting lookup elements for {alert_type}: {e}")
        return []


def process_lookup_labels(alert_data, alert_type, mapping_hook, namespace):
    """
    Process lookup~ mapping elements to enrich alert labels with values from other mappings.

    Args:
        alert_data: Current alert label dictionary
        alert_type: Alert type for mapping element lookup
        mapping_hook: MappingHook instance
        namespace: Mapping namespace (e.g., EXT_EMAIL_ALERTS)

    Returns:
        dict: Modified alert_data with lookup results added
    """
    lookup_value_cache = {}  # Per-alert cache: (mapping_name, resolved_key) -> value

    try:
        elements = get_lookup_elements_for_alert(alert_type, mapping_hook, namespace)

        if not elements:
            return alert_data

        for element in elements:
            try:
                element_key = element.get("key", "")
                element_value = element.get("value", "")

                if not element_key.startswith("lookup~"):
                    continue

                # Extract target label name (strip "lookup~" prefix)
                target_label = element_key[7:]  # Remove "lookup~"
                if not target_label:
                    logger.warning("[EMAIL_ALERT_LOOKUP] Empty target label after removing lookup~ prefix")
                    continue

                # Parse value using canonical algorithm
                parsed = _parse_lookup_value(element_value)
                if parsed is None:
                    logger.warning(f"[EMAIL_ALERT_LOOKUP] Malformed lookup value: '{element_value}' for target '{target_label}'")
                    continue

                mapping_name, mapping_key, flatten, override_ns = parsed

                # Resolve {{variable}} templates in mapping_key
                resolved_key = mapping_key
                try:
                    template_result, _ = _process_template_string(mapping_key, alert_data)
                    if template_result is not None:
                        resolved_key = template_result
                        logger.debug(f"[EMAIL_ALERT_LOOKUP] Template resolved: '{mapping_key}' -> '{resolved_key}'")
                except Exception as e:
                    logger.warning(f"[EMAIL_ALERT_LOOKUP] Template processing failed for '{mapping_key}': {e}. Using original key.")

                # Determine effective namespace (override or default)
                effective_ns = override_ns or namespace

                # Per-alert cache check (includes namespace to avoid cross-namespace collisions)
                cache_key = (effective_ns, mapping_name, resolved_key.lower())
                if cache_key in lookup_value_cache:
                    looked_up_value = lookup_value_cache[cache_key]
                    logger.debug(f"[EMAIL_ALERT_LOOKUP] Cache hit for {mapping_name}:{resolved_key} in {effective_ns}")
                else:
                    # Fetch from mapping API via MappingHook
                    try:
                        lookup_elements = mapping_hook.list_mapping_elements(
                            mapping_name=mapping_name, mapping_namespace_name=effective_ns, mapping_key=resolved_key
                        )
                        if lookup_elements:
                            looked_up_value = lookup_elements[0].get("value", "")
                        else:
                            looked_up_value = None
                            logger.warning(f"[EMAIL_ALERT_LOOKUP] No value found for {mapping_name}:{resolved_key} in namespace {effective_ns}")
                    except Exception as e:
                        looked_up_value = None
                        logger.warning(f"[EMAIL_ALERT_LOOKUP] API error fetching {mapping_name}:{resolved_key}: {e}")

                    lookup_value_cache[cache_key] = looked_up_value

                if looked_up_value is None:
                    continue

                # Apply label(s) - LOOKUP OVERWRITES existing labels
                if flatten:
                    try:
                        json_obj = json.loads(looked_up_value)
                        if isinstance(json_obj, dict):
                            flat_pairs = _flatten_json(target_label, json_obj)
                            for flat_key, flat_value in flat_pairs:
                                alert_data[flat_key] = flat_value
                            logger.debug(f"[EMAIL_ALERT_LOOKUP] Flattened {len(flat_pairs)} labels from {mapping_name}:{resolved_key}")
                        else:
                            alert_data[target_label] = str(looked_up_value)
                            logger.debug(f"[EMAIL_ALERT_LOOKUP] Value not a dict, added as single label: {target_label}")
                    except (json.JSONDecodeError, TypeError):
                        alert_data[target_label] = str(looked_up_value)
                        logger.debug(f"[EMAIL_ALERT_LOOKUP] Value not valid JSON, added as single label: {target_label}")
                else:
                    alert_data[target_label] = str(looked_up_value)
                    logger.debug(f"[EMAIL_ALERT_LOOKUP] Added label: {target_label}={looked_up_value}")

            except Exception as e:
                logger.error(f"[EMAIL_ALERT_LOOKUP] Error processing lookup element: {e}")
                continue

    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_LOOKUP] Error fetching lookup elements for {alert_type}: {e}")

    return alert_data


# --- Conditional mapping functions for email alerts ---
def parse_conditional_mapping(key: str):
    """
    Parse conditional mapping key format: "if~condition~action" or "NN~if~condition~action"

    Args:
        key: The conditional mapping key

    Returns:
        tuple: (condition, action) or (None, None) if invalid format

    Examples:
        "if~severity>=7~add:priority:high" -> ("severity>=7", "add:priority:high")
        "10~if~severity>=7~add:priority:high" -> ("severity>=7", "add:priority:high")
        "if~subject contains ERROR~merge:alert_summary:subject,from" -> ("subject contains ERROR", "merge:alert_summary:subject,from")
    """
    # Strip order prefix if present (e.g., "01~if~cond~action" -> "if~cond~action")
    stripped_key = re.sub(r'^\d{1,3}~', '', key, count=1)

    if not stripped_key.lower().startswith("if~"):
        return None, None

    parts = stripped_key.split("~", 2)  # Split into max 3 parts: ["if", "condition", "action"]

    if len(parts) != 3:
        logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Invalid conditional mapping format: {key}")
        return None, None

    condition = parts[1]
    action = parts[2]

    if not condition or not action:
        logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Empty condition or action in: {key}")
        return None, None

    return condition, action


def sort_conditional_mappings(conditional_elements: list) -> list:
    """
    Sort conditional mapping elements by order number.
    Ordered conditionals (NN~if~) execute first in numeric order,
    followed by unordered conditionals (if~) in original order.
    """
    ordered = []
    unordered = []

    for element in conditional_elements:
        key = element.get("key", "")
        match = re.match(r'^(\d{1,3})~if~', key, re.IGNORECASE)
        if match:
            order_num = int(match.group(1))
            ordered.append((order_num, element))
        else:
            unordered.append(element)

    ordered.sort(key=lambda x: x[0])
    result = [elem for _, elem in ordered] + unordered

    if ordered:
        logger.debug(
            f"[EMAIL_ALERT_CONDITIONAL_ORDER] Sorted {len(ordered)} ordered and {len(unordered)} unordered conditionals"
        )

    return result


def evaluate_condition(condition: str, alert_data: dict) -> bool:
    """
    Evaluate a condition with AND/OR logic against alert data.

    Args:
        condition: Condition string with optional AND/OR logic
        alert_data: Dictionary of alert labels and values

    Returns:
        bool: True if condition is met, False otherwise

    Supported formats:
        - Single: "severity>=7"
        - OR logic: "severity>=7,severity>=8"
        - AND logic: "severity>=7&&subject contains ERROR"
        - Combined: "severity>=7&&subject contains ERROR,severity>=8&&from contains admin"

    Supported operators:
        ==, !=, >=, <=, >, <, contains, !contains, regex, !regex

    Logic operators:
        , (comma) = OR between groups
        && (double ampersand) = AND within groups
    """
    try:
        # Handle OR conditions (comma-separated groups)
        if "," in condition:
            or_groups = [group.strip() for group in condition.split(",")]
            return any(evaluate_condition_group(group, alert_data) for group in or_groups)
        else:
            return evaluate_condition_group(condition, alert_data)
    except Exception as e:
        logger.error(f"[EMAIL_ALERT_CONDITIONAL] Error evaluating condition '{condition}': {e}")
        return False


def evaluate_condition_group(condition_group: str, alert_data: dict) -> bool:
    """
    Evaluate a group of conditions with AND logic.

    Args:
        condition_group: Single condition or AND-separated conditions like "severity>=7&&subject contains ERROR"
        alert_data: Dictionary of alert labels and values

    Returns:
        bool: True if all conditions in the group are met, False otherwise
    """
    # Handle AND conditions (double ampersand-separated)
    if "&&" in condition_group:
        and_conditions = [cond.strip() for cond in condition_group.split("&&")]
        return all(evaluate_single_condition(cond, alert_data) for cond in and_conditions)
    else:
        return evaluate_single_condition(condition_group, alert_data)


def evaluate_single_condition(condition: str, alert_data: dict) -> bool:
    """
    Evaluate a single condition (no AND/OR logic).

    Args:
        condition: Single condition string like "severity>=7" or "subject contains ERROR"
        alert_data: Dictionary of alert labels and values

    Returns:
        bool: True if condition is met, False otherwise
    """
    # Define operators in order of precedence (longer operators first)
    operators = ["!contains", "contains", "!regex", "regex", ">=", "<=", "!=", "==", ">", "<"]

    for operator in operators:
        if operator in condition:
            parts = condition.split(operator, 1)
            if len(parts) == 2:
                label_name = parts[0].strip()
                expected_value = parts[1].strip()

                # Get actual value from alert data (case-insensitive)
                alert_data_lc = {k.lower(): k for k in alert_data.keys()}
                label_name_lc = label_name.lower()
                if label_name_lc in alert_data_lc:
                    orig_key = alert_data_lc[label_name_lc]
                    actual_value = alert_data[orig_key]
                else:
                    actual_value = ""

                # Evaluate based on operator type
                if operator in [">=", "<=", ">", "<"]:
                    # Numeric comparison
                    try:
                        actual_num = float(actual_value) if actual_value != "" else 0
                        expected_num = float(expected_value)

                        if operator == ">=":
                            return actual_num >= expected_num
                        elif operator == "<=":
                            return actual_num <= expected_num
                        elif operator == ">":
                            return actual_num > expected_num
                        elif operator == "<":
                            return actual_num < expected_num
                    except (ValueError, TypeError):
                        logger.warning(
                            f"[EMAIL_ALERT_CONDITIONAL] Non-numeric values in comparison: {actual_value} {operator} {expected_value}"
                        )
                        return False
                else:
                    # String comparison
                    actual_str = str(actual_value).lower()
                    expected_str = str(expected_value).lower()

                    if operator == "==":
                        return actual_str == expected_str
                    elif operator == "!=":
                        return actual_str != expected_str
                    elif operator == "contains":
                        return expected_str in actual_str
                    elif operator == "!contains":
                        return expected_str not in actual_str
                    elif operator == "regex":
                        try:
                            return bool(re.search(expected_str, actual_str, re.IGNORECASE))
                        except re.error:
                            logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Invalid regex pattern: {expected_str}")
                            return False
                    elif operator == "!regex":
                        try:
                            return not bool(re.search(expected_str, actual_str, re.IGNORECASE))
                        except re.error:
                            logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Invalid regex pattern: {expected_str}")
                            return False
            break

    logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Could not parse condition: {condition}")
    return False


def execute_conditional_action(action: str, alert_data: dict) -> dict:
    """
    Execute a conditional action on alert data.

    Args:
        action: Action string like "add:priority:high", "merge:summary:subject,from"
        alert_data: Dictionary of alert labels and values

    Returns:
        dict: Modified alert_data

    Supported actions:
        add:label:value - Add static label
        replace:old:new - Rename label (if exists)
        merge:target:source1,source2 - Merge labels
        merge_remove:target:source1,source2 - Merge and remove sources
        remove:pattern - Remove labels matching pattern
        keep:pattern - Keep only labels matching pattern (remove all others)
        lookup:target:mapping_name:mapping_key[:flat] - Lookup value from another mapping
        extract:target:source:regex - Regex capture group extraction
        regreplace:target:regex:::replacement - Regex pattern substitution
        hash:target:field1,field2 - SHA-256 field fingerprinting
        substr:target:source|start|length - Substring extraction
        split:target:source|delimiter|index - Delimiter splitting
        eval:target:expression - Sandboxed expression evaluation
        cut:target:source:regex[:capture] - Regex match removal with optional capture
    """
    try:
        parts = action.split(":", 2)  # Split into max 3 parts

        if len(parts) < 2:
            logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Invalid action format: {action}")
            return alert_data

        command = parts[0].strip().lower()

        if command == "add" and len(parts) == 3:
            label_name = parts[1].strip()
            label_value = parts[2].strip()
            alert_data[label_name] = label_value
            logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Added label: {label_name}={label_value}")

        elif command == "replace" and len(parts) == 3:
            old_label = parts[1].strip()
            new_label = parts[2].strip()

            # Build case-insensitive lookup for alert_data
            alert_data_lc = {k.lower(): k for k in alert_data.keys()}
            old_label_lc = old_label.lower()

            if old_label_lc in alert_data_lc:
                orig_key = alert_data_lc[old_label_lc]
                alert_data[new_label] = alert_data.pop(orig_key)
                logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Replaced label: {orig_key} -> {new_label}")

        elif command in ["merge", "merge_remove", "merge_update"] and len(parts) == 3:
            target_label = parts[1].strip()
            source_labels = [s.strip() for s in parts[2].split(",")]

            # Build case-insensitive lookup for alert_data
            alert_data_lc = {k.lower(): k for k in alert_data.keys()}

            # Collect values from source labels (case-insensitive)
            values = []
            found_source_keys = []
            for source_label in source_labels:
                source_label_lc = source_label.lower()
                if source_label_lc in alert_data_lc:
                    orig_key = alert_data_lc[source_label_lc]
                    values.append(str(alert_data[orig_key]))
                    found_source_keys.append(orig_key)

            # Create merged value
            if values:
                merged_value = "~".join(values)
                alert_data[target_label] = merged_value
                logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Merged into {target_label}: {merged_value}")

                # Remove source labels if merge_remove
                if command == "merge_remove":
                    for source_key in found_source_keys:
                        alert_data.pop(source_key, None)
                    logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Removed source labels: {found_source_keys}")

        elif command == "remove" and len(parts) == 2:
            pattern = parts[1].strip()

            try:
                # Find labels to remove
                labels_to_remove = []
                for label_name in list(alert_data.keys()):
                    if re.search(pattern, label_name, re.IGNORECASE):
                        labels_to_remove.append(label_name)

                # Remove matching labels
                for label_name in labels_to_remove:
                    alert_data.pop(label_name, None)

                if labels_to_remove:
                    logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Removed labels matching '{pattern}': {labels_to_remove}")

            except re.error:
                logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Invalid regex pattern for remove: {pattern}")

        elif command == "lookup":
            # Lookup uses its own parsing - DO NOT change the top-level split
            if ":" not in action:
                logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Malformed lookup action: no arguments in '{action}'")
                return alert_data
            lookup_raw = action.split(":", 1)[1]

            # Detect :flat suffix
            flatten = False
            if lookup_raw.endswith(":flat"):
                flatten = True
                lookup_raw = lookup_raw[:-5]

            # Split into exactly 3 parts: target_label, mapping_name, mapping_key
            lookup_parts = lookup_raw.split(":", 2)
            if len(lookup_parts) != 3:
                logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Malformed lookup action: expected 3 fields, got {len(lookup_parts)} in '{action}'")
                return alert_data

            target_label = lookup_parts[0].strip()
            mapping_name = lookup_parts[1].strip()
            mapping_key_raw = lookup_parts[2].strip()

            # Extract optional @namespace from mapping_name
            override_ns = None
            if "@" in mapping_name:
                name_ns = mapping_name.split("@", 1)
                mapping_name = name_ns[0].strip()
                override_ns = name_ns[1].strip()
                if not mapping_name or not override_ns:
                    logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Malformed @namespace in lookup mapping_name: '{lookup_parts[1].strip()}'")
                    return alert_data

            # Resolve templates in mapping key
            resolved_key = mapping_key_raw
            try:
                template_result, _ = _process_template_string(mapping_key_raw, alert_data)
                if template_result is not None:
                    resolved_key = template_result
            except Exception as e:
                logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Template processing failed for lookup key '{mapping_key_raw}': {e}")

            # Fetch value from mapping API via MappingHook
            try:
                cond_mapping_hook = MappingHook()
                lookup_elements = cond_mapping_hook.list_mapping_elements(
                    mapping_name=mapping_name, mapping_namespace_name=override_ns or "EXT_EMAIL_ALERTS", mapping_key=resolved_key
                )
                if lookup_elements:
                    looked_up_value = lookup_elements[0].get("value", "")
                else:
                    logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Lookup found no value for {mapping_name}:{resolved_key}")
                    return alert_data
            except Exception as e:
                logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Lookup API error for {mapping_name}:{resolved_key}: {e}")
                return alert_data

            # Apply value (overwrite)
            if flatten:
                try:
                    json_obj = json.loads(looked_up_value)
                    if isinstance(json_obj, dict):
                        flat_pairs = _flatten_json(target_label, json_obj)
                        for flat_key, flat_value in flat_pairs:
                            alert_data[flat_key] = flat_value
                        logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Lookup flattened {len(flat_pairs)} labels")
                    else:
                        alert_data[target_label] = str(looked_up_value)
                except (json.JSONDecodeError, TypeError):
                    alert_data[target_label] = str(looked_up_value)
            else:
                alert_data[target_label] = str(looked_up_value)
            logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Lookup action: {target_label} from {mapping_name}:{resolved_key}")

        elif command == "keep" and len(parts) == 2:
            pattern = parts[1].strip()
            try:
                regex = re.compile(pattern, re.IGNORECASE)
                keys_to_remove = [k for k in alert_data.keys() if not regex.search(k)]
                for k in keys_to_remove:
                    alert_data.pop(k, None)
                logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Keep pattern '{pattern}': kept {len(alert_data)} labels")
            except re.error:
                logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Invalid regex pattern for keep: {pattern}")

        elif command == "extract" and len(parts) == 3:
            # Format: extract:target_label:source_label:regex
            extract_parts = parts[1] + ":" + parts[2]  # rejoin after command split
            sub_parts = extract_parts.split(":", 2)
            if len(sub_parts) >= 3:
                target_label = sub_parts[0].strip()
                source_label = sub_parts[1].strip()
                regex_pattern = sub_parts[2].strip()

                alert_data_lc = {k.lower(): k for k in alert_data.keys()}
                source_lc = source_label.lower()
                if source_lc in alert_data_lc:
                    orig_key = alert_data_lc[source_lc]
                    source_value = str(alert_data[orig_key])
                    try:
                        match = re.search(regex_pattern, source_value, re.IGNORECASE)
                        if match and match.group(1):
                            alert_data[target_label] = match.group(1)
                            logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Extracted '{target_label}'='{match.group(1)}'")
                    except re.error as e:
                        logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Invalid extract regex: {e}")

        elif command == "regreplace" and len(parts) >= 2:
            # Format: regreplace:target_label:regex:::replacement
            regreplace_raw = action.split(":", 1)[1] if ":" in action else ""
            colon_idx = regreplace_raw.find(":")
            if colon_idx > 0:
                target_label = regreplace_raw[:colon_idx].strip()
                pattern_and_repl = regreplace_raw[colon_idx + 1:]

                delim = ":::"
                delim_idx = pattern_and_repl.find(delim)
                if delim_idx >= 0:
                    regex_pattern = pattern_and_repl[:delim_idx]
                    replacement = pattern_and_repl[delim_idx + len(delim):]

                    # Resolve templates in replacement
                    try:
                        tmpl_result, _ = _process_template_string(replacement, alert_data)
                        if tmpl_result is not None:
                            replacement = tmpl_result
                    except Exception:
                        pass

                    alert_data_lc = {k.lower(): k for k in alert_data.keys()}
                    target_lc = target_label.lower()
                    if target_lc in alert_data_lc:
                        orig_key = alert_data_lc[target_lc]
                        current_value = str(alert_data[orig_key])
                        try:
                            new_value = re.sub(regex_pattern, replacement, current_value, flags=re.IGNORECASE)
                            alert_data[orig_key] = new_value
                            logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Regreplace on '{orig_key}'")
                        except re.error as e:
                            logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Invalid regreplace regex: {e}")

        elif command == "hash" and len(parts) >= 2:
            # Format: hash:target_label:field1,field2,field3
            hash_raw = action.split(":", 1)[1] if ":" in action else ""
            hash_parts = hash_raw.split(":", 1)
            if len(hash_parts) == 2:
                target_label = hash_parts[0].strip()
                field_names = [f.strip() for f in hash_parts[1].split(",")]

                alert_data_lc = {k.lower(): k for k in alert_data.keys()}
                canonical_parts = []
                for fn in sorted(field_names, key=str.lower):
                    fn_lc = fn.lower()
                    val = str(alert_data[alert_data_lc[fn_lc]]) if fn_lc in alert_data_lc else ""
                    canonical_parts.append(f"{fn_lc}={val}")
                canonical = "|".join(canonical_parts)
                hash_val = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
                alert_data[target_label] = hash_val
                logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Hash '{target_label}'='{hash_val}'")

        elif command == "substr" and len(parts) >= 2:
            # Format: substr:target_label:source_label|start|length
            substr_raw = action.split(":", 1)[1] if ":" in action else ""
            substr_parts = substr_raw.split(":", 1)
            if len(substr_parts) == 2:
                target_label = substr_parts[0].strip()
                value_spec = substr_parts[1].strip()
                # Resolve templates
                try:
                    tmpl_result, _ = _process_template_string(value_spec, alert_data)
                    if tmpl_result is not None:
                        value_spec = tmpl_result
                except Exception:
                    pass
                pipe_parts = value_spec.split("|")
                if len(pipe_parts) >= 2:
                    source_label = pipe_parts[0].strip()
                    try:
                        start_pos = int(pipe_parts[1].strip())
                    except ValueError:
                        start_pos = 0
                    length = None
                    if len(pipe_parts) >= 3 and pipe_parts[2].strip():
                        try:
                            length = int(pipe_parts[2].strip())
                        except ValueError:
                            pass
                    alert_data_lc = {k.lower(): k for k in alert_data.keys()}
                    source_lc = source_label.lower()
                    if source_lc in alert_data_lc:
                        orig_key = alert_data_lc[source_lc]
                        source_value = str(alert_data[orig_key])
                        if length is not None:
                            result = source_value[start_pos:start_pos + length]
                        else:
                            result = source_value[start_pos:]
                        alert_data[target_label] = result
                        logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Substr '{target_label}'='{result}'")

        elif command == "split" and len(parts) >= 2:
            # Format: split:target_label:source_label|delimiter|index
            split_raw = action.split(":", 1)[1] if ":" in action else ""
            split_parts_raw = split_raw.split(":", 1)
            if len(split_parts_raw) == 2:
                target_label = split_parts_raw[0].strip()
                value_spec = split_parts_raw[1].strip()
                # Resolve templates
                try:
                    tmpl_result, _ = _process_template_string(value_spec, alert_data)
                    if tmpl_result is not None:
                        value_spec = tmpl_result
                except Exception:
                    pass
                pipe_parts = value_spec.split("|")
                if len(pipe_parts) >= 3:
                    source_label = pipe_parts[0].strip()
                    delimiter = pipe_parts[1]  # Don't strip - may be whitespace
                    index_str = pipe_parts[2].strip()
                    alert_data_lc = {k.lower(): k for k in alert_data.keys()}
                    source_lc = source_label.lower()
                    if source_lc in alert_data_lc:
                        orig_key = alert_data_lc[source_lc]
                        source_value = str(alert_data[orig_key])
                        result_parts = source_value.split(delimiter)
                        if index_str == "*":
                            for i, part in enumerate(result_parts):
                                alert_data[f"{target_label}_{i}"] = part.strip()
                        else:
                            try:
                                idx = int(index_str)
                                if abs(idx) < len(result_parts):
                                    alert_data[target_label] = result_parts[idx].strip()
                                    logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Split '{target_label}'='{result_parts[idx].strip()}'")
                            except ValueError:
                                pass

        elif command == "eval" and len(parts) >= 2:
            # Format: eval:target_label:expression
            eval_raw = action.split(":", 1)[1] if ":" in action else ""
            eval_parts = eval_raw.split(":", 1)
            if len(eval_parts) == 2:
                target_label = eval_parts[0].strip()
                expr = eval_parts[1].strip()
                # Resolve templates
                try:
                    tmpl_result, _ = _process_template_string(expr, alert_data)
                    if tmpl_result is not None:
                        expr = tmpl_result
                except Exception:
                    pass
                # Evaluate in restricted sandbox
                safe_funcs = {
                    "int": lambda x: int(float(x)) if isinstance(x, str) else int(x),
                    "str": str, "float": float, "abs": abs,
                    "lower": lambda x: str(x).lower(),
                    "upper": lambda x: str(x).upper(),
                    "len": lambda x: len(str(x)),
                    "strip": lambda x: str(x).strip(),
                    "lstrip": lambda x: str(x).lstrip(),
                    "rstrip": lambda x: str(x).rstrip(),
                }
                try:
                    result = eval(expr, {"__builtins__": {}}, safe_funcs)  # noqa: S307
                    alert_data[target_label] = str(result)
                    logger.debug(f"[EMAIL_ALERT_CONDITIONAL] Eval '{target_label}'='{result}'")
                except Exception as e:
                    logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Eval failed for '{expr}': {e}")

        elif command == "cut" and len(parts) >= 2:
            # Format: cut:target:source:regex[:capture]
            cut_raw = action.split(":", 1)[1] if ":" in action else ""
            cut_parts = cut_raw.split(":", 1)
            if len(cut_parts) == 2:
                target_label = cut_parts[0].strip()
                source_and_rest = cut_parts[1]

                src_parts = source_and_rest.split(":", 2)
                if len(src_parts) >= 2:
                    source_label = src_parts[0].strip()
                    regex_pattern = src_parts[1].strip()
                    capture_label = src_parts[2].strip() if len(src_parts) >= 3 and src_parts[2].strip() else None

                    alert_data_lc = {k.lower(): k for k in alert_data.keys()}
                    source_lc = source_label.lower()
                    if source_lc in alert_data_lc:
                        orig_key = alert_data_lc[source_lc]
                        source_value = str(alert_data[orig_key])
                        try:
                            match = re.search(regex_pattern, source_value, re.IGNORECASE)
                            if match:
                                matched_text = match.group(0)
                                alert_data[target_label] = (
                                    source_value[:match.start()] + source_value[match.end():]
                                )
                                if capture_label:
                                    alert_data[capture_label] = matched_text
                                logger.debug(
                                    f"[EMAIL_ALERT_CONDITIONAL] Cut removed '{matched_text}' "
                                    f"from '{orig_key}' -> '{target_label}'"
                                )
                        except re.error as e:
                            logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Invalid cut regex: {e}")

        else:
            logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Unknown or invalid action: {action}")

    except Exception as e:
        logger.error(f"[EMAIL_ALERT_CONDITIONAL] Error executing action '{action}': {e}")

    return alert_data


def get_conditional_elements_for_alert(alert_type, mapping_hook, namespace):
    """
    Get conditional mapping elements for this alert type.

    Args:
        alert_type: The alert type for mapping lookup
        mapping_hook: The mapping hook instance
        namespace: The namespace for mappings

    Returns:
        list: List of conditional mapping elements
    """
    try:
        # Get all mapping elements for this alert type
        all_elements = mapping_hook.list_mapping_elements_by_mapping_name(
            mapping_namespace_name=namespace, mapping_name=alert_type
        )

        # Filter for conditional elements (those starting with "if~" or "NN~if~")
        conditional_elements = [
            elem for elem in all_elements
            if elem.get("key", "").lower().startswith("if~") or re.match(r'^\d{1,3}~if~', elem.get("key", ""), re.IGNORECASE)
        ]

        if conditional_elements:
            logger.info(
                f"[EMAIL_ALERT_CONDITIONAL] Found {len(conditional_elements)} conditional mappings for {alert_type}"
            )
        else:
            logger.debug(f"[EMAIL_ALERT_CONDITIONAL] No conditional mappings found for {alert_type}")

        return conditional_elements

    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_CONDITIONAL] Error getting conditional elements for {alert_type}: {e}")
        return []


def process_conditional_mappings(alert_data: dict, alert_type: str, mapping_hook, namespace: str) -> dict:
    """
    Process conditional mapping elements for if~condition~action format.

    Args:
        alert_data: The alert data dictionary to update
        alert_type: The alert type for mapping lookup
        mapping_hook: The mapping hook instance
        namespace: The namespace for mappings

    Returns:
        dict: Updated alert_data after processing conditional mappings
    """
    try:
        # Get conditional mapping elements for this alert type
        conditional_elements = get_conditional_elements_for_alert(alert_type, mapping_hook, namespace)

        if not conditional_elements:
            logger.debug(f"[EMAIL_ALERT_CONDITIONAL] No conditional elements found for {alert_type}")
            return alert_data

        conditional_count = 0

        # Sort conditional elements by order prefix
        sorted_elements = sort_conditional_mappings(conditional_elements)

        # Process each conditional mapping element
        for element in sorted_elements:
            key = element.get("key", "")
            value = element.get("value", "")

            conditional_count += 1

            # Parse the conditional mapping (handles both if~ and NN~if~ formats)
            condition, action = parse_conditional_mapping(key)

            if condition and action:
                # Evaluate the condition
                condition_met = evaluate_condition(condition, alert_data)

                logger.debug(
                    f"[EMAIL_ALERT_CONDITIONAL] Condition '{condition}' -> {condition_met} for action '{action}'"
                )

                # Execute action if condition is met
                if condition_met:
                    alert_data = execute_conditional_action(action, alert_data)
                    logger.info(
                        f"[EMAIL_ALERT_CONDITIONAL] Executed conditional action: {action} (condition: {condition})"
                    )
                else:
                    logger.debug(
                        f"[EMAIL_ALERT_CONDITIONAL] Skipped action '{action}' - condition not met: {condition}"
                    )

        if conditional_count > 0:
            logger.debug(
                f"[EMAIL_ALERT_CONDITIONAL] Processed {conditional_count} conditional mappings for {alert_type}"
            )

    except Exception as e:
        logger.error(f"[EMAIL_ALERT_CONDITIONAL] Error processing conditional mappings: {e}")

    return alert_data


# --- End conditional mapping functions ---


def ensure_required_labels(labels: Dict[str, Any], email_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensures that required labels are present with sensible defaults.
    This function is called to provide fallback values when mappings are not found
    or when the mapping pipeline fails.

    Args:
        labels: Current labels dictionary
        email_data: Original email data for extracting defaults

    Returns:
        Labels dictionary with required defaults added if missing
    """
    # Create a copy to avoid modifying the original
    ensured_labels = labels.copy()

    # Helper function to sanitize alertname
    def sanitize_alertname(text):
        if not text:
            return "email_alert"
        # Remove special characters and replace spaces with underscores
        sanitized = re.sub(r"[^\w\s-]", "", str(text))
        sanitized = re.sub(r"[\s-]+", "_", sanitized)
        sanitized = sanitized.lower().strip("_")
        return sanitized if sanitized else "email_alert"

    # Extract subject for alertname generation
    subject = email_data.get("subject", "")

    # Default values for required labels
    defaults = {
        "alertname": sanitize_alertname(subject),
        "severity": "warning",
        "instance": "--",  # Use "--" instead of sender email to match previous behavior
        "timestamp": email_data.get("date", str(datetime.now(timezone.utc))),
        "application": "email_processor",
        "category": "ext_email_alerts",
    }

    # Add defaults only if the label is missing, None, or empty string
    for key, default_value in defaults.items():
        current_value = ensured_labels.get(key)
        if (
            current_value is None
            or current_value == ""
            or (isinstance(current_value, str) and current_value.strip() == "")
        ):
            ensured_labels[key] = default_value
            logger.info(f"[EMAIL_ALERT_DEFAULTS] Added default label {key}={default_value}")

    return ensured_labels


def create_alert_object(email_data: Dict[str, Any], alert_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Creates an alert object from email_data using a handler system based on alert_type.
    The handler performs initial parsing. The result is then processed by a common
    mapping pipeline for label manipulation.
    """
    logger.debug(f"Creating alert object for alert_type: {alert_type} with email_data: {email_data.get('subject')}")

    # Get the appropriate handler
    handler = get_email_handler(alert_type_str=alert_type, email_data=email_data)

    # Handler performs initial parsing and structuring
    # This returns a dict like: {"labels": {...}, "summary": "...", "description": "...", "original_email_data": ...}
    alert_data_from_handler = handler.create_alert_object_from_email()

    # Extract components for further processing
    parsed_labels = alert_data_from_handler.get("labels", {})
    summary = alert_data_from_handler.get("summary", "Default Summary")
    description = alert_data_from_handler.get("description", "Default Description")

    # Initialize labels with parsed labels from handler
    labels = parsed_labels.copy()

    # --- Mapping-based label manipulation pipeline (applied to labels from handler) ---
    mapping_pipeline_success = False
    try:
        mapping_hook = MappingHook()
        MAPPING_NAMESPACE = "EXT_EMAIL_ALERTS"  # TODO: Make this configurable or derived if needed

        # Determine the mapping name: use alert_type if available, otherwise a default/global mapping might apply
        mapping_name_for_pipeline = (
            alert_type if alert_type else handler.alert_type
        )  # Use handler's alert_type if primary one is None

        if mapping_name_for_pipeline:
            logger.info(
                f"[EMAIL_ALERT_MAPPING] Attempting to apply mapping pipeline for type: {mapping_name_for_pipeline}"
            )

            # Combine labels, summary, and description into a single dict for mapping
            combined_labels = {**parsed_labels, "summary": summary, "description": description}

            # --- Add metadata from mapping as labels using get_mapping ---
            try:
                mapping_obj = mapping_hook.get_mapping(
                    mapping_name=mapping_name_for_pipeline,
                    mapping_namespace_name=MAPPING_NAMESPACE,
                )

                # Check if mapping was actually found
                if mapping_obj:
                    logger.info(f"[EMAIL_ALERT_MAPPING] Found mapping for {mapping_name_for_pipeline}")
                    metadata = mapping_obj.get("metadata")
                    if metadata and isinstance(metadata, list):
                        for item in metadata:
                            key = item.get("key")
                            value = item.get("value")
                            if key and value is not None:
                                combined_labels[key] = value
                else:
                    logger.warning(
                        f"[EMAIL_ALERT_MAPPING] No mapping found for {mapping_name_for_pipeline} in namespace {MAPPING_NAMESPACE}"
                    )
            except Exception as e:
                logger.warning(f"[EMAIL_ALERT_MAPPING] Error extracting metadata for {mapping_name_for_pipeline}: {e}")
            # --- End metadata addition ---

            # Apply mapping pipeline steps with individual error handling
            try:
                # Step 1: Add static labels
                add_labels_config = get_add_labels_for_alert(mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE)
                combined_labels = process_add_labels(combined_labels, add_labels_config)
                logger.info(f"Combined labels after ADD operation {combined_labels}")

                # Step 1.25: Lookup labels from other mappings
                combined_labels = process_lookup_labels(
                    combined_labels, mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE
                )
                logger.info(f"Combined labels after LOOKUP operation {combined_labels}")

                # Step 1.5: Process conditional mappings (if~condition~action)
                combined_labels = process_conditional_mappings(
                    combined_labels, mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE
                )
                logger.info(f"Combined labels after CONDITIONAL operation {combined_labels}")

                # Step 2: Replace labels
                replace_map_config = get_replace_map_for_alert(
                    mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE
                )
                combined_labels = process_replace_labels(combined_labels, replace_map_config)
                logger.info(f"Combined labels after REPLACE operation {combined_labels}")

                # Step 5.5a: Substr labels
                substr_elements = get_substr_elements_for_alert(mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE)
                combined_labels = process_substr_labels(combined_labels, substr_elements)
                logger.info(f"Combined labels after SUBSTR operation {combined_labels}")

                # Step 5.5b: Split labels
                split_elements = get_split_elements_for_alert(mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE)
                combined_labels = process_split_labels(combined_labels, split_elements)
                logger.info(f"Combined labels after SPLIT operation {combined_labels}")

                # Step 5.5c: Eval labels
                eval_elements = get_eval_elements_for_alert(mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE)
                combined_labels = process_eval_labels(combined_labels, eval_elements)
                logger.info(f"Combined labels after EVAL operation {combined_labels}")

                # Step 3: Merge/merge_remove labels
                merge_elements_config = get_merge_elements_for_alert(
                    mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE
                )
                combined_labels = process_merge_labels(combined_labels, merge_elements_config)
                logger.info(f"Combined labels after MERGE operation {combined_labels}")

                # Step 6.5a: Extract labels
                extract_elements = get_extract_elements_for_alert(mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE)
                combined_labels = process_extract_labels(combined_labels, extract_elements)
                logger.info(f"Combined labels after EXTRACT operation {combined_labels}")

                # Step 6.5b: Regreplace labels
                regreplace_elements = get_regreplace_elements_for_alert(mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE)
                combined_labels = process_regreplace_labels(combined_labels, regreplace_elements)
                logger.info(f"Combined labels after REGREPLACE operation {combined_labels}")

                # Step 6.5c: Cut labels
                cut_elements = get_cut_elements_for_alert(mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE)
                combined_labels = process_cut_labels(combined_labels, cut_elements)
                logger.info(f"Combined labels after CUT operation {combined_labels}")

                # Step 6.75: Hash labels
                hash_elements = get_hash_elements_for_alert(mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE)
                combined_labels = process_hash_labels(combined_labels, hash_elements)
                logger.info(f"Combined labels after HASH operation {combined_labels}")

                # Step 4: Remove labels by pattern
                remove_patterns_config = get_remove_patterns_for_alert(
                    mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE
                )
                combined_labels = process_remove_labels(combined_labels, remove_patterns_config)
                logger.info(f"Combined labels after REMOVE operation {combined_labels}")

                # Step 8: Keep labels (whitelist filter)
                keep_patterns = get_keep_patterns_for_alert(mapping_name_for_pipeline, mapping_hook, MAPPING_NAMESPACE)
                combined_labels = process_keep_labels(combined_labels, keep_patterns)
                logger.info(f"Combined labels after KEEP operation {combined_labels}")

                # Separate summary, description, and labels after mapping
                summary = combined_labels.pop("summary", summary)
                description = combined_labels.pop("description", description)
                labels = combined_labels

                mapping_pipeline_success = True
                logger.info(
                    f"[EMAIL_ALERT_MAPPING] Successfully applied mapping pipeline for type: {mapping_name_for_pipeline}"
                )

            except Exception as e:
                logger.warning(
                    f"[EMAIL_ALERT_MAPPING] Error in mapping pipeline steps for {mapping_name_for_pipeline}: {e}",
                    exc_info=True,
                )
                # Keep using original labels from handler
                labels = parsed_labels.copy()
        else:
            logger.info("[EMAIL_ALERT_MAPPING] Skipping mapping pipeline as no alert_type/mapping_name was determined.")

    except Exception as e:
        logger.warning(f"[EMAIL_ALERT_MAPPING] Error setting up mapping pipeline for {alert_type}: {e}", exc_info=True)
        # Keep using original labels from handler
        labels = parsed_labels.copy()

    # --- End mapping pipeline ---

    # Always ensure required labels are present, regardless of mapping success/failure
    labels = ensure_required_labels(labels, email_data)

    if not mapping_pipeline_success:
        logger.info("[EMAIL_ALERT_MAPPING] Using fallback labels with required defaults")

    # Normalize all label keys to lowercase before sending to alertmanager
    labels = {k.lower(): v for k, v in labels.items()}
    logger.debug("[EMAIL_ALERT_MAPPING] Normalized all label keys to lowercase")

    # The final alert object structure
    final_alert_object = {
        "labels": labels,
        "summary": summary,
        "description": description,
        # Optionally include original email data or parts of it if needed downstream
        # "original_subject": email_data.get("subject"),
        # "original_body_preview": email_data.get("body", "")[:200] + "..."
    }
    logger.info(f"Final alert object created with {len(labels)} labels: {list(labels.keys())}")
    return final_alert_object


def process_email_alerts(**context):
    """
    This DAG task processes an email alert by writing email details to a worklog,
    evaluating the email for alert criteria, and sending an alert via AlertHook if the evaluation passes.
    """
    # Retrieve email data from dag_run.conf.
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    email_data: Dict[str, Any] = conf.get("email_data", {})

    if not email_data:
        logger.error("Missing email_data in DAG run configuration")
        return None

    # Extract email information.
    subject = email_data.get("subject", "(No Subject)")
    from_addr = email_data.get("from", "(No From Address)")
    to_addr = email_data.get("to", "(No To Address)")
    date = email_data.get("date", str(datetime.now(timezone.utc)))
    body = email_data.get("body", "")

    # Create or re-use worklog.
    worklog_hook = WorkLogHook()
    worklog_id = conf.get("worklog_id")
    if worklog_id:
        worklog_hook.set_worklog_id(worklog_id)
        worklog_hook.info(f"Using existing worklog with ID: {worklog_id}")
    else:
        initial_worklog = worklog_hook.create_worklog(
            name="Email Processing Worklog", description="Worklog for processing email alerts"
        )
        worklog_id = initial_worklog["id"]

    # Log basic email info to the worklog.
    worklog_hook.info(f"Received email: {subject}")
    worklog_hook.info(f"From: {from_addr}")
    worklog_hook.info(f"To: {to_addr}")
    worklog_hook.info(f"Date: {date}")
    if body:
        worklog_hook.info("Email Body:")
        max_chunk_size = 1000
        if len(body) > max_chunk_size:
            chunks = [body[i : i + max_chunk_size] for i in range(0, len(body), max_chunk_size)]
            for i, chunk in enumerate(chunks):
                worklog_hook.info(f"Body Part {i+1}/{len(chunks)}: {chunk}")
        else:
            worklog_hook.info(body)
    else:
        worklog_hook.info("No email body content")

    # ---------------------------
    # Evaluate email for alerting
    # ---------------------------
    alert_rule_hook = RuleHook(namespace="email_alerts")
    alert_properties = {"subject": subject, "body": body, "from": from_addr, "to": to_addr}
    logger.info(f"Evaluating email for alerting with properties: {alert_properties}")
    alert_evaluation_result = alert_rule_hook.evaluate_rules("email_alerts", alert_properties)
    logger.info(f"Alert rule evaluation result: {alert_evaluation_result}")

    if alert_evaluation_result and normalize_boolean(
        alert_evaluation_result.get("evaluation_status")
    ):  # Ensure status is truthy
        applied_labels_alert = alert_evaluation_result.get("applied_labels", {})
        alert_template = applied_labels_alert.get("alert_template", "common_alert_tpl.j2")
        # The alert_type from rule evaluation will drive which handler is used.
        alert_type_from_rules = applied_labels_alert.get("alert_type", None)
        worklog_hook.info(f"Alert evaluation passed. Alert type from rules: {alert_type_from_rules}")

        # Create the alert object using the new handler-based system
        # email_data already contains subject, body, from, to, date.
        try:
            alert_obj = create_alert_object(email_data=email_data, alert_type=alert_type_from_rules)
            worklog_hook.info(f"Alert object created successfully with {len(alert_obj.get('labels', {}))} labels")
        except Exception as e:
            logger.error(f"Failed to create alert object for type '{alert_type_from_rules}': {e}", exc_info=True)
            worklog_hook.error(f"Failed to create alert object: {e}. Using fallback alert object.")

            # Helper function for consistent alertname sanitization
            def sanitize_alertname_fallback(text):
                if not text:
                    return "email_alert"
                # Remove special characters and replace spaces with underscores
                sanitized = re.sub(r"[^\w\s-]", "", str(text))
                sanitized = re.sub(r"[\s-]+", "_", sanitized)
                sanitized = sanitized.lower().strip("_")
                return sanitized if sanitized else "email_alert"

            # Create a fallback alert object with basic required labels
            fallback_labels = {
                "alertname": sanitize_alertname_fallback(subject),
                "severity": "warning",
                "instance": "--",  # Use "--" instead of sender email
                "timestamp": date,
                "application": "email_processor",
                "category": "ext_email_alerts",
                "source": "email_processing_dag",
                "alert_type": alert_type_from_rules if alert_type_from_rules else "unknown",
                "subject": subject,
                "from": from_addr,
                "to": to_addr,
            }

            alert_obj = {
                "labels": fallback_labels,
                "summary": f"Email Alert: {subject}" if subject else "Email Alert",
                "description": (
                    f"Alert generated from email processing. Subject: {subject}. From: {from_addr}. Body preview: {body[:200]}..."
                    if body
                    else f"Alert generated from email processing. Subject: {subject}. From: {from_addr}."
                ),
            }
            worklog_hook.info(f"Created fallback alert object with {len(fallback_labels)} labels")

        alert_hook = AlertHook()
        try:
            alert_response = alert_hook.send_alerts(alert_template, [alert_obj])
            worklog_hook.info(f"Alert sent successfully using template '{alert_template}'. Response: {alert_response}")
        except Exception as e:
            worklog_hook.error(f"Failed to send alert for type '{alert_type_from_rules}': {e}", exc_info=True)

    else:
        eval_status = (
            alert_evaluation_result.get("evaluation_status", "not available")
            if alert_evaluation_result
            else "not available"
        )
        logger.info(f"Alert evaluation did not pass (status: {eval_status}); no alerts triggered.")
        worklog_hook.info(f"Alert evaluation did not pass (status: {eval_status}); no alerts triggered.")

    # Finish up worklog logging.
    worklog_hook.info("Finished writing email details to worklog.")

    closed_worklog = worklog_hook.close_worklog()
    # Instead of using worklog_hook.info (which fails since the worklog is closed),
    # use the standard logger to log the closed worklog ID.
    logger.info(f"Worklog closed with ID: {closed_worklog['id']}")

    return {
        "worklog_id": closed_worklog["id"],
        "status": "processed",
        "email_subject": subject,
    }


with DAG(
    dag_id="process_email_alerts",
    default_args=default_args,
    description="Process email alerts: log details, evaluate for alerting, and send alerts if rules pass",
    schedule=None,  # This DAG is triggered via an API call or upstream process.
    start_date=pendulum.today('UTC').add(days=-1),
    tags=["email", "alerts", "processing"],
) as dag:

    process_email_alerts_task = PythonOperator(
        task_id="process_email_alerts",
        python_callable=process_email_alerts,
    )

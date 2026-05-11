import json
import logging
import re
import copy
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.mapping_hook import MappingHook  # type: ignore


def _process_template_string(template_str: str, data_dict: dict):
    """
    Mirror trapreceiver.process_template_string: return (processed_string or None, found_keys(list)).
    """
    try:
        pattern = re.compile(r"\{\{([^}]+)\}\}")
        placeholders = pattern.findall(template_str)
        if not placeholders:
            return None, []
        result = template_str
        found_keys = []
        # Case-insensitive key lookup
        data_lc = {k.lower(): k for k in data_dict.keys()}
        for placeholder in placeholders:
            key_raw = placeholder.strip()
            key_lookup = data_lc.get(key_raw.lower())
            value = str(data_dict.get(key_lookup, "")) if key_lookup else ""
            if key_lookup:
                found_keys.append(key_lookup)
            result = re.sub(r"\{\{\s*" + re.escape(placeholder) + r"\s*\}\}", value, result, flags=re.IGNORECASE)
        return result, found_keys
    except Exception:
        return None, []


def _get_mapping_metadata(mapping_hook: MappingHook, mapping_name: str, namespace: str) -> dict:
    metadata_labels = {}
    try:
        mapping_obj = mapping_hook.get_mapping(mapping_name=mapping_name, mapping_namespace_name=namespace)
        metadata = mapping_obj.get("metadata") if isinstance(mapping_obj, dict) else None
        if metadata and isinstance(metadata, list):
            for item in metadata:
                key = item.get("key")
                value = item.get("value")
                if key and value is not None:
                    metadata_labels[key] = value
    except Exception as e:
        logging.getLogger(__name__).warning("[MAPPING_TEST] Metadata fetch error: %s", e)
    return metadata_labels


def _get_add_labels(mapping_hook: MappingHook, mapping_name: str, namespace: str):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=mapping_name, mapping_namespace_name=namespace, mapping_key="add~"
        )
        return [(e["key"][4:], e.get("value", "")) for e in elements if e.get("key", "").startswith("add~")]
    except Exception as e:
        logging.getLogger(__name__).warning("[MAPPING_TEST] Add fetch error: %s", e)
        return []


def _get_merge_elements(mapping_hook: MappingHook, mapping_name: str, namespace: str):
    try:
        merge = mapping_hook.list_mapping_elements(
            mapping_name=mapping_name, mapping_namespace_name=namespace, mapping_key="merge~"
        )
        merge_remove = mapping_hook.list_mapping_elements(
            mapping_name=mapping_name, mapping_namespace_name=namespace, mapping_key="merge_remove~"
        )
        update = mapping_hook.list_mapping_elements(
            mapping_name=mapping_name, mapping_namespace_name=namespace, mapping_key="update~"
        )
        return (merge or []) + (merge_remove or []) + (update or [])
    except Exception as e:
        logging.getLogger(__name__).warning("[MAPPING_TEST] Merge fetch error: %s", e)
        return []


def _get_remove_patterns(mapping_hook: MappingHook, mapping_name: str, namespace: str):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=mapping_name, mapping_namespace_name=namespace, mapping_key="remove~"
        )
        return [e["key"][7:] for e in elements if e.get("key", "").startswith("remove~")]
    except Exception as e:
        logging.getLogger(__name__).warning("[MAPPING_TEST] Remove fetch error: %s", e)
        return []


def _get_keep_patterns(mapping_hook: MappingHook, mapping_name: str, namespace: str):
    try:
        elements = mapping_hook.list_mapping_elements(
            mapping_name=mapping_name, mapping_namespace_name=namespace, mapping_key="keep~"
        )
        return [e["key"][5:] for e in elements if e.get("key", "").startswith("keep~")]
    except Exception as e:
        logging.getLogger(__name__).warning("[MAPPING_TEST] Keep fetch error: %s", e)
        return []


def _get_conditional_elements(mapping_hook: MappingHook, mapping_name: str, namespace: str):
    try:
        all_elements = mapping_hook.list_mapping_elements_by_mapping_name(mapping_name, namespace)
        # Support both "if~" and ordered "NNN~if~"
        return [e for e in all_elements if re.match(r"^(\d{1,3}~)?if~", e.get("key", ""), re.IGNORECASE)]
    except Exception as e:
        logging.getLogger(__name__).warning("[MAPPING_TEST] Conditional fetch error: %s", e)
        return []


def _parse_conditional_key(key: str):
    # Strip optional order prefix like "10~"
    parts = key.split("~", 1)
    if parts and parts[0].isdigit() and len(parts) > 1:
        key = parts[1]
    if not key.startswith("if~"):
        return None, None
    pieces = key.split("~", 2)
    if len(pieces) != 3:
        return None, None
    return pieces[1], pieces[2]


def _evaluate_condition(condition: str, data: dict) -> bool:
    try:
        # OR groups separated by comma
        if "," in condition:
            return any(_evaluate_condition_group(group.strip(), data) for group in condition.split(","))
        return _evaluate_condition_group(condition, data)
    except Exception as e:
        logging.getLogger(__name__).warning("[MAPPING_TEST] Condition error '%s': %s", condition, e)
        return False


def _evaluate_condition_group(group: str, data: dict) -> bool:
    if "&&" in group:
        return all(_evaluate_single_condition(cond.strip(), data) for cond in group.split("&&"))
    return _evaluate_single_condition(group, data)


def _evaluate_single_condition(cond: str, data: dict) -> bool:
    operators = ["!contains", "contains", "!regex", "regex", ">=", "<=", "!=", "==", ">", "<"]
    for op in operators:
        if op in cond:
            left, right = cond.split(op, 1)
            label = left.strip()
            expected = right.strip()
            # case-insensitive key lookup
            data_lc = {k.lower(): k for k in data.keys()}
            key = data_lc.get(label.lower())
            actual_val = data.get(key, "") if key else ""
            if op in [">=", "<=", ">", "<"]:
                try:
                    a = float(actual_val) if actual_val != "" else 0
                    b = float(expected)
                    return (op == ">=" and a >= b) or (op == "<=" and a <= b) or (op == ">" and a > b) or (op == "<" and a < b)
                except Exception:
                    return False
            else:
                a = str(actual_val).lower()
                b = str(expected).lower()
                if op == "==":
                    return a == b
                if op == "!=":
                    return a != b
                if op == "contains":
                    return b in a
                if op == "!contains":
                    return b not in a
                if op == "regex":
                    try:
                        return bool(re.search(b, a, re.IGNORECASE))
                    except re.error:
                        return False
                if op == "!regex":
                    try:
                        return not bool(re.search(b, a, re.IGNORECASE))
                    except re.error:
                        return False
    return False


def _execute_action(action: str, data: dict) -> dict:
    try:
        parts = action.split(":", 2)
        if len(parts) < 2:
            return data
        cmd = parts[0].strip()
        if cmd == "add" and len(parts) == 3:
            name = parts[1].strip()
            rendered, _ = _process_template_string(parts[2].strip(), data)
            data[name] = rendered if rendered is not None else parts[2].strip()
        elif cmd == "replace" and len(parts) == 3:
            old = parts[1].strip()
            new = parts[2].strip()
            data_lc = {k.lower(): k for k in data.keys()}
            if old.lower() in data_lc:
                orig = data_lc[old.lower()]
                data[new] = data.pop(orig)
        elif cmd in ["merge", "merge_remove", "update"] and len(parts) == 3:
            target = parts[1].strip()
            rendered_sources, _ = _process_template_string(parts[2].strip(), data)
            sources_str = rendered_sources if rendered_sources is not None else parts[2]
            sources = [s.strip() for s in sources_str.split(",") if s.strip()]
            data_lc = {k.lower(): k for k in data.keys()}
            values = []
            found_keys = []
            for s in sources:
                if s.lower() in data_lc:
                    orig = data_lc[s.lower()]
                    values.append(str(data[orig]))
                    found_keys.append(orig)
            if values:
                merged = "~".join(values)
                if cmd == "update":
                    data[target] = merged
                else:
                    existing = str(data.get(target, ""))
                    data[target] = f"{existing}~{merged}" if existing else merged
                if cmd == "merge_remove":
                    for k in found_keys:
                        data.pop(k, None)
        elif cmd == "remove" and len(parts) == 2:
            pattern = parts[1].strip()
            to_remove = [k for k in list(data.keys()) if re.search(pattern, k, re.IGNORECASE)]
            for k in to_remove:
                data.pop(k, None)
        return data
    except Exception:
        return data


def run_mapping_pipeline(**context):
    logger = logging.getLogger("test_trap_mappings")

    # Accept input via dag_run.conf
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", {}) or {}

    # Inputs
    label_data = conf.get("label_data", {}) or {}
    mapping_name = conf.get("mapping_name") or conf.get("trap_oid")
    namespace = conf.get("namespace", "EXT_TRAP_ALERTS")
    debug = bool(conf.get("debug", True))

    if not mapping_name:
        raise ValueError("mapping_name (trap OID) is required in dag_run.conf")

    logger.info("Input label_data: %s", json.dumps(label_data, ensure_ascii=False))
    logger.info("Using mapping_name='%s', namespace='%s'", mapping_name, namespace)

    # Snapshot helper for step-by-step debugging
    snapshots = []

    def snap(step: str, info: dict = None):
        if not debug:
            return
        snapshots.append({
            "step": step,
            "info": info or {},
            "label_data": copy.deepcopy(label_data),
        })

    snap("initial", {"mapping_name": mapping_name, "namespace": namespace})

    mapping_hook = MappingHook()

    # Step 4: Metadata
    metadata_labels = _get_mapping_metadata(mapping_hook, mapping_name, namespace)
    logger.info("[STEP 4] Metadata labels fetched: %s", json.dumps(metadata_labels, ensure_ascii=False))
    if metadata_labels:
        label_data.update(metadata_labels)
    snap("after_step_4_metadata", {"added_keys": list(metadata_labels.keys())})

    # Step 5: Static add labels
    add_elements = _get_add_labels(mapping_hook, mapping_name, namespace)
    logger.info("[STEP 5] add~ elements: %s", json.dumps(add_elements, ensure_ascii=False))
    # Log labels before add
    labels_before_add = copy.deepcopy(label_data)
    logger.info("[STEP 5] Label data BEFORE add: %s", json.dumps(labels_before_add, ensure_ascii=False))
    snap("before_step_5_add", {"label_data": copy.deepcopy(label_data)})
    added = {}
    skipped = []
    for key, value in add_elements:
        # Trapreceiver uses key as-is; skip if exists
        if key not in label_data:
            rendered, _ = _process_template_string(value, label_data)
            rendered_final = rendered if rendered is not None else value
            label_data[key] = rendered_final
            added[key] = rendered_final
            logger.info("[STEP 5] Added %s=%s (templated from %s)", key, rendered, value)
        else:
            skipped.append(key)
            logger.info("[STEP 5] Skipped %s (already present)", key)
    # Compute and log add-only delta
    _add_before_keys = set(labels_before_add.keys())
    _add_after_keys = set(label_data.keys())
    _add_added = {k: label_data[k] for k in (_add_after_keys - _add_before_keys)}
    _add_removed = sorted(list(_add_before_keys - _add_after_keys))
    _add_updated = {k: label_data[k] for k in (_add_before_keys & _add_after_keys) if str(labels_before_add.get(k)) != str(label_data.get(k))}
    add_overall_delta = {"added": _add_added, "updated": _add_updated, "removed": _add_removed}
    logger.info("[STEP 5] Add-only changes: %s", json.dumps(add_overall_delta, ensure_ascii=False))
    logger.info("[STEP 5] Label data AFTER add: %s", json.dumps(label_data, ensure_ascii=False))
    snap("after_step_5_add", {"added": added, "skipped": skipped, "delta": add_overall_delta})

    # Step 5.5: Conditional mappings (if~condition~action, support ordered too)
    conditional_elements = _get_conditional_elements(mapping_hook, mapping_name, namespace)
    # Sort by numeric prefix if present

    def _order_key(elem):
        k = elem.get("key", "")
        m = re.match(r"^(\d{1,3})~if~", k)
        return int(m.group(1)) if m else 10**6
    logger.info("[STEP 5.5] Conditional elements: %s", json.dumps(conditional_elements, ensure_ascii=False))
    # Log labels before applying any conditionals
    labels_before_conditionals = copy.deepcopy(label_data)
    logger.info("[STEP 5.5] Label data BEFORE conditionals: %s", json.dumps(labels_before_conditionals, ensure_ascii=False))
    snap("before_step_5_5_conditionals", {"label_data": copy.deepcopy(label_data)})
    cond_log = []
    cond_added = {}
    cond_updated = {}
    cond_removed = []
    for elem in sorted(conditional_elements, key=_order_key):
        key = elem.get("key", "")
        value = elem.get("value", "")
        condition, action = _parse_conditional_key(key)
        if not (condition and action):
            cond_log.append({"key": key, "parsed": False})
            continue
        result = _evaluate_condition(condition, label_data)
        cond_entry = {"key": key, "condition": condition, "action": action, "value": value, "matched": result}
        if result:
            before = copy.deepcopy(label_data)
            label_data = _execute_action(action, label_data)
            after = label_data
            # compute diffs
            before_keys = set(before.keys())
            after_keys = set(after.keys())
            added_keys = after_keys - before_keys
            removed_keys = before_keys - after_keys
            changed_keys = {k for k in (before_keys & after_keys) if str(before[k]) != str(after[k])}
            for k in added_keys:
                cond_added[k] = after[k]
            for k in changed_keys:
                cond_updated[k] = after[k]
            cond_removed.extend(sorted(list(removed_keys)))
            cond_entry["added"] = {k: after[k] for k in added_keys}
            cond_entry["updated"] = {k: after[k] for k in changed_keys}
            cond_entry["removed"] = sorted(list(removed_keys))
        cond_log.append(cond_entry)
    logger.info("[STEP 5.5] Conditional evaluation detail: %s", json.dumps(cond_log, ensure_ascii=False))
    # Compute and log conditional-only delta (before vs after conditionals)
    _before_keys = set(labels_before_conditionals.keys())
    _after_keys = set(label_data.keys())
    _added_keys = _after_keys - _before_keys
    _removed_keys = _before_keys - _after_keys
    _updated_keys = {k for k in (_before_keys & _after_keys) if str(labels_before_conditionals.get(k)) != str(label_data.get(k))}
    conditional_overall_delta = {
        "added": {k: label_data[k] for k in _added_keys},
        "updated": {k: label_data[k] for k in _updated_keys},
        "removed": sorted(list(_removed_keys)),
    }
    logger.info("[STEP 5.5] Conditional-only changes: %s", json.dumps(conditional_overall_delta, ensure_ascii=False))
    snap("after_step_5_5_conditionals", {"evaluations": cond_log, "added": cond_added, "updated": cond_updated, "removed": cond_removed, "delta": conditional_overall_delta})
    logger.info("[STEP 5.5] Label data after conditionals: %s", json.dumps(label_data, ensure_ascii=False))

    # Step 6: Replace patterns (optional varBinds to mirror trapreceiver)
    varbinds = conf.get("varBinds") or []
    replace_log = []
    if varbinds:
        # Log labels before replace
        labels_before_replace = copy.deepcopy(label_data)
        logger.info("[STEP 6] Label data BEFORE replace: %s", json.dumps(labels_before_replace, ensure_ascii=False))
        snap("before_step_6_replace", {"label_data": copy.deepcopy(label_data)})
        logger.info("[STEP 6] Processing replace patterns for %d varBinds", len(varbinds))
        for i, vb in enumerate(varbinds):
            oid_num = str(vb.get("oid") or vb.get("oid_numeric") or "")
            oid_name = str(vb.get("name") or vb.get("oid_name") or "")
            val_str = str(vb.get("value", ""))
            # Try mapping elements filtered by mapping_key = oid_num
            elements = []
            try:
                elements = mapping_hook.list_mapping_elements(
                    mapping_name=mapping_name,
                    mapping_namespace_name=namespace,
                    mapping_key=oid_num,
                ) or []
            except Exception as e:
                logger.warning("[STEP 6] Error fetching elements for OID %s: %s", oid_num, e)
            applied = []
            for elem in elements:
                key = elem.get("key", "")
                value = elem.get("value", "")
                if key.startswith("replace~") and value:
                    label_key = key[len("replace~") :]
                    label_data[label_key] = val_str
                    applied.append({"label": label_key, "value": val_str})
            replace_log.append({"index": i, "oid": oid_num, "name": oid_name, "applied": applied})
        logger.info("[STEP 6] Replace results: %s", json.dumps(replace_log, ensure_ascii=False))
        # Compute and log replace-only delta
        _rep_before_keys = set(labels_before_replace.keys())
        _rep_after_keys = set(label_data.keys())
        _rep_added = {k: label_data[k] for k in (_rep_after_keys - _rep_before_keys)}
        _rep_removed = sorted(list(_rep_before_keys - _rep_after_keys))
        _rep_updated = {k: label_data[k] for k in (_rep_before_keys & _rep_after_keys) if str(labels_before_replace.get(k)) != str(label_data.get(k))}
        replace_overall_delta = {"added": _rep_added, "updated": _rep_updated, "removed": _rep_removed}
        logger.info("[STEP 6] Replace-only changes: %s", json.dumps(replace_overall_delta, ensure_ascii=False))
        logger.info("[STEP 6] Label data AFTER replace: %s", json.dumps(label_data, ensure_ascii=False))
        snap("after_step_6_replace", {"replace": replace_log, "delta": replace_overall_delta})

    # Step 7: Merge / update / merge_remove (exact semantics with template support)
    merge_elements = _get_merge_elements(mapping_hook, mapping_name, namespace)
    logger.info("[STEP 7] merge/update/merge_remove elements: %s", json.dumps(merge_elements, ensure_ascii=False))
    # Log labels before merge/update/remove
    labels_before_merge = copy.deepcopy(label_data)
    logger.info("[STEP 7] Label data BEFORE merge/update/remove: %s", json.dumps(labels_before_merge, ensure_ascii=False))
    snap("before_step_7_merge", {"label_data": copy.deepcopy(label_data)})
    merge_log = []
    merge_added = {}
    merge_updated = {}
    merge_removed = []
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

        label_data_lc = {k.lower(): k for k in label_data.keys()}
        target_label_lc = target_label.lower()

        merged_value_part = None
        found_source_keys = []

        # Try template first
        template_result, template_keys = _process_template_string(value_str, label_data)
        if template_result is not None:
            merged_value_part = template_result
            found_source_keys = template_keys
        else:
            # Comma-separated list
            source_labels = [s.strip() for s in value_str.split(",") if s.strip()]
            found_source_values = []
            for source_label in source_labels:
                src_lc = source_label.lower()
                if src_lc in label_data_lc:
                    orig_key = label_data_lc[src_lc]
                    found_source_values.append(str(label_data[orig_key]))
                    found_source_keys.append(orig_key)
            if not found_source_values:
                merge_log.append({"key": key, "skipped": "no sources found"})
                continue
            merged_value_part = "~".join(found_source_values)

        changed = {}
        removed_src = []
        if target_label_lc in label_data_lc:
            orig_target_key = label_data_lc[target_label_lc]
            if command_type == "update~":
                label_data[orig_target_key] = merged_value_part
                changed[orig_target_key] = merged_value_part
                merge_updated[orig_target_key] = merged_value_part
            else:
                existing_value = str(label_data[orig_target_key])
                label_data[orig_target_key] = f"{existing_value}~{merged_value_part}"
                changed[orig_target_key] = label_data[orig_target_key]
                merge_updated[orig_target_key] = label_data[orig_target_key]
        else:
            label_data[target_label] = merged_value_part
            changed[target_label] = merged_value_part
            merge_added[target_label] = merged_value_part

        if command_type == "merge_remove~":
            for sk in found_source_keys:
                if sk in label_data:
                    del label_data[sk]
                    removed_src.append(sk)
                    merge_removed.append(sk)

        merge_log.append({
            "key": key,
            "value": value_str,
            "changed": changed,
            "removed_sources": removed_src,
        })
    logger.info("[STEP 7] Merge/update results: %s", json.dumps(merge_log, ensure_ascii=False))
    # Compute and log merge-only delta
    _m_before_keys = set(labels_before_merge.keys())
    _m_after_keys = set(label_data.keys())
    _m_added = {k: label_data[k] for k in (_m_after_keys - _m_before_keys)}
    _m_removed = sorted(list(_m_before_keys - _m_after_keys))
    _m_updated = {k: label_data[k] for k in (_m_before_keys & _m_after_keys) if str(labels_before_merge.get(k)) != str(label_data.get(k))}
    merge_overall_delta = {"added": _m_added, "updated": _m_updated, "removed": _m_removed}
    logger.info("[STEP 7] Merge-only changes: %s", json.dumps(merge_overall_delta, ensure_ascii=False))
    logger.info("[STEP 7] Label data AFTER merge/update/remove: %s", json.dumps(label_data, ensure_ascii=False))
    snap("after_step_7_merge", {"changes": merge_log, "added": merge_added, "updated": merge_updated, "removed": sorted(list(set(merge_removed))), "delta": merge_overall_delta})

    # Step 8: Remove by pattern
    remove_patterns = _get_remove_patterns(mapping_hook, mapping_name, namespace)
    logger.info("[STEP 8] remove~ patterns: %s", json.dumps(remove_patterns, ensure_ascii=False))
    # Log labels before remove
    labels_before_remove = copy.deepcopy(label_data)
    logger.info("[STEP 8] Label data BEFORE remove: %s", json.dumps(labels_before_remove, ensure_ascii=False))
    snap("before_step_8_remove", {"label_data": copy.deepcopy(label_data)})
    removed_log = []
    for pattern in remove_patterns:
        before_keys = set(label_data.keys())
        label_data = _execute_action(f"remove:{pattern}", label_data)
        after_keys = set(label_data.keys())
        removed_now = sorted(list(before_keys - after_keys))
        if removed_now:
            removed_log.append({"pattern": pattern, "removed_keys": removed_now})
    logger.info("[STEP 8] Remove results: %s", json.dumps(removed_log, ensure_ascii=False))
    # Compute and log remove-only delta
    _rm_before_keys = set(labels_before_remove.keys())
    _rm_after_keys = set(label_data.keys())
    _rm_added = {k: label_data[k] for k in (_rm_after_keys - _rm_before_keys)}
    _rm_removed = sorted(list(_rm_before_keys - _rm_after_keys))
    _rm_updated = {k: label_data[k] for k in (_rm_before_keys & _rm_after_keys) if str(labels_before_remove.get(k)) != str(label_data.get(k))}
    remove_overall_delta = {"added": _rm_added, "updated": _rm_updated, "removed": _rm_removed}
    logger.info("[STEP 8] Remove-only changes: %s", json.dumps(remove_overall_delta, ensure_ascii=False))
    logger.info("[STEP 8] Label data AFTER remove: %s", json.dumps(label_data, ensure_ascii=False))
    snap("after_step_8_remove", {"removed": removed_log, "delta": remove_overall_delta})

    # Step 9: Keep patterns (keep only matching)
    keep_patterns = _get_keep_patterns(mapping_hook, mapping_name, namespace)
    if keep_patterns:
        logger.info("[STEP 9] keep~ patterns: %s", json.dumps(keep_patterns, ensure_ascii=False))
        # Log labels before keep
        labels_before_keep = copy.deepcopy(label_data)
        logger.info("[STEP 9] Label data BEFORE keep: %s", json.dumps(labels_before_keep, ensure_ascii=False))
        snap("before_step_9_keep", {"label_data": copy.deepcopy(label_data)})
        keys_to_keep = set()
        for k in list(label_data.keys()):
            for pattern in keep_patterns:
                try:
                    if re.search(pattern, k, re.IGNORECASE):
                        keys_to_keep.add(k)
                        break
                except re.error:
                    continue
        removed_by_keep = []
        for k in list(label_data.keys()):
            if k not in keys_to_keep:
                removed_by_keep.append(k)
                label_data.pop(k, None)
        # Compute and log keep-only delta
        _k_before_keys = set(labels_before_keep.keys())
        _k_after_keys = set(label_data.keys())
        _k_added = {k: label_data[k] for k in (_k_after_keys - _k_before_keys)}
        _k_removed = sorted(list(_k_before_keys - _k_after_keys))
        _k_updated = {k: label_data[k] for k in (_k_before_keys & _k_after_keys) if str(labels_before_keep.get(k)) != str(label_data.get(k))}
        keep_overall_delta = {"added": _k_added, "updated": _k_updated, "removed": _k_removed}
        logger.info("[STEP 9] Keep-only changes: %s", json.dumps(keep_overall_delta, ensure_ascii=False))
        logger.info("[STEP 9] Label data AFTER keep: %s", json.dumps(label_data, ensure_ascii=False))
        snap("after_step_9_keep", {"kept": sorted(list(keys_to_keep)), "removed": removed_by_keep, "delta": keep_overall_delta})

    logger.info("Result label_data: %s", json.dumps(label_data, ensure_ascii=False))
    # Compute overall delta of new/updated/removed compared to initial
    # Find the initial snapshot
    initial_snapshot = next((s for s in snapshots if s.get("step") == "initial"), None)
    base = (initial_snapshot or {}).get("label_data", {})
    base_keys = set(base.keys())
    final_keys = set(label_data.keys())
    overall_added = {k: label_data[k] for k in (final_keys - base_keys)}
    overall_removed = sorted(list(base_keys - final_keys))
    overall_updated = {k: label_data[k] for k in (base_keys & final_keys) if str(base.get(k)) != str(label_data.get(k))}

    overall_delta = {
        "added": overall_added,
        "updated": overall_updated,
        "removed": overall_removed,
    }

    logger.info("Overall delta: %s", json.dumps(overall_delta, ensure_ascii=False))
    # Return via XCom
    if debug:
        return {"label_data": label_data, "snapshots": snapshots, "new_labels": overall_delta}
    return {"label_data": label_data, "new_labels": overall_delta}


with DAG(
    dag_id="test_trap_mappings",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    test_mappings = PythonOperator(
        task_id="run_mapping_pipeline",
        python_callable=run_mapping_pipeline,
    )

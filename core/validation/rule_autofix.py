import re
from collections import defaultdict

import pandas as pd

from core.helper_unique_values import export_unique_values_from_dataframe
from core.validation.rule_engine import (
    classify_field_value,
    get_active_rule_profile,
    normalize_profile_name,
    normalize_rule_text,
    save_rule_profile,
    set_active_rule_profile,
)


INVALID_DOMAIN_REASON = "Valor fora do dominio configurado."


def _normalize_text_value(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def collect_invalid_domain_values(gdf):
    profile = get_active_rule_profile()
    fields = profile.get("fields", {})
    invalid_by_column = {}

    for column in fields:
        if column not in gdf.columns:
            continue

        counts = gdf[column].value_counts(dropna=False)
        invalid_values = {}

        for value, count in counts.items():
            normalized_value = _normalize_text_value(value)
            result = classify_field_value(column, normalized_value)
            if result["status"] == "invalid" and result["reason"] == INVALID_DOMAIN_REASON:
                invalid_values[normalized_value] = int(count)

        if invalid_values:
            invalid_by_column[column] = invalid_values

    return invalid_by_column


def _alias_stem(value):
    text = normalize_rule_text(value)
    text = re.sub(r"^APP_", "", text)
    text = text.replace("AREA DE PRESERVACAO PERMANENTE", "")
    text = text.replace("A RECOMPOR", "")
    text = re.sub(r"(DAS|DOS|DA|DO|DE)", " ", text)
    text = " ".join(text.split())
    if text.endswith("S") and len(text) > 4:
        text = text[:-1]
    return text


def _infer_alias_target(value, accepted_values):
    value_stem = _alias_stem(value)
    if not value_stem:
        return None

    candidates = [candidate for candidate in accepted_values if _alias_stem(candidate) == value_stem]
    if len(candidates) == 1:
        return candidates[0]
    return None


def _update_fields(profile, invalid_by_column):
    summary = {
        "accepted_values_added": defaultdict(list),
        "aliases_added": defaultdict(dict),
    }

    fields = profile.setdefault("fields", {})

    for column, invalid_values in invalid_by_column.items():
        field_entry = fields.setdefault(column, {})
        accepted_values = list(field_entry.get("accepted_values", []))
        aliases = dict(field_entry.get("aliases", {}))

        accepted_lookup = {normalize_rule_text(item): item for item in accepted_values}
        alias_lookup = {normalize_rule_text(item): item for item in aliases}

        for raw_value in invalid_values:
            value = _normalize_text_value(raw_value)
            if not value:
                continue

            normalized = normalize_rule_text(value)
            if normalized in accepted_lookup or normalized in alias_lookup:
                continue

            alias_target = _infer_alias_target(value, accepted_values)
            if alias_target:
                aliases[value] = alias_target
                summary["aliases_added"][column][value] = alias_target
                alias_lookup[normalized] = value
                continue

            accepted_values.append(value)
            accepted_lookup[normalized] = value
            summary["accepted_values_added"][column].append(value)

        field_entry["accepted_values"] = sorted(dict.fromkeys(accepted_values), key=str)
        if aliases:
            field_entry["aliases"] = dict(sorted(aliases.items()))
        elif "aliases" in field_entry:
            field_entry.pop("aliases")

    return summary


def _resolve_relation_columns(profile, relation_key, gdf):
    if "_to_" not in relation_key:
        return None, None

    source_token, target_token = relation_key.split("_to_", 1)
    field_names = set(profile.get("fields", {}).keys()) | set(gdf.columns)

    def _resolve(token):
        preferred = f"sdb_{token}"
        if preferred in field_names:
            return preferred
        if token in field_names:
            return token
        for field_name in field_names:
            if str(field_name).endswith(f"_{token}"):
                return field_name
        return None

    return _resolve(source_token), _resolve(target_token)


def _update_relations(profile, gdf):
    summary = defaultdict(dict)
    relations = profile.get("relations", {})

    for relation_key, relation_map in relations.items():
        source_column, target_column = _resolve_relation_columns(profile, relation_key, gdf)
        if not source_column or not target_column:
            continue
        if source_column not in gdf.columns or target_column not in gdf.columns:
            continue

        pair_df = gdf[[source_column, target_column]].copy()
        pair_df[source_column] = pair_df[source_column].map(_normalize_text_value)
        pair_df[target_column] = pair_df[target_column].map(_normalize_text_value)
        pair_df = pair_df.dropna()
        pair_df = pair_df[(pair_df[source_column] != "") & (pair_df[target_column] != "")]
        if pair_df.empty:
            continue

        grouped = pair_df.groupby(source_column)[target_column].agg(lambda items: sorted(set(items)))
        for source_value, targets in grouped.items():
            if len(targets) != 1:
                continue

            target_value = targets[0]
            source_result = classify_field_value(source_column, source_value)
            target_result = classify_field_value(target_column, target_value)
            if source_result["status"] == "invalid" or target_result["status"] == "invalid":
                continue

            source_canonical = source_result["normalized_value"]
            target_canonical = target_result["normalized_value"]
            if not source_canonical or not target_canonical:
                continue

            if relation_map.get(source_canonical) == target_canonical:
                continue
            if source_canonical in relation_map and relation_map[source_canonical] != target_canonical:
                continue

            relation_map[source_canonical] = target_canonical
            summary[relation_key][source_canonical] = target_canonical

        relations[relation_key] = dict(sorted(relation_map.items()))

    profile["relations"] = relations
    return summary


def autofix_rule_profile_from_invalid_domains(profile_name, gdf, support_report_path=None):
    invalid_by_column = collect_invalid_domain_values(gdf)
    if not invalid_by_column:
        return {
            "changed": False,
            "report_path": None,
            "invalid_columns": [],
            "accepted_values_added": {},
            "aliases_added": {},
            "relations_added": {},
            "profile_path": None,
        }

    report_path = None
    if support_report_path:
        report_path = str(export_unique_values_from_dataframe(gdf, support_report_path, columns=list(invalid_by_column.keys())))

    profile = get_active_rule_profile()
    field_summary = _update_fields(profile, invalid_by_column)

    profile_path = save_rule_profile(profile_name, profile)
    set_active_rule_profile(normalize_profile_name(profile_name))

    profile = get_active_rule_profile()
    relation_summary = _update_relations(profile, gdf)
    profile_path = save_rule_profile(profile_name, profile)
    set_active_rule_profile(normalize_profile_name(profile_name))

    changed = any(field_summary["accepted_values_added"].values()) or any(field_summary["aliases_added"].values()) or any(relation_summary.values())

    return {
        "changed": changed,
        "report_path": report_path,
        "invalid_columns": sorted(invalid_by_column.keys()),
        "accepted_values_added": {key: value for key, value in field_summary["accepted_values_added"].items() if value},
        "aliases_added": {key: value for key, value in field_summary["aliases_added"].items() if value},
        "relations_added": {key: value for key, value in relation_summary.items() if value},
        "profile_path": profile_path,
    }

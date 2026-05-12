import pandas as pd

from core.rules.engine import classify_field_value, has_field_rules
from core.validation.schema import target_column_name
from core.validation.summary import relation_summary_entry


def build_classification_cache(rule_profile, column, source_series):
    cache = {
        value: classify_field_value(rule_profile, column, value)
        for value in source_series.dropna().drop_duplicates().tolist()
    }
    null_result = (
        classify_field_value(rule_profile, column, None)
        if source_series.isna().any()
        else None
    )
    return cache, null_result


def series_from_cache(source_series, cache, null_result, property_name):
    result = source_series.map(
        lambda value: cache[value][property_name] if pd.notna(value) else None
    )
    if null_result is not None:
        result = result.where(source_series.notna(), null_result[property_name])
    return result


def get_effective_domain_series(gdf, column, rule_profile):
    target_column = target_column_name(column)
    if target_column in gdf.columns:
        return gdf[target_column]

    if column not in gdf.columns:
        return pd.Series(index=gdf.index, dtype="object")

    if not has_field_rules(rule_profile, column):
        return gdf[column]

    classification_cache, null_result = build_classification_cache(
        rule_profile,
        column,
        gdf[column],
    )
    return series_from_cache(
        gdf[column],
        classification_cache,
        null_result,
        "normalized_value",
    )


def resolve_relation_columns(gdf, relation_key):
    if "_to_" not in relation_key:
        return None, None

    source_token, target_token = relation_key.split("_to_", 1)
    available_columns = set(gdf.columns)

    def _resolve(token):
        direct_name = f"sdb_{token}"
        if direct_name in available_columns:
            return direct_name
        if token in available_columns:
            return token
        for field_name in available_columns:
            if str(field_name).endswith(f"_{token}"):
                return field_name
        return None

    return _resolve(source_token), _resolve(target_token)


def apply_relation_consistency_if_needed(
    gdf,
    column,
    normalized_series,
    rule_profile,
    validation_session=None,
):
    relations = rule_profile.get("relations", {})
    if not relations:
        return normalized_series

    updated_series = normalized_series

    for relation_key, relation_map in relations.items():
        if not relation_map:
            continue

        source_column, target_column = resolve_relation_columns(gdf, relation_key)
        if target_column != column or not source_column:
            continue
        if source_column not in gdf.columns or target_column not in gdf.columns:
            continue

        summary = relation_summary_entry(
            relation_key,
            validation_session=validation_session,
        )
        summary["relation_map"] = dict(relation_map)
        source_series = get_effective_domain_series(gdf, source_column, rule_profile)
        expected_target_series = source_series.map(relation_map)
        unchecked_mask = source_series.isna() | expected_target_series.isna()
        consistent_mask = (~unchecked_mask) & (updated_series == expected_target_series)
        autocorrected_mask = (~unchecked_mask) & (~consistent_mask)

        unchecked_count = int(unchecked_mask.sum())
        consistent_count = int(consistent_mask.sum())
        autocorrected_count = int(autocorrected_mask.sum())

        if unchecked_count:
            summary["status_counts"].update({"unchecked": unchecked_count})
            summary["reason_counts"].update(
                {f"Valor fonte fora do dominio configurado para {relation_key}.": unchecked_count}
            )
            unchecked_values = source_series.loc[unchecked_mask].fillna("<NULL>").astype(str)
            summary["unchecked_source_counts"].update(unchecked_values.value_counts().to_dict())

        if consistent_count:
            summary["status_counts"].update({"consistent": consistent_count})

        if autocorrected_count:
            summary["status_counts"].update({"autocorrected": autocorrected_count})
            summary["autocorrected_counts"].update(
                source_series.loc[autocorrected_mask].value_counts().to_dict()
            )

        corrected_series = updated_series.copy()
        corrected_series.loc[~unchecked_mask] = expected_target_series.loc[~unchecked_mask]
        updated_series = corrected_series

    return updated_series


__all__ = [
    "apply_relation_consistency_if_needed",
    "build_classification_cache",
    "get_effective_domain_series",
    "resolve_relation_columns",
    "series_from_cache",
]

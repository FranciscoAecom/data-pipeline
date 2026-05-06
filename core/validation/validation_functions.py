import unicodedata
from collections import Counter
from difflib import get_close_matches

import geopandas as gpd
import pandas as pd

from core.date import parse_date_series
from core.schema import target_column_name
from core.rules.engine import (
    build_field_mapping,
    classify_field_value,
    has_field_rules,
)
from core.spatial.spatial_functions import check_attribute_geometric_duplicates
from core.utils import log
from settings import INTERACTIVE_ATTRIBUTE_REVIEW


_VALIDATE_ATTRIBUTE_MAPPINGS = {}
_VALIDATION_SUMMARY = {
    "fields": {},
    "relations": {},
}


def reset_validate_attribute_mappings():
    _VALIDATE_ATTRIBUTE_MAPPINGS.clear()
    _VALIDATION_SUMMARY["fields"].clear()
    _VALIDATION_SUMMARY["relations"].clear()


def _normalize_for_compare(value):
    if not isinstance(value, str):
        return ""
    text = value.strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if ord(ch) < 128)
    return text.upper()


def _get_non_empty_unique_text_values(series):
    values = series.dropna().astype(str).str.strip()
    values = values[values != ""]
    return values.drop_duplicates().tolist()


def _build_fuzzy_mapping(gdf, column):
    unique_values = _get_non_empty_unique_text_values(gdf[column])
    if not unique_values:
        return {}

    canonical_norms = []
    canonical_values = []
    replacements = {}

    for value in unique_values:
        normalized = _normalize_for_compare(value)

        if not normalized:
            replacements[value] = value
            continue

        close = get_close_matches(normalized, canonical_norms, n=1, cutoff=0.9)
        if close:
            replacements[value] = canonical_values[canonical_norms.index(close[0])]
            continue

        canonical_norms.append(normalized)
        canonical_values.append(value)
        replacements[value] = value

    return replacements


def _build_validate_attribute_mapping(gdf, column, rule_profile):
    unique_values = _get_non_empty_unique_text_values(gdf[column])

    if not unique_values:
        return {
            "replacements": {},
            "corrections": [],
            "invalid_values": [],
            "strategy": "empty",
        }

    if has_field_rules(rule_profile, column):
        replacements, corrections, invalid_values = build_field_mapping(
            rule_profile,
            column,
            unique_values,
        )
        return {
            "replacements": replacements,
            "corrections": corrections,
            "invalid_values": invalid_values,
            "strategy": "domain_rules",
        }

    replacements = _build_fuzzy_mapping(gdf, column)
    corrections = [(value, target) for value, target in replacements.items() if value != target]
    return {
        "replacements": replacements,
        "corrections": corrections,
        "invalid_values": [],
        "strategy": "fuzzy",
    }


def _has_optional_function(funcs, function_name):
    return any(
        str(func) == function_name or str(func).endswith(f".{function_name}")
        for func in funcs
    )


def prepare_validate_shapefile_attribute_mappings(gdf, mapping, rule_profile):
    for column, funcs in mapping.items():
        if not _has_optional_function(funcs, "validate_shapefile_attribute"):
            continue

        if column not in gdf.columns:
            log(f"Atributo {column} nao encontrado para validacao")
            continue

        if column in _VALIDATE_ATTRIBUTE_MAPPINGS:
            continue

        mapping_result = _build_validate_attribute_mapping(gdf, column, rule_profile)
        replacements = mapping_result["replacements"]
        corrections = mapping_result["corrections"]
        strategy = mapping_result["strategy"]

        if not replacements:
            log(f"Nenhum valor unico encontrado em {column} para validacao")
            _VALIDATE_ATTRIBUTE_MAPPINGS[column] = {}
            continue

        if strategy == "domain_rules":
            _VALIDATE_ATTRIBUTE_MAPPINGS[column] = replacements
            continue

        log(f"\nSugestoes de correcao De:Para para {column}:", raw=True)

        if not corrections:
            log("  Nenhuma sugestao de correcao encontrada.", raw=True)
            _VALIDATE_ATTRIBUTE_MAPPINGS[column] = {}
            continue

        max_display = 100
        displayed_suggestions = corrections[:max_display]

        for idx, (value, target) in enumerate(displayed_suggestions, 1):
            log(f" {idx}. {value:80} -> {target}", raw=True)

        if len(corrections) > max_display:
            log(f"  ... e mais {len(corrections) - max_display} correcoes", raw=True)

        log("", raw=True)
        if not INTERACTIVE_ATTRIBUTE_REVIEW:
            log(
                "Modo nao interativo habilitado em settings.py; "
                "nenhuma sugestao fuzzy sera aplicada automaticamente.",
                raw=True,
            )
            _VALIDATE_ATTRIBUTE_MAPPINGS[column] = {}
            continue

        log("Informe os numeros das correcoes que deseja aplicar.", raw=True)
        log("Exemplos: 1,3,4 | all | ENTER para nao aplicar nenhuma", raw=True)

        choice = input(f"Aplicar quais correcoes para a coluna '{column}'? ")
        normalized_choice = choice.strip().lower()

        if not normalized_choice:
            log(f"Nenhuma correcao aplicada para {column}.", raw=True)
            _VALIDATE_ATTRIBUTE_MAPPINGS[column] = {}
            continue

        if normalized_choice == "all":
            selected_suggestions = displayed_suggestions
        else:
            try:
                selected_indices = {
                    int(item.strip()) for item in choice.split(",") if item.strip()
                }
            except ValueError:
                log(f"Entrada invalida para {column}. Nenhuma correcao aplicada.", raw=True)
                _VALIDATE_ATTRIBUTE_MAPPINGS[column] = {}
                continue

            invalid_indices = [
                idx for idx in selected_indices if idx < 1 or idx > len(displayed_suggestions)
            ]
            if invalid_indices:
                log(
                    f"Indices invalidos para {column}: {', '.join(str(i) for i in sorted(invalid_indices))}. "
                    "Nenhuma correcao aplicada.",
                    raw=True,
                )
                _VALIDATE_ATTRIBUTE_MAPPINGS[column] = {}
                continue

            selected_suggestions = [
                displayed_suggestions[idx - 1]
                for idx in sorted(selected_indices)
            ]

        selected_replacements = {value: value for value in replacements.keys()}
        for value, target in selected_suggestions:
            selected_replacements[value] = target

        _VALIDATE_ATTRIBUTE_MAPPINGS[column] = selected_replacements
        log(
            f"{len(selected_suggestions)} correcao(oes) aplicada(s) para {column}.",
            raw=True,
        )


def validate_date_fields(gdf, column, **_context):
    gdf[target_column_name(column)] = parse_date_series(gdf[column])
    return gdf


def _field_summary_entry(column):
    return _VALIDATION_SUMMARY["fields"].setdefault(
        column,
        {
            "status_counts": Counter(),
            "reason_counts": Counter(),
        },
    )


def _register_domain_validation_summary(column, statuses, reasons):
    entry = _field_summary_entry(column)
    entry["status_counts"].update(statuses)
    entry["reason_counts"].update(
        reason for status, reason in zip(statuses, reasons)
        if status in {"invalid", "empty"} and reason
    )


def _series_has_changes(source_series, candidate_series):
    same_mask = source_series.eq(candidate_series)
    same_mask = same_mask | (source_series.isna() & candidate_series.isna())
    same_mask = same_mask.fillna(False)
    return not bool(same_mask.all())


def _apply_target_column_if_needed(gdf, target_column, source_series, candidate_series):
    if _series_has_changes(source_series, candidate_series):
        gdf[target_column] = candidate_series
    elif target_column in gdf.columns:
        gdf = gdf.drop(columns=[target_column])
    return gdf


def _get_effective_domain_series(gdf, column, rule_profile):
    target_column = _target_column_name(column)
    if target_column in gdf.columns:
        return gdf[target_column]

    if column not in gdf.columns:
        return pd.Series(index=gdf.index, dtype="object")

    if not has_field_rules(rule_profile, column):
        return gdf[column]

    classification_cache, null_result = _build_classification_cache(
        rule_profile,
        column,
        gdf[column],
    )
    return _series_from_cache(
        gdf[column],
        classification_cache,
        null_result,
        "normalized_value",
    )


def _resolve_relation_columns(gdf, relation_key):
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


def _relation_summary_entry(relation_key):
    return _VALIDATION_SUMMARY["relations"].setdefault(
        relation_key,
        {
            "status_counts": Counter(),
            "reason_counts": Counter(),
            "autocorrected_counts": Counter(),
            "unchecked_source_counts": Counter(),
            "relation_map": {},
        },
    )


def _apply_relation_consistency_if_needed(gdf, column, normalized_series, rule_profile):
    relations = rule_profile.get("relations", {})
    if not relations:
        return normalized_series

    updated_series = normalized_series

    for relation_key, relation_map in relations.items():
        if not relation_map:
            continue

        source_column, target_column = _resolve_relation_columns(gdf, relation_key)
        if target_column != column or not source_column:
            continue
        if source_column not in gdf.columns or target_column not in gdf.columns:
            continue

        summary = _relation_summary_entry(relation_key)
        summary["relation_map"] = dict(relation_map)
        source_series = _get_effective_domain_series(gdf, source_column, rule_profile)
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


def log_validation_summary():
    for column, entry in _VALIDATION_SUMMARY["fields"].items():
        status_counts = entry["status_counts"]
        normalized_count = status_counts.get("normalized", 0)
        invalid_count = status_counts.get("invalid", 0)
        empty_count = status_counts.get("empty", 0)

        if normalized_count == 0 and invalid_count == 0 and empty_count == 0:
            continue

        parts = []
        if normalized_count:
            parts.append(f"{normalized_count} normalizado(s) por alias")
        if invalid_count:
            parts.append(f"{invalid_count} invalido(s)")
        if empty_count:
            parts.append(f"{empty_count} vazio(s)")

        log(f"Resumo validacao {column}: {', '.join(parts)}")
        for reason, count in entry["reason_counts"].most_common(5):
            log(f"  {count}x - {reason}")

    for relation_key, consistency in _VALIDATION_SUMMARY["relations"].items():
        autocorrected = consistency["status_counts"].get("autocorrected", 0)
        inconsistent = consistency["status_counts"].get("inconsistent", 0)
        unchecked = consistency["status_counts"].get("unchecked", 0)
        if not (autocorrected or inconsistent or unchecked):
            continue

        parts = []
        if autocorrected:
            parts.append(f"{autocorrected} autocorrigido(s) pela relacao {relation_key}")
        if inconsistent:
            parts.append(f"{inconsistent} inconsistente(s)")
        if unchecked:
            parts.append(f"{unchecked} nao verificado(s)")
        log(f"Resumo consistencia relacao {relation_key}: {', '.join(parts)}")

        relation_map = consistency.get("relation_map", {})
        for source_value, count in consistency["autocorrected_counts"].most_common(5):
            expected_target = relation_map.get(source_value)
            if expected_target:
                log(f"  {count}x - Valor ajustado automaticamente para {source_value}: {expected_target}")
        if unchecked:
            for source_value, count in consistency["unchecked_source_counts"].most_common(10):
                log(f"  {count}x - Valor fonte fora do dominio configurado: {source_value}")
        for reason, count in consistency["reason_counts"].most_common(5):
            log(f"  {count}x - {reason}")


def _target_column_name(column):
    return target_column_name(column)


def _build_classification_cache(rule_profile, column, source_series):
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


def _series_from_cache(source_series, cache, null_result, property_name):
    result = source_series.map(
        lambda value: cache[value][property_name] if pd.notna(value) else None
    )
    if null_result is not None:
        result = result.where(source_series.notna(), null_result[property_name])
    return result


def validate_shapefile_attribute(gdf, column, rule_profile=None, **_context):
    if column not in gdf.columns:
        log(f"Atributo {column} nao encontrado")
        return gdf

    target_column = _target_column_name(column)
    source_series = gdf[column]

    replacements = _VALIDATE_ATTRIBUTE_MAPPINGS.get(column, {})

    if rule_profile is None:
        return gdf

    if has_field_rules(rule_profile, column):
        classification_cache, null_result = _build_classification_cache(
            rule_profile,
            column,
            source_series,
        )
        normalized_series = _series_from_cache(
            source_series,
            classification_cache,
            null_result,
            "normalized_value",
        )
        statuses = _series_from_cache(source_series, classification_cache, null_result, "status")
        reasons = _series_from_cache(source_series, classification_cache, null_result, "reason")

        _register_domain_validation_summary(column, statuses.tolist(), reasons.tolist())
        normalized_series = _apply_relation_consistency_if_needed(
            gdf,
            column,
            normalized_series,
            rule_profile,
        )
        gdf = _apply_target_column_if_needed(
            gdf,
            target_column,
            source_series,
            normalized_series,
        )
        return gdf

    if replacements:
        stripped_source = source_series.where(source_series.isna(), source_series.astype(str).str.strip())
        mapped_values = stripped_source.map(replacements)
        candidate_series = mapped_values.where(mapped_values.notna(), stripped_source)
        gdf = _apply_target_column_if_needed(
            gdf,
            target_column,
            source_series,
            candidate_series,
        )

    return gdf


def check_attribute_duplicates(gdf):
    non_geom_columns = [c for c in gdf.columns if c != "geometry"]
    dup = gdf[gdf.duplicated(subset=non_geom_columns)]
    count = len(dup)
    return gdf, count


def _get_duplicate_columns(gdf):
    exclude = {
        "acm_id",
        "acm_a_ha",
        "acm_prm_km",
        "acm_long",
        "acm_lat",
        "geometry",
    }
    return [c for c in gdf.columns if c not in exclude]


def get_attribute_duplicate_mask(gdf):
    compare_columns = _get_duplicate_columns(gdf)
    if not compare_columns:
        return pd.Series(False, index=gdf.index)
    return gdf.duplicated(subset=compare_columns, keep=False)


def get_attribute_duplicate_records(gdf, dup_mask=None):
    if dup_mask is None:
        dup_mask = get_attribute_duplicate_mask(gdf)
    duplicates = gdf[dup_mask].copy()
    count = len(duplicates)
    return duplicates, count


def check_duplicates(gdf):
    attr_count = int(get_attribute_duplicate_mask(gdf).sum())
    gdf, geom_count = check_attribute_geometric_duplicates(gdf)
    return gdf, attr_count, geom_count

import unicodedata
from collections import Counter
from difflib import get_close_matches

import geopandas as gpd
import pandas as pd

from core.validation.rule_engine import (
    build_field_mapping,
    classify_field_value,
    get_active_rule_profile,
    has_field_rules,
)
from core.spatial.spatial_functions import check_attribute_geometric_duplicates
from core.utils import log
from settings import INTERACTIVE_ATTRIBUTE_REVIEW


_VALIDATE_ATTRIBUTE_MAPPINGS = {}
_VALIDATION_SUMMARY = {
    "fields": {},
    "tema_consistency": {
        "status_counts": Counter(),
        "reason_counts": Counter(),
        "autocorrected_counts": Counter(),
        "unchecked_code_counts": Counter(),
    },
}


def reset_validate_attribute_mappings():
    _VALIDATE_ATTRIBUTE_MAPPINGS.clear()
    _VALIDATION_SUMMARY["fields"].clear()
    _VALIDATION_SUMMARY["tema_consistency"] = {
        "status_counts": Counter(),
        "reason_counts": Counter(),
        "autocorrected_counts": Counter(),
        "unchecked_code_counts": Counter(),
    }


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


def _build_validate_attribute_mapping(gdf, column):
    unique_values = _get_non_empty_unique_text_values(gdf[column])

    if not unique_values:
        return {
            "replacements": {},
            "corrections": [],
            "invalid_values": [],
            "strategy": "empty",
        }

    if has_field_rules(column):
        replacements, corrections, invalid_values = build_field_mapping(column, unique_values)
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


def prepare_validate_shapefile_attribute_mappings(gdf, mapping):
    for column, funcs in mapping.items():
        if "validate_shapefile_attribute" not in funcs:
            continue

        if column not in gdf.columns:
            log(f"Atributo {column} nao encontrado para validacao")
            continue

        if column in _VALIDATE_ATTRIBUTE_MAPPINGS:
            continue

        mapping_result = _build_validate_attribute_mapping(gdf, column)
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


def validate_date_fields(gdf, column):
    gdf[column] = pd.to_datetime(gdf[column], errors="coerce")
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


def _register_tema_consistency_summary(gdf):
    required_columns = {"acm_cod_tema", "acm_nom_tema"}
    if not required_columns.issubset(gdf.columns):
        return

    relation = get_active_rule_profile().get("relations", {}).get("cod_tema_to_nom_tema", {})
    if not relation:
        return

    status_counts = _VALIDATION_SUMMARY["tema_consistency"]["status_counts"]
    reason_counts = _VALIDATION_SUMMARY["tema_consistency"]["reason_counts"]
    autocorrected_counts = _VALIDATION_SUMMARY["tema_consistency"]["autocorrected_counts"]
    unchecked_code_counts = _VALIDATION_SUMMARY["tema_consistency"]["unchecked_code_counts"]

    cod_series = gdf["acm_cod_tema"]
    nom_series = gdf["acm_nom_tema"]

    nom_cache = {
        value: classify_field_value("sdb_nom_tema", value)["normalized_value"]
        for value in nom_series.dropna().drop_duplicates().tolist()
    }
    null_normalized = (
        classify_field_value("sdb_nom_tema", None)["normalized_value"]
        if nom_series.isna().any()
        else None
    )

    normalized_nom_series = nom_series.map(nom_cache)
    if null_normalized is not None:
        normalized_nom_series = normalized_nom_series.where(nom_series.notna(), null_normalized)

    expected_nom_series = cod_series.map(relation)
    unchecked_mask = cod_series.isna() | expected_nom_series.isna()
    consistent_mask = (~unchecked_mask) & (normalized_nom_series == expected_nom_series)
    autocorrected_mask = (~unchecked_mask) & (~consistent_mask)

    unchecked_count = int(unchecked_mask.sum())
    consistent_count = int(consistent_mask.sum())
    autocorrected_count = int(autocorrected_mask.sum())

    if unchecked_count:
        status_counts.update({"unchecked": unchecked_count})
        reason_counts.update({"Codigo do tema fora do dominio configurado.": unchecked_count})
        unchecked_values = cod_series.loc[unchecked_mask].fillna("<NULL>").astype(str)
        unchecked_code_counts.update(unchecked_values.value_counts().to_dict())

    if consistent_count:
        status_counts.update({"consistent": consistent_count})

    if autocorrected_count:
        status_counts.update({"autocorrected": autocorrected_count})
        autocorrected_counts.update(cod_series.loc[autocorrected_mask].value_counts().to_dict())

    corrected_nom_series = nom_series.copy()
    corrected_nom_series.loc[~unchecked_mask] = expected_nom_series.loc[~unchecked_mask]
    gdf["acm_nom_tema"] = corrected_nom_series


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

    consistency = _VALIDATION_SUMMARY["tema_consistency"]
    autocorrected = consistency["status_counts"].get("autocorrected", 0)
    inconsistent = consistency["status_counts"].get("inconsistent", 0)
    unchecked = consistency["status_counts"].get("unchecked", 0)
    if autocorrected or inconsistent or unchecked:
        parts = []
        if autocorrected:
            parts.append(f"{autocorrected} autocorrigido(s) pela relacao cod_tema/nom_tema")
        if inconsistent:
            parts.append(f"{inconsistent} inconsistente(s)")
        if unchecked:
            parts.append(f"{unchecked} nao verificado(s)")
        log(f"Resumo consistencia tema: {', '.join(parts)}")
        for cod_tema, count in consistency["autocorrected_counts"].most_common(5):
            expected_nom_tema = get_active_rule_profile().get("relations", {}).get(
                "cod_tema_to_nom_tema",
                {},
            ).get(cod_tema)
            if expected_nom_tema:
                log(f"  {count}x - Descricao ajustada automaticamente para {cod_tema}: {expected_nom_tema}")
        if unchecked:
            for cod_tema, count in consistency["unchecked_code_counts"].most_common(10):
                log(f"  {count}x - Codigo do tema fora do dominio configurado: {cod_tema}")
        for reason, count in consistency["reason_counts"].most_common(5):
            log(f"  {count}x - {reason}")


def _target_column_name(column):
    if column.startswith("sdb_"):
        return f"acm_{column[4:]}"
    return f"acm_{column}"


def _build_classification_cache(column, source_series):
    cache = {
        value: classify_field_value(column, value)
        for value in source_series.dropna().drop_duplicates().tolist()
    }
    null_result = classify_field_value(column, None) if source_series.isna().any() else None
    return cache, null_result


def _series_from_cache(source_series, cache, null_result, property_name):
    result = source_series.map(
        lambda value: cache[value][property_name] if pd.notna(value) else None
    )
    if null_result is not None:
        result = result.where(source_series.notna(), null_result[property_name])
    return result


def validate_shapefile_attribute(gdf, column):
    if column not in gdf.columns:
        log(f"Atributo {column} nao encontrado")
        return gdf

    target_column = _target_column_name(column)
    source_series = gdf[column]

    if target_column not in gdf.columns:
        gdf[target_column] = source_series

    replacements = _VALIDATE_ATTRIBUTE_MAPPINGS.get(column, {})

    if has_field_rules(column):
        classification_cache, null_result = _build_classification_cache(column, source_series)
        gdf[target_column] = _series_from_cache(
            source_series,
            classification_cache,
            null_result,
            "normalized_value",
        )
        statuses = _series_from_cache(source_series, classification_cache, null_result, "status")
        reasons = _series_from_cache(source_series, classification_cache, null_result, "reason")

        _register_domain_validation_summary(column, statuses.tolist(), reasons.tolist())
        if column == "sdb_nom_tema":
            _register_tema_consistency_summary(gdf)
        return gdf

    if replacements:
        stripped_source = source_series.where(source_series.isna(), source_series.astype(str).str.strip())
        mapped_values = stripped_source.map(replacements)
        gdf[target_column] = mapped_values.where(mapped_values.notna(), stripped_source)

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

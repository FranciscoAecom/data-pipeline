from core.date import parse_date_series
from core.rules.engine import has_field_rules
from core.validation.schema import target_column_name
from core.utils import log
from core.validation.relation_validation import (
    apply_relation_consistency_if_needed,
    build_classification_cache,
    series_from_cache,
)
from core.validation.session import validation_session_or_default
from core.validation.summary import register_domain_validation_summary


def validate_date_fields(gdf, column, **_context):
    gdf[target_column_name(column)] = parse_date_series(gdf[column])
    return gdf


def series_has_changes(source_series, candidate_series):
    same_mask = source_series.eq(candidate_series)
    same_mask = same_mask | (source_series.isna() & candidate_series.isna())
    same_mask = same_mask.fillna(False)
    return not bool(same_mask.all())


def apply_target_column_if_needed(gdf, target_column, source_series, candidate_series):
    if series_has_changes(source_series, candidate_series):
        gdf[target_column] = candidate_series
    elif target_column in gdf.columns:
        gdf = gdf.drop(columns=[target_column])
    return gdf


def validate_shapefile_attribute(gdf, column, rule_profile=None, **_context):
    if column not in gdf.columns:
        log(f"Atributo {column} nao encontrado")
        return gdf

    target_column = target_column_name(column)
    source_series = gdf[column]

    validation_session = validation_session_or_default(
        _context.get("validation_session")
    )
    replacements = validation_session.attribute_mappings.get(column, {})

    if rule_profile is None:
        return gdf

    if has_field_rules(rule_profile, column):
        classification_cache, null_result = build_classification_cache(
            rule_profile,
            column,
            source_series,
        )
        normalized_series = series_from_cache(
            source_series,
            classification_cache,
            null_result,
            "normalized_value",
        )
        statuses = series_from_cache(source_series, classification_cache, null_result, "status")
        reasons = series_from_cache(source_series, classification_cache, null_result, "reason")

        register_domain_validation_summary(
            column,
            statuses.tolist(),
            reasons.tolist(),
            validation_session=validation_session,
        )
        normalized_series = apply_relation_consistency_if_needed(
            gdf,
            column,
            normalized_series,
            rule_profile,
            validation_session=validation_session,
        )
        gdf = apply_target_column_if_needed(
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
        gdf = apply_target_column_if_needed(
            gdf,
            target_column,
            source_series,
            candidate_series,
        )

    return gdf


__all__ = [
    "apply_target_column_if_needed",
    "series_has_changes",
    "validate_date_fields",
    "validate_shapefile_attribute",
]

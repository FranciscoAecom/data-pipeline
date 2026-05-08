from core.validation.attribute_mapping import (
    build_fuzzy_mapping as _build_fuzzy_mapping,
    build_validate_attribute_mapping as _build_validate_attribute_mapping,
    get_non_empty_unique_text_values as _get_non_empty_unique_text_values,
    has_optional_function as _has_optional_function,
    normalize_for_compare as _normalize_for_compare,
    prepare_validate_shapefile_attribute_mappings,
)
from core.validation.domain_validation import (
    apply_target_column_if_needed as _apply_target_column_if_needed,
    series_has_changes as _series_has_changes,
    validate_date_fields,
    validate_shapefile_attribute,
)
from core.validation.duplicates import (
    check_attribute_duplicates,
    check_duplicates,
    get_attribute_duplicate_mask,
    get_attribute_duplicate_records,
    get_duplicate_columns as _get_duplicate_columns,
)
from core.validation.relation_validation import (
    apply_relation_consistency_if_needed as _apply_relation_consistency_if_needed,
    build_classification_cache as _build_classification_cache,
    get_effective_domain_series as _get_effective_domain_series,
    resolve_relation_columns as _resolve_relation_columns,
    series_from_cache as _series_from_cache,
)
from core.validation.session import (
    ValidationSession,
    reset_validate_attribute_mappings,
    validation_session_or_default as _validation_session_or_default,
)
from core.validation.summary import (
    field_summary_entry as _field_summary_entry,
    log_validation_summary,
    register_domain_validation_summary as _register_domain_validation_summary,
    relation_summary_entry as _relation_summary_entry,
)


__all__ = [
    "ValidationSession",
    "check_attribute_duplicates",
    "check_duplicates",
    "get_attribute_duplicate_mask",
    "get_attribute_duplicate_records",
    "log_validation_summary",
    "prepare_validate_shapefile_attribute_mappings",
    "reset_validate_attribute_mappings",
    "validate_date_fields",
    "validate_shapefile_attribute",
]

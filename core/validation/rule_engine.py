from settings import RULES_BASE
from core.rules import engine as _engine
from core.rules.domain import (
    build_field_mapping,
    classify_field_value,
    get_auto_function_mapping,
    has_field_rules,
)
from core.rules.normalization import (
    RuleProfileResolutionError,
    normalize_profile_name,
    normalize_rule_text,
)
from core.rules.validation import (
    validate_rule_profile,
    validate_rule_profile_semantics,
    validate_rule_profile_structure,
)


def _sync_engine_settings():
    _engine.RULES_BASE = RULES_BASE


def profile_exists(profile_name):
    _sync_engine_settings()
    return _engine.profile_exists(profile_name)


def list_rule_profiles():
    _sync_engine_settings()
    return _engine.list_rule_profiles()


def expected_rule_profile_name(theme_folder):
    _sync_engine_settings()
    return _engine.expected_rule_profile_name(theme_folder)


def list_duplicate_rule_profile_stems():
    _sync_engine_settings()
    return _engine.list_duplicate_rule_profile_stems()


def find_rule_profile_by_theme_folder(theme_folder):
    _sync_engine_settings()
    return _engine.find_rule_profile_by_theme_folder(theme_folder)


def load_rule_profile(profile_name, optional_functions=None):
    _sync_engine_settings()
    return _engine.load_rule_profile(
        profile_name,
        optional_functions=optional_functions,
    )


def get_rule_profile_project_name(profile_name):
    _sync_engine_settings()
    return _engine.get_rule_profile_project_name(profile_name)


def invalidate_rule_profile_cache(profile_name=None):
    _sync_engine_settings()
    return _engine.invalidate_rule_profile_cache(profile_name)


def save_rule_profile(profile_name, profile):
    _sync_engine_settings()
    return _engine.save_rule_profile(profile_name, profile)

from settings import RULES_BASE
from core.validation import rule_loader as _rule_loader
from core.validation.domain_rules import (
    build_field_mapping,
    classify_field_value,
    get_auto_function_mapping,
    has_field_rules,
)
from core.validation.rule_normalization import (
    RuleProfileResolutionError,
    normalize_profile_name,
    normalize_rule_text,
)
from core.validation.rule_validation import validate_rule_profile


def _sync_loader_settings():
    if _rule_loader.RULES_BASE != RULES_BASE:
        _rule_loader.invalidate_rule_profile_cache()
    _rule_loader.RULES_BASE = RULES_BASE


def profile_exists(profile_name):
    _sync_loader_settings()
    return _rule_loader.profile_exists(profile_name)


def list_rule_profiles():
    _sync_loader_settings()
    return _rule_loader.list_rule_profiles()


def expected_rule_profile_name(theme_folder):
    _sync_loader_settings()
    return _rule_loader.expected_rule_profile_name(theme_folder)


def list_duplicate_rule_profile_stems():
    _sync_loader_settings()
    return _rule_loader.list_duplicate_rule_profile_stems()


def find_rule_profile_by_theme_folder(theme_folder):
    _sync_loader_settings()
    return _rule_loader.find_rule_profile_by_theme_folder(theme_folder)


def load_rule_profile(profile_name, optional_functions=None):
    _sync_loader_settings()
    return _rule_loader.load_rule_profile(
        profile_name,
        optional_functions=optional_functions,
    )


def get_rule_profile_project_name(profile_name):
    _sync_loader_settings()
    return _rule_loader.get_rule_profile_project_name(profile_name)


def invalidate_rule_profile_cache(profile_name=None):
    _sync_loader_settings()
    return _rule_loader.invalidate_rule_profile_cache(profile_name)


def save_rule_profile(profile_name, profile):
    _sync_loader_settings()
    return _rule_loader.save_rule_profile(profile_name, profile)

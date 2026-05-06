from settings import RULES_BASE
from core.validation.rule_repository import RuleRepository, is_auxiliary_rule_path


_REPOSITORY = None


def get_repository():
    global _REPOSITORY
    if _REPOSITORY is None or str(_REPOSITORY.rules_base) != str(RULES_BASE):
        _REPOSITORY = RuleRepository(RULES_BASE)
    return _REPOSITORY


def profile_path(profile_name):
    return get_repository().profile_path(profile_name)


def legacy_profile_path(profile_name):
    return get_repository().legacy_profile_path(profile_name)


def modular_profile_path(profile_name):
    return get_repository().modular_profile_path(profile_name)


def profile_exists(profile_name):
    return get_repository().profile_exists(profile_name)


def load_json_file(path):
    return get_repository().load_json_file(path)


def write_json_file(path, data):
    return get_repository().write_json_file(path, data)


def read_component(profile_dir, component_file):
    return get_repository().read_component(profile_dir, component_file)


def load_modular_profile(profile_dir):
    return get_repository().load_modular_profile(profile_dir)


def load_profile_data(profile_name):
    return get_repository().load_profile_data(profile_name)


def load_profile(profile_name):
    return get_repository().load_profile(profile_name)


def list_rule_profiles():
    return get_repository().list_rule_profiles()


def expected_rule_profile_name(theme_folder):
    return get_repository().expected_rule_profile_name(theme_folder)


def list_duplicate_rule_profile_stems():
    return get_repository().list_duplicate_rule_profile_stems()


def find_rule_profile_by_theme_folder(theme_folder):
    return get_repository().find_rule_profile_by_theme_folder(theme_folder)


def load_rule_profile(profile_name, optional_functions=None):
    return get_repository().load_rule_profile(
        profile_name,
        optional_functions=optional_functions,
    )


def get_rule_profile_project_name(profile_name):
    return get_repository().get_rule_profile_project_name(profile_name)


def invalidate_rule_profile_cache(profile_name=None):
    return get_repository().invalidate(profile_name)


def save_rule_profile(profile_name, profile):
    return get_repository().save_rule_profile(profile_name, profile)


def save_modular_profile(profile_dir, profile):
    from core.validation.rule_models import RuleProfileModel

    profile_model = (
        profile
        if isinstance(profile, RuleProfileModel)
        else RuleProfileModel.from_dict(profile)
    )
    return get_repository().save_modular_profile(profile_dir, profile_model)

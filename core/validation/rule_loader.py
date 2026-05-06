import json
from functools import lru_cache
from pathlib import Path

from projects.configs import resolve_project_name
from settings import DEFAULT_RULE_PROFILE, RULES_BASE
from core.validation.rule_normalization import (
    RuleProfileResolutionError,
    normalize_profile_name,
)
from core.validation.rule_validation import validate_modular_components, validate_rule_profile


RULE_CACHE = {}
PROFILE_COMPONENT_FILES = {
    "profile.json",
    "input_schema.json",
    "domains.json",
    "relations.json",
    "pipeline.json",
}


def profile_path(profile_name):
    normalized_profile_name = normalize_profile_name(profile_name)
    if not normalized_profile_name:
        return Path(RULES_BASE)
    legacy_path = legacy_profile_path(normalized_profile_name)
    if legacy_path.exists():
        return legacy_path
    return modular_profile_path(normalized_profile_name)


def legacy_profile_path(profile_name):
    normalized_profile_name = normalize_profile_name(profile_name)
    if not normalized_profile_name:
        return Path(RULES_BASE)
    return Path(RULES_BASE) / Path(f"{normalized_profile_name}.json")


def modular_profile_path(profile_name):
    normalized_profile_name = normalize_profile_name(profile_name)
    if not normalized_profile_name:
        return Path(RULES_BASE)
    return Path(RULES_BASE) / Path(normalized_profile_name)


def profile_exists(profile_name):
    path = profile_path(profile_name)
    if path.is_dir():
        return (path / "profile.json").exists()
    return path.exists()


def load_json_file(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def write_json_file(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def read_component(profile_dir, component_file):
    path = profile_dir / component_file
    if not path.exists():
        return {}
    data = load_json_file(path)
    if not isinstance(data, dict):
        raise ValueError(f"Componente de perfil invalido: {path}")
    return data


def load_modular_profile(profile_dir):
    if not profile_dir.is_dir():
        raise FileNotFoundError(f"Perfil de regras nao encontrado: {profile_dir}")

    profile_path_value = profile_dir / "profile.json"
    if not profile_path_value.exists():
        raise FileNotFoundError(f"Perfil modular sem profile.json: {profile_dir}")

    profile = read_component(profile_dir, "profile.json")
    input_schema = read_component(profile_dir, "input_schema.json")
    domains = read_component(profile_dir, "domains.json")
    relations = read_component(profile_dir, "relations.json")
    pipeline = read_component(profile_dir, "pipeline.json")
    normalized_profile_name = str(profile_dir.relative_to(RULES_BASE)).replace("\\", "/")
    validate_modular_components(
        profile,
        input_schema,
        domains,
        relations,
        pipeline,
        normalized_profile_name,
    )

    if input_schema:
        profile["input_schema"] = input_schema
    profile["fields"] = domains.get("fields", domains)
    profile["relations"] = relations.get("relations", relations)
    profile["auto_functions"] = pipeline.get("auto_functions", pipeline)
    return profile


def load_profile_data(profile_name):
    path = profile_path(profile_name)
    if path.is_dir():
        return load_modular_profile(path)
    if not path.exists():
        raise FileNotFoundError(f"Perfil de regras nao encontrado: {path}")
    return load_json_file(path)


def load_profile(profile_name):
    normalized_profile_name = normalize_profile_name(profile_name)
    if normalized_profile_name in RULE_CACHE:
        return RULE_CACHE[normalized_profile_name]

    profile = load_profile_data(normalized_profile_name)
    validate_rule_profile(profile, normalized_profile_name)
    RULE_CACHE[normalized_profile_name] = profile
    return profile


@lru_cache(maxsize=1)
def list_rule_profiles():
    base_path = Path(RULES_BASE)
    if not base_path.exists():
        return []
    profiles = set()

    for profile_dir in base_path.rglob("profile.json"):
        relative_parent = profile_dir.parent.relative_to(base_path)
        if is_auxiliary_rule_path(relative_parent):
            continue
        profiles.add(str(relative_parent).replace("\\", "/"))

    for path in base_path.rglob("*.json"):
        relative_path = path.relative_to(base_path)
        if is_auxiliary_rule_path(relative_path):
            continue
        if path.name in PROFILE_COMPONENT_FILES and (path.parent / "profile.json").exists():
            continue
        profiles.add(str(relative_path.with_suffix("")).replace("\\", "/"))

    return sorted(profiles)


def is_auxiliary_rule_path(relative_path):
    return any(str(part).startswith("_") for part in Path(relative_path).parts)


def expected_rule_profile_name(theme_folder):
    normalized_theme_folder = normalize_profile_name(theme_folder)
    if not normalized_theme_folder:
        return None

    project_name = resolve_project_name(normalized_theme_folder)
    if project_name == DEFAULT_RULE_PROFILE:
        return normalized_theme_folder
    return f"{project_name}/{normalized_theme_folder}"


def list_duplicate_rule_profile_stems():
    profiles_by_stem = {}
    for profile_name in list_rule_profiles():
        profile_stem = normalize_profile_name(profile_name).rsplit("/", 1)[-1]
        profiles_by_stem.setdefault(profile_stem, []).append(profile_name)

    return {
        profile_stem: profiles
        for profile_stem, profiles in profiles_by_stem.items()
        if len(profiles) > 1
    }


@lru_cache(maxsize=None)
def find_rule_profile_by_theme_folder(theme_folder):
    normalized_theme_folder = normalize_profile_name(theme_folder)
    if not normalized_theme_folder:
        return None

    expected_profile_name = expected_rule_profile_name(normalized_theme_folder)
    exact_matches = []
    stem_matches = []

    for profile_name in list_rule_profiles():
        normalized_profile_name = normalize_profile_name(profile_name)
        profile_stem = normalized_profile_name.rsplit("/", 1)[-1]

        if normalized_profile_name == normalized_theme_folder:
            exact_matches.append(profile_name)
        elif profile_stem == normalized_theme_folder:
            stem_matches.append(profile_name)

    if exact_matches:
        return exact_matches[0]

    if expected_profile_name in stem_matches:
        return expected_profile_name

    if stem_matches:
        preferred_project = "_".join(normalized_theme_folder.split("_")[:-1])
        for profile_name in stem_matches:
            if preferred_project and profile_name.startswith(f"{preferred_project}/"):
                return profile_name
        if len(stem_matches) > 1:
            matches = ", ".join(stem_matches)
            raise RuleProfileResolutionError(
                "Mais de um perfil de regras corresponde ao "
                f"theme_folder '{theme_folder}': {matches}. "
                f"Perfil esperado pelo projeto: {expected_profile_name}."
            )
        return stem_matches[0]

    return None


def load_rule_profile(profile_name, optional_functions=None):
    normalized_profile_name = normalize_profile_name(profile_name)
    if optional_functions is None:
        return load_profile(normalized_profile_name)

    profile = load_profile_data(normalized_profile_name)
    validate_rule_profile(
        profile,
        normalized_profile_name,
        optional_functions=optional_functions,
    )
    RULE_CACHE[normalized_profile_name] = profile
    return profile


def get_rule_profile_project_name(profile_name):
    profile = load_profile(profile_name)
    project_name = profile.get("project_name")
    if not isinstance(project_name, str):
        return ""
    return project_name.strip()


def invalidate_rule_profile_cache(profile_name=None):
    if profile_name is None:
        RULE_CACHE.clear()
        list_rule_profiles.cache_clear()
        find_rule_profile_by_theme_folder.cache_clear()
        return

    RULE_CACHE.pop(normalize_profile_name(profile_name), None)
    list_rule_profiles.cache_clear()
    find_rule_profile_by_theme_folder.cache_clear()


def save_rule_profile(profile_name, profile):
    normalized_profile_name = normalize_profile_name(profile_name)
    if not normalized_profile_name:
        raise ValueError("Nome do perfil de regras invalido.")

    validate_rule_profile(profile, normalized_profile_name)

    modular_path = modular_profile_path(normalized_profile_name)
    legacy_path = legacy_profile_path(normalized_profile_name)
    if modular_path.is_dir() or (modular_path / "profile.json").exists():
        path = save_modular_profile(modular_path, profile)
    else:
        path = legacy_path
        write_json_file(path, profile)

    invalidate_rule_profile_cache(normalized_profile_name)
    return str(path)


def save_modular_profile(profile_dir, profile):
    profile_data = {
        key: value
        for key, value in profile.items()
        if key not in {"input_schema", "fields", "relations", "auto_functions"}
    }
    input_schema_data = profile.get("input_schema", {})
    domains_data = {"fields": profile.get("fields", {})}
    relations_data = {"relations": profile.get("relations", {})}
    pipeline_data = {"auto_functions": profile.get("auto_functions", {})}

    validate_modular_components(
        profile_data,
        input_schema_data,
        domains_data,
        relations_data,
        pipeline_data,
        str(profile_dir.relative_to(RULES_BASE)).replace("\\", "/"),
    )

    write_json_file(profile_dir / "profile.json", profile_data)
    write_json_file(profile_dir / "input_schema.json", input_schema_data)
    write_json_file(profile_dir / "domains.json", domains_data)
    write_json_file(profile_dir / "relations.json", relations_data)
    write_json_file(profile_dir / "pipeline.json", pipeline_data)
    return profile_dir

import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

from settings import DEFAULT_RULE_PROFILE, RULES_BASE


_ACTIVE_RULE_PROFILE = DEFAULT_RULE_PROFILE
_RULE_CACHE = {}


def normalize_rule_text(value):
    if not isinstance(value, str):
        return ""
    text = value.strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if ord(ch) < 128)
    text = " ".join(text.split())
    return text.upper()


def normalize_profile_name(value):
    if not isinstance(value, str):
        return ""
    raw_text = value.strip().replace("\\", "/")
    parts = [part for part in raw_text.split("/") if part.strip()]
    normalized_parts = []

    for part in parts:
        text = part.strip().lower()
        text = re.sub(r"\s+", "_", text)
        text = re.sub(r"_+", "_", text)
        text = text.strip("_")
        if text:
            normalized_parts.append(text)

    return "/".join(normalized_parts)


def _profile_path(profile_name):
    normalized_profile_name = normalize_profile_name(profile_name)
    if not normalized_profile_name:
        return Path(RULES_BASE)
    return Path(RULES_BASE) / Path(f"{normalized_profile_name}.json")


def profile_exists(profile_name):
    return _profile_path(profile_name).exists()


def _load_profile(profile_name):
    normalized_profile_name = normalize_profile_name(profile_name)
    if normalized_profile_name in _RULE_CACHE:
        return _RULE_CACHE[normalized_profile_name]

    path = _profile_path(normalized_profile_name)
    if not path.exists():
        raise FileNotFoundError(f"Perfil de regras nao encontrado: {path}")

    with open(path, "r", encoding="utf-8-sig") as f:
        profile = json.load(f)

    _RULE_CACHE[normalized_profile_name] = profile
    return profile


@lru_cache(maxsize=1)
def list_rule_profiles():
    base_path = Path(RULES_BASE)
    if not base_path.exists():
        return []
    return sorted(
        str(path.relative_to(base_path).with_suffix("")).replace("\\", "/")
        for path in base_path.rglob("*.json")
    )


@lru_cache(maxsize=None)
def find_rule_profile_by_theme_folder(theme_folder):
    normalized_theme_folder = normalize_profile_name(theme_folder)
    if not normalized_theme_folder:
        return None

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

    if stem_matches:
        preferred_project = "_".join(normalized_theme_folder.split("_")[:-1])
        for profile_name in stem_matches:
            if preferred_project and profile_name.startswith(f"{preferred_project}/"):
                return profile_name
        return stem_matches[0]

    return None


def set_active_rule_profile(profile_name):
    global _ACTIVE_RULE_PROFILE
    normalized_profile_name = normalize_profile_name(profile_name)
    _load_profile(normalized_profile_name)
    _ACTIVE_RULE_PROFILE = normalized_profile_name


def get_active_rule_profile_name():
    return _ACTIVE_RULE_PROFILE


def get_active_rule_profile():
    return _load_profile(_ACTIVE_RULE_PROFILE)


def get_auto_function_mapping():
    profile = get_active_rule_profile()
    auto_functions = profile.get("auto_functions", {})
    return {
        column: list(functions)
        for column, functions in auto_functions.items()
    }


def _get_field_entry(column):
    profile = get_active_rule_profile()
    fields = profile.get("fields", {})
    return fields.get(column)


def has_field_rules(column):
    return _get_field_entry(column) is not None


def _build_normalized_lookup(values):
    return {normalize_rule_text(value): value for value in values}


def classify_field_value(column, value):
    field_rules = _get_field_entry(column)

    if field_rules is None:
        return {
            "normalized_value": value,
            "status": "unconfigured",
            "reason": "",
        }

    if value is None:
        return {
            "normalized_value": value,
            "status": "empty",
            "reason": "Valor nulo.",
        }

    text = str(value).strip()
    if not text:
        return {
            "normalized_value": value,
            "status": "empty",
            "reason": "Valor vazio.",
        }

    accepted_values = field_rules.get("accepted_values", [])
    aliases = field_rules.get("aliases", {})
    accepted_lookup = _build_normalized_lookup(accepted_values)
    alias_lookup = _build_normalized_lookup(aliases.keys())
    normalized = normalize_rule_text(text)

    if normalized in accepted_lookup:
        canonical = accepted_lookup[normalized]
        return {
            "normalized_value": canonical,
            "status": "valid",
            "reason": "",
        }

    if normalized in alias_lookup:
        alias_value = alias_lookup[normalized]
        canonical = aliases[alias_value]
        return {
            "normalized_value": canonical,
            "status": "normalized",
            "reason": f"Valor normalizado por alias: {text} -> {canonical}",
        }

    return {
        "normalized_value": text,
        "status": "invalid",
        "reason": "Valor fora do dominio configurado.",
    }


def build_field_mapping(column, values):
    replacements = {}
    corrections = []
    invalid_values = []

    for value in values:
        result = classify_field_value(column, value)
        replacements[value] = result["normalized_value"]
        if result["status"] == "normalized":
            corrections.append((value, result["normalized_value"]))
        elif result["status"] == "invalid":
            invalid_values.append(value)

    return replacements, corrections, invalid_values


def invalidate_rule_profile_cache(profile_name=None):
    if profile_name is None:
        _RULE_CACHE.clear()
        list_rule_profiles.cache_clear()
        find_rule_profile_by_theme_folder.cache_clear()
        return

    _RULE_CACHE.pop(normalize_profile_name(profile_name), None)
    list_rule_profiles.cache_clear()
    find_rule_profile_by_theme_folder.cache_clear()


def save_rule_profile(profile_name, profile):
    normalized_profile_name = normalize_profile_name(profile_name)
    if not normalized_profile_name:
        raise ValueError("Nome do perfil de regras invalido.")

    path = _profile_path(normalized_profile_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False, indent=2)
        f.write("\n")

    invalidate_rule_profile_cache(normalized_profile_name)
    return str(path)

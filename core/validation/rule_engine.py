import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

from projects.configs import PROJECT_CONFIGS
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

    validate_rule_profile(profile, normalized_profile_name)
    _RULE_CACHE[normalized_profile_name] = profile
    return profile


def _validate_profile_name_entry(profile, normalized_profile_name, errors):
    profile_name = profile.get("profile_name")
    if profile_name is None:
        return

    if not isinstance(profile_name, str) or not profile_name.strip():
        errors.append("Campo 'profile_name' deve ser uma string nao vazia.")


def _validate_project_name_entry(profile, errors):
    project_name = profile.get("project_name")
    if not isinstance(project_name, str) or not project_name.strip():
        errors.append("Campo 'project_name' deve ser uma string nao vazia.")
        return DEFAULT_RULE_PROFILE

    normalized_project_name = project_name.strip()
    valid_projects = set(PROJECT_CONFIGS.keys()) | {DEFAULT_RULE_PROFILE}
    if normalized_project_name not in valid_projects:
        valid_list = ", ".join(sorted(valid_projects))
        errors.append(
            f"Campo 'project_name' invalido: {normalized_project_name}. Valores aceitos: {valid_list}."
        )
    return normalized_project_name


def _validate_fields_entry(fields, errors):
    if fields is None:
        return {}
    if not isinstance(fields, dict):
        errors.append("Campo 'fields' deve ser um objeto JSON.")
        return {}

    for field_name, field_rules in fields.items():
        if not isinstance(field_name, str) or not field_name.strip():
            errors.append("Chaves de 'fields' devem ser strings nao vazias.")
            continue

        if not isinstance(field_rules, dict):
            errors.append(f"Configuracao de field '{field_name}' deve ser um objeto.")
            continue

        accepted_values = field_rules.get("accepted_values", [])
        aliases = field_rules.get("aliases", {})

        if accepted_values is None:
            accepted_values = []
        if not isinstance(accepted_values, list):
            errors.append(f"'accepted_values' de '{field_name}' deve ser uma lista.")
            accepted_values = []

        if not all(isinstance(value, str) for value in accepted_values):
            errors.append(
                f"'accepted_values' de '{field_name}' deve conter apenas strings."
            )

        if aliases is None:
            aliases = {}
        if not isinstance(aliases, dict):
            errors.append(f"'aliases' de '{field_name}' deve ser um objeto.")
            aliases = {}

        if not all(isinstance(key, str) for key in aliases.keys()):
            errors.append(f"'aliases' de '{field_name}' deve usar chaves string.")
        if not all(isinstance(value, str) for value in aliases.values()):
            errors.append(f"'aliases' de '{field_name}' deve usar valores string.")

        accepted_values_set = set(accepted_values)
        for alias, canonical in aliases.items():
            if not isinstance(alias, str) or not isinstance(canonical, str):
                continue
            if canonical not in accepted_values_set:
                errors.append(
                    f"Alias '{alias}' de '{field_name}' aponta para valor fora de 'accepted_values': {canonical}."
                )

    return fields


def _validate_relations_entry(relations, fields, errors):
    if relations is None:
        return
    if not isinstance(relations, dict):
        errors.append("Campo 'relations' deve ser um objeto JSON.")
        return

    known_fields = set(fields.keys())
    for relation_name, relation_mapping in relations.items():
        if not isinstance(relation_name, str) or not relation_name.strip():
            errors.append("Chaves de 'relations' devem ser strings nao vazias.")
            continue

        if not isinstance(relation_mapping, dict):
            errors.append(f"Relacao '{relation_name}' deve ser um objeto.")
            continue

        if "_to_" not in relation_name:
            errors.append(
                f"Relacao '{relation_name}' deve seguir o padrao '<origem>_to_<destino>'."
            )
        else:
            source_token, target_token = relation_name.split("_to_", 1)
            source_field = f"sdb_{source_token}"
            target_field = f"sdb_{target_token}"
            if known_fields and source_field not in known_fields and source_token not in known_fields:
                errors.append(
                    f"Relacao '{relation_name}' referencia campo de origem nao configurado: {source_field}."
                )
            if known_fields and target_field not in known_fields and target_token not in known_fields:
                errors.append(
                    f"Relacao '{relation_name}' referencia campo de destino nao configurado: {target_field}."
                )

        for source_value, target_value in relation_mapping.items():
            if not isinstance(source_value, str) or not isinstance(target_value, str):
                errors.append(
                    f"Relacao '{relation_name}' deve conter apenas pares string -> string."
                )
                break


def _validate_auto_functions_entry(auto_functions, fields, project_name, errors):
    from core.optional_functions import is_optional_function_registered

    if auto_functions is None:
        return
    if not isinstance(auto_functions, dict):
        errors.append("Campo 'auto_functions' deve ser um objeto JSON.")
        return

    known_fields = set(fields.keys())
    for column, functions in auto_functions.items():
        if not isinstance(column, str) or not column.strip():
            errors.append("Chaves de 'auto_functions' devem ser strings nao vazias.")
            continue

        if not isinstance(functions, list) or not functions:
            errors.append(
                f"'auto_functions.{column}' deve ser uma lista nao vazia de funcoes."
            )
            continue

        for func_name in functions:
            if not isinstance(func_name, str) or not func_name.strip():
                errors.append(
                    f"'auto_functions.{column}' deve conter apenas nomes de funcao string."
                )
                continue

            if not is_optional_function_registered(func_name, project_name=project_name):
                errors.append(
                    f"Funcao '{func_name}' em 'auto_functions.{column}' nao esta registrada para o projeto '{project_name}'."
                )
                continue

            if (
                func_name == "validate_shapefile_attribute"
                and known_fields
                and column not in known_fields
            ):
                errors.append(
                    f"Campo '{column}' usa 'validate_shapefile_attribute' mas nao possui configuracao correspondente em 'fields'."
                )


def validate_rule_profile(profile, profile_name):
    normalized_profile_name = normalize_profile_name(profile_name)
    if not isinstance(profile, dict):
        raise ValueError(
            f"Perfil de regras invalido '{normalized_profile_name}': o conteudo deve ser um objeto JSON."
        )

    errors = []
    _validate_profile_name_entry(profile, normalized_profile_name, errors)
    project_name = _validate_project_name_entry(profile, errors)
    fields = _validate_fields_entry(profile.get("fields", {}), errors)
    _validate_relations_entry(profile.get("relations", {}), fields, errors)
    _validate_auto_functions_entry(
        profile.get("auto_functions", {}),
        fields,
        project_name,
        errors,
    )

    if errors:
        message = "\n".join(f"- {error}" for error in errors)
        raise ValueError(
            f"Perfil de regras invalido '{normalized_profile_name}':\n{message}"
        )


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


def get_rule_profile_project_name(profile_name):
    profile = _load_profile(profile_name)
    project_name = profile.get("project_name")
    if not isinstance(project_name, str):
        return ""
    return project_name.strip()


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

    validate_rule_profile(profile, normalized_profile_name)

    path = _profile_path(normalized_profile_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False, indent=2)
        f.write("\n")

    invalidate_rule_profile_cache(normalized_profile_name)
    return str(path)

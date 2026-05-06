import json
from functools import lru_cache
from pathlib import Path

from projects.configs import resolve_project_name
from settings import DEFAULT_RULE_PROFILE
from core.rules.constants import (
    DOMAINS_COMPONENT,
    INPUT_SCHEMA_COMPONENT,
    PIPELINE_COMPONENT,
    PROFILE_COMPONENT,
    PROFILE_COMPONENT_FILES,
    RELATIONS_COMPONENT,
)
from core.rules.models import RuleProfileModel
from core.rules.normalization import (
    RuleProfileResolutionError,
    normalize_profile_name,
)
from core.rules.validation import validate_modular_components, validate_rule_profile


class RuleRepository:
    def __init__(self, rules_base):
        self.rules_base = Path(rules_base)
        self._cache = {}

    def profile_path(self, profile_name):
        normalized_profile_name = normalize_profile_name(profile_name)
        if not normalized_profile_name:
            return self.rules_base
        legacy_path = self.legacy_profile_path(normalized_profile_name)
        if legacy_path.exists():
            return legacy_path
        return self.modular_profile_path(normalized_profile_name)

    def legacy_profile_path(self, profile_name):
        normalized_profile_name = normalize_profile_name(profile_name)
        if not normalized_profile_name:
            return self.rules_base
        return self.rules_base / Path(f"{normalized_profile_name}.json")

    def modular_profile_path(self, profile_name):
        normalized_profile_name = normalize_profile_name(profile_name)
        if not normalized_profile_name:
            return self.rules_base
        return self.rules_base / Path(normalized_profile_name)

    def profile_exists(self, profile_name):
        path = self.profile_path(profile_name)
        if path.is_dir():
            return (path / PROFILE_COMPONENT).exists()
        return path.exists()

    def load_json_file(self, path):
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)

    def write_json_file(self, path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")

    def read_component(self, profile_dir, component_file):
        path = profile_dir / component_file
        if not path.exists():
            return {}
        data = self.load_json_file(path)
        if not isinstance(data, dict):
            raise ValueError(f"Componente de perfil invalido: {path}")
        return data

    def load_modular_profile(self, profile_dir):
        if not profile_dir.is_dir():
            raise FileNotFoundError(f"Perfil de regras nao encontrado: {profile_dir}")

        profile_path_value = profile_dir / PROFILE_COMPONENT
        if not profile_path_value.exists():
            raise FileNotFoundError(f"Perfil modular sem profile.json: {profile_dir}")

        profile = self.read_component(profile_dir, PROFILE_COMPONENT)
        input_schema = self.read_component(profile_dir, INPUT_SCHEMA_COMPONENT)
        domains = self.read_component(profile_dir, DOMAINS_COMPONENT)
        relations = self.read_component(profile_dir, RELATIONS_COMPONENT)
        pipeline = self.read_component(profile_dir, PIPELINE_COMPONENT)
        normalized_profile_name = str(profile_dir.relative_to(self.rules_base)).replace("\\", "/")
        validate_modular_components(
            profile,
            input_schema,
            domains,
            relations,
            pipeline,
            normalized_profile_name,
        )

        return RuleProfileModel.from_components(
            profile,
            input_schema,
            domains,
            relations,
            pipeline,
        ).to_dict(include_empty_input_schema=bool(input_schema))

    def load_profile_data(self, profile_name):
        path = self.profile_path(profile_name)
        if path.is_dir():
            return self.load_modular_profile(path)
        if not path.exists():
            raise FileNotFoundError(f"Perfil de regras nao encontrado: {path}")
        profile = self.load_json_file(path)
        return RuleProfileModel.from_dict(profile).to_dict(
            include_empty_input_schema="input_schema" in profile
        )

    def load_profile(self, profile_name):
        normalized_profile_name = normalize_profile_name(profile_name)
        if normalized_profile_name in self._cache:
            return self._cache[normalized_profile_name]

        profile = self.load_profile_data(normalized_profile_name)
        validate_rule_profile(profile, normalized_profile_name)
        self._cache[normalized_profile_name] = profile
        return profile

    @lru_cache(maxsize=1)
    def list_rule_profiles(self):
        if not self.rules_base.exists():
            return []
        profiles = set()

        for profile_dir in self.rules_base.rglob(PROFILE_COMPONENT):
            relative_parent = profile_dir.parent.relative_to(self.rules_base)
            if is_auxiliary_rule_path(relative_parent):
                continue
            profiles.add(str(relative_parent).replace("\\", "/"))

        for path in self.rules_base.rglob("*.json"):
            relative_path = path.relative_to(self.rules_base)
            if is_auxiliary_rule_path(relative_path):
                continue
            if path.name in PROFILE_COMPONENT_FILES and (path.parent / PROFILE_COMPONENT).exists():
                continue
            profiles.add(str(relative_path.with_suffix("")).replace("\\", "/"))

        return sorted(profiles)

    def expected_rule_profile_name(self, theme_folder):
        normalized_theme_folder = normalize_profile_name(theme_folder)
        if not normalized_theme_folder:
            return None

        project_name = resolve_project_name(normalized_theme_folder)
        if project_name == DEFAULT_RULE_PROFILE:
            return normalized_theme_folder
        return f"{project_name}/{normalized_theme_folder}"

    def list_duplicate_rule_profile_stems(self):
        profiles_by_stem = {}
        for profile_name in self.list_rule_profiles():
            profile_stem = normalize_profile_name(profile_name).rsplit("/", 1)[-1]
            profiles_by_stem.setdefault(profile_stem, []).append(profile_name)

        return {
            profile_stem: profiles
            for profile_stem, profiles in profiles_by_stem.items()
            if len(profiles) > 1
        }

    @lru_cache(maxsize=None)
    def find_rule_profile_by_theme_folder(self, theme_folder):
        normalized_theme_folder = normalize_profile_name(theme_folder)
        if not normalized_theme_folder:
            return None

        expected_profile_name = self.expected_rule_profile_name(normalized_theme_folder)
        exact_matches = []
        stem_matches = []

        for profile_name in self.list_rule_profiles():
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

    def load_rule_profile(self, profile_name, optional_functions=None):
        normalized_profile_name = normalize_profile_name(profile_name)
        if optional_functions is None:
            return self.load_profile(normalized_profile_name)

        profile = self.load_profile_data(normalized_profile_name)
        validate_rule_profile(
            profile,
            normalized_profile_name,
            optional_functions=optional_functions,
        )
        self._cache[normalized_profile_name] = profile
        return profile

    def get_rule_profile_project_name(self, profile_name):
        profile = self.load_profile(profile_name)
        project_name = profile.get("project_name")
        if not isinstance(project_name, str):
            return ""
        return project_name.strip()

    def invalidate(self, profile_name=None):
        if profile_name is None:
            self._cache.clear()
            self.list_rule_profiles.cache_clear()
            self.find_rule_profile_by_theme_folder.cache_clear()
            return

        self._cache.pop(normalize_profile_name(profile_name), None)
        self.list_rule_profiles.cache_clear()
        self.find_rule_profile_by_theme_folder.cache_clear()

    def save_rule_profile(self, profile_name, profile):
        normalized_profile_name = normalize_profile_name(profile_name)
        if not normalized_profile_name:
            raise ValueError("Nome do perfil de regras invalido.")

        validate_rule_profile(profile, normalized_profile_name)

        modular_path = self.modular_profile_path(normalized_profile_name)
        legacy_path = self.legacy_profile_path(normalized_profile_name)
        profile_model = RuleProfileModel.from_dict(profile)
        if modular_path.is_dir() or (modular_path / PROFILE_COMPONENT).exists():
            path = self.save_modular_profile(modular_path, profile_model)
        else:
            path = legacy_path
            self.write_json_file(path, profile_model.to_dict(
                include_empty_input_schema="input_schema" in profile
            ))

        self.invalidate(normalized_profile_name)
        return str(path)

    def save_modular_profile(self, profile_dir, profile_model):
        profile_data, input_schema_data, domains_data, relations_data, pipeline_data = (
            profile_model.to_components()
        )

        validate_modular_components(
            profile_data,
            input_schema_data,
            domains_data,
            relations_data,
            pipeline_data,
            str(profile_dir.relative_to(self.rules_base)).replace("\\", "/"),
        )

        self.write_json_file(profile_dir / PROFILE_COMPONENT, profile_data)
        self.write_json_file(profile_dir / INPUT_SCHEMA_COMPONENT, input_schema_data)
        self.write_json_file(profile_dir / DOMAINS_COMPONENT, domains_data)
        self.write_json_file(profile_dir / RELATIONS_COMPONENT, relations_data)
        self.write_json_file(profile_dir / PIPELINE_COMPONENT, pipeline_data)
        return profile_dir


def is_auxiliary_rule_path(relative_path):
    return any(str(part).startswith("_") for part in Path(relative_path).parts)

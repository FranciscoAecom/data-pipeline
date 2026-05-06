from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import re

import pandas as pd

from projects.configs import resolve_project_name
from core.rules.engine import RuleProfileResolutionError
from core.rules.engine import expected_rule_profile_name
from core.rules.engine import find_rule_profile_by_theme_folder
from core.rules.engine import get_rule_profile_project_name
from settings import (
    DICTIONARIES_SHEET_NAME,
    INGEST_READY_STATUS,
    INGEST_SHEET_NAME,
    INGEST_WORKBOOK_PATH,
)


_DICTIONARY_THEME_CACHE = None


@dataclass
class IngestRecord:
    sheet_row: int
    record_id: object
    theme: str
    theme_folder: str
    status: str
    source_path: str
    input_path: str
    rule_profile: str


@dataclass
class IngestIssue:
    sheet_row: int
    record_id: object
    theme_folder: str
    status: str
    source_path: str
    reason: str


def _stringify(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_lookup_value(value):
    return " ".join(_stringify(value).split()).casefold()


def normalize_status(value):
    return _normalize_lookup_value(value)


def normalize_theme_folder(value):
    text = _stringify(value)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_").lower()


def normalize_attribute_name(value):
    return _stringify(value).lower()


def _is_zip_path(path_value):
    return _stringify(path_value).lower().endswith(".zip")


def _resolve_numbered_sibling_datasets(path, supported_suffixes):
    if path.suffix.lower() not in supported_suffixes or not path.exists():
        return []

    match = re.match(r"^(?P<prefix>.+?)_(?P<index>\d+)$", path.stem, flags=re.IGNORECASE)
    if not match:
        return []

    prefix = match.group("prefix")
    sibling_pattern = re.compile(
        rf"^{re.escape(prefix)}_(\d+){re.escape(path.suffix)}$",
        flags=re.IGNORECASE,
    )
    sibling_files = sorted(
        candidate for candidate in path.parent.iterdir()
        if candidate.is_file()
        and candidate.suffix.lower() in supported_suffixes
        and sibling_pattern.match(candidate.name)
    )

    if len(sibling_files) <= 1:
        return []

    return [str(candidate) for candidate in sibling_files]


def _resolve_input_dataset_paths(path_value):
    raw_path = _stringify(path_value)
    if not raw_path:
        raise FileNotFoundError("Campo path_shapefile_temp vazio.")

    if _is_zip_path(raw_path):
        raise ValueError("Caminho aponta para arquivo ZIP; leitura desabilitada.")

    path = Path(raw_path)
    supported_suffixes = {".shp", ".gpkg"}

    if path.suffix.lower() in supported_suffixes:
        if not path.exists():
            raise FileNotFoundError(f"Arquivo de entrada nao encontrado: {path}")
        sibling_dataset_paths = _resolve_numbered_sibling_datasets(path, supported_suffixes)
        if sibling_dataset_paths:
            return sibling_dataset_paths
        return [str(path)]

    if not path.exists():
        raise FileNotFoundError(f"Caminho nao encontrado: {path}")

    if not path.is_dir():
        raise ValueError(f"Caminho nao suportado para leitura automatica: {path}")

    direct_dataset_files = sorted(
        candidate for candidate in path.iterdir()
        if candidate.is_file() and candidate.suffix.lower() in supported_suffixes
    )

    if direct_dataset_files:
        return [str(candidate) for candidate in direct_dataset_files]

    dataset_files = sorted(
        candidate for candidate in path.rglob("*")
        if candidate.is_file()
        and candidate.suffix.lower() in supported_suffixes
        and ".zip" not in {part.lower() for part in candidate.parts}
    )

    if not dataset_files:
        raise FileNotFoundError(f"Nenhum shapefile ou gpkg encontrado dentro de: {path}")

    return [str(candidate) for candidate in dataset_files]


@lru_cache(maxsize=None)
def _resolve_input_dataset_paths_cached(path_value):
    return tuple(_resolve_input_dataset_paths(path_value))


def _load_dictionary_theme_map(workbook_path=INGEST_WORKBOOK_PATH, sheet_name=DICTIONARIES_SHEET_NAME):
    global _DICTIONARY_THEME_CACHE
    if _DICTIONARY_THEME_CACHE is not None:
        return _DICTIONARY_THEME_CACHE

    dataframe = pd.read_excel(workbook_path, sheet_name=sheet_name)
    theme_map = {}

    for _, row in dataframe.iterrows():
        raw_theme = _stringify(row.get("theme"))
        raw_attribute = _stringify(row.get("original_attribute_name"))
        normalized_theme = _normalize_lookup_value(raw_theme)
        normalized_attribute = normalize_attribute_name(raw_attribute)

        if not normalized_theme or not normalized_attribute or normalized_attribute == "-":
            continue

        entry = theme_map.setdefault(
            normalized_theme,
            {
                "theme": raw_theme,
                "attributes": set(),
                "attribute_labels": {},
            },
        )
        entry["attributes"].add(normalized_attribute)
        entry["attribute_labels"].setdefault(normalized_attribute, raw_attribute)

    _DICTIONARY_THEME_CACHE = theme_map
    return theme_map


def validate_theme_and_attributes(theme_value, input_attributes):
    theme_map = _load_dictionary_theme_map()
    normalized_theme = _normalize_lookup_value(theme_value)
    dictionary_entry = theme_map.get(normalized_theme)

    if not dictionary_entry:
        return {
            "theme_found": False,
            "dictionary_theme": "",
            "missing_attributes": [],
            "extra_attributes": [],
        }

    input_map = {}
    for attribute in input_attributes:
        normalized_attribute = normalize_attribute_name(attribute)
        if normalized_attribute and normalized_attribute != "geometry":
            input_map.setdefault(normalized_attribute, attribute)

    expected_attributes = dictionary_entry["attributes"]
    input_attributes_normalized = set(input_map.keys())

    missing = sorted(expected_attributes - input_attributes_normalized)
    extra = sorted(input_attributes_normalized - expected_attributes)

    return {
        "theme_found": True,
        "dictionary_theme": dictionary_entry["theme"],
        "missing_attributes": [dictionary_entry["attribute_labels"][item] for item in missing],
        "extra_attributes": [input_map[item] for item in extra],
    }


def load_processing_queue(
    workbook_path=INGEST_WORKBOOK_PATH,
    sheet_name=INGEST_SHEET_NAME,
    ready_status=INGEST_READY_STATUS,
):
    dataframe = pd.read_excel(workbook_path, sheet_name=sheet_name)
    ready_status_normalized = normalize_status(ready_status)

    eligible_records = []
    issues = []
    ready_candidates = 0

    for idx, row in dataframe.iterrows():
        sheet_row = idx + 2
        record_id = row.get("ID")
        theme = _stringify(row.get("theme"))
        theme_folder = _stringify(row.get("theme_folder"))
        status = _stringify(row.get("status"))
        source_path = _stringify(row.get("path_shapefile_temp"))

        if normalize_status(status) != ready_status_normalized:
            continue

        ready_candidates += 1

        if _is_zip_path(source_path):
            issues.append(
                IngestIssue(
                    sheet_row=sheet_row,
                    record_id=record_id,
                    theme_folder=theme_folder,
                    status=status,
                    source_path=source_path,
                    reason="Base ignorada porque o caminho informado e um arquivo ZIP.",
                )
            )
            continue

        expected_rule_profile = expected_rule_profile_name(theme_folder)
        try:
            rule_profile = find_rule_profile_by_theme_folder(theme_folder)
        except RuleProfileResolutionError as exc:
            issues.append(
                IngestIssue(
                    sheet_row=sheet_row,
                    record_id=record_id,
                    theme_folder=theme_folder,
                    status=status,
                    source_path=source_path,
                    reason=str(exc),
                )
            )
            continue

        if not rule_profile:
            issues.append(
                IngestIssue(
                    sheet_row=sheet_row,
                    record_id=record_id,
                    theme_folder=theme_folder,
                    status=status,
                    source_path=source_path,
                    reason=(
                        "Nenhum arquivo de regra correspondente foi encontrado em rules/. "
                        f"Perfil esperado: rules/{expected_rule_profile}.json."
                    ),
                )
            )
            continue

        resolved_project_name = resolve_project_name(theme_folder)
        rule_project_name = get_rule_profile_project_name(rule_profile)
        if rule_project_name and rule_project_name != resolved_project_name:
            issues.append(
                IngestIssue(
                    sheet_row=sheet_row,
                    record_id=record_id,
                    theme_folder=theme_folder,
                    status=status,
                    source_path=source_path,
                    reason=(
                        "Perfil de regras inconsistente com o projeto resolvido: "
                        f"theme_folder={theme_folder} -> projeto {resolved_project_name}, "
                        f"mas o perfil {rule_profile} declara project_name={rule_project_name}."
                    ),
                )
            )
            continue

        try:
            input_paths = _resolve_input_dataset_paths_cached(source_path)
        except (FileNotFoundError, ValueError, PermissionError, OSError) as exc:
            issues.append(
                IngestIssue(
                    sheet_row=sheet_row,
                    record_id=record_id,
                    theme_folder=theme_folder,
                    status=status,
                    source_path=source_path,
                    reason=str(exc),
                )
            )
            continue

        for input_path in input_paths:
            eligible_records.append(
                IngestRecord(
                    sheet_row=sheet_row,
                    record_id=record_id,
                    theme=theme,
                    theme_folder=theme_folder,
                    status=status,
                    source_path=source_path,
                    input_path=input_path,
                    rule_profile=rule_profile,
                )
            )

    summary = {
        "total_records": len(dataframe),
        "ready_candidates": ready_candidates,
        "eligible_records": len(eligible_records),
        "issues": len(issues),
    }

    return eligible_records, issues, summary

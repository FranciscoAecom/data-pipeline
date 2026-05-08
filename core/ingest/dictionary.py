import pandas as pd

from core.ingest.normalization import (
    normalize_attribute_name,
    normalize_lookup_value,
    stringify,
)
from settings import DICTIONARIES_SHEET_NAME, INGEST_WORKBOOK_PATH


_DICTIONARY_THEME_CACHE = None


def load_dictionary_theme_map(
    workbook_path=INGEST_WORKBOOK_PATH,
    sheet_name=DICTIONARIES_SHEET_NAME,
):
    global _DICTIONARY_THEME_CACHE
    if _DICTIONARY_THEME_CACHE is not None:
        return _DICTIONARY_THEME_CACHE

    dataframe = pd.read_excel(workbook_path, sheet_name=sheet_name)
    theme_map = {}

    for _, row in dataframe.iterrows():
        raw_theme = stringify(row.get("theme"))
        raw_attribute = stringify(row.get("original_attribute_name"))
        normalized_theme = normalize_lookup_value(raw_theme)
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
    theme_map = load_dictionary_theme_map()
    normalized_theme = normalize_lookup_value(theme_value)
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


__all__ = [
    "load_dictionary_theme_map",
    "validate_theme_and_attributes",
]

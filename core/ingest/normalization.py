import re

import pandas as pd


def stringify(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def normalize_lookup_value(value):
    return " ".join(stringify(value).split()).casefold()


def normalize_status(value):
    return normalize_lookup_value(value)


def normalize_theme_folder(value):
    text = stringify(value)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_").lower()


def normalize_attribute_name(value):
    return stringify(value).lower()


__all__ = [
    "normalize_attribute_name",
    "normalize_lookup_value",
    "normalize_status",
    "normalize_theme_folder",
    "stringify",
]

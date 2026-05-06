import re
import unicodedata


class RuleProfileResolutionError(ValueError):
    pass


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

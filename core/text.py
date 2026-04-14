import re
import unicodedata


def normalize_ascii_text(value):
    if not isinstance(value, str):
        return value

    text = value.strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if ord(ch) < 128)
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_for_compare(value):
    normalized = normalize_ascii_text(value)
    if not isinstance(normalized, str):
        return ""
    return normalized.upper()

from core.rules.normalization import normalize_rule_text


def get_auto_function_mapping(profile):
    auto_functions = profile.get("auto_functions", {})
    return {
        column: list(functions)
        for column, functions in auto_functions.items()
    }


def _get_field_entry(profile, column):
    fields = profile.get("fields", {})
    return fields.get(column)


def has_field_rules(profile, column):
    return _get_field_entry(profile, column) is not None


def _build_normalized_lookup(values):
    return {normalize_rule_text(value): value for value in values}


def classify_field_value(profile, column, value):
    field_rules = _get_field_entry(profile, column)

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


def build_field_mapping(profile, column, values):
    replacements = {}
    corrections = []
    invalid_values = []

    for value in values:
        result = classify_field_value(profile, column, value)
        replacements[value] = result["normalized_value"]
        if result["status"] == "normalized":
            corrections.append((value, result["normalized_value"]))
        elif result["status"] == "invalid":
            invalid_values.append(value)

    return replacements, corrections, invalid_values

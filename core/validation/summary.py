from collections import Counter

from core.utils import log
from core.validation.session import validation_session_or_default


def field_summary_entry(column, validation_session=None):
    validation_session = validation_session_or_default(validation_session)
    return validation_session.summary["fields"].setdefault(
        column,
        {
            "status_counts": Counter(),
            "reason_counts": Counter(),
        },
    )


def register_domain_validation_summary(column, statuses, reasons, validation_session=None):
    entry = field_summary_entry(column, validation_session=validation_session)
    entry["status_counts"].update(statuses)
    entry["reason_counts"].update(
        reason for status, reason in zip(statuses, reasons)
        if status in {"invalid", "empty"} and reason
    )


def relation_summary_entry(relation_key, validation_session=None):
    validation_session = validation_session_or_default(validation_session)
    return validation_session.summary["relations"].setdefault(
        relation_key,
        {
            "status_counts": Counter(),
            "reason_counts": Counter(),
            "autocorrected_counts": Counter(),
            "unchecked_source_counts": Counter(),
            "relation_map": {},
        },
    )


def log_validation_summary(validation_session=None):
    validation_session = validation_session_or_default(validation_session)
    for column, entry in validation_session.summary["fields"].items():
        status_counts = entry["status_counts"]
        normalized_count = status_counts.get("normalized", 0)
        invalid_count = status_counts.get("invalid", 0)
        empty_count = status_counts.get("empty", 0)

        if normalized_count == 0 and invalid_count == 0 and empty_count == 0:
            continue

        parts = []
        if normalized_count:
            parts.append(f"{normalized_count} normalizado(s) por alias")
        if invalid_count:
            parts.append(f"{invalid_count} invalido(s)")
        if empty_count:
            parts.append(f"{empty_count} vazio(s)")

        log(f"Resumo validacao {column}: {', '.join(parts)}")
        for reason, count in entry["reason_counts"].most_common(5):
            log(f"  {count}x - {reason}")

    for relation_key, consistency in validation_session.summary["relations"].items():
        autocorrected = consistency["status_counts"].get("autocorrected", 0)
        inconsistent = consistency["status_counts"].get("inconsistent", 0)
        unchecked = consistency["status_counts"].get("unchecked", 0)
        if not (autocorrected or inconsistent or unchecked):
            continue

        parts = []
        if autocorrected:
            parts.append(f"{autocorrected} autocorrigido(s) pela relacao {relation_key}")
        if inconsistent:
            parts.append(f"{inconsistent} inconsistente(s)")
        if unchecked:
            parts.append(f"{unchecked} nao verificado(s)")
        log(f"Resumo consistencia relacao {relation_key}: {', '.join(parts)}")

        relation_map = consistency.get("relation_map", {})
        for source_value, count in consistency["autocorrected_counts"].most_common(5):
            expected_target = relation_map.get(source_value)
            if expected_target:
                log(f"  {count}x - Valor ajustado automaticamente para {source_value}: {expected_target}")
        if unchecked:
            for source_value, count in consistency["unchecked_source_counts"].most_common(10):
                log(f"  {count}x - Valor fonte fora do dominio configurado: {source_value}")
        for reason, count in consistency["reason_counts"].most_common(5):
            log(f"  {count}x - {reason}")


__all__ = [
    "field_summary_entry",
    "log_validation_summary",
    "register_domain_validation_summary",
    "relation_summary_entry",
]

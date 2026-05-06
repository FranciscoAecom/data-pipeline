from dataclasses import dataclass, field

from core.utils import log


@dataclass(frozen=True)
class ProcessingError(Exception):
    code: str
    message: str
    details: dict = field(default_factory=dict)

    def __str__(self):
        return self.message


def input_error(message, **details):
    return ProcessingError("input_error", message, details)


def schema_error(message, **details):
    return ProcessingError("schema_error", message, details)


def rule_error(message, **details):
    return ProcessingError("rule_error", message, details)


def output_error(message, **details):
    return ProcessingError("output_error", message, details)


def log_processing_error(prefix, exc):
    if isinstance(exc, ProcessingError):
        log(f"{prefix}: [{exc.code}] {exc.message}")
        return
    log(f"{prefix}: {exc}")

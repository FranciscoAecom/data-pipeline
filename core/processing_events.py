from dataclasses import dataclass, field

from core.utils import log


@dataclass(frozen=True)
class ProcessingEvent:
    kind: str
    message: str
    context: dict = field(default_factory=dict)


def emit_processing_event(kind, message, **context):
    event = ProcessingEvent(kind=kind, message=message, context=context)
    log(event.message)
    return event

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


def emit_record_start_events(record):
    emit_processing_event("record.blank_line", "")
    emit_processing_event(
        "record.start",
        f"Processando linha {record.sheet_row} da ingest | "
        f"ID={record.record_id} | theme_folder={record.theme_folder}",
        sheet_row=record.sheet_row,
        record_id=record.record_id,
        theme_folder=record.theme_folder,
    )
    emit_processing_event("record.theme", f"Theme informado na ingest: {record.theme}")
    emit_processing_event("record.source", f"Caminho de origem informado: {record.source_path}")
    emit_processing_event("record.input", f"Arquivo de entrada resolvido: {record.input_path}")
    emit_processing_event("record.rule_profile", f"Perfil de regras associado: {record.rule_profile}")


def emit_project_resolved_event(context):
    project_name = getattr(
        context,
        "project_name",
        context.project_config["project_name"],
    )
    return emit_processing_event(
        "project.resolved",
        f"Projeto resolvido: {project_name}",
        project_name=project_name,
    )

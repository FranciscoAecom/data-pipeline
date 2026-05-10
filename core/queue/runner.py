import os

from core.ingest.loader import load_processing_queue
from core.input_preparation import log_queue_summary
from core.output.manager import (
    append_group_consolidated_output,
    build_group_log_path,
)
from core.processing.record_processor import process_record
from core.queue.group_state import QueueGroupState
from core.utils import clear_context_log, log, set_context_log
from settings import (
    ENABLE_GROUP_CONSOLIDATION,
    KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING,
    OUTPUT_BASE,
)


def run_processing_queue(output_base=OUTPUT_BASE):
    try:
        processing_queue, queue_issues, queue_summary = load_processing_queue()
    except Exception as exc:
        log(f"Erro ao carregar a fila ingest: {exc}")
        return

    log_queue_summary(queue_summary, queue_issues)

    if not processing_queue:
        log("Nenhum arquivo elegivel encontrado para iniciar a esteira.")
        return

    output_dir = str(output_base)
    os.makedirs(output_dir, exist_ok=True)

    group_state = QueueGroupState(
        processing_queue,
        enable_group_consolidation=ENABLE_GROUP_CONSOLIDATION,
    )

    for record in processing_queue:
        try:
            set_context_log(
                build_group_log_path(record, output_dir),
                reset=group_state.should_reset_context_log(record),
            )
            group_state.mark_context_log_started(record)
            record_result = process_record(
                record,
                output_dir,
                id_start=group_state.id_start_for(record),
                use_configured_final_name=group_state.use_configured_final_name(record),
                persist_individual_output=group_state.persist_individual_output(
                    record,
                    KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING,
                ),
            )
            group_state.register_result(record, record_result)
            if group_state.should_append_consolidated_output(record, record_result):
                append_group_consolidated_output(
                    record,
                    record_result.final_gdf,
                    output_dir,
                    append=group_state.append_started_for(record),
                )
                group_state.mark_append_started(record)
        finally:
            clear_context_log()

    log("Processamento finalizado")

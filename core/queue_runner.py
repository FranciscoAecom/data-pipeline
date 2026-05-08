import os
from collections import defaultdict

from core.ingest.loader import load_processing_queue
from core.input_preparation import log_queue_summary
from core.output.manager import (
    append_group_consolidated_output,
    build_group_log_path,
    build_processing_group_key,
)
from core.record_processor import process_record
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

    group_expected_counts = defaultdict(int)
    for record in processing_queue:
        group_expected_counts[build_processing_group_key(record)] += 1

    next_group_id_start = defaultdict(lambda: 1)
    group_append_started = defaultdict(bool)
    context_log_started = defaultdict(bool)

    for record in processing_queue:
        group_key = build_processing_group_key(record)
        is_grouped_consolidation = (
            ENABLE_GROUP_CONSOLIDATION and group_expected_counts[group_key] > 1
        )
        try:
            set_context_log(
                build_group_log_path(record, output_dir),
                reset=not context_log_started[group_key],
            )
            context_log_started[group_key] = True
            record_result = process_record(
                record,
                output_dir,
                id_start=next_group_id_start[group_key],
                use_configured_final_name=(
                    group_expected_counts[group_key] == 1 or not ENABLE_GROUP_CONSOLIDATION
                ),
                persist_individual_output=(
                    not is_grouped_consolidation or KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING
                ),
            )
            if record_result.processed_count:
                next_group_id_start[group_key] += record_result.processed_count
            if (
                ENABLE_GROUP_CONSOLIDATION
                and group_expected_counts[group_key] > 1
                and record_result.final_gdf is not None
            ):
                append_group_consolidated_output(
                    record,
                    record_result.final_gdf,
                    output_dir,
                    append=group_append_started[group_key],
                )
                group_append_started[group_key] = True
        finally:
            clear_context_log()

    log("Processamento finalizado")

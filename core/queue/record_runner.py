from core.output.consolidation import append_group_consolidated_output
from core.output.paths import build_group_log_path
from core.processing.record_processor import process_record
from core.utils import clear_context_log, set_context_log


def run_queue_record(
    record,
    output_dir,
    group_state,
    keep_individual_outputs_when_grouping,
):
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
                keep_individual_outputs_when_grouping,
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

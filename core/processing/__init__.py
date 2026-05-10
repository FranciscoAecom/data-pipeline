from importlib import import_module


_EXPORTS = {
    "ProcessingError": ("core.processing.errors", "ProcessingError"),
    "input_error": ("core.processing.errors", "input_error"),
    "log_processing_error": ("core.processing.errors", "log_processing_error"),
    "output_error": ("core.processing.errors", "output_error"),
    "rule_error": ("core.processing.errors", "rule_error"),
    "schema_error": ("core.processing.errors", "schema_error"),
    "ProcessingEvent": ("core.processing.events", "ProcessingEvent"),
    "emit_processing_event": ("core.processing.events", "emit_processing_event"),
    "emit_project_resolved_event": ("core.processing.events", "emit_project_resolved_event"),
    "emit_record_start_events": ("core.processing.events", "emit_record_start_events"),
    "load_input_step": ("core.processing.input_step", "load_input_step"),
    "prepare_mapping_step": ("core.processing.mapping_step", "prepare_mapping_step"),
    "persist_outputs_step": ("core.processing.output_step", "persist_outputs_step"),
    "run_pipeline_step": ("core.processing.pipeline_step", "run_pipeline_step"),
    "postprocess_step": ("core.processing.postprocess_step", "postprocess_step"),
    "process_record": ("core.processing.record_processor", "process_record"),
    "ProcessRecordResult": ("core.processing.result", "ProcessRecordResult"),
    "failure_result": ("core.processing.result", "failure_result"),
    "success_result": ("core.processing.result", "success_result"),
    "attach_rule_profile_step": ("core.processing.rules_step", "attach_rule_profile_step"),
    "validate_input_schema_step": ("core.processing.schema_step", "validate_input_schema_step"),
    "ProcessingService": ("core.processing.service", "ProcessingService"),
    "log_dataset_overview": ("core.processing.summary", "log_dataset_overview"),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attribute_name = _EXPORTS[name]
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value

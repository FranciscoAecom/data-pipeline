from core.processing.context_factory import build_processing_context
from core.processing.errors import log_processing_error
from core.processing.events import emit_project_resolved_event, emit_record_start_events
from core.processing.pipeline_runner import run_processing_pipeline
from core.processing.result import failure_result, success_result
from core.rules.autofix_service import RuleAutofixService


class ProcessingService:
    def __init__(self, autofix_service=None):
        self.autofix_service = autofix_service or RuleAutofixService()

    def _failure_result(self):
        return failure_result()

    def process(
        self,
        record,
        output_dir,
        id_start=1,
        use_configured_final_name=False,
        persist_individual_output=True,
    ):
        emit_record_start_events(record)

        try:
            context = build_processing_context(record, output_dir, id_start=id_start)
        except Exception as exc:
            log_processing_error("Erro ao resolver configuracao do projeto", exc)
            return self._failure_result()

        emit_project_resolved_event(context)

        context = run_processing_pipeline(
            context,
            self.autofix_service,
            use_configured_final_name=use_configured_final_name,
            persist_individual_output=persist_individual_output,
        )
        if context is None:
            return self._failure_result()

        return success_result(context)

from dataclasses import dataclass

import geopandas as gpd

from core.execution_context import ProcessingContext
from core.processing.pipeline_runner import run_processing_pipeline
from core.processing_errors import log_processing_error
from core.processing_events import emit_project_resolved_event, emit_record_start_events
from core.rules.autofix_service import RuleAutofixService
from core.validation.session import ValidationSession
from projects.configs import resolve_project_config
from projects.registry import get_project_optional_functions


@dataclass(frozen=True)
class ProcessRecordResult:
    processed_count: int
    output_path: str | None
    final_gdf: gpd.GeoDataFrame | None


class ProcessingService:
    def __init__(self, autofix_service=None):
        self.autofix_service = autofix_service or RuleAutofixService()

    def _failure_result(self):
        return ProcessRecordResult(0, None, None)

    def build_context(self, record, output_dir, id_start=1):
        project_config = resolve_project_config(record.theme_folder)
        optional_functions = get_project_optional_functions(project_config["project_name"])
        return ProcessingContext(
            record=record,
            output_dir=output_dir,
            project_config=project_config,
            rule_profile_name=record.rule_profile,
            rule_profile=None,
            optional_functions=optional_functions,
            id_start=id_start,
            validation_session=ValidationSession(),
        )

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
            context = self.build_context(record, output_dir, id_start=id_start)
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

        return ProcessRecordResult(len(context.final_gdf), context.output_path, context.final_gdf)

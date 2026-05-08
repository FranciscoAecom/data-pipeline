from dataclasses import dataclass

import geopandas as gpd

from core.execution_context import ProcessingContext
from core.input_preparation import log_dataset_overview
from core.output.writer import persist_outputs_step
from core.processing import (
    attach_rule_profile_step,
    load_input_step,
    postprocess_step,
    prepare_mapping_step,
    run_pipeline_step,
    validate_input_schema_step,
)
from core.processing_errors import log_processing_error
from core.processing_events import emit_project_resolved_event, emit_record_start_events
from core.rules.autofix_service import RuleAutofixService
from core.utils import timed_log_step
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

    def _run_timed_step(self, label, error_message, operation):
        try:
            with timed_log_step(label):
                return operation()
        except Exception as exc:
            log_processing_error(error_message, exc)
            return None

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

        context = self._run_timed_step(
            "Carga e preparo da base de entrada",
            "Erro ao carregar ou validar arquivo de entrada",
            lambda: load_input_step(context),
        )
        if context is None:
            return self._failure_result()

        context = self._run_timed_step(
            "Carregamento do perfil de regras",
            "Erro ao carregar perfil de regras",
            lambda: attach_rule_profile_step(context),
        )
        if context is None:
            return self._failure_result()

        context = self._run_timed_step(
            "Validacao de schema tabular",
            "Erro na validacao de schema tabular",
            lambda: validate_input_schema_step(context),
        )
        if context is None:
            return self._failure_result()

        log_dataset_overview(context.gdf)

        with timed_log_step("Preparacao do mapeamento de validacao"):
            context = prepare_mapping_step(context)

        with timed_log_step("Processamento principal em batches"):
            context = run_pipeline_step(context)

        context = postprocess_step(context)
        autofix_summary = self.autofix_rule_profile(context, context.final_gdf)
        self.log_autofix_summary(autofix_summary)

        context = self._run_timed_step(
            "Persistencia de saidas",
            "Erro ao salvar arquivo",
            lambda: persist_outputs_step(
                context,
                use_configured_final_name=use_configured_final_name,
                persist_dataset=persist_individual_output,
            ),
        )
        if context is None:
            return self._failure_result()

        return ProcessRecordResult(len(context.final_gdf), context.output_path, context.final_gdf)

    def autofix_rule_profile(self, context, final_gdf):
        return self.autofix_service.autofix_rule_profile(context, final_gdf)

    def log_autofix_summary(self, summary):
        self.autofix_service.log_autofix_summary(summary)

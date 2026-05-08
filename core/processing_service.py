import os
from dataclasses import dataclass

import geopandas as gpd

from core.execution_context import ProcessingContext, replace_context
from core.input_preparation import log_dataset_overview
from core.naming import build_theme_output_dir
from core.output.writer import persist_outputs_step
from core.processing_errors import log_processing_error
from core.processing_events import emit_processing_event
from core.processing_steps import (
    attach_rule_profile_step,
    load_input_step,
    postprocess_step,
    run_pipeline_step,
    validate_input_schema_step,
)
from core.rule_runtime import build_auto_mapping
from core.utils import log, timed_log_step
from core.rules.autofix import autofix_rule_profile_from_invalid_domains
from core.validation.attribute_mapping import prepare_validate_shapefile_attribute_mappings
from core.validation.session import ValidationSession
from projects.configs import resolve_project_config
from projects.registry import get_project_optional_functions


@dataclass(frozen=True)
class ProcessRecordResult:
    processed_count: int
    output_path: str | None
    final_gdf: gpd.GeoDataFrame | None


class ProcessingService:
    def _failure_result(self):
        return ProcessRecordResult(0, None, None)

    def _emit_record_start_events(self, record):
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
        self._emit_record_start_events(record)

        try:
            context = self.build_context(record, output_dir, id_start=id_start)
        except Exception as exc:
            log_processing_error("Erro ao resolver configuracao do projeto", exc)
            return self._failure_result()

        emit_processing_event(
            "project.resolved",
            f"Projeto resolvido: {self.project_name(context)}",
            project_name=self.project_name(context),
        )

        context = self._run_timed_step(
            "Carga e preparo da base de entrada",
            "Erro ao carregar ou validar arquivo de entrada",
            lambda: replace_context(context, gdf=self.load_input(context)),
        )
        if context is None:
            return self._failure_result()

        context = self._run_timed_step(
            "Carregamento do perfil de regras",
            "Erro ao carregar perfil de regras",
            lambda: self.attach_rule_profile(context),
        )
        if context is None:
            return self._failure_result()

        context = self._run_timed_step(
            "Validacao de schema tabular",
            "Erro na validacao de schema tabular",
            lambda: replace_context(
                context,
                gdf=self.validate_tabular_schema(context, context.gdf),
            ),
        )
        if context is None:
            return self._failure_result()

        log_dataset_overview(context.gdf)

        with timed_log_step("Preparacao do mapeamento de validacao"):
            context = replace_context(context, mapping=self.build_mapping(context, context.gdf))
            prepare_validate_shapefile_attribute_mappings(
                context.gdf,
                context.mapping,
                context.rule_profile,
                validation_session=context.validation_session,
            )

        with timed_log_step("Processamento principal em batches"):
            context = run_pipeline_step(context)

        context = replace_context(context, final_gdf=self.postprocess(context, context.final_gdf))
        autofix_summary = self.autofix_rule_profile(context, context.final_gdf)
        self.log_autofix_summary(autofix_summary)

        context = self._run_timed_step(
            "Persistencia de saidas",
            "Erro ao salvar arquivo",
            lambda: self.persist_outputs(
                context,
                use_configured_final_name=use_configured_final_name,
                persist_dataset=persist_individual_output,
            ),
        )
        if context is None:
            return self._failure_result()

        return ProcessRecordResult(len(context.final_gdf), context.output_path, context.final_gdf)

    def load_input(self, context):
        return load_input_step(context).gdf

    def attach_rule_profile(self, context):
        return attach_rule_profile_step(context)

    def validate_tabular_schema(self, context, gdf):
        return validate_input_schema_step(replace_context(context, gdf=gdf)).gdf

    def build_mapping(self, context, gdf):
        return build_auto_mapping(list(gdf.columns), context.rule_profile)

    def project_name(self, context):
        if hasattr(context, "project_name"):
            return context.project_name
        return context.project_config["project_name"]

    def postprocess(self, context, final_gdf):
        return postprocess_step(replace_context(context, final_gdf=final_gdf)).final_gdf

    def persist_outputs(
        self,
        context,
        use_configured_final_name=False,
        persist_dataset=True,
    ):
        return persist_outputs_step(
            context,
            use_configured_final_name=use_configured_final_name,
            persist_dataset=persist_dataset,
        )

    def autofix_rule_profile(self, context, final_gdf):
        with timed_log_step("Ajuste automatico do perfil de regras"):
            theme_output_dir = build_theme_output_dir(
                context.output_dir,
                context.record.theme_folder,
            )
            os.makedirs(theme_output_dir, exist_ok=True)
            base_name = os.path.splitext(os.path.basename(context.record.input_path))[0]
            support_report_path = os.path.join(
                theme_output_dir,
                f"{base_name}_inconsistencias_dominio.xlsx",
            )

            try:
                return autofix_rule_profile_from_invalid_domains(
                    context.rule_profile_name,
                    context.rule_profile,
                    final_gdf,
                    support_report_path=support_report_path,
                )
            except Exception as exc:
                log(f"Erro ao tentar corrigir automaticamente o perfil de regras: {exc}")
                return None

    def log_autofix_summary(self, summary):
        if not summary or not summary["changed"]:
            return

        log("Inconsistencias de dominio detectadas. Perfil de regras atualizado automaticamente.")
        log(f"Perfil atualizado: {summary['profile_path']}")
        if summary["invalid_columns"]:
            log(f"Atributos analisados para ajuste: {', '.join(summary['invalid_columns'])}")
        if summary["report_path"]:
            log(f"Relatorio de apoio com valores unicos: {summary['report_path']}")
        for column, values in summary["accepted_values_added"].items():
            log(f"  Novos valores aceitos em {column}: {', '.join(values)}")
        for column, aliases in summary["aliases_added"].items():
            alias_parts = [f"{source} -> {target}" for source, target in aliases.items()]
            log(f"  Novos aliases em {column}: {', '.join(alias_parts)}")
        for relation_key, mapping in summary["relations_added"].items():
            relation_parts = [f"{source} -> {target}" for source, target in mapping.items()]
            log(f"  Novas relacoes em {relation_key}: {', '.join(relation_parts)}")
        log(
            "As novas regras foram salvas para as proximas execucoes. "
            "Reprocesse a base se quiser aplicar o ajuste automaticamente neste mesmo arquivo."
        )

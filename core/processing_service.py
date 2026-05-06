import os
from dataclasses import dataclass

import geopandas as gpd

from core.execution_context import ProcessingContext, replace_context
from core.input_preparation import log_dataset_overview
from core.naming import build_theme_output_dir
from core.output_writer import persist_outputs_step
from core.processing_errors import log_processing_error
from core.processing_steps import (
    attach_rule_profile_step,
    load_input_step,
    postprocess_step,
    run_pipeline_step,
    validate_input_schema_step,
)
from core.rule_runtime import build_auto_mapping
from core.utils import log, timed_log_step
from core.validation.rule_autofix import autofix_rule_profile_from_invalid_domains
from core.validation.validation_functions import (
    prepare_validate_shapefile_attribute_mappings,
    reset_validate_attribute_mappings,
)
from projects.configs import resolve_project_config
from projects.registry import get_project_optional_functions


@dataclass(frozen=True)
class ProcessRecordResult:
    processed_count: int
    output_path: str | None
    final_gdf: gpd.GeoDataFrame | None


class ProcessingService:
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
        )

    def process(
        self,
        record,
        output_dir,
        id_start=1,
        use_configured_final_name=False,
        persist_individual_output=True,
    ):
        log("")
        log(
            f"Processando linha {record.sheet_row} da ingest | "
            f"ID={record.record_id} | theme_folder={record.theme_folder}"
        )
        log(f"Theme informado na ingest: {record.theme}")
        log(f"Caminho de origem informado: {record.source_path}")
        log(f"Arquivo de entrada resolvido: {record.input_path}")
        log(f"Perfil de regras associado: {record.rule_profile}")

        reset_validate_attribute_mappings()

        try:
            context = self.build_context(record, output_dir, id_start=id_start)
        except Exception as exc:
            log_processing_error("Erro ao resolver configuracao do projeto", exc)
            return ProcessRecordResult(0, None, None)

        log(f"Projeto resolvido: {self.project_name(context)}")

        try:
            with timed_log_step("Carga e preparo da base de entrada"):
                context = replace_context(context, gdf=self.load_input(context))
        except Exception as exc:
            log_processing_error("Erro ao carregar ou validar arquivo de entrada", exc)
            return ProcessRecordResult(0, None, None)

        try:
            with timed_log_step("Carregamento do perfil de regras"):
                context = self.attach_rule_profile(context)
        except Exception as exc:
            log_processing_error("Erro ao carregar perfil de regras", exc)
            return ProcessRecordResult(0, None, None)

        try:
            with timed_log_step("Validacao de schema tabular"):
                context = replace_context(
                    context,
                    gdf=self.validate_tabular_schema(context, context.gdf),
                )
        except Exception as exc:
            log_processing_error("Erro na validacao de schema tabular", exc)
            return ProcessRecordResult(0, None, None)

        log_dataset_overview(context.gdf)

        with timed_log_step("Preparacao do mapeamento de validacao"):
            context = replace_context(context, mapping=self.build_mapping(context, context.gdf))
            prepare_validate_shapefile_attribute_mappings(
                context.gdf,
                context.mapping,
                context.rule_profile,
            )

        with timed_log_step("Processamento principal em batches"):
            context = run_pipeline_step(context)

        context = replace_context(context, final_gdf=self.postprocess(context, context.final_gdf))
        autofix_summary = self.autofix_rule_profile(context, context.final_gdf)
        self.log_autofix_summary(autofix_summary)

        try:
            with timed_log_step("Persistencia de saidas"):
                context = self.persist_outputs(
                    context,
                    use_configured_final_name=use_configured_final_name,
                    persist_dataset=persist_individual_output,
                )
        except Exception as exc:
            log_processing_error("Erro ao salvar arquivo", exc)
            return ProcessRecordResult(0, None, None)

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

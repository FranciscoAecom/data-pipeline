import os
from dataclasses import dataclass

import geopandas as gpd

from core.batch_processor import process_in_batches
from core.geometry_repair import repair_invalid_geometries
from core.input_preparation import load_and_prepare_input, log_dataset_overview
from core.naming import build_theme_output_dir
from core.output_manager import assign_output_identifiers, save_outputs
from core.rule_runtime import build_auto_mapping
from core.spatial.spatial_functions import fill_missing_spatial_metrics
from core.utils import log, timed_log_step
from core.validation.rule_autofix import autofix_rule_profile_from_invalid_domains
from core.validation.rule_engine import set_active_rule_profile
from core.validation.validation_functions import (
    prepare_validate_shapefile_attribute_mappings,
    reset_validate_attribute_mappings,
)
from projects.configs import resolve_project_config


@dataclass(frozen=True)
class ProcessRecordResult:
    processed_count: int
    output_path: str | None
    final_gdf: gpd.GeoDataFrame | None


def _autofix_rule_profile(final_gdf, record, output_dir):
    theme_output_dir = build_theme_output_dir(output_dir, record.theme_folder)
    os.makedirs(theme_output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(record.input_path))[0]
    support_report_path = os.path.join(
        theme_output_dir,
        f"{base_name}_inconsistencias_dominio.xlsx",
    )

    try:
        summary = autofix_rule_profile_from_invalid_domains(
            record.rule_profile,
            final_gdf,
            support_report_path=support_report_path,
        )
    except Exception as exc:
        log(f"Erro ao tentar corrigir automaticamente o perfil de regras: {exc}")
        return None

    return summary


def _log_autofix_summary(summary):
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


def process_record(
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
    project_config = resolve_project_config(record.theme_folder)
    log(f"Projeto resolvido: {project_config['project_name']}")

    reset_validate_attribute_mappings()

    try:
        with timed_log_step("Carga e preparo da base de entrada"):
            gdf = load_and_prepare_input(record)
    except Exception as exc:
        log(f"Erro ao carregar ou validar arquivo de entrada: {exc}")
        return ProcessRecordResult(0, None, None)

    try:
        with timed_log_step("Carregamento do perfil de regras"):
            set_active_rule_profile(record.rule_profile)
    except Exception as exc:
        log(f"Erro ao carregar perfil de regras: {exc}")
        return ProcessRecordResult(0, None, None)

    columns = list(gdf.columns)
    log_dataset_overview(gdf)

    with timed_log_step("Preparacao do mapeamento de validacao"):
        mapping = build_auto_mapping(columns)
        prepare_validate_shapefile_attribute_mappings(gdf, mapping)
    with timed_log_step("Processamento principal em batches"):
        final_gdf, _ = process_in_batches(
            gdf,
            mapping,
            id_start=id_start,
            project_name=project_config["project_name"],
        )

    log(f"Resultado final: {len(final_gdf)} registros processados")

    try:
        final_gdf = gpd.GeoDataFrame(final_gdf, geometry="geometry", crs=final_gdf.crs)
    except Exception as exc:
        log(f"Erro ao garantir GeoDataFrame com geometria: {exc}")

    with timed_log_step("Atribuicao de identificadores finais"):
        final_gdf = assign_output_identifiers(final_gdf, id_start)
    with timed_log_step("Reparo de geometrias invalidas"):
        final_gdf = repair_invalid_geometries(final_gdf)
    with timed_log_step("Complemento de metricas espaciais"):
        final_gdf = fill_missing_spatial_metrics(final_gdf)
    with timed_log_step("Ajuste automatico do perfil de regras"):
        autofix_summary = _autofix_rule_profile(final_gdf, record, output_dir)
    _log_autofix_summary(autofix_summary)

    try:
        with timed_log_step("Persistencia de saidas"):
            output_path = save_outputs(
                final_gdf,
                record,
                output_dir,
                use_configured_final_name=use_configured_final_name,
                persist_dataset=persist_individual_output,
            )
    except Exception as exc:
        log(f"Erro ao salvar arquivo: {exc}")
        return ProcessRecordResult(0, None, None)

    return ProcessRecordResult(len(final_gdf), output_path, final_gdf)

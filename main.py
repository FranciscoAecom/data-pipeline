# main.py

import os
import re
from collections import defaultdict

import geopandas as gpd

from core.transforms.attribute_transforms import clean_whitespace, normalize_columns
from core.batch_processor import process_in_batches
from core.dataset_io import read_input_dataset, write_output_gpkg
from core.ingest_loader import (
    load_processing_queue,
    validate_theme_and_attributes,
)
from core.reporting import export_duplicate_reports
from core.validation.rule_autofix import autofix_rule_profile_from_invalid_domains
from core.validation.rule_engine import (
    get_auto_function_mapping,
    set_active_rule_profile,
)
from core.naming import build_final_output_base_name, build_theme_output_dir
from projects.configs import resolve_project_config
from settings import (
    ENABLE_ATTRIBUTE_DUPLICATE_REPORT,
    ENABLE_GEOMETRIC_DUPLICATE_REPORT,
    ENABLE_GROUP_CONSOLIDATION,
    ENABLE_OGC_INVALID_REPORT,
    INGEST_READY_STATUS,
    INGEST_SHEET_NAME,
    INGEST_WORKBOOK_PATH,
    KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING,
    OUTPUT_BASE,
)
from core.utils import clear_context_log, log, set_context_log, timed_log_step
from core.validation.validation_functions import (
    get_attribute_duplicate_mask,
    get_attribute_duplicate_records,
    prepare_validate_shapefile_attribute_mappings,
    reset_validate_attribute_mappings,
)
from core.spatial.spatial_functions import get_finite_geometry_mask, get_geometric_duplicate_mask, get_geometric_duplicate_records, get_invalid_ogc_records
from core.spatial.spatial_functions import fill_missing_spatial_metrics, repair_geometry_safely

INTERNAL_SAFE_REPAIR_FLAG = "__internal_geom_null_safe_repair"


def merge_function_mapping(base_mapping, new_mapping):
    merged = {column: list(funcs) for column, funcs in base_mapping.items()}

    for column, funcs in new_mapping.items():
        if column not in merged:
            merged[column] = list(funcs)
            continue

        for func in funcs:
            if func not in merged[column]:
                merged[column].append(func)

    return merged


def log_dataset_overview(gdf):
    columns = list(gdf.columns)
    log(f"Atributos encontrados: {len(columns)}")
    for column in columns:
        log(f"  - {column} ({gdf[column].dtype})")


def log_queue_summary(summary, issues):
    log("Resumo da planilha ingest:")
    log(f"  Aba analisada: {INGEST_SHEET_NAME}")
    log(f"  Caminho da planilha: {INGEST_WORKBOOK_PATH}")
    log(f"  Registros lidos: {summary['total_records']}")
    log(f"  Status elegivel: {INGEST_READY_STATUS}")
    log(f"  Registros com status elegivel: {summary['ready_candidates']}")
    log(f"  Arquivos aptos para processamento: {summary['eligible_records']}")
    log(f"  Registros ignorados com excecao: {summary['issues']}")

    if issues:
        log("Excecoes encontradas na fila ingest:")
        for issue in issues:
            log(
                "  "
                f"Linha {issue.sheet_row} | ID={issue.record_id} | "
                f"theme_folder={issue.theme_folder or '<vazio>'} | "
                f"motivo={issue.reason}"
            )


def _build_processing_group_key(record):
    return (
        record.sheet_row,
        str(record.record_id),
        str(record.theme_folder),
        str(record.source_path),
    )


def _build_group_merged_output_path(record, output_dir):
    theme_output_dir = build_theme_output_dir(output_dir, record.theme_folder)
    os.makedirs(theme_output_dir, exist_ok=True)
    return os.path.join(theme_output_dir, f"{build_final_output_base_name(record)}.gpkg")


def _build_group_log_path(record, output_dir):
    theme_output_dir = build_theme_output_dir(output_dir, record.theme_folder)
    os.makedirs(theme_output_dir, exist_ok=True)
    if ENABLE_GROUP_CONSOLIDATION:
        base_name = build_final_output_base_name(record)
    else:
        base_name = build_final_output_base_name(record)
    return os.path.join(theme_output_dir, f"{base_name}.txt")


def _assign_output_identifiers(gdf, start_id):
    identified = gdf.copy()
    if "acm_id" not in identified.columns:
        identified["acm_id"] = range(start_id, start_id + len(identified))
    identified["fid"] = identified["acm_id"]
    return identified


def _drop_internal_output_columns(gdf):
    internal_columns = [
        INTERNAL_SAFE_REPAIR_FLAG,
    ]
    existing_columns = [column for column in internal_columns if column in gdf.columns]
    if not existing_columns:
        return gdf
    return gdf.drop(columns=existing_columns)


def _append_group_consolidated_output(record, final_gdf, output_dir, append=False):
    merged_output_path = _build_group_merged_output_path(record, output_dir)
    export_gdf = _drop_internal_output_columns(final_gdf)
    if append:
        log(f"Acrescentando resultado ao consolidado {merged_output_path}")
    else:
        log(f"Criando consolidado {merged_output_path}")
    write_output_gpkg(
        export_gdf,
        merged_output_path,
        append=append,
        overwrite_existing=not append,
    )
    return merged_output_path


def _log_dictionary_validation(record, input_attributes):
    result = validate_theme_and_attributes(record.theme, input_attributes)

    if not result["theme_found"]:
        log(
            f"Theme sem correspondencia na aba dictionaries: '{record.theme}'. "
            "Validacao estrutural nao executada."
        )
        return

    if not result["missing_attributes"] and not result["extra_attributes"]:
        log(
            f"Validacao dictionaries OK para theme '{result['dictionary_theme']}'. "
            "Estrutura do arquivo compativel com original_attribute_name."
        )
        return

    log(
        f"Divergencias estruturais encontradas para theme '{result['dictionary_theme']}'."
    )
    if result["missing_attributes"]:
        log(f"  Campos ausentes no arquivo: {', '.join(result['missing_attributes'])}")
    if result["extra_attributes"]:
        log(f"  Campos excedentes no arquivo: {', '.join(result['extra_attributes'])}")


def _load_and_prepare_input(record):
    gdf = read_input_dataset(record.input_path)
    input_attributes = list(gdf.columns)
    _log_dictionary_validation(record, input_attributes)

    gdf = normalize_columns(gdf)
    gdf = clean_whitespace(gdf)
    return gdf


def _build_auto_mapping(columns):
    auto_mapping = {
        column: funcs
        for column, funcs in get_auto_function_mapping().items()
        if column in columns
    }
    mapping = merge_function_mapping({}, auto_mapping)

    if not auto_mapping:
        log("Nenhuma auto_function configurada para o perfil ativo. Apenas funcoes obrigatorias serao executadas.")
    else:
        log(f"Auto_functions carregadas para {len(auto_mapping)} atributo(s).")

    return mapping


def _repair_invalid_geometries(gdf):
    if "geometry" not in gdf.columns:
        log("Erro: coluna 'geometry' ausente no GeoDataFrame final. O arquivo GPKG pode nao conter feicoes.")
        return gdf

    repair_flag_column = INTERNAL_SAFE_REPAIR_FLAG
    if repair_flag_column not in gdf.columns:
        gdf[repair_flag_column] = False

    geometry = gdf.geometry

    if geometry.is_empty.all():
        log("Aviso: todas as geometrias estao vazias.")

    invalid_mask = geometry.notna() & (~geometry.is_valid)
    invalid_geoms = int(invalid_mask.sum())
    if invalid_geoms > 0:
        log(f"Atencao: {invalid_geoms} geometrias invalidas encontradas. Tentando reparar...")
        repaired_geometry = geometry.copy()
        invalid_geometry = repaired_geometry.loc[invalid_mask].copy()

        finite_invalid_mask = get_finite_geometry_mask(invalid_geometry)
        fast_repair_mask = finite_invalid_mask.copy()
        if fast_repair_mask.any():
            try:
                repaired_geometry.loc[fast_repair_mask.index[fast_repair_mask]] = (
                    invalid_geometry.loc[fast_repair_mask].buffer(0)
                )
            except Exception:
                pass

        remaining_invalid_mask = repaired_geometry.notna() & (~repaired_geometry.is_valid)
        remaining_invalid_mask &= invalid_mask
        if remaining_invalid_mask.any():
            repaired_geometry.loc[remaining_invalid_mask] = (
                repaired_geometry.loc[remaining_invalid_mask].apply(repair_geometry_safely)
            )

        safe_null_mask = invalid_mask & repaired_geometry.isna()
        gdf["geometry"] = repaired_geometry
        gdf.loc[safe_null_mask, repair_flag_column] = True
        remaining_invalid_mask = gdf.geometry.notna() & (~gdf.geometry.is_valid)
        repaired_invalid = int(remaining_invalid_mask.sum())
        log(f"Geometrias invalidas apos reparo: {repaired_invalid}")

        dropped_geometry = int(safe_null_mask.sum())
        if dropped_geometry > 0:
            log(
                "Aviso: "
                f"{dropped_geometry} geometria(s) nao puderam ser reparadas com seguranca e ficaram nulas."
            )

    return gdf


def _autofix_rule_profile_if_needed(final_gdf, record, output_dir):
    theme_output_dir = build_theme_output_dir(output_dir, record.theme_folder)
    os.makedirs(theme_output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(record.input_path))[0]
    support_report_path = os.path.join(theme_output_dir, f"{base_name}_inconsistencias_dominio.xlsx")

    try:
        summary = autofix_rule_profile_from_invalid_domains(
            record.rule_profile,
            final_gdf,
            support_report_path=support_report_path,
        )
    except Exception as exc:
        log(f"Erro ao tentar corrigir automaticamente o perfil de regras: {exc}")
        return

    if not summary["changed"]:
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
    log("As novas regras foram salvas para as proximas execucoes. Reprocesse a base se quiser aplicar o ajuste automaticamente neste mesmo arquivo.")



def _save_outputs(final_gdf, record, output_dir, use_configured_final_name=False, persist_dataset=True):
    theme_output_dir = build_theme_output_dir(output_dir, record.theme_folder)
    os.makedirs(theme_output_dir, exist_ok=True)

    if use_configured_final_name:
        base_name = build_final_output_base_name(record)
    else:
        base_name = os.path.splitext(os.path.basename(record.input_path))[0] + "_validado"

    output_path = os.path.join(theme_output_dir, f"{base_name}.gpkg")
    export_gdf = _drop_internal_output_columns(final_gdf)

    if persist_dataset:
        log(f"Salvando resultado em {output_path}")
        write_output_gpkg(
            export_gdf,
            output_path,
            overwrite_existing=True,
        )
        log("Arquivo salvo com sucesso")
    else:
        log(
            "Saida individual omitida porque a consolidacao em grupo esta habilitada "
            "e KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING=False."
        )
        output_path = None

    attr_dup_mask = None
    geom_dup_mask = None
    attr_count = 0
    geom_count = 0
    attr_duplicates = None
    geom_duplicates = None
    ogc_invalid = None
    ogc_invalid_count = 0
    ogc_error_summary = {}

    if ENABLE_ATTRIBUTE_DUPLICATE_REPORT:
        attr_dup_mask = get_attribute_duplicate_mask(final_gdf)
        attr_count = int(attr_dup_mask.sum())
        attr_duplicates = get_attribute_duplicate_records(final_gdf, dup_mask=attr_dup_mask)[0] if attr_count else None

    if ENABLE_GEOMETRIC_DUPLICATE_REPORT:
        geom_dup_mask = get_geometric_duplicate_mask(final_gdf)
        geom_count = int(geom_dup_mask.sum())
        geom_duplicates = get_geometric_duplicate_records(final_gdf, dup_mask=geom_dup_mask)[0] if geom_count else None

    if ENABLE_OGC_INVALID_REPORT:
        ogc_invalid, ogc_invalid_count, ogc_error_summary = get_invalid_ogc_records(final_gdf)

    attr_report = None
    geom_report = None
    ogc_report = None

    if any(
        [
            ENABLE_ATTRIBUTE_DUPLICATE_REPORT and attr_count > 0,
            ENABLE_GEOMETRIC_DUPLICATE_REPORT and geom_count > 0,
            ENABLE_OGC_INVALID_REPORT and ogc_invalid_count > 0,
        ]
    ):
        (
            attr_report,
            geom_report,
            ogc_report,
            _,
            _,
            _,
            _,
        ) = export_duplicate_reports(
            final_gdf,
            theme_output_dir,
            base_name,
            attr_duplicates=attr_duplicates,
            attr_count=attr_count,
            geom_duplicates=geom_duplicates,
            geom_count=geom_count,
            ogc_invalid=ogc_invalid,
            ogc_invalid_count=ogc_invalid_count,
            ogc_error_summary=ogc_error_summary,
        )

    if ENABLE_ATTRIBUTE_DUPLICATE_REPORT:
        log(f"Relatorio duplicados atributos: {attr_report or 'nao gerado'}")
    else:
        log("Relatorio duplicados atributos: desabilitado em settings.py")

    if ENABLE_GEOMETRIC_DUPLICATE_REPORT:
        log(f"Relatorio duplicados geometricos: {geom_report or 'nao gerado'}")
    else:
        log("Relatorio duplicados geometricos: desabilitado em settings.py")

    if ENABLE_OGC_INVALID_REPORT:
        if ogc_report:
            log(f"Relatorio geometrias invalidas OGC: {ogc_report}")
        else:
            log("Relatorio geometrias invalidas OGC: nao gerado")
    else:
        log("Relatorio geometrias invalidas OGC: desabilitado em settings.py")

    log(f"Total duplicados atributos: {attr_count}")
    log(f"Total duplicados geometricos: {geom_count}")
    log(f"Total geometrias invalidas OGC: {ogc_invalid_count}")
    safe_null_count = int(
        final_gdf[INTERNAL_SAFE_REPAIR_FLAG].fillna(False).sum()
    ) if INTERNAL_SAFE_REPAIR_FLAG in final_gdf.columns else 0
    if safe_null_count:
        log(f"Total geometrias nulas por reparo seguro: {safe_null_count}")
    if ENABLE_OGC_INVALID_REPORT and ogc_error_summary:
        log("Resumo erros OGC:")
        for erro, quantidade in ogc_error_summary.items():
            log(f"  {quantidade}x - {erro}")

    return output_path


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
            gdf = _load_and_prepare_input(record)
    except Exception as exc:
        log(f"Erro ao carregar ou validar arquivo de entrada: {exc}")
        return 0, None, None

    try:
        with timed_log_step("Carregamento do perfil de regras"):
            set_active_rule_profile(record.rule_profile)
    except Exception as exc:
        log(f"Erro ao carregar perfil de regras: {exc}")
        return 0, None, None

    columns = list(gdf.columns)
    log_dataset_overview(gdf)

    with timed_log_step("Preparacao do mapeamento de validacao"):
        mapping = _build_auto_mapping(columns)
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
        final_gdf = _assign_output_identifiers(final_gdf, id_start)
    with timed_log_step("Reparo de geometrias invalidas"):
        final_gdf = _repair_invalid_geometries(final_gdf)
    with timed_log_step("Complemento de metricas espaciais"):
        final_gdf = fill_missing_spatial_metrics(final_gdf)
    with timed_log_step("Ajuste automatico do perfil de regras"):
        _autofix_rule_profile_if_needed(final_gdf, record, output_dir)

    try:
        with timed_log_step("Persistencia de saidas"):
            output_path = _save_outputs(
                final_gdf,
                record,
                output_dir,
                use_configured_final_name=use_configured_final_name,
                persist_dataset=persist_individual_output,
            )
    except Exception as exc:
        log(f"Erro ao salvar arquivo: {exc}")
        return 0, None, None

    return len(final_gdf), output_path, final_gdf


def main():
    log("DATA PIPELINE")
    log("Modo de execucao: fila automatica por planilha ingest")

    try:
        processing_queue, queue_issues, queue_summary = load_processing_queue()
    except Exception as exc:
        log(f"Erro ao carregar a fila ingest: {exc}")
        return

    log_queue_summary(queue_summary, queue_issues)

    if not processing_queue:
        log("Nenhum arquivo elegivel encontrado para iniciar a esteira.")
        return

    output_dir = str(OUTPUT_BASE)
    os.makedirs(output_dir, exist_ok=True)

    group_expected_counts = defaultdict(int)
    for record in processing_queue:
        group_expected_counts[_build_processing_group_key(record)] += 1

    next_group_id_start = defaultdict(lambda: 1)
    group_append_started = defaultdict(bool)
    context_log_started = defaultdict(bool)

    for record in processing_queue:
        group_key = _build_processing_group_key(record)
        is_grouped_consolidation = (
            ENABLE_GROUP_CONSOLIDATION and group_expected_counts[group_key] > 1
        )
        set_context_log(
            _build_group_log_path(record, output_dir),
            reset=not context_log_started[group_key],
        )
        context_log_started[group_key] = True
        processed_count, output_path, final_gdf = process_record(
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
        if processed_count:
            next_group_id_start[group_key] += processed_count
        if (
            ENABLE_GROUP_CONSOLIDATION
            and group_expected_counts[group_key] > 1
            and final_gdf is not None
        ):
            _append_group_consolidated_output(
                record,
                final_gdf,
                output_dir,
                append=group_append_started[group_key],
            )
            group_append_started[group_key] = True

        clear_context_log()

    log("Processamento finalizado")


if __name__ == "__main__":
    main()

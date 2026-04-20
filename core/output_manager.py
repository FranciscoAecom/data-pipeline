import os

from core.dataset_io import write_output_gpkg
from core.geometry_repair import INTERNAL_SAFE_REPAIR_FLAG
from core.naming import build_final_output_base_name, build_theme_output_dir
from core.reporting import export_duplicate_reports
from core.spatial.spatial_functions import (
    get_geometric_duplicate_mask,
    get_geometric_duplicate_records,
    get_invalid_ogc_records,
)
from core.utils import log
from core.validation.validation_functions import (
    get_attribute_duplicate_mask,
    get_attribute_duplicate_records,
)
from settings import (
    ENABLE_ATTRIBUTE_DUPLICATE_REPORT,
    ENABLE_GEOMETRIC_DUPLICATE_REPORT,
    ENABLE_OGC_INVALID_REPORT,
)


def build_processing_group_key(record):
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


def build_group_log_path(record, output_dir):
    theme_output_dir = build_theme_output_dir(output_dir, record.theme_folder)
    os.makedirs(theme_output_dir, exist_ok=True)
    base_name = build_final_output_base_name(record)
    return os.path.join(theme_output_dir, f"{base_name}.txt")


def assign_output_identifiers(gdf, start_id):
    identified = gdf.copy()
    if "acm_id" not in identified.columns:
        identified["acm_id"] = range(start_id, start_id + len(identified))
    identified["fid"] = identified["acm_id"]
    return identified


def drop_internal_output_columns(gdf):
    internal_columns = [INTERNAL_SAFE_REPAIR_FLAG]
    existing_columns = [column for column in internal_columns if column in gdf.columns]
    if not existing_columns:
        return gdf
    return gdf.drop(columns=existing_columns)


def append_group_consolidated_output(record, final_gdf, output_dir, append=False):
    merged_output_path = _build_group_merged_output_path(record, output_dir)
    export_gdf = drop_internal_output_columns(final_gdf)
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


def save_outputs(final_gdf, record, output_dir, use_configured_final_name=False, persist_dataset=True):
    theme_output_dir = build_theme_output_dir(output_dir, record.theme_folder)
    os.makedirs(theme_output_dir, exist_ok=True)

    if use_configured_final_name:
        base_name = build_final_output_base_name(record)
    else:
        base_name = os.path.splitext(os.path.basename(record.input_path))[0] + "_validado"

    output_path = os.path.join(theme_output_dir, f"{base_name}.gpkg")
    export_gdf = drop_internal_output_columns(final_gdf)

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
        if attr_count:
            attr_duplicates = get_attribute_duplicate_records(
                final_gdf,
                dup_mask=attr_dup_mask,
            )[0]

    if ENABLE_GEOMETRIC_DUPLICATE_REPORT:
        geom_dup_mask = get_geometric_duplicate_mask(final_gdf)
        geom_count = int(geom_dup_mask.sum())
        if geom_count:
            geom_duplicates = get_geometric_duplicate_records(
                final_gdf,
                dup_mask=geom_dup_mask,
            )[0]

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

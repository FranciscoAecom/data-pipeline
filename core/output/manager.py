from core.dataset_io import write_output_gpkg
from core.geometry_repair import INTERNAL_SAFE_REPAIR_FLAG
from core.output.paths import (
    build_group_log_path,
    build_group_merged_output_path,
    build_processing_group_key,
    resolve_output_path,
)
from core.output.quality import (
    OutputQualitySummary,
    build_output_quality_summary,
    log_output_quality_summary,
)
from core.utils import log


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
    merged_output_path = build_group_merged_output_path(record, output_dir)
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
    theme_output_dir, base_name, output_path = resolve_output_path(
        record,
        output_dir,
        use_configured_final_name,
    )
    export_gdf = drop_internal_output_columns(final_gdf)
    persisted_output_path = persist_output_dataset(export_gdf, output_path, persist_dataset)
    quality_summary = build_output_quality_summary(final_gdf, theme_output_dir, base_name)
    log_output_quality_summary(quality_summary)

    return persisted_output_path


def persist_output_dataset(export_gdf, output_path, persist_dataset):
    if persist_dataset:
        log(f"Salvando resultado em {output_path}")
        write_output_gpkg(
            export_gdf,
            output_path,
            overwrite_existing=True,
        )
        log("Arquivo salvo com sucesso")
        return output_path

    log(
        "Saida individual omitida porque a consolidacao em grupo esta habilitada "
        "e KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING=False."
    )
    return None

import geopandas as gpd

from core.processing.batch import process_in_batches
from core.processing.context import replace_context
from core.utils import log


def run_pipeline_step(context):
    final_gdf, _ = process_in_batches(
        context.gdf,
        context.mapping,
        id_start=context.id_start,
        project_name=context.project_name,
        rule_profile=context.rule_profile,
        optional_functions=context.optional_functions,
        validation_session=getattr(context, "validation_session", None),
    )
    log(f"Resultado final: {len(final_gdf)} registros processados")
    return replace_context(context, final_gdf=_ensure_final_geodataframe(final_gdf))


def _ensure_final_geodataframe(final_gdf):
    try:
        return gpd.GeoDataFrame(final_gdf, geometry="geometry", crs=final_gdf.crs)
    except Exception as exc:
        log(f"Erro ao garantir GeoDataFrame com geometria: {exc}")
        return final_gdf

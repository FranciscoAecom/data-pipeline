import geopandas as gpd

from core.batch_processor import process_in_batches
from core.execution_context import replace_context
from core.geometry_repair import repair_invalid_geometries
from core.input_preparation import load_and_prepare_input
from core.output.manager import assign_output_identifiers
from core.rule_runtime import build_auto_mapping
from core.spatial.regional_bounds import enforce_car_state_bounds
from core.spatial.spatial_functions import fill_missing_spatial_metrics
from core.utils import log, timed_log_step
from core.rules.engine import load_rule_profile
from core.validation.tabular_schema import get_tabular_schema, normalize_input_schema
from core.validation.validation_functions import (
    prepare_validate_shapefile_attribute_mappings,
)


def load_input_step(context):
    return replace_context(context, gdf=load_and_prepare_input(context.record))


def attach_rule_profile_step(context):
    rule_profile = load_rule_profile(
        context.rule_profile_name,
        optional_functions=context.optional_functions,
    )
    return replace_context(context, rule_profile=rule_profile)


def validate_input_schema_step(context):
    if get_tabular_schema(context.rule_profile) is None:
        return context

    gdf, errors = normalize_input_schema(
        context.record,
        context.gdf,
        context.rule_profile,
    )
    if errors:
        message = "\n".join(f"- {error}" for error in errors)
        raise ValueError(
            f"Schema tabular invalido para {context.record.theme_folder}:\n{message}"
        )

    log(f"Validacao de schema tabular OK para {context.record.theme_folder}.")
    return replace_context(context, gdf=gdf)


def prepare_mapping_step(context):
    mapping = build_auto_mapping(list(context.gdf.columns), context.rule_profile)
    prepare_validate_shapefile_attribute_mappings(
        context.gdf,
        mapping,
        context.rule_profile,
    )
    return replace_context(context, mapping=mapping)


def run_pipeline_step(context):
    final_gdf, _ = process_in_batches(
        context.gdf,
        context.mapping,
        id_start=context.id_start,
        project_name=_project_name(context),
        rule_profile=context.rule_profile,
        optional_functions=context.optional_functions,
    )
    log(f"Resultado final: {len(final_gdf)} registros processados")
    return replace_context(context, final_gdf=_ensure_final_geodataframe(final_gdf))


def postprocess_step(context):
    final_gdf = context.final_gdf
    with timed_log_step("Atribuicao de identificadores finais"):
        final_gdf = assign_output_identifiers(final_gdf, context.id_start)
    with timed_log_step("Reparo de geometrias invalidas"):
        final_gdf = repair_invalid_geometries(final_gdf)
    if _project_name(context) in {"app_car", "reserva_legal_car"}:
        with timed_log_step("Validacao de bbox regional CAR"):
            final_gdf = enforce_car_state_bounds(final_gdf, context.record).gdf
    with timed_log_step("Complemento de metricas espaciais"):
        final_gdf = fill_missing_spatial_metrics(final_gdf)
    return replace_context(context, final_gdf=final_gdf)


def _ensure_final_geodataframe(final_gdf):
    try:
        return gpd.GeoDataFrame(final_gdf, geometry="geometry", crs=final_gdf.crs)
    except Exception as exc:
        log(f"Erro ao garantir GeoDataFrame com geometria: {exc}")
        return final_gdf


def _project_name(context):
    if hasattr(context, "project_name"):
        return context.project_name
    return context.project_config["project_name"]

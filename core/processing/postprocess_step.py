from core.execution_context import replace_context
from core.geometry_repair import repair_invalid_geometries
from core.output.identifiers import assign_output_identifiers
from core.processing.context import project_name
from core.spatial.metrics import fill_missing_spatial_metrics
from core.spatial.regional_bounds import enforce_car_state_bounds
from core.utils import timed_log_step


def postprocess_step(context):
    final_gdf = context.final_gdf
    with timed_log_step("Atribuicao de identificadores finais"):
        final_gdf = assign_output_identifiers(final_gdf, context.id_start)
    with timed_log_step("Reparo de geometrias invalidas"):
        final_gdf = repair_invalid_geometries(final_gdf)
    if project_name(context) in {"app_car", "reserva_legal_car"}:
        with timed_log_step("Validacao de bbox regional CAR"):
            final_gdf = enforce_car_state_bounds(final_gdf, context.record).gdf
    with timed_log_step("Complemento de metricas espaciais"):
        final_gdf = fill_missing_spatial_metrics(final_gdf)
    return replace_context(context, final_gdf=final_gdf)

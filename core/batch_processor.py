import geopandas as gpd
import pandas as pd

from core.pipeline import MANDATORY_FUNCTIONS, run_pipeline
from settings import BATCH_SIZE
from core.utils import log
from core.validation.summary import log_validation_summary


def _new_stats_total():
    return {
        "forced_to_2d": 0,
        "reprojected_to_wgs84": 0,
        "optional_functions": [],
        "_optional_seen": set(),
    }


def _merge_batch_stats(stats_total, batch_stats):
    stats_total["forced_to_2d"] += batch_stats.get("forced_to_2d", 0)
    stats_total["reprojected_to_wgs84"] += batch_stats.get("reprojected_to_wgs84", 0)

    for func_name in batch_stats.get("optional_functions", []):
        if func_name in stats_total["_optional_seen"]:
            continue
        stats_total["_optional_seen"].add(func_name)
        stats_total["optional_functions"].append(func_name)


def _finalize_stats(stats_total):
    return {
        "forced_to_2d": stats_total["forced_to_2d"],
        "reprojected_to_wgs84": stats_total["reprojected_to_wgs84"],
        "optional_functions": stats_total["optional_functions"],
    }


def process_in_batches(
    gdf,
    mapping,
    batch_size=BATCH_SIZE,
    id_start=1,
    project_name=None,
    rule_profile=None,
    optional_functions=None,
    validation_session=None,
):
    total = len(gdf)
    log(f"Iniciando processamento em batches ({total} registros)")

    if total == 0:
        empty_gdf = gpd.GeoDataFrame(gdf.copy(), geometry="geometry", crs=gdf.crs)
        log("Nenhum registro encontrado para processamento em batch.")
        return empty_gdf, {"forced_to_2d": 0, "reprojected_to_wgs84": 0, "optional_functions": []}

    results = []
    stats_total = _new_stats_total()

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        log(f"Processando registros {start} ate {end}")

        batch = gdf.iloc[start:end].copy()
        processed, batch_stats = run_pipeline(
            batch,
            mapping,
            id_start=id_start + start,
            project_name=project_name,
            rule_profile=rule_profile,
            optional_functions=optional_functions,
            validation_session=validation_session,
        )

        results.append(processed)
        _merge_batch_stats(stats_total, batch_stats)

    log("Unindo batches...")

    final_gdf = gpd.GeoDataFrame(
        pd.concat(results, ignore_index=True),
        geometry="geometry",
        crs=results[0].crs,
    )

    final_stats = _finalize_stats(stats_total)

    log(f"Total processado: {len(final_gdf)} registros")
    log("Resumo final do processamento:")
    log(f"Funcoes obrigatorias executadas: {', '.join(MANDATORY_FUNCTIONS)}")
    if final_stats["optional_functions"]:
        log(f"Funcoes opcionais executadas: {', '.join(final_stats['optional_functions'])}")
    if final_stats["forced_to_2d"] > 0:
        log(f"Forcado para 2D: {final_stats['forced_to_2d']} geometria(s) com Z/M ajustada(s)")
    if final_stats["reprojected_to_wgs84"] > 0:
        log("Reprojetado para WGS84")

    log_validation_summary(validation_session=validation_session)

    return final_gdf, final_stats

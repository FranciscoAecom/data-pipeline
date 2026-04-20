from core.spatial.spatial_functions import get_finite_geometry_mask, repair_geometry_safely
from core.utils import log


INTERNAL_SAFE_REPAIR_FLAG = "__internal_geom_null_safe_repair"


def repair_invalid_geometries(gdf):
    if "geometry" not in gdf.columns:
        log(
            "Erro: coluna 'geometry' ausente no GeoDataFrame final. "
            "O arquivo GPKG pode nao conter feicoes."
        )
        return gdf

    repair_flag_column = INTERNAL_SAFE_REPAIR_FLAG
    if repair_flag_column not in gdf.columns:
        gdf[repair_flag_column] = False

    geometry = gdf.geometry

    if geometry.is_empty.all():
        log("Aviso: todas as geometrias estao vazias.")

    invalid_mask = geometry.notna() & (~geometry.is_valid)
    invalid_geoms = int(invalid_mask.sum())
    if invalid_geoms <= 0:
        return gdf

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

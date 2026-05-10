import math

import numpy as np
import pandas as pd
from shapely import force_2d, get_coordinate_dimension
from shapely.validation import make_valid

from core.utils import log


INTERNAL_SAFE_REPAIR_FLAG = "__internal_geom_null_safe_repair"


def force_geometry_2d(gdf):
    if "geometry" not in gdf.columns:
        return gdf, 0

    geometry = gdf.geometry
    candidate_mask = geometry.notna() & (~geometry.is_empty)
    if not candidate_mask.any():
        return gdf, 0

    dimensions = geometry.loc[candidate_mask].apply(get_coordinate_dimension)
    convert_mask = pd.Series(False, index=gdf.index)
    convert_mask.loc[candidate_mask] = dimensions > 2

    converted_count = int(convert_mask.sum())
    if not converted_count:
        return gdf, 0

    converted_geometry = geometry.copy()
    converted_geometry.loc[convert_mask] = converted_geometry.loc[convert_mask].apply(force_2d)
    gdf["geometry"] = converted_geometry
    return gdf, converted_count


def geometry_bounds_are_finite(geom):
    if geom is None:
        return False

    try:
        if geom.is_empty:
            return False
    except Exception:
        return False

    try:
        bounds = geom.bounds
    except Exception:
        return False

    if not bounds:
        return False

    return all(math.isfinite(value) for value in bounds if value is not None)


def get_finite_geometry_mask(geometry):
    base_mask = geometry.notna() & (~geometry.is_empty)
    if not base_mask.any():
        return base_mask

    try:
        bounds = geometry.bounds
        finite_bounds_mask = pd.Series(
            np.isfinite(bounds.to_numpy()).all(axis=1),
            index=geometry.index,
        )
        return base_mask & finite_bounds_mask
    except Exception:
        return geometry.apply(geometry_bounds_are_finite)


def safe_prepare_invalid_geometry_for_measurement(geom):
    if geom is None or not geometry_bounds_are_finite(geom):
        return None

    prepared = geom

    if prepared is None:
        return None

    try:
        if prepared.is_empty:
            return None
    except Exception:
        return None

    try:
        if not prepared.is_valid:
            prepared = make_valid(prepared)
    except Exception:
        try:
            prepared = prepared.buffer(0)
        except Exception:
            return None

    if prepared is None or not geometry_bounds_are_finite(prepared):
        return None

    try:
        if prepared.is_empty or not prepared.is_valid:
            return None
    except Exception:
        return None

    return prepared


def repair_geometry_safely(geom):
    if geom is None:
        return None

    try:
        if geom.is_empty:
            return geom
    except Exception:
        return None

    if not geometry_bounds_are_finite(geom):
        return None

    repaired = geom

    try:
        if not repaired.is_valid:
            repaired = make_valid(repaired)
    except Exception:
        try:
            repaired = repaired.buffer(0)
        except Exception:
            return None

    try:
        if repaired is None or repaired.is_empty or not geometry_bounds_are_finite(repaired):
            return None
        if not repaired.is_valid:
            fallback = repaired.buffer(0)
            if fallback is not None:
                repaired = fallback
    except Exception:
        return None

    return repaired


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


__all__ = [
    "INTERNAL_SAFE_REPAIR_FLAG",
    "force_geometry_2d",
    "geometry_bounds_are_finite",
    "get_finite_geometry_mask",
    "repair_geometry_safely",
    "repair_invalid_geometries",
    "safe_prepare_invalid_geometry_for_measurement",
]

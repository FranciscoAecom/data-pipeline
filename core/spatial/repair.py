import math

import numpy as np
import pandas as pd
from shapely import force_2d, get_coordinate_dimension
from shapely.validation import make_valid


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


__all__ = [
    "INTERNAL_SAFE_REPAIR_FLAG",
    "force_geometry_2d",
    "geometry_bounds_are_finite",
    "get_finite_geometry_mask",
    "repair_geometry_safely",
    "safe_prepare_invalid_geometry_for_measurement",
]

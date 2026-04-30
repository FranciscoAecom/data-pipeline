import re
import warnings
from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from settings import CRS_EQUAL_AREA
from core.utils import log


STATE_BOUNDS = {
    "ac": (-74.1, -11.2, -66.5, -7.0),
    "al": (-38.4, -10.7, -35.1, -8.7),
    "am": (-74.0, -10.1, -56.0, 2.4),
    "ap": (-54.9, -1.4, -49.6, 4.6),
    "ba": (-46.8, -18.5, -37.2, -8.4),
    "ce": (-41.5, -7.9, -37.1, -2.7),
    "df": (-48.4, -16.1, -47.3, -15.4),
    "es": (-41.9, -21.4, -39.6, -17.8),
    "go": (-53.4, -19.6, -45.8, -12.3),
    "ma": (-49.0, -11.0, -41.0, 0.0),
    "mg": (-52.5, -23.0, -39.8, -14.0),
    "ms": (-58.3, -24.2, -50.8, -17.0),
    "mt": (-61.8, -18.2, -50.0, -7.2),
    "pa": (-59.0, -10.0, -46.0, 2.7),
    "pb": (-38.9, -8.4, -34.7, -6.0),
    "pe": (-41.5, -9.7, -34.8, -7.2),
    "pi": (-46.9, -11.1, -40.2, -2.7),
    "pr": (-54.8, -26.8, -48.0, -22.2),
    "rj": (-44.9, -23.4, -40.9, -20.7),
    "rn": (-38.8, -7.0, -34.8, -4.8),
    "ro": (-66.9, -13.8, -59.7, -7.8),
    "rr": (-64.9, -1.7, -58.8, 5.4),
    "rs": (-57.8, -33.9, -49.6, -27.0),
    "sc": (-54.0, -29.4, -48.3, -25.8),
    "se": (-38.3, -11.6, -36.3, -9.5),
    "sp": (-53.2, -25.4, -43.9, -19.7),
    "to": (-50.9, -13.7, -45.5, -5.0),
}


@dataclass(frozen=True)
class RegionalBoundsResult:
    gdf: gpd.GeoDataFrame
    state: str | None
    clipped_count: int
    outside_without_intersection_count: int


def infer_state_code(record) -> str | None:
    candidates = [
        getattr(record, "theme_folder", None),
        getattr(record, "rule_profile", None),
        getattr(record, "input_path", None),
        getattr(record, "source_path", None),
        getattr(record, "theme", None),
    ]

    for value in candidates:
        if value is None:
            continue
        text = str(value).lower()
        match = re.search(
            r"(?:app_car|rl_car|reserva_legal_car|pol_pcd_app_car|pol_pcd_rl_car|sld_pcd_app_car|sld_pcd_rl_car|md_pcd_app_car|md_pcd_rl_car)_([a-z]{2})(?:_|\.|$)",
            text,
        )
        if match and match.group(1) in STATE_BOUNDS:
            return match.group(1)

    return None


def _outside_bounds_mask(geometry, bounds):
    valid_mask = geometry.notna() & (~geometry.is_empty)
    if not valid_mask.any():
        return pd.Series(False, index=geometry.index)

    geom_bounds = geometry.bounds
    minx, miny, maxx, maxy = bounds
    return (
        valid_mask
        & (
            (geom_bounds["minx"] < minx)
            | (geom_bounds["maxx"] > maxx)
            | (geom_bounds["miny"] < miny)
            | (geom_bounds["maxy"] > maxy)
        )
    )


def _clip_geometry_to_bounds(geom, bounds_geom):
    if geom is None:
        return None
    if geom.is_empty:
        return geom

    clipped = geom.intersection(bounds_geom)
    if clipped.is_empty:
        return None
    return clipped


def _recalculate_spatial_metrics_for_mask(gdf, mask):
    if not mask.any():
        return gdf

    geometry = gdf.loc[mask].geometry
    finite_mask = geometry.notna() & (~geometry.is_empty)
    if not finite_mask.any():
        return gdf

    target_index = geometry.loc[finite_mask].index
    target_geometry = geometry.loc[finite_mask]

    if "acm_a_ha" in gdf.columns or "acm_prm_km" in gdf.columns:
        projected = gpd.GeoSeries(target_geometry, crs=gdf.crs).to_crs(CRS_EQUAL_AREA)
        if "acm_a_ha" in gdf.columns:
            gdf.loc[target_index, "acm_a_ha"] = (projected.area / 10000).round(6)
        if "acm_prm_km" in gdf.columns:
            gdf.loc[target_index, "acm_prm_km"] = (projected.length / 1000).round(6)

    if "acm_long" in gdf.columns or "acm_lat" in gdf.columns:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r".*Geometry is in a geographic CRS.*",
                category=UserWarning,
            )
            centroids = target_geometry.centroid
        if "acm_long" in gdf.columns:
            gdf.loc[target_index, "acm_long"] = centroids.x.round(6)
        if "acm_lat" in gdf.columns:
            gdf.loc[target_index, "acm_lat"] = centroids.y.round(6)

    return gdf


def enforce_car_state_bounds(gdf, record) -> RegionalBoundsResult:
    state = infer_state_code(record)
    if state is None or state not in STATE_BOUNDS or "geometry" not in gdf.columns:
        return RegionalBoundsResult(gdf=gdf, state=state, clipped_count=0, outside_without_intersection_count=0)

    bounds = STATE_BOUNDS[state]
    outside_mask = _outside_bounds_mask(gdf.geometry, bounds)
    if not outside_mask.any():
        return RegionalBoundsResult(gdf=gdf, state=state, clipped_count=0, outside_without_intersection_count=0)

    bounds_geom = box(*bounds)
    corrected = gdf.copy()
    clipped_count = 0
    outside_without_intersection_count = 0
    changed_mask = pd.Series(False, index=gdf.index)

    for idx, geom in gdf.loc[outside_mask].geometry.items():
        clipped = _clip_geometry_to_bounds(geom, bounds_geom)
        if clipped is None:
            outside_without_intersection_count += 1
            continue
        corrected.at[idx, "geometry"] = clipped
        changed_mask.loc[idx] = True
        clipped_count += 1

    if clipped_count:
        corrected = gpd.GeoDataFrame(corrected, geometry="geometry", crs=gdf.crs)
        corrected = _recalculate_spatial_metrics_for_mask(corrected, changed_mask)
        log(
            "BBox regional CAR: "
            f"{clipped_count} geometria(s) recortada(s) para o envelope da UF {state.upper()}."
        )

    if outside_without_intersection_count:
        log(
            "BBox regional CAR: "
            f"{outside_without_intersection_count} geometria(s) fora do envelope da UF {state.upper()} "
            "nao foram alteradas porque nao intersectam o estado."
        )

    return RegionalBoundsResult(
        gdf=corrected,
        state=state,
        clipped_count=clipped_count,
        outside_without_intersection_count=outside_without_intersection_count,
    )


def enforce_app_car_state_bounds(gdf, record) -> RegionalBoundsResult:
    return enforce_car_state_bounds(gdf, record)

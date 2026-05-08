import math

import geopandas as gpd
import pandas as pd
from pyproj import CRS
from shapely.errors import GEOSException

from settings import CRS_WGS84, SPATIAL_TRANSFORM_CHUNK_SIZE


__all__ = [
    "geometry_series_matches_crs_coordinate_range",
    "is_geographic_crs",
    "is_wgs84_crs",
    "is_within_geographic_bounds",
    "reproject_shapefile",
    "transform_geometry_chunk",
    "transform_geometry_in_chunks",
]


def is_wgs84_crs(crs):
    if crs is None:
        return False

    try:
        normalized_crs = CRS.from_user_input(crs)
    except Exception:
        return False

    epsg = normalized_crs.to_epsg()
    if epsg == 4326:
        return True

    try:
        return normalized_crs == CRS.from_epsg(4326)
    except Exception:
        return False


def is_geographic_crs(crs):
    if crs is None:
        return False

    try:
        normalized_crs = CRS.from_user_input(crs)
    except Exception:
        return False

    try:
        return bool(normalized_crs.is_geographic)
    except Exception:
        return False


def is_within_geographic_bounds(bounds):
    if bounds is None or len(bounds) != 4:
        return False

    minx, miny, maxx, maxy = bounds
    values = (minx, miny, maxx, maxy)
    if not all(value is not None and math.isfinite(value) for value in values):
        return False

    return (
        -180.0 <= minx <= 180.0
        and -180.0 <= maxx <= 180.0
        and -90.0 <= miny <= 90.0
        and -90.0 <= maxy <= 90.0
    )


def geometry_series_matches_crs_coordinate_range(geometry, crs):
    if not is_geographic_crs(crs):
        return pd.Series(True, index=geometry.index)

    base_mask = geometry.notna() & (~geometry.is_empty)
    if not base_mask.any():
        return pd.Series(True, index=geometry.index)

    try:
        bounds = geometry.bounds
        compatible_mask = (
            bounds.notna().all(axis=1)
            & bounds["minx"].between(-180.0, 180.0)
            & bounds["maxx"].between(-180.0, 180.0)
            & bounds["miny"].between(-90.0, 90.0)
            & bounds["maxy"].between(-90.0, 90.0)
        )
        compatible_mask = pd.Series(compatible_mask, index=geometry.index)
        compatible_mask.loc[~base_mask] = True
        return compatible_mask
    except Exception:
        compatible_mask = pd.Series(True, index=geometry.index)
        for idx, geom in geometry.items():
            if geom is None:
                continue
            try:
                if geom.is_empty:
                    continue
            except Exception:
                compatible_mask.loc[idx] = False
                continue

            try:
                compatible_mask.loc[idx] = is_within_geographic_bounds(geom.bounds)
            except Exception:
                compatible_mask.loc[idx] = False
        return compatible_mask


def transform_geometry_chunk(geometry_chunk, target_crs):
    if geometry_chunk.empty:
        return gpd.GeoSeries(geometry_chunk, crs=target_crs)

    series = gpd.GeoSeries(geometry_chunk, crs=geometry_chunk.crs)

    try:
        return series.to_crs(target_crs)
    except (GEOSException, MemoryError):
        if len(series) <= 1:
            raise

        midpoint = len(series) // 2
        left = transform_geometry_chunk(series.iloc[:midpoint], target_crs)
        right = transform_geometry_chunk(series.iloc[midpoint:], target_crs)
        return gpd.GeoSeries(pd.concat([left, right]).sort_index(), crs=target_crs)


def transform_geometry_in_chunks(
    geometry,
    target_crs,
    chunk_size=SPATIAL_TRANSFORM_CHUNK_SIZE,
):
    if geometry.empty:
        return gpd.GeoSeries(geometry, crs=target_crs)

    transformed_chunks = []
    total = len(geometry)

    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        chunk = geometry.iloc[start:end]
        transformed_chunks.append(transform_geometry_chunk(chunk, target_crs))

    return gpd.GeoSeries(pd.concat(transformed_chunks).sort_index(), crs=target_crs)


def reproject_shapefile(gdf):
    if gdf.crs and not is_wgs84_crs(gdf.crs):
        projected_geometry = transform_geometry_in_chunks(gdf.geometry, CRS_WGS84)
        gdf = gdf.copy()
        gdf["geometry"] = projected_geometry
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=CRS_WGS84)
        return gdf, True
    return gdf, False

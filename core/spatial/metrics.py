import warnings

import pandas as pd

from core.spatial.crs import transform_geometry_in_chunks
from core.spatial.repair import get_finite_geometry_mask
from core.utils import log
from settings import CRS_EQUAL_AREA


def calculate_area_and_perimeter(gdf):
    geometry = gdf.geometry
    measurable_mask = get_finite_geometry_mask(geometry)

    area_series = pd.Series(float("nan"), index=gdf.index, dtype="float64")
    perimeter_series = pd.Series(float("nan"), index=gdf.index, dtype="float64")

    if measurable_mask.any():
        measurable_geometry = geometry.loc[measurable_mask]
        projected_geometry = transform_geometry_in_chunks(
            measurable_geometry,
            CRS_EQUAL_AREA,
        )
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r".*invalid value encountered in area.*",
                category=RuntimeWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message=r".*invalid value encountered in length.*",
                category=RuntimeWarning,
            )
            area_series.loc[measurable_mask] = (projected_geometry.area / 10000).round(6)
            perimeter_series.loc[measurable_mask] = (projected_geometry.length / 1000).round(6)

    skipped_count = int((~measurable_mask).sum())
    if skipped_count:
        log(
            "Aviso: "
            f"{skipped_count} geometria(s) nao puderam ser medidas com seguranca; "
            "acm_a_ha e acm_prm_km ficaram vazios nesses registros."
        )

    gdf["acm_a_ha"] = area_series
    gdf["acm_prm_km"] = perimeter_series
    return gdf


def calculate_area_hectares(gdf):
    if "acm_a_ha" not in gdf.columns or "acm_prm_km" not in gdf.columns:
        gdf = calculate_area_and_perimeter(gdf)
    elif "acm_a_ha" not in gdf.columns:
        geometry = gdf.geometry
        measurable_mask = get_finite_geometry_mask(geometry)
        area_series = pd.Series(float("nan"), index=gdf.index, dtype="float64")
        if measurable_mask.any():
            projected_geometry = transform_geometry_in_chunks(
                geometry.loc[measurable_mask],
                CRS_EQUAL_AREA,
            )
            area_series.loc[measurable_mask] = (projected_geometry.area / 10000).round(6)
        gdf["acm_a_ha"] = area_series
    return gdf


def calculate_perimeter_km(gdf):
    if "acm_a_ha" not in gdf.columns or "acm_prm_km" not in gdf.columns:
        gdf = calculate_area_and_perimeter(gdf)
    elif "acm_prm_km" not in gdf.columns:
        geometry = gdf.geometry
        measurable_mask = get_finite_geometry_mask(geometry)
        perimeter_series = pd.Series(float("nan"), index=gdf.index, dtype="float64")
        if measurable_mask.any():
            projected_geometry = transform_geometry_in_chunks(
                geometry.loc[measurable_mask],
                CRS_EQUAL_AREA,
            )
            perimeter_series.loc[measurable_mask] = (projected_geometry.length / 1000).round(6)
        gdf["acm_prm_km"] = perimeter_series
    return gdf


def add_centroid_coordinates(gdf):
    geometry = gdf.geometry
    centroid_mask = get_finite_geometry_mask(geometry)

    longitudes = pd.Series(float("nan"), index=gdf.index, dtype="float64")
    latitudes = pd.Series(float("nan"), index=gdf.index, dtype="float64")

    if centroid_mask.any():
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r".*Geometry is in a geographic CRS.*",
                category=UserWarning,
            )
            centroids = geometry.loc[centroid_mask].centroid
        longitudes.loc[centroid_mask] = centroids.x.round(6)
        latitudes.loc[centroid_mask] = centroids.y.round(6)

    gdf["acm_long"] = longitudes
    gdf["acm_lat"] = latitudes
    return gdf


def fill_missing_spatial_metrics(gdf):
    if "geometry" not in gdf.columns:
        return gdf

    needs_area = "acm_a_ha" in gdf.columns
    needs_perimeter = "acm_prm_km" in gdf.columns
    needs_long = "acm_long" in gdf.columns
    needs_lat = "acm_lat" in gdf.columns

    if not any([needs_area, needs_perimeter, needs_long, needs_lat]):
        return gdf

    missing_mask = gdf.geometry.notna()
    if needs_area:
        missing_mask &= gdf["acm_a_ha"].isna()
    if needs_perimeter:
        missing_mask |= gdf.geometry.notna() & gdf["acm_prm_km"].isna()
    if needs_long:
        missing_mask |= gdf.geometry.notna() & gdf["acm_long"].isna()
    if needs_lat:
        missing_mask |= gdf.geometry.notna() & gdf["acm_lat"].isna()

    if not missing_mask.any():
        return gdf

    subset = gdf.loc[missing_mask].copy()
    subset = calculate_area_and_perimeter(subset)
    subset = add_centroid_coordinates(subset)

    for column in ["acm_a_ha", "acm_prm_km", "acm_long", "acm_lat"]:
        if column in subset.columns:
            gdf.loc[missing_mask, column] = subset[column]

    return gdf


__all__ = [
    "add_centroid_coordinates",
    "calculate_area_and_perimeter",
    "calculate_area_hectares",
    "calculate_perimeter_km",
    "fill_missing_spatial_metrics",
]

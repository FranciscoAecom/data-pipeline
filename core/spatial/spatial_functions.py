import math
import warnings
from collections import Counter

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import CRS
from shapely import force_2d, get_coordinate_dimension, get_srid
from shapely.errors import GEOSException
from shapely.geometry import (
    GeometryCollection,
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
from shapely.geometry.base import BaseGeometry
from shapely.ops import orient
from shapely.validation import explain_validity, make_valid

from settings import CRS_EQUAL_AREA, CRS_WGS84, SPATIAL_TRANSFORM_CHUNK_SIZE
from core.utils import log

INTERNAL_SAFE_REPAIR_FLAG = "__internal_geom_null_safe_repair"


GEOMETRY_TYPES = {
    "Point",
    "LineString",
    "LinearRing",
    "Polygon",
    "MultiPoint",
    "MultiLineString",
    "MultiPolygon",
    "GeometryCollection",
}


def _is_wgs84_crs(crs):
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


def _is_geographic_crs(crs):
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


def _is_within_geographic_bounds(bounds):
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


def _validate_coordinate_ranges_for_crs(geom, crs, result, label=None):
    if geom is None:
        return

    try:
        if geom.is_empty:
            return
    except Exception:
        return

    current_label = label or geom.geom_type

    if isinstance(geom, MultiPoint):
        for index, item in enumerate(geom.geoms, 1):
            _validate_coordinate_ranges_for_crs(
                item,
                crs,
                result,
                label=f"MultiPoint[{index}]",
            )
        return

    if isinstance(geom, MultiLineString):
        for index, item in enumerate(geom.geoms, 1):
            _validate_coordinate_ranges_for_crs(
                item,
                crs,
                result,
                label=f"MultiLineString[{index}]",
            )
        return

    if isinstance(geom, MultiPolygon):
        for index, item in enumerate(geom.geoms, 1):
            _validate_coordinate_ranges_for_crs(
                item,
                crs,
                result,
                label=f"MultiPolygon[{index}]",
            )
        return

    if isinstance(geom, GeometryCollection):
        for index, item in enumerate(geom.geoms, 1):
            _validate_coordinate_ranges_for_crs(
                item,
                crs,
                result,
                label=f"GeometryCollection[{index}]",
            )
        return

    if not _is_geographic_crs(crs):
        return

    try:
        bounds = geom.bounds
    except Exception:
        _add_error(
            result,
            f"{current_label}: nao foi possivel obter bounds para validar compatibilidade com o CRS {crs}.",
        )
        return

    if _is_within_geographic_bounds(bounds):
        return

    minx, miny, maxx, maxy = bounds
    _add_error(
        result,
        (
            f"{current_label}: bounds incompativeis com o CRS geografico {crs} "
            f"(minx={minx}, miny={miny}, maxx={maxx}, maxy={maxy})."
        ),
    )


def _geometry_series_matches_crs_coordinate_range(geometry, crs):
    if not _is_geographic_crs(crs):
        return pd.Series(True, index=geometry.index)

    base_mask = geometry.notna() & (~geometry.is_empty)
    if not base_mask.any():
        return pd.Series(True, index=geometry.index)

    try:
        bounds = geometry.bounds
        compatible_mask = (
            bounds.notna().all(axis=1)
            & np.isfinite(bounds.to_numpy()).all(axis=1)
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
                compatible_mask.loc[idx] = _is_within_geographic_bounds(geom.bounds)
            except Exception:
                compatible_mask.loc[idx] = False
        return compatible_mask


def _transform_geometry_chunk(geometry_chunk, target_crs):
    if geometry_chunk.empty:
        return gpd.GeoSeries(geometry_chunk, crs=target_crs)

    series = gpd.GeoSeries(geometry_chunk, crs=geometry_chunk.crs)

    try:
        return series.to_crs(target_crs)
    except (GEOSException, MemoryError):
        if len(series) <= 1:
            raise

        midpoint = len(series) // 2
        left = _transform_geometry_chunk(series.iloc[:midpoint], target_crs)
        right = _transform_geometry_chunk(series.iloc[midpoint:], target_crs)
        return gpd.GeoSeries(pd.concat([left, right]).sort_index(), crs=target_crs)


def _transform_geometry_in_chunks(geometry, target_crs, chunk_size=SPATIAL_TRANSFORM_CHUNK_SIZE):
    if geometry.empty:
        return gpd.GeoSeries(geometry, crs=target_crs)

    transformed_chunks = []
    total = len(geometry)

    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        chunk = geometry.iloc[start:end]
        transformed_chunks.append(_transform_geometry_chunk(chunk, target_crs))

    return gpd.GeoSeries(pd.concat(transformed_chunks).sort_index(), crs=target_crs)


def reproject_shapefile(gdf):
    if gdf.crs and not _is_wgs84_crs(gdf.crs):
        projected_geometry = _transform_geometry_in_chunks(gdf.geometry, CRS_WGS84)
        gdf = gdf.copy()
        gdf["geometry"] = projected_geometry
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=CRS_WGS84)
        return gdf, True
    return gdf, False


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


def _geometry_bounds_are_finite(geom):
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


def _get_finite_geometry_mask(geometry):
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
        return geometry.apply(_geometry_bounds_are_finite)


def get_finite_geometry_mask(geometry):
    return _get_finite_geometry_mask(geometry)


def _safe_prepare_invalid_geometry_for_measurement(geom):
    if geom is None or not _geometry_bounds_are_finite(geom):
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

    if prepared is None or not _geometry_bounds_are_finite(prepared):
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

    if not _geometry_bounds_are_finite(geom):
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
        if repaired is None or repaired.is_empty or not _geometry_bounds_are_finite(repaired):
            return None
        if not repaired.is_valid:
            fallback = repaired.buffer(0)
            if fallback is not None:
                repaired = fallback
    except Exception:
        return None

    return repaired


def _calculate_area_and_perimeter(gdf):
    geometry = gdf.geometry
    measurable_mask = _get_finite_geometry_mask(geometry)

    area_series = pd.Series(float("nan"), index=gdf.index, dtype="float64")
    perimeter_series = pd.Series(float("nan"), index=gdf.index, dtype="float64")

    if measurable_mask.any():
        measurable_geometry = geometry.loc[measurable_mask]
        projected_geometry = _transform_geometry_in_chunks(
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
        gdf = _calculate_area_and_perimeter(gdf)
    elif "acm_a_ha" not in gdf.columns:
        geometry = gdf.geometry
        measurable_mask = _get_finite_geometry_mask(geometry)
        area_series = pd.Series(float("nan"), index=gdf.index, dtype="float64")
        if measurable_mask.any():
            projected_geometry = _transform_geometry_in_chunks(
                geometry.loc[measurable_mask],
                CRS_EQUAL_AREA,
            )
            area_series.loc[measurable_mask] = (projected_geometry.area / 10000).round(6)
        gdf["acm_a_ha"] = area_series
    return gdf


def calculate_perimeter_km(gdf):
    if "acm_a_ha" not in gdf.columns or "acm_prm_km" not in gdf.columns:
        gdf = _calculate_area_and_perimeter(gdf)
    elif "acm_prm_km" not in gdf.columns:
        geometry = gdf.geometry
        measurable_mask = _get_finite_geometry_mask(geometry)
        perimeter_series = pd.Series(float("nan"), index=gdf.index, dtype="float64")
        if measurable_mask.any():
            projected_geometry = _transform_geometry_in_chunks(
                geometry.loc[measurable_mask],
                CRS_EQUAL_AREA,
            )
            perimeter_series.loc[measurable_mask] = (projected_geometry.length / 1000).round(6)
        gdf["acm_prm_km"] = perimeter_series
    return gdf


def add_centroid_coordinates(gdf):
    geometry = gdf.geometry
    centroid_mask = _get_finite_geometry_mask(geometry)

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
    subset = _calculate_area_and_perimeter(subset)
    subset = add_centroid_coordinates(subset)

    for column in ["acm_a_ha", "acm_prm_km", "acm_long", "acm_lat"]:
        if column in subset.columns:
            gdf.loc[missing_mask, column] = subset[column]

    return gdf


def _get_geometry_signature(gdf):
    try:
        return gdf.geometry.to_wkb()
    except Exception:
        return gdf.geometry.apply(lambda geom: geom.wkb if geom is not None else None)


def get_geometric_duplicate_mask(gdf):
    geom_signature = _get_geometry_signature(gdf)
    return geom_signature.duplicated(keep=False)


def check_attribute_geometric_duplicates(gdf):
    dup_mask = get_geometric_duplicate_mask(gdf)
    count = int(dup_mask.sum())
    return gdf, count


def get_geometric_duplicate_records(gdf, dup_mask=None):
    if dup_mask is None:
        dup_mask = get_geometric_duplicate_mask(gdf)
    duplicates = gdf[dup_mask].copy()
    count = len(duplicates)
    return duplicates, count


def _can_skip_detailed_ogc_check(gdf, crs_esperado=None, srid_esperado=None, normalizar=False):
    if normalizar or srid_esperado is not None or crs_esperado is not None:
        return False

    geometry = gdf.geometry
    if geometry is None:
        return False

    if geometry.isna().any() or geometry.is_empty.any():
        return False

    geom_types = set(geometry.geom_type.unique())
    if not geom_types.issubset(GEOMETRY_TYPES):
        return False

    coordinate_range_mask = _geometry_series_matches_crs_coordinate_range(geometry, gdf.crs)
    if not bool(coordinate_range_mask.all()):
        return False

    return bool(geometry.is_valid.all())


def get_invalid_ogc_records(gdf, crs_esperado=None, srid_esperado=None, normalizar=False):
    if _can_skip_detailed_ogc_check(
        gdf,
        crs_esperado=crs_esperado,
        srid_esperado=srid_esperado,
        normalizar=normalizar,
    ):
        empty_gdf = gpd.GeoDataFrame(
            columns=list(gdf.columns) + ["ogc_motivo"],
            geometry="geometry",
            crs=gdf.crs,
        )
        return empty_gdf, 0, {}

    repair_flag_column = INTERNAL_SAFE_REPAIR_FLAG
    safe_repair_null_mask = (
        gdf[repair_flag_column].fillna(False).astype(bool)
        if repair_flag_column in gdf.columns
        else pd.Series(False, index=gdf.index)
    )

    invalid_indices = []
    invalid_reasons = []
    error_counter = Counter()

    for idx, geom in gdf.geometry.items():
        if safe_repair_null_mask.loc[idx]:
            reason_text = "Geometria ficou nula apos tentativa de reparo seguro."
            invalid_indices.append(idx)
            invalid_reasons.append(reason_text)
            error_counter.update([reason_text])
            continue

        resultado = validar_geometria(
            geom,
            crs=gdf.crs,
            srid_esperado=srid_esperado,
            crs_esperado=crs_esperado,
            normalizar=normalizar,
        )

        if resultado["valido"]:
            continue

        reason_text = " | ".join(resultado["erros"])
        invalid_indices.append(idx)
        invalid_reasons.append(reason_text)
        error_counter.update(resultado["erros"])

    if not invalid_indices:
        empty_gdf = gpd.GeoDataFrame(
            columns=list(gdf.columns) + ["ogc_motivo"],
            geometry="geometry",
            crs=gdf.crs,
        )
        return empty_gdf, 0, {}

    invalid_gdf = gdf.loc[invalid_indices].copy()
    invalid_gdf["ogc_motivo"] = invalid_reasons
    return invalid_gdf, len(invalid_gdf), dict(error_counter.most_common())


def _new_validation_result(geom):
    return {
        "valido": True,
        "tipo": getattr(geom, "geom_type", None),
        "erros": [],
        "avisos": [],
        "normalizada": False,
        "geometria": geom,
    }


def _add_error(result, message):
    result["valido"] = False
    result["erros"].append(message)


def _is_finite_number(value):
    return isinstance(value, (int, float)) and math.isfinite(value)


def _validate_coordinate_tuple(coord, label, result):
    if coord is None:
        _add_error(result, f"{label}: coordenada nula.")
        return

    if len(coord) < 2:
        _add_error(result, f"{label}: coordenada com dimensao insuficiente.")
        return

    for axis_index, axis_value in enumerate(coord):
        if axis_value is None:
            _add_error(result, f"{label}: eixo {axis_index} com valor nulo.")
            continue

        if not _is_finite_number(axis_value):
            _add_error(
                result,
                f"{label}: eixo {axis_index} com valor invalido ({axis_value})."
            )


def _validate_linestring_coords(coords, label, result, minimum_points=2, must_be_closed=False):
    if len(coords) < minimum_points:
        _add_error(
            result,
            f"{label}: quantidade insuficiente de pontos ({len(coords)})."
        )

    for coord in coords:
        _validate_coordinate_tuple(coord, label, result)

    distinct_points = {tuple(coord[:2]) for coord in coords if coord is not None and len(coord) >= 2}
    if len(distinct_points) < 2:
        _add_error(result, f"{label}: precisa de pelo menos 2 pontos distintos.")

    if must_be_closed and coords and coords[0] != coords[-1]:
        _add_error(result, f"{label}: anel nao esta fechado.")


def _validate_polygon_coords(polygon, label, result):
    exterior_coords = list(polygon.exterior.coords)
    _validate_linestring_coords(
        exterior_coords,
        f"{label} - anel externo",
        result,
        minimum_points=4,
        must_be_closed=True,
    )

    for ring_index, interior in enumerate(polygon.interiors, 1):
        interior_coords = list(interior.coords)
        _validate_linestring_coords(
            interior_coords,
            f"{label} - buraco {ring_index}",
            result,
            minimum_points=4,
            must_be_closed=True,
        )


def validar_tipo(geom):
    result = _new_validation_result(geom)

    if geom is None:
        _add_error(result, "Geometria nula.")
        return result

    if not isinstance(geom, BaseGeometry):
        _add_error(result, "Objeto informado nao e uma geometria Shapely.")
        return result

    if geom.geom_type not in GEOMETRY_TYPES:
        _add_error(result, f"Tipo geometrico nao suportado: {geom.geom_type}.")

    return result


def validar_coordenadas(geom, crs=None):
    result = _new_validation_result(geom)

    if geom is None:
        _add_error(result, "Geometria nula.")
        return result

    if geom.is_empty:
        _add_error(result, "Geometria vazia.")
        return result

    if isinstance(geom, Point):
        _validate_coordinate_tuple(geom.coords[0], "Point", result)
        _validate_coordinate_ranges_for_crs(geom, crs, result)
        return result

    if isinstance(geom, (LineString, LinearRing)):
        _validate_linestring_coords(
            list(geom.coords),
            geom.geom_type,
            result,
            minimum_points=4 if isinstance(geom, LinearRing) else 2,
            must_be_closed=isinstance(geom, LinearRing),
        )
        _validate_coordinate_ranges_for_crs(geom, crs, result)
        return result

    if isinstance(geom, Polygon):
        _validate_polygon_coords(geom, "Polygon", result)
        _validate_coordinate_ranges_for_crs(geom, crs, result)
        return result

    if isinstance(geom, MultiPoint):
        if len(geom.geoms) == 0:
            _add_error(result, "MultiPoint sem geometrias.")
        for index, item in enumerate(geom.geoms, 1):
            child_result = validar_coordenadas(item, crs=crs)
            for error in child_result["erros"]:
                _add_error(result, f"MultiPoint[{index}]: {error}")
        return result

    if isinstance(geom, MultiLineString):
        if len(geom.geoms) == 0:
            _add_error(result, "MultiLineString sem geometrias.")
        for index, item in enumerate(geom.geoms, 1):
            child_result = validar_coordenadas(item, crs=crs)
            for error in child_result["erros"]:
                _add_error(result, f"MultiLineString[{index}]: {error}")
        return result

    if isinstance(geom, MultiPolygon):
        if len(geom.geoms) == 0:
            _add_error(result, "MultiPolygon sem geometrias.")
        for index, item in enumerate(geom.geoms, 1):
            child_result = validar_coordenadas(item, crs=crs)
            for error in child_result["erros"]:
                _add_error(result, f"MultiPolygon[{index}]: {error}")
        return result

    if isinstance(geom, GeometryCollection):
        if len(geom.geoms) == 0:
            _add_error(result, "GeometryCollection sem geometrias.")
        for index, item in enumerate(geom.geoms, 1):
            child_result = validar_coordenadas(item, crs=crs)
            for error in child_result["erros"]:
                _add_error(result, f"GeometryCollection[{index}]: {error}")
        return result

    _add_error(result, f"Nao foi possivel validar coordenadas para {geom.geom_type}.")
    return result


def validar_regras_topologicas(geom):
    result = _new_validation_result(geom)

    if geom is None:
        _add_error(result, "Geometria nula.")
        return result

    if geom.is_empty:
        _add_error(result, "Geometria vazia.")
        return result

    if not geom.is_valid:
        reason = explain_validity(geom)
        _add_error(result, f"Geometria invalida segundo OGC: {reason}.")

    if isinstance(geom, Polygon):
        exterior_coords = list(geom.exterior.coords)
        if exterior_coords and exterior_coords[0] != exterior_coords[-1]:
            _add_error(result, "Polygon: anel externo nao esta fechado.")

        for ring_index, interior in enumerate(geom.interiors, 1):
            interior_coords = list(interior.coords)
            if interior_coords and interior_coords[0] != interior_coords[-1]:
                _add_error(result, f"Polygon: buraco {ring_index} nao esta fechado.")

    elif isinstance(geom, MultiPolygon):
        for index, polygon in enumerate(geom.geoms, 1):
            child_result = validar_regras_topologicas(polygon)
            for error in child_result["erros"]:
                _add_error(result, f"MultiPolygon[{index}]: {error}")

    elif isinstance(geom, GeometryCollection):
        for index, item in enumerate(geom.geoms, 1):
            child_result = validar_regras_topologicas(item)
            for error in child_result["erros"]:
                _add_error(result, f"GeometryCollection[{index}]: {error}")

    return result


def validar_srid_ou_crs(geom, crs=None, srid_esperado=None, crs_esperado=None):
    result = _new_validation_result(geom)

    if geom is None:
        _add_error(result, "Geometria nula.")
        return result

    srid_atual = None
    try:
        srid_atual = get_srid(geom)
    except Exception:
        srid_atual = None

    if srid_esperado is not None:
        if srid_atual in (None, 0):
            _add_error(result, f"SRID ausente. Esperado: {srid_esperado}.")
        elif srid_atual != srid_esperado:
            _add_error(
                result,
                f"SRID incompativel. Atual: {srid_atual}. Esperado: {srid_esperado}."
            )

    if crs_esperado is not None:
        if crs is None:
            _add_error(result, f"CRS ausente. Esperado: {crs_esperado}.")
        elif str(crs) != str(crs_esperado):
            _add_error(
                result,
                f"CRS incompativel. Atual: {crs}. Esperado: {crs_esperado}."
            )

    return result


def normalizar_geometria(geom):
    result = _new_validation_result(geom)

    if geom is None:
        _add_error(result, "Geometria nula.")
        return result

    normalized = geom

    try:
        if not normalized.is_valid:
            normalized = make_valid(normalized)
            result["normalizada"] = True
    except Exception as exc:
        _add_error(result, f"Erro ao corrigir geometria com make_valid: {exc}")
        result["geometria"] = geom
        return result

    try:
        if isinstance(normalized, Polygon):
            normalized = orient(normalized, sign=1.0)
            result["normalizada"] = True
        elif isinstance(normalized, MultiPolygon):
            normalized = MultiPolygon([orient(poly, sign=1.0) for poly in normalized.geoms])
            result["normalizada"] = True
    except Exception as exc:
        result["avisos"].append(f"Nao foi possivel orientar aneis: {exc}")

    result["geometria"] = normalized
    return result


def validar_geometria(geom, crs=None, srid_esperado=None, crs_esperado=None, normalizar=False):
    resultado = _new_validation_result(geom)
    geometria_avaliada = geom

    if normalizar:
        normalizacao = normalizar_geometria(geometria_avaliada)
        resultado["avisos"].extend(normalizacao["avisos"])
        resultado["erros"].extend(normalizacao["erros"])
        resultado["normalizada"] = normalizacao["normalizada"]
        geometria_avaliada = normalizacao["geometria"]
        resultado["geometria"] = geometria_avaliada

    validacoes = [
        validar_tipo(geometria_avaliada),
        validar_coordenadas(geometria_avaliada, crs=crs),
        validar_regras_topologicas(geometria_avaliada),
        validar_srid_ou_crs(
            geometria_avaliada,
            crs=crs,
            srid_esperado=srid_esperado,
            crs_esperado=crs_esperado,
        ),
    ]

    for parcial in validacoes:
        resultado["erros"].extend(parcial["erros"])
        resultado["avisos"].extend(parcial["avisos"])

    if resultado["erros"]:
        resultado["valido"] = False

    return resultado


def gerar_relatorio_erros(resultado):
    if not resultado:
        return "Resultado de validacao ausente."

    linhas = [
        f"Geometria: {resultado.get('tipo') or 'desconhecida'}",
        f"Valida: {'sim' if resultado.get('valido') else 'nao'}",
        f"Normalizada: {'sim' if resultado.get('normalizada') else 'nao'}",
    ]

    erros = resultado.get("erros", [])
    avisos = resultado.get("avisos", [])

    if erros:
        linhas.append("Erros:")
        for erro in erros:
            linhas.append(f"- {erro}")
    else:
        linhas.append("Erros: nenhum")

    if avisos:
        linhas.append("Avisos:")
        for aviso in avisos:
            linhas.append(f"- {aviso}")

    return "\n".join(linhas)

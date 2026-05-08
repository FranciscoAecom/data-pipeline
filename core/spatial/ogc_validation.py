import math
from collections import Counter

import geopandas as gpd
import pandas as pd
from shapely import get_srid
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

from core.spatial.crs import (
    geometry_series_matches_crs_coordinate_range,
    is_geographic_crs,
    is_within_geographic_bounds,
)
from core.spatial.repair import INTERNAL_SAFE_REPAIR_FLAG


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


def validate_coordinate_ranges_for_crs(geom, crs, result, label=None):
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
            validate_coordinate_ranges_for_crs(
                item,
                crs,
                result,
                label=f"MultiPoint[{index}]",
            )
        return

    if isinstance(geom, MultiLineString):
        for index, item in enumerate(geom.geoms, 1):
            validate_coordinate_ranges_for_crs(
                item,
                crs,
                result,
                label=f"MultiLineString[{index}]",
            )
        return

    if isinstance(geom, MultiPolygon):
        for index, item in enumerate(geom.geoms, 1):
            validate_coordinate_ranges_for_crs(
                item,
                crs,
                result,
                label=f"MultiPolygon[{index}]",
            )
        return

    if isinstance(geom, GeometryCollection):
        for index, item in enumerate(geom.geoms, 1):
            validate_coordinate_ranges_for_crs(
                item,
                crs,
                result,
                label=f"GeometryCollection[{index}]",
            )
        return

    if not is_geographic_crs(crs):
        return

    try:
        bounds = geom.bounds
    except Exception:
        add_error(
            result,
            f"{current_label}: nao foi possivel obter bounds para validar compatibilidade com o CRS {crs}.",
        )
        return

    if is_within_geographic_bounds(bounds):
        return

    minx, miny, maxx, maxy = bounds
    add_error(
        result,
        (
            f"{current_label}: bounds incompativeis com o CRS geografico {crs} "
            f"(minx={minx}, miny={miny}, maxx={maxx}, maxy={maxy})."
        ),
    )


def can_skip_detailed_ogc_check(gdf, crs_esperado=None, srid_esperado=None, normalizar=False):
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

    coordinate_range_mask = geometry_series_matches_crs_coordinate_range(geometry, gdf.crs)
    if not bool(coordinate_range_mask.all()):
        return False

    return bool(geometry.is_valid.all())


def get_invalid_ogc_records(gdf, crs_esperado=None, srid_esperado=None, normalizar=False):
    if can_skip_detailed_ogc_check(
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


def new_validation_result(geom):
    return {
        "valido": True,
        "tipo": getattr(geom, "geom_type", None),
        "erros": [],
        "avisos": [],
        "normalizada": False,
        "geometria": geom,
    }


def add_error(result, message):
    result["valido"] = False
    result["erros"].append(message)


def is_finite_number(value):
    return isinstance(value, (int, float)) and math.isfinite(value)


def validate_coordinate_tuple(coord, label, result):
    if coord is None:
        add_error(result, f"{label}: coordenada nula.")
        return

    if len(coord) < 2:
        add_error(result, f"{label}: coordenada com dimensao insuficiente.")
        return

    for axis_index, axis_value in enumerate(coord):
        if axis_value is None:
            add_error(result, f"{label}: eixo {axis_index} com valor nulo.")
            continue

        if not is_finite_number(axis_value):
            add_error(
                result,
                f"{label}: eixo {axis_index} com valor invalido ({axis_value})."
            )


def validate_linestring_coords(coords, label, result, minimum_points=2, must_be_closed=False):
    if len(coords) < minimum_points:
        add_error(
            result,
            f"{label}: quantidade insuficiente de pontos ({len(coords)})."
        )

    for coord in coords:
        validate_coordinate_tuple(coord, label, result)

    distinct_points = {tuple(coord[:2]) for coord in coords if coord is not None and len(coord) >= 2}
    if len(distinct_points) < 2:
        add_error(result, f"{label}: precisa de pelo menos 2 pontos distintos.")

    if must_be_closed and coords and coords[0] != coords[-1]:
        add_error(result, f"{label}: anel nao esta fechado.")


def validate_polygon_coords(polygon, label, result):
    exterior_coords = list(polygon.exterior.coords)
    validate_linestring_coords(
        exterior_coords,
        f"{label} - anel externo",
        result,
        minimum_points=4,
        must_be_closed=True,
    )

    for ring_index, interior in enumerate(polygon.interiors, 1):
        interior_coords = list(interior.coords)
        validate_linestring_coords(
            interior_coords,
            f"{label} - buraco {ring_index}",
            result,
            minimum_points=4,
            must_be_closed=True,
        )


def validar_tipo(geom):
    result = new_validation_result(geom)

    if geom is None:
        add_error(result, "Geometria nula.")
        return result

    if not isinstance(geom, BaseGeometry):
        add_error(result, "Objeto informado nao e uma geometria Shapely.")
        return result

    if geom.geom_type not in GEOMETRY_TYPES:
        add_error(result, f"Tipo geometrico nao suportado: {geom.geom_type}.")

    return result


def validar_coordenadas(geom, crs=None):
    result = new_validation_result(geom)

    if geom is None:
        add_error(result, "Geometria nula.")
        return result

    if geom.is_empty:
        add_error(result, "Geometria vazia.")
        return result

    if isinstance(geom, Point):
        validate_coordinate_tuple(geom.coords[0], "Point", result)
        validate_coordinate_ranges_for_crs(geom, crs, result)
        return result

    if isinstance(geom, (LineString, LinearRing)):
        validate_linestring_coords(
            list(geom.coords),
            geom.geom_type,
            result,
            minimum_points=4 if isinstance(geom, LinearRing) else 2,
            must_be_closed=isinstance(geom, LinearRing),
        )
        validate_coordinate_ranges_for_crs(geom, crs, result)
        return result

    if isinstance(geom, Polygon):
        validate_polygon_coords(geom, "Polygon", result)
        validate_coordinate_ranges_for_crs(geom, crs, result)
        return result

    if isinstance(geom, MultiPoint):
        if len(geom.geoms) == 0:
            add_error(result, "MultiPoint sem geometrias.")
        for index, item in enumerate(geom.geoms, 1):
            child_result = validar_coordenadas(item, crs=crs)
            for error in child_result["erros"]:
                add_error(result, f"MultiPoint[{index}]: {error}")
        return result

    if isinstance(geom, MultiLineString):
        if len(geom.geoms) == 0:
            add_error(result, "MultiLineString sem geometrias.")
        for index, item in enumerate(geom.geoms, 1):
            child_result = validar_coordenadas(item, crs=crs)
            for error in child_result["erros"]:
                add_error(result, f"MultiLineString[{index}]: {error}")
        return result

    if isinstance(geom, MultiPolygon):
        if len(geom.geoms) == 0:
            add_error(result, "MultiPolygon sem geometrias.")
        for index, item in enumerate(geom.geoms, 1):
            child_result = validar_coordenadas(item, crs=crs)
            for error in child_result["erros"]:
                add_error(result, f"MultiPolygon[{index}]: {error}")
        return result

    if isinstance(geom, GeometryCollection):
        if len(geom.geoms) == 0:
            add_error(result, "GeometryCollection sem geometrias.")
        for index, item in enumerate(geom.geoms, 1):
            child_result = validar_coordenadas(item, crs=crs)
            for error in child_result["erros"]:
                add_error(result, f"GeometryCollection[{index}]: {error}")
        return result

    add_error(result, f"Nao foi possivel validar coordenadas para {geom.geom_type}.")
    return result


def validar_regras_topologicas(geom):
    result = new_validation_result(geom)

    if geom is None:
        add_error(result, "Geometria nula.")
        return result

    if geom.is_empty:
        add_error(result, "Geometria vazia.")
        return result

    if not geom.is_valid:
        reason = explain_validity(geom)
        add_error(result, f"Geometria invalida segundo OGC: {reason}.")

    if isinstance(geom, Polygon):
        exterior_coords = list(geom.exterior.coords)
        if exterior_coords and exterior_coords[0] != exterior_coords[-1]:
            add_error(result, "Polygon: anel externo nao esta fechado.")

        for ring_index, interior in enumerate(geom.interiors, 1):
            interior_coords = list(interior.coords)
            if interior_coords and interior_coords[0] != interior_coords[-1]:
                add_error(result, f"Polygon: buraco {ring_index} nao esta fechado.")

    elif isinstance(geom, MultiPolygon):
        for index, polygon in enumerate(geom.geoms, 1):
            child_result = validar_regras_topologicas(polygon)
            for error in child_result["erros"]:
                add_error(result, f"MultiPolygon[{index}]: {error}")

    elif isinstance(geom, GeometryCollection):
        for index, item in enumerate(geom.geoms, 1):
            child_result = validar_regras_topologicas(item)
            for error in child_result["erros"]:
                add_error(result, f"GeometryCollection[{index}]: {error}")

    return result


def validar_srid_ou_crs(geom, crs=None, srid_esperado=None, crs_esperado=None):
    result = new_validation_result(geom)

    if geom is None:
        add_error(result, "Geometria nula.")
        return result

    srid_atual = None
    try:
        srid_atual = get_srid(geom)
    except Exception:
        srid_atual = None

    if srid_esperado is not None:
        if srid_atual in (None, 0):
            add_error(result, f"SRID ausente. Esperado: {srid_esperado}.")
        elif srid_atual != srid_esperado:
            add_error(
                result,
                f"SRID incompativel. Atual: {srid_atual}. Esperado: {srid_esperado}."
            )

    if crs_esperado is not None:
        if crs is None:
            add_error(result, f"CRS ausente. Esperado: {crs_esperado}.")
        elif str(crs) != str(crs_esperado):
            add_error(
                result,
                f"CRS incompativel. Atual: {crs}. Esperado: {crs_esperado}."
            )

    return result


def normalizar_geometria(geom):
    result = new_validation_result(geom)

    if geom is None:
        add_error(result, "Geometria nula.")
        return result

    normalized = geom

    try:
        if not normalized.is_valid:
            normalized = make_valid(normalized)
            result["normalizada"] = True
    except Exception as exc:
        add_error(result, f"Erro ao corrigir geometria com make_valid: {exc}")
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
    resultado = new_validation_result(geom)
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


__all__ = [
    "GEOMETRY_TYPES",
    "add_error",
    "can_skip_detailed_ogc_check",
    "gerar_relatorio_erros",
    "get_invalid_ogc_records",
    "is_finite_number",
    "new_validation_result",
    "normalizar_geometria",
    "validar_coordenadas",
    "validar_geometria",
    "validar_regras_topologicas",
    "validar_srid_ou_crs",
    "validar_tipo",
    "validate_coordinate_ranges_for_crs",
    "validate_coordinate_tuple",
    "validate_linestring_coords",
    "validate_polygon_coords",
]

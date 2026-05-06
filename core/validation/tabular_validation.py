import geopandas as gpd
import pandas as pd


def validate_tabular_schema(gdf, schema):
    errors = []

    if schema.require_geometry:
        errors.extend(validate_geometry(gdf))

    missing_columns = []
    for column, rule in schema.columns.items():
        if column not in gdf.columns:
            if rule.required:
                missing_columns.append(column)
            continue

        errors.extend(validate_column(gdf[column], column, rule))

    if missing_columns:
        errors.append(
            "Colunas obrigatorias ausentes: "
            f"{', '.join(sorted(missing_columns))}."
        )

    if not schema.allow_extra_columns:
        expected_columns = set(schema.columns) | {"geometry"}
        extra_columns = sorted(set(gdf.columns) - expected_columns)
        if extra_columns:
            errors.append(
                "Colunas nao previstas no schema: "
                f"{', '.join(extra_columns)}."
            )

    return errors


def validate_geometry(gdf):
    if not isinstance(gdf, gpd.GeoDataFrame):
        return ["Entrada nao e um GeoDataFrame."]
    if "geometry" not in gdf.columns:
        return ["Coluna obrigatoria ausente: geometry."]
    if gdf.geometry.name != "geometry":
        return ["Coluna geometry nao esta configurada como geometria ativa."]
    return []


def validate_column(series, column, rule):
    errors = []

    if not rule.nullable and series.isna().any():
        errors.append(f"Coluna {column} nao permite valores nulos.")

    if not matches_dtype(series, rule.dtype):
        errors.append(
            f"Coluna {column} tem tipo invalido: "
            f"esperado {rule.dtype}, encontrado {series.dtype}."
        )

    return errors


def matches_dtype(series, expected_dtype):
    expected = str(expected_dtype).strip().lower()

    if expected in {"string", "str", "text"}:
        return (
            pd.api.types.is_string_dtype(series)
            or pd.api.types.is_object_dtype(series)
        )

    if expected in {"number", "numeric"}:
        return pd.api.types.is_numeric_dtype(series)

    if expected in {"float", "double"}:
        return pd.api.types.is_float_dtype(series) or pd.api.types.is_numeric_dtype(series)

    if expected in {"integer", "int"}:
        return pd.api.types.is_integer_dtype(series)

    if expected in {"datetime", "date"}:
        return pd.api.types.is_datetime64_any_dtype(series)

    if expected in {"boolean", "bool"}:
        return pd.api.types.is_bool_dtype(series)

    return True

from dataclasses import dataclass, field

import geopandas as gpd
import pandas as pd

from core.utils import log


@dataclass(frozen=True)
class ColumnRule:
    dtype: str
    required: bool = True
    nullable: bool = True


@dataclass(frozen=True)
class TabularSchema:
    columns: dict[str, ColumnRule] = field(default_factory=dict)
    require_geometry: bool = True
    allow_extra_columns: bool = True


def get_tabular_schema(rule_profile):
    if not isinstance(rule_profile, dict):
        return None

    configured_schema = rule_profile.get("input_schema")
    if configured_schema is not None:
        return _build_configured_schema(configured_schema)

    fields = rule_profile.get("fields", {})
    if not isinstance(fields, dict) or not fields:
        return None

    return TabularSchema(
        columns={
            column: ColumnRule("string")
            for column in fields
            if isinstance(column, str) and column.strip()
        }
    )


def validate_input_schema(record, gdf, rule_profile):
    schema = get_tabular_schema(rule_profile)
    if schema is None:
        return []
    return validate_tabular_schema(gdf, schema)


def normalize_input_schema(record, gdf, rule_profile):
    schema = get_tabular_schema(rule_profile)
    if schema is None:
        return gdf, []
    return normalize_tabular_schema(gdf, schema)


def normalize_tabular_schema(gdf, schema):
    errors = []

    if schema.require_geometry:
        errors.extend(_validate_geometry(gdf))

    missing_columns = []
    for column, rule in schema.columns.items():
        if column not in gdf.columns:
            if rule.required:
                missing_columns.append(column)
            continue

        gdf, column_errors = _normalize_column(gdf, column, rule)
        errors.extend(column_errors)

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

    return gdf, errors


def validate_tabular_schema(gdf, schema):
    errors = []

    if schema.require_geometry:
        errors.extend(_validate_geometry(gdf))

    missing_columns = []
    for column, rule in schema.columns.items():
        if column not in gdf.columns:
            if rule.required:
                missing_columns.append(column)
            continue

        errors.extend(_validate_column(gdf[column], column, rule))

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


def _build_configured_schema(configured_schema):
    if not isinstance(configured_schema, dict):
        raise ValueError("Campo 'input_schema' deve ser um objeto JSON.")

    raw_columns = configured_schema.get("columns", {})
    if not isinstance(raw_columns, dict):
        raise ValueError("Campo 'input_schema.columns' deve ser um objeto JSON.")

    columns = {}
    for column, raw_rule in raw_columns.items():
        if not isinstance(column, str) or not column.strip():
            raise ValueError("Chaves de 'input_schema.columns' devem ser strings nao vazias.")

        columns[column] = _build_column_rule(column, raw_rule)

    return TabularSchema(
        columns=columns,
        require_geometry=_get_bool(configured_schema, "require_geometry", True),
        allow_extra_columns=_get_bool(configured_schema, "allow_extra_columns", True),
    )


def _build_column_rule(column, raw_rule):
    if isinstance(raw_rule, str):
        return ColumnRule(raw_rule)

    if not isinstance(raw_rule, dict):
        raise ValueError(
            f"Regra de 'input_schema.columns.{column}' deve ser string ou objeto JSON."
        )

    dtype = raw_rule.get("dtype", "string")
    if not isinstance(dtype, str) or not dtype.strip():
        raise ValueError(f"'dtype' de 'input_schema.columns.{column}' deve ser string.")

    return ColumnRule(
        dtype=dtype.strip(),
        required=_get_bool(raw_rule, "required", True),
        nullable=_get_bool(raw_rule, "nullable", True),
    )


def _get_bool(data, key, default):
    value = data.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"Campo '{key}' deve ser booleano.")
    return value


def _validate_geometry(gdf):
    if not isinstance(gdf, gpd.GeoDataFrame):
        return ["Entrada nao e um GeoDataFrame."]
    if "geometry" not in gdf.columns:
        return ["Coluna obrigatoria ausente: geometry."]
    if gdf.geometry.name != "geometry":
        return ["Coluna geometry nao esta configurada como geometria ativa."]
    return []


def _validate_column(series, column, rule):
    errors = []

    if not rule.nullable and series.isna().any():
        errors.append(f"Coluna {column} nao permite valores nulos.")

    if not _matches_dtype(series, rule.dtype):
        errors.append(
            f"Coluna {column} tem tipo invalido: "
            f"esperado {rule.dtype}, encontrado {series.dtype}."
        )

    return errors


def _normalize_column(gdf, column, rule):
    source_series = gdf[column]
    normalized_series = source_series

    if not _matches_dtype(source_series, rule.dtype):
        normalized_series = _coerce_series(source_series, rule.dtype)

        if _matches_dtype(normalized_series, rule.dtype):
            changed_nulls = _new_null_count(source_series, normalized_series)
            if changed_nulls and not rule.nullable:
                return gdf, [
                    f"Coluna {column} nao pode ser convertida para {rule.dtype} "
                    f"sem gerar {changed_nulls} valor(es) nulo(s)."
                ]

            gdf[column] = normalized_series
            log(
                "Schema tabular: coluna "
                f"{column} convertida de {source_series.dtype} para {gdf[column].dtype}."
            )

    return gdf, _validate_column(gdf[column], column, rule)


def _coerce_series(series, expected_dtype):
    expected = str(expected_dtype).strip().lower()

    if expected in {"string", "str", "text"}:
        return series.astype("string")

    if expected in {"number", "numeric", "float", "double"}:
        return pd.to_numeric(series, errors="coerce")

    if expected in {"integer", "int"}:
        numeric = pd.to_numeric(series, errors="coerce")
        return numeric.astype("Int64") if numeric.notna().all() else numeric

    if expected in {"datetime", "date"}:
        return pd.to_datetime(series, errors="coerce")

    if expected in {"boolean", "bool"}:
        return _coerce_bool_series(series)

    return series


def _coerce_bool_series(series):
    if pd.api.types.is_numeric_dtype(series):
        mapped = series.map({0: False, 1: True})
        return mapped.astype("boolean") if mapped.notna().all() else mapped

    normalized = series.astype("string").str.strip().str.casefold()
    mapped = normalized.map(
        {
            "true": True,
            "false": False,
            "1": True,
            "0": False,
            "sim": True,
            "nao": False,
            "não": False,
            "yes": True,
            "no": False,
        }
    )
    return mapped.astype("boolean") if mapped.notna().all() else mapped


def _new_null_count(source_series, candidate_series):
    source_nulls = source_series.isna()
    candidate_nulls = candidate_series.isna()
    return int((~source_nulls & candidate_nulls).sum())


def _matches_dtype(series, expected_dtype):
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

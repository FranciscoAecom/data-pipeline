from dataclasses import dataclass, field

from core.validation.tabular_coercion import normalize_tabular_schema
from core.validation.tabular_validation import validate_tabular_schema


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

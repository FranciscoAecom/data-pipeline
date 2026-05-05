import unittest
from types import SimpleNamespace

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from core.validation.tabular_schema import (
    ColumnRule,
    TabularSchema,
    get_tabular_schema,
    normalize_tabular_schema,
    validate_input_schema,
    validate_tabular_schema,
)


class TabularSchemaTests(unittest.TestCase):
    def test_reports_missing_required_column(self):
        gdf = gpd.GeoDataFrame(
            {"sdb_nom_tema": ["Reserva Legal"], "geometry": [Point(0, 0)]},
            geometry="geometry",
            crs="EPSG:4326",
        )
        schema = TabularSchema(
            columns={
                "sdb_cod_tema": ColumnRule("string", required=True),
                "sdb_nom_tema": ColumnRule("string", required=True),
            }
        )

        errors = validate_tabular_schema(gdf, schema)

        self.assertIn("Colunas obrigatorias ausentes: sdb_cod_tema.", errors)

    def test_converts_invalid_string_dtype(self):
        gdf = gpd.GeoDataFrame(
            {"sdb_cod_tema": [1.5], "geometry": [Point(0, 0)]},
            geometry="geometry",
            crs="EPSG:4326",
        )
        schema = TabularSchema(
            columns={"sdb_cod_tema": ColumnRule("string", required=True)}
        )

        result, errors = normalize_tabular_schema(gdf, schema)

        self.assertEqual(errors, [])
        self.assertTrue(pd.api.types.is_string_dtype(result["sdb_cod_tema"]))
        self.assertEqual(result.loc[0, "sdb_cod_tema"], "1.5")

    def test_converts_numeric_dtype(self):
        gdf = gpd.GeoDataFrame(
            {"sdb_area": ["10.5"], "geometry": [Point(0, 0)]},
            geometry="geometry",
            crs="EPSG:4326",
        )
        schema = TabularSchema(
            columns={"sdb_area": ColumnRule("number", required=True)}
        )

        result, errors = normalize_tabular_schema(gdf, schema)

        self.assertEqual(errors, [])
        self.assertTrue(pd.api.types.is_numeric_dtype(result["sdb_area"]))
        self.assertEqual(result.loc[0, "sdb_area"], 10.5)

    def test_reports_failed_conversion_when_new_null_is_not_allowed(self):
        gdf = gpd.GeoDataFrame(
            {"sdb_area": ["dez"], "geometry": [Point(0, 0)]},
            geometry="geometry",
            crs="EPSG:4326",
        )
        schema = TabularSchema(
            columns={"sdb_area": ColumnRule("number", required=True, nullable=False)}
        )

        _, errors = normalize_tabular_schema(gdf, schema)

        self.assertEqual(len(errors), 1)
        self.assertIn("nao pode ser convertida", errors[0])

    def test_reports_nulls_when_column_is_not_nullable(self):
        gdf = gpd.GeoDataFrame(
            {"sdb_cod_tema": pd.Series([None], dtype="object"), "geometry": [Point(0, 0)]},
            geometry="geometry",
            crs="EPSG:4326",
        )
        schema = TabularSchema(
            columns={
                "sdb_cod_tema": ColumnRule("string", required=True, nullable=False)
            }
        )

        errors = validate_tabular_schema(gdf, schema)

        self.assertIn("Coluna sdb_cod_tema nao permite valores nulos.", errors)

    def test_infers_schema_from_rule_profile_fields(self):
        schema = get_tabular_schema(
            {
                "fields": {
                    "sdb_cod_tema": {"accepted_values": ["A"]},
                    "sdb_nom_tema": {"accepted_values": ["Tema A"]},
                }
            }
        )

        self.assertIsNotNone(schema)
        self.assertIn("sdb_cod_tema", schema.columns)

    def test_uses_explicit_input_schema_from_rule_profile(self):
        schema = get_tabular_schema(
            {
                "input_schema": {
                    "columns": {
                        "sdb_cod_tema": {
                            "dtype": "string",
                            "required": True,
                            "nullable": False,
                        },
                        "sdb_area": "number",
                    },
                    "allow_extra_columns": False,
                }
            }
        )

        self.assertFalse(schema.allow_extra_columns)
        self.assertFalse(schema.columns["sdb_cod_tema"].nullable)
        self.assertEqual(schema.columns["sdb_area"].dtype, "number")

    def test_validate_input_schema_rejects_invalid_rule_profile_schema(self):
        record = SimpleNamespace(
            theme_folder="rl_car_ac",
            theme="Reserva Legal",
        )
        gdf = gpd.GeoDataFrame(
            {
                "sdb_cod_tema": ["ARL_AVERBADA"],
                "geometry": [Point(0, 0)],
            },
            geometry="geometry",
            crs="EPSG:4326",
        )
        rule_profile = {
            "fields": {
                "sdb_cod_tema": {"accepted_values": ["ARL_AVERBADA"]},
                "sdb_nom_tema": {"accepted_values": ["Reserva Legal Averbada"]},
            }
        }

        errors = validate_input_schema(record, gdf, rule_profile)

        self.assertIn("Colunas obrigatorias ausentes: sdb_nom_tema.", errors)

if __name__ == "__main__":
    unittest.main()

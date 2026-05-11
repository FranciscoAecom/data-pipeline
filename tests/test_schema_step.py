import unittest
from types import SimpleNamespace

import geopandas as gpd
from shapely.geometry import Point

from core.processing.schema_step import validate_input_schema_step


class SchemaStepTests(unittest.TestCase):
    def test_normalizes_columns_before_tabular_schema_validation(self):
        gdf = gpd.GeoDataFrame(
            {"COD_TEMA": ["ARL_AVERBADA"], "geometry": [Point(0, 0)]},
            geometry="geometry",
            crs="EPSG:4326",
        )
        record = SimpleNamespace(
            theme_folder="sa_car_ac",
            theme="Servidão Administrativa",
        )
        rule_profile = {
            "input_schema": {
                "columns": {
                    "sdb_cod_tema": {
                        "dtype": "string",
                        "required": True,
                        "nullable": False,
                    }
                }
            }
        }
        context = SimpleNamespace(record=record, gdf=gdf, rule_profile=rule_profile)

        result = validate_input_schema_step(context)

        self.assertIn("sdb_cod_tema", result.gdf.columns)
        self.assertNotIn("COD_TEMA", result.gdf.columns)


if __name__ == "__main__":
    unittest.main()

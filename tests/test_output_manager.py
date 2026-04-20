import unittest

import geopandas as gpd
from shapely.geometry import Point

from core.geometry_repair import INTERNAL_SAFE_REPAIR_FLAG
from core.output_manager import assign_output_identifiers, drop_internal_output_columns


class OutputManagerTests(unittest.TestCase):
    def test_assign_output_identifiers_adds_acm_id_when_missing(self):
        gdf = gpd.GeoDataFrame(
            {"nome": ["a", "b"], "geometry": [Point(0, 0), Point(1, 1)]},
            geometry="geometry",
            crs="EPSG:4326",
        )

        result = assign_output_identifiers(gdf, start_id=7)

        self.assertEqual(result["acm_id"].tolist(), [7, 8])
        self.assertEqual(result["fid"].tolist(), [7, 8])

    def test_assign_output_identifiers_preserves_existing_acm_id(self):
        gdf = gpd.GeoDataFrame(
            {
                "acm_id": [101, 102],
                "nome": ["a", "b"],
                "geometry": [Point(0, 0), Point(1, 1)],
            },
            geometry="geometry",
            crs="EPSG:4326",
        )

        result = assign_output_identifiers(gdf, start_id=7)

        self.assertEqual(result["acm_id"].tolist(), [101, 102])
        self.assertEqual(result["fid"].tolist(), [101, 102])

    def test_drop_internal_output_columns_removes_safe_repair_flag(self):
        gdf = gpd.GeoDataFrame(
            {
                "nome": ["a"],
                INTERNAL_SAFE_REPAIR_FLAG: [True],
                "geometry": [Point(0, 0)],
            },
            geometry="geometry",
            crs="EPSG:4326",
        )

        result = drop_internal_output_columns(gdf)

        self.assertNotIn(INTERNAL_SAFE_REPAIR_FLAG, result.columns)
        self.assertIn("nome", result.columns)


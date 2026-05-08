import unittest
from types import SimpleNamespace

import geopandas as gpd
from shapely.geometry import Point

from core.processing.result import ProcessRecordResult, failure_result, success_result


def _gdf():
    return gpd.GeoDataFrame(
        {"coluna": ["A"], "geometry": [Point(0, 0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )


class ProcessingResultTests(unittest.TestCase):
    def test_failure_result_returns_empty_result(self):
        self.assertEqual(failure_result(), ProcessRecordResult(0, None, None))

    def test_success_result_from_context(self):
        final_gdf = _gdf()
        context = SimpleNamespace(
            final_gdf=final_gdf,
            output_path="tests/_tmp_output/saida.gpkg",
        )

        result = success_result(context)

        self.assertEqual(result.processed_count, 1)
        self.assertEqual(result.output_path, "tests/_tmp_output/saida.gpkg")
        self.assertTrue(result.final_gdf.equals(final_gdf))

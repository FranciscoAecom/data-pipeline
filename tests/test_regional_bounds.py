import unittest
from types import SimpleNamespace

import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon

from core.spatial.regional_bounds import enforce_car_state_bounds, infer_state_code


class RegionalBoundsTests(unittest.TestCase):
    def test_infers_state_from_app_car_record(self):
        record = SimpleNamespace(
            theme_folder="app_car_ma",
            input_path="C:/dados/pol_pcd_app_car_ma_20260301.gpkg",
            rule_profile="app_car/app_car_ma",
            source_path="",
            theme="",
        )

        self.assertEqual(infer_state_code(record), "ma")

    def test_infers_state_from_reserva_legal_car_record(self):
        record = SimpleNamespace(
            theme_folder="rl_car_ma",
            input_path="C:/dados/pol_pcd_rl_car_ma_20260301.gpkg",
            rule_profile="reserva_legal_car/rl_car_ma",
            source_path="",
            theme="",
        )

        self.assertEqual(infer_state_code(record), "ma")

    def test_clips_geometry_to_state_bounds_without_removing_record(self):
        valid_part = Polygon(
            [
                (-44.60, -6.87),
                (-44.57, -6.87),
                (-44.57, -6.84),
                (-44.60, -6.84),
                (-44.60, -6.87),
            ]
        )
        outlier_part = Polygon(
            [
                (-41.70, 83.20),
                (-41.60, 83.20),
                (-41.60, 83.30),
                (-41.70, 83.30),
                (-41.70, 83.20),
            ]
        )
        gdf = gpd.GeoDataFrame(
            {
                "acm_a_ha": [0.0],
                "acm_prm_km": [0.0],
                "acm_long": [0.0],
                "acm_lat": [83.0],
                "geometry": [MultiPolygon([valid_part, outlier_part])],
            },
            geometry="geometry",
            crs="EPSG:4326",
        )
        record = SimpleNamespace(
            theme_folder="app_car_ma",
            input_path="pol_pcd_app_car_ma_20260301.gpkg",
            rule_profile="app_car/app_car_ma",
            source_path="",
            theme="",
        )

        result = enforce_car_state_bounds(gdf, record)

        self.assertEqual(len(result.gdf), 1)
        self.assertEqual(result.clipped_count, 1)
        minx, miny, maxx, maxy = result.gdf.geometry.iloc[0].bounds
        self.assertGreaterEqual(minx, -49.0)
        self.assertGreaterEqual(miny, -11.0)
        self.assertLessEqual(maxx, -41.0)
        self.assertLessEqual(maxy, 0.0)
        self.assertLess(result.gdf.loc[0, "acm_lat"], 0.0)


if __name__ == "__main__":
    unittest.main()

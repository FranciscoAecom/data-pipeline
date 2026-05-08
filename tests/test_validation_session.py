import unittest

import geopandas as gpd
from shapely.geometry import Point

from core.validation.attribute_mapping import prepare_validate_shapefile_attribute_mappings
from core.validation.domain_validation import validate_shapefile_attribute
from core.validation.session import ValidationSession


def _gdf(value):
    return gpd.GeoDataFrame(
        {"sdb_codigo": [value], "geometry": [Point(0, 0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )


def _profile(alias_target):
    return {
        "fields": {
            "sdb_codigo": {
                "accepted_values": [alias_target],
                "aliases": {"apelido": alias_target},
            },
        },
    }


class ValidationSessionTests(unittest.TestCase):
    def test_sessions_keep_attribute_mappings_isolated(self):
        first_session = ValidationSession()
        second_session = ValidationSession()
        mapping = {"sdb_codigo": ["validate_shapefile_attribute"]}

        prepare_validate_shapefile_attribute_mappings(
            _gdf("apelido"),
            mapping,
            _profile("A"),
            validation_session=first_session,
        )
        prepare_validate_shapefile_attribute_mappings(
            _gdf("apelido"),
            mapping,
            _profile("B"),
            validation_session=second_session,
        )

        self.assertEqual(first_session.attribute_mappings["sdb_codigo"]["apelido"], "A")
        self.assertEqual(second_session.attribute_mappings["sdb_codigo"]["apelido"], "B")

    def test_validate_shapefile_attribute_writes_summary_to_selected_session(self):
        selected_session = ValidationSession()
        untouched_session = ValidationSession()

        result = validate_shapefile_attribute(
            _gdf("apelido"),
            "sdb_codigo",
            rule_profile=_profile("A"),
            validation_session=selected_session,
        )

        self.assertEqual(result["acm_codigo"].tolist(), ["A"])
        self.assertIn("sdb_codigo", selected_session.summary["fields"])
        self.assertEqual(untouched_session.summary["fields"], {})


if __name__ == "__main__":
    unittest.main()

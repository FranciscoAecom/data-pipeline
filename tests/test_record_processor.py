import unittest
from types import SimpleNamespace
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point

from core.record_processor import ProcessRecordResult, process_record


def _record():
    return SimpleNamespace(
        sheet_row=2,
        record_id=10,
        theme="tema_teste",
        theme_folder="rl_car_ac",
        status="Waiting Update",
        source_path="origem_a",
        input_path="origem_a.gpkg",
        rule_profile="reserva_legal_car/rl_car_ac",
    )


def _gdf():
    return gpd.GeoDataFrame(
        {"coluna": ["A"], "geometry": [Point(0, 0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )


class RecordProcessorTests(unittest.TestCase):
    @patch("core.record_processor.reset_validate_attribute_mappings")
    @patch("core.record_processor.resolve_project_config")
    @patch("core.record_processor.load_and_prepare_input")
    def test_returns_zero_when_input_loading_fails(
        self,
        mock_load_and_prepare_input,
        mock_resolve_project_config,
        mock_reset_validate_attribute_mappings,
    ):
        mock_resolve_project_config.return_value = {"project_name": "reserva_legal_car"}
        mock_load_and_prepare_input.side_effect = RuntimeError("falha na carga")

        result = process_record(_record(), output_dir="tests/_tmp_output")

        self.assertEqual(result, ProcessRecordResult(0, None, None))
        mock_reset_validate_attribute_mappings.assert_called_once()

    @patch("core.record_processor.save_outputs")
    @patch("core.record_processor._autofix_rule_profile_if_needed")
    @patch("core.record_processor.fill_missing_spatial_metrics")
    @patch("core.record_processor.repair_invalid_geometries")
    @patch("core.record_processor.assign_output_identifiers")
    @patch("core.record_processor.process_in_batches")
    @patch("core.record_processor.prepare_validate_shapefile_attribute_mappings")
    @patch("core.record_processor.build_auto_mapping")
    @patch("core.record_processor.log_dataset_overview")
    @patch("core.record_processor.set_active_rule_profile")
    @patch("core.record_processor.load_and_prepare_input")
    @patch("core.record_processor.resolve_project_config")
    @patch("core.record_processor.reset_validate_attribute_mappings")
    def test_processes_record_and_returns_final_gdf(
        self,
        mock_reset_validate_attribute_mappings,
        mock_resolve_project_config,
        mock_load_and_prepare_input,
        mock_set_active_rule_profile,
        mock_log_dataset_overview,
        mock_build_auto_mapping,
        mock_prepare_validate_shapefile_attribute_mappings,
        mock_process_in_batches,
        mock_assign_output_identifiers,
        mock_repair_invalid_geometries,
        mock_fill_missing_spatial_metrics,
        mock_autofix_rule_profile_if_needed,
        mock_save_outputs,
    ):
        record = _record()
        source_gdf = _gdf()
        final_gdf = _gdf()

        mock_resolve_project_config.return_value = {"project_name": "reserva_legal_car"}
        mock_load_and_prepare_input.return_value = source_gdf
        mock_build_auto_mapping.return_value = {"coluna": ["validate_shapefile_attribute"]}
        mock_process_in_batches.return_value = (final_gdf, {"optional_functions": []})
        mock_assign_output_identifiers.return_value = final_gdf
        mock_repair_invalid_geometries.return_value = final_gdf
        mock_fill_missing_spatial_metrics.return_value = final_gdf
        mock_save_outputs.return_value = "tests/_tmp_output/saida.gpkg"

        result = process_record(
            record,
            output_dir="tests/_tmp_output",
            id_start=5,
            use_configured_final_name=True,
            persist_individual_output=False,
        )

        self.assertEqual(result.processed_count, 1)
        self.assertEqual(result.output_path, "tests/_tmp_output/saida.gpkg")
        self.assertTrue(result.final_gdf.equals(final_gdf))
        mock_reset_validate_attribute_mappings.assert_called_once()
        mock_set_active_rule_profile.assert_called_once_with(record.rule_profile)
        mock_build_auto_mapping.assert_called_once_with(list(source_gdf.columns))
        mock_prepare_validate_shapefile_attribute_mappings.assert_called_once_with(
            source_gdf,
            {"coluna": ["validate_shapefile_attribute"]},
        )
        mock_process_in_batches.assert_called_once_with(
            source_gdf,
            {"coluna": ["validate_shapefile_attribute"]},
            id_start=5,
            project_name="reserva_legal_car",
        )
        self.assertTrue(mock_assign_output_identifiers.call_args.args[0].equals(final_gdf))
        self.assertEqual(mock_assign_output_identifiers.call_args.args[1], 5)
        self.assertTrue(mock_repair_invalid_geometries.call_args.args[0].equals(final_gdf))
        self.assertTrue(mock_fill_missing_spatial_metrics.call_args.args[0].equals(final_gdf))
        self.assertTrue(
            mock_autofix_rule_profile_if_needed.call_args.args[0].equals(final_gdf)
        )
        self.assertIs(mock_autofix_rule_profile_if_needed.call_args.args[1], record)
        self.assertEqual(
            mock_autofix_rule_profile_if_needed.call_args.args[2],
            "tests/_tmp_output",
        )
        self.assertTrue(mock_save_outputs.call_args.args[0].equals(final_gdf))
        self.assertIs(mock_save_outputs.call_args.args[1], record)
        self.assertEqual(mock_save_outputs.call_args.args[2], "tests/_tmp_output")
        self.assertEqual(
            mock_save_outputs.call_args.kwargs,
            {
                "use_configured_final_name": True,
                "persist_dataset": False,
            },
        )

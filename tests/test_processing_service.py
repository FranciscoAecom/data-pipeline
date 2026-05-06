import unittest
from types import SimpleNamespace
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point

from core.processing_service import ProcessRecordResult, ProcessingService


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


class ProcessingServiceTests(unittest.TestCase):
    @patch("core.processing_service.reset_validate_attribute_mappings")
    @patch.object(ProcessingService, "attach_rule_profile")
    @patch.object(ProcessingService, "build_context")
    @patch.object(ProcessingService, "load_input")
    def test_returns_zero_when_input_loading_fails(
        self,
        mock_load_input,
        mock_build_context,
        mock_attach_rule_profile,
        mock_reset_validate_attribute_mappings,
    ):
        record = _record()
        mock_build_context.return_value = SimpleNamespace(
            project_config={"project_name": "reserva_legal_car"},
            record=record,
            output_dir="tests/_tmp_output",
            rule_profile={},
            rule_profile_name=record.rule_profile,
            optional_functions={},
            id_start=1,
        )
        mock_load_input.side_effect = RuntimeError("falha na carga")

        result = ProcessingService().process(record, output_dir="tests/_tmp_output")

        self.assertEqual(result, ProcessRecordResult(0, None, None))
        mock_reset_validate_attribute_mappings.assert_called_once()
        mock_attach_rule_profile.assert_not_called()

    @patch("core.processing_service.log_dataset_overview")
    @patch("core.processing_service.reset_validate_attribute_mappings")
    @patch.object(ProcessingService, "attach_rule_profile")
    @patch.object(ProcessingService, "build_context")
    @patch.object(ProcessingService, "load_input")
    def test_returns_zero_when_tabular_schema_validation_fails(
        self,
        mock_load_input,
        mock_build_context,
        mock_attach_rule_profile,
        mock_reset_validate_attribute_mappings,
        mock_log_dataset_overview,
    ):
        record = _record()
        context = SimpleNamespace(
            project_config={"project_name": "reserva_legal_car"},
            record=record,
            output_dir="tests/_tmp_output",
            rule_profile=None,
            rule_profile_name=record.rule_profile,
            optional_functions={},
            id_start=1,
        )
        context_with_profile = SimpleNamespace(
            **{
                **context.__dict__,
                "gdf": _gdf(),
                "rule_profile": {
                    "fields": {
                        "sdb_cod_tema": {"accepted_values": ["ARL_AVERBADA"]},
                    }
                },
            }
        )
        mock_build_context.return_value = context
        mock_load_input.return_value = _gdf()
        mock_attach_rule_profile.return_value = context_with_profile

        result = ProcessingService().process(record, output_dir="tests/_tmp_output")

        self.assertEqual(result, ProcessRecordResult(0, None, None))
        mock_reset_validate_attribute_mappings.assert_called_once()
        mock_log_dataset_overview.assert_not_called()

    @patch("core.output_writer.save_outputs")
    @patch.object(ProcessingService, "log_autofix_summary")
    @patch.object(ProcessingService, "autofix_rule_profile")
    @patch.object(ProcessingService, "postprocess")
    @patch("core.processing_steps.process_in_batches")
    @patch("core.processing_service.prepare_validate_shapefile_attribute_mappings")
    @patch.object(ProcessingService, "build_mapping")
    @patch("core.processing_service.log_dataset_overview")
    @patch.object(ProcessingService, "attach_rule_profile")
    @patch.object(ProcessingService, "load_input")
    @patch.object(ProcessingService, "build_context")
    @patch("core.processing_service.reset_validate_attribute_mappings")
    def test_processes_record_and_returns_final_gdf(
        self,
        mock_reset_validate_attribute_mappings,
        mock_build_context,
        mock_load_input,
        mock_attach_rule_profile,
        mock_log_dataset_overview,
        mock_build_mapping,
        mock_prepare_validate_shapefile_attribute_mappings,
        mock_process_in_batches,
        mock_postprocess,
        mock_autofix_rule_profile,
        mock_log_autofix_summary,
        mock_save_outputs,
    ):
        record = _record()
        source_gdf = _gdf()
        final_gdf = _gdf()
        context = SimpleNamespace(
            project_config={"project_name": "reserva_legal_car"},
            record=record,
            output_dir="tests/_tmp_output",
            gdf=source_gdf,
            final_gdf=None,
            mapping=None,
            output_path=None,
            rule_profile={"fields": {}},
            rule_profile_name=record.rule_profile,
            optional_functions={"validate_shapefile_attribute": object()},
            id_start=5,
        )
        context_with_profile = SimpleNamespace(
            **{**context.__dict__, "rule_profile": {"fields": {}}}
        )

        mock_build_context.return_value = context
        mock_load_input.return_value = source_gdf
        mock_attach_rule_profile.return_value = context_with_profile
        mock_build_mapping.return_value = {"coluna": ["validate_shapefile_attribute"]}
        mock_process_in_batches.return_value = (final_gdf, {"optional_functions": []})
        mock_postprocess.return_value = final_gdf
        mock_autofix_rule_profile.return_value = {"changed": False}
        mock_save_outputs.return_value = "tests/_tmp_output/saida.gpkg"

        result = ProcessingService().process(
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
        mock_build_context.assert_called_once_with(record, "tests/_tmp_output", id_start=5)
        mock_attach_rule_profile.assert_called_once_with(context)
        mock_build_mapping.assert_called_once_with(context_with_profile, source_gdf)
        mock_prepare_validate_shapefile_attribute_mappings.assert_called_once_with(
            source_gdf,
            {"coluna": ["validate_shapefile_attribute"]},
            context_with_profile.rule_profile,
        )
        mock_process_in_batches.assert_called_once_with(
            source_gdf,
            {"coluna": ["validate_shapefile_attribute"]},
            id_start=5,
            project_name="reserva_legal_car",
            rule_profile=context_with_profile.rule_profile,
            optional_functions=context_with_profile.optional_functions,
        )
        mock_postprocess.assert_called_once()
        self.assertTrue(mock_autofix_rule_profile.call_args.args[1].equals(final_gdf))
        mock_log_autofix_summary.assert_called_once_with({"changed": False})
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

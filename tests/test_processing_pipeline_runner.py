import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import geopandas as gpd
from shapely.geometry import Point

from core.processing.pipeline_runner import run_processing_pipeline
from core.validation.session import ValidationSession


def _record():
    return SimpleNamespace(
        sheet_row=2,
        record_id=10,
        theme="tema_teste",
        theme_folder="rl_car_ac",
        source_path="origem_a",
        input_path="origem_a.gpkg",
        rule_profile="reserva_legal_car/rl_car_ac",
    )


def _context():
    record = _record()
    return SimpleNamespace(
        project_config={"project_name": "reserva_legal_car"},
        record=record,
        output_dir="tests/_tmp_output",
        gdf=None,
        final_gdf=None,
        mapping=None,
        output_path=None,
        rule_profile=None,
        rule_profile_name=record.rule_profile,
        optional_functions={"validate_shapefile_attribute": object()},
        id_start=5,
        validation_session=ValidationSession(),
    )


def _gdf():
    return gpd.GeoDataFrame(
        {"coluna": ["A"], "geometry": [Point(0, 0)]},
        geometry="geometry",
        crs="EPSG:4326",
    )


class ProcessingPipelineRunnerTests(unittest.TestCase):
    @patch("core.processing.pipeline_runner.attach_rule_profile_step")
    @patch("core.processing.pipeline_runner.load_input_step")
    def test_returns_none_when_input_loading_fails(
        self,
        mock_load_input_step,
        mock_attach_rule_profile_step,
    ):
        mock_load_input_step.side_effect = RuntimeError("falha na carga")

        result = run_processing_pipeline(_context(), autofix_service=Mock())

        self.assertIsNone(result)
        mock_attach_rule_profile_step.assert_not_called()

    @patch("core.processing.pipeline_runner.log_dataset_overview")
    @patch("core.processing.pipeline_runner.validate_input_schema_step")
    @patch("core.processing.pipeline_runner.attach_rule_profile_step")
    @patch("core.processing.pipeline_runner.load_input_step")
    def test_returns_none_when_tabular_schema_validation_fails(
        self,
        mock_load_input_step,
        mock_attach_rule_profile_step,
        mock_validate_input_schema_step,
        mock_log_dataset_overview,
    ):
        context = _context()
        context_with_input = SimpleNamespace(**{**context.__dict__, "gdf": _gdf()})
        context_with_profile = SimpleNamespace(
            **{**context_with_input.__dict__, "rule_profile": {"fields": {}}}
        )
        mock_load_input_step.return_value = context_with_input
        mock_attach_rule_profile_step.return_value = context_with_profile
        mock_validate_input_schema_step.side_effect = RuntimeError("schema invalido")

        result = run_processing_pipeline(context, autofix_service=Mock())

        self.assertIsNone(result)
        mock_log_dataset_overview.assert_not_called()

    @patch("core.processing.pipeline_runner.persist_outputs_step")
    @patch("core.processing.pipeline_runner.postprocess_step")
    @patch("core.processing.pipeline_runner.run_pipeline_step")
    @patch("core.processing.pipeline_runner.prepare_mapping_step")
    @patch("core.processing.pipeline_runner.log_dataset_overview")
    @patch("core.processing.pipeline_runner.validate_input_schema_step")
    @patch("core.processing.pipeline_runner.attach_rule_profile_step")
    @patch("core.processing.pipeline_runner.load_input_step")
    def test_runs_steps_and_returns_persisted_context(
        self,
        mock_load_input_step,
        mock_attach_rule_profile_step,
        mock_validate_input_schema_step,
        mock_log_dataset_overview,
        mock_prepare_mapping_step,
        mock_run_pipeline_step,
        mock_postprocess_step,
        mock_persist_outputs_step,
    ):
        context = _context()
        source_gdf = _gdf()
        final_gdf = _gdf()
        context_with_input = SimpleNamespace(**{**context.__dict__, "gdf": source_gdf})
        context_with_profile = SimpleNamespace(
            **{**context_with_input.__dict__, "rule_profile": {"fields": {}}}
        )
        context_with_mapping = SimpleNamespace(
            **{
                **context_with_profile.__dict__,
                "mapping": {"coluna": ["validate_shapefile_attribute"]},
            }
        )
        context_with_final = SimpleNamespace(
            **{**context_with_mapping.__dict__, "final_gdf": final_gdf}
        )
        persisted_context = SimpleNamespace(
            **{**context_with_final.__dict__, "output_path": "tests/_tmp_output/saida.gpkg"}
        )
        autofix_service = SimpleNamespace(
            autofix_rule_profile=Mock(return_value={"changed": False}),
            log_autofix_summary=Mock(),
        )

        mock_load_input_step.return_value = context_with_input
        mock_attach_rule_profile_step.return_value = context_with_profile
        mock_validate_input_schema_step.return_value = context_with_profile
        mock_prepare_mapping_step.return_value = context_with_mapping
        mock_run_pipeline_step.return_value = context_with_final
        mock_postprocess_step.return_value = context_with_final
        mock_persist_outputs_step.return_value = persisted_context

        result = run_processing_pipeline(
            context,
            autofix_service,
            use_configured_final_name=True,
            persist_individual_output=False,
        )

        self.assertIs(result, persisted_context)
        mock_load_input_step.assert_called_once_with(context)
        mock_attach_rule_profile_step.assert_called_once_with(context_with_input)
        mock_validate_input_schema_step.assert_called_once_with(context_with_profile)
        mock_log_dataset_overview.assert_called_once_with(source_gdf)
        mock_prepare_mapping_step.assert_called_once_with(context_with_profile)
        mock_run_pipeline_step.assert_called_once_with(context_with_mapping)
        mock_postprocess_step.assert_called_once_with(context_with_final)
        autofix_service.autofix_rule_profile.assert_called_once_with(
            context_with_final,
            final_gdf,
        )
        autofix_service.log_autofix_summary.assert_called_once_with({"changed": False})
        mock_persist_outputs_step.assert_called_once_with(
            context_with_final,
            use_configured_final_name=True,
            persist_dataset=False,
        )

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.processing.context_factory import build_processing_context
from core.validation.session import ValidationSession


def _record():
    return SimpleNamespace(
        theme_folder="rl_car_ac",
        rule_profile="reserva_legal_car/rl_car_ac",
    )


class ProcessingContextFactoryTests(unittest.TestCase):
    @patch("core.processing.context_factory.get_project_optional_functions")
    @patch("core.processing.context_factory.resolve_project_config")
    def test_builds_processing_context_from_record(
        self,
        mock_resolve_project_config,
        mock_get_project_optional_functions,
    ):
        record = _record()
        project_config = {"project_name": "reserva_legal_car"}
        optional_functions = {"validate_shapefile_attribute": object()}
        mock_resolve_project_config.return_value = project_config
        mock_get_project_optional_functions.return_value = optional_functions

        context = build_processing_context(
            record,
            "tests/_tmp_output",
            id_start=5,
        )

        self.assertIs(context.record, record)
        self.assertEqual(context.output_dir, "tests/_tmp_output")
        self.assertEqual(context.project_config, project_config)
        self.assertEqual(context.rule_profile_name, record.rule_profile)
        self.assertIsNone(context.rule_profile)
        self.assertEqual(context.optional_functions, optional_functions)
        self.assertEqual(context.id_start, 5)
        self.assertIsInstance(context.validation_session, ValidationSession)
        mock_resolve_project_config.assert_called_once_with("rl_car_ac")
        mock_get_project_optional_functions.assert_called_once_with("reserva_legal_car")

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.rules.autofix_service import RuleAutofixService


def _context():
    return SimpleNamespace(
        output_dir="tests/_tmp_output",
        record=SimpleNamespace(
            theme_folder="rl_car_ac",
            input_path="origem.gpkg",
        ),
        rule_profile_name="reserva_legal_car/rl_car_ac",
        rule_profile={"fields": {}},
    )


class RuleAutofixServiceTests(unittest.TestCase):
    @patch("core.rules.autofix_service.autofix_rule_profile_from_invalid_domains")
    def test_autofix_builds_support_report_path(self, mock_autofix):
        mock_autofix.return_value = {"changed": False}

        result = RuleAutofixService().autofix_rule_profile(_context(), "gdf")

        self.assertEqual(result, {"changed": False})
        self.assertEqual(mock_autofix.call_args.args[:3], (
            "reserva_legal_car/rl_car_ac",
            {"fields": {}},
            "gdf",
        ))
        self.assertTrue(
            mock_autofix.call_args.kwargs["support_report_path"].endswith(
                "origem_inconsistencias_dominio.xlsx"
            )
        )

    @patch("core.rules.autofix_service.log")
    def test_log_autofix_summary_ignores_unchanged_summary(self, mock_log):
        RuleAutofixService().log_autofix_summary({"changed": False})

        mock_log.assert_not_called()


if __name__ == "__main__":
    unittest.main()

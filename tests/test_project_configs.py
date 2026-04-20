import unittest

from projects.configs import (
    DEFAULT_PROJECT_CONFIG,
    get_project_config,
    resolve_project_config,
    resolve_project_name,
)


class ProjectConfigsTests(unittest.TestCase):
    def test_resolve_project_name_by_theme_prefix(self):
        self.assertEqual(resolve_project_name("app_car_ac"), "app_car")
        self.assertEqual(resolve_project_name("rl_car_sp"), "reserva_legal_car")
        self.assertEqual(resolve_project_name("estado"), "estado")
        self.assertEqual(resolve_project_name("auth_supn"), "autorizacao_para_supressao_vegetal")

    def test_resolve_project_name_falls_back_to_default(self):
        self.assertEqual(resolve_project_name("tema_desconhecido"), DEFAULT_PROJECT_CONFIG["project_name"])
        self.assertEqual(resolve_project_name(None), DEFAULT_PROJECT_CONFIG["project_name"])

    def test_resolve_project_config_returns_expected_template(self):
        config = resolve_project_config("rl_car_sp")
        self.assertEqual(config["project_name"], "reserva_legal_car")
        self.assertEqual(config["output_name_template"], "pol_pcd_{theme_folder}_{date_yyyymmdd}")

    def test_get_project_config_unknown_returns_default_copy(self):
        config = get_project_config("nao_existe")
        self.assertEqual(config["project_name"], "default")
        self.assertIsNone(config["reference_date"])


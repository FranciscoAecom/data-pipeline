import unittest
from pathlib import Path

from core.rules.engine import (
    expected_rule_profile_name,
    find_rule_profile_by_theme_folder,
    get_rule_profile_project_name,
    load_rule_profile,
    list_duplicate_rule_profile_stems,
    list_rule_profiles,
    profile_exists,
)
from settings import RULES_BASE


class RuleProfilesIntegrationTests(unittest.TestCase):
    def test_all_rule_profiles_load_and_validate(self):
        profiles = list_rule_profiles()
        self.assertGreater(len(profiles), 0)

        for profile_name in profiles:
            with self.subTest(profile_name=profile_name):
                self.assertTrue(profile_exists(profile_name))
                project_name = get_rule_profile_project_name(profile_name)
                self.assertIsInstance(project_name, str)
                self.assertTrue(project_name.strip())

    def test_expected_real_profiles_are_listed(self):
        profiles = set(list_rule_profiles())

        self.assertIn("default", profiles)
        self.assertIn("app_car/app_car_ac", profiles)
        self.assertIn("estado/estado", profiles)
        self.assertIn("reserva_legal_car/rl_car_sp", profiles)
        self.assertIn("autorizacao_para_supressao_vegetal/auth_supn", profiles)

    def test_modular_rule_profile_loads_as_consolidated_profile(self):
        profiles = set(list_rule_profiles())

        self.assertIn("reserva_legal_car/rl_car_ac", profiles)
        self.assertNotIn("reserva_legal_car/rl_car_ac/profile", profiles)
        self.assertNotIn("reserva_legal_car/rl_car_ac/domains", profiles)

        profile = load_rule_profile("reserva_legal_car/rl_car_ac")

        self.assertEqual(profile["profile_name"], "reserva_legal_car_ac")
        self.assertIn("input_schema", profile)
        self.assertIn("sdb_cod_tema", profile["fields"])
        self.assertIn("cod_tema_to_nom_tema", profile["relations"])
        self.assertEqual(
            profile["auto_functions"]["sdb_cod_tema"],
            ["validate_shapefile_attribute"],
        )

    def test_theme_folder_resolves_to_expected_project_profile(self):
        cases = {
            "app_car_ac": "app_car/app_car_ac",
            "rl_car_sp": "reserva_legal_car/rl_car_sp",
            "estado": "estado/estado",
            "auth_supn": "autorizacao_para_supressao_vegetal/auth_supn",
        }

        for theme_folder, expected_profile in cases.items():
            with self.subTest(theme_folder=theme_folder):
                self.assertEqual(expected_rule_profile_name(theme_folder), expected_profile)
                self.assertEqual(
                    find_rule_profile_by_theme_folder(theme_folder),
                    expected_profile,
                )

    def test_rule_profile_stems_are_not_duplicated(self):
        self.assertEqual(list_duplicate_rule_profile_stems(), {})

    def test_rule_profiles_do_not_contain_utf8_mojibake(self):
        bad_tokens = ("Ã", "Â", "\ufffd")

        for path in Path(RULES_BASE).rglob("*.json"):
            with self.subTest(path=str(path)):
                text = path.read_text(encoding="utf-8-sig")
                self.assertFalse(
                    any(token in text for token in bad_tokens),
                    f"Possivel problema de UTF-8 em {path}",
                )

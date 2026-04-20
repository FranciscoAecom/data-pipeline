import unittest

from core.validation.rule_engine import (
    get_rule_profile_project_name,
    list_rule_profiles,
    profile_exists,
)


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


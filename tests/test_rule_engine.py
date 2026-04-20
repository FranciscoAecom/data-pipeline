import unittest

from core.validation.rule_engine import validate_rule_profile


class ValidateRuleProfileTests(unittest.TestCase):
    def test_accepts_registered_short_function_names(self):
        profile = {
            "profile_name": "demo",
            "project_name": "reserva_legal_car",
            "auto_functions": {
                "sdb_cod_tema": ["validate_shapefile_attribute"],
                "sdb_des_condic": ["reserva_legal_car_transform_desc_condic"],
            },
            "fields": {
                "sdb_cod_tema": {
                    "accepted_values": ["A"],
                    "aliases": {"a": "A"},
                },
                "sdb_des_condic": {
                    "accepted_values": ["Analizado"],
                    "aliases": {},
                },
            },
            "relations": {},
        }

        validate_rule_profile(profile, "reserva_legal_car/demo")

    def test_rejects_unknown_optional_function(self):
        profile = {
            "profile_name": "demo",
            "project_name": "app_car",
            "auto_functions": {
                "sdb_cod_tema": ["funcao_que_nao_existe"],
            },
            "fields": {
                "sdb_cod_tema": {
                    "accepted_values": ["A"],
                    "aliases": {},
                },
            },
            "relations": {},
        }

        with self.assertRaisesRegex(ValueError, "nao esta registrada"):
            validate_rule_profile(profile, "app_car/demo")

    def test_rejects_alias_target_outside_accepted_values(self):
        profile = {
            "profile_name": "demo",
            "project_name": "estado",
            "auto_functions": {},
            "fields": {
                "sdb_nm_uf": {
                    "accepted_values": ["Acre"],
                    "aliases": {"AC": "Amazonas"},
                },
            },
            "relations": {},
        }

        with self.assertRaisesRegex(ValueError, "fora de 'accepted_values'"):
            validate_rule_profile(profile, "estado/demo")

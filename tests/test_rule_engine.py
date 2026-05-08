import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from projects.registry import get_project_optional_functions
from core.rules.engine import (
    invalidate_rule_profile_cache,
    load_rule_profile,
    save_rule_profile,
    validate_rule_profile,
)


class ValidateRuleProfileTests(unittest.TestCase):
    def tearDown(self):
        invalidate_rule_profile_cache()

    def test_accepts_registered_short_function_names(self):
        profile = {
            "profile_name": "demo",
            "project_name": "reserva_legal_car",
            "auto_functions": {
                "sdb_cod_tema": ["validate_shapefile_attribute"],
                "sdb_desc_condic": ["reserva_legal_car_transform_desc_condic"],
            },
            "fields": {
                "sdb_cod_tema": {
                    "accepted_values": ["A"],
                    "aliases": {"a": "A"},
                },
                "sdb_desc_condic": {
                    "accepted_values": ["Analizado"],
                    "aliases": {},
                },
            },
            "relations": {},
        }

        validate_rule_profile(
            profile,
            "reserva_legal_car/demo",
            optional_functions=get_project_optional_functions("reserva_legal_car"),
        )

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
            validate_rule_profile(
                profile,
                "app_car/demo",
                optional_functions=get_project_optional_functions("app_car"),
            )

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

    def test_rejects_invalid_modular_input_schema_component(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_dir = Path(temp_dir) / "rules" / "demo" / "perfil"
            self._write_modular_profile(
                profile_dir,
                input_schema={
                    "columns": {
                        "sdb_codigo": {
                            "dtype": "string",
                            "required": "sim",
                        }
                    }
                },
            )

            with patch("core.rules.engine.RULES_BASE", str(Path(temp_dir) / "rules")):
                invalidate_rule_profile_cache()
                with self.assertRaisesRegex(ValueError, "input_schema.json"):
                    load_rule_profile("demo/perfil")

    def test_rejects_invalid_modular_profile_component(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_dir = Path(temp_dir) / "rules" / "demo" / "perfil"
            self._write_modular_profile(profile_dir)
            self._write_json(
                profile_dir / "profile.json",
                {
                    "project_name": "demo",
                    "theme_folder": "perfil",
                },
            )

            with patch("core.rules.engine.RULES_BASE", str(Path(temp_dir) / "rules")):
                invalidate_rule_profile_cache()
                with self.assertRaisesRegex(ValueError, "profile.json"):
                    load_rule_profile("demo/perfil")

    def test_rejects_invalid_modular_domains_component(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_dir = Path(temp_dir) / "rules" / "demo" / "perfil"
            self._write_modular_profile(
                profile_dir,
                domains={
                    "fields": {
                        "sdb_codigo": {
                            "accepted_values": "A",
                            "aliases": {},
                        }
                    }
                },
            )

            with patch("core.rules.engine.RULES_BASE", str(Path(temp_dir) / "rules")):
                invalidate_rule_profile_cache()
                with self.assertRaisesRegex(ValueError, "domains.json"):
                    load_rule_profile("demo/perfil")

    def test_rejects_invalid_modular_pipeline_component(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_dir = Path(temp_dir) / "rules" / "demo" / "perfil"
            self._write_modular_profile(
                profile_dir,
                pipeline={
                    "auto_functions": {
                        "sdb_codigo": "validate_shapefile_attribute",
                    }
                },
            )

            with patch("core.rules.engine.RULES_BASE", str(Path(temp_dir) / "rules")):
                invalidate_rule_profile_cache()
                with self.assertRaisesRegex(ValueError, "pipeline.json"):
                    load_rule_profile("demo/perfil")

    def test_save_rule_profile_updates_modular_components(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            rules_base = Path(temp_dir) / "rules"
            profile_dir = rules_base / "demo" / "perfil"
            self._write_modular_profile(profile_dir)

            with patch("core.rules.engine.RULES_BASE", str(rules_base)):
                invalidate_rule_profile_cache()
                profile = load_rule_profile("demo/perfil")
                profile["fields"]["sdb_codigo"]["accepted_values"].append("B")
                profile["fields"]["sdb_nome"] = {
                    "accepted_values": ["Alpha"],
                    "aliases": {},
                }
                profile["relations"]["codigo_to_nome"] = {"A": "Alpha"}

                saved_path = save_rule_profile("demo/perfil", profile)

                self.assertEqual(Path(saved_path), profile_dir)
                self.assertFalse((rules_base / "demo" / "perfil.json").exists())
                domains = json.loads((profile_dir / "domains.json").read_text(encoding="utf-8"))
                relations = json.loads((profile_dir / "relations.json").read_text(encoding="utf-8"))
                self.assertIn("B", domains["fields"]["sdb_codigo"]["accepted_values"])
                self.assertEqual(
                    relations["relations"]["codigo_to_nome"],
                    {"A": "Alpha"},
                )

    def _write_modular_profile(
        self,
        profile_dir,
        input_schema=None,
        domains=None,
        relations=None,
        pipeline=None,
    ):
        profile_dir.mkdir(parents=True, exist_ok=True)
        self._write_json(
            profile_dir / "profile.json",
            {
                "profile_name": "perfil",
                "project_name": "demo",
                "theme_folder": "perfil",
            },
        )
        self._write_json(
            profile_dir / "input_schema.json",
            input_schema
            if input_schema is not None
            else {
                "columns": {
                    "sdb_codigo": {
                        "dtype": "string",
                        "required": True,
                        "nullable": True,
                    }
                }
            },
        )
        self._write_json(
            profile_dir / "domains.json",
            domains
            if domains is not None
            else {
                "fields": {
                    "sdb_codigo": {
                        "accepted_values": ["A"],
                        "aliases": {},
                    }
                }
            },
        )
        self._write_json(
            profile_dir / "relations.json",
            relations if relations is not None else {"relations": {}},
        )
        self._write_json(
            profile_dir / "pipeline.json",
            pipeline
            if pipeline is not None
            else {
                "auto_functions": {
                    "sdb_codigo": ["validate_shapefile_attribute"],
                }
            },
        )

    def _write_json(self, path, data):
        with path.open("w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
            file.write("\n")

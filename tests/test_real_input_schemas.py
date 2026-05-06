import json
import unittest
from pathlib import Path

from projects.registry import get_project_optional_functions
from core.validation.rule_engine import (
    list_rule_profiles,
    load_rule_profile,
    validate_rule_profile_semantics,
)
from core.validation.tabular_schema import get_tabular_schema


SUPPORTED_DTYPES = {
    "string",
    "str",
    "text",
    "number",
    "numeric",
    "float",
    "double",
    "integer",
    "int",
    "datetime",
    "date",
    "boolean",
    "bool",
}


class RealInputSchemaTests(unittest.TestCase):
    def test_all_real_profiles_have_valid_input_schema(self):
        profiles = list_rule_profiles()
        self.assertGreater(len(profiles), 0)

        for profile_name in profiles:
            with self.subTest(profile=profile_name):
                profile = load_rule_profile(profile_name)
                input_schema = profile.get("input_schema")
                if not input_schema or not input_schema.get("columns"):
                    continue
                schema = get_tabular_schema(profile)

                self.assertIsNotNone(schema)
                self.assertGreater(len(schema.columns), 0)
                for column, rule in schema.columns.items():
                    self.assertTrue(column.startswith("sdb_"))
                    self.assertFalse(column.startswith("acm_"))
                    self.assertIn(rule.dtype.lower(), SUPPORTED_DTYPES)
                    self.assertIsInstance(rule.required, bool)
                    self.assertIsInstance(rule.nullable, bool)

    def test_rule_json_files_do_not_use_old_desc_condic_name(self):
        offenders = []
        for path in Path("rules").rglob("*.json"):
            data = path.read_text(encoding="utf-8-sig")
            if "sdb_des_condic" in data or "acm_des_condic" in data:
                offenders.append(str(path))

        self.assertEqual(offenders, [])

    def test_input_schema_files_use_explicit_column_rule_objects(self):
        offenders = []
        for path in Path("rules").rglob("input_schema.json"):
            if any(part.startswith("_") for part in path.parts):
                continue

            data = json.loads(path.read_text(encoding="utf-8-sig"))
            columns = data.get("columns", {})
            for column, rule in columns.items():
                if not isinstance(rule, dict):
                    offenders.append(f"{path}:{column}")
                    continue

                for key in ("dtype", "required", "nullable"):
                    if key not in rule:
                        offenders.append(f"{path}:{column}.{key}")

        self.assertEqual(offenders, [])

    def test_all_real_profiles_have_registered_pipeline_functions(self):
        for profile_name in list_rule_profiles():
            with self.subTest(profile=profile_name):
                profile = load_rule_profile(profile_name)
                project_name = profile.get("project_name")
                optional_functions = get_project_optional_functions(project_name)

                validate_rule_profile_semantics(
                    profile,
                    profile_name,
                    optional_functions=optional_functions,
                )

    def test_all_real_profile_relations_reference_configured_fields(self):
        offenders = []
        for profile_name in list_rule_profiles():
            profile = load_rule_profile(profile_name)
            fields = set(profile.get("fields", {}))

            for relation_name in profile.get("relations", {}):
                if "_to_" not in relation_name:
                    offenders.append(f"{profile_name}:{relation_name}")
                    continue
                source_token, target_token = relation_name.split("_to_", 1)
                expected_source = f"sdb_{source_token}"
                expected_target = f"sdb_{target_token}"
                if expected_source not in fields and source_token not in fields:
                    offenders.append(f"{profile_name}:{relation_name}:{expected_source}")
                if expected_target not in fields and target_token not in fields:
                    offenders.append(f"{profile_name}:{relation_name}:{expected_target}")

        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()

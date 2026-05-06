import unittest
from pathlib import Path


class ArchitectureBoundaryTests(unittest.TestCase):
    def test_tabular_validation_modules_do_not_import_rule_layer(self):
        offenders = self._files_containing(
            Path("core/validation"),
            "tabular_*.py",
            ["core.rules", "core.validation.rule_"],
        )

        self.assertEqual(offenders, [])

    def test_rules_package_does_not_import_geopandas(self):
        offenders = self._files_containing(
            Path("core/rules"),
            "*.py",
            ["geopandas"],
        )

        self.assertEqual(offenders, [])

    def test_output_paths_does_not_import_geopandas(self):
        text = Path("core/output/paths.py").read_text(encoding="utf-8")

        self.assertNotIn("geopandas", text)

    def test_output_quality_does_not_write_gpkg_directly(self):
        text = Path("core/output/quality.py").read_text(encoding="utf-8")

        self.assertNotIn("write_output_gpkg", text)

    def test_legacy_rule_modules_are_compatibility_wrappers(self):
        for path in Path("core/validation").glob("rule_*.py"):
            text = path.read_text(encoding="utf-8").strip()
            self.assertIn("core.rules", text, msg=str(path))

    def _files_containing(self, root, pattern, forbidden_terms):
        offenders = []
        for path in root.glob(pattern):
            text = path.read_text(encoding="utf-8")
            if any(term in text for term in forbidden_terms):
                offenders.append(str(path))
        return offenders


if __name__ == "__main__":
    unittest.main()

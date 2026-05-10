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

    def test_legacy_rule_modules_have_been_removed(self):
        offenders = list(Path("core/validation").glob("rule_*.py"))
        offenders.extend(Path("core/validation").glob("domain_rules.py"))

        self.assertEqual([str(path) for path in offenders], [])

    def test_root_compatibility_facades_stay_thin(self):
        facade_paths = [
            Path("core/processing_service.py"),
            Path("core/processing_events.py"),
            Path("core/processing_errors.py"),
            Path("core/record_processor.py"),
            Path("core/queue_runner.py"),
            Path("core/output_manager.py"),
            Path("core/output_paths.py"),
            Path("core/output_quality.py"),
            Path("core/output_writer.py"),
        ]

        for path in facade_paths:
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("\ndef ", text, path)
            self.assertNotIn("\nclass ", text, path)

    def test_runtime_code_uses_new_queue_and_processing_modules(self):
        offenders = self._files_containing(
            Path("core"),
            "*.py",
            [
                "from core.queue_runner import",
                "from core.record_processor import",
                "from core.processing_service import",
            ],
        )

        self.assertEqual(offenders, [])

    def _files_containing(self, root, pattern, forbidden_terms):
        offenders = []
        for path in root.glob(pattern):
            text = path.read_text(encoding="utf-8")
            if any(term in text for term in forbidden_terms):
                offenders.append(str(path))
        return offenders


if __name__ == "__main__":
    unittest.main()

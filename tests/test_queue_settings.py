import unittest

from core.queue.settings import QueueRunSettings


class QueueRunSettingsTests(unittest.TestCase):
    def test_from_output_base_overrides_only_output_base(self):
        settings = QueueRunSettings.from_output_base("tests/_tmp_output")

        self.assertEqual(settings.output_base, "tests/_tmp_output")
        self.assertIsInstance(settings.enable_group_consolidation, bool)
        self.assertIsInstance(settings.keep_individual_outputs_when_grouping, bool)

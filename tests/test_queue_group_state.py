import unittest
from types import SimpleNamespace

from core.processing.result import ProcessRecordResult
from core.queue.group_state import QueueGroupState


def _record(theme_folder, source_path):
    return SimpleNamespace(
        sheet_row=2,
        record_id=10,
        theme_folder=theme_folder,
        source_path=source_path,
    )


class QueueGroupStateTests(unittest.TestCase):
    def test_tracks_group_counts_and_id_start(self):
        first = _record("rl_car_ac", "origem_a")
        second = _record("rl_car_ac", "origem_a")
        state = QueueGroupState([first, second], enable_group_consolidation=True)

        self.assertTrue(state.is_grouped_consolidation(first))
        self.assertEqual(state.id_start_for(first), 1)

        state.register_result(first, ProcessRecordResult(3, None, "gdf1"))

        self.assertEqual(state.id_start_for(second), 4)

    def test_output_flags_for_single_record(self):
        record = _record("auth_supn", "origem_b")
        state = QueueGroupState([record], enable_group_consolidation=True)

        self.assertFalse(state.is_grouped_consolidation(record))
        self.assertTrue(state.use_configured_final_name(record))
        self.assertTrue(
            state.persist_individual_output(
                record,
                keep_individual_outputs_when_grouping=False,
            )
        )

    def test_context_log_and_append_flags(self):
        record = _record("rl_car_ac", "origem_a")
        state = QueueGroupState([record, record], enable_group_consolidation=True)

        self.assertTrue(state.should_reset_context_log(record))
        state.mark_context_log_started(record)
        self.assertFalse(state.should_reset_context_log(record))

        self.assertFalse(state.append_started_for(record))
        self.assertTrue(
            state.should_append_consolidated_output(
                record,
                ProcessRecordResult(1, None, "gdf"),
            )
        )
        state.mark_append_started(record)
        self.assertTrue(state.append_started_for(record))

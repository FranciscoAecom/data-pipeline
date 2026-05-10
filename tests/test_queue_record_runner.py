import unittest
from types import SimpleNamespace
from unittest.mock import call, patch

from core.processing.result import ProcessRecordResult
from core.queue.group_state import QueueGroupState
from core.queue.record_runner import run_queue_record


def _record(sheet_row, record_id, theme_folder, source_path):
    return SimpleNamespace(
        sheet_row=sheet_row,
        record_id=record_id,
        theme="tema_teste",
        status="Waiting Update",
        theme_folder=theme_folder,
        source_path=source_path,
        input_path=f"{source_path}.gpkg",
        rule_profile=f"{theme_folder}/{theme_folder}",
    )


class QueueRecordRunnerTests(unittest.TestCase):
    @patch("core.queue.record_runner.clear_context_log")
    @patch("core.queue.record_runner.append_group_consolidated_output")
    @patch("core.queue.record_runner.process_record")
    @patch("core.queue.record_runner.set_context_log")
    def test_processes_record_and_updates_group_state(
        self,
        mock_set_context_log,
        mock_process_record,
        mock_append_group_consolidated_output,
        mock_clear_context_log,
    ):
        records = [
            _record(2, 10, "rl_car_ac", "origem_a"),
            _record(2, 10, "rl_car_ac", "origem_a"),
        ]
        group_state = QueueGroupState(records, enable_group_consolidation=True)
        mock_process_record.side_effect = [
            ProcessRecordResult(3, None, "gdf1"),
            ProcessRecordResult(2, None, "gdf2"),
        ]

        run_queue_record(
            records[0],
            "tests/_tmp_output",
            group_state,
            keep_individual_outputs_when_grouping=False,
        )
        run_queue_record(
            records[1],
            "tests/_tmp_output",
            group_state,
            keep_individual_outputs_when_grouping=False,
        )

        self.assertEqual(mock_process_record.call_args_list[0].kwargs["id_start"], 1)
        self.assertEqual(mock_process_record.call_args_list[1].kwargs["id_start"], 4)
        self.assertEqual(
            [
                call_args.kwargs["persist_individual_output"]
                for call_args in mock_process_record.call_args_list
            ],
            [False, False],
        )
        self.assertEqual(
            mock_append_group_consolidated_output.call_args_list,
            [
                call(records[0], "gdf1", "tests/_tmp_output", append=False),
                call(records[1], "gdf2", "tests/_tmp_output", append=True),
            ],
        )
        self.assertEqual(mock_set_context_log.call_count, 2)
        self.assertEqual(mock_clear_context_log.call_count, 2)

    @patch("core.queue.record_runner.clear_context_log")
    @patch("core.queue.record_runner.process_record", side_effect=RuntimeError("boom"))
    @patch("core.queue.record_runner.set_context_log")
    def test_clears_context_log_even_when_record_processing_fails(
        self,
        mock_set_context_log,
        mock_process_record,
        mock_clear_context_log,
    ):
        record = _record(2, 10, "rl_car_ac", "origem_a")
        group_state = QueueGroupState([record], enable_group_consolidation=True)

        with self.assertRaisesRegex(RuntimeError, "boom"):
            run_queue_record(
                record,
                "tests/_tmp_output",
                group_state,
                keep_individual_outputs_when_grouping=False,
            )

        mock_set_context_log.assert_called_once()
        mock_process_record.assert_called_once()
        mock_clear_context_log.assert_called_once()

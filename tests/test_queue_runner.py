import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import call, patch

from core.processing.result import ProcessRecordResult
from core.queue_runner import run_processing_queue


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


class QueueRunnerTests(unittest.TestCase):
    def setUp(self):
        self.output_base = str(Path("tests") / "_tmp_output")

    @patch("core.queue_runner.clear_context_log")
    @patch("core.queue_runner.append_group_consolidated_output")
    @patch("core.queue_runner.process_record")
    @patch("core.queue_runner.set_context_log")
    @patch("core.queue_runner.log_queue_summary")
    @patch("core.queue_runner.load_processing_queue")
    def test_grouped_records_increment_ids_and_append_consolidated_output(
        self,
        mock_load_processing_queue,
        mock_log_queue_summary,
        mock_set_context_log,
        mock_process_record,
        mock_append_group_consolidated_output,
        mock_clear_context_log,
    ):
        records = [
            _record(2, 10, "rl_car_ac", "origem_a"),
            _record(2, 10, "rl_car_ac", "origem_a"),
        ]
        mock_load_processing_queue.return_value = (
            records,
            [],
            {"total_records": 2, "ready_candidates": 2, "eligible_records": 2, "issues": 0},
        )
        mock_process_record.side_effect = [
            ProcessRecordResult(3, None, "gdf1"),
            ProcessRecordResult(2, None, "gdf2"),
        ]

        run_processing_queue(output_base=self.output_base)

        mock_log_queue_summary.assert_called_once()
        self.assertEqual(mock_process_record.call_args_list[0].kwargs["id_start"], 1)
        self.assertEqual(mock_process_record.call_args_list[1].kwargs["id_start"], 4)
        self.assertEqual(mock_process_record.call_count, 2)
        self.assertEqual(mock_append_group_consolidated_output.call_count, 2)
        self.assertEqual(
            mock_append_group_consolidated_output.call_args_list,
            [
                call(records[0], "gdf1", self.output_base, append=False),
                call(records[1], "gdf2", self.output_base, append=True),
            ],
        )
        self.assertEqual(mock_clear_context_log.call_count, 2)
        self.assertEqual(mock_set_context_log.call_count, 2)

    @patch("core.queue_runner.clear_context_log")
    @patch("core.queue_runner.process_record", side_effect=RuntimeError("boom"))
    @patch("core.queue_runner.set_context_log")
    @patch("core.queue_runner.log_queue_summary")
    @patch("core.queue_runner.load_processing_queue")
    def test_clears_context_log_even_when_record_processing_fails(
        self,
        mock_load_processing_queue,
        mock_log_queue_summary,
        mock_set_context_log,
        mock_process_record,
        mock_clear_context_log,
    ):
        records = [_record(2, 10, "rl_car_ac", "origem_a")]
        mock_load_processing_queue.return_value = (
            records,
            [],
            {"total_records": 1, "ready_candidates": 1, "eligible_records": 1, "issues": 0},
        )

        with self.assertRaisesRegex(RuntimeError, "boom"):
            run_processing_queue(output_base=self.output_base)

        mock_log_queue_summary.assert_called_once()
        mock_set_context_log.assert_called_once()
        mock_clear_context_log.assert_called_once()

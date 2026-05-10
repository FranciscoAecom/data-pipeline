from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from core.queue.queue_loader import QueueRunContext
from core.queue.runner import run_processing_queue


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

    @patch("core.queue.runner.run_queue_record")
    @patch("core.queue.runner.prepare_processing_queue")
    def test_runs_each_record_with_group_state(
        self,
        mock_prepare_processing_queue,
        mock_run_queue_record,
    ):
        records = [
            _record(2, 10, "rl_car_ac", "origem_a"),
            _record(2, 10, "rl_car_ac", "origem_a"),
        ]
        mock_prepare_processing_queue.return_value = QueueRunContext(
            records=records,
            output_dir=self.output_base,
        )

        run_processing_queue(output_base=self.output_base)

        mock_prepare_processing_queue.assert_called_once_with(self.output_base)
        self.assertEqual(mock_run_queue_record.call_count, 2)
        self.assertIs(mock_run_queue_record.call_args_list[0].args[0], records[0])
        self.assertIs(mock_run_queue_record.call_args_list[1].args[0], records[1])
        self.assertEqual(mock_run_queue_record.call_args_list[0].args[1], self.output_base)
        self.assertEqual(
            mock_run_queue_record.call_args_list[0].kwargs,
            {"keep_individual_outputs_when_grouping": False},
        )

    @patch("core.queue.runner.run_queue_record", side_effect=RuntimeError("boom"))
    @patch("core.queue.runner.prepare_processing_queue")
    def test_propagates_record_processing_errors(
        self,
        mock_prepare_processing_queue,
        mock_run_queue_record,
    ):
        records = [_record(2, 10, "rl_car_ac", "origem_a")]
        mock_prepare_processing_queue.return_value = QueueRunContext(
            records=records,
            output_dir=self.output_base,
        )

        with self.assertRaisesRegex(RuntimeError, "boom"):
            run_processing_queue(output_base=self.output_base)

        mock_prepare_processing_queue.assert_called_once_with(self.output_base)
        mock_run_queue_record.assert_called_once()

    @patch("core.queue.runner.run_queue_record")
    @patch("core.queue.runner.prepare_processing_queue", return_value=None)
    def test_returns_when_queue_cannot_be_prepared(
        self,
        mock_prepare_processing_queue,
        mock_run_queue_record,
    ):
        run_processing_queue(output_base=self.output_base)

        mock_prepare_processing_queue.assert_called_once_with(self.output_base)
        mock_run_queue_record.assert_not_called()

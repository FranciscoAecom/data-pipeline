import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from core.queue.queue_loader import QueueRunContext, prepare_processing_queue


def _record():
    return SimpleNamespace(
        sheet_row=2,
        record_id=10,
        theme_folder="rl_car_ac",
        source_path="origem_a",
    )


class QueueLoaderTests(unittest.TestCase):
    def setUp(self):
        self.output_base = str(Path("tests") / "_tmp_output")

    @patch("core.queue.queue_loader.os.makedirs")
    @patch("core.queue.queue_loader.log_queue_summary")
    @patch("core.queue.queue_loader.load_processing_queue")
    def test_prepares_queue_context(
        self,
        mock_load_processing_queue,
        mock_log_queue_summary,
        mock_makedirs,
    ):
        records = [_record()]
        summary = {"total_records": 1}
        issues = []
        mock_load_processing_queue.return_value = (records, issues, summary)

        result = prepare_processing_queue(self.output_base)

        self.assertEqual(
            result,
            QueueRunContext(records=records, output_dir=self.output_base),
        )
        mock_log_queue_summary.assert_called_once_with(summary, issues)
        mock_makedirs.assert_called_once_with(self.output_base, exist_ok=True)

    @patch("core.queue.queue_loader.log")
    @patch("core.queue.queue_loader.log_queue_summary")
    @patch("core.queue.queue_loader.load_processing_queue")
    def test_returns_none_for_empty_queue(
        self,
        mock_load_processing_queue,
        mock_log_queue_summary,
        mock_log,
    ):
        summary = {"total_records": 0}
        issues = []
        mock_load_processing_queue.return_value = ([], issues, summary)

        result = prepare_processing_queue(self.output_base)

        self.assertIsNone(result)
        mock_log_queue_summary.assert_called_once_with(summary, issues)
        mock_log.assert_called_once_with("Nenhum arquivo elegivel encontrado para iniciar a esteira.")

    @patch("core.queue.queue_loader.log")
    @patch("core.queue.queue_loader.load_processing_queue")
    def test_returns_none_when_queue_loading_fails(
        self,
        mock_load_processing_queue,
        mock_log,
    ):
        mock_load_processing_queue.side_effect = RuntimeError("boom")

        result = prepare_processing_queue(self.output_base)

        self.assertIsNone(result)
        mock_log.assert_called_once_with("Erro ao carregar a fila ingest: boom")

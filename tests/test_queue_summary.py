import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.queue.summary import log_queue_summary


class QueueSummaryTests(unittest.TestCase):
    @patch("core.queue.summary.log")
    def test_logs_queue_summary_and_issues(self, mock_log):
        summary = {
            "total_records": 2,
            "ready_candidates": 1,
            "eligible_records": 1,
            "issues": 1,
        }
        issues = [
            SimpleNamespace(
                sheet_row=3,
                record_id=20,
                theme_folder="",
                reason="arquivo ausente",
            )
        ]

        log_queue_summary(summary, issues)

        messages = [call.args[0] for call in mock_log.call_args_list]
        self.assertIn("Resumo da planilha ingest:", messages)
        self.assertIn("  Registros lidos: 2", messages)
        self.assertIn("Excecoes encontradas na fila ingest:", messages)
        self.assertIn(
            "  Linha 3 | ID=20 | theme_folder=<vazio> | motivo=arquivo ausente",
            messages,
        )

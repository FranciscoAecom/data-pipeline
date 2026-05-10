import unittest


class CompatibilityFacadeTests(unittest.TestCase):
    def test_processing_facades_reexport_public_api(self):
        from core.processing.service import ProcessingService
        from core.processing.result import ProcessRecordResult
        from core.processing_events import emit_record_start_events
        from core.processing_errors import ProcessingError
        from core.processing_service import ProcessingService as LegacyProcessingService
        from core.record_processor import process_record

        self.assertIs(LegacyProcessingService, ProcessingService)
        self.assertEqual(ProcessRecordResult(0, None, None).processed_count, 0)
        self.assertTrue(callable(emit_record_start_events))
        self.assertTrue(issubclass(ProcessingError, Exception))
        self.assertTrue(callable(process_record))

    def test_queue_and_output_facades_reexport_public_api(self):
        from core.output.columns import drop_internal_output_columns
        from core.output.identifiers import assign_output_identifiers
        from core.output_manager import assign_output_identifiers as legacy_assign_ids
        from core.output_manager import drop_internal_output_columns as legacy_drop_columns
        from core.queue.runner import run_processing_queue
        from core.queue_runner import run_processing_queue as legacy_run_queue

        self.assertIs(legacy_run_queue, run_processing_queue)
        self.assertIs(legacy_assign_ids, assign_output_identifiers)
        self.assertIs(legacy_drop_columns, drop_internal_output_columns)

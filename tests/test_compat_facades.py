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

    def test_domain_facades_reexport_new_module_apis(self):
        from core.io.dataset import read_input_dataset
        from core.output.naming import build_final_output_base_name
        from core.processing.batch import process_in_batches
        from core.processing.context import ProcessingContext
        from core.processing.mandatory_pipeline import run_pipeline
        from core.processing.operations import infer_operation_kind
        from core.rules.runtime import build_auto_mapping
        from core.rules.unique_values import export_unique_values_from_dataframe
        from core.spatial.repair import repair_invalid_geometries
        from core.validation.schema import target_column_name

        from core.batch_processor import process_in_batches as legacy_process_in_batches
        from core.dataset_io import read_input_dataset as legacy_read_input_dataset
        from core.execution_context import ProcessingContext as legacy_processing_context
        from core.geometry_repair import repair_invalid_geometries as legacy_repair_invalid
        from core.helper_unique_values import (
            export_unique_values_from_dataframe as legacy_export_unique_values,
        )
        from core.naming import build_final_output_base_name as legacy_build_base_name
        from core.pipeline import run_pipeline as legacy_run_pipeline
        from core.pipeline_operations import infer_operation_kind as legacy_infer_operation_kind
        from core.rule_runtime import build_auto_mapping as legacy_build_auto_mapping
        from core.schema import target_column_name as legacy_target_column_name

        self.assertIs(legacy_process_in_batches, process_in_batches)
        self.assertIs(legacy_read_input_dataset, read_input_dataset)
        self.assertIs(legacy_processing_context, ProcessingContext)
        self.assertIs(legacy_repair_invalid, repair_invalid_geometries)
        self.assertIs(legacy_export_unique_values, export_unique_values_from_dataframe)
        self.assertIs(legacy_build_base_name, build_final_output_base_name)
        self.assertIs(legacy_run_pipeline, run_pipeline)
        self.assertIs(legacy_infer_operation_kind, infer_operation_kind)
        self.assertIs(legacy_build_auto_mapping, build_auto_mapping)
        self.assertIs(legacy_target_column_name, target_column_name)

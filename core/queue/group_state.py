from collections import defaultdict

from core.output.manager import build_processing_group_key


class QueueGroupState:
    def __init__(self, records, enable_group_consolidation):
        self.enable_group_consolidation = enable_group_consolidation
        self.expected_counts = defaultdict(int)
        self.next_id_start = defaultdict(lambda: 1)
        self.append_started = defaultdict(bool)
        self.context_log_started = defaultdict(bool)

        for record in records:
            self.expected_counts[self.group_key(record)] += 1

    def group_key(self, record):
        return build_processing_group_key(record)

    def is_grouped_consolidation(self, record):
        return self.enable_group_consolidation and self.expected_count(record) > 1

    def expected_count(self, record):
        return self.expected_counts[self.group_key(record)]

    def id_start_for(self, record):
        return self.next_id_start[self.group_key(record)]

    def use_configured_final_name(self, record):
        return self.expected_count(record) == 1 or not self.enable_group_consolidation

    def persist_individual_output(self, record, keep_individual_outputs_when_grouping):
        return (
            not self.is_grouped_consolidation(record)
            or keep_individual_outputs_when_grouping
        )

    def should_reset_context_log(self, record):
        return not self.context_log_started[self.group_key(record)]

    def mark_context_log_started(self, record):
        self.context_log_started[self.group_key(record)] = True

    def register_result(self, record, result):
        if result.processed_count:
            self.next_id_start[self.group_key(record)] += result.processed_count

    def should_append_consolidated_output(self, record, result):
        return (
            self.enable_group_consolidation
            and self.expected_count(record) > 1
            and result.final_gdf is not None
        )

    def append_started_for(self, record):
        return self.append_started[self.group_key(record)]

    def mark_append_started(self, record):
        self.append_started[self.group_key(record)] = True

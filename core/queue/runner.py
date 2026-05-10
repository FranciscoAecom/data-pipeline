from core.queue.group_state import QueueGroupState
from core.queue.queue_loader import prepare_processing_queue
from core.queue.record_runner import run_queue_record
from core.queue.settings import QueueRunSettings
from core.utils import log


def run_processing_queue(output_base=None, settings=None):
    settings = settings or QueueRunSettings.from_output_base(output_base)
    queue_context = prepare_processing_queue(settings.output_base)
    if queue_context is None:
        return

    group_state = QueueGroupState(
        queue_context.records,
        enable_group_consolidation=settings.enable_group_consolidation,
    )

    for record in queue_context.records:
        run_queue_record(
            record,
            queue_context.output_dir,
            group_state,
            keep_individual_outputs_when_grouping=(
                settings.keep_individual_outputs_when_grouping
            ),
        )

    log("Processamento finalizado")

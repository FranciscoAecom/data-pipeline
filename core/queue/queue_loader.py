import os
from dataclasses import dataclass

from core.ingest.loader import load_processing_queue
from core.queue.summary import log_queue_summary
from core.utils import log


@dataclass(frozen=True)
class QueueRunContext:
    records: list
    output_dir: str


def prepare_processing_queue(output_base):
    try:
        processing_queue, queue_issues, queue_summary = load_processing_queue()
    except Exception as exc:
        log(f"Erro ao carregar a fila ingest: {exc}")
        return None

    log_queue_summary(queue_summary, queue_issues)

    if not processing_queue:
        log("Nenhum arquivo elegivel encontrado para iniciar a esteira.")
        return None

    output_dir = str(output_base)
    os.makedirs(output_dir, exist_ok=True)
    return QueueRunContext(records=processing_queue, output_dir=output_dir)

# main.py

from core.queue.runner import run_processing_queue
from core.utils import log


def main():
    log("DATA PIPELINE")
    log("Modo de execucao: fila automatica por planilha ingest")
    run_processing_queue()


if __name__ == "__main__":
    main()

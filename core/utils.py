from datetime import datetime
from contextlib import contextmanager
import os
from time import perf_counter

CONTEXT_LOG_FILE = None


def set_context_log(path, reset=False):
    global CONTEXT_LOG_FILE

    CONTEXT_LOG_FILE = path
    if not CONTEXT_LOG_FILE:
        return

    context_dir = os.path.dirname(CONTEXT_LOG_FILE)
    if context_dir:
        os.makedirs(context_dir, exist_ok=True)

    if reset and os.path.exists(CONTEXT_LOG_FILE):
        os.remove(CONTEXT_LOG_FILE)


def clear_context_log():
    global CONTEXT_LOG_FILE
    CONTEXT_LOG_FILE = None


def log(message="", raw=False):

    global CONTEXT_LOG_FILE

    # modo raw (sem timestamp)
    if raw:
        text = message
    else:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        text = f"[{timestamp}] {message}"

    print(text, flush=True)

    if CONTEXT_LOG_FILE:
        with open(CONTEXT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(text + "\n")


@contextmanager
def timed_log_step(label):
    start = perf_counter()
    try:
        yield
    finally:
        elapsed = perf_counter() - start
        log(f"{label} concluido em {elapsed:.2f}s")

"""Tests for nassync.workers — task queue, dedup, delayed re-enqueue, shutdown."""

import sys
import time
import queue
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nassync.workers import TaskWorkerPool


class FakeCfg:
    TASK_QUEUE_MAX_SIZE = 100


class TrackingProcessor:
    """Records process() calls and allows configuring return values."""

    def __init__(self, return_value=None, side_effect=None):
        self.cfg = FakeCfg()
        self.processed = []
        self.return_value = return_value
        self.side_effect = side_effect
        self._lock = threading.Lock()

    def process(self, path):
        with self._lock:
            self.processed.append(path)
        if self.side_effect:
            raise self.side_effect
        return self.return_value


# ── Basic enqueue / process tests ────────────────────────────────────


def test_enqueue_and_process():
    """Enqueued file should be processed by a worker."""
    processor = TrackingProcessor()
    pool = TaskWorkerPool(processor, worker_count=1, max_queue_size=10)

    pool.enqueue(Path("/a/b.zip"))
    pool.queue.join()
    pool.shutdown()

    assert len(processor.processed) == 1
    assert processor.processed[0] == Path("/a/b.zip")


def test_enqueue_dedup_in_queue():
    """Same path enqueued twice should only be processed once."""
    processor = TrackingProcessor()
    pool = TaskWorkerPool(processor, worker_count=1, max_queue_size=10)

    pool.enqueue(Path("/a/b.zip"))
    pool.enqueue(Path("/a/b.zip"))
    pool.queue.join()
    pool.shutdown()

    assert len(processor.processed) == 1


def test_enqueue_full_queue_drops():
    """Enqueue beyond queue capacity should be silently dropped."""
    processor = TrackingProcessor()
    pool = TaskWorkerPool(processor, worker_count=0, max_queue_size=2)

    pool.enqueue(Path("/a/1.zip"))
    pool.enqueue(Path("/a/2.zip"))
    pool.enqueue(Path("/a/3.zip"))  # should be dropped

    assert pool.queue.qsize() == 2
    pool.shutdown(wait=False)


def test_enqueue_after_shutdown_ignored():
    """Enqueue after shutdown should be ignored."""
    processor = TrackingProcessor()
    pool = TaskWorkerPool(processor, worker_count=1, max_queue_size=10)
    pool.shutdown()

    pool.enqueue(Path("/a/b.zip"))
    assert pool.queue.qsize() == 0


# ── Delayed re-enqueue test ─────────────────────────────────────────


def test_delayed_reenqueue():
    """process() returning a delay should cause re-enqueue after delay."""
    call_count = 0

    class CountProcessor:
        cfg = FakeCfg()

        def process(self, path):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return 0.1  # request delayed re-enqueue
            return None

    processor = CountProcessor()
    pool = TaskWorkerPool(processor, worker_count=1, max_queue_size=10)

    pool.enqueue(Path("/a/retry.zip"))
    time.sleep(0.5)  # wait for delayed re-enqueue
    pool.queue.join()
    pool.shutdown()

    assert call_count == 2


# ── Worker exception safety test ─────────────────────────────────────


def test_worker_survives_uncaught_exception():
    """Worker should continue after uncaught exception."""
    call_count = 0

    class CrashOnceProcessor:
        cfg = FakeCfg()

        def process(self, path):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("boom")
            return None

    processor = CrashOnceProcessor()
    pool = TaskWorkerPool(processor, worker_count=1, max_queue_size=10)

    pool.enqueue(Path("/a/crash.zip"))
    pool.queue.join()
    pool.enqueue(Path("/a/ok.zip"))
    pool.queue.join()
    pool.shutdown()

    assert call_count == 2

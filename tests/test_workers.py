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
    TASK_FETCH_BATCH_SIZE = 20
    TASK_LOCK_SEC = 120


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


def test_persistent_queue_claim_batch_processes_all_items():
    class BatchCfg(FakeCfg):
        TASK_FETCH_BATCH_SIZE = 3

    class BatchProcessor:
        cfg = BatchCfg()

        def __init__(self):
            self.processed = []

        def process(self, path):
            self.processed.append(path)
            return None

    class BatchPg:
        def __init__(self):
            self.calls = 0

        def claim_due_tasks(self, worker_id, batch_size, lock_sec):
            self.calls += 1
            if self.calls == 1:
                return [Path("/db/1.zip"), Path("/db/2.zip"), Path("/db/3.zip")]
            return []

    processor = BatchProcessor()
    pg = BatchPg()
    pool = TaskWorkerPool(
        processor,
        worker_count=1,
        max_queue_size=10,
        pg=pg,
        persistent_queue=True,
        worker_id_prefix="test",
    )

    deadline = time.time() + 3
    while time.time() < deadline and len(processor.processed) < 3:
        time.sleep(0.05)

    pool.shutdown(wait=False)
    assert processor.processed == [Path("/db/1.zip"), Path("/db/2.zip"), Path("/db/3.zip")]


def test_persistent_queue_not_starved_by_memory_queue():
    class FairCfg(FakeCfg):
        TASK_FETCH_BATCH_SIZE = 2

    class FairProcessor:
        cfg = FairCfg()

        def __init__(self):
            self.processed = []

        def process(self, path):
            self.processed.append(path)
            time.sleep(0.01)
            return None

    class FairPg:
        def __init__(self):
            self.claim_calls = 0

        def enqueue_task(self, _path):
            return None

        def claim_due_tasks(self, worker_id, batch_size, lock_sec):
            self.claim_calls += 1
            if self.claim_calls == 1:
                return [Path("/db/retry.zip")]
            return []

    processor = FairProcessor()
    pg = FairPg()
    pool = TaskWorkerPool(
        processor,
        worker_count=1,
        max_queue_size=20,
        pg=pg,
        persistent_queue=True,
        worker_id_prefix="test",
    )

    for i in range(6):
        pool.enqueue(Path(f"/mem/{i}.zip"))

    deadline = time.time() + 5
    while time.time() < deadline:
        if Path("/db/retry.zip") in processor.processed:
            break
        time.sleep(0.05)

    pool.shutdown(wait=False)
    assert Path("/db/retry.zip") in processor.processed


def test_non_leader_does_not_process_memory_task_with_persistent_queue():
    class RoleCfg(FakeCfg):
        TASK_LOCK_RENEW_INTERVAL_SEC = 1

    class RoleProcessor:
        cfg = RoleCfg()

        def __init__(self):
            self.processed = []

        def process(self, path):
            self.processed.append(path)
            return None

    class RolePg:
        def __init__(self):
            self.enqueued = []

        def enqueue_task(self, path):
            self.enqueued.append(Path(path))

        def claim_due_tasks(self, worker_id, batch_size, lock_sec):
            return []

    processor = RoleProcessor()
    pg = RolePg()
    pool = TaskWorkerPool(
        processor,
        worker_count=1,
        max_queue_size=10,
        pg=pg,
        persistent_queue=True,
        worker_id_prefix="test",
        can_process=lambda: False,
    )

    pool.enqueue(Path("/mem/not-leader.zip"))
    deadline = time.time() + 1
    while time.time() < deadline and pool.queue.qsize() > 0:
        time.sleep(0.02)

    pool.shutdown(wait=False)
    assert processor.processed == []
    assert Path("/mem/not-leader.zip") in pg.enqueued

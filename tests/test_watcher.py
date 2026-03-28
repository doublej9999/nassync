"""Tests for nassync.watcher — event dedup and lifecycle."""

import sys
import time
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nassync.watcher import Handler, ServiceLifecycle


class FakeWorkerPool:
    """Minimal worker pool that records enqueued paths."""

    def __init__(self):
        self.enqueued = []

    def enqueue(self, path):
        self.enqueued.append(path)


# ── Handler dedup tests ─────────────────────────────────────────────


def test_handler_dedup_suppresses_duplicate_within_window():
    pool = FakeWorkerPool()
    handler = Handler(pool, dedup_window_sec=5.0)

    handler._handle_event(Path("/a/b.zip"))
    handler._handle_event(Path("/a/b.zip"))  # should be deduped

    assert len(pool.enqueued) == 1


def test_handler_dedup_allows_different_paths():
    pool = FakeWorkerPool()
    handler = Handler(pool, dedup_window_sec=5.0)

    handler._handle_event(Path("/a/b.zip"))
    handler._handle_event(Path("/a/c.zip"))

    assert len(pool.enqueued) == 2


def test_handler_dedup_disabled_when_window_is_zero():
    pool = FakeWorkerPool()
    handler = Handler(pool, dedup_window_sec=0.0)

    handler._handle_event(Path("/a/b.zip"))
    handler._handle_event(Path("/a/b.zip"))

    assert len(pool.enqueued) == 2


def test_handler_dedup_evicts_old_entries():
    pool = FakeWorkerPool()
    handler = Handler(pool, dedup_window_sec=0.5, dedup_max_entries=100)

    for i in range(150):
        handler._handle_event(Path(f"/a/{i}.zip"))

    assert len(handler._last_event_ts) <= 100
    assert len(pool.enqueued) == 150


def test_handler_disable_stops_events():
    pool = FakeWorkerPool()
    handler = Handler(pool, dedup_window_sec=0.0)

    handler.disable()
    handler._handle_event(Path("/a/b.zip"))

    assert len(pool.enqueued) == 0
    assert handler.is_active() is False


# ── ServiceLifecycle tests ───────────────────────────────────────────


def test_lifecycle_web_health():
    handler = Handler(FakeWorkerPool())
    lifecycle = ServiceLifecycle(handler)

    assert lifecycle.web_healthy() is False
    lifecycle.mark_web_started()
    assert lifecycle.web_healthy() is True
    lifecycle.mark_web_stopped()
    assert lifecycle.web_healthy() is False


def test_lifecycle_shutdown():
    handler = Handler(FakeWorkerPool())
    lifecycle = ServiceLifecycle(handler)

    assert lifecycle.is_shutting_down() is False
    lifecycle.request_shutdown()
    assert lifecycle.is_shutting_down() is True
    assert handler.is_active() is False


def test_lifecycle_reload_request():
    handler = Handler(FakeWorkerPool())
    lifecycle = ServiceLifecycle(handler)

    assert lifecycle.consume_reload_request() is False
    lifecycle.request_reload()
    assert lifecycle.consume_reload_request() is True
    assert lifecycle.consume_reload_request() is False

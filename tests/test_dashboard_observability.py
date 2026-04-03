import json
import sys
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nassync.dashboard import create_dashboard_handler


class _FakePg:
    def check_health(self):
        return True, None

    def get_dashboard_metrics(self, scope="all"):
        return {
            "summary": {
                "total_tasks": 1,
                "pending_tasks": 0,
                "success_tasks": 1,
                "failed_tasks": 0,
                "feedback_tasks": 0,
                "normal_tasks": 1,
                "total_records": 1,
            },
            "by_type": [],
            "generated_at": "2026-01-01 00:00:00",
            "scope": scope,
        }

    def get_recent_tasks(self, **kwargs):
        return {"items": [], "page": 1, "page_size": 20, "total": 0, "total_pages": 1}

    def get_recent_records(self, **kwargs):
        return {"items": [], "page": 1, "page_size": 20, "total": 0, "total_pages": 1}


class _FakeLifecycle:
    def __init__(self, watcher_ok, web_ok):
        self._watcher_ok = watcher_ok
        self._web_ok = web_ok

    def watcher_healthy(self):
        return self._watcher_ok

    def web_healthy(self):
        return self._web_ok

    @staticmethod
    def is_shutting_down():
        return False


class _FakeCfg:
    DASHBOARD_CACHE_TTL_SEC = 0


class _FakeQueue:
    def __init__(self, size):
        self._size = size

    def qsize(self):
        return self._size


class _FakeWorkerPool:
    def __init__(self, size):
        self.queue = _FakeQueue(size)


class _FakeRuntimeMetrics:
    @staticmethod
    def snapshot():
        return {"events_received_total": 5, "queue_full_drop_total": 2}


def _start_server(handler_cls):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _get_json(url):
    with urllib.request.urlopen(url, timeout=3) as resp:
        return json.loads(resp.read().decode("utf-8"))


def test_healthz_reports_degraded_when_watcher_down_and_includes_queue_depth():
    handler_cls = create_dashboard_handler(
        _FakePg(),
        _FakeLifecycle(watcher_ok=False, web_ok=True),
        _FakeCfg(),
        runtime_metrics=_FakeRuntimeMetrics(),
        worker_pool=_FakeWorkerPool(7),
    )
    server, thread = _start_server(handler_cls)
    try:
        payload = _get_json(f"http://127.0.0.1:{server.server_port}/healthz")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert payload["status"] == "degraded"
    assert payload["watcher"] == "stopped"
    assert payload["queue_depth"] == 7


def test_dashboard_api_includes_runtime_metrics_snapshot():
    handler_cls = create_dashboard_handler(
        _FakePg(),
        _FakeLifecycle(watcher_ok=True, web_ok=True),
        _FakeCfg(),
        runtime_metrics=_FakeRuntimeMetrics(),
        worker_pool=_FakeWorkerPool(3),
    )
    server, thread = _start_server(handler_cls)
    try:
        payload = _get_json(f"http://127.0.0.1:{server.server_port}/api/dashboard")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert "runtime" in payload
    assert payload["runtime"]["events_received_total"] == 5
    assert payload["runtime"]["queue_depth"] == 3

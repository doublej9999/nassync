import logging
import threading
import time
from collections import OrderedDict
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

logger = logging.getLogger("watcher")

_DEFAULT_DEDUP_MAX_ENTRIES = 5000


class Handler(FileSystemEventHandler):
    def __init__(self, worker_pool, dedup_window_sec=1.0, dedup_max_entries=_DEFAULT_DEDUP_MAX_ENTRIES):
        self.worker_pool = worker_pool
        self._dedup_window_sec = max(0.0, float(dedup_window_sec))
        self._dedup_max_entries = max(100, int(dedup_max_entries))
        self._event_lock = threading.Lock()
        self._last_event_ts: OrderedDict[str, float] = OrderedDict()
        self._active = threading.Event()
        self._active.set()

    def is_active(self):
        return self._active.is_set()

    def disable(self):
        self._active.clear()

    def _handle_event(self, path: Path):
        if not self.is_active():
            logger.info("监听器已停用，忽略事件：%s", path)
            return
        key = str(path)
        ts = time.monotonic()

        with self._event_lock:
            if self._dedup_window_sec > 0:
                last_ts = self._last_event_ts.get(key)
                if last_ts is not None and (ts - last_ts) < self._dedup_window_sec:
                    logger.debug("事件去抖跳过：%s", path)
                    return
            # Move to end (most recent) and update timestamp
            self._last_event_ts[key] = ts
            self._last_event_ts.move_to_end(key)

            # Evict oldest entries when exceeding limit
            while len(self._last_event_ts) > self._dedup_max_entries:
                self._last_event_ts.popitem(last=False)

        self.worker_pool.enqueue(path)

    def on_created(self, e):
        if not e.is_directory:
            self._handle_event(Path(e.src_path))

    def on_modified(self, e):
        if not e.is_directory:
            self._handle_event(Path(e.src_path))

    def on_moved(self, e):
        if not e.is_directory:
            self._handle_event(Path(e.dest_path))


class ServiceLifecycle:
    def __init__(self, handler: Handler, observer: Observer = None):
        self.handler = handler
        self._observer_lock = threading.Lock()
        self.observer = None
        if observer is not None:
            self.set_observer(observer)
        self._web_running = threading.Event()
        self._shutdown_requested = threading.Event()
        self._reload_requested = threading.Event()

    def mark_web_started(self):
        self._web_running.set()

    def mark_web_stopped(self):
        self._web_running.clear()

    def request_shutdown(self):
        if not self._shutdown_requested.is_set():
            self._shutdown_requested.set()
            self.handler.disable()

    def request_reload(self):
        self._reload_requested.set()

    def consume_reload_request(self):
        if self._reload_requested.is_set():
            self._reload_requested.clear()
            return True
        return False

    def set_observer(self, observer: Observer):
        with self._observer_lock:
            self.observer = observer

    def get_observer(self):
        with self._observer_lock:
            return self.observer

    def watcher_healthy(self):
        observer = self.get_observer()
        return self.handler.is_active() and observer is not None and observer.is_alive()

    def web_healthy(self):
        return self._web_running.is_set()

    def is_shutting_down(self):
        return self._shutdown_requested.is_set()

import threading
import time


class RuntimeMetrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._queue_depth_provider = None

        self._counters = {
            "events_received_total": 0,
            "events_dedup_skipped_total": 0,
            "queue_enqueued_total": 0,
            "queue_full_drop_total": 0,
            "tasks_started_total": 0,
            "tasks_success_total": 0,
            "tasks_failed_total": 0,
            "tasks_retry_scheduled_total": 0,
        }
        self._duration_total_sec = 0.0
        self._duration_count = 0
        self._duration_max_sec = 0.0

        self._timestamps = {
            "last_event_at": None,
            "last_task_started_at": None,
            "last_task_success_at": None,
            "last_task_failed_at": None,
        }

    def set_queue_depth_provider(self, provider):
        self._queue_depth_provider = provider

    def _inc(self, key: str, delta: int = 1):
        with self._lock:
            self._counters[key] += delta

    def on_event_received(self):
        with self._lock:
            self._counters["events_received_total"] += 1
            self._timestamps["last_event_at"] = time.time()

    def on_event_dedup_skipped(self):
        self._inc("events_dedup_skipped_total")

    def on_queue_enqueued(self):
        self._inc("queue_enqueued_total")

    def on_queue_full_drop(self):
        self._inc("queue_full_drop_total")

    def on_task_started(self):
        with self._lock:
            self._counters["tasks_started_total"] += 1
            self._timestamps["last_task_started_at"] = time.time()

    def on_task_success(self, duration_sec: float):
        duration = max(0.0, float(duration_sec or 0.0))
        with self._lock:
            self._counters["tasks_success_total"] += 1
            self._timestamps["last_task_success_at"] = time.time()
            self._duration_total_sec += duration
            self._duration_count += 1
            if duration > self._duration_max_sec:
                self._duration_max_sec = duration

    def on_task_failed(self, duration_sec: float):
        duration = max(0.0, float(duration_sec or 0.0))
        with self._lock:
            self._counters["tasks_failed_total"] += 1
            self._timestamps["last_task_failed_at"] = time.time()
            self._duration_total_sec += duration
            self._duration_count += 1
            if duration > self._duration_max_sec:
                self._duration_max_sec = duration

    def on_task_retry_scheduled(self):
        self._inc("tasks_retry_scheduled_total")

    def snapshot(self):
        with self._lock:
            counters = dict(self._counters)
            timestamps = dict(self._timestamps)
            duration_count = self._duration_count
            duration_total = self._duration_total_sec
            duration_max = self._duration_max_sec

        queue_depth = None
        if self._queue_depth_provider is not None:
            try:
                queue_depth = int(self._queue_depth_provider())
            except Exception:
                queue_depth = None

        avg_duration = (duration_total / duration_count) if duration_count else 0.0
        return {
            **counters,
            **timestamps,
            "task_duration_avg_sec": round(avg_duration, 4),
            "task_duration_max_sec": round(duration_max, 4),
            "queue_depth": queue_depth,
        }

import heapq
import logging
import os
import queue
import threading
import time
from pathlib import Path

logger = logging.getLogger("watcher")

DEFAULT_WORKER_COUNT = max(2, os.cpu_count() or 2)


class TaskWorkerPool:
    def __init__(
        self,
        processor,
        worker_count=DEFAULT_WORKER_COUNT,
        max_queue_size=None,
        runtime_metrics=None,
        pg=None,
        persistent_queue=False,
        worker_id_prefix="worker",
    ):
        self.processor = processor
        self.worker_count = worker_count
        self.pg = pg
        self.persistent_queue = bool(persistent_queue and pg is not None)
        self.worker_id_prefix = str(worker_id_prefix or "worker")
        queue_size = max_queue_size or processor.cfg.TASK_QUEUE_MAX_SIZE
        self.queue = queue.Queue(maxsize=max(1, int(queue_size)))
        self.runtime_metrics = runtime_metrics or getattr(processor, "runtime_metrics", None)
        self._stop_event = threading.Event()
        self._queued_paths = set()
        self._queue_lock = threading.Lock()
        self._workers = []

        self._retry_heap = []
        self._retry_seq = 0
        self._retry_lock = threading.Lock()
        self._retry_event = threading.Event()

        self._retry_thread = None
        if not self.persistent_queue:
            self._retry_thread = threading.Thread(
                target=self._run_retry_scheduler,
                name="retry-scheduler",
                daemon=True,
            )
            self._retry_thread.start()
        if self.runtime_metrics is not None:
            self.runtime_metrics.set_queue_depth_provider(self.queue.qsize)

        for idx in range(worker_count):
            worker = threading.Thread(
                target=self._run_worker,
                name=f"processor-worker-{idx}",
                daemon=True,
            )
            worker.start()
            self._workers.append(worker)

    def enqueue(self, path):
        if self._stop_event.is_set():
            logger.info("任务队列已停止，忽略新文件：%s", path)
            return
        target = path if isinstance(path, Path) else Path(path)
        key = str(target)

        if self.persistent_queue:
            try:
                self.pg.enqueue_task(target)
            except Exception:
                logger.exception("持久化入队失败：%s", target)

        with self._queue_lock:
            if key in self._queued_paths:
                logger.debug("事件去重（队列中已存在）：%s", target)
                return
            self._queued_paths.add(key)

        try:
            self.queue.put_nowait(target)
            if self.runtime_metrics is not None:
                self.runtime_metrics.on_queue_enqueued()
        except queue.Full:
            with self._queue_lock:
                self._queued_paths.discard(key)
            if self.persistent_queue:
                logger.warning("内存队列已满，任务已落库等待拉取：%s", target)
            else:
                logger.warning("任务队列已满，忽略文件：%s", target)
                if self.runtime_metrics is not None:
                    self.runtime_metrics.on_queue_full_drop()

    def _run_worker(self):
        worker_name = threading.current_thread().name
        while True:
            try:
                path = self.queue.get(timeout=1)
            except queue.Empty:
                if self._stop_event.is_set():
                    break
                if self.persistent_queue:
                    claimed = self._claim_one(worker_name)
                    if claimed is not None:
                        self._process_path(claimed, from_queue=False)
                continue

            if path is None:
                self.queue.task_done()
                break

            self._process_path(path, from_queue=True)

    def _process_path(self, path: Path, from_queue: bool):
        try:
            retry_delay = self.processor.process(path)
            if retry_delay is not None and not self.persistent_queue:
                self._schedule_retry(path, retry_delay)
                if self.runtime_metrics is not None:
                    self.runtime_metrics.on_task_retry_scheduled()
        except Exception:
            logger.exception("Worker 处理时遇到未捕获异常：%s", path)
        finally:
            if from_queue:
                with self._queue_lock:
                    self._queued_paths.discard(str(path))
                self.queue.task_done()

    def _claim_one(self, worker_name: str):
        if not self.persistent_queue:
            return None
        try:
            items = self.pg.claim_due_tasks(
                worker_id=f"{self.worker_id_prefix}:{worker_name}",
                batch_size=self.processor.cfg.TASK_FETCH_BATCH_SIZE,
                lock_sec=self.processor.cfg.TASK_LOCK_SEC,
            )
        except Exception:
            logger.exception("持久化队列拉取失败")
            return None
        if not items:
            return None
        return items[0]

    def _schedule_retry(self, path: Path, delay_sec):
        try:
            delay = float(delay_sec)
        except (TypeError, ValueError):
            delay = 0.0
        delay = max(0.1, delay)
        due = time.monotonic() + delay

        with self._retry_lock:
            self._retry_seq += 1
            heapq.heappush(self._retry_heap, (due, self._retry_seq, Path(path), delay))
        self._retry_event.set()

    def _run_retry_scheduler(self):
        while not self._stop_event.is_set():
            item = None
            wait_timeout = None

            with self._retry_lock:
                if self._retry_heap:
                    due, seq, path, delay = self._retry_heap[0]
                    now = time.monotonic()
                    if due <= now:
                        item = heapq.heappop(self._retry_heap)
                    else:
                        wait_timeout = max(0.05, due - now)

            if item is not None:
                _, _, path, delay = item
                if self._stop_event.is_set():
                    return
                logger.info("任务重入队列：%s（延迟 %.2fs）", path, delay)
                self.enqueue(path)
                continue

            self._retry_event.wait(timeout=wait_timeout)
            self._retry_event.clear()

    def shutdown(self, wait=True):
        self._stop_event.set()
        self._retry_event.set()
        if wait:
            self.queue.join()
        for _ in self._workers:
            while True:
                try:
                    self.queue.put_nowait(None)
                    break
                except queue.Full:
                    time.sleep(0.05)
        for worker in self._workers:
            worker.join(timeout=5)
        if self._retry_thread is not None and self._retry_thread.is_alive():
            self._retry_thread.join(timeout=5)

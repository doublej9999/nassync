import heapq
import logging
import os
import queue
import threading
import time
from contextlib import contextmanager
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
        can_process=None,
    ):
        self.processor = processor
        self.worker_count = worker_count
        self.pg = pg
        self.persistent_queue = bool(persistent_queue and pg is not None)
        self.worker_id_prefix = str(worker_id_prefix or "worker")
        self._can_process = can_process if callable(can_process) else (lambda: True)
        claim_batch_size = getattr(processor.cfg, "TASK_FETCH_BATCH_SIZE", 20)
        self._claim_batch_size = max(1, int(claim_batch_size))
        self._memory_burst_before_claim = self._claim_batch_size
        self._lock_renew_interval = max(
            1, int(getattr(processor.cfg, "TASK_LOCK_RENEW_INTERVAL_SEC", 30))
        )
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
        worker_id = f"{self.worker_id_prefix}:{worker_name}"
        memory_processed_since_claim = 0
        while True:
            if (
                self.persistent_queue
                and memory_processed_since_claim >= self._memory_burst_before_claim
                and self._can_process()
            ):
                claimed = self._claim_due_batch(worker_id)
                if claimed:
                    for claimed_path in claimed:
                        self._process_path(claimed_path, from_queue=False, worker_id=worker_id)
                memory_processed_since_claim = 0

            try:
                path = self.queue.get(timeout=0.5 if self.persistent_queue else 1)
            except queue.Empty:
                if self._stop_event.is_set():
                    break
                if self.persistent_queue and self._can_process():
                    claimed = self._claim_due_batch(worker_id)
                    if claimed:
                        for claimed_path in claimed:
                            self._process_path(claimed_path, from_queue=False, worker_id=worker_id)
                        memory_processed_since_claim = 0
                continue

            if path is None:
                self.queue.task_done()
                break

            if not self._can_process():
                self._defer_when_not_owner(path)
                with self._queue_lock:
                    self._queued_paths.discard(str(path))
                self.queue.task_done()
                continue

            self._process_path(path, from_queue=True, worker_id=worker_id)
            memory_processed_since_claim += 1

    @contextmanager
    def _lock_heartbeat(self, path: Path, worker_id: str):
        if not self.persistent_queue or self.pg is None:
            yield
            return

        stop_event = threading.Event()

        def run():
            while not stop_event.wait(timeout=self._lock_renew_interval):
                try:
                    ok = self.pg.renew_task_lock(
                        path,
                        worker_id=worker_id,
                        lock_sec=self.processor.cfg.TASK_LOCK_SEC,
                    )
                    if not ok:
                        logger.warning("任务锁续期失败（记录不存在或不再归属当前 worker）：%s", path)
                        return
                except Exception:
                    logger.exception("任务锁续期异常：%s", path)
                    return

        thread = threading.Thread(
            target=run,
            name=f"task-lock-heartbeat-{threading.current_thread().name}",
            daemon=True,
        )
        thread.start()
        try:
            yield
        finally:
            stop_event.set()
            thread.join(timeout=2)

    def _process_path(self, path: Path, from_queue: bool, worker_id: str):
        if not self._can_process():
            if self.persistent_queue and self.pg is not None:
                try:
                    self.pg.release_task_claim(path, delay_sec=1.0, reason="当前实例非 leader，任务回退")
                except Exception:
                    logger.exception("回退任务失败：%s", path)
            return
        try:
            with self._lock_heartbeat(path, worker_id):
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

    def _claim_due_batch(self, worker_id: str):
        if not self.persistent_queue:
            return []
        try:
            items = self.pg.claim_due_tasks(
                worker_id=worker_id,
                batch_size=self._claim_batch_size,
                lock_sec=self.processor.cfg.TASK_LOCK_SEC,
            )
        except Exception:
            logger.exception("持久化队列拉取失败")
            return []
        if not items:
            return []
        return items

    def _defer_when_not_owner(self, path: Path):
        if self.persistent_queue and self.pg is not None:
            try:
                self.pg.enqueue_task(path)
                return
            except Exception:
                logger.exception("回退任务入库失败：%s", path)
            logger.warning("持久化任务回退失败，当前任务将等待下次事件触发：%s", path)
            return
        self._schedule_retry(path, 1.0)

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

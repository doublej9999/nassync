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
    ):
        self.processor = processor
        self.worker_count = worker_count
        queue_size = max_queue_size or processor.cfg.TASK_QUEUE_MAX_SIZE
        self.queue = queue.Queue(maxsize=max(1, int(queue_size)))
        self._stop_event = threading.Event()
        self._queued_paths = set()
        self._queue_lock = threading.Lock()
        self._workers = []
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

        with self._queue_lock:
            if key in self._queued_paths:
                logger.debug("事件去重（队列中已存在）：%s", target)
                return
            self._queued_paths.add(key)

        try:
            self.queue.put_nowait(target)
        except queue.Full:
            with self._queue_lock:
                self._queued_paths.discard(key)
            logger.warning("任务队列已满，忽略文件：%s", target)

    def _run_worker(self):
        while True:
            try:
                path = self.queue.get(timeout=1)
            except queue.Empty:
                if self._stop_event.is_set():
                    break
                continue

            if path is None:
                self.queue.task_done()
                break

            try:
                self.processor.process(path)
            except Exception:
                logger.exception("Worker 处理时遇到未捕获异常：%s", path)
            finally:
                with self._queue_lock:
                    self._queued_paths.discard(str(path))
                self.queue.task_done()

    def shutdown(self, wait=True):
        self._stop_event.set()
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

import json
import logging
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path

from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from .config import is_nas_path, load_config, validate_config
from .dashboard import create_dashboard_handler
from .db import PgClient
from .errors import summarize_error_msg
from .logging_utils import setup_logging
from .processor import Processor
from .watcher import Handler, ServiceLifecycle
from .workers import TaskWorkerPool

logger = logging.getLogger("watcher")


class InitialScanState:
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self._lock = threading.Lock()
        self._state = self._load()

    def _load(self):
        if not self.state_file.exists():
            return {}
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        return {}

    def get_last_scan_ts(self, watch_dir: Path) -> float:
        key = str(watch_dir).lower()
        raw = self._state.get(key)
        try:
            return float(raw or 0)
        except Exception:
            return 0.0

    def update_last_scan_ts(self, watch_dir: Path, ts: float):
        key = str(watch_dir).lower()
        with self._lock:
            self._state[key] = float(ts)
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp_file = self.state_file.with_suffix(".tmp")
            tmp_file.write_text(
                json.dumps(self._state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp_file.replace(self.state_file)


def _create_observer(watch_dirs):
    if isinstance(watch_dirs, (str, Path)):
        dirs = [Path(watch_dirs)]
    else:
        dirs = [Path(item) for item in watch_dirs]

    if any(is_nas_path(item) for item in dirs):
        logger.info("检测到 NAS 目录，监听器切换为轮询模式")
        return PollingObserver(timeout=1.0)
    return Observer()


def main():
    cfg = load_config()
    validate_config(cfg)
    setup_logging(cfg)

    scan_state = InitialScanState(cfg.LOG_DIR / "initial_scan_state.json")

    pg = PgClient(cfg)
    try:
        pg.ensure_default_map_path_config()
    except Exception as ex:
        logger.warning("初始化 map_path_config 默认配置失败：%s", summarize_error_msg(ex))

    processor = Processor(cfg, pg)
    worker_pool = TaskWorkerPool(processor, max_queue_size=cfg.TASK_QUEUE_MAX_SIZE)

    handler = Handler(worker_pool, dedup_window_sec=cfg.EVENT_DEDUP_WINDOW_SEC)
    lifecycle = ServiceLifecycle(handler)
    watch_retry_interval = max(1.0, float(cfg.PROCESS_RETRY_INTERVAL_SEC))
    last_watch_attempt_at = 0.0

    scanned_watch_dirs = set()
    current_watch_key = tuple()

    def load_watch_dirs():
        watch_dirs = []
        try:
            watch_dirs = [Path(item) for item in pg.get_active_watch_dirs() if item]
        except Exception as ex:
            logger.warning("读取 map_path_config 失败，回退到 config WATCH_DIR：%s", summarize_error_msg(ex))

        if not watch_dirs:
            watch_dirs = [cfg.WATCH_DIR]

        unique_dirs = []
        seen = set()
        for item in watch_dirs:
            key = str(item).lower()
            if key in seen:
                continue
            seen.add(key)
            unique_dirs.append(item)
        return unique_dirs

    def run_initial_scan_for_dir(watch_dir: Path):
        key = str(watch_dir).lower()
        if key in scanned_watch_dirs:
            return
        if not cfg.INITIAL_SCAN:
            return

        last_scan_ts = scan_state.get_last_scan_ts(watch_dir)
        scheduled_count = 0
        newest_mtime = last_scan_ts

        for f in watch_dir.rglob("*.zip"):
            if not f.is_file():
                continue
            try:
                mtime = float(f.stat().st_mtime)
            except Exception:
                continue
            newest_mtime = max(newest_mtime, mtime)
            if mtime <= last_scan_ts:
                continue
            handler._handle_event(f)
            scheduled_count += 1

        scan_state.update_last_scan_ts(watch_dir, max(time.time(), newest_mtime))
        scanned_watch_dirs.add(key)
        logger.info(
            "启动增量扫描完成：%s（上次=%.3f，本次入队=%s）",
            watch_dir,
            last_scan_ts,
            scheduled_count,
        )

    def ensure_observer_running(force=False):
        nonlocal last_watch_attempt_at, current_watch_key

        observer = lifecycle.get_observer()
        reload_requested = lifecycle.consume_reload_request()
        watch_dirs = load_watch_dirs()

        usable_watch_dirs = []
        unavailable_watch_dirs = []
        for watch_dir in watch_dirs:
            if watch_dir.exists() and watch_dir.is_dir():
                usable_watch_dirs.append(watch_dir)
            else:
                unavailable_watch_dirs.append(watch_dir)

        watch_key = tuple(sorted(str(x).lower() for x in usable_watch_dirs))

        if (
            observer is not None
            and observer.is_alive()
            and not force
            and not reload_requested
            and watch_key == current_watch_key
        ):
            return

        now = time.monotonic()
        if not force and (now - last_watch_attempt_at) < watch_retry_interval:
            return
        last_watch_attempt_at = now

        for watch_dir in unavailable_watch_dirs:
            logger.warning("监听目录不可用：%s（将继续重试）", watch_dir)

        if not usable_watch_dirs:
            return

        try:
            if observer is not None:
                try:
                    observer.stop()
                    observer.join(timeout=1)
                except Exception:
                    pass

            new_observer = _create_observer(usable_watch_dirs)
            for watch_dir in usable_watch_dirs:
                new_observer.schedule(handler, str(watch_dir), recursive=True)

            new_observer.start()
            lifecycle.set_observer(new_observer)
            current_watch_key = tuple(sorted(str(x).lower() for x in usable_watch_dirs))
            logger.info("启动监听目录：%s", ", ".join(str(x) for x in usable_watch_dirs))

            for watch_dir in usable_watch_dirs:
                run_initial_scan_for_dir(watch_dir)
        except Exception as ex:
            logger.warning(
                "监听启动失败，%.1fs 后重试，err=%s",
                watch_retry_interval,
                summarize_error_msg(ex),
            )

    ensure_observer_running(force=True)

    web_server = ThreadingHTTPServer(
        (cfg.WEB_HOST, cfg.WEB_PORT),
        create_dashboard_handler(pg, lifecycle, cfg),
    )
    lifecycle.mark_web_started()
    web_thread = threading.Thread(target=web_server.serve_forever, daemon=True)
    web_thread.start()
    logger.info("Web 监控已启动：http://%s:%s/dashboard", cfg.WEB_HOST, cfg.WEB_PORT)

    def shutdown(reason: str):
        if lifecycle.is_shutting_down():
            return
        lifecycle.request_shutdown()
        logger.info("开始优雅关闭服务：%s", reason)

        observer = lifecycle.get_observer()
        try:
            if observer is not None and observer.is_alive():
                observer.stop()
        except Exception as ex:
            logger.exception("停止监听失败：%s", ex)

        try:
            if lifecycle.web_healthy():
                web_server.shutdown()
            web_server.server_close()
        except Exception as ex:
            logger.exception("关闭 Web 服务失败：%s", ex)
        finally:
            lifecycle.mark_web_stopped()

        if web_thread.is_alive():
            web_thread.join(timeout=5)
        if observer is not None:
            observer.join(timeout=5)

        worker_pool.shutdown()

        try:
            pg.close()
        except Exception as ex:
            logger.exception("关闭数据库连接失败：%s", ex)

        logger.info("所有服务已停止")

    stop_reason = "正常退出"
    try:
        while True:
            time.sleep(1)
            ensure_observer_running()
    except KeyboardInterrupt:
        stop_reason = "收到键盘中断"
        logger.info("收到退出信号")
    except Exception as ex:
        stop_reason = f"运行异常：{type(ex).__name__}"
        logger.exception("主循环异常：%s", ex)
    finally:
        shutdown(stop_reason)

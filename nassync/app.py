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
from .observability import RuntimeMetrics
from .processor import Processor
from .watcher import Handler, ServiceLifecycle
from .workers import TaskWorkerPool

logger = logging.getLogger("watcher")


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

    pg = PgClient(cfg)
    runtime_metrics = RuntimeMetrics()
    try:
        pg.ensure_default_map_path_config()
    except Exception as ex:
        logger.warning("初始化 map_path_config 默认配置失败：%s", summarize_error_msg(ex))

    processor = Processor(cfg, pg, runtime_metrics=runtime_metrics)
    lifecycle_ref = {"obj": None}

    def can_process():
        obj = lifecycle_ref.get("obj")
        return bool(obj and obj.is_leader())

    worker_pool = TaskWorkerPool(
        processor,
        max_queue_size=cfg.TASK_QUEUE_MAX_SIZE,
        runtime_metrics=runtime_metrics,
        pg=pg,
        persistent_queue=cfg.USE_PERSISTENT_QUEUE,
        worker_id_prefix=cfg.INSTANCE_ID,
        can_process=can_process,
    )

    handler = Handler(
        worker_pool,
        dedup_window_sec=cfg.EVENT_DEDUP_WINDOW_SEC,
        runtime_metrics=runtime_metrics,
    )
    lifecycle = ServiceLifecycle(handler)
    lifecycle_ref["obj"] = lifecycle
    lifecycle.set_role("standby")
    watch_retry_interval = max(1.0, float(cfg.PROCESS_RETRY_INTERVAL_SEC))
    last_watch_attempt_at = 0.0
    lease_renew_interval = max(1, int(cfg.LEASE_RENEW_INTERVAL_SEC))
    last_lease_check_at = 0.0

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

        try:
            last_scan_ts = pg.get_map_path_last_scan(watch_dir)
        except Exception as ex:
            logger.warning("读取 last_scan 失败，回退为 0：%s, err=%s", watch_dir, summarize_error_msg(ex))
            last_scan_ts = 0.0

        scheduled_count = 0
        for f in watch_dir.rglob("*"):
            if not f.is_file():
                continue
            if not processor.is_valid(f):
                continue
            try:
                mtime = float(f.stat().st_mtime)
            except Exception:
                continue
            if mtime <= last_scan_ts:
                continue
            worker_pool.enqueue(f)
            scheduled_count += 1

        scanned_watch_dirs.add(key)
        logger.info(
            "启动增量扫描完成：%s（上次=%.3f，本次入队=%s）",
            watch_dir,
            last_scan_ts,
            scheduled_count,
        )

    def ensure_observer_running(force=False):
        nonlocal last_watch_attempt_at, current_watch_key

        if not lifecycle.is_leader():
            observer = lifecycle.get_observer()
            if observer is not None and observer.is_alive():
                try:
                    observer.stop()
                    observer.join(timeout=1)
                except Exception:
                    pass
                lifecycle.set_observer(None)
                current_watch_key = tuple()
                logger.info("当前为 standby，已停止目录监听")
            return

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

    def refresh_leader_role(force=False):
        nonlocal last_lease_check_at
        now = time.monotonic()
        if not force and (now - last_lease_check_at) < lease_renew_interval:
            return
        last_lease_check_at = now

        try:
            is_leader, lease_token = pg.try_acquire_or_renew_lease(
                cfg.SERVICE_NAME,
                cfg.INSTANCE_ID,
                cfg.LEASE_DURATION_SEC,
                return_token=True,
            )
        except Exception as ex:
            logger.warning("续约失败，保持 standby：%s", summarize_error_msg(ex))
            is_leader = False
            lease_token = None

        prev = lifecycle.get_role()
        prev_token = lifecycle.get_lease_token() if hasattr(lifecycle, "get_lease_token") else None
        lifecycle.set_role("leader" if is_leader else "standby")
        if is_leader:
            pg.set_fencing_context(cfg.SERVICE_NAME, cfg.INSTANCE_ID, lease_token)
            lifecycle.set_lease_token(lease_token)
        else:
            if prev_token is not None:
                pg.set_fencing_context(cfg.SERVICE_NAME, cfg.INSTANCE_ID, prev_token)
            else:
                pg.clear_fencing_context()
        cur = lifecycle.get_role()
        if cur != prev:
            logger.info("实例角色切换：%s -> %s（instance=%s）", prev, cur, cfg.INSTANCE_ID)

    refresh_leader_role(force=True)
    ensure_observer_running(force=True)

    web_server = ThreadingHTTPServer(
        (cfg.WEB_HOST, cfg.WEB_PORT),
        create_dashboard_handler(
            pg,
            lifecycle,
            cfg,
            runtime_metrics=runtime_metrics,
            worker_pool=worker_pool,
        ),
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
            refresh_leader_role()
            ensure_observer_running()
    except KeyboardInterrupt:
        stop_reason = "收到键盘中断"
        logger.info("收到退出信号")
    except Exception as ex:
        stop_reason = f"运行异常：{type(ex).__name__}"
        logger.exception("主循环异常：%s", ex)
    finally:
        shutdown(stop_reason)

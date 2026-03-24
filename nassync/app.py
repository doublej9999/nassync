import logging
import threading
import time
from http.server import ThreadingHTTPServer

from watchdog.observers import Observer

from .config import load_config, validate_config
from .dashboard import create_dashboard_handler
from .db import PgClient
from .errors import summarize_error_msg
from .logging_utils import setup_logging
from .processor import Processor
from .watcher import Handler, ServiceLifecycle
from .workers import TaskWorkerPool

logger = logging.getLogger("watcher")


def main():
    cfg = load_config()
    validate_config(cfg)
    setup_logging(cfg)

    pg = PgClient(cfg)
    processor = Processor(cfg, pg)
    worker_pool = TaskWorkerPool(processor, max_queue_size=cfg.TASK_QUEUE_MAX_SIZE)

    handler = Handler(worker_pool, dedup_window_sec=cfg.EVENT_DEDUP_WINDOW_SEC)
    lifecycle = ServiceLifecycle(handler)
    watch_retry_interval = max(1.0, float(cfg.PROCESS_RETRY_INTERVAL_SEC))
    last_watch_attempt_at = 0.0
    initial_scan_done = False

    def run_initial_scan_once():
        nonlocal initial_scan_done
        if initial_scan_done or not cfg.INITIAL_SCAN:
            return
        for f in cfg.WATCH_DIR.rglob("*"):
            if f.is_file() and f.suffix.lower() == ".zip":
                handler._handle_event(f)
        initial_scan_done = True
        logger.info("启动扫描完成")

    def ensure_observer_running(force=False):
        nonlocal last_watch_attempt_at

        observer = lifecycle.get_observer()
        if observer is not None and observer.is_alive():
            return

        now = time.monotonic()
        if not force and (now - last_watch_attempt_at) < watch_retry_interval:
            return
        last_watch_attempt_at = now

        try:
            if not cfg.WATCH_DIR.exists() or not cfg.WATCH_DIR.is_dir():
                logger.warning(
                    "监听目录不可用，%.1fs 后重试：%s",
                    watch_retry_interval,
                    cfg.WATCH_DIR,
                )
                return

            if observer is not None:
                try:
                    observer.stop()
                    observer.join(timeout=1)
                except Exception:
                    pass

            new_observer = Observer()
            new_observer.schedule(handler, str(cfg.WATCH_DIR), recursive=True)
            new_observer.start()
            lifecycle.set_observer(new_observer)
            logger.info("启动监听：%s", cfg.WATCH_DIR)
            run_initial_scan_once()
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

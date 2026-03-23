import logging
import threading
import time
from http.server import ThreadingHTTPServer

from watchdog.observers import Observer

from .config import load_config, validate_config
from .dashboard import create_dashboard_handler
from .db import PgClient
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
    observer = Observer()
    lifecycle = ServiceLifecycle(handler, observer)

    if cfg.INITIAL_SCAN:
        for f in cfg.WATCH_DIR.rglob("*"):
            if f.is_file() and f.suffix.lower() == ".zip":
                handler._handle_event(f)

    observer.schedule(handler, str(cfg.WATCH_DIR), recursive=True)
    observer.start()
    logger.info("启动监听...")

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
        try:
            if observer.is_alive():
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
    except KeyboardInterrupt:
        stop_reason = "收到键盘中断"
        logger.info("收到退出信号")
    except Exception as ex:
        stop_reason = f"运行异常：{type(ex).__name__}"
        logger.exception("主循环异常：%s", ex)
    finally:
        shutdown(stop_reason)

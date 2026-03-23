"""兼容入口：保留原有 `main` 模块导入方式。"""

from nassync.app import main
from nassync.config import Config, load_config, validate_config
from nassync.dashboard import DASHBOARD_HTML, create_dashboard_handler
from nassync.db import PgClient
from nassync.errors import RetryableProcessError, summarize_error_msg
from nassync.logging_utils import setup_logging
from nassync.processor import Processor
from nassync.watcher import Handler, ServiceLifecycle
from nassync.workers import DEFAULT_WORKER_COUNT, TaskWorkerPool

__all__ = [
    "Config",
    "PgClient",
    "Processor",
    "TaskWorkerPool",
    "Handler",
    "ServiceLifecycle",
    "RetryableProcessError",
    "summarize_error_msg",
    "setup_logging",
    "load_config",
    "validate_config",
    "create_dashboard_handler",
    "DASHBOARD_HTML",
    "DEFAULT_WORKER_COUNT",
    "main",
]


if __name__ == "__main__":
    main()

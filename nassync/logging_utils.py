import logging
import logging.handlers

from .config import Config


def setup_logging(cfg: Config) -> logging.Logger:
    logger = logging.getLogger("watcher")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    try:
        cfg.LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = cfg.LOG_DIR / "watcher.log"
        fh = logging.handlers.TimedRotatingFileHandler(
            log_file, when="midnight", backupCount=30, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception as ex:
        logger.warning("日志目录不可用，已回退到控制台日志：%s", ex)

    return logger

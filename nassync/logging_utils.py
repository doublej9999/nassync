import logging
import logging.handlers

from .config import Config


def setup_logging(cfg: Config) -> logging.Logger:
    cfg.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = cfg.LOG_DIR / "watcher.log"

    logger = logging.getLogger("watcher")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.handlers.TimedRotatingFileHandler(
        log_file, when="midnight", backupCount=30, encoding="utf-8"
    )
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

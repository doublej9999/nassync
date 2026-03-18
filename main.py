import logging
import logging.handlers
import os
import shutil
import tempfile
import threading
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path

from psycopg2 import pool
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


# =========================
# 配置
# =========================
@dataclass(frozen=True)
class Config:
    WATCH_DIR: Path = Path(r"C:\Users\Administrator\Desktop\nassync\A")
    TARGET_DIR: Path = Path(r"C:\Users\Administrator\Desktop\nassync\B")

    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "postgres"
    DB_SCHEMA: str = "public"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "123456"
    DB_TABLE: str = "zip_record"
    DB_TASK_TABLE: str = "zip_task_status"

    LOG_DIR: Path = Path(r".\logs")

    FILE_STABLE_CHECK_TIMES: int = 3
    FILE_STABLE_CHECK_INTERVAL_SEC: float = 2.0

    PROCESS_RETRY_TIMES: int = 3
    PROCESS_RETRY_INTERVAL_SEC: float = 3.0

    INITIAL_SCAN: bool = True


CONFIG = Config()


# =========================
# 日志
# =========================
def setup_logging():
    CONFIG.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = CONFIG.LOG_DIR / "watcher.log"

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


logger = setup_logging()


# =========================
# PG
# =========================
class PgClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.pool = pool.SimpleConnectionPool(
            1,
            5,
            host=cfg.DB_HOST,
            port=cfg.DB_PORT,
            dbname=cfg.DB_NAME,
            user=cfg.DB_USER,
            password=cfg.DB_PASSWORD,
            options=f"-c search_path={cfg.DB_SCHEMA}",
        )

    def insert_records(self, rec_type, lot_wafer_pairs, zip_name, zip_path):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                sql = f"""
                INSERT INTO {self.cfg.DB_TABLE}
                (type, lot_id, wafer_id, zip_name, zip_path)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (type, lot_id, wafer_id) DO NOTHING
                """
                data = [
                    (rec_type, lot_id, wafer_id, zip_name, zip_path)
                    for lot_id, wafer_id in lot_wafer_pairs
                ]
                cur.executemany(sql, data)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

    def upsert_task_status(self, rec_type, zip_name, zip_path, status, error_msg=None):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                sql = f"""
                INSERT INTO {self.cfg.DB_TASK_TABLE}
                (type, zip_name, zip_path, status, error_msg, updated_at)
                VALUES (%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (zip_path) DO UPDATE SET
                  type = EXCLUDED.type,
                  zip_name = EXCLUDED.zip_name,
                  status = EXCLUDED.status,
                  error_msg = EXCLUDED.error_msg,
                  updated_at = NOW()
                """
                cur.execute(sql, (rec_type, zip_name, zip_path, status, error_msg))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)


# =========================
# 核心处理
# =========================
class Processor:
    def __init__(self, cfg: Config, pg: PgClient):
        self.cfg = cfg
        self.pg = pg
        self.processing = set()
        self.lock = threading.Lock()

    def is_valid(self, path: Path):
        if path.suffix.lower() != ".zip":
            return False

        if "BACKUP" in [p.upper() for p in path.parts]:
            return False

        try:
            rel = path.relative_to(self.cfg.WATCH_DIR)
        except Exception:
            return False

        if len(rel.parts) < 3:
            return False

        if rel.parts[-2].upper() != "WAFER_MAP":
            return False

        return True

    def wait_stable(self, path: Path):
        last = None
        count = 0
        max_checks = max(
            self.cfg.FILE_STABLE_CHECK_TIMES * 5,
            self.cfg.FILE_STABLE_CHECK_TIMES + 1,
        )
        for _ in range(max_checks):
            if not path.exists():
                return False
            stat = (path.stat().st_size, path.stat().st_mtime)
            if stat == last:
                count += 1
                if count >= self.cfg.FILE_STABLE_CHECK_TIMES:
                    return True
            else:
                count = 0
                last = stat
            time.sleep(self.cfg.FILE_STABLE_CHECK_INTERVAL_SEC)
        return False

    def process(self, path: Path):
        if not self.is_valid(path):
            logger.info(f"跳过（不符合规则）：{path}")
            return

        rel = path.relative_to(self.cfg.WATCH_DIR)
        rec_type = rel.parts[-3]

        key = str(path)
        with self.lock:
            if key in self.processing:
                return
            self.processing.add(key)

        try:
            self.pg.upsert_task_status(rec_type, path.name, str(path), "PENDING")
            for i in range(self.cfg.PROCESS_RETRY_TIMES):
                done = self._process(path)
                if done:
                    self.pg.upsert_task_status(rec_type, path.name, str(path), "SUCCESS")
                    break
                if i < self.cfg.PROCESS_RETRY_TIMES - 1:
                    logger.info(
                        f"文件未稳定，准备重试：{path} ({i + 1}/{self.cfg.PROCESS_RETRY_TIMES})"
                    )
                    time.sleep(self.cfg.PROCESS_RETRY_INTERVAL_SEC)
            else:
                logger.warning(f"处理放弃（重试后仍未稳定）：{path}")
                self.pg.upsert_task_status(
                    rec_type,
                    path.name,
                    str(path),
                    "FAILED",
                    "文件重试后仍未稳定",
                )
        except Exception as ex:
            # 避免事件回调线程因未捕获异常中断，记录后继续监听
            logger.exception(f"处理失败：{path}, err={ex}")
            self.pg.upsert_task_status(
                rec_type, path.name, str(path), "FAILED", str(ex)[:1000]
            )
        finally:
            with self.lock:
                self.processing.remove(key)

    def _process(self, path: Path):
        # 文件已被其他并发事件处理并移走时，视为当前事件无需再处理
        if not path.exists():
            logger.info(f"跳过（文件不存在，可能已处理）：{path}")
            return True

        logger.info(f"处理：{path}")

        if not self.wait_stable(path):
            return False

        rel = path.relative_to(self.cfg.WATCH_DIR)

        rec_type = rel.parts[-3]
        unzip_dir = self.cfg.TARGET_DIR / rel.parent
        backup_dir = path.parent / "BACKUP"

        lot_wafer_pairs = self.extract(path, unzip_dir)

        self.pg.insert_records(rec_type, lot_wafer_pairs, path.name, str(path))

        backup_dir.mkdir(exist_ok=True)
        shutil.move(str(path), str(backup_dir / path.name))

        logger.info(f"完成：{path}")
        return True

    def extract(self, zip_path, target_dir):
        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(zip_path) as z:
                z.extractall(tmp)

            lot_wafer_pairs = []

            for root, _, files in os.walk(tmp):
                for f in files:
                    if Path(f).suffix.lower() != ".map":
                        continue

                    stem = Path(f).stem
                    parts = stem.split("-", 2)
                    if len(parts) < 2:
                        logger.warning(f"跳过（MAP 文件名不符合 lot-wafer 规则）：{f}")
                        continue

                    lot_id = parts[0].strip().upper()
                    wafer_id = parts[1].strip().upper()
                    if not lot_id or not wafer_id:
                        logger.warning(f"跳过（MAP 文件名 lot/wafer 为空）：{f}")
                        continue

                    lot_wafer_pairs.append((lot_id, wafer_id))

                    src = Path(root) / f
                    dst = target_dir / f
                    target_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)

            return sorted(set(lot_wafer_pairs))


# =========================
# 监听
# =========================
class Handler(FileSystemEventHandler):
    def __init__(self, p):
        self.p = p

    def on_created(self, e):
        if not e.is_directory:
            self.p.process(Path(e.src_path))

    def on_modified(self, e):
        if not e.is_directory:
            self.p.process(Path(e.src_path))

    def on_moved(self, e):
        if not e.is_directory:
            # 兼容“先写临时文件再重命名为 .zip”的上传方式
            self.p.process(Path(e.dest_path))


# =========================
# 主程序
# =========================
def main():
    pg = PgClient(CONFIG)
    p = Processor(CONFIG, pg)

    if CONFIG.INITIAL_SCAN:
        for f in CONFIG.WATCH_DIR.rglob("*"):
            if f.is_file() and f.suffix.lower() == ".zip":
                p.process(f)

    obs = Observer()
    obs.schedule(Handler(p), str(CONFIG.WATCH_DIR), recursive=True)
    obs.start()

    logger.info("启动监听...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        obs.stop()

    obs.join()


if __name__ == "__main__":
    main()

import json
import logging
import logging.handlers
import os
import queue
import errno
import re
import shutil
import sys
import threading
import time
import zipfile
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from psycopg2 import DatabaseError, InterfaceError, OperationalError, pool
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
    PROCESS_RETRY_BACKOFF_MAX_SEC: float = 30.0

    TASK_QUEUE_MAX_SIZE: int = 2000
    EVENT_DEDUP_WINDOW_SEC: float = 1.0
    DASHBOARD_CACHE_TTL_SEC: float = 2.0

    CHECK_ZIP_MAP_SAME_PREFIX: bool = True
    CHECK_MAP_FILENAME_FORMAT: bool = True

    INITIAL_SCAN: bool = True

    WEB_HOST: str = "0.0.0.0"
    WEB_PORT: int = 8080
    SYNC_TYPES: tuple[str, ...] = tuple()


def _app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _to_bool(val):
    if isinstance(val, bool):
        return val
    if val is None:
        return False
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


_SYNC_TYPES_RAW = None


def _normalize_sync_types(raw_types):
    if raw_types is None:
        return tuple()
    if not isinstance(raw_types, list):
        return tuple()
    normalized = []
    seen = set()
    for item in raw_types:
        if not isinstance(item, str):
            continue
        value = item.strip().upper()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return tuple(normalized)


def load_config() -> Config:
    default_cfg = Config()
    cfg_path = Path(os.getenv("NASSYNC_CONFIG", _app_base_dir() / "config.json"))
    raw = {}
    config_loaded = False

    if not cfg_path.exists():
        print(f"[配置] 未找到配置文件，使用默认配置: {cfg_path}")
    else:
        try:
            with cfg_path.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                raw = loaded
                config_loaded = True
            else:
                print("[配置] 配置文件格式错误（需为 JSON 对象），使用默认配置")
        except Exception as e:
            print(f"[配置] 读取配置失败，使用默认配置: {e}")

    global _SYNC_TYPES_RAW
    _SYNC_TYPES_RAW = raw.get("SYNC_TYPES") if raw else None

    merged = dict(default_cfg.__dict__)
    if raw:
        merged.update({k: v for k, v in raw.items() if k in merged})

    env_db_password = os.getenv("NASSYNC_DB_PASSWORD")
    if env_db_password is not None and env_db_password != "":
        merged["DB_PASSWORD"] = env_db_password
        print("[配置] 已从环境变量 NASSYNC_DB_PASSWORD 覆盖数据库密码")

    # 类型转换，确保外部 JSON 配置能正确映射
    merged["WATCH_DIR"] = Path(merged["WATCH_DIR"])
    merged["TARGET_DIR"] = Path(merged["TARGET_DIR"])
    merged["LOG_DIR"] = Path(merged["LOG_DIR"])
    merged["DB_PORT"] = int(merged["DB_PORT"])
    merged["FILE_STABLE_CHECK_TIMES"] = int(merged["FILE_STABLE_CHECK_TIMES"])
    merged["FILE_STABLE_CHECK_INTERVAL_SEC"] = float(
        merged["FILE_STABLE_CHECK_INTERVAL_SEC"]
    )
    merged["PROCESS_RETRY_TIMES"] = int(merged["PROCESS_RETRY_TIMES"])
    merged["PROCESS_RETRY_INTERVAL_SEC"] = float(merged["PROCESS_RETRY_INTERVAL_SEC"])
    merged["PROCESS_RETRY_BACKOFF_MAX_SEC"] = float(
        merged["PROCESS_RETRY_BACKOFF_MAX_SEC"]
    )
    merged["TASK_QUEUE_MAX_SIZE"] = int(merged["TASK_QUEUE_MAX_SIZE"])
    merged["EVENT_DEDUP_WINDOW_SEC"] = float(merged["EVENT_DEDUP_WINDOW_SEC"])
    merged["DASHBOARD_CACHE_TTL_SEC"] = float(merged["DASHBOARD_CACHE_TTL_SEC"])
    merged["CHECK_ZIP_MAP_SAME_PREFIX"] = _to_bool(merged["CHECK_ZIP_MAP_SAME_PREFIX"])
    merged["CHECK_MAP_FILENAME_FORMAT"] = _to_bool(merged["CHECK_MAP_FILENAME_FORMAT"])
    merged["INITIAL_SCAN"] = _to_bool(merged["INITIAL_SCAN"])
    merged["WEB_PORT"] = int(merged["WEB_PORT"])
    merged["SYNC_TYPES"] = _normalize_sync_types(_SYNC_TYPES_RAW)

    if config_loaded:
        print(f"[配置] 已加载配置文件: {cfg_path}")
    return Config(**merged)


def validate_config(cfg: Config):
    errors = []

    def report(message: str):
        errors.append(message)

    def check_dir(name: str, path: Path, must_exist: bool = True):
        if not isinstance(path, Path):
            report(f"{name} 不是有效路径: {path}")
            return
        if must_exist:
            if not path.exists():
                report(f"{name} 路径不存在: {path}")
                return
            if not path.is_dir():
                report(f"{name} 不是目录: {path}")
        else:
            if path.exists() and not path.is_dir():
                report(f"{name} 不是目录: {path}")

    check_dir("WATCH_DIR", cfg.WATCH_DIR)
    check_dir("TARGET_DIR", cfg.TARGET_DIR)
    check_dir("LOG_DIR", cfg.LOG_DIR, must_exist=False)

    if not (1 <= cfg.WEB_PORT <= 65535):
        report(f"WEB_PORT 必须在 1-65535 之间: {cfg.WEB_PORT}")

    if cfg.FILE_STABLE_CHECK_TIMES < 1:
        report("FILE_STABLE_CHECK_TIMES 必须大于 0")

    if cfg.FILE_STABLE_CHECK_INTERVAL_SEC <= 0:
        report("FILE_STABLE_CHECK_INTERVAL_SEC 必须大于 0")

    if cfg.PROCESS_RETRY_TIMES < 0:
        report("PROCESS_RETRY_TIMES 不能为负")

    if cfg.PROCESS_RETRY_INTERVAL_SEC <= 0:
        report("PROCESS_RETRY_INTERVAL_SEC 必须大于 0")

    if cfg.PROCESS_RETRY_BACKOFF_MAX_SEC <= 0:
        report("PROCESS_RETRY_BACKOFF_MAX_SEC 必须大于 0")

    if cfg.PROCESS_RETRY_BACKOFF_MAX_SEC < cfg.PROCESS_RETRY_INTERVAL_SEC:
        report("PROCESS_RETRY_BACKOFF_MAX_SEC 不能小于 PROCESS_RETRY_INTERVAL_SEC")

    if cfg.TASK_QUEUE_MAX_SIZE < 1:
        report("TASK_QUEUE_MAX_SIZE 必须大于 0")

    if cfg.EVENT_DEDUP_WINDOW_SEC < 0:
        report("EVENT_DEDUP_WINDOW_SEC 不能为负")

    if cfg.DASHBOARD_CACHE_TTL_SEC < 0:
        report("DASHBOARD_CACHE_TTL_SEC 不能为负")

    def check_non_empty(name: str, value: str):
        if not isinstance(value, str) or not value.strip():
            report(f"{name} 不能为空")

    for key in [
        "DB_HOST",
        "DB_NAME",
        "DB_USER",
        "DB_TABLE",
        "DB_TASK_TABLE",
        "DB_SCHEMA",
        "WEB_HOST",
    ]:
        check_non_empty(key, getattr(cfg, key))

    if _SYNC_TYPES_RAW is not None:
        if not isinstance(_SYNC_TYPES_RAW, list):
            report("SYNC_TYPES 必须是字符串数组（可为空）")
        else:
            for idx, raw_item in enumerate(_SYNC_TYPES_RAW):
                if not isinstance(raw_item, str):
                    report(f"SYNC_TYPES[{idx}] 必须是字符串")
                elif not raw_item.strip():
                    report(f"SYNC_TYPES[{idx}] 必须是非空字符串")

    if errors:
        print("[配置] 启动前校验失败:")
        for err in errors:
            print(f"  - {err}")
        raise SystemExit(1)


CONFIG = load_config()

DEFAULT_WORKER_COUNT = max(2, os.cpu_count() or 2)
validate_config(CONFIG)

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
# 错误信息提炼
# =========================
def summarize_error_msg(error_msg, max_len=200):
    """仅保留主要错误信息，避免把完整堆栈写入任务表。"""
    if error_msg is None:
        return None
    text = str(error_msg).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return None
    # 只取首行，过滤 traceback 等细节
    major = text.split("\n", 1)[0].strip()
    return major[:max_len]


class RetryableProcessError(Exception):
    """表示短暂性错误，可重试。"""


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
                ON CONFLICT (type, lot_id, wafer_id) DO UPDATE SET
                  created_at = NOW()
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
            major_error = summarize_error_msg(error_msg, max_len=200)
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
                cur.execute(sql, (rec_type, zip_name, zip_path, status, major_error))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

    def get_dashboard_metrics(self):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      COUNT(*) AS total_tasks,
                      COUNT(*) FILTER (WHERE status='PENDING') AS pending_tasks,
                      COUNT(*) FILTER (WHERE status='SUCCESS') AS success_tasks,
                      COUNT(*) FILTER (WHERE status='FAILED') AS failed_tasks
                    FROM {self.cfg.DB_TASK_TABLE}
                    """
                )
                total_tasks, pending_tasks, success_tasks, failed_tasks = cur.fetchone()

                cur.execute(f"SELECT COUNT(*) FROM {self.cfg.DB_TABLE}")
                (total_records,) = cur.fetchone()

                cur.execute(
                    f"""
                    SELECT type,
                           COUNT(*) AS task_count,
                           MAX(updated_at) AS last_update
                    FROM {self.cfg.DB_TASK_TABLE}
                    GROUP BY type
                    ORDER BY type
                    """
                )
                by_type = [
                    {
                        "type": row[0],
                        "task_count": row[1],
                        "last_update": row[2].strftime("%Y-%m-%d %H:%M:%S")
                        if row[2]
                        else None,
                    }
                    for row in cur.fetchall()
                ]

            return {
                "summary": {
                    "total_tasks": total_tasks or 0,
                    "pending_tasks": pending_tasks or 0,
                    "success_tasks": success_tasks or 0,
                    "failed_tasks": failed_tasks or 0,
                    "total_records": total_records or 0,
                },
                "by_type": by_type,
                "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        finally:
            self.pool.putconn(conn)

    def get_recent_tasks(self, page=1, page_size=20, keyword=""):
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        offset = (page - 1) * page_size
        kw = (keyword or "").strip()

        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                if kw:
                    like_kw = f"%{kw}%"
                    where_sql = """
                    WHERE type ILIKE %s
                       OR zip_name ILIKE %s
                       OR status ILIKE %s
                       OR COALESCE(error_msg, '') ILIKE %s
                       OR zip_path ILIKE %s
                    """
                    params = (like_kw, like_kw, like_kw, like_kw, like_kw)
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {self.cfg.DB_TASK_TABLE}
                        {where_sql}
                        """,
                        params,
                    )
                    (total,) = cur.fetchone()
                    cur.execute(
                        f"""
                        SELECT type, zip_name, status, error_msg, zip_path, updated_at
                        FROM {self.cfg.DB_TASK_TABLE}
                        {where_sql}
                        ORDER BY updated_at DESC
                        LIMIT %s OFFSET %s
                        """,
                        params + (page_size, offset),
                    )
                else:
                    cur.execute(f"SELECT COUNT(*) FROM {self.cfg.DB_TASK_TABLE}")
                    (total,) = cur.fetchone()
                    cur.execute(
                        f"""
                        SELECT type, zip_name, status, error_msg, zip_path, updated_at
                        FROM {self.cfg.DB_TASK_TABLE}
                        ORDER BY updated_at DESC
                        LIMIT %s OFFSET %s
                        """,
                        (page_size, offset),
                    )
                rows = cur.fetchall()
            items = [
                {
                    "type": row[0],
                    "zip_name": row[1],
                    "status": row[2],
                    "error_msg": row[3],
                    "zip_path": row[4],
                    "updated_at": row[5].strftime("%Y-%m-%d %H:%M:%S")
                    if row[5]
                    else None,
                }
                for row in rows
            ]
            total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
            return {
                "items": items,
                "page": page,
                "page_size": page_size,
                "total": total or 0,
                "total_pages": total_pages,
                "q": kw,
            }
        finally:
            self.pool.putconn(conn)

    def get_recent_records(self, page=1, page_size=20, keyword=""):
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        offset = (page - 1) * page_size
        kw = (keyword or "").strip()

        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                if kw:
                    like_kw = f"%{kw}%"
                    where_sql = """
                    WHERE type ILIKE %s
                       OR lot_id ILIKE %s
                       OR wafer_id ILIKE %s
                       OR zip_name ILIKE %s
                    """
                    params = (like_kw, like_kw, like_kw, like_kw)
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {self.cfg.DB_TABLE}
                        {where_sql}
                        """,
                        params,
                    )
                    (total,) = cur.fetchone()
                    cur.execute(
                        f"""
                        SELECT type, lot_id, wafer_id, zip_name, created_at
                        FROM {self.cfg.DB_TABLE}
                        {where_sql}
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                        """,
                        params + (page_size, offset),
                    )
                else:
                    cur.execute(f"SELECT COUNT(*) FROM {self.cfg.DB_TABLE}")
                    (total,) = cur.fetchone()
                    cur.execute(
                        f"""
                        SELECT type, lot_id, wafer_id, zip_name, created_at
                        FROM {self.cfg.DB_TABLE}
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                        """,
                        (page_size, offset),
                    )
                rows = cur.fetchall()
            items = [
                {
                    "type": row[0],
                    "lot_id": row[1],
                    "wafer_id": row[2],
                    "zip_name": row[3],
                    "created_at": row[4].strftime("%Y-%m-%d %H:%M:%S")
                    if row[4]
                    else None,
                }
                for row in rows
            ]
            total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
            return {
                "items": items,
                "page": page,
                "page_size": page_size,
                "total": total or 0,
                "total_pages": total_pages,
                "q": kw,
            }
        finally:
            self.pool.putconn(conn)

    def check_health(self):
        conn = None
        try:
            conn = self.pool.getconn()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True, None
        except Exception as ex:
            return False, summarize_error_msg(ex)
        finally:
            if conn:
                try:
                    self.pool.putconn(conn)
                except Exception:
                    pass

    def close(self):
        try:
            self.pool.closeall()
        except Exception as ex:
            logger.exception("关闭数据库连接池时出错：%s", ex)


# =========================
# 核心处理
# =========================
class Processor:
    def __init__(self, cfg: Config, pg: PgClient):
        self.cfg = cfg
        self.pg = pg
        self.processing = set()
        self.lock = threading.Lock()
        self.sync_types = set(cfg.SYNC_TYPES)

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
        rec_type_upper = rec_type.upper()
        if self.sync_types and rec_type_upper not in self.sync_types:
            logger.info(
                "跳过（类型未配置）：%s type=%s",
                path,
                rec_type_upper,
            )
            return
        rec_type = rec_type_upper

        key = str(path)
        with self.lock:
            if key in self.processing:
                return
            self.processing.add(key)

        max_attempts = max(1, int(self.cfg.PROCESS_RETRY_TIMES))
        last_error = None

        try:
            self.pg.upsert_task_status(rec_type, path.name, str(path), "PENDING")
            for attempt in range(max_attempts):
                try:
                    self._process(path)
                    self.pg.upsert_task_status(rec_type, path.name, str(path), "SUCCESS")
                    return
                except Exception as ex:
                    last_error = ex
                    retryable = self._is_retryable_error(ex)
                    has_next = attempt < max_attempts - 1

                    if retryable and has_next:
                        delay = self._retry_delay(attempt)
                        logger.warning(
                            "处理失败，准备重试：%s (%s/%s), %ss 后重试, err=%s",
                            path,
                            attempt + 1,
                            max_attempts,
                            f"{delay:.2f}",
                            summarize_error_msg(ex),
                        )
                        time.sleep(delay)
                        continue

                    if retryable:
                        logger.exception(
                            "处理失败（达到最大重试次数）：%s, err=%s",
                            path,
                            summarize_error_msg(ex),
                        )
                    else:
                        logger.warning(
                            "处理失败（不可重试）：%s, err=%s",
                            path,
                            summarize_error_msg(ex),
                        )
                    break

            failure_reason = (
                f"{type(last_error).__name__}: {last_error}"
                if last_error
                else "处理失败（未知错误）"
            )
            self.pg.upsert_task_status(
                rec_type,
                path.name,
                str(path),
                "FAILED",
                failure_reason,
            )
        except Exception as ex:
            # 避免事件回调线程因未捕获异常中断，记录后继续监听
            logger.exception(f"处理失败：{path}, err={ex}")
            try:
                self.pg.upsert_task_status(
                    rec_type, path.name, str(path), "FAILED", f"{type(ex).__name__}: {ex}"
                )
            except Exception:
                logger.exception("写入 FAILED 状态再次失败：%s", path)
        finally:
            with self.lock:
                self.processing.discard(key)

    def _retry_delay(self, attempt: int) -> float:
        base = max(0.1, float(self.cfg.PROCESS_RETRY_INTERVAL_SEC))
        max_delay = max(base, float(self.cfg.PROCESS_RETRY_BACKOFF_MAX_SEC))
        return min(base * (2**attempt), max_delay)

    def _is_retryable_error(self, ex: Exception) -> bool:
        if isinstance(ex, RetryableProcessError):
            return True
        if isinstance(
            ex,
            (
                PermissionError,
                TimeoutError,
                zipfile.BadZipFile,
                OperationalError,
                InterfaceError,
                DatabaseError,
            ),
        ):
            return True
        if isinstance(ex, OSError) and ex.errno in {
            errno.EACCES,
            errno.EBUSY,
            errno.ETXTBSY,
        }:
            return True
        return False

    def _process(self, path: Path):
        # 文件已被其他并发事件处理并移走时，视为当前事件无需再处理
        if not path.exists():
            logger.info(f"跳过（文件不存在，可能已处理）：{path}")
            return True

        logger.info(f"处理：{path}")

        if not self.wait_stable(path):
            raise RetryableProcessError("文件未稳定")

        rel = path.relative_to(self.cfg.WATCH_DIR)

        rec_type = rel.parts[-3].upper()
        target_dir = self.cfg.TARGET_DIR / rel.parent
        backup_dir = path.parent / "BACKUP"
        backup_zip_path = backup_dir / path.name

        lot_wafer_pairs = self.scan_zip(path, target_dir)

        self.pg.insert_records(rec_type, lot_wafer_pairs, path.name, str(path))

        backup_dir.mkdir(parents=True, exist_ok=True)
        if backup_zip_path.exists():
            backup_zip_path.unlink()
        shutil.move(str(path), str(backup_zip_path))

        logger.info(f"完成：{path} -> {backup_zip_path}，MAP 已解压到 {target_dir}")
        return True

    def scan_zip(self, zip_path, target_dir: Path):
        lot_wafer_pairs = []
        map_name_pattern = re.compile(r"^([A-Za-z0-9]{6})-([A-Za-z0-9]{2})$")
        zip_prefix = Path(zip_path).stem.split("-", 1)[0].upper()
        map_entries = []

        with zipfile.ZipFile(zip_path) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                f = Path(info.filename).name
                if Path(f).suffix.lower() != ".map":
                    continue

                stem = Path(f).stem
                if self.cfg.CHECK_ZIP_MAP_SAME_PREFIX:
                    map_prefix = stem.split("-", 1)[0].upper()
                    if map_prefix != zip_prefix:
                        raise ValueError(
                            f"ZIP 与 MAP 文件名前缀不一致：zip={Path(zip_path).name}, map={f}"
                        )

                match = map_name_pattern.match(stem)
                if self.cfg.CHECK_MAP_FILENAME_FORMAT and not match:
                    raise ValueError(f"MAP 文件名格式错误：{f}，期望格式为 XXXXXX-XX")

                if match:
                    lot_id = match.group(1).upper()
                    wafer_id = match.group(2).upper()
                else:
                    parts = stem.split("-", 1)
                    lot_id = (parts[0] if parts and parts[0] else "UNKNOWN").upper()
                    wafer_id = (
                        parts[1] if len(parts) > 1 and parts[1] else "UNKNOWN"
                    ).upper()

                lot_wafer_pairs.append((lot_id, wafer_id))
                map_entries.append((info, f))

            if not map_entries:
                raise ValueError(f"ZIP 内未找到 MAP 文件：{Path(zip_path).name}")

            target_dir.mkdir(parents=True, exist_ok=True)
            for info, map_file_name in map_entries:
                target_map_path = target_dir / map_file_name
                temp_path = target_map_path.with_suffix(f"{target_map_path.suffix}.tmp")
                with z.open(info, "r") as src, temp_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                temp_path.replace(target_map_path)

            return sorted(set(lot_wafer_pairs))


# =========================
# 任务队列
# =========================
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


# =========================
# 监听
# =========================
class Handler(FileSystemEventHandler):
    def __init__(self, worker_pool, dedup_window_sec=1.0):
        self.worker_pool = worker_pool
        self._dedup_window_sec = max(0.0, float(dedup_window_sec))
        self._event_lock = threading.Lock()
        self._last_event_ts = {}
        self._active = threading.Event()
        self._active.set()

    def is_active(self):
        return self._active.is_set()

    def disable(self):
        self._active.clear()

    def _handle_event(self, path: Path):
        if not self.is_active():
            logger.info("监听器已停用，忽略事件：%s", path)
            return
        key = str(path)
        now = time.monotonic()

        with self._event_lock:
            if self._dedup_window_sec > 0:
                last_ts = self._last_event_ts.get(key)
                if last_ts is not None and (now - last_ts) < self._dedup_window_sec:
                    logger.debug("事件去抖跳过：%s", path)
                    return
            self._last_event_ts[key] = now

            if len(self._last_event_ts) > 5000:
                expire_before = now - max(1.0, self._dedup_window_sec * 5)
                self._last_event_ts = {
                    p: ts for p, ts in self._last_event_ts.items() if ts >= expire_before
                }

        self.worker_pool.enqueue(path)

    def on_created(self, e):
        if not e.is_directory:
            self._handle_event(Path(e.src_path))

    def on_modified(self, e):
        if not e.is_directory:
            self._handle_event(Path(e.src_path))

    def on_moved(self, e):
        if not e.is_directory:
            # 兼容“先写临时文件再重命名为 .zip”的上传方式
            self._handle_event(Path(e.dest_path))


class ServiceLifecycle:
    def __init__(self, handler: Handler, observer: Observer):
        self.handler = handler
        self.observer = observer
        self._web_running = threading.Event()
        self._shutdown_requested = threading.Event()

    def mark_web_started(self):
        self._web_running.set()

    def mark_web_stopped(self):
        self._web_running.clear()

    def request_shutdown(self):
        if not self._shutdown_requested.is_set():
            self._shutdown_requested.set()
            self.handler.disable()

    def watcher_healthy(self):
        return self.handler.is_active() and self.observer.is_alive()

    def web_healthy(self):
        return self._web_running.is_set()

    def is_shutting_down(self):
        return self._shutdown_requested.is_set()


# =========================
# Web 监控
# =========================
DASHBOARD_HTML = """<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>NAS 同步监控</title>
  <style>
    :root {
      --bg: #f4f7fb;
      --panel: #ffffff;
      --text: #1f2a37;
      --muted: #6b7280;
      --ok: #15803d;
      --warn: #b45309;
      --err: #b91c1c;
      --border: #e5e7eb;
    }
    body { margin: 0; font-family: \"Microsoft YaHei\", \"PingFang SC\", sans-serif; background: var(--bg); color: var(--text); }
    .wrap { max-width: 1280px; margin: 0 auto; padding: 16px; }
    .title { font-size: 24px; font-weight: 700; margin: 8px 0; }
    .sub { color: var(--muted); margin-bottom: 16px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr)); gap: 12px; }
    .card { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 14px; }
    .label { font-size: 13px; color: var(--muted); margin-bottom: 6px; }
    .value { font-size: 28px; font-weight: 700; }
    .ok { color: var(--ok); }
    .warn { color: var(--warn); }
    .err { color: var(--err); }
    .section { margin-top: 14px; background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 14px; }
    .toolbar { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; align-items: center; }
    .toolbar input, .toolbar select { border: 1px solid var(--border); border-radius: 8px; padding: 6px 8px; font-size: 13px; background: #fff; }
    .toolbar button { border: 1px solid var(--border); border-radius: 8px; padding: 6px 10px; font-size: 13px; background: #fff; cursor: pointer; }
    .toolbar button:disabled { color: #9ca3af; cursor: not-allowed; }
    .pager { color: var(--muted); font-size: 12px; }
    h2 { font-size: 16px; margin: 0 0 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 10px 8px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }
    th { color: var(--muted); font-weight: 600; }
    .mono { font-family: Consolas, monospace; }
    .status { padding: 2px 8px; border-radius: 12px; color: #fff; font-size: 12px; }
    .status.SUCCESS { background: var(--ok); }
    .status.PENDING { background: var(--warn); }
    .status.FAILED { background: var(--err); }
    .meta { margin-top: 12px; color: var(--muted); font-size: 12px; display: flex; justify-content: space-between; }
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"title\">NAS 文件同步监控</div>
    <div class=\"sub\">自动刷新间隔：5 秒</div>

    <div class=\"grid\">
      <div class=\"card\"><div class=\"label\">任务总数</div><div id=\"total_tasks\" class=\"value\">-</div></div>
      <div class=\"card\"><div class=\"label\">处理中</div><div id=\"pending_tasks\" class=\"value warn\">-</div></div>
      <div class=\"card\"><div class=\"label\">成功</div><div id=\"success_tasks\" class=\"value ok\">-</div></div>
      <div class=\"card\"><div class=\"label\">失败</div><div id=\"failed_tasks\" class=\"value err\">-</div></div>
      <div class=\"card\"><div class=\"label\">已入库记录</div><div id=\"total_records\" class=\"value\">-</div></div>
    </div>

    <div class=\"section\">
      <h2>类型统计</h2>
      <table>
        <thead><tr><th>类型</th><th>任务数</th><th>最近更新时间</th></tr></thead>
        <tbody id=\"type_tbody\"></tbody>
      </table>
    </div>

    <div class=\"section\">
      <h2>最近任务</h2>
      <div class=\"toolbar\">
        <input id=\"task_q\" type=\"text\" placeholder=\"搜索类型/压缩包/状态/错误信息/路径\">
        <select id=\"task_page_size\">
          <option value=\"10\">10 条/页</option>
          <option value=\"20\" selected>20 条/页</option>
          <option value=\"50\">50 条/页</option>
        </select>
        <button id=\"task_search_btn\" type=\"button\">搜索</button>
        <button id=\"task_prev_btn\" type=\"button\">上一页</button>
        <button id=\"task_next_btn\" type=\"button\">下一页</button>
        <span id=\"task_pager\" class=\"pager\">第 -/- 页，共 0 条</span>
      </div>
      <table>
        <thead><tr><th>时间</th><th>类型</th><th>压缩包</th><th>状态</th><th>错误信息</th></tr></thead>
        <tbody id=\"task_tbody\"></tbody>
      </table>
    </div>

    <div class=\"section\">
      <h2>最近入库记录</h2>
      <div class=\"toolbar\">
        <input id=\"record_q\" type=\"text\" placeholder=\"搜索类型/LOT/WAFER/来源压缩包\">
        <select id=\"record_page_size\">
          <option value=\"10\">10 条/页</option>
          <option value=\"20\" selected>20 条/页</option>
          <option value=\"50\">50 条/页</option>
        </select>
        <button id=\"record_search_btn\" type=\"button\">搜索</button>
        <button id=\"record_prev_btn\" type=\"button\">上一页</button>
        <button id=\"record_next_btn\" type=\"button\">下一页</button>
        <span id=\"record_pager\" class=\"pager\">第 -/- 页，共 0 条</span>
      </div>
      <table>
        <thead><tr><th>时间</th><th>类型</th><th>LOT</th><th>WAFER</th><th>来源压缩包</th></tr></thead>
        <tbody id=\"record_tbody\"></tbody>
      </table>
    </div>

    <div class=\"meta\">
      <span id=\"refresh_time\">更新时间：-</span>
      <span>接口：/api/dashboard</span>
    </div>
  </div>

  <script>
    const state = {
      task: { page: 1, page_size: 20, q: '' },
      record: { page: 1, page_size: 20, q: '' },
    };

    function esc(v) {
      if (v === null || v === undefined) return '';
      return String(v).replace(/[&<>\"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[m]));
    }

    async function loadData() {
      const params = new URLSearchParams({
        task_page: String(state.task.page),
        task_page_size: String(state.task.page_size),
        task_q: state.task.q,
        record_page: String(state.record.page),
        record_page_size: String(state.record.page_size),
        record_q: state.record.q,
      });
      const resp = await fetch('/api/dashboard?' + params.toString());
      const data = await resp.json();
      const s = data.summary || {};

      document.getElementById('total_tasks').textContent = s.total_tasks ?? 0;
      document.getElementById('pending_tasks').textContent = s.pending_tasks ?? 0;
      document.getElementById('success_tasks').textContent = s.success_tasks ?? 0;
      document.getElementById('failed_tasks').textContent = s.failed_tasks ?? 0;
      document.getElementById('total_records').textContent = s.total_records ?? 0;

      document.getElementById('type_tbody').innerHTML = (data.by_type || []).map(x => `
        <tr>
          <td>${esc(x.type)}</td>
          <td>${esc(x.task_count)}</td>
          <td>${esc(x.last_update || '-')}</td>
        </tr>
      `).join('');

      const taskData = data.recent_tasks || {};
      const taskItems = taskData.items || [];
      document.getElementById('task_tbody').innerHTML = taskItems.map(x => `
        <tr>
          <td>${esc(x.updated_at || '-')}</td>
          <td>${esc(x.type)}</td>
          <td class=\"mono\">${esc(x.zip_name)}</td>
          <td><span class=\"status ${esc(x.status)}\">${esc(x.status)}</span></td>
          <td>${esc(x.error_msg || '-')}</td>
        </tr>
      `).join('');
      document.getElementById('task_pager').textContent = `第 ${taskData.page || 1}/${taskData.total_pages || 1} 页，共 ${taskData.total || 0} 条`;
      document.getElementById('task_prev_btn').disabled = (taskData.page || 1) <= 1;
      document.getElementById('task_next_btn').disabled = (taskData.page || 1) >= (taskData.total_pages || 1);

      const recordData = data.recent_records || {};
      const recordItems = recordData.items || [];
      document.getElementById('record_tbody').innerHTML = recordItems.map(x => `
        <tr>
          <td>${esc(x.created_at || '-')}</td>
          <td>${esc(x.type)}</td>
          <td>${esc(x.lot_id)}</td>
          <td>${esc(x.wafer_id)}</td>
          <td class=\"mono\">${esc(x.zip_name)}</td>
        </tr>
      `).join('');
      document.getElementById('record_pager').textContent = `第 ${recordData.page || 1}/${recordData.total_pages || 1} 页，共 ${recordData.total || 0} 条`;
      document.getElementById('record_prev_btn').disabled = (recordData.page || 1) <= 1;
      document.getElementById('record_next_btn').disabled = (recordData.page || 1) >= (recordData.total_pages || 1);

      document.getElementById('refresh_time').textContent = '更新时间：' + esc(data.generated_at || '-');
    }

    let loading = false;
    async function tick() {
      if (loading) return;
      loading = true;
      try {
        await loadData();
      } catch (e) {
        console.error('加载失败', e);
      } finally {
        loading = false;
      }
    }

    function bindToolbar() {
      const taskQ = document.getElementById('task_q');
      const taskPageSize = document.getElementById('task_page_size');
      const recordQ = document.getElementById('record_q');
      const recordPageSize = document.getElementById('record_page_size');

      document.getElementById('task_search_btn').addEventListener('click', () => {
        state.task.q = taskQ.value.trim();
        state.task.page_size = Number(taskPageSize.value) || 20;
        state.task.page = 1;
        tick();
      });
      taskQ.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') document.getElementById('task_search_btn').click();
      });
      taskPageSize.addEventListener('change', () => {
        state.task.page_size = Number(taskPageSize.value) || 20;
        state.task.page = 1;
        tick();
      });
      document.getElementById('task_prev_btn').addEventListener('click', () => {
        if (state.task.page > 1) {
          state.task.page -= 1;
          tick();
        }
      });
      document.getElementById('task_next_btn').addEventListener('click', () => {
        state.task.page += 1;
        tick();
      });

      document.getElementById('record_search_btn').addEventListener('click', () => {
        state.record.q = recordQ.value.trim();
        state.record.page_size = Number(recordPageSize.value) || 20;
        state.record.page = 1;
        tick();
      });
      recordQ.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') document.getElementById('record_search_btn').click();
      });
      recordPageSize.addEventListener('change', () => {
        state.record.page_size = Number(recordPageSize.value) || 20;
        state.record.page = 1;
        tick();
      });
      document.getElementById('record_prev_btn').addEventListener('click', () => {
        if (state.record.page > 1) {
          state.record.page -= 1;
          tick();
        }
      });
      document.getElementById('record_next_btn').addEventListener('click', () => {
        state.record.page += 1;
        tick();
      });
    }

    bindToolbar();
    tick();
    setInterval(tick, 5000);
  </script>
</body>
</html>
"""


def create_dashboard_handler(pg: PgClient, lifecycle: ServiceLifecycle, cfg: Config):
    cache_lock = threading.Lock()
    cache_ttl_sec = max(0.0, float(cfg.DASHBOARD_CACHE_TTL_SEC))
    cache_key = None
    cache_payload = None
    cache_expire_at = 0.0

    class DashboardHandler(BaseHTTPRequestHandler):
        def _send_json(self, payload, status=HTTPStatus.OK):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, html, status=HTTPStatus.OK):
            body = html.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path in ("/", "/dashboard"):
                self._send_html(DASHBOARD_HTML)
                return

            if parsed.path == "/healthz":
                watcher_state = "running" if lifecycle.watcher_healthy() else "stopped"
                web_state = "running" if lifecycle.web_healthy() else "stopped"
                db_ok, db_msg = pg.check_health()
                payload = {
                    "status": "ok" if db_ok else "degraded",
                    "watcher": watcher_state,
                    "web": web_state,
                    "db": "ok" if db_ok else "error",
                    "db_error": db_msg,
                    "shutting_down": lifecycle.is_shutting_down(),
                }
                self._send_json(payload)
                return

            if parsed.path == "/api/dashboard":
                nonlocal cache_key, cache_payload, cache_expire_at
                qs = parse_qs(parsed.query or "")
                try:
                    task_page = max(1, int((qs.get("task_page") or ["1"])[0]))
                    task_page_size = max(
                        1, min(int((qs.get("task_page_size") or ["20"])[0]), 100)
                    )
                    task_q = (qs.get("task_q") or [""])[0]

                    record_page = max(1, int((qs.get("record_page") or ["1"])[0]))
                    record_page_size = max(
                        1, min(int((qs.get("record_page_size") or ["20"])[0]), 100)
                    )
                    record_q = (qs.get("record_q") or [""])[0]
                except ValueError:
                    self._send_json(
                        {"error": "分页参数必须是数字"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                current_key = (
                    task_page,
                    task_page_size,
                    task_q,
                    record_page,
                    record_page_size,
                    record_q,
                )
                now = time.monotonic()

                with cache_lock:
                    if (
                        cache_key == current_key
                        and cache_payload is not None
                        and now < cache_expire_at
                    ):
                        self._send_json(cache_payload)
                        return

                data = pg.get_dashboard_metrics()
                data["recent_tasks"] = pg.get_recent_tasks(
                    page=task_page, page_size=task_page_size, keyword=task_q
                )
                data["recent_records"] = pg.get_recent_records(
                    page=record_page, page_size=record_page_size, keyword=record_q
                )

                if cache_ttl_sec > 0:
                    with cache_lock:
                        cache_key = current_key
                        cache_payload = data
                        cache_expire_at = now + cache_ttl_sec

                self._send_json(data)
                return

            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

        def log_message(self, fmt, *args):
            logger.info("WEB %s - %s", self.address_string(), fmt % args)

    return DashboardHandler


# =========================
# 主程序
# =========================
def main():
    pg = PgClient(CONFIG)
    p = Processor(CONFIG, pg)
    worker_pool = TaskWorkerPool(p, max_queue_size=CONFIG.TASK_QUEUE_MAX_SIZE)

    handler = Handler(worker_pool, dedup_window_sec=CONFIG.EVENT_DEDUP_WINDOW_SEC)
    observer = Observer()
    lifecycle = ServiceLifecycle(handler, observer)

    if CONFIG.INITIAL_SCAN:
        for f in CONFIG.WATCH_DIR.rglob("*"):
            if f.is_file() and f.suffix.lower() == ".zip":
                handler._handle_event(f)

    observer.schedule(handler, str(CONFIG.WATCH_DIR), recursive=True)
    observer.start()
    logger.info("启动监听...")

    web_server = ThreadingHTTPServer(
        (CONFIG.WEB_HOST, CONFIG.WEB_PORT),
        create_dashboard_handler(pg, lifecycle, CONFIG),
    )
    lifecycle.mark_web_started()
    web_thread = threading.Thread(target=web_server.serve_forever, daemon=True)
    web_thread.start()
    logger.info(f"Web 监控已启动：http://{CONFIG.WEB_HOST}:{CONFIG.WEB_PORT}/dashboard")

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


if __name__ == "__main__":
    main()

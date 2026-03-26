import logging
import os
import threading
import time
from pathlib import Path

from psycopg2 import InterfaceError, OperationalError
from psycopg2 import pool

from .config import Config
from .errors import summarize_error_msg

logger = logging.getLogger("watcher")


class PgClient:
    MAP_PATH_TABLE = "map_path_config"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.pool = None
        self._pool_lock = threading.Lock()
        self._connect_failures = 0
        self._next_connect_retry_ts = 0.0

        self._map_table_ready = False
        self._map_table_lock = threading.Lock()
        self._map_cache_lock = threading.Lock()
        self._map_config_cache = []
        self._map_config_cache_expire_at = 0.0

    def _new_pool(self):
        return pool.SimpleConnectionPool(
            1,
            5,
            host=self.cfg.DB_HOST,
            port=self.cfg.DB_PORT,
            dbname=self.cfg.DB_NAME,
            user=self.cfg.DB_USER,
            password=self.cfg.DB_PASSWORD,
            options=f"-c search_path={self.cfg.DB_SCHEMA}",
        )

    def _is_connection_error(self, ex: Exception) -> bool:
        if isinstance(ex, (OperationalError, InterfaceError, pool.PoolError)):
            return True
        text = str(ex).lower()
        keywords = [
            "could not connect",
            "connection refused",
            "connection timed out",
            "timeout expired",
            "server closed the connection unexpectedly",
            "terminating connection",
            "connection already closed",
            "connection not open",
            "could not receive data from server",
            "no connection to the server",
        ]
        return any(keyword in text for keyword in keywords)

    def _reset_pool(self):
        old_pool = None
        with self._pool_lock:
            old_pool = self.pool
            self.pool = None

        if old_pool is not None:
            try:
                old_pool.closeall()
            except Exception as ex:
                logger.warning("关闭旧数据库连接池失败：%s", summarize_error_msg(ex))

    def _ensure_pool(self):
        existing = self.pool
        if existing is not None:
            return existing

        now = time.monotonic()
        if now < self._next_connect_retry_ts:
            wait_sec = self._next_connect_retry_ts - now
            raise OperationalError(f"数据库连接不可用，约 {wait_sec:.1f}s 后重试")

        with self._pool_lock:
            if self.pool is not None:
                return self.pool

            now = time.monotonic()
            if now < self._next_connect_retry_ts:
                wait_sec = self._next_connect_retry_ts - now
                raise OperationalError(f"数据库连接不可用，约 {wait_sec:.1f}s 后重试")

            try:
                self.pool = self._new_pool()
                self._connect_failures = 0
                self._next_connect_retry_ts = 0.0
                logger.info("数据库连接池建立成功")
            except Exception as ex:
                self._connect_failures += 1
                base = max(1.0, float(self.cfg.PROCESS_RETRY_INTERVAL_SEC))
                max_delay = max(base, float(self.cfg.PROCESS_RETRY_BACKOFF_MAX_SEC))
                delay = min(base * (2 ** (self._connect_failures - 1)), max_delay)
                self._next_connect_retry_ts = time.monotonic() + delay
                logger.warning(
                    "数据库连接失败，%.1fs 后重试，err=%s",
                    delay,
                    summarize_error_msg(ex),
                )
                raise

            return self.pool

    def _acquire_conn(self):
        active_pool = self._ensure_pool()
        try:
            conn = active_pool.getconn()
            return active_pool, conn
        except Exception as ex:
            if self._is_connection_error(ex):
                self._reset_pool()
            raise

    def _release_conn(self, active_pool, conn):
        if not active_pool or not conn:
            return
        try:
            active_pool.putconn(conn)
        except Exception as ex:
            logger.debug("归还数据库连接失败：%s", summarize_error_msg(ex))

    def _handle_db_exception(self, ex: Exception):
        if self._is_connection_error(ex):
            self._reset_pool()

    def acquire_connection(self):
        return self._acquire_conn()

    def release_connection(self, active_pool, conn):
        self._release_conn(active_pool, conn)

    def _insert_records_with_cursor(self, cur, rec_type, lot_wafer_pairs, zip_name, zip_path):
        sql = f"""
        INSERT INTO {self.cfg.DB_TABLE}
        (type, lot_id, wafer_id, zip_name, zip_path)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (type, lot_id, wafer_id) DO UPDATE SET
          created_at = NOW(),
          zip_name = EXCLUDED.zip_name,
          zip_path = EXCLUDED.zip_path
        """
        data = [
            (rec_type, lot_id, wafer_id, zip_name, zip_path)
            for lot_id, wafer_id in lot_wafer_pairs
        ]
        cur.executemany(sql, data)

    def insert_records(self, rec_type, lot_wafer_pairs, zip_name, zip_path, conn=None):
        if conn is not None:
            with conn.cursor() as cur:
                self._insert_records_with_cursor(
                    cur, rec_type, lot_wafer_pairs, zip_name, zip_path
                )
            return

        active_pool = None
        owned_conn = None
        try:
            active_pool, owned_conn = self._acquire_conn()
            with owned_conn.cursor() as cur:
                self._insert_records_with_cursor(
                    cur, rec_type, lot_wafer_pairs, zip_name, zip_path
                )
            owned_conn.commit()
        except Exception as ex:
            if owned_conn is not None:
                try:
                    owned_conn.rollback()
                except Exception:
                    pass
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, owned_conn)

    def upsert_task_status(self, rec_type, zip_name, zip_path, status, error_msg=None):
        active_pool = None
        conn = None
        try:
            active_pool, conn = self._acquire_conn()
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
        except Exception as ex:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, conn)

    def _ensure_map_path_config_table(self):
        if self._map_table_ready:
            return

        with self._map_table_lock:
            if self._map_table_ready:
                return

            active_pool = None
            conn = None
            try:
                active_pool, conn = self._acquire_conn()
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {self.MAP_PATH_TABLE} (
                            id BIGSERIAL PRIMARY KEY,
                            sync_types VARCHAR(50) NOT NULL,
                            watch_dir VARCHAR(1000) NOT NULL,
                            target_dir VARCHAR(1000) NOT NULL,
                            enabled BOOLEAN NOT NULL DEFAULT TRUE,
                            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            CONSTRAINT uq_map_path_config_watch UNIQUE (watch_dir)
                        )
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE INDEX IF NOT EXISTS idx_map_path_config_enabled
                        ON {self.MAP_PATH_TABLE} (enabled, sync_types)
                        """
                    )
                conn.commit()
                self._map_table_ready = True
            except Exception as ex:
                if conn is not None:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                self._handle_db_exception(ex)
                raise
            finally:
                self._release_conn(active_pool, conn)

    @staticmethod
    def _normalize_fs_path(path_value) -> str:
        return os.path.normcase(os.path.normpath(str(Path(path_value))))

    @staticmethod
    def _normalize_sync_type(sync_types: str) -> str:
        return (sync_types or "").strip().upper()

    def _invalidate_map_path_cache(self):
        with self._map_cache_lock:
            self._map_config_cache = []
            self._map_config_cache_expire_at = 0.0

    def _get_map_path_configs_cached(self, only_enabled=False):
        self._ensure_map_path_config_table()
        now = time.monotonic()
        cache_ttl = max(0.1, float(self.cfg.DASHBOARD_CACHE_TTL_SEC or 2.0))

        with self._map_cache_lock:
            if now < self._map_config_cache_expire_at and self._map_config_cache:
                rows = list(self._map_config_cache)
            else:
                rows = self._query_map_path_configs_from_db()
                self._map_config_cache = list(rows)
                self._map_config_cache_expire_at = now + cache_ttl

        if only_enabled:
            return [item for item in rows if item.get("enabled")]
        return rows

    def _query_map_path_configs_from_db(self):
        active_pool = None
        conn = None
        try:
            active_pool, conn = self._acquire_conn()
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, sync_types, watch_dir, target_dir, enabled, created_at, updated_at
                    FROM {self.MAP_PATH_TABLE}
                    ORDER BY sync_types, watch_dir
                    """
                )
                rows = cur.fetchall()

            data = []
            for row in rows:
                data.append(
                    {
                        "id": row[0],
                        "sync_types": row[1],
                        "watch_dir": row[2],
                        "target_dir": row[3],
                        "enabled": bool(row[4]),
                        "created_at": row[5].strftime("%Y-%m-%d %H:%M:%S") if row[5] else None,
                        "updated_at": row[6].strftime("%Y-%m-%d %H:%M:%S") if row[6] else None,
                    }
                )
            return data
        except Exception as ex:
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, conn)

    def get_map_path_configs(self, only_enabled=False):
        return self._get_map_path_configs_cached(only_enabled=only_enabled)

    def create_map_path_config(self, sync_types, watch_dir, target_dir, enabled=True):
        normalized_sync_types = self._normalize_sync_type(sync_types)
        if not normalized_sync_types:
            raise ValueError("SYNC_TYPES 不能为空")

        normalized_watch = str(Path(watch_dir))
        normalized_target = str(Path(target_dir))

        active_pool = None
        conn = None
        try:
            self._ensure_map_path_config_table()
            active_pool, conn = self._acquire_conn()
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.MAP_PATH_TABLE}
                    (sync_types, watch_dir, target_dir, enabled, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    """,
                    (
                        normalized_sync_types,
                        normalized_watch,
                        normalized_target,
                        bool(enabled),
                    ),
                )
            conn.commit()
            self._invalidate_map_path_cache()
        except Exception as ex:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, conn)

    def update_map_path_config(
        self, config_id, sync_types, watch_dir, target_dir, enabled=True
    ):
        normalized_sync_types = self._normalize_sync_type(sync_types)
        if not normalized_sync_types:
            raise ValueError("SYNC_TYPES 不能为空")

        normalized_watch = str(Path(watch_dir))
        normalized_target = str(Path(target_dir))

        active_pool = None
        conn = None
        try:
            self._ensure_map_path_config_table()
            active_pool, conn = self._acquire_conn()
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.MAP_PATH_TABLE}
                    SET sync_types = %s,
                        watch_dir = %s,
                        target_dir = %s,
                        enabled = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        normalized_sync_types,
                        normalized_watch,
                        normalized_target,
                        bool(enabled),
                        int(config_id),
                    ),
                )
                if cur.rowcount == 0:
                    raise ValueError(f"配置不存在：id={config_id}")
            conn.commit()
            self._invalidate_map_path_cache()
        except Exception as ex:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, conn)

    def upsert_map_path_config(self, sync_types, watch_dir, target_dir, enabled=True):
        # 兼容旧调用：按 watch_dir 幂等写入
        normalized_watch = str(Path(watch_dir))
        exists = None
        for row in self.get_map_path_configs(only_enabled=False):
            if str(Path(row.get("watch_dir") or "")) == normalized_watch:
                exists = row
                break
        if exists:
            return self.update_map_path_config(
                exists["id"], sync_types, watch_dir, target_dir, enabled
            )
        return self.create_map_path_config(sync_types, watch_dir, target_dir, enabled)

    def delete_map_path_config(self, config_id):
        active_pool = None
        conn = None
        try:
            self._ensure_map_path_config_table()
            active_pool, conn = self._acquire_conn()
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.MAP_PATH_TABLE} WHERE id = %s",
                    (int(config_id),),
                )
            conn.commit()
            self._invalidate_map_path_cache()
        except Exception as ex:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, conn)

    def get_active_watch_dirs(self):
        rows = self.get_map_path_configs(only_enabled=True)
        uniq = []
        seen = set()
        for item in rows:
            watch_dir = item.get("watch_dir")
            if not watch_dir:
                continue
            normalized = self._normalize_fs_path(watch_dir)
            if normalized in seen:
                continue
            seen.add(normalized)
            uniq.append(str(Path(watch_dir)))
        return uniq

    def match_map_path_config(self, zip_path: Path):
        path_norm = self._normalize_fs_path(zip_path)
        candidates = self.get_map_path_configs(only_enabled=True)
        matched = None
        matched_len = -1

        for item in candidates:
            watch_dir = item.get("watch_dir")
            if not watch_dir:
                continue

            watch_norm = self._normalize_fs_path(watch_dir)
            if path_norm == watch_norm:
                length = len(watch_norm)
            elif path_norm.startswith(watch_norm + os.sep):
                length = len(watch_norm)
            else:
                continue

            if length > matched_len:
                matched = {
                    **item,
                    "watch_dir_path": Path(item["watch_dir"]),
                    "target_dir_path": Path(item["target_dir"]),
                    "sync_type": self._normalize_sync_type(item["sync_types"]),
                }
                matched_len = length

        return matched

    def ensure_default_map_path_config(self):
        defaults = self.get_map_path_configs(only_enabled=False)
        if defaults:
            return

        default_sync = "BP"
        if self.cfg.SYNC_TYPES:
            default_sync = self._normalize_sync_type(self.cfg.SYNC_TYPES[0]) or "BP"

        default_watch = Path(self.cfg.WATCH_DIR)
        default_target = Path(self.cfg.TARGET_DIR)
        self.upsert_map_path_config(
            sync_types=default_sync,
            watch_dir=default_watch,
            target_dir=default_target,
            enabled=True,
        )
        logger.info(
            "已初始化默认 map_path_config：SYNC_TYPES=%s, WATCH_DIR=%s, TARGET_DIR=%s",
            default_sync,
            default_watch,
            default_target,
        )

    def get_dashboard_metrics(self):
        active_pool = None
        conn = None
        try:
            active_pool, conn = self._acquire_conn()
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
        except Exception as ex:
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, conn)

    def get_recent_tasks(self, page=1, page_size=20, keyword=""):
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        offset = (page - 1) * page_size
        kw = (keyword or "").strip()

        active_pool = None
        conn = None
        try:
            active_pool, conn = self._acquire_conn()
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
        except Exception as ex:
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, conn)

    def get_recent_records(self, page=1, page_size=20, keyword=""):
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        offset = (page - 1) * page_size
        kw = (keyword or "").strip()

        active_pool = None
        conn = None
        try:
            active_pool, conn = self._acquire_conn()
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
        except Exception as ex:
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, conn)

    def check_health(self):
        active_pool = None
        conn = None
        try:
            active_pool, conn = self._acquire_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True, None
        except Exception as ex:
            self._handle_db_exception(ex)
            return False, summarize_error_msg(ex)
        finally:
            self._release_conn(active_pool, conn)

    def close(self):
        try:
            self._reset_pool()
        except Exception as ex:
            logger.exception("关闭数据库连接池时出错：%s", ex)

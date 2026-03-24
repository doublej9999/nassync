import logging
import threading
import time

from psycopg2 import InterfaceError, OperationalError
from psycopg2 import pool

from .config import Config
from .errors import summarize_error_msg

logger = logging.getLogger("watcher")


class PgClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.pool = None
        self._pool_lock = threading.Lock()
        self._connect_failures = 0
        self._next_connect_retry_ts = 0.0

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

    def insert_records(self, rec_type, lot_wafer_pairs, zip_name, zip_path):
        active_pool = None
        conn = None
        try:
            active_pool, conn = self._acquire_conn()
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

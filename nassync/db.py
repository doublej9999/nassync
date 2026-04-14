import logging
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from psycopg2 import InterfaceError, OperationalError
from psycopg2 import pool
from psycopg2.extras import execute_values

from .config import Config
from .errors import summarize_error_msg

logger = logging.getLogger("watcher")


class PgClient:
    MAP_PATH_TABLE = "map_path_config"
    SERVICE_LEASE_TABLE = "service_lease"
    TASK_QUEUE_TABLE = "zip_task_queue"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._validate_sql_identifiers()
        self.pool = None
        self._pool_lock = threading.Lock()
        self._connect_failures = 0
        self._next_connect_retry_ts = 0.0

        self._map_table_ready = False
        self._map_table_lock = threading.Lock()
        self._ha_tables_ready = False
        self._ha_tables_lock = threading.Lock()
        self._map_cache_lock = threading.Lock()
        self._map_config_cache = []
        self._map_config_cache_expire_at = 0.0

    def _validate_sql_identifiers(self):
        identifiers = {
            "DB_SCHEMA": self.cfg.DB_SCHEMA,
            "DB_TABLE": self.cfg.DB_TABLE,
            "DB_TASK_TABLE": self.cfg.DB_TASK_TABLE,
            "MAP_PATH_TABLE": self.MAP_PATH_TABLE,
            "SERVICE_LEASE_TABLE": self.SERVICE_LEASE_TABLE,
            "TASK_QUEUE_TABLE": self.TASK_QUEUE_TABLE,
        }
        for name, value in identifiers.items():
            raw = str(value or "").strip()
            if not raw:
                raise ValueError(f"{name} 不能为空")
            if not (raw[0].isalpha() or raw[0] == "_"):
                raise ValueError(f"{name} 非法（需以字母或下划线开头）: {value}")
            if not all(ch.isalnum() or ch == "_" for ch in raw):
                raise ValueError(f"{name} 非法（仅允许字母/数字/下划线）: {value}")

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

    @contextmanager
    def _transaction(self):
        """获取连接并自动 commit/rollback/release 的上下文管理器。"""
        active_pool, conn = self._acquire_conn()
        try:
            yield conn
            conn.commit()
        except Exception as ex:
            try:
                conn.rollback()
            except Exception:
                pass
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, conn)

    @contextmanager
    def _readonly(self):
        """获取只读连接的上下文管理器（不 commit）。"""
        active_pool, conn = self._acquire_conn()
        try:
            yield conn
        except Exception as ex:
            self._handle_db_exception(ex)
            raise
        finally:
            self._release_conn(active_pool, conn)

    def acquire_connection(self):
        return self._acquire_conn()

    def release_connection(self, active_pool, conn):
        self._release_conn(active_pool, conn)

    def _insert_records_with_cursor(self, cur, rec_type, lot_wafer_pairs, zip_name, zip_path):
        sql = f"""
        INSERT INTO {self.cfg.DB_TABLE}
        (type, lot_id, wafer_id, zip_name, zip_path)
        VALUES %s
        ON CONFLICT (type, lot_id, wafer_id) DO UPDATE SET
          created_at = NOW(),
          zip_name = EXCLUDED.zip_name,
          zip_path = EXCLUDED.zip_path
        """
        data = [
            (rec_type, lot_id, wafer_id, zip_name, zip_path)
            for lot_id, wafer_id in lot_wafer_pairs
        ]
        execute_values(cur, sql, data)

    def insert_records(self, rec_type, lot_wafer_pairs, zip_name, zip_path, conn=None):
        if conn is not None:
            with conn.cursor() as cur:
                self._insert_records_with_cursor(
                    cur, rec_type, lot_wafer_pairs, zip_name, zip_path
                )
            return

        with self._transaction() as owned_conn:
            with owned_conn.cursor() as cur:
                self._insert_records_with_cursor(
                    cur, rec_type, lot_wafer_pairs, zip_name, zip_path
                )

    def upsert_task_status(self, rec_type, zip_name, zip_path, status, error_msg=None, is_feedback=False):
        major_error = summarize_error_msg(error_msg, max_len=200)
        with self._transaction() as conn:
            with conn.cursor() as cur:
                sql = f"""
                INSERT INTO {self.cfg.DB_TASK_TABLE}
                (type, zip_name, zip_path, status, error_msg, is_feedback, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (zip_path) DO UPDATE SET
                  type = EXCLUDED.type,
                  zip_name = EXCLUDED.zip_name,
                  status = EXCLUDED.status,
                  error_msg = EXCLUDED.error_msg,
                  is_feedback = EXCLUDED.is_feedback,
                  updated_at = NOW()
                """
                cur.execute(sql, (rec_type, zip_name, zip_path, status, major_error, bool(is_feedback)))

    def _ensure_map_path_config_table(self):
        if self._map_table_ready:
            return

        with self._map_table_lock:
            if self._map_table_ready:
                return

            with self._transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {self.MAP_PATH_TABLE} (
                            id BIGSERIAL PRIMARY KEY,
                            sync_types VARCHAR(50) NOT NULL,
                            watch_dir VARCHAR(1000) NOT NULL,
                            target_dir VARCHAR(1000) NOT NULL,
                            file_suffixes VARCHAR(500) NOT NULL DEFAULT '',
                            last_scan DOUBLE PRECISION NOT NULL DEFAULT 0,
                            is_feedback BOOLEAN NOT NULL DEFAULT FALSE,
                            enabled BOOLEAN NOT NULL DEFAULT TRUE,
                            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            CONSTRAINT uq_map_path_config_watch UNIQUE (watch_dir)
                        )
                        """
                    )
                    cur.execute(
                        f"""
                        ALTER TABLE {self.MAP_PATH_TABLE}
                        ADD COLUMN IF NOT EXISTS file_suffixes VARCHAR(500) NOT NULL DEFAULT ''
                        """
                    )
                    cur.execute(
                        f"""
                        ALTER TABLE {self.MAP_PATH_TABLE}
                        ADD COLUMN IF NOT EXISTS is_feedback BOOLEAN NOT NULL DEFAULT FALSE
                        """
                    )
                    cur.execute(
                        f"""
                        ALTER TABLE {self.MAP_PATH_TABLE}
                        ADD COLUMN IF NOT EXISTS last_scan DOUBLE PRECISION NOT NULL DEFAULT 0
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE INDEX IF NOT EXISTS idx_map_path_config_enabled
                        ON {self.MAP_PATH_TABLE} (enabled, sync_types)
                        """
                    )
                    # Ensure zip_task_status has is_feedback column for direct lookup
                    cur.execute(
                        f"""
                        ALTER TABLE {self.cfg.DB_TASK_TABLE}
                        ADD COLUMN IF NOT EXISTS is_feedback BOOLEAN NOT NULL DEFAULT FALSE
                        """
                    )
            self._map_table_ready = True

    def _ensure_ha_tables(self):
        if self._ha_tables_ready:
            return

        with self._ha_tables_lock:
            if self._ha_tables_ready:
                return
            with self._transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {self.SERVICE_LEASE_TABLE} (
                            service_name VARCHAR(128) PRIMARY KEY,
                            owner_id VARCHAR(255) NOT NULL,
                            lease_until TIMESTAMP NOT NULL,
                            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                        )
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {self.TASK_QUEUE_TABLE} (
                            id BIGSERIAL PRIMARY KEY,
                            zip_path VARCHAR(1000) NOT NULL,
                            status VARCHAR(20) NOT NULL,
                            attempt INT NOT NULL DEFAULT 0,
                            next_retry_at TIMESTAMP NULL,
                            locked_by VARCHAR(255),
                            locked_until TIMESTAMP NULL,
                            last_error VARCHAR(1000),
                            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            CONSTRAINT uq_zip_task_queue_zip_path UNIQUE (zip_path),
                            CONSTRAINT ck_zip_task_queue_status CHECK (
                                status IN ('PENDING', 'RUNNING', 'RETRYING', 'SUCCESS', 'FAILED', 'SKIPPED')
                            )
                        )
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE INDEX IF NOT EXISTS idx_zip_task_queue_pick
                        ON {self.TASK_QUEUE_TABLE} (status, next_retry_at, updated_at)
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE INDEX IF NOT EXISTS idx_zip_task_queue_locked_until
                        ON {self.TASK_QUEUE_TABLE} (locked_until)
                        """
                    )
            self._ha_tables_ready = True

    def try_acquire_or_renew_lease(self, service_name: str, owner_id: str, lease_sec: int) -> bool:
        self._ensure_ha_tables()
        lease_sec = max(5, int(lease_sec or 30))
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.SERVICE_LEASE_TABLE} (service_name, owner_id, lease_until, updated_at)
                    VALUES (%s, %s, NOW() + (%s || ' seconds')::interval, NOW())
                    ON CONFLICT (service_name) DO UPDATE
                    SET owner_id = EXCLUDED.owner_id,
                        lease_until = EXCLUDED.lease_until,
                        updated_at = NOW()
                    WHERE {self.SERVICE_LEASE_TABLE}.owner_id = EXCLUDED.owner_id
                       OR {self.SERVICE_LEASE_TABLE}.lease_until < NOW()
                    """,
                    (service_name, owner_id, lease_sec),
                )
                cur.execute(
                    f"SELECT owner_id FROM {self.SERVICE_LEASE_TABLE} WHERE service_name = %s",
                    (service_name,),
                )
                row = cur.fetchone()
        return bool(row and row[0] == owner_id)

    def get_lease_owner(self, service_name: str):
        self._ensure_ha_tables()
        with self._readonly() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT owner_id
                    FROM {self.SERVICE_LEASE_TABLE}
                    WHERE service_name = %s
                      AND lease_until >= NOW()
                    """,
                    (service_name,),
                )
                row = cur.fetchone()
        return row[0] if row else None

    def enqueue_task(self, zip_path: Path | str):
        self._ensure_ha_tables()
        path_value = str(Path(zip_path))
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.TASK_QUEUE_TABLE}
                    (zip_path, status, attempt, next_retry_at, locked_by, locked_until, last_error, updated_at)
                    VALUES (%s, 'PENDING', 0, NOW(), NULL, NULL, NULL, NOW())
                    ON CONFLICT (zip_path) DO UPDATE
                    SET status = CASE
                        WHEN {self.TASK_QUEUE_TABLE}.status IN ('SUCCESS', 'FAILED', 'SKIPPED') THEN 'PENDING'
                        WHEN {self.TASK_QUEUE_TABLE}.status = 'RUNNING' THEN {self.TASK_QUEUE_TABLE}.status
                        ELSE 'PENDING'
                    END,
                    next_retry_at = CASE
                        WHEN {self.TASK_QUEUE_TABLE}.status = 'RUNNING'
                            THEN {self.TASK_QUEUE_TABLE}.next_retry_at
                        ELSE NOW()
                    END,
                    locked_by = CASE
                        WHEN {self.TASK_QUEUE_TABLE}.status = 'RUNNING'
                            THEN {self.TASK_QUEUE_TABLE}.locked_by
                        ELSE NULL
                    END,
                    locked_until = CASE
                        WHEN {self.TASK_QUEUE_TABLE}.status = 'RUNNING'
                            THEN {self.TASK_QUEUE_TABLE}.locked_until
                        ELSE NULL
                    END,
                    updated_at = NOW()
                    """,
                    (path_value,),
                )

    def claim_due_tasks(self, worker_id: str, batch_size=1, lock_sec=120):
        self._ensure_ha_tables()
        limit = max(1, int(batch_size or 1))
        lock_seconds = max(5, int(lock_sec or 120))
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    WITH picked AS (
                        SELECT id
                        FROM {self.TASK_QUEUE_TABLE}
                        WHERE status IN ('PENDING', 'RETRYING')
                          AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                          AND (locked_until IS NULL OR locked_until < NOW())
                        ORDER BY updated_at ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {self.TASK_QUEUE_TABLE} q
                    SET status = 'RUNNING',
                        locked_by = %s,
                        locked_until = NOW() + (%s || ' seconds')::interval,
                        updated_at = NOW()
                    FROM picked
                    WHERE q.id = picked.id
                    RETURNING q.zip_path
                    """,
                    (limit, worker_id, lock_seconds),
                )
                rows = cur.fetchall()
        return [Path(row[0]) for row in rows]

    def mark_task_success(self, zip_path: Path | str):
        self._ensure_ha_tables()
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.TASK_QUEUE_TABLE}
                    SET status = 'SUCCESS',
                        locked_by = NULL,
                        locked_until = NULL,
                        next_retry_at = NULL,
                        last_error = NULL,
                        updated_at = NOW()
                    WHERE zip_path = %s
                    """,
                    (str(Path(zip_path)),),
                )

    def mark_task_running(self, zip_path: Path | str, worker_id=""):
        self._ensure_ha_tables()
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.TASK_QUEUE_TABLE}
                    SET status = 'RUNNING',
                        locked_by = NULLIF(%s, ''),
                        locked_until = NOW() + (%s || ' seconds')::interval,
                        updated_at = NOW()
                    WHERE zip_path = %s
                    """,
                    (str(worker_id or ""), max(5, int(self.cfg.TASK_LOCK_SEC or 120)), str(Path(zip_path))),
                )

    def mark_task_retry(self, zip_path: Path | str, delay_sec: float, error_msg=""):
        self._ensure_ha_tables()
        delay = max(0.1, float(delay_sec or 0.1))
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.TASK_QUEUE_TABLE}
                    SET status = 'RETRYING',
                        attempt = attempt + 1,
                        next_retry_at = NOW() + (%s || ' seconds')::interval,
                        locked_by = NULL,
                        locked_until = NULL,
                        last_error = %s,
                        updated_at = NOW()
                    WHERE zip_path = %s
                    """,
                    (delay, summarize_error_msg(error_msg, max_len=200), str(Path(zip_path))),
                )

    def mark_task_failed(self, zip_path: Path | str, error_msg=""):
        self._ensure_ha_tables()
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.TASK_QUEUE_TABLE}
                    SET status = 'FAILED',
                        attempt = attempt + 1,
                        next_retry_at = NULL,
                        locked_by = NULL,
                        locked_until = NULL,
                        last_error = %s,
                        updated_at = NOW()
                    WHERE zip_path = %s
                    """,
                    (summarize_error_msg(error_msg, max_len=200), str(Path(zip_path))),
                )

    def mark_task_skipped(self, zip_path: Path | str, reason="不符合规则"):
        self._ensure_ha_tables()
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.TASK_QUEUE_TABLE}
                    SET status = 'SKIPPED',
                        next_retry_at = NULL,
                        locked_by = NULL,
                        locked_until = NULL,
                        last_error = %s,
                        updated_at = NOW()
                    WHERE zip_path = %s
                    """,
                    (summarize_error_msg(reason, max_len=200), str(Path(zip_path))),
                )

    def get_runtime_metrics(self):
        metrics = {
            "queue_depth": 0,
            "retrying_count": 0,
            "oldest_pending_sec": 0,
            "db_connect_failures": int(self._connect_failures or 0),
        }
        try:
            self._ensure_ha_tables()
            with self._readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                          COUNT(*) FILTER (WHERE status IN ('PENDING', 'RUNNING', 'RETRYING')) AS queue_depth,
                          COUNT(*) FILTER (WHERE status = 'RETRYING') AS retrying_count,
                          COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))::BIGINT, 0) AS oldest_pending_sec
                        FROM {self.TASK_QUEUE_TABLE}
                        WHERE status IN ('PENDING', 'RUNNING', 'RETRYING')
                        """
                    )
                    row = cur.fetchone()
                    if row:
                        metrics["queue_depth"] = int(row[0] or 0)
                        metrics["retrying_count"] = int(row[1] or 0)
                        metrics["oldest_pending_sec"] = int(row[2] or 0)
        except Exception:
            # 指标采集失败不影响主流程
            pass
        return metrics

    @staticmethod
    def _normalize_fs_path(path_value) -> str:
        return os.path.normcase(os.path.normpath(str(Path(path_value))))

    @staticmethod
    def _normalize_sync_type(sync_types: str) -> str:
        return (sync_types or "").strip().upper()

    @staticmethod
    def _normalize_file_suffixes(file_suffixes) -> list[str]:
        if file_suffixes is None:
            return []
        if isinstance(file_suffixes, str):
            raw_items = file_suffixes.split(",")
        elif isinstance(file_suffixes, (list, tuple, set)):
            raw_items = list(file_suffixes)
        else:
            return []

        normalized = []
        seen = set()
        for item in raw_items:
            if not isinstance(item, str):
                continue
            value = item.strip().lower()
            if not value:
                continue
            if not value.startswith("."):
                value = f".{value}"
            if value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    @classmethod
    def _serialize_file_suffixes(cls, file_suffixes) -> str:
        return ",".join(cls._normalize_file_suffixes(file_suffixes))

    @staticmethod
    def _normalize_stats_scope(scope) -> str:
        value = str(scope or "all").strip().lower()
        if value in {"feedback", "normal", "all"}:
            return value
        return "all"

    def _feedback_match_condition(self, alias: str) -> str:
        return f"""
        EXISTS (
          SELECT 1
          FROM {self.MAP_PATH_TABLE} m
          WHERE m.is_feedback = TRUE
            AND (
              LOWER({alias}.zip_path) = LOWER(m.watch_dir)
              OR (
                POSITION(LOWER(m.watch_dir) IN LOWER({alias}.zip_path)) = 1
                AND SUBSTRING(
                  {alias}.zip_path
                  FROM CHAR_LENGTH(m.watch_dir) + 1
                  FOR 1
                ) IN ('\\', '/')
              )
            )
        )
        """

    def _scope_where_sql(self, scope: str, alias: str) -> str:
        normalized = self._normalize_stats_scope(scope)
        feedback_match = self._feedback_match_condition(alias)
        if normalized == "feedback":
            return f"WHERE {feedback_match}"
        if normalized == "normal":
            return f"WHERE NOT ({feedback_match})"
        return ""

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
        with self._readonly() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, sync_types, watch_dir, target_dir, file_suffixes, last_scan, is_feedback, enabled, created_at, updated_at
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
                    "file_suffixes": self._normalize_file_suffixes(row[4]),
                    "last_scan": float(row[5] or 0),
                    "is_feedback": bool(row[6]),
                    "enabled": bool(row[7]),
                    "created_at": row[8].strftime("%Y-%m-%d %H:%M:%S") if row[8] else None,
                    "updated_at": row[9].strftime("%Y-%m-%d %H:%M:%S") if row[9] else None,
                }
            )
        return data

    def get_map_path_configs(self, only_enabled=False):
        return self._get_map_path_configs_cached(only_enabled=only_enabled)

    def create_map_path_config(
        self, sync_types, watch_dir, target_dir, enabled=True, is_feedback=False, file_suffixes=None
    ):
        normalized_sync_types = self._normalize_sync_type(sync_types)
        if not normalized_sync_types:
            raise ValueError("SYNC_TYPES 不能为空")

        normalized_watch = str(Path(watch_dir))
        normalized_target = str(Path(target_dir))
        normalized_suffixes = self._serialize_file_suffixes(file_suffixes)

        self._ensure_map_path_config_table()
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.MAP_PATH_TABLE}
                    (sync_types, watch_dir, target_dir, file_suffixes, is_feedback, enabled, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        normalized_sync_types,
                        normalized_watch,
                        normalized_target,
                        normalized_suffixes,
                        bool(is_feedback),
                        bool(enabled),
                    ),
                )
        self._invalidate_map_path_cache()

    def update_map_path_config(
        self,
        config_id,
        sync_types,
        watch_dir,
        target_dir,
        enabled=True,
        is_feedback=False,
        file_suffixes=None,
    ):
        normalized_sync_types = self._normalize_sync_type(sync_types)
        if not normalized_sync_types:
            raise ValueError("SYNC_TYPES 不能为空")

        normalized_watch = str(Path(watch_dir))
        normalized_target = str(Path(target_dir))
        normalized_suffixes = self._serialize_file_suffixes(file_suffixes)

        self._ensure_map_path_config_table()
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.MAP_PATH_TABLE}
                    SET sync_types = %s,
                        watch_dir = %s,
                        target_dir = %s,
                        file_suffixes = %s,
                        is_feedback = %s,
                        enabled = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        normalized_sync_types,
                        normalized_watch,
                        normalized_target,
                        normalized_suffixes,
                        bool(is_feedback),
                        bool(enabled),
                        int(config_id),
                    ),
                )
                if cur.rowcount == 0:
                    raise ValueError(f"配置不存在：id={config_id}")
        self._invalidate_map_path_cache()

    def upsert_map_path_config(
        self, sync_types, watch_dir, target_dir, enabled=True, is_feedback=False, file_suffixes=None
    ):
        # 兼容旧调用：按 watch_dir 幂等写入
        normalized_watch = str(Path(watch_dir))
        exists = None
        for row in self.get_map_path_configs(only_enabled=False):
            if str(Path(row.get("watch_dir") or "")) == normalized_watch:
                exists = row
                break
        if exists:
            return self.update_map_path_config(
                exists["id"],
                sync_types,
                watch_dir,
                target_dir,
                enabled,
                is_feedback,
                file_suffixes,
            )
        return self.create_map_path_config(
            sync_types, watch_dir, target_dir, enabled, is_feedback, file_suffixes
        )

    def delete_map_path_config(self, config_id):
        self._ensure_map_path_config_table()
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.MAP_PATH_TABLE} WHERE id = %s",
                    (int(config_id),),
                )
        self._invalidate_map_path_cache()

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

    def get_map_path_last_scan(self, watch_dir: Path) -> float:
        self._ensure_map_path_config_table()
        watch_dir_str = str(Path(watch_dir))
        with self._readonly() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT last_scan
                    FROM {self.MAP_PATH_TABLE}
                    WHERE LOWER(watch_dir) = LOWER(%s)
                    LIMIT 1
                    """,
                    (watch_dir_str,),
                )
                row = cur.fetchone()
        if not row:
            return 0.0
        try:
            return float(row[0] or 0)
        except Exception:
            return 0.0

    def update_map_path_last_scan(self, watch_dir: Path, last_scan_ts: float):
        self._ensure_map_path_config_table()
        watch_dir_str = str(Path(watch_dir))
        value = float(last_scan_ts or 0)
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.MAP_PATH_TABLE}
                    SET last_scan = %s,
                        updated_at = NOW()
                    WHERE LOWER(watch_dir) = LOWER(%s)
                    """,
                    (value, watch_dir_str),
                )
        self._invalidate_map_path_cache()

    def advance_map_path_last_scan(self, watch_dir: Path, candidate_ts: float):
        self._ensure_map_path_config_table()
        watch_dir_str = str(Path(watch_dir))
        value = float(candidate_ts or 0)
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.MAP_PATH_TABLE}
                    SET last_scan = GREATEST(last_scan, %s),
                        updated_at = NOW()
                    WHERE LOWER(watch_dir) = LOWER(%s)
                    """,
                    (value, watch_dir_str),
                )
        self._invalidate_map_path_cache()

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
                    "file_suffixes": self._normalize_file_suffixes(item.get("file_suffixes")),
                    "is_feedback": bool(item.get("is_feedback")),
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

    def _scope_where_sql_direct(self, scope: str, alias: str) -> str:
        """使用 is_feedback 列直接过滤，避免子查询。"""
        normalized = self._normalize_stats_scope(scope)
        if normalized == "feedback":
            return f"WHERE {alias}.is_feedback = TRUE"
        if normalized == "normal":
            return f"WHERE {alias}.is_feedback = FALSE"
        return ""

    def get_dashboard_metrics(self, scope="all"):
        scope = self._normalize_stats_scope(scope)
        with self._readonly() as conn:
            with conn.cursor() as cur:
                task_scope_where = self._scope_where_sql_direct(scope, "t")
                cur.execute(
                    f"""
                    SELECT
                      COUNT(*) AS total_tasks,
                      COUNT(*) FILTER (WHERE status='PENDING') AS pending_tasks,
                      COUNT(*) FILTER (WHERE status='SUCCESS') AS success_tasks,
                      COUNT(*) FILTER (WHERE status='FAILED') AS failed_tasks
                    FROM {self.cfg.DB_TASK_TABLE} t
                    {task_scope_where}
                    """
                )
                total_tasks, pending_tasks, success_tasks, failed_tasks = cur.fetchone()

                record_scope_where = self._scope_where_sql(scope, "r")
                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {self.cfg.DB_TABLE} r
                    {record_scope_where}
                    """
                )
                (total_records,) = cur.fetchone()

                cur.execute(
                    f"""
                    SELECT type,
                           COUNT(*) AS task_count,
                           MAX(updated_at) AS last_update
                    FROM {self.cfg.DB_TASK_TABLE} t
                    {task_scope_where}
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
                "feedback_tasks": (total_tasks or 0)
                if scope == "feedback"
                else 0,
                "normal_tasks": (total_tasks or 0)
                if scope == "normal"
                else 0,
                "total_records": total_records or 0,
            },
            "by_type": by_type,
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "scope": scope,
        }

    def get_recent_tasks(self, page=1, page_size=20, keyword="", scope="all"):
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        offset = (page - 1) * page_size
        kw = (keyword or "").strip()
        scope = self._normalize_stats_scope(scope)

        with self._readonly() as conn:
            with conn.cursor() as cur:
                where_parts = []
                params = []
                if kw:
                    like_kw = f"%{kw}%"
                    where_parts.append(
                        "(t.type ILIKE %s OR t.zip_name ILIKE %s OR t.status ILIKE %s OR COALESCE(t.error_msg, '') ILIKE %s OR t.zip_path ILIKE %s)"
                    )
                    params.extend([like_kw, like_kw, like_kw, like_kw, like_kw])
                scope_where = self._scope_where_sql_direct(scope, "t")
                if scope_where:
                    where_parts.append(scope_where.replace("WHERE ", "", 1).strip())
                where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
                base_params = tuple(params)

                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {self.cfg.DB_TASK_TABLE} t
                    {where_sql}
                    """,
                    base_params,
                )
                (total,) = cur.fetchone()

                cur.execute(
                    f"""
                    SELECT
                      t.type,
                      t.zip_name,
                      t.status,
                      t.error_msg,
                      t.zip_path,
                      t.updated_at,
                      t.is_feedback
                    FROM {self.cfg.DB_TASK_TABLE} t
                    {where_sql}
                    ORDER BY t.updated_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    base_params + (page_size, offset),
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
                "is_feedback": bool(row[6]),
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
            "scope": scope,
        }

    def get_recent_records(self, page=1, page_size=20, keyword="", scope="all"):
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 20), 100))
        offset = (page - 1) * page_size
        kw = (keyword or "").strip()
        scope = self._normalize_stats_scope(scope)

        with self._readonly() as conn:
            with conn.cursor() as cur:
                where_parts = []
                params = []
                if kw:
                    like_kw = f"%{kw}%"
                    where_parts.append(
                        "(r.type ILIKE %s OR r.lot_id ILIKE %s OR r.wafer_id ILIKE %s OR r.zip_name ILIKE %s OR r.zip_path ILIKE %s)"
                    )
                    params.extend([like_kw, like_kw, like_kw, like_kw, like_kw])
                scope_where = self._scope_where_sql(scope, "r")
                if scope_where:
                    where_parts.append(scope_where.replace("WHERE ", "", 1).strip())
                where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
                base_params = tuple(params)

                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {self.cfg.DB_TABLE} r
                    {where_sql}
                    """,
                    base_params,
                )
                (total,) = cur.fetchone()
                cur.execute(
                    f"""
                    SELECT r.type, r.lot_id, r.wafer_id, r.zip_name, r.zip_path, r.created_at
                    FROM {self.cfg.DB_TABLE} r
                    {where_sql}
                    ORDER BY r.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    base_params + (page_size, offset),
                )
                rows = cur.fetchall()
        items = [
            {
                "type": row[0],
                "lot_id": row[1],
                "wafer_id": row[2],
                "zip_name": row[3],
                "zip_path": row[4],
                "created_at": row[5].strftime("%Y-%m-%d %H:%M:%S")
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
            "scope": scope,
        }

    def check_health(self):
        try:
            with self._readonly() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return True, None
        except Exception as ex:
            return False, summarize_error_msg(ex)

    def close(self):
        try:
            self._reset_pool()
        except Exception as ex:
            logger.exception("关闭数据库连接池时出错：%s", ex)


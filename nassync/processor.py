import errno
import logging
import os
import re
import shutil
import threading
import time
import zipfile
from pathlib import Path

from psycopg2 import DatabaseError, InterfaceError, OperationalError

from .config import Config
from .errors import RetryableProcessError, summarize_error_msg

logger = logging.getLogger("watcher")


class Processor:
    def __init__(self, cfg: Config, pg):
        self.cfg = cfg
        self.pg = pg
        self.processing = set()
        self.lock = threading.Lock()
        self.sync_types = set(cfg.SYNC_TYPES)

    def _resolve_route(self, path: Path):
        if path.suffix.lower() != ".zip":
            return None

        if "BACKUP" in [p.upper() for p in path.parts]:
            return None

        if self.pg is not None and hasattr(self.pg, "match_map_path_config"):
            row = self.pg.match_map_path_config(path)
            if row:
                rec_type = (row.get("sync_type") or "").strip().upper()
                if not rec_type:
                    return None
                return {
                    "rec_type": rec_type,
                    "watch_dir": Path(row["watch_dir"]),
                    "target_dir": Path(row["target_dir"]),
                    "is_feedback": bool(row.get("is_feedback")),
                    "source": "map_path_config",
                }
            return None

        try:
            rel = path.relative_to(self.cfg.WATCH_DIR)
        except Exception:
            return None

        if len(rel.parts) < 3:
            return None
        if rel.parts[-2].upper() != "WAFER_MAP":
            return None

        rec_type = rel.parts[-3].upper()
        if self.sync_types and rec_type not in self.sync_types:
            return None

        return {
            "rec_type": rec_type,
            "watch_dir": self.cfg.WATCH_DIR,
            "target_dir": self.cfg.TARGET_DIR / rel.parent,
            "is_feedback": False,
            "source": "config_json",
        }

    def is_valid(self, path: Path):
        return self._resolve_route(path) is not None

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
        route = self._resolve_route(path)
        if not route:
            logger.info("跳过（不符合规则或未配置 map_path_config）：%s", path)
            return None

        rec_type = route["rec_type"]
        key = str(path)
        with self.lock:
            if key in self.processing:
                return None
            self.processing.add(key)

        max_attempts = max(1, int(self.cfg.PROCESS_RETRY_TIMES))
        max_attempt_idx = max_attempts - 1
        last_error = None

        try:
            if self.pg is None:
                raise RuntimeError("未配置数据库客户端，无法处理文件")

            for attempt in range(max_attempts):
                try:
                    self.pg.upsert_task_status(rec_type, path.name, str(path), "PENDING")
                    self._process(path)
                    self.pg.upsert_task_status(rec_type, path.name, str(path), "SUCCESS")
                    return None
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
                        deferred_delay = self._retry_delay(max_attempt_idx)
                        logger.warning(
                            "处理失败（可恢复），将延迟重试：%s, %ss 后重入队列, err=%s",
                            path,
                            f"{deferred_delay:.2f}",
                            summarize_error_msg(ex),
                        )
                        return deferred_delay

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
            self._mark_failed(rec_type, path, failure_reason)
            return None
        except Exception as ex:
            logger.exception("处理失败：%s, err=%s", path, ex)
            if self._is_retryable_error(ex):
                deferred_delay = self._retry_delay(max_attempt_idx)
                logger.warning(
                    "处理失败（外层可恢复），将延迟重试：%s, %ss 后重入队列, err=%s",
                    path,
                    f"{deferred_delay:.2f}",
                    summarize_error_msg(ex),
                )
                return deferred_delay

            self._mark_failed(rec_type, path, f"{type(ex).__name__}: {ex}")
            return None
        finally:
            with self.lock:
                self.processing.discard(key)

    def _mark_failed(self, rec_type: str, path: Path, reason: str):
        if self.pg is None:
            logger.warning("数据库客户端不可用，FAILED 状态未写入：%s, err=%s", path, reason)
            return
        try:
            self.pg.upsert_task_status(rec_type, path.name, str(path), "FAILED", reason)
        except Exception:
            logger.exception("写入 FAILED 状态失败：%s", path)

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

    @staticmethod
    def _same_path(a: Path, b: Path) -> bool:
        return os.path.normcase(os.path.normpath(str(a))) == os.path.normcase(
            os.path.normpath(str(b))
        )

    def _rollback_file_changes(
        self,
        source_zip_path: Path,
        target_zip_path: Path,
        moved_zip: bool,
    ):
        errors = []

        if moved_zip:
            try:
                if target_zip_path.exists():
                    source_zip_path.parent.mkdir(parents=True, exist_ok=True)
                    if source_zip_path.exists():
                        source_zip_path.unlink()
                    shutil.move(str(target_zip_path), str(source_zip_path))
            except Exception as ex:
                errors.append(
                    f"回滚 ZIP 失败: {target_zip_path} -> {source_zip_path} ({ex})"
                )

        if errors:
            raise RuntimeError("; ".join(errors))

    def _process(self, path: Path):
        route = self._resolve_route(path)
        if not route:
            logger.info("跳过（未命中 map_path_config）：%s", path)
            return True

        rec_type = route["rec_type"]
        target_dir = Path(route["target_dir"])
        is_feedback = bool(route.get("is_feedback"))

        if not path.exists():
            logger.info("跳过（文件不存在，可能已处理）：%s", path)
            return True

        logger.info(
            "处理：%s，SYNC_TYPES=%s，WATCH_DIR=%s，TARGET_DIR=%s",
            path,
            rec_type,
            route["watch_dir"],
            target_dir,
        )

        if not self.wait_stable(path):
            raise RetryableProcessError("文件未稳定")

        source_zip_path = path
        target_dir.mkdir(parents=True, exist_ok=True)
        target_zip_path = target_dir / path.name
        backup_dir = source_zip_path.parent / "BACKUP"
        backup_zip_path = backup_dir / source_zip_path.name

        active_pool = None
        conn = None
        moved_zip = False

        try:
            active_pool, conn = self.pg.acquire_connection()
            working_zip_path = source_zip_path

            backup_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(source_zip_path), str(backup_zip_path))

            if not self._same_path(source_zip_path, target_zip_path):
                if target_zip_path.exists():
                    target_zip_path.unlink()
                shutil.move(str(source_zip_path), str(target_zip_path))
                moved_zip = True
                working_zip_path = target_zip_path

            lot_wafer_pairs = self.scan_zip(working_zip_path)

            if is_feedback:
                logger.info("命中回传配置，跳过 zip_record 入库：%s", working_zip_path)
            else:
                self.pg.insert_records(
                    rec_type,
                    lot_wafer_pairs,
                    working_zip_path.name,
                    str(working_zip_path),
                    conn=conn,
                )
            conn.commit()

            logger.info(
                "完成：%s -> %s，备份=%s，入库类型=%s，是否回传=%s",
                source_zip_path,
                working_zip_path,
                backup_zip_path,
                rec_type,
                "是" if is_feedback else "否",
            )
            return True
        except Exception as ex:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass

            try:
                self._rollback_file_changes(
                    source_zip_path,
                    target_zip_path,
                    moved_zip,
                )
            except Exception as rollback_ex:
                raise RetryableProcessError(
                    f"处理失败且回滚异常: {summarize_error_msg(ex)}; rollback={rollback_ex}"
                ) from ex

            raise
        finally:
            if self.pg is not None and active_pool is not None and conn is not None:
                self.pg.release_connection(active_pool, conn)

    def scan_zip(self, zip_path):
        lot_wafer_pairs = []
        map_name_pattern = re.compile(r"^([A-Za-z0-9]{6})-([A-Za-z0-9]{2})$")
        zip_prefix = Path(zip_path).stem.split("-", 1)[0].upper()
        has_map = False

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

                has_map = True
                lot_wafer_pairs.append((lot_id, wafer_id))

            if not has_map:
                raise ValueError(f"ZIP 内未找到 MAP 文件：{Path(zip_path).name}")

            return sorted(set(lot_wafer_pairs))

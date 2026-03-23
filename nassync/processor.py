import errno
import logging
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
            logger.info("跳过（不符合规则）：%s", path)
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
            if self.pg is None:
                raise RuntimeError("未配置数据库客户端，无法处理文件")
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
            logger.exception("处理失败：%s, err=%s", path, ex)
            if self.pg is not None:
                try:
                    self.pg.upsert_task_status(
                        rec_type,
                        path.name,
                        str(path),
                        "FAILED",
                        f"{type(ex).__name__}: {ex}",
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
        if not path.exists():
            logger.info("跳过（文件不存在，可能已处理）：%s", path)
            return True

        logger.info("处理：%s", path)

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

        logger.info("完成：%s -> %s，MAP 已解压到 %s", path, backup_zip_path, target_dir)
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

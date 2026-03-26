import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse


@dataclass(frozen=True)
class Config:
    WATCH_DIR: Path = Path(r"C:\Users\Administrator\Desktop\nassync\A\BP\WAFER_MAP")
    TARGET_DIR: Path = Path(r"C:\Users\Administrator\Desktop\nassync\B\BP\WAFER_MAP")

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
    return Path(__file__).resolve().parents[1]


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


def _is_smb_url(raw: str) -> bool:
    lowered = raw.lower()
    return lowered.startswith("smb://") or lowered.startswith("cifs://")


def _smb_url_to_unc(raw: str) -> str:
    parsed = urlparse(raw)
    host = parsed.hostname
    parts = [unquote(item) for item in parsed.path.split("/") if item]

    if not host or len(parts) < 1:
        raise ValueError(f"NAS 地址格式错误，需为 smb://server/share[/dir]：{raw}")
    if parsed.port is not None:
        raise ValueError(f"NAS 地址不支持端口号，请移除 :{parsed.port}：{raw}")

    return "\\\\" + "\\".join([host, *parts])


def normalize_dir_path(raw_value, cfg_path: Path | None = None) -> Path:
    if isinstance(raw_value, Path):
        raw = str(raw_value)
    elif isinstance(raw_value, str):
        raw = raw_value.strip()
    else:
        raise TypeError(f"目录路径类型错误：{type(raw_value).__name__}")

    if not raw:
        raise ValueError("目录路径不能为空")

    raw = os.path.expandvars(os.path.expanduser(raw))
    if _is_smb_url(raw):
        raw = _smb_url_to_unc(raw)

    path = Path(raw)
    if not path.is_absolute() and cfg_path is not None:
        path = cfg_path.parent / path

    return Path(os.path.normpath(str(path)))


def is_nas_path(path: Path) -> bool:
    if not isinstance(path, Path):
        return False

    raw = str(path)
    if raw.startswith("\\\\") or raw.startswith("//"):
        return True

    drive = getattr(path, "drive", "")
    return isinstance(drive, str) and (
        drive.startswith("\\\\") or drive.startswith("//")
    )


def load_config() -> Config:
    default_cfg = Config()
    cfg_path = Path(os.getenv("NASSYNC_CONFIG", _app_base_dir() / "config.json"))
    raw = {}
    config_loaded = False

    if not cfg_path.exists():
        print(f"[配置] 未找到配置文件，使用默认配置: {cfg_path}")
    else:
        try:
            with cfg_path.open("r", encoding="utf-8-sig") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                raw = loaded
                config_loaded = True
            else:
                print("[配置] 配置文件格式错误（需为 JSON 对象），使用默认配置")
        except Exception as ex:
            print(f"[配置] 读取配置失败，使用默认配置: {ex}")

    global _SYNC_TYPES_RAW
    _SYNC_TYPES_RAW = raw.get("SYNC_TYPES") if raw else None

    merged = dict(default_cfg.__dict__)
    if raw:
        merged.update({k: v for k, v in raw.items() if k in merged})

    env_db_password = os.getenv("NASSYNC_DB_PASSWORD")
    if env_db_password is not None and env_db_password != "":
        merged["DB_PASSWORD"] = env_db_password
        print("[配置] 已从环境变量 NASSYNC_DB_PASSWORD 覆盖数据库密码")

    try:
        merged["WATCH_DIR"] = normalize_dir_path(merged["WATCH_DIR"], cfg_path=cfg_path)
        merged["TARGET_DIR"] = normalize_dir_path(
            merged["TARGET_DIR"], cfg_path=cfg_path
        )
        merged["LOG_DIR"] = normalize_dir_path(merged["LOG_DIR"], cfg_path=cfg_path)
    except Exception as ex:
        print(f"[配置] 目录配置解析失败: {ex}")
        raise SystemExit(1)

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

    def check_dir(name: str, path: Path, must_exist: bool = False):
        if not isinstance(path, Path):
            report(f"{name} 不是有效路径: {path}")
            return
        if must_exist and not path.exists():
            report(f"{name} 路径不存在: {path}")
            return
        if path.exists() and not path.is_dir():
            report(f"{name} 不是目录: {path}")

    # 目录暂时不可用时允许启动，运行期由重试机制恢复。
    check_dir("WATCH_DIR", cfg.WATCH_DIR, must_exist=False)
    check_dir("TARGET_DIR", cfg.TARGET_DIR, must_exist=False)
    check_dir("LOG_DIR", cfg.LOG_DIR, must_exist=False)

    if not (1 <= cfg.WEB_PORT <= 65535):
        report(f"WEB_PORT 必须在 1-65535 之间: {cfg.WEB_PORT}")

    if cfg.FILE_STABLE_CHECK_TIMES < 1:
        report("FILE_STABLE_CHECK_TIMES 必须大于 0")

    if cfg.FILE_STABLE_CHECK_INTERVAL_SEC <= 0:
        report("FILE_STABLE_CHECK_INTERVAL_SEC 必须大于 0")

    if cfg.PROCESS_RETRY_TIMES < 0:
        report("PROCESS_RETRY_TIMES 不能为负数")

    if cfg.PROCESS_RETRY_INTERVAL_SEC <= 0:
        report("PROCESS_RETRY_INTERVAL_SEC 必须大于 0")

    if cfg.PROCESS_RETRY_BACKOFF_MAX_SEC <= 0:
        report("PROCESS_RETRY_BACKOFF_MAX_SEC 必须大于 0")

    if cfg.PROCESS_RETRY_BACKOFF_MAX_SEC < cfg.PROCESS_RETRY_INTERVAL_SEC:
        report("PROCESS_RETRY_BACKOFF_MAX_SEC 不能小于 PROCESS_RETRY_INTERVAL_SEC")

    if cfg.TASK_QUEUE_MAX_SIZE < 1:
        report("TASK_QUEUE_MAX_SIZE 必须大于 0")

    if cfg.EVENT_DEDUP_WINDOW_SEC < 0:
        report("EVENT_DEDUP_WINDOW_SEC 不能为负数")

    if cfg.DASHBOARD_CACHE_TTL_SEC < 0:
        report("DASHBOARD_CACHE_TTL_SEC 不能为负数")

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
        print("[配置] 启动前校验失败：")
        for err in errors:
            print(f"  - {err}")
        raise SystemExit(1)

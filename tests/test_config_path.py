from pathlib import Path
import json
import os
import sys

import pytest
from watchdog.observers.polling import PollingObserver

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nassync.app import _create_observer
from nassync.config import is_nas_path, load_config, normalize_dir_path, validate_config


def test_normalize_dir_path_supports_relative_path(tmp_path: Path):
    cfg_path = tmp_path / "conf" / "config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)

    path = normalize_dir_path("watch", cfg_path=cfg_path)

    assert path == cfg_path.parent / "watch"


def test_normalize_dir_path_supports_unc_path():
    path = normalize_dir_path(r"\\NAS01\fab\A")

    assert is_nas_path(path) is True
    assert str(path).replace("/", "\\").startswith(r"\\NAS01\fab\A")


def test_normalize_dir_path_supports_smb_url():
    path = normalize_dir_path("smb://nas01/fab/A")

    assert is_nas_path(path) is True
    assert str(path).replace("/", "\\").startswith(r"\\nas01\fab\A")


def test_normalize_dir_path_rejects_empty_value():
    with pytest.raises(ValueError):
        normalize_dir_path("   ")


def test_create_observer_uses_polling_for_nas_path():
    observer = _create_observer(Path(r"\\NAS01\fab\A"))

    assert isinstance(observer, PollingObserver)


def test_create_observer_uses_native_for_local_path(tmp_path: Path):
    observer = _create_observer(tmp_path / "A")

    assert isinstance(observer, PollingObserver) is False


def test_load_config_supports_utf8_bom_and_bool_flags(tmp_path: Path, monkeypatch):
    cfg_path = tmp_path / "config-bom.json"
    cfg_path.write_text(
        json.dumps(
            {
                "WATCH_DIR": str(tmp_path / "watch"),
                "TARGET_DIR": str(tmp_path / "target"),
                "LOG_DIR": str(tmp_path / "logs"),
                "CHECK_ZIP_MAP_SAME_PREFIX": False,
                "CHECK_MAP_FILENAME_FORMAT": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8-sig",
    )

    monkeypatch.setenv("NASSYNC_CONFIG", str(cfg_path))
    cfg = load_config()

    assert cfg.CHECK_ZIP_MAP_SAME_PREFIX is False
    assert cfg.CHECK_MAP_FILENAME_FORMAT is False


def test_validate_config_rejects_invalid_identifier(tmp_path: Path):
    cfg_path = tmp_path / "config-invalid-id.json"
    cfg_path.write_text(
        json.dumps(
            {
                "WATCH_DIR": str(tmp_path / "watch"),
                "TARGET_DIR": str(tmp_path / "target"),
                "LOG_DIR": str(tmp_path / "logs"),
                "DB_TABLE": "zip-record",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.environ["NASSYNC_CONFIG"] = str(cfg_path)
    cfg = load_config()

    with pytest.raises(SystemExit):
        validate_config(cfg)


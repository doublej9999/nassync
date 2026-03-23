import json
import os
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_RUNTIME_DIR = Path(__file__).resolve().parent / ".runtime"
_WATCH_DIR = _RUNTIME_DIR / "A"
_TARGET_DIR = _RUNTIME_DIR / "B"
_LOG_DIR = _RUNTIME_DIR / "logs"
_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
_WATCH_DIR.mkdir(parents=True, exist_ok=True)
_TARGET_DIR.mkdir(parents=True, exist_ok=True)
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_CONFIG_PATH = _RUNTIME_DIR / "config.test.json"
if not _CONFIG_PATH.exists():
    _CONFIG_PATH.write_text(
        json.dumps(
            {
                "WATCH_DIR": str(_WATCH_DIR),
                "TARGET_DIR": str(_TARGET_DIR),
                "LOG_DIR": str(_LOG_DIR),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
os.environ.setdefault("NASSYNC_CONFIG", str(_CONFIG_PATH))

from main import Config, Processor


def make_processor(tmp_path: Path) -> Processor:
    watch_dir = tmp_path / "A"
    target_dir = tmp_path / "B"
    watch_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    cfg = Config(
        WATCH_DIR=watch_dir,
        TARGET_DIR=target_dir,
        LOG_DIR=tmp_path / "logs",
    )
    return Processor(cfg, pg=None)


def test_is_valid_accepts_expected_path(tmp_path: Path):
    p = make_processor(tmp_path)
    candidate = tmp_path / "A" / "BP" / "WAFER_MAP" / "A00001.zip"
    assert p.is_valid(candidate) is True


def test_is_valid_rejects_backup_path(tmp_path: Path):
    p = make_processor(tmp_path)
    candidate = tmp_path / "A" / "BP" / "WAFER_MAP" / "BACKUP" / "A00001.zip"
    assert p.is_valid(candidate) is False


def test_scan_zip_raises_when_no_map(tmp_path: Path):
    p = make_processor(tmp_path)
    zip_path = tmp_path / "A" / "BP" / "WAFER_MAP" / "ABC123.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("note.txt", "dummy")

    with pytest.raises(ValueError, match="未找到 MAP"):
        p.scan_zip(zip_path, tmp_path / "B" / "BP" / "WAFER_MAP")


def test_scan_zip_extracts_map_and_returns_pairs(tmp_path: Path):
    p = make_processor(tmp_path)
    zip_path = tmp_path / "A" / "BP" / "WAFER_MAP" / "ABC123.zip"
    target_dir = tmp_path / "B" / "BP" / "WAFER_MAP"
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("nested/ABC123-01.MAP", "MAP-DATA")

    pairs = p.scan_zip(zip_path, target_dir)

    assert pairs == [("ABC123", "01")]
    extracted = target_dir / "ABC123-01.MAP"
    assert extracted.exists() is True
    assert extracted.read_text(encoding="utf-8") == "MAP-DATA"

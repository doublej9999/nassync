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


class DummyPg:
    def __init__(self):
        self.status_calls = []

    def upsert_task_status(self, rec_type, zip_name, zip_path, status, error_msg=None):
        self.status_calls.append(
            {
                "type": rec_type,
                "zip_name": zip_name,
                "zip_path": zip_path,
                "status": status,
                "error_msg": error_msg,
            }
        )

    def insert_records(self, rec_type, lot_wafer_pairs, zip_name, zip_path):
        return None


def test_process_retryable_error_returns_delay_and_no_failed(tmp_path: Path):
    watch_dir = tmp_path / "A"
    target_dir = tmp_path / "B"
    watch_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    zip_path = watch_dir / "BP" / "WAFER_MAP" / "ABC123.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    zip_path.write_bytes(b"dummy")

    cfg = Config(
        WATCH_DIR=watch_dir,
        TARGET_DIR=target_dir,
        LOG_DIR=tmp_path / "logs",
        PROCESS_RETRY_TIMES=1,
        PROCESS_RETRY_INTERVAL_SEC=0.2,
        PROCESS_RETRY_BACKOFF_MAX_SEC=0.2,
    )
    pg = DummyPg()
    processor = Processor(cfg, pg=pg)

    def fail_process(_):
        raise PermissionError("folder temporarily unavailable")

    processor._process = fail_process
    delay = processor.process(zip_path)

    assert delay == pytest.approx(0.2, rel=0.01)
    statuses = [item["status"] for item in pg.status_calls]
    assert "PENDING" in statuses
    assert "FAILED" not in statuses


def test_process_non_retryable_error_marks_failed(tmp_path: Path):
    watch_dir = tmp_path / "A"
    target_dir = tmp_path / "B"
    watch_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    zip_path = watch_dir / "BP" / "WAFER_MAP" / "ABC123.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    zip_path.write_bytes(b"dummy")

    cfg = Config(
        WATCH_DIR=watch_dir,
        TARGET_DIR=target_dir,
        LOG_DIR=tmp_path / "logs",
        PROCESS_RETRY_TIMES=1,
    )
    pg = DummyPg()
    processor = Processor(cfg, pg=pg)

    def fail_process(_):
        raise ValueError("invalid map content")

    processor._process = fail_process
    delay = processor.process(zip_path)

    assert delay is None
    statuses = [item["status"] for item in pg.status_calls]
    assert statuses[-1] == "FAILED"

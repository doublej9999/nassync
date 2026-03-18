import json
import logging
import logging.handlers
import os
import shutil
import tempfile
import threading
import time
import zipfile
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from psycopg2 import pool
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

    INITIAL_SCAN: bool = True

    WEB_HOST: str = "0.0.0.0"
    WEB_PORT: int = 8080


CONFIG = Config()


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
                ON CONFLICT (type, lot_id, wafer_id) DO NOTHING
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
                cur.execute(sql, (rec_type, zip_name, zip_path, status, error_msg))
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

    def get_recent_tasks(self, limit=20):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT type, zip_name, status, error_msg, zip_path, updated_at
                    FROM {self.cfg.DB_TASK_TABLE}
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
            return [
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
        finally:
            self.pool.putconn(conn)

    def get_recent_records(self, limit=20):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT type, lot_id, wafer_id, zip_name, created_at
                    FROM {self.cfg.DB_TABLE}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
            return [
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
        finally:
            self.pool.putconn(conn)


# =========================
# 核心处理
# =========================
class Processor:
    def __init__(self, cfg: Config, pg: PgClient):
        self.cfg = cfg
        self.pg = pg
        self.processing = set()
        self.lock = threading.Lock()

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

        key = str(path)
        with self.lock:
            if key in self.processing:
                return
            self.processing.add(key)

        try:
            self.pg.upsert_task_status(rec_type, path.name, str(path), "PENDING")
            for i in range(self.cfg.PROCESS_RETRY_TIMES):
                done = self._process(path)
                if done:
                    self.pg.upsert_task_status(rec_type, path.name, str(path), "SUCCESS")
                    break
                if i < self.cfg.PROCESS_RETRY_TIMES - 1:
                    logger.info(
                        f"文件未稳定，准备重试：{path} ({i + 1}/{self.cfg.PROCESS_RETRY_TIMES})"
                    )
                    time.sleep(self.cfg.PROCESS_RETRY_INTERVAL_SEC)
            else:
                logger.warning(f"处理放弃（重试后仍未稳定）：{path}")
                self.pg.upsert_task_status(
                    rec_type,
                    path.name,
                    str(path),
                    "FAILED",
                    "文件重试后仍未稳定",
                )
        except Exception as ex:
            # 避免事件回调线程因未捕获异常中断，记录后继续监听
            logger.exception(f"处理失败：{path}, err={ex}")
            self.pg.upsert_task_status(
                rec_type, path.name, str(path), "FAILED", str(ex)[:1000]
            )
        finally:
            with self.lock:
                self.processing.remove(key)

    def _process(self, path: Path):
        # 文件已被其他并发事件处理并移走时，视为当前事件无需再处理
        if not path.exists():
            logger.info(f"跳过（文件不存在，可能已处理）：{path}")
            return True

        logger.info(f"处理：{path}")

        if not self.wait_stable(path):
            return False

        rel = path.relative_to(self.cfg.WATCH_DIR)

        rec_type = rel.parts[-3]
        unzip_dir = self.cfg.TARGET_DIR / rel.parent
        backup_dir = path.parent / "BACKUP"

        lot_wafer_pairs = self.extract(path, unzip_dir)

        self.pg.insert_records(rec_type, lot_wafer_pairs, path.name, str(path))

        backup_dir.mkdir(exist_ok=True)
        shutil.move(str(path), str(backup_dir / path.name))

        logger.info(f"完成：{path}")
        return True

    def extract(self, zip_path, target_dir):
        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(zip_path) as z:
                z.extractall(tmp)

            lot_wafer_pairs = []

            for root, _, files in os.walk(tmp):
                for f in files:
                    if Path(f).suffix.lower() != ".map":
                        continue

                    stem = Path(f).stem
                    parts = stem.split("-", 2)
                    if len(parts) < 2:
                        logger.warning(f"跳过（MAP 文件名不符合 lot-wafer 规则）：{f}")
                        continue

                    lot_id = parts[0].strip().upper()
                    wafer_id = parts[1].strip().upper()
                    if not lot_id or not wafer_id:
                        logger.warning(f"跳过（MAP 文件名 lot/wafer 为空）：{f}")
                        continue

                    lot_wafer_pairs.append((lot_id, wafer_id))

                    src = Path(root) / f
                    dst = target_dir / f
                    target_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)

            return sorted(set(lot_wafer_pairs))


# =========================
# 监听
# =========================
class Handler(FileSystemEventHandler):
    def __init__(self, p):
        self.p = p

    def on_created(self, e):
        if not e.is_directory:
            self.p.process(Path(e.src_path))

    def on_modified(self, e):
        if not e.is_directory:
            self.p.process(Path(e.src_path))

    def on_moved(self, e):
        if not e.is_directory:
            # 兼容“先写临时文件再重命名为 .zip”的上传方式
            self.p.process(Path(e.dest_path))


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
      <table>
        <thead><tr><th>时间</th><th>类型</th><th>压缩包</th><th>状态</th><th>错误信息</th></tr></thead>
        <tbody id=\"task_tbody\"></tbody>
      </table>
    </div>

    <div class=\"section\">
      <h2>最近入库记录</h2>
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
    function esc(v) {
      if (v === null || v === undefined) return '';
      return String(v).replace(/[&<>\"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[m]));
    }

    async function loadData() {
      const resp = await fetch('/api/dashboard');
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

      document.getElementById('task_tbody').innerHTML = (data.recent_tasks || []).map(x => `
        <tr>
          <td>${esc(x.updated_at || '-')}</td>
          <td>${esc(x.type)}</td>
          <td class=\"mono\">${esc(x.zip_name)}</td>
          <td><span class=\"status ${esc(x.status)}\">${esc(x.status)}</span></td>
          <td>${esc(x.error_msg || '-')}</td>
        </tr>
      `).join('');

      document.getElementById('record_tbody').innerHTML = (data.recent_records || []).map(x => `
        <tr>
          <td>${esc(x.created_at || '-')}</td>
          <td>${esc(x.type)}</td>
          <td>${esc(x.lot_id)}</td>
          <td>${esc(x.wafer_id)}</td>
          <td class=\"mono\">${esc(x.zip_name)}</td>
        </tr>
      `).join('');

      document.getElementById('refresh_time').textContent = '更新时间：' + esc(data.generated_at || '-');
    }

    async function tick() {
      try {
        await loadData();
      } catch (e) {
        console.error('加载失败', e);
      }
    }

    tick();
    setInterval(tick, 5000);
  </script>
</body>
</html>
"""


def create_dashboard_handler(pg: PgClient):
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

            if parsed.path == "/api/dashboard":
                qs = parse_qs(parsed.query or "")
                try:
                    task_limit = max(1, min(int((qs.get("task_limit") or ["20"])[0]), 100))
                    record_limit = max(
                        1, min(int((qs.get("record_limit") or ["20"])[0]), 100)
                    )
                except ValueError:
                    self._send_json(
                        {"error": "task_limit 和 record_limit 必须是数字"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                data = pg.get_dashboard_metrics()
                data["recent_tasks"] = pg.get_recent_tasks(task_limit)
                data["recent_records"] = pg.get_recent_records(record_limit)
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

    if CONFIG.INITIAL_SCAN:
        for f in CONFIG.WATCH_DIR.rglob("*"):
            if f.is_file() and f.suffix.lower() == ".zip":
                p.process(f)

    obs = Observer()
    obs.schedule(Handler(p), str(CONFIG.WATCH_DIR), recursive=True)
    obs.start()
    logger.info("启动监听...")

    web_server = ThreadingHTTPServer(
        (CONFIG.WEB_HOST, CONFIG.WEB_PORT), create_dashboard_handler(pg)
    )
    web_thread = threading.Thread(target=web_server.serve_forever, daemon=True)
    web_thread.start()
    logger.info(f"Web 监控已启动：http://{CONFIG.WEB_HOST}:{CONFIG.WEB_PORT}/dashboard")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        web_server.shutdown()
        web_server.server_close()
        obs.stop()

    obs.join()


if __name__ == "__main__":
    main()

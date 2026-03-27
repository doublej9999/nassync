import json
import logging
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .config import Config, normalize_dir_path
from .db import PgClient
from .watcher import ServiceLifecycle

logger = logging.getLogger("watcher")
_TEMPLATE_PATH = Path(__file__).resolve().parent / "templates" / "dashboard.html"
DASHBOARD_HTML = _TEMPLATE_PATH.read_text(encoding="utf-8")


def create_dashboard_handler(pg: PgClient, lifecycle: ServiceLifecycle, cfg: Config):
    cache_lock = threading.Lock()
    cache_ttl_sec = max(0.0, float(cfg.DASHBOARD_CACHE_TTL_SEC))
    cache_key = None
    cache_payload = None
    cache_expire_at = 0.0

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

        def _read_json_body(self):
            try:
                content_len = int(self.headers.get("Content-Length") or "0")
            except ValueError:
                return None
            if content_len <= 0:
                return None
            raw = self.rfile.read(content_len)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                return None
            if not isinstance(payload, dict):
                return None
            return payload

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path in ("/", "/dashboard"):
                self._send_html(DASHBOARD_HTML)
                return

            if parsed.path == "/api/map-path-config":
                try:
                    rows = pg.get_map_path_configs(only_enabled=False)
                except Exception as ex:
                    self._send_json(
                        {"error": f"读取 map_path_config 失败：{ex}"},
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                    return
                self._send_json({"items": rows})
                return

            if parsed.path == "/healthz":
                watcher_state = "running" if lifecycle.watcher_healthy() else "stopped"
                web_state = "running" if lifecycle.web_healthy() else "stopped"
                db_ok, db_msg = pg.check_health()
                payload = {
                    "status": "ok" if db_ok else "degraded",
                    "watcher": watcher_state,
                    "web": web_state,
                    "db": "ok" if db_ok else "error",
                    "db_error": db_msg,
                    "shutting_down": lifecycle.is_shutting_down(),
                }
                self._send_json(payload)
                return

            if parsed.path == "/api/dashboard":
                nonlocal cache_key, cache_payload, cache_expire_at
                qs = parse_qs(parsed.query or "")
                try:
                    task_page = max(1, int((qs.get("task_page") or ["1"])[0]))
                    task_page_size = max(
                        1, min(int((qs.get("task_page_size") or ["20"])[0]), 100)
                    )
                    task_q = (qs.get("task_q") or [""])[0]
                    stats_scope = (qs.get("stats_scope") or ["normal"])[0]

                    record_page = max(1, int((qs.get("record_page") or ["1"])[0]))
                    record_page_size = max(
                        1, min(int((qs.get("record_page_size") or ["20"])[0]), 100)
                    )
                    record_q = (qs.get("record_q") or [""])[0]
                except ValueError:
                    self._send_json(
                        {"error": "分页参数必须是数字"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                current_key = (
                    task_page,
                    task_page_size,
                    task_q,
                    stats_scope,
                    record_page,
                    record_page_size,
                    record_q,
                )
                now = time.monotonic()

                with cache_lock:
                    if (
                        cache_key == current_key
                        and cache_payload is not None
                        and now < cache_expire_at
                    ):
                        self._send_json(cache_payload)
                        return

                try:
                    data = pg.get_dashboard_metrics(scope=stats_scope)
                    data["recent_tasks"] = pg.get_recent_tasks(
                        page=task_page,
                        page_size=task_page_size,
                        keyword=task_q,
                        scope=stats_scope,
                    )
                    data["recent_records"] = pg.get_recent_records(
                        page=record_page,
                        page_size=record_page_size,
                        keyword=record_q,
                        scope=stats_scope,
                    )
                except Exception as ex:
                    logger.warning("查询看板数据失败：%s", ex)
                    with cache_lock:
                        stale = cache_payload
                    if stale is not None:
                        degraded_payload = dict(stale)
                        degraded_payload["degraded"] = True
                        degraded_payload["error"] = "数据库暂不可用，返回缓存数据"
                        self._send_json(
                            degraded_payload,
                            status=HTTPStatus.SERVICE_UNAVAILABLE,
                        )
                        return
                    self._send_json(
                        {"error": "数据库暂不可用，请稍后重试"},
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                    return

                if cache_ttl_sec > 0:
                    with cache_lock:
                        cache_key = current_key
                        cache_payload = data
                        cache_expire_at = now + cache_ttl_sec

                self._send_json(data)
                return

            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

        def do_POST(self):
            parsed = urlparse(self.path)

            if parsed.path == "/api/map-path-config":
                payload = self._read_json_body()
                if not payload:
                    self._send_json(
                        {"error": "请求体必须是 JSON 对象"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                sync_types = (payload.get("sync_types") or "").strip().upper()
                watch_dir_raw = payload.get("watch_dir")
                target_dir_raw = payload.get("target_dir")
                enabled = payload.get("enabled", True)
                is_feedback = payload.get("is_feedback", False)
                config_id = payload.get("id")

                if not sync_types:
                    self._send_json(
                        {"error": "SYNC_TYPES 不能为空"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                try:
                    watch_dir = normalize_dir_path(watch_dir_raw)
                    target_dir = normalize_dir_path(target_dir_raw)
                except Exception as ex:
                    self._send_json(
                        {"error": f"路径格式不合法：{ex}"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                try:
                    if config_id in (None, ""):
                        pg.create_map_path_config(
                            sync_types=sync_types,
                            watch_dir=str(watch_dir),
                            target_dir=str(target_dir),
                            enabled=bool(enabled),
                            is_feedback=bool(is_feedback),
                        )
                    else:
                        pg.update_map_path_config(
                            config_id=int(config_id),
                            sync_types=sync_types,
                            watch_dir=str(watch_dir),
                            target_dir=str(target_dir),
                            enabled=bool(enabled),
                            is_feedback=bool(is_feedback),
                        )
                    lifecycle.request_reload()
                    rows = pg.get_map_path_configs(only_enabled=False)
                except Exception as ex:
                    self._send_json(
                        {"error": f"保存 map_path_config 失败：{ex}"},
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                    return

                self._send_json({"ok": True, "items": rows})
                return

            if parsed.path == "/api/map-path-config/delete":
                payload = self._read_json_body()
                if not payload or "id" not in payload:
                    self._send_json(
                        {"error": "必须提供 id"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    pg.delete_map_path_config(payload.get("id"))
                    lifecycle.request_reload()
                    rows = pg.get_map_path_configs(only_enabled=False)
                except Exception as ex:
                    self._send_json(
                        {"error": f"删除 map_path_config 失败：{ex}"},
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                    return
                self._send_json({"ok": True, "items": rows})
                return

            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

        def log_message(self, fmt, *args):
            logger.info("WEB %s - %s", self.address_string(), fmt % args)

    return DashboardHandler

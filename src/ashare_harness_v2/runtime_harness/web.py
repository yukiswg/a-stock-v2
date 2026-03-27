from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from ..advice_harness import answer_user_query, discover_top_ideas
from ..utils import load_json, write_text


def serve_dashboard(*, homepage_dir: str | Path, host: str = "127.0.0.1", port: int = 8765) -> None:
    _serve(mode="dashboard", root=Path(homepage_dir), config=None, host=host, port=port)


def serve_api(*, config: dict[str, Any], homepage_dir: str | Path, host: str = "127.0.0.1", port: int = 8766) -> None:
    _serve(mode="api", root=Path(homepage_dir), config=config, host=host, port=port)


def _serve(*, mode: str, root: Path, config: dict[str, Any] | None, host: str, port: int) -> None:
    handler = make_handler(mode=mode, root=root, config=config)
    server = ThreadingHTTPServer((host, port), handler)
    try:
        server.serve_forever()
    finally:  # pragma: no cover - server teardown is environment-specific
        server.server_close()


def make_handler(*, mode: str, root: Path, config: dict[str, Any] | None):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            homepage = load_json(root / "latest_homepage.json", default={}) or {}
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            if mode == "dashboard":
                if parsed.path not in {"/", "/index.html"}:
                    self.send_error(404, "Not Found")
                    return
                html_path = root / "index.html"
                if not html_path.exists():
                    write_text(html_path, "<html><body><p>Homepage not generated.</p></body></html>")
                payload = html_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if parsed.path == "/api/homepage":
                return self._json_response(homepage)
            if parsed.path == "/api/status":
                return self._json_response(
                    {
                        "ok": True,
                        "as_of": homepage.get("as_of"),
                        "today_action": homepage.get("today_action"),
                        "alert_count": len(homepage.get("latest_alerts", [])),
                        "price_count": len(homepage.get("current_prices", [])),
                    }
                )
            if parsed.path == "/api/advice":
                question = first_param(params, "question", "q")
                if not question:
                    return self._json_response({"ok": False, "error": "Missing query parameter `question`."}, status=400)
                if config is None:
                    return self._json_response({"ok": False, "error": "API advice mode is not configured."}, status=500)
                as_of = first_param(params, "as_of") or str(homepage.get("as_of") or "")
                if not as_of:
                    return self._json_response({"ok": False, "error": "Missing `as_of` and no homepage snapshot available."}, status=400)
                refresh = (first_param(params, "refresh") or "").lower() in {"1", "true", "yes"}
                payload = answer_user_query(config, question=question, as_of=as_of, write_output=False, allow_live_enrich=refresh)
                return self._json_response(payload)
            if parsed.path == "/api/discovery":
                if config is None:
                    return self._json_response({"ok": False, "error": "API discovery mode is not configured."}, status=500)
                as_of = first_param(params, "as_of") or str(homepage.get("as_of") or "")
                if not as_of:
                    return self._json_response({"ok": False, "error": "Missing `as_of` and no homepage snapshot available."}, status=400)
                limit_raw = first_param(params, "limit")
                limit = int(limit_raw) if limit_raw and limit_raw.isdigit() else 5
                payload = discover_top_ideas(config, as_of=as_of, limit=limit, write_output=False)
                return self._json_response(payload)
            self.send_error(404, "Not Found")

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def _json_response(self, payload: dict[str, Any], *, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def first_param(params: dict[str, list[str]], *keys: str) -> str | None:
    for key in keys:
        values = params.get(key)
        if values and values[0].strip():
            return values[0].strip()
    return None

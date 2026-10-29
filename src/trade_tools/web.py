"""Authenticated web application for the live portfolio monitoring dashboard."""

from __future__ import annotations

import hmac
import logging
import os
import tempfile
from pathlib import Path

from flask import Flask, Response, abort, jsonify, request

from trade_tools.cli import generate_monitoring_dashboard

logger = logging.getLogger(__name__)


def create_app(
    username: str | None = None,
    password: str | None = None,
    config_path: str | Path | None = None,
) -> Flask:
    """Create the dashboard app; authentication credentials are mandatory."""
    expected_username = username if username is not None else os.getenv("DASHBOARD_USERNAME", "")
    expected_password = password if password is not None else os.getenv("DASHBOARD_PASSWORD", "")
    if not expected_username or len(expected_password) < 16:
        raise RuntimeError(
            "Set DASHBOARD_USERNAME and a DASHBOARD_PASSWORD of at least 16 characters."
        )

    research_config = Path(
        config_path or os.getenv("RESEARCH_CONFIG_PATH", "config/research.json")
    )
    app = Flask(__name__)

    @app.before_request
    def require_authentication() -> Response | None:
        if request.endpoint == "health":
            return None

        credentials = request.authorization
        provided_username = credentials.username if credentials else ""
        provided_password = credentials.password if credentials else ""
        username_matches = hmac.compare_digest(provided_username, expected_username)
        password_matches = hmac.compare_digest(provided_password, expected_password)
        if username_matches & password_matches:
            return None
        return Response(
            "Authentication required.\n",
            status=401,
            headers={"WWW-Authenticate": 'Basic realm="Trade Tools", charset="UTF-8"'},
            mimetype="text/plain",
        )

    @app.after_request
    def set_private_response_headers(response: Response) -> Response:
        response.headers["Cache-Control"] = "no-store"
        response.headers["Pragma"] = "no-cache"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'"
        )
        return response

    @app.get("/healthz")
    def health() -> Response:
        return jsonify(status="ok")

    @app.get("/")
    def dashboard() -> Response:
        try:
            with tempfile.TemporaryDirectory(prefix="trade-tools-dashboard-") as directory:
                report_path = generate_monitoring_dashboard(
                    str(research_config),
                    Path(directory) / "index.html",
                )
                page = report_path.read_text(encoding="utf-8")
            page = page.replace(
                "</head>",
                '<meta http-equiv="refresh" content="300"></head>',
                1,
            )
        except Exception:
            logger.exception("Unable to refresh the portfolio monitoring dashboard.")
            abort(503, description="Dashboard data is temporarily unavailable.")
        return Response(page, mimetype="text/html")

    return app

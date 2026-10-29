from __future__ import annotations

from pathlib import Path

import pytest

from trade_tools import web


def test_app_requires_strong_password():
    with pytest.raises(RuntimeError, match="DASHBOARD_PASSWORD"):
        web.create_app(username="investor", password="short")


def test_dashboard_requires_auth_but_health_is_public():
    app = web.create_app(username="investor", password="correct-horse-battery")
    client = app.test_client()

    dashboard = client.get("/")
    health = client.get("/healthz")

    assert dashboard.status_code == 401
    assert dashboard.headers["WWW-Authenticate"].startswith("Basic")
    assert health.status_code == 200
    assert health.json == {"status": "ok"}


def test_authenticated_dashboard_refreshes_and_disables_caching(monkeypatch):
    def render(config_path: str, output: Path) -> Path:
        assert config_path == "config/research.json"
        output.write_text(
            "<!doctype html><html><head><title>Test</title></head>"
            "<body><h1>Live portfolio dashboard</h1></body></html>",
            encoding="utf-8",
        )
        return output

    monkeypatch.setattr(web, "generate_monitoring_dashboard", render)
    app = web.create_app(username="investor", password="correct-horse-battery")

    response = app.test_client().get(
        "/",
        headers={"Authorization": "Basic aW52ZXN0b3I6Y29ycmVjdC1ob3JzZS1iYXR0ZXJ5"},
    )

    assert response.status_code == 200
    assert b"Live portfolio dashboard" in response.data
    assert b'http-equiv="refresh" content="300"' in response.data
    assert response.mimetype == "text/html"
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["X-Frame-Options"] == "DENY"


def test_dashboard_failure_returns_generic_unavailable_response(monkeypatch):
    def fail_render(config_path: str, output: Path) -> Path:
        raise RuntimeError("private database connection detail")

    monkeypatch.setattr(web, "generate_monitoring_dashboard", fail_render)
    app = web.create_app(username="investor", password="correct-horse-battery")

    response = app.test_client().get(
        "/",
        headers={"Authorization": "Basic aW52ZXN0b3I6Y29ycmVjdC1ob3JzZS1iYXR0ZXJ5"},
    )

    assert response.status_code == 503
    assert b"private database connection detail" not in response.data

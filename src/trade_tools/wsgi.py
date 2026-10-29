"""Gunicorn entry point; application startup requires configured credentials."""

from trade_tools.web import create_app

app = create_app()

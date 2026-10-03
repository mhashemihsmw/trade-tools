# Environment Setup & Initialization — Reference

## Purpose

This document records the steps required to set up a working environment for
`trade-tools` from scratch: local development setup, the cloud PostgreSQL
database, and the scheduled GitHub Actions ingestion job. Follow this whenever
setting up a new machine, recovering from a broken environment, or explaining
the setup to someone else.

This is a companion reference to
[market_data_infrastructure_agent_instructions_final.md](./market_data_infrastructure_agent_instructions_final.md),
which defines the architecture and specification. This document covers *how to
stand the environment up*, not the architecture itself.

---

## Architecture Overview

- **Database**: PostgreSQL, cloud-hosted (currently [Neon](https://neon.tech),
  free tier). Not run locally via Docker — the database must be reachable both
  from your machine and from GitHub Actions, so a cloud-hosted instance is used
  as the single source of truth for all environments.
- **Application code**: Python, managed with Poetry, runs locally via the
  `trade-tools` CLI ([cli.py](../src/trade_tools/cli.py)) for manual use.
- **Scheduled ingestion**: GitHub Actions workflow
  ([daily_ingestion.yml](../.github/workflows/daily_ingestion.yml)) runs daily,
  independent of whether your own computer is on. This is the free
  always-on alternative to a paid VM/server.

---

## Prerequisites

- Python 3.11+ (managed here via `pyenv`)
- [Poetry](https://python-poetry.org/) for dependency and packaging management
- A free cloud PostgreSQL database (Neon, Supabase, Railway, etc.)
- A GitHub repository (for the scheduled Actions workflow)

---

## 1. Install Poetry (if not already installed)

```bash
python3 -m pip install poetry
poetry --version
```

---

## 2. Install Project Dependencies

From the repository root:

```bash
poetry install
```

This creates a Poetry-managed virtualenv and installs all runtime and dev
dependencies defined in [pyproject.toml](../pyproject.toml)
(`yfinance`, `pandas`, `sqlalchemy`, `psycopg2-binary`, `python-dotenv`,
`click`, plus `pytest`/`pytest-mock`/`pytest-cov` in the `dev` group).

---

## 3. Set Up the Cloud PostgreSQL Database (Neon)

1. Sign up at [neon.tech](https://neon.tech) (free tier, no credit card
   required) and create a new project.
2. Copy the connection string from the Neon dashboard. It looks like:
   ```text
   postgresql://user:password@host/dbname?sslmode=require&channel_binding=require
   ```
3. Configure your local environment:
   ```bash
   cp .env.example .env
   ```
   Then edit `.env` and set `DATABASE_URL` to the connection string above.

Any other standard PostgreSQL provider (Supabase, Railway, RDS, DigitalOcean
Managed Databases, etc.) works the same way — just use its connection string.

> Note: A local Docker Postgres container was used earlier in this project's
> setup, but was replaced with a cloud database. GitHub Actions runs in the
> cloud and cannot reach a database on `localhost`, so the database must be
> reachable over the internet for the scheduled job to work. Docker is not
> required anywhere in the current setup.

---

## 4. Initialize Database Schema and Seed Initial Assets

```bash
poetry run trade-tools init-db
```

This creates the `assets`, `daily_prices`, and `hourly_prices` tables and
seeds the 80 current assets defined in
[initial_universe.py](../src/trade_tools/market_data/initial_universe.py).
This step is idempotent — re-running it seeds 0 new assets if already present.

---

## 5. Run Initial Market Data Ingestion

```bash
poetry run trade-tools ingest
```

Downloads ~20 years of daily history and the maximum available hourly history
(up to ~2 years) from Yahoo Finance for all active assets, and updates asset
metadata (name, currency, exchange, sector, country). This can take
several minutes for the full 80-asset universe on first run (longer on a free
cloud database tier than on local Docker, due to network latency).
Subsequent runs are incremental (only new/updated observations are fetched).

---

## 6. Verify the Setup

```bash
poetry run trade-tools asset list
```

Or connect with a SQL client (e.g. DBeaver) using the same connection details
as `DATABASE_URL`, and check:

```sql
SELECT (SELECT count(*) FROM assets) AS assets,
       (SELECT count(*) FROM daily_prices) AS daily_prices,
       (SELECT count(*) FROM hourly_prices) AS hourly_prices;
```

Expected result after a successful full initialization: 99 assets, with
metadata populated, and non-zero counts in `daily_prices` and `hourly_prices`.

---

## 7. Run Tests

Tests use an in-memory SQLite database and fully mock `yfinance`, so they do
not require a live database connection or network access:

```bash
poetry run pytest
```

---

## 8. Set Up the Scheduled GitHub Actions Job

The daily ingestion job runs automatically via GitHub Actions
([daily_ingestion.yml](../.github/workflows/daily_ingestion.yml)), independent
of whether your local machine is on. To enable it:

1. Push the repository to GitHub.
2. In the repo: **Settings → Secrets and variables → Actions → New repository
   secret**.
   - Name: `DATABASE_URL`
   - Value: the same connection string used in your local `.env`.
3. The workflow runs daily at 06:00 UTC (`cron: "0 6 * * *"`), and can also be
   triggered manually from the **Actions** tab (`workflow_dispatch`).

### Cost expectations

- With ~45–150 assets and **daily** (not hourly-triggered) incremental runs,
  each run takes roughly 5–10 minutes.
- Public repositories get unlimited free GitHub Actions minutes.
- Private repositories on the Free plan get 2,000 free minutes/month — a daily
  run at ~10 minutes uses ~300 minutes/month, well within the free quota.
- Running the workflow much more frequently (e.g. hourly instead of daily)
  or scaling to hundreds of assets would use significantly more minutes and
  could exceed the free quota on a private repo.

---

## Quick Reference — Full Setup From Scratch

```bash
# Local setup
poetry install
cp .env.example .env
# edit .env: set DATABASE_URL to your Neon (or other cloud Postgres) connection string

poetry run trade-tools init-db
poetry run trade-tools ingest
poetry run pytest

# GitHub Actions setup (once repo is pushed to GitHub)
# Settings -> Secrets and variables -> Actions -> New repository secret
#   Name: DATABASE_URL
#   Value: <same connection string as in .env>
```

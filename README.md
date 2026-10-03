# Trade Tools — Market Data Infrastructure

## 1. Project Purpose

`trade-tools` is a modular Python toolkit designed for personal long-term investment research, price data ingestion, feature engineering, trade strategies, portfolio optimization, backtesting, and rebalancing.

This initial phase establishes the core **Market Data Infrastructure**:
- Storing asset master data and metadata in PostgreSQL (or SQLite for development/testing).
- Automated ingestion of historical daily (~20 years) and hourly price observations from Yahoo Finance.
- Idempotent upserts and scheduled daily incremental updates.
- Modular architecture allowing future strategy, portfolio, and ML/feature components to consume stored database data cleanly.

---

## 2. Architecture

```text
PostgreSQL / SQLite Database: assets
        │
        ▼
MarketDataIngestion
        │
        ├──► YahooFinanceClient
        │        │
        │        └──► Yahoo Finance API (yfinance)
        │
        ▼
PostgreSQL / SQLite Database
        │
        ├── daily_prices  (20+ years history)
        └── hourly_prices (Max available history)
```

The system is structured under `src/trade_tools/` into distinct modules:

```text
src/trade_tools/
├── config.py              # Centralized environment configuration
├── cli.py                 # Click CLI interface
├── db/                    # Database ORM, engine, session, and models
│   ├── base.py
│   ├── session.py
│   └── models/
│       ├── asset.py       # Asset master table ORM
│       └── price.py       # DailyPrice & HourlyPrice ORM models
├── market_data/           # Market data ingestion domain logic
│   ├── clients/
│   │   └── yfinance_client.py # Yahoo Finance client wrapper
│   ├── initial_universe.py    # Seed universe definition (80 current assets)
│   ├── validation.py          # Price validation and cleaning
│   └── ingestion.py           # Orchestrator for metadata and price updates
├── jobs/                  # Job entry points (init_db, ingest_market_data)
├── strategies/            # (Extension namespace for trade strategies)
├── portfolio/             # (Extension namespace for portfolio optimization)
└── features/              # (Extension namespace for feature engineering / ML)
```

---

## 3. Database Schema

### Asset Master Table (`assets`)
| Column | Type | Constraints / Description |
|---|---|---|
| `id` | Integer | Primary Key, Autoincrement |
| `ticker` | String(32) | Unique, Index, Non-null |
| `type` | String(32) | Controlled asset type (`equity`, `etf`, `reit`, `fixed_income_proxy`, `commodity`, `crypto`, `index`, `fx`) |
| `name` | String(255) | Nullable asset name |
| `currency` | String(16) | Nullable currency code (e.g. USD, EUR) |
| `exchange` | String(64) | Nullable exchange name |
| `country` | String(64) | Nullable country |
| `sector` | String(128) | Nullable sector |
| `style_box_category` | String(32) | Equity/REIT style box (e.g. `Large Blend`), else `N/A` |
| `bond_matrix_category` | String(32) | Fixed-income duration/quality (e.g. `Intermediate High-Quality`), else `N/A` |
| `data_source` | String(64) | Default `'yahoo_finance'` |
| `active` | Boolean | Non-null, default `True` (controls ingestion) |
| `created_at` | DateTime(tz=True) | Non-null UTC timestamp |
| `updated_at` | DateTime(tz=True) | Non-null UTC timestamp |
| `deactivated_at` | DateTime(tz=True) | Nullable UTC timestamp |
| `metadata_updated_at` | DateTime(tz=True) | Nullable UTC timestamp |

### Daily Prices Table (`daily_prices`)
| Column | Type | Constraints / Description |
|---|---|---|
| `id` | Integer | Primary Key |
| `asset_id` | Integer | Foreign Key (`assets.id`, CASCADE) |
| `timestamp` | DateTime(tz=True) | Market observation timestamp in UTC |
| `open` | Float | Nullable |
| `high` | Float | Nullable |
| `low` | Float | Nullable |
| `close` | Float | Nullable |
| `adjusted_close` | Float | Nullable |
| `volume` | Float | Nullable |
| `created_at` | DateTime(tz=True) | UTC insertion time |

- **Unique Constraint**: `(asset_id, timestamp)`
- **Index**: `(asset_id, timestamp)`

### Hourly Prices Table (`hourly_prices`)
| Column | Type | Constraints / Description |
|---|---|---|
| `id` | Integer | Primary Key |
| `asset_id` | Integer | Foreign Key (`assets.id`, CASCADE) |
| `timestamp` | DateTime(tz=True) | Market observation timestamp in UTC |
| `open` | Float | Nullable |
| `high` | Float | Nullable |
| `low` | Float | Nullable |
| `close` | Float | Nullable |
| `adjusted_close` | Float | Nullable |
| `volume` | Float | Nullable |
| `created_at` | DateTime(tz=True) | UTC insertion time |

- **Unique Constraint**: `(asset_id, timestamp)`
- **Index**: `(asset_id, timestamp)`

---

## 4. Configuration

Configuration is managed via environment variables or a `.env` file in the root directory:

| Environment Variable | Default Value | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/trade_tools` | SQLAlchemy connection string |
| `HISTORICAL_START_DATE` | `2005-01-01` | Default start date for initial daily price ingestion |
| `OVERLAP_DAYS_DAILY` | `5` | Days of overlap during incremental daily ingestion |
| `OVERLAP_DAYS_HOURLY` | `2` | Days of overlap during incremental hourly ingestion |
| `LOG_LEVEL` | `INFO` | Logging level |

---

## 5. Installation

Prerequisites: Python 3.11+, Poetry, and a PostgreSQL database.

```bash
# Clone the repository
cd trade-tools

# Install dependencies and package with Poetry
poetry install

# Copy the example environment file and adjust if needed
cp .env.example .env
```

### Database Hosting

This project uses a cloud-hosted PostgreSQL database rather than a local Docker
container, since the database must be reachable both from your machine and from
the scheduled GitHub Actions workflow. A free option is [Neon](https://neon.tech)
(serverless Postgres, generous free tier, no credit card required):

1. Sign up at [neon.tech](https://neon.tech) and create a new project.
2. Copy the connection string it gives you (format:
   `postgresql://user:password@host/dbname?sslmode=require`).
3. Paste it into `.env` as `DATABASE_URL`.

Any standard PostgreSQL provider works the same way (Supabase, Railway, RDS,
DigitalOcean Managed Databases, etc.) — just set `DATABASE_URL` accordingly.

---

---

## 6. Database Initialization

Run the database initialization command to create tables and seed the default asset universe:

```bash
poetry run trade-tools init-db
```

Alternatively, run as a python module:

```bash
poetry run python -m trade_tools.jobs.init_db
```

---

## 7. Initial Asset Universe

The database seed universe currently contains 80 assets (the original 45 plus
35 additional European UCITS ETF/ETC listings):
- **Equities**: AAPL, MSFT, NVDA, AMZN, GOOGL, META, BRK-B, JPM, JNJ, XOM, SAP.DE, ASML.AS, NESN.SW
- **ETFs**: SPY, QQQ, VTI, VT, VXUS, EFA, EEM, IWM, AGG, BND, VWCE.DE, EUNL.DE, IUSQ.DE, SPYI.DE, SXR8.DE, VUAA.DE, SXRV.DE, EQQB.DE, IS3N.DE, VFEA.DE, EXSA.DE, EXS1.DE, IQQJ.DE, EUNK.DE, IUSN.DE, ZPRS.DE, CUSS.L, QDVE.DE, EXV3.DE, WITS.L, QDVG.DE, EXV4.DE, QDVH.DE, EXV1.DE, QDVF.DE, IQQH.DE, 2B7D.DE, VAPX.L, EUNJ.DE, ICGA.DE, ASHR.L, 36BZ.DE, KWBE.DE, VAGF.DE
- **REITs**: VNQ, O, PLD, AMT
- **Fixed Income Proxies**: ^TNX, ^FVX, ^IRX
- **Commodities / ETCs**: GLD, SLV, DBC, PPFB.DE
- **Crypto**: BTC-USD, ETH-USD
- **Indices**: ^GSPC, ^IXIC, ^STOXX50E, ^GDAXI, ^FTSE, ^N225
- **FX**: EURUSD=X, GBPUSD=X, JPY=X, CHF=X

The requested iShares MSCI AC Asia ex Japan and Xtrackers MSCI AC Asia ex Japan
products are not seeded yet: the exact Yahoo result for the former is
non-UCITS, while the supported Xetra listing for the latter is ESG-screened.
They are withheld pending confirmation of the intended ISIN/share class.

---

## 8. Adding, Deactivating, and Reactivating Assets

Manage assets using the CLI:

### List Assets
```bash
poetry run trade-tools asset list
```

### Add a New Asset
```bash
poetry run trade-tools asset add --ticker TSLA --type equity
```

### Deactivate an Asset
Deactivating an asset stops regular ingestion while retaining historical prices for backtesting:
```bash
poetry run trade-tools asset deactivate --ticker AAPL
```

### Reactivate an Asset
```bash
poetry run trade-tools asset reactivate --ticker AAPL
```

---

## 9. Running Ingestion Manually

Run manual ingestion for active assets:

```bash
# Ingest both daily and hourly data
poetry run trade-tools ingest

# Ingest daily data only
poetry run trade-tools ingest --no-hourly

# Backfill only selected active assets
poetry run trade-tools backfill --ticker VWCE.DE --ticker EUNL.DE
```

---

## 10. Daily Job Execution

The scheduling mechanism is external to the ingestion logic, so it can run via
cron, a CI/CD scheduler, or any other trigger. This project ships with a
**GitHub Actions workflow** ([.github/workflows/daily_ingestion.yml](./.github/workflows/daily_ingestion.yml))
that runs daily and does not require your own computer to be on:

1. Push this repository to GitHub.
2. In the GitHub repo, go to **Settings → Secrets and variables → Actions →
   New repository secret**, and add:
   - Name: `DATABASE_URL`
   - Value: your cloud PostgreSQL connection string (same as in your local `.env`)
3. The workflow runs automatically every day at 06:00 UTC. You can also trigger
   it manually from the **Actions** tab → *Daily Market Data Ingestion* →
   **Run workflow**.

Alternatively, to run on your own always-on machine/server via cron:

```bash
# Example crontab entry running daily at 01:00 UTC
0 1 * * * cd /path/to/trade-tools && /usr/local/bin/poetry run python -m trade_tools.jobs.ingest_market_data >> /var/log/trade_tools.log 2>&1
```

Note: a cron job on your own laptop only runs while that machine is powered on,
awake, and network-connected — it will not run while the computer is off or
asleep. GitHub Actions runs independently of your machine.

---

## 11. Basic Troubleshooting

1. **Database Connection Errors**:
   Verify `DATABASE_URL` is set correctly and that your cloud PostgreSQL
   provider (e.g. Neon) shows the database as active/reachable in its dashboard.
2. **GitHub Actions failures**:
   Check the **Actions** tab in GitHub for the failed run's logs. The most
   common cause is a missing or incorrect `DATABASE_URL` repository secret.
3. **Missing Price Data**:
   Yahoo Finance may return empty results for invalid or delisted tickers. Check logs for warnings.
4. **Running Tests**:
   Run unit tests locally (all Yahoo Finance calls are mocked in tests, no live DB required):
   ```bash
   poetry run pytest
   ```

> Style-box and bond-matrix classifications are maintained manually in
> `src/trade_tools/market_data/classification.py` and applied to the DB on every
> `init-db`/`ingest` run. Unlisted tickers default to `N/A`.

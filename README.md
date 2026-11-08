# Trade Tools

A personal toolkit for a **monthly momentum strategy on European-listed ETFs**:
daily price data from Yahoo Finance, a monthly decision (which funds to hold and
with what weights), a backtest of that decision over history, and a dashboard of
your real portfolio built from the trades you record.

No broker connection: the tool suggests trades, you place them, and you record
what you actually got.

## How the strategy works (short version)

Every month-end, for each candidate fund in [config/universe.csv](config/universe.csv):

1. **Eligible?** ≥ 252 trading days of history and a recent price.
2. **One listing per index.** Several ETFs track e.g. the S&P 500; keep the preferred one.
3. **Score** = momentum ÷ volatility, with momentum = (12·r1 + 4·r3 + 2·r6 + r12) / 4.
4. **Filters**: momentum > 0 and price above its 10-month moving average.
5. **Pick** the top 5 by score. Fewer passing → the empty slots stay in cash.
6. **Weights**: inverse volatility, max 30% each, scaled down if the portfolio's
   estimated volatility exceeds 10%.

Details, formulas and the reasoning behind each step: [docs/strategy.md](docs/strategy.md).
Every number above is a setting in [config/strategy.toml](config/strategy.toml).

## Monthly routine

```bash
poetry run trade-tools decide --contribution 500   # targets + suggested trades
# ... place the trades with your broker ...
poetry run trade-tools tx add --type deposit --date 2026-12-01 --quantity 500
poetry run trade-tools tx add --type buy --date 2026-12-01 --ticker VWCE.DE \
    --quantity 4 --price 123.45 --fees 2
poetry run trade-tools dashboard                   # reports/dashboard/index.html
```

`decide` writes `reports/decisions/<date>/` with `summary.md`, `candidates.csv`
(every fund with its signals and why it was or wasn't chosen) and `trades.csv`.
The GitHub workflow runs it automatically on the 1st–3rd of each month.

## Commands

| Command | What it does |
|---|---|
| `decide [--date D] [--contribution EUR]` | Monthly decision and suggested trades |
| `backtest [--start D] [--end D]` | Run the decision over history; writes `reports/backtests/<timestamp>/` |
| `tx add / list / reverse ID / import FILE` | Record real transactions (`buy`, `sell`, `deposit`, `withdrawal`, `fee`, `dividend`) |
| `portfolio` | Print holdings and performance |
| `dashboard [--output FILE]` | Write the portfolio dashboard HTML |
| `ingest`, `backfill --ticker T`, `refresh-adjusted-history` | Update prices from Yahoo |
| `init-db`, `asset list / add / deactivate / reactivate` | Database and asset master data |

Transactions are never edited: `tx reverse ID` adds an opposite row instead. CSV import
columns: `date,type,quantity[,ticker,price,fees,currency,fx_to_eur,notes,reference]`;
`quantity` is units for buy/sell and an amount otherwise; non-EUR trades need `fx_to_eur`.

## Analysis in a notebook

Everything is plain pandas. [notebooks/walkthrough.py](notebooks/walkthrough.py) runs one
monthly decision step by step and a backtest (open it in VS Code and run the `# %%` cells).

```python
import pandas as pd
from trade_tools.db import get_session
from trade_tools.prices import load_prices
from trade_tools.settings import load_settings, load_universe
from trade_tools.decision import make_decision
from trade_tools.backtest import run_backtest

settings, universe = load_settings(), load_universe()
with get_session() as session:
    prices = load_prices(session)          # wide EUR tables: prices.adj_close, ...
decision = make_decision(prices, universe, settings, pd.Timestamp("2026-10-31"))
decision.table                             # every candidate, signals, status, weight
result = run_backtest(prices, universe, settings)
result.metrics
```

## Project structure

```text
config/
  strategy.toml        all strategy settings
  universe.csv         candidate funds: group, tradable, preferred
src/trade_tools/
  settings.py          load the two config files
  prices.py            DB -> wide EUR price tables (close, adj_close, adj_open)
  signals.py           returns, momentum, volatility, trend per asset
  selection.py         eligibility, one listing per group, filters, top N
  weights.py           inverse volatility, caps, volatility target
  decision.py          make_decision (the steps above) + suggested trades
  backtest.py          make_decision at every past month-end + accounting + metrics
  ledger.py            transactions -> holdings, cash, daily value, time-weighted return
  dashboard.py         portfolio summary + HTML page
  web.py, wsgi.py      password-protected web server for the dashboard
  cli.py               the commands above
  db/                  SQLAlchemy models: assets, daily/hourly prices, transactions
  market_data/         Yahoo Finance ingestion (asset seed list, validation, style boxes)
tests/                 pytest; uses an in-memory database, never the real one
```

## Setup

Python 3.11+, Poetry and a PostgreSQL database reachable from your machine and from
GitHub Actions (e.g. a free [Neon](https://neon.tech) project).

```bash
poetry install
echo 'DATABASE_URL=postgresql://user:password@host/dbname?sslmode=require' > .env
poetry run trade-tools init-db      # create tables and seed the 99-asset list
poetry run trade-tools ingest       # ~20 years of daily prices on first run
poetry run pytest                   # tests refuse to run against the .env database
```

Don't `source .env` or let your editor export it into the terminal: the app reads
`.env` itself, and the tests refuse to start if `DATABASE_URL` points at it.

Other environment variables: `HISTORICAL_START_DATE` (default `2005-01-01`),
`OVERLAP_DAYS_DAILY` (5), `OVERLAP_DAYS_HOURLY` (2), `LOG_LEVEL` (`INFO`).

### Price data

- Tables: `assets`, `daily_prices`, `hourly_prices` (unique per asset and timestamp), `transactions`.
- The seed list is in [src/trade_tools/market_data/initial_universe.py](src/trade_tools/market_data/initial_universe.py).
  The strategy only considers funds listed in `config/universe.csv`; add a fund to both
  (`trade-tools asset add` then a CSV row) to make it a candidate.
- Prices are converted to EUR with Yahoo FX rates (EURUSD, GBPUSD, JPY, CHF; pence → pounds).
- A fund whose price jumps more than 3× in one day has its earlier history dropped
  (vendor data on a different scale); this is listed in the backtest summary.

## Scheduled workflows (GitHub Actions)

Add the repository secret `DATABASE_URL` (Settings → Secrets and variables → Actions).

| Workflow | When | What |
|---|---|---|
| `daily_ingestion.yml` | daily 06:00 UTC | `ingest` |
| `monthly_decision.yml` | 1st–3rd, 08:00 UTC | `refresh-adjusted-history`, `decide`; report uploaded as artifact |
| `monitoring_refresh.yml` | daily 09:00 UTC | `dashboard`; HTML uploaded as artifact |
| `backtest.yml` | manual | `backtest`; report uploaded as artifact |

Scheduled workflows in public repositories are disabled after 60 days without activity.

## Hosted dashboard

The [Dockerfile](Dockerfile) runs the dashboard as a Gunicorn web service that rebuilds
the page on every request and refreshes every 5 minutes. Set `DATABASE_URL`,
`DASHBOARD_USERNAME` and `DASHBOARD_PASSWORD` (≥ 16 characters) in the host's secret
manager; it listens on `PORT` (8080) and exposes `/healthz`. Serve it over HTTPS only
(it uses HTTP Basic Auth). The "Target" column appears when the host has a saved
decision in `reports/decisions/`.

```bash
docker build -t trade-tools-dashboard .
docker run -d --name trade-tools-dashboard -p 8080:8080 \
  --env DATABASE_URL --env DASHBOARD_USERNAME --env DASHBOARD_PASSWORD trade-tools-dashboard
```

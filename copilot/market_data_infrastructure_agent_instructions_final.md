# Market Data Infrastructure — Agent Instructions

## Purpose

Build and maintain a simple Python market-data infrastructure for a personal long-term investment research and portfolio-optimization project.

This document is the authoritative implementation reference. Always read and follow it when implementing, modifying, or debugging this infrastructure. Preserve the defined architecture unless an explicit instruction changes it.

The scope is:

- Asset master data and metadata
- Historical and incremental market-price ingestion
- Daily and hourly price storage
- Daily scheduled ingestion
- Clean interfaces for future portfolio, feature, ML, optimization, backtesting, and rebalancing components

Keep the implementation simple, clear, conventional, and maintainable.

## Technology

Use:

- Python 3.11+
- `yfinance` for Yahoo Finance data
- `pandas` for data handling
- SQLAlchemy for database access/ORM
- PostgreSQL for persistent storage
- pytest for tests
- Environment variables for configuration

Use OOP with a small number of clearly defined classes and responsibilities. Prefer simple composition and small methods over complex abstractions.

Follow the existing repository structure and conventions. Do not impose a fixed directory or filename structure.

## Asset Universe

The initial asset universe must be stored directly in the database. The database is the source of truth.

Initial assets:

| ticker | type |
|---|---|
| AAPL | equity |
| MSFT | equity |
| NVDA | equity |
| AMZN | equity |
| GOOGL | equity |
| META | equity |
| BRK-B | equity |
| JPM | equity |
| JNJ | equity |
| XOM | equity |
| SAP.DE | equity |
| ASML.AS | equity |
| NESN.SW | equity |
| SPY | etf |
| QQQ | etf |
| VTI | etf |
| VT | etf |
| VXUS | etf |
| EFA | etf |
| EEM | etf |
| IWM | etf |
| AGG | etf |
| BND | etf |
| VNQ | reit |
| O | reit |
| PLD | reit |
| AMT | reit |
| ^TNX | fixed_income_proxy |
| ^FVX | fixed_income_proxy |
| ^IRX | fixed_income_proxy |
| GLD | commodity |
| SLV | commodity |
| DBC | commodity |
| BTC-USD | crypto |
| ETH-USD | crypto |
| ^GSPC | index |
| ^IXIC | index |
| ^STOXX50E | index |
| ^GDAXI | index |
| ^FTSE | index |
| ^N225 | index |
| EURUSD=X | fx |
| GBPUSD=X | fx |
| JPY=X | fx |
| CHF=X | fx |

The seed universe has since been extended with 35 additional UCITS ETF/ETC listings:

| ticker | type |
|---|---|
| VWCE.DE | etf |
| EUNL.DE | etf |
| IUSQ.DE | etf |
| SPYI.DE | etf |
| SXR8.DE | etf |
| VUAA.DE | etf |
| SXRV.DE | etf |
| EQQB.DE | etf |
| IS3N.DE | etf |
| VFEA.DE | etf |
| EXSA.DE | etf |
| EXS1.DE | etf |
| IQQJ.DE | etf |
| EUNK.DE | etf |
| IUSN.DE | etf |
| ZPRS.DE | etf |
| CUSS.L | etf |
| QDVE.DE | etf |
| EXV3.DE | etf |
| WITS.L | etf |
| QDVG.DE | etf |
| EXV4.DE | etf |
| QDVH.DE | etf |
| EXV1.DE | etf |
| QDVF.DE | etf |
| IQQH.DE | etf |
| 2B7D.DE | etf |
| VAPX.L | etf |
| EUNJ.DE | etf |
| ICGA.DE | etf |
| ASHR.L | etf |
| 36BZ.DE | etf |
| KWBE.DE | etf |
| PPFB.DE | commodity |
| VAGF.DE | etf |

Two additional requested Asia ex-Japan products were withheld pending exact
instrument identification: the closest iShares Yahoo result is non-UCITS, and
the Xtrackers Xetra result is ESG-screened. Do not substitute these without
confirmation because they track different indexes or apply additional screens.

Controlled asset types:

- equity
- etf
- reit
- fixed_income_proxy
- commodity
- crypto
- index
- fx

Assets must be addable, deactivatable, and reactivatable through the database.

## Asset Master Table

Create an `assets` table with one row per asset.

Store at least:

- `id`
- `ticker`
- `type`
- `name`
- `currency`
- `exchange`
- `country`
- `sector`
- `style_box_category` (manual; `N/A` for non-equity)
- `bond_matrix_category` (manual; `N/A` for non-bond)
- `data_source`
- `active`
- `created_at`
- `updated_at`
- `deactivated_at`
- `metadata_updated_at`

`ticker` must be unique.

The initial ticker and type come from the initial universe. Retrieve the remaining available metadata from Yahoo Finance and store it.

Yahoo Finance metadata is not complete for every instrument. Store NULL where information is unavailable or unreliable.

Use `active` to control regular ingestion.

When an asset is no longer required, set `active = false` rather than deleting it or its historical prices. Preserve inactive assets and historical data for historical analysis and backtesting and to reduce survivorship bias.

Allow inactive assets to be reactivated.

## Daily Prices

Create a `daily_prices` table containing:

- `id`
- `asset_id`
- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `adjusted_close`
- `volume`
- `created_at`

Create a unique constraint on:

```text
(asset_id, timestamp)
```

Create an index for efficient queries by asset and timestamp.

Import approximately 20 years of available daily history. If an asset has less history, import the maximum available history.

## Hourly Prices

Create a separate `hourly_prices` table containing:

- `id`
- `asset_id`
- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `adjusted_close`
- `volume`
- `created_at`

Create a unique constraint on:

```text
(asset_id, timestamp)
```

Create an index for efficient queries by asset and timestamp.

Import the maximum useful hourly history available from Yahoo Finance. Yahoo Finance provides substantially less historical hourly data than daily data, so do not expect 10–20 years of hourly history.

Daily and hourly data must remain in separate tables so they can be queried and managed independently.

## Timestamps and Time Zones

Market timestamps must be timezone-aware.

Normalize stored timestamps to UTC while preserving correct source-market timing during ingestion.

Do not assume that all assets trade in the same timezone.

The market timestamp represents when the observation occurred. `created_at` represents when the observation was stored. Keep these concepts separate.

## Yahoo Finance Client

Create a `YahooFinanceClient` class responsible for all interaction with `yfinance`.

Provide methods equivalent to:

```python
get_asset_metadata(ticker)
get_prices(ticker, interval, start, end)
```

Retrieve available metadata such as:

- name
- currency
- exchange
- country
- sector
- market capitalization and other basic metadata when reliably available

Handle:

- invalid tickers
- empty responses
- missing expected columns
- temporary download failures

Use logging for failures and useful ingestion information.

## Market Data Ingestion

Create a `MarketDataIngestion` class responsible for orchestration.

For each active asset:

1. Read the asset from the database.
2. Retrieve and update available Yahoo Finance metadata.
3. Determine the latest stored timestamp separately from `daily_prices` and `hourly_prices`.
4. Download only the required historical or new data.
5. Validate the downloaded data.
6. Upsert observations into the appropriate price table.
7. Record a concise ingestion result.

For assets with no stored history, start from the configured historical start date.

For existing history, ingest incrementally from the latest stored timestamp with a small overlap so corrected provider data can be refreshed.

The process must be idempotent.

Only assets with `active = true` are processed by the regular ingestion job.

If one asset fails, log the failure and continue with the remaining assets.

## Historical Data Configuration

Use:

```text
HISTORICAL_START_DATE
```

from configuration.

The default should represent approximately 20 years of daily history.

Do not require all assets to have the same amount of historical data.

## Data Validation

Before storing downloaded data:

- verify required columns exist
- verify timestamps are valid
- remove completely empty observations
- remove duplicate observations before insertion
- ensure numeric price fields are numeric
- store volume appropriately
- preserve genuine missing values
- never fabricate prices

## Database Layer

Create a small database component responsible for:

- SQLAlchemy engine creation
- session management
- table initialization
- database access used by the ingestion layer

Use:

```text
DATABASE_URL
```

from the environment.

Do not hard-code credentials.

Use transactions for database writes.

Use PostgreSQL upserts for asset metadata and price observations.

## Daily Scheduled Job

Create a daily job that runs market-data ingestion.

Each execution should:

- process all active assets
- update daily prices
- update the latest available hourly prices
- update asset metadata where appropriate

The scheduling mechanism should be external to the ingestion logic. The job must be compatible with cron, CI/CD scheduling, or another standard scheduler.

Log:

- execution start/end
- number of active assets processed
- daily records inserted/updated
- hourly records inserted/updated
- failed assets

## Tests

Add focused tests covering:

- asset creation
- metadata persistence
- duplicate asset prevention
- activation/deactivation
- daily price uniqueness
- hourly price uniqueness
- daily ingestion
- hourly ingestion
- incremental ingestion
- upsert behavior
- empty Yahoo Finance responses
- failed asset downloads

Mock `yfinance` in tests. Tests must not depend on a live Yahoo Finance connection.

## Code Style

Use:

- type hints
- clear class and method names
- small functions
- standard Python conventions
- useful docstrings
- structured logging
- straightforward error handling

Keep the implementation compact and readable.

Use the simplest conventional implementation that satisfies this specification.

## Documentation

Provide a README documenting:

1. Project purpose
2. Architecture
3. Database schema
4. Configuration
5. Installation
6. Database initialization
7. Initial asset universe
8. Adding, deactivating, and reactivating assets
9. Running ingestion manually
10. Daily job execution
11. Basic troubleshooting

## Architecture

Maintain this logical flow:

```text
PostgreSQL: assets
        │
        ▼
MarketDataIngestion
        │
        ├──► YahooFinanceClient
        │        │
        │        └──► Yahoo Finance
        │
        ▼
PostgreSQL
        │
        ├── daily_prices  → ~20 years of available history
        └── hourly_prices → maximum available Yahoo Finance history
```

The database is the persistent source of market data for all future components.

Future feature engineering, portfolio optimization, machine learning, backtesting, and rebalancing components should consume stored database data rather than directly depending on Yahoo Finance.

## Implementation and Maintenance Rules

When modifying the project:

1. Read this document first.
2. Inspect the existing implementation before making changes.
3. Preserve functionality that conforms to this specification.
4. Fix underlying implementation issues rather than adding unnecessary workarounds.
5. Keep the architecture simple and conventional.
6. Avoid introducing complexity unless required by this specification or an explicit future requirement.
7. Update this document only when the agreed architecture or requirements actually change.

This document is the baseline specification and ongoing reference for the market-data infrastructure.

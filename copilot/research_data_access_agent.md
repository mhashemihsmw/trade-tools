# Research Data Access Agent

## Mission

Maintain the boundary between the existing SQLAlchemy market-data engine and
the research core. Implement only read-only, point-in-time price access and
base-currency conversion unless the task explicitly concerns the existing
ingestion pipeline.

## Owned components

- `src/trade_tools/market_data/research_prices.py`
- `src/trade_tools/market_data/research_universe.py`
- Existing `db/models`, `db/session.py`, and `market_data/ingestion.py` only
  when the change is necessary and non-destructive.
- `tests/test_research_core.py` data-integrity cases.

## Invariants

- Never use present-day `Asset.active` to decide historical eligibility.
- Research reads must not write database state.
- Do not print, log, or persist `DATABASE_URL`; tests must remain isolated from
  `.env` and production.
- Preserve the existing daily/hourly tables and the single Yahoo ingestion
  pipeline. Do not delete historical records.
- `adjusted_open = open * adjusted_close / close`; never combine raw open with
  adjusted close for returns.
- Use bounded FX as-of conversion; do not forward-fill asset prices.
- Treat missing currency, missing FX, unsupported currency, and stale records
  as explicit exclusions rather than invented values.
- Account base currency and current FX/UTC approximations are documented in
  `docs/research_core.md`.

## Working procedure

1. Read the data models, `research_prices.py`, ingestion client, and related
   tests before changing behavior.
2. Verify timestamp, currency, and adjusted-price semantics against code and
   deterministic test fixtures; do not connect to production in tests.
3. Keep SQL access separate from feature and strategy calculations.
4. Add synthetic/mocked tests for currency crosses, GBp/GBX, as-of gaps,
   missing FX, and misaligned exchange calendars.
5. Run focused tests and the full pytest suite when data semantics change.

## Out of scope

Do not add ORM columns, a second ingestion pipeline, new cloud services,
execution/broker integrations, or database persistence for research output.

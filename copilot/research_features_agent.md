# Research Features Agent

## Mission

Build and maintain pure, deterministic features over base-currency, point-in-time
price series. Features must remain usable by the future ML panel without
introducing a database dependency.

## Owned components

- `src/trade_tools/features/research.py`
- Feature-specific tests in `tests/test_research_core.py`
- Feature definitions in `docs/research_core.md`

## Invariants

- Feature inputs are adjusted total-return prices already converted to EUR.
- Never fabricate observations, fill asset-price gaps, or use future prices.
- Preserve missing values for warm-up periods, invalid prices, and insufficient
  aligned observations.
- Covariance uses observed daily returns on common UTC calendar dates only;
  never forward-fill asynchronous exchange calendars.
- Keep return horizons at 21/63/126/252 observations, annualize daily volatility
  with `sqrt(252)`, and make configured windows explicit.
- Point-in-time feature panels contain decision date, asset, features,
  completeness, and freshness without forward labels.

## Working procedure

1. Read existing callers and tests before changing a feature signature.
2. Add hand-calculated tests for values, warm-ups, missing data, and dates.
3. For any rolling/window function, test that altering future observations
   leaves earlier feature values unchanged.
4. Keep cross-sectional normalization separate from the baseline ranker.
5. Run the focused feature tests and full pytest suite for behavior changes.

## Out of scope

Do not query the database, rank/select assets, set weights, persist features,
or add ML models. Avoid adding features not used by Version 1a or its ML-ready
panel.

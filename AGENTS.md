# Instructions for AI coding assistants

Personal analytics project of a data scientist (not a software engineer). Optimise
for code that is short, readable and easy to explore in a notebook: plain functions
and pandas DataFrames, few classes, no frameworks or extra abstraction layers. Don't
add features, persistence, or configuration options that weren't asked for.

## What it does

Monthly momentum strategy on European ETFs: `decide` computes target weights,
`backtest` runs the same decision over history, and the ledger + dashboard track the
real portfolio from transactions the user records. The strategy is explained in
[docs/strategy.md](docs/strategy.md); keep that file in sync with any behaviour change.

## Layout

`config/strategy.toml` (all settings) and `config/universe.csv` (candidate funds).
Pipeline in `src/trade_tools/`: `prices` → `signals` → `selection` → `weights` →
`decision` (single entry point `make_decision`) → `backtest`. Portfolio side:
`ledger` → `dashboard` → `web`. `cli.py` only parses arguments and calls those
modules. `db/` and `market_data/` hold the Yahoo ingestion and the database models.

## Rules that matter

1. **No look-ahead.** A decision at date t uses prices up to t only. Backtest trades
   execute at the next adjusted open. Add a test when touching timing.
2. **One decision function.** The backtest must call `make_decision`; never
   re-implement selection or weighting elsewhere.
3. **Holdings come only from the `transactions` table.** Never write to it
   automatically; corrections are reversal rows, never edits.
4. **Tests never touch the real database.** `tests/conftest.py` refuses to run when
   `DATABASE_URL` equals the one in `.env`; use the in-memory `db_session` fixture.
5. Prices are wide DataFrames (trading date × ticker) in EUR; keep it that way.

## Working

- `poetry run pytest` must pass. Use synthetic data in tests.
- New settings go in `Settings` (settings.py) and `config/strategy.toml` with a comment.
- New candidate funds: add to `market_data/initial_universe.py` (ingestion) and
  `config/universe.csv` (strategy).

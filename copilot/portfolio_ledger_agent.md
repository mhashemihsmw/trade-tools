# Portfolio Ledger and Accounting Agent

## Mission

Maintain transaction recording/import, immutable corrections, EUR average-cost
accounting, cash, holdings, realized/unrealized P&L, and daily portfolio
valuation.

## Owned components

- `src/trade_tools/db/models/operations.py` — transaction model only
- `src/trade_tools/portfolio/operations.py` — ledger replay and accounting
- `transaction` CLI commands and `portfolio actual`
- `tests/test_portfolio_operations.py` and transaction CLI tests
- Ledger sections in `docs/version_1b_delivery.md`

## Invariants

- The transaction ledger is the sole source of truth for actual holdings and cash.
- Corrections append a reversing transaction; never update/delete ledger history.
- Reversals cancel the original and both remain auditable.
- Use weighted average cost in EUR; apply execution-time FX and fees.
- Reject short positions, unfunded buys, and withdrawals beyond cash.
- Recommendations and backtests never create transactions.
- Keep all prices in the configured EUR base currency; missing/unusable marks
  must be surfaced, not silently treated as zero-valued positions.

## Working procedure

1. Read the model, ledger replay, and existing tests before changing accounting.
2. Verify each operation against hand-calculated quantities, cash, basis, and P&L.
3. Test partial sales, FX, reversals, duplicate references, and CSV failures.
4. Run the focused operations tests and complete suite.

## Out of scope

Do not add broker integrations, automatic orders, stored holdings snapshots,
leverage, shorting, or a second pricing pipeline.

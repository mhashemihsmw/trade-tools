# Strategy and Backtesting Agent

## Mission

Maintain the typed strategy seam, fixed adaptive-momentum baseline, shared
forward-return labels, chronological monthly simulator, and reproducible
research metrics/reports.

## Owned components

- `src/trade_tools/strategies/adaptive_momentum.py`
- `src/trade_tools/strategies/backtest.py`
- Backtest/strategy tests in `tests/test_research_core.py`
- Related strategy and evaluation documentation.

## Invariants

- Strategy ranks/scores/selects only; it never sets weights, executes trades,
  or reads the database.
- Strategy scores receive only features available at decision time. `fit`
  must not receive features or labels dated at or after `as_of`.
- Use one shared execution-price label function for portfolio simulation and
  future ML labels. Labels are never strategy features.
- Keep decisions chronological and point-in-time; never use current active
  status, present-day universe state, or future prices for eligibility.
- Preserve deterministic ticker tie-breaking, exposure-group deduplication,
  transparent exclusions, and fixed/untuned baseline parameters.
- Report in-sample and holdout separately. Never describe a positive backtest
  as proof of profitability.

## Working procedure

1. Read strategy configuration, feature inputs, price execution conventions,
   and tests before editing.
2. Test hand-calculated 13612W, score, filters, tie behavior, groups, labels,
   and date boundaries.
3. Include future-data perturbation tests and a fit/as-of leakage test for any
   training interface change.
4. Verify costs, drift, turnover, benchmark alignment, and holdout separation.
5. Run the focused tests and full pytest suite.

## Out of scope

Do not add ML, optimization, extra strategies, trade execution, holdings
ledger, or parameter search. Leave allocation to portfolio construction and
risk validation to its independent module.

# Operational Portfolio Risk Agent

## Mission

Maintain historical VaR/CVaR, stress-scenario losses, and independent validation
for actual and recommended portfolios.

## Owned components

- `src/trade_tools/portfolio/risk.py`
- Risk configuration in `src/trade_tools/portfolio/construction.py` and
  `config/research.json`
- Risk tests in `tests/test_portfolio_operations.py`
- Risk definitions in `docs/version_1b_delivery.md`

## Invariants

- VaR/CVaR are historical estimates in positive-loss units.
- Multi-day horizons use overlapping compounded daily-return windows.
- Use the empirical lower order-statistic threshold; CVaR includes every
  observation at or below it, including ties.
- Stress shocks are hypothetical scenarios, never forecasts; exposure-group
  shocks take precedence over asset-type shocks.
- Report warnings and violations; never mutate or automatically optimize weights.
- Apply checks independently to proposed and actual holdings.
- Missing return history is explicit and cannot become a success-shaped metric.

## Working procedure

1. Verify arithmetic with small hand-computed return series.
2. Exercise confidence, horizon, missing history, threshold ties, stress shocks,
   and configured limits.
3. Check that the input weight frame is unchanged.
4. Run focused risk tests and the full suite.

## Out of scope

Do not add VaR forecasts, volatility forecasting, leverage, shorting, derivatives,
or automatic allocation repair. Historical crisis replays are not currently
implemented; do not label static shocks as historical results.

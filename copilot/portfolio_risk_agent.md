# Portfolio Construction and Risk Agent

## Mission

Maintain the deterministic target-weight constructor and independent risk
validation/concentration reporting, without allowing portfolio code to select
assets or silently repair violations.

## Owned components

- `src/trade_tools/portfolio/construction.py`
- `src/trade_tools/portfolio/risk.py`
- Related tests in `tests/test_research_core.py`
- Portfolio/risk sections of `docs/research_core.md`

## Invariants

- Inputs are selected strategy rows, configuration, and point-in-time data.
- Filled slots determine the risky budget; empty slots stay in cash.
- Inverse-volatility/equal weighting, asset/group caps, and realized-volatility
  scaling must preserve long-only, no-leverage weights.
- Caps leave excess in cash; volatility targeting scales down only.
- Constructor does not select assets.
- Risk checks report limits, violations, concentration, volatility, and
  drawdown. They never alter weights. Operational VaR/CVaR and scenario risks
  are implemented in `operational_risk_agent.md`.
- Concentration must say look-through constituents are unavailable.

## Working procedure

1. Check selection and construction interfaces before touching portfolio logic.
2. Add arithmetic/hand-computed tests for budget, caps, cash, covariance,
   scaling, and infeasible weights.
3. Verify the input DataFrame is unchanged and risk validation never mutates
   targets.
4. Run focused tests and then the full pytest suite.

## Out of scope

Do not add asset selection, mean-variance optimization, leverage, shorting,
order placement, or actual portfolio ledger state. Coordinate VaR/CVaR and
stress-scenario changes with the operational-risk role.

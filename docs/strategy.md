# The strategy, step by step

A long-only, monthly **dual-momentum** strategy on ETFs and ETCs: hold the funds that
have risen most (relative momentum), but only while they are rising at all (absolute
momentum) and above their long-term trend; otherwise hold cash. Weights favour calmer
funds and the whole portfolio is kept near a volatility budget.

Code: `signals.py` → `selection.py` → `weights.py`, chained by `decision.make_decision`.
Settings: [config/strategy.toml](../config/strategy.toml). Candidates:
[config/universe.csv](../config/universe.csv).

## 0. Data and timing

- Daily prices from Yahoo, converted to EUR. Signals use **adjusted closes** (dividends
  and splits folded in, so returns are total returns).
- Each bar is assigned to its exchange's **trading date** (Yahoo stamps bars at local
  midnight in UTC; adding 12 h before taking the date maps every exchange correctly).
- The decision is taken at the **last trading day of the month** using only data up to
  that day. In the backtest, trades execute at the **next day's adjusted open** and are
  held until the open after the next month-end. A month enters the backtest only once
  that exit price exists, so the month in progress is never used.
- Data cleaning: non-positive prices are dropped; a fund whose price jumps more than
  3× in one day (vendor scale change) loses the history before the jump.

## 1. Candidates

`config/universe.csv` lists 68 funds, each with an **exposure group** (what it tracks,
e.g. `sp500`, `gold`, `euro_corporate_bonds`). `tradable_only = true` keeps the
European listings (`.DE`, `.L`, `.MI`, `.MU`); `false` also admits US-listed funds,
which have longer histories and are useful for research only.

## 2. Eligibility

A fund is dropped this month if it has

| Status | Rule |
|---|---|
| `no_data` | no prices at all |
| `short_history` | fewer than `min_history_days` (252) observations |
| `stale_price` | its latest price is more than `max_stale_days` (5) business days old |
| `missing_signal` | a signal below could not be computed |

## 3. One listing per exposure group

Many groups contain near-identical listings (three S&P 500 ETFs). Picking the
best-scoring one would make the portfolio switch between them on noise and pay
costs for nothing, so each group is represented by one listing: the `preferred` one
in the CSV, else the one with the longest history. The others get
`other_listing_in_group`.

## 4. Signals

For each fund, on its own trading days:

| Signal | Definition |
|---|---|
| `r1, r3, r6, r12` | simple return over 21, 63, 126, 252 trading days |
| `momentum` | (12·r1 + 4·r3 + 2·r6 + 1·r12) / 4 — "13612W" momentum (Keller & Keuning): the average of the four returns, each roughly annualised (12·r1 annualises a 1-month return), so the most recent month weighs most |
| `volatility` | annualised standard deviation of the last 63 daily returns (√252 scaling) |
| `trend_distance` | price / mean(last 10 month-end prices, the current price being the latest) − 1 |
| `score` | momentum / volatility (`volatility_adjusted = true`), i.e. a return-per-risk ranking |

## 5. Filters and ranking

| Status | Rule |
|---|---|
| `negative_momentum` | momentum ≤ 0 (`absolute_momentum_filter`) — the fund is falling |
| `below_trend` | trend_distance ≤ 0 (`trend_filter`) — price below its 10-month average |
| `selected` | among the remaining funds, the `top_n` (5) highest scores |
| `not_in_top_n` | passed everything but ranked lower |

The filters are what move the portfolio to cash in broad sell-offs.

## 6. Weights

1. **Slot budget**: each of the `top_n` slots is worth 1/`top_n` (20%). Three selected
   funds → 60% invested, 40% cash.
2. **Inverse volatility** inside the budget: wᵢ ∝ 1/σᵢ, so a fund half as volatile gets
   twice the weight (`inverse_volatility = false` → equal weights).
3. **Cap** at `max_weight` (30%); the excess goes to the other selected funds pro rata
   (to cash only if all are capped).
4. **Volatility target**: estimate portfolio volatility √(wᵀΣw) from the last 63 days
   on which all selected funds traded. If it exceeds `volatility_target` (10%), scale
   all weights by target / estimate; the rest goes to cash.

## 7. Suggested trades (`decide`)

Capital = your holdings at the latest close + ledger cash + this month's contribution.
Target value = weight × capital; the difference to what you hold, divided by the
price and rounded toward zero (whole units unless `fractional_shares`), gives the
trade. Sells are listed first. Estimated cost = `cost_bps` × value + `fixed_fee`.

## 8. Backtest

`run_backtest` calls the same `make_decision` at every month-end, then runs three
portfolios through identical accounting:

- **strategy**: the decision's weights;
- **equal_weight**: every eligible group (one listing each) at equal weight — tells
  you whether the *selection* adds value over just owning the universe;
- **benchmark**: 100% in VWCE.DE (VT before VWCE existed) — tells you whether the
  whole exercise beats a single world-equity fund.

Each month: drifted weights from last month → rebalance to target (skipping trades
below `min_trade_eur`) → costs = `cost_bps` × traded value + `fixed_fee` per traded
fund → returns from open to open → new drifted weights. Cash earns `cash_rate`.

Outputs (`reports/backtests/<timestamp>/`): `monthly.csv` (return, turnover, costs,
cash per portfolio and month), `weights.csv`, `candidates.csv` (every fund every month
with signals, status and the next month's return — the raw material for your own
analysis), `metrics.csv` (full period, in-sample, last `holdout_months`),
`equity_curve.png`, `summary.md`. **Rank IC** in the summary is the mean monthly
Spearman correlation between score and next-month return across eligible groups:
above 0 means the score has some predictive power.

## Known limitations

- Survivorship: the universe is today's list of funds; funds that closed in the past
  are missing, which flatters history.
- Many European listings only exist since ~2010–2019, so early backtest months have
  few candidates (and lots of cash).
- Yahoo adjusted prices are revised over time; `refresh-adjusted-history` re-downloads them.
- Costs are a simple model (no spreads, taxes or market impact).
- The holdout period is only out-of-sample if you don't tune settings while looking
  at its results.

## Changes from the previous implementation (November 2026)

- One listing per group is chosen by preference/history, not by score (less churn).
- Weight trimmed by the 30% cap is redistributed instead of going to cash.
- Trading dates fixed: summer bars of European funds were assigned to the previous day,
  so "month-end" decisions sometimes used the first day of the next month.
- COPA.L, CORN.L, NGAS.L and WEAT.L were always excluded as "missing currency" because
  of a single historical FX gap; they are now candidates.
- Vendor price-scale jumps (e.g. NGAS.L ×1,785 on 2012-01-03) are cleaned.
- Exposure-group weight cap removed (redundant with one fund per group); VaR/CVaR/stress
  reporting removed (it never changed the weights).
- Decisions are files in `reports/decisions/`, not database rows, and are never
  written to the ledger: record your real fills with `tx add`.

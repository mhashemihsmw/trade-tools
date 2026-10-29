# Version 1a Research Core (Historical Scope)

## Status and scope

Version 1a added point-in-time price conversion, pure price features, a deterministic
adaptive-momentum ranker, long-only target construction, independent risk checks,
monthly simulation, CLI commands, and manual/scheduled report workflows. It does
not itself add holdings persistence, orders, VaR/CVaR, stress tests, dashboards,
ML, or additional strategies. Version 1b adds the operating components described
in the [delivery report](./version_1b_delivery.md), while preserving the existing
PostgreSQL/SQLAlchemy market-data tables and daily ingestion workflow.

The market-data tests were run without connecting to the configured cloud
database. The stored-price comparison against a fresh Yahoo download has not been
performed because the local `.env` points to the production database. The code
semantics are verifiable: `YahooFinanceClient` calls `history(auto_adjust=False)`,
stores Yahoo `Open`/`High`/`Low`/`Close` as raw prices and `Adj Close` as
`adjusted_close`. Incremental ingestion rewrites only the configured overlap, so
past adjusted closes can become inconsistent when Yahoo later restates a
dividend/split adjustment. Run `trade-tools refresh-adjusted-history` before
research; it uses the existing ingestion client and idempotent upsert, and
refreshes daily history for every active asset without deleting data or changing
the daily schedule.

## Architecture

- `market_data/research_prices.py` is the point-in-time data-access/conversion
  layer. It loads all assets and daily prices without consulting present-day
  `active` state. Features and strategy modules do not access the database.
- `features/research.py` contains pure return, momentum, volatility, covariance,
  trend, drawdown, high-distance, normalization, and feature-panel functions.
- `strategies/adaptive_momentum.py` defines a typed strategy protocol and the
  baseline rank/eligibility/selection implementation. `strategies/backtest.py`
  contains shared forward-return labels, monthly simulation, metrics, and report
  output.
- `portfolio/construction.py` turns selected assets into weights and cash.
  `portfolio/risk.py` reports limits, concentration, volatility, drawdown,
  warnings, and violations without changing weights.
- `config/research.json` is the documented default configuration. Version 1a
  signals/backtests remain non-persistent; Version 1b snapshots decision-time
  feature values and risk outputs in additive tables.

The strategy receives only features at its decision time. `fit(panel, labels,
as_of)` is called each rebalance with feature rows and labels strictly before
`as_of`; the baseline fit is a no-op. Labels are computed by the shared
`forward_holding_returns` function and are not passed to `score`.

## Classification proposal

`is_fund` identifies funds/ETFs/ETCs (not single stocks, indices, yields, FX, or
crypto spot). `tradable` is the Germany/EU retail-account proposal: EU/UCITS
listings and directly held shares are marked tradable; US-listed retail
investment funds are not. Exposure groups identify repeated or close
underlyings. `preferred` is used as the representative when eligible; if no
preferred listing exists, the longest eligible history is the fallback.
**Review** marks a classification needing instrument/share-class verification.
These classifications are maintained in code, not database columns.

| Ticker | is_fund | tradable | exposure_group | preferred | uncertainty |
|---|---:|---:|---|---:|---|
| 2B7D.DE | true | true | us_consumer_staples | false | — |
| 36BD.DE | true | true | development_bank_bonds | false | — |
| 36BZ.DE | true | true | china_equity | false | review |
| AAPL | false | true | single_stock_aapl | false | — |
| AGAP.L | true | true | agriculture | false | — |
| AGG | true | false | us_aggregate_bonds | false | — |
| AMT | false | true | us_equity_reit_amt | false | — |
| AMZN | false | true | single_stock_amzn | false | — |
| ASHR.L | true | true | china_equity | false | review |
| ASML.AS | false | true | single_stock_asml | false | — |
| BND | true | false | us_aggregate_bonds | false | — |
| BRK-B | false | true | single_stock_brk_b | false | — |
| BTC-USD | false | false | bitcoin_spot | false | — |
| CHF=X | false | false | usd_chf_fx | false | — |
| COPA.L | true | true | copper | false | — |
| CORN.L | true | true | corn | false | — |
| CRUD.MI | true | true | crude_oil | false | — |
| CUSS.L | true | true | us_small_cap | false | — |
| DBC | true | false | broad_commodities | false | — |
| EEM | true | false | emerging_markets | false | — |
| EFA | true | false | developed_ex_us | false | — |
| EQQB.DE | true | true | nasdaq100 | false | — |
| ETH-USD | false | false | ethereum_spot | false | — |
| ETL2.DE | true | true | long_commodities | false | — |
| EUN5.DE | true | true | euro_corporate_bonds | false | — |
| EUNJ.DE | true | true | asia_pacific_ex_japan | false | — |
| EUNK.DE | true | true | europe_equity | false | — |
| EUNL.DE | true | true | developed_world | false | — |
| EURUSD=X | false | false | eur_usd_fx | false | — |
| EXS1.DE | true | true | germany_equity | false | — |
| EXSA.DE | true | true | europe_equity_600 | false | — |
| EXV1.DE | true | true | europe_banks | false | — |
| EXV3.DE | true | true | europe_information_technology | false | — |
| EXV4.DE | true | true | europe_healthcare | false | — |
| GBPUSD=X | false | false | gbp_usd_fx | false | — |
| GLD | true | false | gold | false | — |
| GOOGL | false | true | single_stock_googl | false | — |
| IBCD.DE | true | true | us_corporate_bonds | false | — |
| ICGA.DE | true | true | china_equity | false | — |
| INDU.L | true | true | industrial_metals | false | — |
| IQQ6.DE | true | true | global_property | true | — |
| IQQH.DE | true | true | global_clean_energy | false | review |
| IQQJ.DE | true | true | japan_equity | false | — |
| IS00.MU | true | true | us_high_yield_bonds | false | — |
| IS04.DE | true | true | long_us_treasuries | false | — |
| IS3N.DE | true | true | emerging_markets | true | — |
| IUS5.DE | true | true | global_inflation_linked_bonds | false | — |
| IUS7.DE | true | true | em_bonds_usd | false | — |
| IUSN.DE | true | true | world_small_cap | false | — |
| IUSQ.DE | true | true | global_equity_all_world | false | — |
| IUST.DE | true | true | us_tips | false | — |
| IWM | true | false | us_small_cap | false | — |
| JNJ | false | true | single_stock_jnj | false | — |
| JPM | false | true | single_stock_jpm | false | — |
| JPY=X | false | false | usd_jpy_fx | false | — |
| KWBE.DE | true | true | china_equity | false | review |
| META | false | true | single_stock_meta | false | — |
| MSFT | false | true | single_stock_msft | false | — |
| NESN.SW | false | true | single_stock_nestle | false | — |
| NGAS.L | true | true | natural_gas | false | — |
| NVDA | false | true | single_stock_nvda | false | — |
| O | false | true | us_equity_reit_o | false | — |
| PLD | false | true | us_equity_reit_pld | false | — |
| PPFB.DE | true | true | gold | false | — |
| QDVE.DE | true | true | us_information_technology | false | — |
| QDVF.DE | true | true | us_energy | false | — |
| QDVG.DE | true | true | us_healthcare | false | — |
| QDVH.DE | true | true | us_financials | false | — |
| QQQ | true | false | nasdaq100 | false | — |
| SAP.DE | false | true | single_stock_sap | false | — |
| SLV | true | false | silver | false | — |
| SOYB.MI | true | true | soybeans | false | — |
| SPY | true | false | sp500 | false | — |
| SPYI.DE | true | true | global_equity_all_world | false | — |
| SXR8.DE | true | true | sp500 | true | — |
| SXRV.DE | true | true | nasdaq100 | true | — |
| VAGF.DE | true | true | global_aggregate_bonds_eur_hedged | true | — |
| VAPX.L | true | true | asia_pacific_ex_japan | false | — |
| VFEA.DE | true | true | emerging_markets | false | — |
| VNQ | true | false | us_property_reit | false | — |
| VT | true | false | global_equity_all_world | false | — |
| VTI | true | false | us_total_market | false | — |
| VUAA.DE | true | true | sp500 | false | — |
| VWCE.DE | true | true | global_equity_all_world | true | — |
| VWCG.DE | true | true | europe_equity | false | — |
| VXUS | true | false | developed_ex_us | false | — |
| WEAT.L | true | true | wheat | false | — |
| WITS.L | true | true | global_information_technology | false | — |
| XOM | false | true | single_stock_xom | false | — |
| ZPRS.DE | true | true | world_small_cap | false | — |
| ^FTSE | false | false | ftse_100_index | false | — |
| ^FVX | false | false | us_5y_yield | false | — |
| ^GDAXI | false | false | dax_index | false | — |
| ^GSPC | false | false | sp500_index | false | — |
| ^IRX | false | false | us_3m_yield | false | — |
| ^IXIC | false | false | nasdaq_composite_index | false | — |
| ^N225 | false | false | nikkei_225_index | false | — |
| ^STOXX50E | false | false | euro_stoxx_50_index | false | — |
| ^TNX | false | false | us_10y_yield | false | — |

Values for European direct share and selected commodity/China listings should be
reviewed with the user's broker and exact ISIN/share class. The seed list is
unchanged.

## Data integrity and currency

- Investment returns use `adjusted_close` as a total-return proxy. Yahoo's raw
  open is adjusted to the same basis on that date:
  `adjusted_open = open * adjusted_close / close`.
- USD to EUR uses `1 / EURUSD=X`; GBP to EUR uses
  `GBPUSD=X / EURUSD=X`; JPY and CHF, quoted as units per USD, use
  `1 / (units-per-USD * EURUSD=X)`. The last FX close at or before each
  observation is used, bounded by five calendar days. There is no forward-fill
  in asset prices or returns.
- GBp/GBX quotes are divided by 100 before currency conversion. Missing
  currency or missing/stale FX is reported as an exclusion. Supported conversion
  currencies are EUR, USD, GBP/GBp, JPY, and CHF; unsupported currencies are
  excluded rather than guessed.
- Calendar alignment for cross-asset covariance uses each asset's observed
  daily return on the UTC calendar date and complete common dates only. No
  prices or returns are synthesized.
- Decision features use the last valid adjusted close at or before each
  month-end timestamp. Execution labels use the first adjusted open after the
  decision and the first adjusted open after the next rebalance date.

## Features, universe, and strategy

`features/research.py` provides exact 21/63/126/252-observation simple returns,
13612W momentum, annualized realized volatility, annualized sample covariance,
10-month month-end SMA (or configured daily SMA), rolling drawdown, distance
from rolling high, completeness/freshness fields, cross-sectional [0, 1] rank,
winsorized z-score, and a historical decision-date/asset feature panel.
Warm-up and invalid-price observations remain missing.

The default ranking universe is `tradable`: `is_fund`, Germany/EU `tradable`,
known currency conversion, adequate history (252 observations plus configurable
buffer), and freshness are required. `research` includes non-tradable funds as
history proxies. Static `active` state is never used in historical eligibility.
The rank report includes a reason for exclusions. Exposure groups are filled
greedily in score order, with at most one selected listing per group by default.

The fixed-parameter baseline computes
`(12*r1 + 4*r3 + 2*r6 + r12) / 4`, optionally divides by annualized volatility,
then applies the absolute-momentum and month-end SMA trend filters. It selects
up to five assets; it does not set weights. Ties are ordered by ticker.
`--ablation-off` disables volatility-adjusted ranking, both selection filters,
inverse-volatility weighting, and volatility targeting. Parameters are fixed in
advance; no optimization or ML is performed.

## Portfolio and risk

Portfolio weights are long-only. The risky budget equals selected slots divided
by `top_n`; inverse-volatility weights (or equal weights) are capped per asset
and exposure group. Any cap excess remains cash. Volatility targeting estimates
annualized sample covariance and scales risky weights down only. Weight sums
plus cash equal 100%; the constructor never selects assets. Risk validation
reports violations and warnings without modifying target weights. Concentration
breakdowns cover asset, group, type, country, and sector when available.
Look-through constituent exposure is unavailable. VaR/CVaR and scenario risk
extensions are covered in the Version 1b delivery report.

## Backtest conventions and disclosures

The backtester is chronological and monthly, charges configurable proportional
basis-point cost plus a fixed fee for each order, skips trades under a configured
minimum size, and reports one-way turnover. Monthly contributions are added
before rebalancing; returns are time-weighted. Cash return and Sharpe risk-free
rate default to 0%. The equal-weight group-representative benchmark is rebalanced
with the same cost model. The passive benchmark uses VWCE.DE when available and
VT in EUR as an explicitly disclosed earlier-history proxy when enabled.

The most recent 36 months are reserved as holdout by default; `--include-holdout`
adds separately labeled in-sample/holdout metrics. Backtest reports disclose
universe mode, the hand-selected 2025–26 universe and its survivorship bias,
history start-date bias, eligible assets per year, FX/data limitations, and the
fixed untuned baseline. A positive historical result is not described as proof
of profitability. Reports are written under
`reports/backtests/<configuration-and-data-hash>/` and are not persisted to the
database.

## Commands and workflows

```bash
poetry run trade-tools refresh-adjusted-history
poetry run trade-tools strategy signals --date 2025-12-31 --output data/signals/signals.csv
poetry run trade-tools portfolio target --date 2025-12-31 --capital 10000
poetry run trade-tools strategy monthly-report
poetry run trade-tools operations monthly-decision --new-contribution 500
poetry run trade-tools operations monitor-refresh
poetry run trade-tools backtest run --universe-mode tradable
poetry run trade-tools backtest run --universe-mode research --ablation-off --include-holdout
poetry run trade-tools backtest compare <run_id> <run_id>
poetry run pytest
```

`monthly_decision.yml` runs at 08:00 UTC on days 1–3 after daily ingestion at
06:00 UTC. It refreshes full adjusted history, checks staleness, and creates an
idempotent persisted decision plus report; manual inputs support date,
contribution, and explicit regeneration. `monitoring_refresh.yml` runs weekly
and uploads the static monitoring report; it also refreshes a tradable
backtest snapshot. `backtest.yml` is manual-only and uploads a run directory.
All workflows read `DATABASE_URL` only from `secrets.DATABASE_URL`. GitHub
Actions cron timing can be delayed under load; scheduled workflows in public
repositories are disabled after 60 days without repository activity. Run the
same CLI commands locally when a scheduled run is delayed or disabled.

Tests use in-memory SQLite fixtures, mock Yahoo Finance, and replace the
configured database URL before importing application code. They abort if the
configured URL matches the `.env` production URL. `.env` remains ignored by Git.

## Not implemented / limitations

- No full-history comparison was made against production-stored observations;
  run the refresh before relying on old adjusted-close history.
- No empirical backtest was run here because no production or external market
  data connection was used. Run both configured modes and the ablation using
  the CLI; the resulting report includes metrics and bias disclosures.
- FX and market-close timestamp alignment are explicit approximations; broader
  currency and corporate-action verification is needed before live execution.
- Staggered tranches, broker integration, automatic order execution, ML, and
  complex web applications remain out of scope. See the Version 1b report for
  the transaction-derived holdings and static HTML monitoring implementation.

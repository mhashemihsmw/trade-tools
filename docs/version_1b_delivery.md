# Version 1b Operations — Delivery Report

## Status and go-live caveat

Version 1b adds a transaction-derived actual portfolio, immutable monthly
decision snapshots, rebalance proposals recorded as modelled ledger fills, additional risk checks,
and static HTML monitoring. The broker boundary remains manual: nothing in this
release sends an order.

The Version 1b prompt made implementation conditional on review of the Version
1a delivery, confirmation of the proposed classifications, and an empirical
baseline backtest judged worth operating. The classification table remains a
proposal and no production-data baseline backtest was run. This implementation
does **not** constitute that go-live approval. Review the classification
proposal and run/evaluate the baseline in an authorized environment before
relying on real-money recommendations.

## Architecture and additive schema

The existing SQLAlchemy `Base.metadata.create_all` initialization creates five
new tables and does not drop, rewrite, or add columns to existing market-data
tables:

- `transactions` — immutable trade/cash ledger; optional unique external
  reference; optional `reversal_of_id` links a correction to its source.
- `strategy_decisions` — strategy/version, canonical parameter hash, date,
  universe, generation, data cut-off, resolved parameter snapshot, status, and
  report path. A uniqueness constraint covers decision identity plus
  generation.
- `decision_items` — one ticker per decision, score/rank/selection/target,
  stored feature values, and exclusion reason.
- `decision_trades` — persisted non-executing proposed trade rows, so the live
  dashboard does not depend on files from the monthly workflow runner.
- `risk_assessments` — one row per decision, containing recommended and actual
  assessment outputs, configured limits, violations, and warnings.

No separate holdings table exists. Actual holdings, cash, cost basis, realized
P&L, and unrealized P&L are replayed from `transactions`. Backtests remain
hypothetical and do not write to the actual ledger.

The schema additions use the project's current additive initialization
approach; there is no separate migration framework in this repository. Run
`trade-tools init-db` after deployment to create missing tables. The database
URL is not included in logs/reports.

## Ledger and cost-basis rules

`buy` and `sell` quantities are positive share amounts; `deposit`, `withdrawal`,
`fee`, and `dividend` quantities are positive cash amounts in the stated
currency. Every transaction records `fx_to_eur` at execution. EUR entries must
use an FX rate of 1. Buys include fees in their EUR average cost; sells subtract
fees from proceeds. Average cost is quantity-weighted:

```text
new average cost = (old EUR cost basis + new EUR purchase cost) / new quantity
realized P&L = EUR sale proceeds - EUR sale fees - average cost of shares sold
unrealized P&L = latest EUR close value - remaining EUR cost basis
```

Cash purchases and withdrawals that exceed available cash and sales that
exceed held quantity are rejected; there is no shorting or margin. Cash-only
fees are entered as `fee` transactions. Dividends are recorded as cash income.
An incorrect entry is never edited/deleted: append `transaction reverse ID`,
then record the corrected entry if needed. Reversal pairs remain in the audit
table and are excluded from effective ledger replay.

`transaction import-csv` accepts `date,type,quantity` and optional
`ticker,price,fees,currency,fx_to_eur,reference,notes` columns. A unique
`reference` makes imports reject duplicate source rows; use stable source
references for repeatable imports.

Examples:

```bash
poetry run trade-tools transaction record --type deposit --date 2026-01-02 --quantity 10000
poetry run trade-tools transaction record --type buy --date 2026-01-05 \
  --ticker VWCE.DE --quantity 10 --price 120 --fees 2 --reference broker-123
poetry run trade-tools transaction list
poetry run trade-tools transaction reverse 12 --notes "Correct erroneous fill"
poetry run trade-tools transaction import-csv transactions.csv
poetry run trade-tools portfolio actual
```

Prices are marked in EUR using the Version 1a read/conversion layer. An
unpriced holding makes total portfolio valuation incomplete; the operating
decision refuses to allocate against an incomplete value.

## Monthly decisions and proposed trades

The operating command reuses Version 1a eligibility, ranking, exposure
deduplication, target weights, adjusted-history refresh, and proportional/fixed
cost configuration. It requires the `tradable` universe and checks stale data
before creating a new decision.

Recommended capital is actual market value plus recorded cash plus the optional
`--new-contribution`. The command computes whole-share quantities by default
(or fractional quantities when configured), applies minimum order size and
costs, and blocks proposed buys that cannot be funded by available cash plus
the contribution. The report separates actual holdings from recommended
targets and trades.

**Automatic ledger recording (default `--auto-record`).** When the proposal's
risk status is `valid`, the command writes the contribution as a `deposit` and
each proposed sell, then buy, as a `transaction` dated the decision date. They
are *modelled fills* at the proposal's close price with the estimated cost as
the fee, not broker confirmations; entries carry the reference
`decision-<id>-<side>-<ticker>` and a note saying so. The ledger is replayed
afterwards and the whole decision rolls back if it would oversell or overspend.
`--regenerate` reverses the superseded generation's entries before recording
the new ones, and a matching rerun records nothing twice. Use
`--no-auto-record` to keep proposals only and record real fills yourself;
correct any modelled fill with `transaction reverse` plus the actual fill.

Decision identity is strategy name/version, a SHA-256 parameter hash, resolved
decision date, and universe mode. The parameter hash includes strategy,
portfolio, backtest/cost parameters, and contribution. A repeated matching
decision returns the stored record without refreshing or rereading prices. Use
`--regenerate` to mark the earlier generation `superseded` and store a new
generation. `operations show-decision ID` replays stored parameters, feature
values, rankings, exclusions, targets, and risk without consulting current
prices, so provider revisions do not rewrite the recorded decision.

Example monthly run:

```bash
# Review/record actual cash and transactions first.
poetry run trade-tools transaction list
poetry run trade-tools operations monthly-decision --new-contribution 500
# The output contains a report path; fills are modelled and recorded in the ledger.
poetry run trade-tools operations show-decision 1
# If your real broker fills differ, reverse the modelled entry and record the real one:
poetry run trade-tools transaction reverse 12
poetry run trade-tools transaction record --type buy --date 2026-02-02 \
  --ticker VWCE.DE --quantity 2 --price 125 --fees 2
```

The workflow runs at 08:00 UTC on days 1–3 after daily ingestion at 06:00 UTC.
`monthly_decision.yml` invokes the CLI with `DATABASE_URL` from
`secrets.DATABASE_URL`, persists the idempotent decision in PostgreSQL, and
uploads the HTML/CSV report. Manual inputs support a date, new EUR contribution,
and deliberate regeneration. GitHub Actions cron can be delayed; scheduled
workflows in public repositories can be disabled after 60 days without
repository activity. The daily ingestion workflow and its schedule are
unchanged.

## Risk conventions

Historical risk uses point-in-time EUR daily returns and current fixed
portfolio weights. This is a historical fixed-weight estimate, not a forecast
or a fully replayed historical account path.

- Confidence defaults to 95%; horizons default to 1 and 21 trading days.
- Multi-day horizons use **overlapping** rolling windows, compounded as
  `product(1 + daily_return) - 1`.
- The empirical threshold is NumPy's **lower order statistic** at quantile
  `1 - confidence`.
- VaR is a positive loss: `max(0, -threshold_return)`.
- CVaR/Expected Shortfall is the positive loss from the arithmetic mean of all
  window returns `<= threshold_return`. All observations tied at the threshold
  are included.
- Default configured limits are 5% 1-day VaR, 20% 21-day CVaR, and 35%
  scenario loss. They are configuration defaults, not suitability advice.
- Configured static shocks apply exposure-group shocks first and otherwise
  asset-type shocks; unmatched assets receive the scenario's default (zero by
  default). The shipped scenario applies equity, bond, and gold shocks. These
  are hypothetical scenarios, not forecasts.

Insufficient historical return data and missing look-back observations are
reported as warnings. A configured limit breach makes the proposed risk
assessment invalid; the monthly command saves the assessment/report, omits
proposed orders, and exits non-zero. Risk validation does not optimize or
silently repair weights. Historical crisis-window replays are not implemented
in this phase.

## Static monitoring report

Generate a local static HTML snapshot on demand:

```bash
poetry run trade-tools operations monitor-refresh
```

The generated `reports/monitoring/index.html` contains:

1. **Strategy:** latest persisted ranking/selection and feature snapshot;
   backtest strategy/benchmark/holdout summaries, monthly returns, and ranking
   quality files when available. Backtest outputs remain hypothetical.
2. **Current portfolio and next allocation:** explicitly labeled actual
   holdings/weights/P&L/cash, latest recommended targets, proposed trades,
   decision-linked actual/recommended risk, and next decision date.
3. **Overall actual portfolio:** daily actual NAV, cumulative net contributions,
   realized/unrealized P&L, daily time-weighted return history, and saved
   historical VaR/CVaR/risk assessments.

Daily TWR uses end-of-day cash-flow treatment: each return is
`(ending NAV - same-day external deposits + same-day withdrawals) / prior NAV - 1`.
Deposits/withdrawals are external flows; buys, sells, fees, and dividends remain
inside portfolio NAV. Missing prices break the return interval rather than
being silently forward-valued as zero. The CLI command produces a static file.
A continuously hosted, authenticated web application can also be deployed
from the repository's `Dockerfile`; see the README's *Continuously hosted
dashboard* section. The web service renders the dashboard from current
database/report data on each request and refreshes the page every five minutes.
It requires `DATABASE_URL`, `DASHBOARD_USERNAME`, and a `DASHBOARD_PASSWORD`
of at least 16 characters. Monthly targets and proposed trades are persisted in
the database; backtest report panels use local `reports/backtests` files and
will be empty until those reports are present on the host. Place the service
behind HTTPS/TLS and use a host secret manager for credentials.

`monitoring_refresh.yml` runs weekly on Monday and on manual dispatch. It
refreshes a tradable-universe backtest snapshot, generates the HTML dashboard,
and uploads the report artifact. It uses only the existing database secret and
does not add cloud services.

## Files and commands

- Persistence: `src/trade_tools/db/models/operations.py`
- Ledger/accounting/proposals/TWR: `src/trade_tools/portfolio/operations.py`
- Historical risk and scenarios: `src/trade_tools/portfolio/risk.py`
- CLI and static monitoring: `src/trade_tools/cli.py`
- Authenticated web dashboard: `src/trade_tools/web.py`, `src/trade_tools/wsgi.py`,
  and `Dockerfile`
- Settings: `config/research.json`
- Workflows: `.github/workflows/monthly_decision.yml`,
  `.github/workflows/monitoring_refresh.yml`
- Future role briefs: `copilot/portfolio_ledger_agent.md`,
  `copilot/monthly_decisions_agent.md`, `copilot/operational_risk_agent.md`,
  `copilot/monitoring_workflows_agent.md` and matching `.github/agents/` files.

## Verification and limitations

The complete pytest suite, including existing ingestion and Version 1a tests,
was run with isolated SQLite and synthetic inputs: **46 passed**. An
end-to-end synthetic SQLite CLI smoke test covered transaction recording,
monthly decision persistence/idempotency, stored-decision replay, backtest run
and comparison, and dashboard generation. Workflow YAML and JSON configuration
were parsed, source/tests compiled, and `git diff --check` passed.

No production database was accessed, no live Yahoo request was made for
validation, and no empirical baseline/backtest was run. The classification
table remains a proposed mapping and must be reviewed. Other limitations:
dividend/corporate-action reconciliation depends on user-recorded transactions;
there is no broker integration, automatic execution, historical crisis replay,
intraday accounting, look-through fund exposure, or financial-advice guarantee.

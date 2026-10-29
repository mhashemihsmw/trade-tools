# Research-core Copilot agents

The role briefs in this folder are the durable project-specific instructions
for future work. Matching VS Code custom-agent entry points live in
[`../.github/agents/`](../.github/agents/); each entry point directs its agent
to the relevant brief below.

- [`research_data_access_agent.md`](./research_data_access_agent.md) — point-in-time database reads, FX, currency conversion, and adjusted execution prices.
- [`research_features_agent.md`](./research_features_agent.md) — pure features, feature panels, and leakage-resistant calculations.
- [`strategy_backtesting_agent.md`](./strategy_backtesting_agent.md) — strategy interface, selection, labels, walk-forward backtests, and metrics.
- [`portfolio_risk_agent.md`](./portfolio_risk_agent.md) — allocation, cash, caps, volatility targeting, and independent validation.
- [`research_cli_workflows_agent.md`](./research_cli_workflows_agent.md) — read-only research commands, reports, workflows, test safety, and documentation.
- [`portfolio_ledger_agent.md`](./portfolio_ledger_agent.md) — transaction imports/reversals, EUR average-cost accounting, cash, and derived actual holdings.
- [`monthly_decisions_agent.md`](./monthly_decisions_agent.md) — immutable point-in-time decision snapshots, idempotency, regeneration, and proposed trades.
- [`operational_risk_agent.md`](./operational_risk_agent.md) — historical VaR/CVaR, overlapping horizons, stress scenarios, and risk limits.
- [`monitoring_workflows_agent.md`](./monitoring_workflows_agent.md) — static dashboard generation, monthly/weekly workflows, and operations docs.

The source of truth is the implementation plus
[`../docs/research_core.md`](../docs/research_core.md) and
[`../docs/version_1b_delivery.md`](../docs/version_1b_delivery.md). The first
five briefs focus on Version 1a research components; the Version 1b briefs own
ledger, decision persistence, operational risk, and monitoring responsibilities.

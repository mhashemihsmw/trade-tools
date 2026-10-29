# Monitoring and Operations Workflows Agent

## Mission

Maintain the static HTML monitoring snapshot, monthly/weekly GitHub Actions
orchestration, and operational documentation.

## Owned components

- `operations monitor-refresh` in `src/trade_tools/cli.py`
- `.github/workflows/monthly_decision.yml`
- `.github/workflows/monitoring_refresh.yml`
- Operations docs and test-database safety

## Invariants

- The monitoring interface is a local/static report; no deployed service,
  external data service, or new cloud infrastructure.
- Label all values as actual or recommended; proposed trades are not executions.
- Show strategy/backtest, holdings/next allocation, and overall TWR/risk views.
- Monthly workflow is unique, scheduled after daily ingestion, idempotent, and
  never executes orders.
- Workflows only orchestrate CLI calls; credentials come from
  `secrets.DATABASE_URL`; never print or commit credentials.
- Preserve `daily_ingestion.yml` and its schedule.
- Document cron delays and the public-repository 60-day inactivity limitation.

## Working procedure

1. Read the CLI command, static report, workflows, and delivery docs.
2. Test rendering with isolated synthetic database data and saved backtest output.
3. Parse workflow YAML and JSON configuration.
4. Run focused tests and the complete suite.

## Out of scope

Do not add a web server, broker integration, or a second scheduled monthly
workflow.

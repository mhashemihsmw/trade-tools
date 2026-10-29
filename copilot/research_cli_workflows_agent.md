# Research CLI and Workflows Agent

## Mission

Maintain Click commands, resolved JSON configuration, report artifacts,
GitHub Actions orchestration, test-database safety, and directly related docs.

## Owned components

- `src/trade_tools/cli.py`
- `src/trade_tools/research_config.py`
- `config/research.json`
- `.github/workflows/monthly_decision.yml`
- `.github/workflows/monitoring_refresh.yml`
- `.github/workflows/backtest.yml`
- `tests/conftest.py` and CLI/workflow safety tests
- `README.md`, `docs/research_core.md`, and this `copilot/` folder

## Invariants

- Version 1a signals, portfolio targets, and backtests are read-only.
  Version 1b monthly decisions and actual transactions persist only through
  their designated commands; history refresh uses the existing ingestion path.
- Tests force isolated SQLite and abort if the configured URL matches `.env`
  production; never display connection strings.
- Workflows contain orchestration only and invoke CLI commands. Database
  credentials come only from `secrets.DATABASE_URL`.
- Preserve `daily_ingestion.yml` and its 06:00 UTC schedule.
- Reports state universe mode, data freshness, exclusions, risk results, and
  known biases. Run IDs and monthly guards must be deterministic.
- Keep workflow-dispatch backtests unscheduled; document public-repository
  scheduled-workflow inactivity behavior.

## Working procedure

1. Read the existing Click patterns, workflow, config schema, and test fixtures.
2. Test command help/options without making database/network requests.
3. Use mocked/in-memory CLI tests for report guards, stale-data failures, and
   config overrides.
4. Run YAML/config parsing checks and the full test suite.
5. Update user-facing docs for any command or workflow change.

## Out of scope

Do not put market-data business logic in workflows, add DB persistence for
research runs, commit `.env`, or alter the scheduled daily ingestion behavior.

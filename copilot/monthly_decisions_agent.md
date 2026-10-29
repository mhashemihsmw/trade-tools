# Monthly Decisions and Rebalancing Agent

## Mission

Maintain idempotent monthly decision generation, immutable feature/ranking
snapshots, proposed trades, and human-readable decision reports.

## Owned components

- `StrategyDecision`, `DecisionItem`, and decision-linked `RiskAssessment`
  persistence in `src/trade_tools/db/models/operations.py`
- `operations monthly-decision` and `operations show-decision`
- Monthly decision workflow and decision report tests
- Decision-operation sections in `docs/version_1b_delivery.md`

## Invariants

- Reuse Version 1a ranking, target construction, adjusted prices, and costs.
- Store decision-time features and resolved parameters so later price revisions
  cannot change a saved decision.
- Same strategy/version/parameter hash/date/universe returns the stored record.
- Only explicit `--regenerate` supersedes and increments a generation.
- Actual capital is actual market value plus recorded cash plus a prospective
  contribution; the contribution itself is not recorded until the user enters it.
- Recommended targets and trades are never executions; do not mutate the ledger.
- Enforce data-freshness checks before a new decision.

## Working procedure

1. Check exact uniqueness/idempotency and transaction boundaries.
2. Test duplicate runs, deliberate regeneration, and replay after price revision.
3. Test share rounding, minimum ticket size, estimates, cash availability, and
   invalid-risk rejection.
4. Run focused operations tests and the full suite.

## Out of scope

Do not execute orders, modify actual transactions, or overwrite decision
snapshots when prices/configuration later change.

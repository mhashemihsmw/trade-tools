---
name: "Monthly Decisions and Rebalancing"
description: "Use when changing monthly decision persistence, point-in-time feature snapshots, idempotent reruns, regeneration, proposed trades, or decision reports."
tools: [read, edit, search, execute]
---
Read `copilot/monthly_decisions_agent.md`, `docs/version_1b_delivery.md`, and
the Version 1a research guide before editing. Stored decision snapshots must be
replayable without loading current market prices. Never execute orders.

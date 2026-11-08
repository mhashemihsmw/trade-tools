# %% [markdown]
# # Monthly decision, step by step
# Run cell by cell in VS Code ("Run Cell" above each `# %%`). Reads prices from the
# database in `.env`; nothing is written. See docs/strategy.md for the method.

# %%
import matplotlib.pyplot as plt
import pandas as pd

from trade_tools.backtest import rank_ic, run_backtest
from trade_tools.db import get_session
from trade_tools.decision import make_decision
from trade_tools.prices import load_prices
from trade_tools.selection import candidate_universe, select_assets
from trade_tools.signals import compute_signals
from trade_tools.weights import compute_weights
from trade_tools.settings import load_settings, load_universe

settings = load_settings()
universe = load_universe()
with get_session() as session:
    prices = load_prices(session)
print(prices.adj_close.shape, "trading dates x tickers;", prices.notes)

# %% [markdown]
# ## 1. Candidates and their price history

# %%
candidates = candidate_universe(universe, settings)
as_of = pd.Timestamp("2026-10-30")
known = prices.adj_close.loc[:as_of]
(known[candidates.index].notna().sum(axis=1)).plot(title="Candidates with a price, per day")
candidates.groupby("group").size().sort_values(ascending=False).head(10)

# %% [markdown]
# ## 2. Signals at the decision date

# %%
signals = compute_signals(known, as_of, candidates.index, settings)
signals.sort_values("score", ascending=False).head(15)

# %% [markdown]
# ## 3. Selection: why each fund was or wasn't picked

# %%
table = select_assets(signals, universe, as_of, settings)
print(table["status"].value_counts())
table[["group", "status", "rank", "momentum", "volatility", "trend_distance", "score"]].head(15)

# %% [markdown]
# ## 4. Weights

# %%
weights, volatility = compute_weights(table, known, settings)
print(f"invested {weights.sum():.1%}, cash {1 - weights.sum():.1%}, est. volatility {volatility:.1%}")
weights.sort_values().plot.barh(title="Target weights")

# %%
# The same thing in one call (this is what `trade-tools decide` and the backtest use):
decision = make_decision(prices, universe, settings, as_of)
assert decision.weights.equals(weights)

# %% [markdown]
# ## 5. Backtest

# %%
result = run_backtest(prices, universe, settings)
result.metrics.round(3)

# %%
returns = result.monthly["return"].unstack("portfolio")
(1 + returns).cumprod().plot(logy=True, title="Growth of 1")
plt.show()

# %%
# Does the score predict next month's return? (Spearman rank IC per month)
ic = rank_ic(result.candidates)
print(f"mean IC {ic.mean():.3f}, positive in {(ic > 0).mean():.0%} of months")
ic.rolling(12).mean().plot(title="12-month rolling rank IC")

# %%
# Try a variant: e.g. no volatility target. Settings are immutable; `replace` makes a copy.
from dataclasses import replace

variant = run_backtest(prices, universe, replace(settings, volatility_target=0))
pd.concat({"base": result.metrics, "no_vol_target": variant.metrics}).xs("full", level="period").round(3)

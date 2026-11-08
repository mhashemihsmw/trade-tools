"""Choose which assets to hold at a decision date.

Every candidate gets a `status` explaining the outcome, checked in this order:
    no_data / short_history / stale_price / missing_signal   -> not eligible
    other_listing_in_group   another listing of the same index was preferred
    negative_momentum        momentum <= 0               (absolute_momentum_filter)
    below_trend              price <= trend average      (trend_filter)
    not_in_top_n             passed everything but ranked below top_n
    selected
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from trade_tools.settings import Settings


def candidate_universe(universe: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """Funds that may be chosen: all of config/universe.csv, or only tradable ones."""
    return universe.loc[universe["tradable"]] if settings.tradable_only else universe


def select_assets(
    signals: pd.DataFrame,
    universe: pd.DataFrame,
    as_of: pd.Timestamp,
    settings: Settings,
) -> pd.DataFrame:
    """Return signals joined with group info, a `status` per asset, `rank` and `selected`."""
    table = candidate_universe(universe, settings)[["group", "preferred"]].join(signals, how="left")
    table["status"] = pd.Series(pd.NA, index=table.index, dtype="string")

    def mark(mask: pd.Series, status: str) -> None:
        table.loc[mask & table["status"].isna(), "status"] = status

    stale_days = table["last_date"].map(
        lambda last: len(pd.bdate_range(last, as_of)) - 1 if pd.notna(last) else np.inf
    )
    mark(table["n_obs"].fillna(0) == 0, "no_data")
    mark(table["n_obs"] < settings.min_history_days, "short_history")
    mark(stale_days > settings.max_stale_days, "stale_price")
    mark(~np.isfinite(table["score"].astype(float)), "missing_signal")

    # Several ETFs often track the same index. Pick one per group, so the portfolio
    # doesn't flip between near-identical listings: preferred first, then longest history.
    eligible = table.loc[table["status"].isna()].sort_values(
        ["group", "preferred", "n_obs"], ascending=[True, False, False], kind="mergesort"
    )
    duplicates = eligible.index[eligible["group"].duplicated()]
    mark(table.index.isin(duplicates), "other_listing_in_group")
    # One eligible listing per group: the population for ranking and the equal-weight benchmark.
    table["representative"] = table["status"].isna()

    if settings.absolute_momentum_filter:
        mark(table["momentum"] <= 0, "negative_momentum")
    if settings.trend_filter:
        mark(table["trend_distance"] <= 0, "below_trend")

    passing = table.loc[table["status"].isna()].sort_index().sort_values(
        "score", ascending=False, kind="mergesort"
    )
    table["rank"] = np.nan
    table.loc[passing.index, "rank"] = np.arange(1, len(passing) + 1)
    mark(table.index.isin(passing.index[: settings.top_n]), "selected")
    mark(table.index.isin(passing.index[settings.top_n:]), "not_in_top_n")

    table["selected"] = table["status"].eq("selected")
    return table.sort_values(["selected", "score"], ascending=[False, False], na_position="last")

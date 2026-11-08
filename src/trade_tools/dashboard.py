"""Portfolio dashboard: one static HTML page built from the ledger and market prices.

`portfolio_summary` does the analysis (plain pandas, usable from a notebook);
`render_html` only formats it. The page has no JavaScript, so it also works under the
web app's strict Content-Security-Policy.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from trade_tools.ledger import holdings, portfolio_history
from trade_tools.prices import Prices

DECISIONS_DIR = Path("reports/decisions")


@dataclass
class PortfolioSummary:
    holdings: pd.DataFrame       # open positions with weights and day change
    closed: pd.DataFrame         # fully sold positions with realised P&L
    history: pd.DataFrame        # daily value, cash, net invested, TWR index
    benchmark: pd.Series         # benchmark growth index over the same days
    benchmark_name: str
    monthly_returns: pd.Series   # time-weighted return per calendar month
    stats: dict[str, float]
    transactions: pd.DataFrame
    warnings: list[str]


def latest_target_weights(folder: Path = DECISIONS_DIR) -> pd.Series | None:
    """Target weights from the newest saved decision (reports/decisions/<date>/candidates.csv)."""
    files = sorted(folder.glob("*/candidates.csv"))
    if not files:
        return None
    table = pd.read_csv(files[-1], index_col="ticker")
    weights = table.loc[table["weight"] > 0, "weight"]
    weights.attrs["date"] = files[-1].parent.name
    return weights


def portfolio_summary(
    transactions: pd.DataFrame,
    prices: Prices,
    benchmark_tickers: tuple[str, ...] = ("VWCE.DE",),
) -> PortfolioSummary:
    close = prices.close.ffill()
    latest = close.iloc[-1] if not close.empty else pd.Series(dtype=float)
    previous = close.shift(1).iloc[-1] if len(close) > 1 else pd.Series(dtype=float)
    table = holdings(transactions, latest)
    history = portfolio_history(transactions, prices.close)
    warnings: list[str] = []

    open_ = table.loc[table["units"] > 0].copy()
    closed = table.loc[(table["units"] == 0)].copy()
    total = history["value_eur"].iloc[-1] if not history.empty else 0.0
    open_["weight"] = open_["value_eur"] / total if total > 0 else np.nan
    open_["day_change"] = latest.reindex(open_.index) / previous.reindex(open_.index) - 1
    unpriced = open_.index[open_["price_eur"].isna()].tolist()
    if unpriced:
        warnings.append(f"No market price for {', '.join(unpriced)}; valued at last trade price.")
    if not history.empty and history["cash_eur"].min() < -0.01:
        warnings.append("Cash went negative at some point: a deposit is probably missing from the ledger.")

    benchmark, benchmark_name = pd.Series(dtype=float), ""
    for ticker in benchmark_tickers:
        series = prices.adj_close.get(ticker)
        if series is not None and not history.empty:
            aligned = series.reindex(history.index.union(series.index)).ffill().reindex(history.index)
            if aligned.notna().any():
                benchmark, benchmark_name = aligned / aligned.dropna().iloc[0], ticker
                break

    stats: dict[str, float] = {}
    if not history.empty:
        last = history.iloc[-1]
        twr = history["twr_index"]
        years = max((history.index[-1] - history.index[0]).days / 365.25, 1e-9)
        stats = {
            "value": last["value_eur"],
            "cash": last["cash_eur"],
            "net_invested": last["net_invested_eur"],
            "gain": last["gain_eur"],
            "gain_pct": last["gain_eur"] / last["net_invested_eur"] if last["net_invested_eur"] > 0 else np.nan,
            "twr": twr.iloc[-1] - 1,
            "twr_annual": twr.iloc[-1] ** (1 / years) - 1 if years >= 1 else np.nan,
            # today's gain excludes money deposited today
            "day_change": history["gain_eur"].diff().iloc[-1] if len(history) > 1 else np.nan,
            "day_return": history["daily_return"].iloc[-1],
            "max_drawdown": (twr / twr.cummax() - 1).min(),
            "volatility": history["daily_return"].std() * np.sqrt(252) if len(history) > 20 else np.nan,
            "realised": table["realised_pnl_eur"].sum(),
            "unrealised": open_["unrealised_pnl_eur"].sum(),
            "dividends": transactions.loc[transactions["type"] == "dividend", "cash_flow_eur"].sum(),
            "fees": (transactions["fees"] * transactions["fx_to_eur"]).sum()
            - transactions.loc[transactions["type"] == "fee", "cash_flow_eur"].sum(),
            "benchmark_return": benchmark.iloc[-1] - 1 if not benchmark.empty else np.nan,
        }
    monthly = (
        (1 + history["daily_return"].fillna(0)).groupby(history.index.to_period("M")).prod() - 1
        if not history.empty else pd.Series(dtype=float)
    )
    return PortfolioSummary(
        holdings=open_.sort_values("value_eur", ascending=False),
        closed=closed,
        history=history,
        benchmark=benchmark,
        benchmark_name=benchmark_name,
        monthly_returns=monthly,
        stats=stats,
        transactions=transactions,
        warnings=warnings,
    )


# --- formatting ----------------------------------------------------------------------

def _money(value: float) -> str:
    return f"€{value:,.2f}" if pd.notna(value) and np.isfinite(value) else "—"


def _pct(value: float, signed: bool = True) -> str:
    if pd.isna(value) or not np.isfinite(value):
        return "—"
    return f"{value:+.2%}" if signed else f"{value:.1%}"


def _tone(value: float) -> str:
    if pd.isna(value) or value == 0:
        return ""
    return "pos" if value > 0 else "neg"


def line_chart(series: dict[str, pd.Series], label: str, percent: bool = False) -> str:
    """Inline SVG line chart; `percent` plots (value - 1) as a percentage."""
    series = {name: s.dropna() for name, s in series.items() if s is not None and s.notna().any()}
    if not series:
        return '<p class="empty">Not enough history yet.</p>'
    if percent:
        series = {name: (s - 1) * 100 for name, s in series.items()}
    width, height, left, right, top, bottom = 900, 280, 72, 16, 16, 34
    values = np.concatenate([s.to_numpy(float) for s in series.values()])
    low, high = float(values.min()), float(values.max())
    pad = max(high - low, 1e-9) * 0.08
    low, high = low - pad, high + pad
    start = min(s.index[0] for s in series.values())
    end = max(s.index[-1] for s in series.values())
    span = max((end - start).total_seconds(), 1.0)

    def x(t: pd.Timestamp) -> float:
        return left + (t - start).total_seconds() / span * (width - left - right)

    def y(v: float) -> float:
        return top + (high - v) / (high - low) * (height - top - bottom)

    parts = [f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(label)}">']
    for fraction in (0, 0.25, 0.5, 0.75, 1):
        v = low + fraction * (high - low)
        text = f"{v:.0f}%" if percent else f"€{v:,.0f}"
        parts.append(
            f'<line x1="{left}" x2="{width - right}" y1="{y(v):.1f}" y2="{y(v):.1f}" class="grid"/>'
            f'<text x="{left - 8}" y="{y(v) + 4:.1f}" text-anchor="end">{text}</text>'
        )
    if percent and low < 0 < high:
        parts.append(f'<line x1="{left}" x2="{width - right}" y1="{y(0):.1f}" y2="{y(0):.1f}" class="zero"/>')
    legend = []
    for i, (name, s) in enumerate(series.items()):
        points = " ".join(f"{x(t):.1f},{y(v):.1f}" for t, v in s.items())
        parts.append(f'<polyline points="{points}" class="line c{i % 5}"/>')
        legend.append(f'<span><i class="c{i % 5}"></i>{html.escape(name)}</span>')
    parts.append(
        f'<text x="{left}" y="{height - 10}">{start.date()}</text>'
        f'<text x="{width - right}" y="{height - 10}" text-anchor="end">{end.date()}</text></svg>'
    )
    return "".join(parts) + f'<div class="legend">{"".join(legend)}</div>'


def _table(headers: list[str], rows: list[list[str]], empty: str) -> str:
    if not rows:
        return f'<p class="empty">{html.escape(empty)}</p>'
    head = "".join(f"<th>{h}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in rows)
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def render_html(
    summary: PortfolioSummary,
    *,
    names: dict[str, str] | None = None,
    target_weights: pd.Series | None = None,
    generated_at: datetime | None = None,
) -> str:
    names = names or {}
    s = summary.stats
    generated = (generated_at or datetime.now(timezone.utc)).strftime("%d %b %Y, %H:%M UTC")

    def card(title: str, value: str, sub: str = "", tone: str = "") -> str:
        return (f'<div class="card"><div class="label">{title}</div>'
                f'<div class="metric {tone}">{value}</div><div class="sub">{sub}</div></div>')

    cards = "".join([
        card("Portfolio value", _money(s.get("value", np.nan)),
             f"incl. cash {_money(s.get('cash', np.nan))}"),
        card("Net invested", _money(s.get("net_invested", np.nan)), "deposits minus withdrawals"),
        card("Total gain", _money(s.get("gain", np.nan)),
             f"{_pct(s.get('gain_pct', np.nan))} of net invested", _tone(s.get("gain", np.nan))),
        card("Time-weighted return", _pct(s.get("twr", np.nan)),
             f"annualised {_pct(s.get('twr_annual', np.nan))} · "
             f"{html.escape(summary.benchmark_name or 'benchmark')} {_pct(s.get('benchmark_return', np.nan))}",
             _tone(s.get("twr", np.nan))),
        card("Today", _money(s.get("day_change", np.nan)), _pct(s.get("day_return", np.nan)),
             _tone(s.get("day_change", np.nan))),
        card("Max drawdown", _pct(s.get("max_drawdown", np.nan)),
             f"volatility {_pct(s.get('volatility', np.nan), signed=False)} p.a."),
    ])

    show_target = target_weights is not None
    holding_rows = []
    for ticker, row in summary.holdings.iterrows():
        cells = [
            f"<strong>{html.escape(ticker)}</strong><small>{html.escape(names.get(ticker, ''))}</small>",
            f"{row['units']:,.4g}", _money(row["avg_cost_eur"]), _money(row["price_eur"]),
            _money(row["value_eur"]), _pct(row["weight"], signed=False),
        ]
        if show_target:
            cells.append(_pct(target_weights.get(ticker, 0.0), signed=False))
        cells += [
            f'<span class="{_tone(row["unrealised_pnl_eur"])}">{_money(row["unrealised_pnl_eur"])}</span>',
            f'<span class="{_tone(row["unrealised_return"])}">{_pct(row["unrealised_return"])}</span>',
            f'<span class="{_tone(row["day_change"])}">{_pct(row["day_change"])}</span>',
        ]
        holding_rows.append(cells)
    if s:
        cash_share = s["cash"] / s["value"] if s["value"] > 0 else np.nan
        cash_row = ["<strong>Cash</strong>", "", "", "", _money(s["cash"]), _pct(cash_share, signed=False)]
        if show_target:
            cash_row.append(_pct(max(0.0, 1 - float(target_weights.sum())), signed=False))
        holding_rows.append(cash_row + ["", "", ""])
    if show_target:
        for ticker in target_weights.index.difference(summary.holdings.index):
            holding_rows.append([
                f"<strong>{html.escape(ticker)}</strong><small>target only, not held</small>",
                "", "", "", "", "", _pct(target_weights[ticker], signed=False), "", "", "",
            ])
    holding_headers = ["Asset", "Units", "Avg cost", "Price", "Value", "Weight"]
    holding_headers += ["Target"] if show_target else []
    holding_headers += ["Unrealised P&amp;L", "Return", "Today"]

    closed_rows = [
        [html.escape(t), _money(r["realised_pnl_eur"])] for t, r in summary.closed.iterrows()
    ]
    month_rows = [
        [str(period), f'<span class="{_tone(v)}">{_pct(v)}</span>']
        for period, v in summary.monthly_returns.tail(12).iloc[::-1].items()
    ]
    tx_rows = [
        [str(t["date"].date()), t["type"], html.escape(t["ticker"] if isinstance(t["ticker"], str) else ""), f"{t['quantity']:,.4g}",
         _money(t["price"] * t["fx_to_eur"]) if pd.notna(t["price"]) else "",
         _money(t["cash_flow_eur"])]
        for _, t in summary.transactions.sort_values(["date", "id"]).tail(15).iloc[::-1].iterrows()
    ]
    target_note = (
        f'<p class="note">Target = latest monthly decision ({html.escape(target_weights.attrs.get("date", ""))}).</p>'
        if show_target else ""
    )
    warnings = "".join(f'<div class="warn">{html.escape(w)}</div>' for w in summary.warnings)
    history = summary.history
    value_chart = line_chart(
        {"Portfolio value": history.get("value_eur"), "Net invested": history.get("net_invested_eur")},
        "Portfolio value and net invested",
    )
    return_chart = line_chart(
        {"Portfolio (time-weighted)": history.get("twr_index"), summary.benchmark_name or "Benchmark": summary.benchmark},
        "Cumulative return versus benchmark", percent=True,
    )
    income = (f"Realised P&amp;L {_money(s.get('realised', np.nan))} · Dividends {_money(s.get('dividends', np.nan))}"
              f" · Fees paid {_money(s.get('fees', np.nan))}") if s else ""

    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Portfolio</title>
<style>
:root{{--ink:#172033;--muted:#6b7688;--line:#e3e8ef;--paper:#fff;--bg:#f4f6fa;--pos:#08875f;--neg:#c23b4e;
--c0:#2563eb;--c1:#94a3b8;--c2:#e08a1e;--c3:#8b5cf6;--c4:#0891b2}}
@media (prefers-color-scheme:dark){{:root{{--ink:#e6eaf2;--muted:#98a2b3;--line:#2a3242;--paper:#161b26;--bg:#0e121a;
--pos:#34d399;--neg:#f87171;--c1:#64748b}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}}
main{{max-width:1180px;margin:auto;padding:32px 16px 48px}}header{{display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap;gap:8px}}
h1{{margin:0;font-size:26px;letter-spacing:-.02em}}h2{{margin:0 0 4px;font-size:16px}}.stamp,.note,.sub,.label,small{{color:var(--muted)}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin:20px 0}}
.card,section{{background:var(--paper);border:1px solid var(--line);border-radius:12px}}.card{{padding:14px 16px}}
.label{{font-size:12px;font-weight:600}}.metric{{font-size:22px;font-weight:700;letter-spacing:-.02em;font-variant-numeric:tabular-nums}}.sub{{font-size:12px}}
section{{padding:18px;margin-top:14px}}.grid2{{display:grid;grid-template-columns:2fr 1fr;gap:14px}}.grid2 section{{margin-top:0}}
@media(max-width:820px){{.grid2{{grid-template-columns:1fr}}}}
svg{{width:100%;height:auto;display:block;margin-top:8px}}svg text{{fill:var(--muted);font-size:11px}}.grid{{stroke:var(--line)}}.zero{{stroke:var(--muted);stroke-dasharray:4 4}}
.line{{fill:none;stroke-width:2}}polyline.c0{{stroke:var(--c0)}}polyline.c1{{stroke:var(--c1)}}polyline.c2{{stroke:var(--c2)}}polyline.c3{{stroke:var(--c3)}}polyline.c4{{stroke:var(--c4)}}
.legend{{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:var(--muted)}}.legend i{{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:5px}}
i.c0{{background:var(--c0)}}i.c1{{background:var(--c1)}}i.c2{{background:var(--c2)}}i.c3{{background:var(--c3)}}i.c4{{background:var(--c4)}}
.scroll{{overflow-x:auto}}table{{width:100%;border-collapse:collapse;white-space:nowrap;font-variant-numeric:tabular-nums}}
th{{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);text-align:right;padding:8px 10px;border-bottom:1px solid var(--line)}}
td{{text-align:right;padding:9px 10px;border-bottom:1px solid var(--line)}}th:first-child,td:first-child{{text-align:left}}td small{{display:block;font-size:11px}}
.pos{{color:var(--pos)}}.neg{{color:var(--neg)}}.empty{{color:var(--muted);text-align:center;padding:18px}}
.warn{{background:#fff4d6;color:#7a5200;border-radius:8px;padding:8px 12px;margin-top:8px}}
</style></head><body><main>
<header><h1>Portfolio</h1><span class="stamp">Updated {generated} · values in EUR at latest close</span></header>
{warnings}
<div class="cards">{cards}</div>
<section><h2>Value and money invested</h2>{value_chart}</section>
<section><h2>Cumulative time-weighted return</h2><p class="note">Excludes the effect of deposits and withdrawals, so it is comparable with the benchmark.</p>{return_chart}</section>
<section><h2>Holdings</h2>{target_note}{_table(holding_headers, holding_rows, "No holdings yet. Record trades with: trade-tools tx add")}<p class="note">{income}</p></section>
<div class="grid2" style="margin-top:14px">
<section><h2>Recent transactions</h2>{_table(["Date", "Type", "Asset", "Quantity", "Price", "Cash effect"], tx_rows, "No transactions recorded.")}</section>
<section><h2>Monthly returns</h2>{_table(["Month", "Return"], month_rows, "No history yet.")}
{('<h2 style="margin-top:16px">Closed positions</h2>' + _table(["Asset", "Realised P&amp;L"], closed_rows, "")) if closed_rows else ""}</section>
</div>
</main></body></html>"""


def build_dashboard(
    output: Path | None = None,
    *,
    settings_path: str | Path | None = None,
    decisions_dir: Path = DECISIONS_DIR,
) -> str:
    """Load ledger and prices from the database, render the page, optionally write it."""
    from sqlalchemy import select

    from trade_tools.db import get_session, init_db
    from trade_tools.db.models import Asset
    from trade_tools.ledger import load_transactions
    from trade_tools.prices import load_prices
    from trade_tools.settings import DEFAULT_SETTINGS_PATH, load_settings

    settings = load_settings(settings_path or DEFAULT_SETTINGS_PATH)
    init_db()
    with get_session() as session:
        transactions = load_transactions(session)
        # Only what the page needs: traded funds + benchmark, from shortly before the
        # first transaction (a few days earlier so the first day has a previous close).
        tickers = [*transactions["ticker"].dropna().unique(), *settings.benchmark]
        start = transactions["date"].min() - pd.Timedelta(days=10) if not transactions.empty else None
        prices = load_prices(session, tickers=tickers, start=start)
        names = dict(session.execute(select(Asset.ticker, Asset.name)).all())
    summary = portfolio_summary(transactions, prices, settings.benchmark)
    page = render_html(
        summary,
        names={ticker: name or "" for ticker, name in names.items()},
        target_weights=latest_target_weights(decisions_dir),
    )
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(page, encoding="utf-8")
    return page

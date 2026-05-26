"""
src/ui/intl_etf_analysis.py
────────────────────────────
Backend for the 🌍 Intl ETFs Streamlit tab.

All public functions return plain DataFrames / Plotly figures so Streamlit can
cache and display them without touching ClickHouse on every rerun.
"""
from __future__ import annotations

import warnings
warnings.filterwarnings("ignore")

from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
import lightgbm as lgb

# ── Constants ─────────────────────────────────────────────────────────────────

INTL_ETFS = ["MAFANG", "HNGSNGBEES", "MON100", "MASPTOP50", "MAHKTECH", "MONQ50"]

ETF_LABELS = {
    "MAFANG":     "MAFANG · China Tech",
    "HNGSNGBEES": "HNGSNGBEES · Hang Seng",
    "MON100":     "MON100 · Nasdaq 100",
    "MASPTOP50":  "MASPTOP50 · S&P 500",
    "MAHKTECH":   "MAHKTECH · HK Tech",
    "MONQ50":     "MONQ50 · Nasdaq 50",
}

# MASPTOP50 NAV is on a different unit base — exclude from premium analysis
PREMIUM_EXCLUDE = {"MASPTOP50"}

RISK_FREE_ANNUAL = 0.065
TODAY = date.today()
START_3Y = TODAY - timedelta(days=3 * 365)

# ── Data loading ──────────────────────────────────────────────────────────────

def load_data(pool) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (price_wide, nav_wide) pivoted on date, columns = symbol."""
    with pool.acquire() as ch:
        price_rows = ch.query(
            """
            SELECT symbol, trade_date, close
            FROM market_data.daily_prices
            WHERE symbol IN {syms:Array(String)}
              AND trade_date >= {start:Date}
            ORDER BY symbol, trade_date
            """,
            parameters={"syms": INTL_ETFS + ["USDINR"], "start": START_3Y},
        ).result_rows

        nav_rows = ch.query(
            """
            SELECT symbol, nav_date, nav
            FROM market_data.mf_nav
            WHERE symbol IN {syms:Array(String)}
              AND nav_date >= {start:Date}
            ORDER BY symbol, nav_date
            """,
            parameters={"syms": INTL_ETFS, "start": START_3Y},
        ).result_rows

    def _wide(rows, cols):
        df = pd.DataFrame(rows, columns=cols)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").groupby(["date", "symbol"], as_index=False).last()
        return df.pivot(index="date", columns="symbol", values=cols[2]).sort_index()

    return _wide(price_rows, ["symbol", "date", "close"]), _wide(nav_rows, ["symbol", "date", "nav"])


def _premium_series(price_wide: pd.DataFrame, nav_wide: pd.DataFrame) -> pd.DataFrame:
    premiums = {}
    for sym in INTL_ETFS:
        if sym in PREMIUM_EXCLUDE:
            continue
        if sym not in price_wide.columns or sym not in nav_wide.columns:
            continue
        merged = pd.merge_asof(
            price_wide[sym].dropna().rename("price").reset_index(),
            nav_wide[sym].dropna().rename("nav").reset_index(),
            on="date", direction="backward",
        ).dropna()
        merged["premium"] = (merged["price"] / merged["nav"] - 1) * 100
        premiums[sym] = merged.set_index("date")["premium"]
    return pd.DataFrame(premiums)


# ── Section 1: Performance ────────────────────────────────────────────────────

def compute_performance(price_wide: pd.DataFrame) -> pd.DataFrame:
    etfs = [c for c in price_wide.columns if c in INTL_ETFS]
    rf_daily = RISK_FREE_ANNUAL / 252
    rows = []
    for sym in etfs:
        s = price_wide[sym].dropna()
        if len(s) < 60:
            continue
        ret = s.pct_change().dropna()
        r3y = (s.iloc[-1] / s.iloc[0] - 1) * 100
        r1y = (s.iloc[-1] / s[s.index >= s.index[-1] - pd.Timedelta(days=365)].iloc[0] - 1) * 100
        r6m = (s.iloc[-1] / s[s.index >= s.index[-1] - pd.Timedelta(days=182)].iloc[0] - 1) * 100
        ann_vol = ret.std() * np.sqrt(252) * 100
        excess = ret - rf_daily
        sharpe = (excess.mean() / excess.std()) * np.sqrt(252) if excess.std() > 0 else np.nan
        roll_max = s.cummax()
        max_dd = ((s - roll_max) / roll_max).min() * 100
        ann_ret = (1 + r3y / 100) ** (1 / 3) - 1
        calmar = ann_ret / abs(max_dd / 100) if max_dd < 0 else np.nan
        rows.append({
            "ETF": ETF_LABELS.get(sym, sym),
            "3Y Ret %": round(r3y, 1),
            "1Y Ret %": round(r1y, 1),
            "6M Ret %": round(r6m, 1),
            "Ann Vol %": round(ann_vol, 1),
            "Sharpe": round(sharpe, 2),
            "Max DD %": round(max_dd, 1),
            "Calmar": round(calmar, 2),
            "_sym": sym,
        })
    return pd.DataFrame(rows)


def perf_bar_chart(perf: pd.DataFrame) -> go.Figure:
    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=["Total Return (%)", "Annualised Volatility (%)", "Sharpe Ratio"],
    )
    colors = px.colors.qualitative.Set2
    labels = perf["ETF"].str.split(" · ").str[0]

    for col_idx, (col, bar_color) in enumerate([
        ("3Y Ret %", None), ("Ann Vol %", None), ("Sharpe", None)
    ], start=1):
        bar_colors = [
            colors[i % len(colors)] for i in range(len(perf))
        ]
        fig.add_trace(
            go.Bar(x=labels, y=perf[col], marker_color=bar_colors, showlegend=False),
            row=1, col=col_idx,
        )

    fig.update_layout(height=350, margin=dict(t=40, b=20), template="plotly_white")
    return fig


# ── Section 2: Premium Analysis ───────────────────────────────────────────────

def compute_premium_stats(price_wide: pd.DataFrame, nav_wide: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Return (stats_df, premium_series_wide, anomaly_dates_per_sym)."""
    prem = _premium_series(price_wide, nav_wide)
    iso = IsolationForest(contamination=0.05, random_state=42)
    stats_rows = []
    anomaly_dates: dict[str, pd.DatetimeIndex] = {}

    for sym in prem.columns:
        s = prem[sym].dropna()
        if len(s) < 30:
            continue
        feat = pd.DataFrame({"p": s, "dp": s.diff()}).dropna()
        labels = iso.fit_predict(feat.values)
        anom_idx = feat.index[labels == -1]
        anomaly_dates[sym] = anom_idx

        slope, _, _, _, _ = stats.linregress(np.arange(len(s)), s.values)
        stats_rows.append({
            "ETF": ETF_LABELS.get(sym, sym),
            "Mean %": round(s.mean(), 2),
            "Std %": round(s.std(), 2),
            "Min %": round(s.min(), 2),
            "Max %": round(s.max(), 2),
            "Current %": round(s.iloc[-1], 2),
            "Trend /mo": round(slope * 30, 2),
            "Anomaly Days": int((labels == -1).sum()),
            "_sym": sym,
        })

    return pd.DataFrame(stats_rows), prem, anomaly_dates


def premium_chart(prem: pd.DataFrame, anomaly_dates: dict) -> go.Figure:
    syms = [c for c in prem.columns]
    n = len(syms)
    fig = make_subplots(rows=n, cols=1, shared_xaxes=True,
                        subplot_titles=[ETF_LABELS.get(s, s) for s in syms],
                        vertical_spacing=0.04)
    palette = px.colors.qualitative.Plotly

    for i, sym in enumerate(syms, start=1):
        s = prem[sym].dropna()
        color = palette[(i - 1) % len(palette)]

        # Mean ± 1σ band
        mu, sigma = s.mean(), s.std()
        fig.add_trace(go.Scatter(
            x=s.index, y=[mu + sigma] * len(s), mode="lines",
            line=dict(width=0), showlegend=False, hoverinfo="skip",
        ), row=i, col=1)
        fig.add_trace(go.Scatter(
            x=s.index, y=[mu - sigma] * len(s), mode="lines",
            fill="tonexty", fillcolor="rgba(150,150,150,0.15)",
            line=dict(width=0), showlegend=False, hoverinfo="skip",
        ), row=i, col=1)

        fig.add_trace(go.Scatter(
            x=s.index, y=s.values, name=sym, line=dict(color=color, width=1.2),
            hovertemplate="%{x|%b %d %Y}: %{y:.2f}%<extra></extra>",
        ), row=i, col=1)
        fig.add_hline(y=mu, line_dash="dot", line_color="gray", line_width=0.8, row=i, col=1)

        if sym in anomaly_dates and len(anomaly_dates[sym]) > 0:
            anom_vals = s.reindex(anomaly_dates[sym]).dropna()
            fig.add_trace(go.Scatter(
                x=anom_vals.index, y=anom_vals.values, mode="markers",
                marker=dict(color="red", size=5, symbol="x"),
                name=f"{sym} anomaly", showlegend=False,
                hovertemplate="%{x|%b %d}: %{y:.2f}% ⚠<extra></extra>",
            ), row=i, col=1)

    fig.update_layout(
        height=280 * n, title_text="Scarcity Premium % (Market Price / NAV − 1)",
        template="plotly_white", margin=dict(t=60, b=20),
        legend=dict(orientation="h", y=1.02),
    )
    return fig


# ── Section 3: Regime Detection ───────────────────────────────────────────────

def compute_regimes(price_wide: pd.DataFrame, prem: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    etfs = [c for c in price_wide.columns if c in INTL_ETFS]
    NAMES = {0: "Bear", 1: "Sideways", 2: "Bull"}
    summary_rows = []
    regime_series: dict[str, pd.Series] = {}

    for sym in etfs:
        s = price_wide[sym].dropna()
        if len(s) < 120:
            continue
        ret = s.pct_change()
        feat = pd.DataFrame({
            "ret_30d": ret.rolling(30).mean(),
            "vol_30d": ret.rolling(30).std(),
            "momentum": ret.rolling(5).mean() - ret.rolling(20).mean(),
        })
        if sym in prem.columns:
            feat["premium"] = prem[sym]
        feat = feat.dropna()
        X = StandardScaler().fit_transform(feat.values)
        km = KMeans(n_clusters=3, random_state=42, n_init=10)
        labels = km.fit_predict(X)
        cluster_ret = {c: feat["ret_30d"].values[labels == c].mean() for c in range(3)}
        remap = {sorted(cluster_ret, key=cluster_ret.get)[k]: k for k in range(3)}
        rs = pd.Series([remap[l] for l in labels], index=feat.index)
        regime_series[sym] = rs

        counts = rs.value_counts(normalize=True) * 100
        current = int(rs.iloc[-1])
        streak = sum(1 for v in reversed(rs.values) if v == current)
        summary_rows.append({
            "ETF": ETF_LABELS.get(sym, sym),
            "Bear %": round(counts.get(0, 0)),
            "Sideways %": round(counts.get(1, 0)),
            "Bull %": round(counts.get(2, 0)),
            "Current": NAMES[current],
            "Days in Current": streak,
            "_sym": sym,
        })

    return pd.DataFrame(summary_rows), regime_series


def regime_chart(price_wide: pd.DataFrame, regime_series: dict) -> go.Figure:
    syms = list(regime_series.keys())
    n = len(syms)
    fig = make_subplots(rows=n, cols=1, shared_xaxes=True,
                        subplot_titles=[ETF_LABELS.get(s, s) for s in syms],
                        vertical_spacing=0.04)
    REGIME_COLORS = {0: "rgba(255,80,80,0.25)", 1: "rgba(255,210,50,0.20)", 2: "rgba(60,180,75,0.25)"}

    for i, sym in enumerate(syms, start=1):
        s = price_wide[sym].dropna()
        rs = regime_series[sym].reindex(s.index, method="ffill").dropna()

        # Shade regime bands
        prev_regime = None
        band_start = None
        for d in rs.index:
            r = rs[d]
            if r != prev_regime:
                if prev_regime is not None:
                    fig.add_vrect(x0=band_start, x1=d,
                                  fillcolor=REGIME_COLORS[prev_regime],
                                  layer="below", line_width=0, row=i, col=1)
                prev_regime = r
                band_start = d
        if prev_regime is not None:
            fig.add_vrect(x0=band_start, x1=rs.index[-1],
                          fillcolor=REGIME_COLORS[prev_regime],
                          layer="below", line_width=0, row=i, col=1)

        fig.add_trace(go.Scatter(
            x=s.index, y=s.values, name=sym,
            line=dict(color="#2c7bb6", width=1.2), showlegend=False,
            hovertemplate="%{x|%b %Y}: ₹%{y:.2f}<extra>" + sym + "</extra>",
        ), row=i, col=1)

    fig.update_layout(
        height=260 * n, title_text="Price + Regime (🔴 Bear · 🟡 Sideways · 🟢 Bull)",
        template="plotly_white", margin=dict(t=60, b=20),
    )
    return fig


# ── Section 4: Correlation ────────────────────────────────────────────────────

def compute_correlation(price_wide: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    etfs = [c for c in price_wide.columns if c in INTL_ETFS]
    cols = etfs + (["USDINR"] if "USDINR" in price_wide.columns else [])
    ret = price_wide[cols].pct_change().dropna()
    full_corr = ret.corr()

    # USDINR rolling correlation (last 180 days)
    usdinr_rows = []
    if "USDINR" in ret.columns:
        cutoff = ret.index[-1] - pd.Timedelta(days=180)
        recent = ret.loc[ret.index >= cutoff]
        for sym in etfs:
            if sym in recent.columns:
                usdinr_rows.append({
                    "ETF": ETF_LABELS.get(sym, sym),
                    "Full-Period": round(ret[sym].corr(ret["USDINR"]), 3),
                    "Last 6M": round(recent[sym].corr(recent["USDINR"]), 3),
                })

    return full_corr, pd.DataFrame(usdinr_rows)


def correlation_heatmap(corr: pd.DataFrame) -> go.Figure:
    short = {s: s[:8] for s in corr.columns}
    labels = [short.get(c, c) for c in corr.columns]
    z = corr.values.round(2)
    fig = go.Figure(go.Heatmap(
        z=z, x=labels, y=labels,
        colorscale="RdBu", zmid=0, zmin=-1, zmax=1,
        text=z, texttemplate="%{text:.2f}",
        hovertemplate="%{x} × %{y}: %{z:.3f}<extra></extra>",
        colorbar=dict(title="ρ"),
    ))
    fig.update_layout(
        title="Return Correlation Matrix (3Y daily)",
        height=420, template="plotly_white",
        margin=dict(t=50, b=20),
    )
    return fig


# ── Section 5: Seasonality ────────────────────────────────────────────────────

def compute_seasonality(price_wide: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    etfs = [c for c in price_wide.columns if c in INTL_ETFS]
    # Monthly returns (resample to month-end, then pct_change)
    monthly = price_wide[etfs].resample("ME").last().pct_change().dropna() * 100
    monthly["month"] = monthly.index.month
    MNAMES = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
              7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
    med = monthly.groupby("month")[etfs].median()
    med.index = [MNAMES[m] for m in med.index]

    # Best/worst per ETF
    bw_rows = []
    for sym in etfs:
        s = med[sym]
        bw_rows.append({
            "ETF": ETF_LABELS.get(sym, sym),
            "Best Month": s.idxmax(),
            "Best Ret %": round(s.max(), 2),
            "Worst Month": s.idxmin(),
            "Worst Ret %": round(s.min(), 2),
            "Apr-Sep avg %": round(s[["Apr","May","Jun","Jul","Aug","Sep"]].mean(), 2),
            "Oct-Mar avg %": round(s[["Oct","Nov","Dec","Jan","Feb","Mar"]].mean(), 2),
        })
    return med, pd.DataFrame(bw_rows)


def seasonality_heatmap(med: pd.DataFrame) -> go.Figure:
    short_labels = {s: s for s in med.columns}
    fig = go.Figure(go.Heatmap(
        z=med.values.T,
        x=med.index.tolist(),
        y=[ETF_LABELS.get(s, s).split(" · ")[0] for s in med.columns],
        colorscale="RdYlGn", zmid=0,
        text=med.values.T.round(1),
        texttemplate="%{text}%",
        hovertemplate="%{y} · %{x}: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Median<br>Ret %"),
    ))
    fig.update_layout(
        title="Median Monthly Return % (3Y)",
        height=350, template="plotly_white",
        margin=dict(t=50, b=20),
        xaxis=dict(side="top"),
    )
    return fig


# ── Section 6: LightGBM ───────────────────────────────────────────────────────

def compute_lgbm(price_wide: pd.DataFrame, prem: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    etfs = [c for c in price_wide.columns if c in INTL_ETFS]
    usdinr = price_wide["USDINR"].pct_change() if "USDINR" in price_wide.columns else None
    summary_rows = []
    importance_data: dict[str, pd.Series] = {}

    for sym in etfs:
        s = price_wide[sym].dropna()
        if len(s) < 200:
            continue
        ret = s.pct_change()
        feat = pd.DataFrame(index=ret.index)
        feat["ret_1d"]   = ret.shift(1)
        feat["ret_5d"]   = ret.rolling(5).mean().shift(1)
        feat["ret_10d"]  = ret.rolling(10).mean().shift(1)
        feat["ret_20d"]  = ret.rolling(20).mean().shift(1)
        feat["vol_10d"]  = ret.rolling(10).std().shift(1)
        feat["vol_30d"]  = ret.rolling(30).std().shift(1)
        feat["momentum"] = (ret.rolling(5).mean() - ret.rolling(20).mean()).shift(1)
        feat["month"]    = feat.index.month
        if usdinr is not None:
            feat["usdinr_1d"] = usdinr.shift(1)
            feat["usdinr_5d"] = usdinr.rolling(5).mean().shift(1)
        if sym in prem.columns:
            p = prem[sym]
            feat["premium"]        = p.shift(1)
            feat["premium_zscore"] = ((p - p.rolling(60).mean()) / p.rolling(60).std()).shift(1)
            feat["premium_chg"]    = p.diff().shift(1)

        fwd = ret.rolling(5).sum().shift(-5)
        target = (fwd > 0).astype(int)
        df = pd.concat([feat, target.rename("target")], axis=1).dropna()
        if len(df) < 100:
            continue

        X, y = df.drop("target", axis=1), df["target"]
        importances = np.zeros(X.shape[1])
        accs = []
        for tr, te in TimeSeriesSplit(n_splits=3).split(X):
            m = lgb.LGBMClassifier(n_estimators=200, learning_rate=0.05, num_leaves=16,
                                   min_child_samples=20, random_state=42, verbose=-1)
            m.fit(X.iloc[tr], y.iloc[tr])
            importances += m.feature_importances_
            accs.append((m.predict(X.iloc[te]) == y.iloc[te]).mean())

        importances /= 3
        fi = pd.Series(importances, index=X.columns).sort_values(ascending=False)
        importance_data[sym] = fi
        p_rank = int(fi.index.get_loc("premium") + 1) if "premium" in fi.index else None
        summary_rows.append({
            "ETF": ETF_LABELS.get(sym, sym),
            "CV Accuracy": round(np.mean(accs) * 100, 1),
            "Premium Rank": str(p_rank) if p_rank else "—",
            "Top Feature": fi.index[0],
            "2nd Feature": fi.index[1] if len(fi) > 1 else "—",
            "3rd Feature": fi.index[2] if len(fi) > 2 else "—",
        })

    df = pd.DataFrame(summary_rows)
    if not df.empty and "Premium Rank" in df.columns:
        df["Premium Rank"] = df["Premium Rank"].astype(str)
    return df, importance_data


def lgbm_importance_chart(importance_data: dict) -> go.Figure:
    syms = list(importance_data.keys())
    if not syms:
        return go.Figure()
    # Show top 8 features for each ETF as grouped bars
    top_n = 8
    all_feats: list[str] = []
    for fi in importance_data.values():
        all_feats.extend(fi.head(top_n).index.tolist())
    feat_order = pd.Series(all_feats).value_counts().head(top_n).index.tolist()

    fig = go.Figure()
    palette = px.colors.qualitative.Set2
    for i, sym in enumerate(syms):
        fi = importance_data[sym].reindex(feat_order).fillna(0)
        fig.add_trace(go.Bar(
            name=sym,
            x=feat_order,
            y=fi.values,
            marker_color=palette[i % len(palette)],
        ))

    fig.update_layout(
        barmode="group",
        title="LightGBM Feature Importance (5-Day Return Direction)",
        xaxis_title="Feature",
        yaxis_title="Mean Importance",
        height=400,
        template="plotly_white",
        margin=dict(t=50, b=80),
        legend=dict(orientation="h", y=1.02),
    )
    return fig


# ── Section 7: Drawdowns ──────────────────────────────────────────────────────

def compute_drawdowns(price_wide: pd.DataFrame) -> pd.DataFrame:
    etfs = [c for c in price_wide.columns if c in INTL_ETFS]
    rows = []
    for sym in etfs:
        s = price_wide[sym].dropna()
        if len(s) < 60:
            continue
        roll_max = s.cummax()
        dd = (s - roll_max) / roll_max
        in_dd = False
        peak_date = peak_val = trough_date = trough_val = None

        for d_idx in range(len(s)):
            price = s.iloc[d_idx]
            peak = roll_max.iloc[d_idx]
            dval = dd.iloc[d_idx]
            curr_date = s.index[d_idx]

            if dval < -0.10 and not in_dd:
                in_dd = True
                peak_date = s.index[max(0, d_idx - 1)]
                peak_val = peak
                trough_val = price
                trough_date = curr_date
            elif in_dd:
                if price < trough_val:
                    trough_val = price
                    trough_date = curr_date
                if dval >= -0.02:
                    dd_pct = (trough_val / peak_val - 1) * 100
                    if dd_pct < -10:
                        rows.append({
                            "ETF": ETF_LABELS.get(sym, sym).split(" · ")[0],
                            "Peak Date": str(peak_date.date()),
                            "Trough Date": str(trough_date.date()),
                            "Max DD %": round(dd_pct, 1),
                            "Recovery Date": str(curr_date.date()),
                            "Recovery Days": (curr_date - trough_date).days,
                            "Recovered": True,
                        })
                    in_dd = False

        if in_dd and trough_val and peak_val:
            dd_pct = (trough_val / peak_val - 1) * 100
            if dd_pct < -10:
                rows.append({
                    "ETF": ETF_LABELS.get(sym, sym).split(" · ")[0],
                    "Peak Date": str(peak_date.date()),
                    "Trough Date": str(trough_date.date()),
                    "Max DD %": round(dd_pct, 1),
                    "Recovery Date": "—",
                    "Recovery Days": None,
                    "Recovered": False,
                })

    return pd.DataFrame(rows)


def drawdown_gantt(dd_df: pd.DataFrame) -> go.Figure:
    if dd_df.empty:
        return go.Figure()
    fig = go.Figure()
    palette = px.colors.qualitative.Set2
    etf_list = dd_df["ETF"].unique().tolist()
    today_str = str(TODAY)

    for i, etf in enumerate(etf_list):
        sub = dd_df[dd_df["ETF"] == etf]
        for _, row in sub.iterrows():
            end = row["Recovery Date"] if row["Recovered"] else today_str
            dd_pct = abs(row["Max DD %"])
            fig.add_trace(go.Bar(
                x=[row["Peak Date"], end],
                y=[etf, etf],
                orientation="h",
                base=row["Trough Date"],
                marker_color=palette[i % len(palette)],
                opacity=0.7,
                name=etf,
                showlegend=False,
                hovertemplate=(
                    f"<b>{etf}</b><br>"
                    f"Peak: {row['Peak Date']}<br>"
                    f"Trough: {row['Trough Date']}<br>"
                    f"Max DD: {row['Max DD %']}%<br>"
                    f"Recovery: {row['Recovery Date']}<extra></extra>"
                ),
            ))

    # Simpler timeline scatter
    recovered = dd_df[dd_df["Recovered"]]
    not_recovered = dd_df[~dd_df["Recovered"]]

    fig2 = px.scatter(
        dd_df,
        x="Trough Date",
        y="Max DD %",
        color="ETF",
        size=dd_df["Max DD %"].abs(),
        symbol="Recovered",
        hover_data=["Peak Date", "Recovery Date", "Recovery Days"],
        title="Drawdown Events > 10% (size = depth, △ = not yet recovered)",
        height=420,
        template="plotly_white",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig2.update_yaxes(autorange="reversed")
    fig2.update_layout(margin=dict(t=60, b=20))
    return fig2


# ── Full analysis entry-point (called by Streamlit with cache) ────────────────

def run_full_analysis(pool) -> dict:
    """Run all 7 sections and return a dict of results for the UI."""
    price_wide, nav_wide = load_data(pool)
    prem = _premium_series(price_wide, nav_wide)

    perf_df = compute_performance(price_wide)
    prem_stats, prem_wide, anomaly_dates = compute_premium_stats(price_wide, nav_wide)
    regime_df, regime_series = compute_regimes(price_wide, prem)
    corr_df, usdinr_corr = compute_correlation(price_wide)
    season_med, season_bw = compute_seasonality(price_wide)
    lgbm_df, imp_data = compute_lgbm(price_wide, prem)
    dd_df = compute_drawdowns(price_wide)

    return {
        "perf_df": perf_df,
        "perf_chart": perf_bar_chart(perf_df),
        "prem_stats": prem_stats,
        "prem_chart": premium_chart(prem_wide, anomaly_dates),
        "regime_df": regime_df,
        "regime_chart": regime_chart(price_wide, regime_series),
        "corr_df": corr_df,
        "usdinr_corr": usdinr_corr,
        "corr_chart": correlation_heatmap(corr_df),
        "season_med": season_med,
        "season_bw": season_bw,
        "season_chart": seasonality_heatmap(season_med),
        "lgbm_df": lgbm_df,
        "lgbm_chart": lgbm_importance_chart(imp_data),
        "dd_df": dd_df,
        "dd_chart": drawdown_gantt(dd_df),
    }

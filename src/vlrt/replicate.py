"""
VLRT v3 — replication diagnostic (secondary).

Scoring model weights against a fund's disclosed book is harder than it looks, and v2
got three things wrong that each biased the result:

* **Bucket labels lie.** ``asset_type='gold'`` is gold *and silver*; ``asset_type='bond'``
  is ~70% TREPS overnight repo; and ``asset_type='other'`` is not a residual — it holds
  a single-stock equity position (LIC, up to 9.06% of NAV), certificates of deposit,
  g-secs and *short* single-stock futures. Classifying at bucket level therefore
  misstates equity by several points in some months. We reclassify at security level.
* **Renormalising the wrong base.** Dropping ``other`` leaves the remaining three
  summing to ~90.3%, and dividing by that inflates mean equity by roughly 5pp. The
  full classified total averages ~97.3%; the residual is receivables/payables and
  belongs in cash.
* **A broken direction metric.** v2's ``dir_correct`` counted frozen and NaN rows as
  wrong, so a model that could not move scored 18.6% where the clean figure is 34.8%.

Note the ceiling: at AR(1) ~ 0.79 over ~43 months there are only about five independent
observations here, so nothing in this module can establish skill. It can only refute.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from scipy import stats

from src.db.pool import get_pool
from src.vlrt.data import FUND_DYNAMIC_AA, FUND_MULTI_ASSET, load_house_equity

#: Ordered (pattern, sleeve) rules applied to ``security_name``. First match wins.
RECLASS_RULES: tuple[tuple[str, str], ...] = (
    (r"treps|tri[- ]?party|repo|t[- ]?bill|\bcd\b|certificate of deposit|commercial paper", "cash"),
    (r"cstrip|sdl|\bsgs\b|gs\d{4}|government of india|india \(republic|gilt", "cash"),
    (r"silver", "pm"),
    (r"gold", "pm"),
    (r"future on |\d{2}/\d{2}/\d{4}\s*$", "equity_deriv"),
    # Sit in asset_type='other' but are single-stock equity holdings.
    (r"life insurance corporation of india", "equity"),
    (r"railway catering|\birctc\b", "equity"),
    # Cross-holding in another Quant fund; arbitrage funds are debt-like/near-cash.
    (r"arbitrage fund", "cash"),
    (r"invit|reit|realty trust|infra trust|highways trust", "hybrid"),
)

#: Any unmatched security above this share of NAV is a hard failure, not a rounding item.
UNMATCHED_TOLERANCE_PCT = 0.5

#: A fund weight change smaller than this is drift, not a decision.
MIN_FUND_MOVE_PP = 1.0
#: A model weight change smaller than this is an abstention, not a wrong call.
MIN_MODEL_MOVE_PP = 0.5


@dataclass
class ReplicationResult:
    fund_name: str
    merged: pd.DataFrame
    mae: dict[str, float]
    baselines: dict[str, dict[str, float]]
    direction: dict[str, float]
    ceiling: dict[str, float]
    warnings: list[str] = field(default_factory=list)


@dataclass
class PanelResult:
    """
    Pooled diagnostic across every fund in the panel, plus each fund's own result.

    Pooling — not averaging per-fund MAEs — is what actually lifts the effective
    sample size: every (fund, month) row enters the pooled MAE and direction test
    as its own observation.
    """
    per_fund: dict[str, ReplicationResult]
    pooled_mae: dict[str, float]
    pooled_baselines: dict[str, dict[str, float]]
    pooled_direction: dict[str, float]
    warnings: list[str] = field(default_factory=list)


def _classify(security: str, asset_type: str) -> str:
    s = (security or "").lower()
    for pat, sleeve in RECLASS_RULES:
        if re.search(pat, s):
            return sleeve
    if asset_type == "equity":
        return "equity"
    if asset_type == "gold":
        return "pm"
    if asset_type in ("bond", "cash"):
        return "cash"
    return "unmatched"


def fund_weights(fund_name: str = FUND_MULTI_ASSET) -> tuple[pd.DataFrame, list[str]]:
    """
    Security-level reclassified, renormalised month-end weights on (equity, pm, cash).

    Short single-stock futures keep their sign, so a disclosed short overlay reduces
    net equity where the source discloses it.
    """
    df = get_pool().query_df(
        f"""SELECT as_of_month, security_name, asset_type, sum(pct_of_nav) AS pct
            FROM market_data.mf_holdings FINAL
            WHERE fund_name = '{fund_name}'
            GROUP BY as_of_month, security_name, asset_type"""
    )
    warns: list[str] = []
    if df.empty:
        return pd.DataFrame(), ["no holdings rows"]

    df["as_of_month"] = pd.to_datetime(df["as_of_month"])
    # Keep only true month-end rows: first-of-month rows come from a second scraper
    # with an incompatible schema (totals of exactly 100.00, negative `other`).
    df = df[df["as_of_month"] == df["as_of_month"] + pd.offsets.MonthEnd(0)].copy()
    df["sleeve"] = [_classify(s, a) for s, a in zip(df["security_name"], df["asset_type"])]

    bad = df[df["sleeve"] == "unmatched"]
    if not bad.empty and bad["pct"].abs().max() > UNMATCHED_TOLERANCE_PCT:
        worst = bad.reindex(bad["pct"].abs().sort_values(ascending=False).index).head(3)
        warns.append(
            "unclassified securities above tolerance: "
            + "; ".join(f"{r.security_name[:40]} {r.pct:.2f}%" for r in worst.itertuples())
        )

    # Derivatives and hybrids fold into equity; hybrids are equity-like by risk.
    df["sleeve"] = df["sleeve"].replace({"equity_deriv": "equity", "hybrid": "equity"})
    wide = (
        df.pivot_table(index="as_of_month", columns="sleeve", values="pct", aggfunc="sum")
        .fillna(0.0).sort_index()
    )
    for c in ("equity", "pm", "cash"):
        if c not in wide.columns:
            wide[c] = 0.0
    # Residual (receivables/payables) is cash, then renormalise to exactly 1.
    total = wide[["equity", "pm", "cash", "unmatched"]].sum(axis=1) if "unmatched" in wide else wide[["equity", "pm", "cash"]].sum(axis=1)
    wide["cash"] = wide["cash"] + (100.0 - total)
    out = wide[["equity", "pm", "cash"]].div(wide[["equity", "pm", "cash"]].sum(axis=1), axis=0)
    out.index = out.index.to_period("M")
    return out, warns


def _mae(a: pd.Series, b: pd.Series) -> float:
    return float((a - b).abs().mean() * 100)


def _evaluate_one(
    model_w: pd.DataFrame,
    fund_name: str,
    n_null: int = 2000,
    seed: int = 5,
) -> ReplicationResult:
    """Score model weights against one fund's disclosed book, beside naive baselines."""
    fund, warns = fund_weights(fund_name)
    if fund.empty:
        empty = {s: np.nan for s in ("equity", "pm", "cash")}
        return ReplicationResult(fund_name, pd.DataFrame(), empty,
                                  {k: dict(empty) for k in ("random_walk", "constant_mean", "static_55_20_25")},
                                  {"hit_rate_pct": np.nan, "n_scored": 0, "n_eligible": 0, "n_total": 0,
                                   "abstention_pct": np.nan, "p_vs_shuffled_null": np.nan, "spearman_delta": np.nan},
                                  {}, warns)

    mw = model_w.copy()
    mw.index = mw.index.to_period("M")
    j = mw.index.intersection(fund.index)
    m, f = mw.loc[j], fund.loc[j]

    mae = {s: _mae(m[s], f[s]) for s in ("equity", "pm", "cash")}
    baselines = {
        "random_walk": {s: _mae(f[s].shift(1).dropna(), f[s].iloc[1:]) for s in ("equity", "pm", "cash")},
        "constant_mean": {s: _mae(pd.Series(f[s].mean(), index=f.index), f[s]) for s in ("equity", "pm", "cash")},
        "static_55_20_25": {
            s: _mae(pd.Series(v, index=f.index), f[s])
            for s, v in (("equity", 0.55), ("pm", 0.20), ("cash", 0.25))
        },
    }

    # ── direction, with an honest denominator ────────────────────────────────
    d_f = f["equity"].diff() * 100
    d_m = m["equity"].diff() * 100
    ok = d_f.notna() & d_m.notna()
    eligible = ok & (d_f.abs() >= MIN_FUND_MOVE_PP)
    scored = eligible & (d_m.abs() >= MIN_MODEL_MOVE_PP)
    hits = int(((d_f * d_m) > 0)[scored].sum())
    n_scored = int(scored.sum())

    rng = np.random.default_rng(seed)
    null_hits = np.empty(n_null)
    dm_s, df_s = d_m[scored].to_numpy(), d_f[scored].to_numpy()
    for i in range(n_null):
        null_hits[i] = ((df_s * rng.permutation(dm_s)) > 0).mean() if n_scored else np.nan
    obs_rate = hits / n_scored if n_scored else np.nan
    p_null = (1 + int((null_hits >= obs_rate).sum())) / (1 + n_null) if n_scored else np.nan

    direction = {
        "hit_rate_pct": obs_rate * 100 if n_scored else np.nan,
        "n_scored": n_scored,
        "n_eligible": int(eligible.sum()),
        "n_total": int(len(j)),
        "abstention_pct": (1 - n_scored / max(int(eligible.sum()), 1)) * 100,
        "p_vs_shuffled_null": p_null,
        "spearman_delta": float(stats.spearmanr(d_m[ok], d_f[ok])[0]) if ok.sum() > 8 else np.nan,
    }

    # ── ceiling: how much is knowable from Quant's other equity funds ────────
    # load_house_equity() excludes BOTH multi-asset funds already, so this is
    # never circular whichever fund_name is being scored.
    house = load_house_equity()
    ceiling: dict[str, float] = {}
    if not house.empty:
        hj = house.index.intersection(f.index)
        if len(hj) > 8:
            x = house.loc[hj, "house_equity_pct"].to_numpy(float)
            y = (f.loc[hj, "equity"] * 100).to_numpy(float)
            r = stats.pearsonr(x, y)
            ceiling = {"r2": float(r[0] ** 2), "p": float(r[1]), "n": len(hj)}

    merged = pd.concat(
        [m.add_prefix("model_"), f.add_prefix("fund_")], axis=1
    ).assign(d_fund_equity=d_f, d_model_equity=d_m, fund_name=fund_name)
    return ReplicationResult(fund_name, merged, mae, baselines, direction, ceiling, warns)


def evaluate(
    model_w: pd.DataFrame,
    fund_names: tuple[str, ...] = (FUND_MULTI_ASSET, FUND_DYNAMIC_AA),
    n_null: int = 2000,
    seed: int = 5,
) -> PanelResult:
    """
    Score model weights against every fund in the panel, then pool at the row level.

    Pooling (not averaging per-fund MAEs) is what actually lifts the effective sample
    size the plan asked for: every (fund, month) becomes its own observation in the
    pooled MAE and the pooled direction test, rather than collapsing each fund to one
    number first. ``QUANT_DYNAMIC_ASSET_ALLOCATION`` carries no precious-metals sleeve
    (its ``pm`` weight is 0 in every disclosed month), so its ``pm`` MAE reflects the
    fund's mandate, not a model error — reported per-fund so that is visible.
    """
    per_fund: dict[str, ReplicationResult] = {}
    warns: list[str] = []
    for i, fname in enumerate(fund_names):
        res = _evaluate_one(model_w, fname, n_null=n_null, seed=seed + i)
        per_fund[fname] = res
        warns.extend(f"[{fname}] {w}" for w in res.warnings)

    merged_all = pd.concat(
        [r.merged for r in per_fund.values() if not r.merged.empty], axis=0
    )
    if merged_all.empty:
        pooled_mae = {s: np.nan for s in ("equity", "pm", "cash")}
        pooled_baselines = {k: dict(pooled_mae) for k in ("random_walk", "constant_mean", "static_55_20_25")}
        pooled_direction = {"hit_rate_pct": np.nan, "n_scored": 0, "n_eligible": 0, "n_total": 0,
                             "abstention_pct": np.nan, "p_vs_shuffled_null": np.nan, "spearman_delta": np.nan}
        return PanelResult(per_fund, pooled_mae, pooled_baselines, pooled_direction, warns)

    pooled_mae = {
        s: float((merged_all[f"model_{s}"] - merged_all[f"fund_{s}"]).abs().mean() * 100)
        for s in ("equity", "pm", "cash")
    }
    # Random-walk / constant-mean baselines need each fund's OWN time axis for
    # .shift(1) and its own mean, so they are built per fund per sleeve, then pooled
    # at the row-error level (not by averaging each fund's scalar MAE).
    pooled_baselines = {"random_walk": {}, "constant_mean": {}}
    for s in ("equity", "pm", "cash"):
        rw_rows, cm_rows = [], []
        for res in per_fund.values():
            if res.merged.empty:
                continue
            fs = res.merged[f"fund_{s}"]
            rw_rows.append((fs - fs.shift(1)).abs().dropna() * 100)
            cm_rows.append((fs - fs.mean()).abs() * 100)
        pooled_baselines["random_walk"][s] = float(pd.concat(rw_rows).mean()) if rw_rows else np.nan
        pooled_baselines["constant_mean"][s] = float(pd.concat(cm_rows).mean()) if cm_rows else np.nan
    pooled_baselines["static_55_20_25"] = {
        s: float((merged_all[f"fund_{s}"] - v).abs().mean() * 100)
        for s, v in (("equity", 0.55), ("pm", 0.20), ("cash", 0.25))
    }

    d_f = merged_all["d_fund_equity"]
    d_m = merged_all["d_model_equity"]
    ok = d_f.notna() & d_m.notna()
    eligible = ok & (d_f.abs() >= MIN_FUND_MOVE_PP)
    scored = eligible & (d_m.abs() >= MIN_MODEL_MOVE_PP)
    hits = int(((d_f * d_m) > 0)[scored].sum())
    n_scored = int(scored.sum())

    rng = np.random.default_rng(seed)
    dm_s, df_s = d_m[scored].to_numpy(), d_f[scored].to_numpy()
    null_hits = np.empty(n_null)
    for i in range(n_null):
        null_hits[i] = ((df_s * rng.permutation(dm_s)) > 0).mean() if n_scored else np.nan
    obs_rate = hits / n_scored if n_scored else np.nan
    p_null = (1 + int((null_hits >= obs_rate).sum())) / (1 + n_null) if n_scored else np.nan

    pooled_direction = {
        "hit_rate_pct": obs_rate * 100 if n_scored else np.nan,
        "n_scored": n_scored,
        "n_eligible": int(eligible.sum()),
        "n_total": int(len(merged_all)),
        "abstention_pct": (1 - n_scored / max(int(eligible.sum()), 1)) * 100,
        "p_vs_shuffled_null": p_null,
        "spearman_delta": float(stats.spearmanr(d_m[ok], d_f[ok])[0]) if ok.sum() > 8 else np.nan,
    }

    return PanelResult(per_fund, pooled_mae, pooled_baselines, pooled_direction, warns)

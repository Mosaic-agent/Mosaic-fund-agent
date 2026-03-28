"""
src/agents/visualization_agent.py
──────────────────────────────────
Converts the latest portfolio JSON report into a self-contained HTML dashboard.

The dashboard is pure client-side React (loaded from CDN) + Recharts + Tailwind.
No Node.js build step, no bundler, no server — just open in a browser.

Usage
-----
    from src.agents.visualization_agent import VisualizationAgent
    agent = VisualizationAgent()
    path = agent.generate(report_dict)  # returns ./output/dashboard.html
"""

from __future__ import annotations

import json
import logging
import os
import re
import webbrowser
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _strip_inr(val: str | float | None) -> float:
    """Parse '₹52,438.00' → 52438.0  OR  pass-through a plain float."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    cleaned = re.sub(r"[₹,\s]", "", str(val))
    cleaned = cleaned.replace("%", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _strip_pct(val: str | float | None) -> float:
    """Parse '87.95%' → 87.95  OR pass-through a plain float."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    return _strip_inr(val)


def _signal_colour(signal: str) -> str:
    mapping = {
        "STRONG BULLISH": "#22c55e",
        "BULLISH":        "#86efac",
        "NEUTRAL":        "#facc15",
        "BEARISH":        "#f97316",
        "STRONG BEARISH": "#ef4444",
    }
    return mapping.get(signal.upper(), "#a1a1aa")


# ── Data builder ────────────────────────────────────────────────────────────────

def _build_dashboard_data(report: dict[str, Any]) -> dict[str, Any]:
    """
    Flatten the raw report JSON into a structure optimised for the React template.
    All monetary strings are stripped to plain floats; null guards applied.
    """
    summary = report.get("portfolio_summary", {})
    holdings_raw = report.get("holdings_analysis", [])
    sector_raw = report.get("sector_allocation", {})
    comex_raw = report.get("comex_signals", {})

    # ── Summary ──────────────────────────────────────────────────────────────
    portfolio_summary = {
        "total_value":               _strip_inr(summary.get("total_value")),
        "total_invested":            _strip_inr(summary.get("total_invested")),
        "total_pnl":                 _strip_inr(summary.get("total_pnl")),
        "total_pnl_percent":         _strip_pct(summary.get("total_pnl_percent")),
        "health_score":              float(summary.get("health_score") or 0),
        "diversification_score":     float(summary.get("diversification_score") or 0),
        "num_holdings":              int(summary.get("num_holdings") or 0),
        "etf_count":                 int(summary.get("etf_count") or 0),
        "stock_count":               int(summary.get("stock_count") or 0),
        "etf_allocation_pct":        float(summary.get("etf_allocation_pct") or 0),
        "direct_equity_allocation_pct": float(summary.get("direct_equity_allocation_pct") or 0),
    }

    # ── Sector allocation ─────────────────────────────────────────────────────
    sector_data = [
        {"name": k, "value": round(float(v), 2)}
        for k, v in (sector_raw or {}).items()
    ]

    # ── Holdings ──────────────────────────────────────────────────────────────
    holdings = []
    for h in holdings_raw:
        # iNAV (ETF only, may be null)
        inav = h.get("inav_analysis") or {}
        inav_data = None
        if inav:
            inav_data = {
                "inav":                  float(inav.get("inav") or 0),
                "market_price":          float(inav.get("market_price") or 0),
                "premium_discount_pct":  float(inav.get("premium_discount_pct") or 0),
                "label":                 inav.get("premium_discount_label", ""),
            }

        # historic iNAV sparkline (ETF only)
        hist = h.get("historic_inav") or {}
        hist_records = []
        if hist and hist.get("records"):
            for r in hist["records"]:
                hist_records.append({
                    "date": r["date"],
                    "nav":  round(float(r.get("nav") or 0), 4),
                    "market_close": round(float(r.get("market_close") or 0), 4),
                    "pct":  round(float(r.get("premium_discount_pct") or 0), 2),
                    "label": r.get("label", ""),
                })

        # Latest quarterly results
        results = h.get("latest_results") or {}

        holdings.append({
            "symbol":               h.get("symbol", ""),
            "exchange":             h.get("exchange", "NSE"),
            "instrument_type":      h.get("instrument_type", "STOCK"),
            "quantity":             int(h.get("quantity") or 0),
            "average_buy_price":    float(h.get("average_buy_price") or 0),
            "current_price":        float(h.get("current_price") or 0),
            "invested_value_inr":   float(h.get("invested_value_inr") or 0),
            "current_value_inr":    float(h.get("current_value_inr") or 0),
            "pnl_percent":          float(h.get("pnl_percent") or 0),
            "sector":               h.get("sector", ""),
            "sentiment_score":      float(h.get("sentiment_score") or 0),
            "risk_score":           float(h.get("risk_score") or 0),
            "summary":              h.get("summary", ""),
            "key_insights":         h.get("key_insights") or [],
            "risk_signals":         h.get("risk_signals") or [],
            "recommendation":       h.get("recommendation", "HOLD"),
            "comex_linked_commodities": h.get("comex_linked_commodities") or [],
            "inav_analysis":        inav_data,
            "historic_records":     hist_records,
            "latest_results": {
                "period":         results.get("period", ""),
                "revenue_cr":     float(results.get("revenue_cr") or 0),
                "net_profit_cr":  float(results.get("net_profit_cr") or 0),
                "eps":            float(results.get("eps") or 0),
                "revenue_yoy_pct":float(results.get("revenue_yoy_pct") or 0),
                "profit_yoy_pct": float(results.get("profit_yoy_pct") or 0),
            },
        })

    # ── COMEX ─────────────────────────────────────────────────────────────────
    comex_commodities = []
    for sym, c in (comex_raw.get("commodities") or {}).items():
        comex_commodities.append({
            "symbol":     sym,
            "name":       c.get("name", sym),
            "emoji":      c.get("emoji", ""),
            "signal":     c.get("signal", "NEUTRAL"),
            "change_pct": round(float(c.get("change_pct") or 0), 3),
            "live_price": float(c.get("live_price") or 0),
            "prev_close": float(c.get("prev_close") or 0),
            "unit":       c.get("unit", ""),
            "nse_etfs":   c.get("nse_etfs") or [],
            "colour":     _signal_colour(c.get("signal", "NEUTRAL")),
        })

    comex = {
        "overall_signal": comex_raw.get("overall_signal", ""),
        "summary":        comex_raw.get("summary", ""),
        "run_time_ist":   comex_raw.get("run_time_ist", ""),
        "pre_market":     bool(comex_raw.get("pre_market")),
        "commodities":    comex_commodities,
    }

    return {
        "generated_at":       report.get("generated_at", datetime.now().isoformat()),
        "portfolio_summary":  portfolio_summary,
        "sector_data":        sector_data,
        "holdings":           holdings,
        "comex":              comex,
        "portfolio_risks":    report.get("portfolio_risks") or [],
        "actionable_insights":report.get("actionable_insights") or [],
        "rebalancing_signals":report.get("rebalancing_signals") or [],
    }


# ── HTML template ───────────────────────────────────────────────────────────────

_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Portfolio Insight Dashboard</title>
  <!-- Tailwind -->
  <script src="https://cdn.tailwindcss.com"></script>
  <!-- React + ReactDOM (UMD) -->
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <!-- Babel standalone for JSX transpilation -->
  <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>

  <script>
    tailwind.config = {
      theme: {
        extend: {
          colors: {
            surface: "#0f172a",
            card:    "#1e293b",
            border:  "#334155",
          }
        }
      }
    }
  </script>
  <style>
    body { background:#0f172a; color:#f1f5f9; font-family:'Inter',sans-serif; }
    .scrollbar-thin::-webkit-scrollbar{width:4px;height:4px}
    .scrollbar-thin::-webkit-scrollbar-track{background:#1e293b}
    .scrollbar-thin::-webkit-scrollbar-thumb{background:#475569;border-radius:4px}
  </style>
</head>
<body>
<div id="root"></div>

<script>
  window.__PORTFOLIO_DATA__ = __PORTFOLIO_DATA_PLACEHOLDER__;
</script>

<script type="text/babel">
const { useState, useEffect, useRef } = React;

// ── Auto-refresh hook ──────────────────────────────────────────
const REFRESH_SECS = 300;  // 5 minutes

function useCountdown() {
  const [secs, setSecs] = useState(REFRESH_SECS);
  useEffect(() => {
    const id = setInterval(() => {
      setSecs(s => {
        if (s <= 1) { location.reload(); return REFRESH_SECS; }
        return s - 1;
      });
    }, 1000);
    return () => clearInterval(id);
  }, []);
  const m = String(Math.floor(secs / 60)).padStart(2, '0');
  const s = String(secs % 60).padStart(2, '0');
  return `${m}:${s}`;
}

const DATA = window.__PORTFOLIO_DATA__;

// ── helpers ────────────────────────────────────────────────────────────────
const fmt_inr = (n) => new Intl.NumberFormat('en-IN', {
  style:'currency', currency:'INR', maximumFractionDigits:2
}).format(n);

const fmt_pct = (n, always_sign=false) =>
  (always_sign && n >= 0 ? '+' : '') + n.toFixed(2) + '%';

const rec_color = {
  BUY:  '#22c55e',
  SELL: '#ef4444',
  HOLD: '#facc15',
};

const signal_color = (s='') => {
  const m = {
    'STRONG BULLISH': '#22c55e',
    'BULLISH':        '#86efac',
    'NEUTRAL':        '#facc15',
    'BEARISH':        '#f97316',
    'STRONG BEARISH': '#ef4444',
  };
  return m[s.toUpperCase()] || '#a1a1aa';
};

const risk_color = (score) => {
  if (score >= 8) return '#ef4444';
  if (score >= 6) return '#f97316';
  if (score >= 4) return '#facc15';
  return '#22c55e';
};

// ── MetricCard ─────────────────────────────────────────────────────────────
function MetricCard({ label, value, sub, colour }) {
  return (
    <div className="bg-card rounded-2xl p-5 border border-border flex flex-col gap-1 shadow">
      <span className="text-xs text-slate-400 uppercase tracking-widest">{label}</span>
      <span className="text-2xl font-bold" style={{ color: colour || '#f1f5f9' }}>{value}</span>
      {sub && <span className="text-xs text-slate-500">{sub}</span>}
    </div>
  );
}

// ── SectionTitle ───────────────────────────────────────────────────────────
function SectionTitle({ children }) {
  return (
    <h2 className="text-lg font-bold text-slate-200 border-l-4 border-indigo-500 pl-3 mb-4">
      {children}
    </h2>
  );
}

// ── COMEX Panel ────────────────────────────────────────────────────────────
function ComexPanel({ comex }) {
  if (!comex || !comex.commodities.length) return null;
  const overall_col = signal_color(comex.overall_signal);
  return (
    <div className="bg-card rounded-2xl p-5 border border-border">
      <div className="flex items-center justify-between mb-4">
        <SectionTitle>🌍 COMEX Pre-Market Signals</SectionTitle>
        <span className="text-xs px-2 py-1 rounded-full font-semibold"
          style={{ background: overall_col + '22', color: overall_col, border:`1px solid ${overall_col}` }}>
          {comex.overall_signal}
        </span>
      </div>
      <p className="text-xs text-slate-400 mb-1">{comex.summary}</p>
      {comex.run_time_ist &&
        <p className="text-xs text-slate-600 mb-3">Updated: {comex.run_time_ist}{comex.pre_market ? ' (pre-market)' : ''}</p>}

      <div className="overflow-x-auto scrollbar-thin">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-slate-400 border-b border-border">
              <th className="text-left pb-2 pr-4">Commodity</th>
              <th className="text-right pb-2 pr-4">Live Price</th>
              <th className="text-right pb-2 pr-4">Change</th>
              <th className="text-center pb-2 pr-4">Signal</th>
              <th className="text-left pb-2">NSE ETFs</th>
            </tr>
          </thead>
          <tbody>
            {comex.commodities.map(c => {
              const col = signal_color(c.signal);
              return (
                <tr key={c.symbol} className="border-b border-border/50 hover:bg-slate-800/30 transition">
                  <td className="py-2 pr-4 font-medium">
                    {c.emoji} {c.name} <span className="text-slate-500 text-xs">({c.symbol})</span>
                  </td>
                  <td className="py-2 pr-4 text-right text-slate-300">
                    ${c.live_price.toLocaleString(undefined, {minimumFractionDigits:2,maximumFractionDigits:4})}
                    <span className="text-xs ml-1 text-slate-500">{c.unit}</span>
                  </td>
                  <td className="py-2 pr-4 text-right font-semibold" style={{color:col}}>
                    {fmt_pct(c.change_pct, true)}
                  </td>
                  <td className="py-2 pr-4 text-center">
                    <span className="text-xs px-2 py-0.5 rounded-full"
                      style={{background: col+'22', color:col, border:`1px solid ${col}`}}>
                      {c.signal}
                    </span>
                  </td>
                  <td className="py-2 text-xs text-slate-400">
                    {c.nse_etfs.length ? c.nse_etfs.join(', ') : <span className="text-slate-600">—</span>}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Sector Allocation Bar Chart (pure SVG) ────────────────────────────────
const SECTOR_COLORS = [
  '#6366f1','#22d3ee','#f59e0b','#10b981',
  '#ec4899','#8b5cf6','#f97316','#14b8a6',
];

function SvgSectorChart({ data }) {
  const ROW_H = 30, GAP = 8, LABEL_W = 150, BAR_MAX = 260, TEXT_W = 64;
  const svgH = data.length * (ROW_H + GAP) + 8;
  const svgW = LABEL_W + BAR_MAX + TEXT_W;
  return (
    <div className="bg-card rounded-2xl p-5 border border-border">
      <SectionTitle>📊 Sector Allocation</SectionTitle>
      <svg viewBox={`0 0 ${svgW} ${svgH}`} width="100%" style={{display:'block'}}>
        {data.map((d, i) => {
          const y = 4 + i * (ROW_H + GAP);
          const barW = (d.value / 100) * BAR_MAX;
          const col = SECTOR_COLORS[i % SECTOR_COLORS.length];
          return (
            <g key={d.name}>
              <text x={LABEL_W - 8} y={y + ROW_H * 0.63}
                textAnchor="end" fill="#94a3b8" fontSize="12" fontFamily="system-ui,sans-serif">
                {d.name}
              </text>
              <rect x={LABEL_W} y={y + 4} width={Math.max(barW, 4)} height={ROW_H - 8}
                fill={col} rx="4" opacity="0.9"/>
              <text x={LABEL_W + barW + 8} y={y + ROW_H * 0.63}
                fill="#cbd5e1" fontSize="12" fontFamily="system-ui,sans-serif">
                {d.value.toFixed(1)}%
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

// ── iNAV Chart (pure SVG) ─────────────────────────────────────────────────
function SvgInavChart({ records, symbol }) {
  if (!records || records.length < 2) return null;

  const W = 480;
  const P = {t:18, r:12, b:22, l:42};
  const innerW = W - P.l - P.r;
  const AREA_H = 150, BAR_H = 80;
  const aIH = AREA_H - P.t - P.b;  // inner height of area chart
  const bIH = BAR_H  - P.t - P.b;  // inner height of bar chart
  const n = records.length;

  // ── area chart scales ──
  const navVals = records.map(r => r.nav);
  const mktVals = records.map(r => r.market_close);
  const allV = [...navVals, ...mktVals];
  const minY = Math.min(...allV) * 0.9985;
  const maxY = Math.max(...allV) * 1.0015;
  const rangeY = (maxY - minY) || 1;
  const xAt = i => P.l + (n > 1 ? i / (n - 1) : 0.5) * innerW;
  const yAt = v  => P.t + (1 - (v - minY) / rangeY) * aIH;
  const pathFor = vals => vals.map((v, i) =>
    `${i === 0 ? 'M' : 'L'}${xAt(i).toFixed(1)},${yAt(v).toFixed(1)}`
  ).join(' ');
  const navPath = pathFor(navVals);
  const mktPath = pathFor(mktVals);
  const bottom  = (P.t + aIH).toFixed(1);
  const navArea = `${navPath} L${xAt(n-1).toFixed(1)},${bottom} L${P.l},${bottom} Z`;
  const mktArea = `${mktPath} L${xAt(n-1).toFixed(1)},${bottom} L${P.l},${bottom} Z`;

  // ── bar chart (P/D %) ──
  const pcts    = records.map(r => r.pct);
  const maxAbs  = Math.max(...pcts.map(Math.abs), 0.5);
  const zeroY   = P.t + bIH * 0.5;
  const barW    = (innerW / n) * 0.65;
  const barHFor = v => Math.max(Math.abs(v) / maxAbs * (bIH / 2), 1);

  // x-axis label indices (~5 spread)
  const step = Math.max(1, Math.floor(n / 4));
  const xLbl = [...new Set([0, ...Array.from({length:3},(_,i)=>(i+1)*step), n-1])]
    .filter(i => i < n);

  const gapY  = AREA_H + 10;
  const totalH = gapY + BAR_H;
  const yTicks = [minY, (minY + maxY) / 2, maxY];

  return (
    <div className="mt-4">
      <p className="text-xs text-slate-400 mb-2 font-semibold">
        {symbol} — iNAV vs Market Price
      </p>
      <svg viewBox={`0 0 ${W} ${totalH}`} width="100%" style={{display:'block'}}>
        <defs>
          <linearGradient id={`gn-${symbol}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%"   stopColor="#6366f1" stopOpacity="0.35"/>
            <stop offset="100%" stopColor="#6366f1" stopOpacity="0.02"/>
          </linearGradient>
          <linearGradient id={`gm-${symbol}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%"   stopColor="#22d3ee" stopOpacity="0.2"/>
            <stop offset="100%" stopColor="#22d3ee" stopOpacity="0.02"/>
          </linearGradient>
        </defs>

        {/* ── Area chart ── */}
        {yTicks.map((v, ti) => {
          const y = yAt(v);
          return (
            <g key={ti}>
              <line x1={P.l} y1={y} x2={P.l + innerW} y2={y}
                stroke="#334155" strokeDasharray="4 2" strokeWidth="0.6"/>
              <text x={P.l - 4} y={y + 3.5} textAnchor="end"
                fill="#64748b" fontSize="9" fontFamily="system-ui,sans-serif">
                ₹{v.toFixed(0)}
              </text>
            </g>
          );
        })}
        <path d={navArea} fill={`url(#gn-${symbol})`}/>
        <path d={mktArea} fill={`url(#gm-${symbol})`}/>
        <path d={navPath} fill="none" stroke="#6366f1" strokeWidth="1.5" strokeLinejoin="round"/>
        <path d={mktPath} fill="none" stroke="#22d3ee" strokeWidth="1.5" strokeLinejoin="round"/>
        {/* Legend */}
        <rect x={P.l}    y={3} width={8} height={8} fill="#6366f1" rx="2"/>
        <text x={P.l+10} y={10} fill="#94a3b8" fontSize="9" fontFamily="system-ui,sans-serif">iNAV</text>
        <rect x={P.l+46} y={3} width={8} height={8} fill="#22d3ee" rx="2"/>
        <text x={P.l+56} y={10} fill="#94a3b8" fontSize="9" fontFamily="system-ui,sans-serif">Market</text>

        {/* ── P/D bar chart ── */}
        <g transform={`translate(0,${gapY})`}>
          <text x={P.l} y={P.t - 4} fill="#94a3b8" fontSize="9" fontFamily="system-ui,sans-serif">
            Premium / Discount %
          </text>
          <line x1={P.l} y1={P.t + bIH*0.5} x2={P.l + innerW} y2={P.t + bIH*0.5}
            stroke="#475569" strokeDasharray="4 2" strokeWidth="0.8"/>
          <text x={P.l - 4} y={P.t + bIH*0.5 + 3.5} textAnchor="end"
            fill="#64748b" fontSize="9" fontFamily="system-ui,sans-serif">0</text>
          {records.map((r, i) => {
            const bh = barHFor(r.pct);
            const bx = xAt(i) - barW / 2;
            const by = r.pct >= 0 ? zeroY - bh : zeroY;
            return (
              <rect key={i} x={bx} y={by} width={barW} height={bh}
                fill={r.pct >= 0 ? '#22c55e' : '#ef4444'} rx="2" opacity="0.85"/>
            );
          })}
          {xLbl.map(i => (
            <text key={i} x={xAt(i)} y={P.t + bIH + 14}
              textAnchor="middle" fill="#64748b" fontSize="8" fontFamily="system-ui,sans-serif">
              {records[i].date.slice(5)}
            </text>
          ))}
        </g>
      </svg>
    </div>
  );
}

// ── HoldingCard ────────────────────────────────────────────────────────────
function HoldingCard({ h }) {
  const [open, setOpen] = useState(false);
  const pnl_col = h.pnl_percent >= 0 ? '#22c55e' : '#ef4444';
  const rec_col = rec_color[h.recommendation] || '#94a3b8';
  const isETF = h.instrument_type === 'ETF';

  return (
    <div className="bg-card rounded-2xl border border-border overflow-hidden shadow">
      {/* Header row */}
      <div
        className="flex items-center justify-between px-5 py-4 cursor-pointer hover:bg-slate-800/40 transition select-none"
        onClick={() => setOpen(o => !o)}
      >
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl flex items-center justify-center text-sm font-bold"
            style={{background: pnl_col+'22', color: pnl_col, border:`1px solid ${pnl_col}`}}>
            {h.symbol.slice(0,3)}
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="font-semibold text-slate-100">{h.symbol}</span>
              {isETF && <span className="text-xs px-1.5 py-0.5 rounded bg-indigo-900/60 text-indigo-300 border border-indigo-700">ETF</span>}
              <span className="text-xs text-slate-500">{h.exchange}</span>
            </div>
            <div className="text-xs text-slate-400">{h.sector}</div>
          </div>
        </div>

        <div className="flex items-center gap-6">
          {/* Current value */}
          <div className="text-right">
            <div className="text-sm font-semibold text-slate-100">{fmt_inr(h.current_value_inr)}</div>
            <div className="text-xs text-slate-500">{h.quantity} qty @ ₹{h.current_price.toFixed(2)}</div>
          </div>
          {/* P&L */}
          <div className="text-right min-w-[64px]">
            <div className="font-bold text-sm" style={{color: pnl_col}}>
              {fmt_pct(h.pnl_percent, true)}
            </div>
            <div className="text-xs" style={{color: pnl_col}}>
              {h.current_value_inr - h.invested_value_inr >= 0 ? '+' : ''}
              {fmt_inr(h.current_value_inr - h.invested_value_inr)}
            </div>
          </div>
          {/* Risk */}
          <div className="text-right min-w-[48px]">
            <div className="text-xs text-slate-400">Risk</div>
            <div className="font-bold text-sm" style={{color: risk_color(h.risk_score)}}>
              {h.risk_score.toFixed(1)}/10
            </div>
          </div>
          {/* Recommendation badge */}
          <span className="text-xs px-2.5 py-1 rounded-full font-semibold"
            style={{background: rec_col+'22', color: rec_col, border:`1px solid ${rec_col}`}}>
            {h.recommendation}
          </span>
          {/* Chevron */}
          <span className="text-slate-500 text-lg">{open ? '▲' : '▼'}</span>
        </div>
      </div>

      {/* Expanded body */}
      {open && (
        <div className="px-5 pb-5 border-t border-border/40 pt-4 space-y-4">
          {/* Summary */}
          {h.summary && (
            <p className="text-sm text-slate-300 leading-relaxed">{h.summary}</p>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Key Insights */}
            {h.key_insights.length > 0 && (
              <div>
                <p className="text-xs font-semibold text-indigo-400 mb-2 uppercase tracking-wider">Key Insights</p>
                <ul className="space-y-1.5">
                  {h.key_insights.map((ins, i) => (
                    <li key={i} className="text-xs text-slate-300 flex gap-2">
                      <span className="text-indigo-500 mt-0.5 shrink-0">▸</span>
                      <span>{ins}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Risk Signals */}
            {h.risk_signals.length > 0 && (
              <div>
                <p className="text-xs font-semibold text-orange-400 mb-2 uppercase tracking-wider">Risk Signals</p>
                <ul className="space-y-1.5">
                  {h.risk_signals.map((rs, i) => (
                    <li key={i} className="text-xs text-orange-300 flex gap-2">
                      <span className="shrink-0">⚠</span>
                      <span>{rs}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>

          {/* COMEX linked */}
          {h.comex_linked_commodities.length > 0 && (
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-xs text-slate-500">COMEX linked:</span>
              {h.comex_linked_commodities.map(c => (
                <span key={c} className="text-xs px-2 py-0.5 rounded-full bg-yellow-900/40 text-yellow-300 border border-yellow-700">
                  {c}
                </span>
              ))}
            </div>
          )}

          {/* Quarterly results (stocks only) */}
          {h.latest_results.period && (
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 mt-2">
              <div className="bg-slate-800/50 rounded-xl p-3">
                <div className="text-xs text-slate-500">Revenue ({h.latest_results.period})</div>
                <div className="font-semibold text-slate-100">₹{h.latest_results.revenue_cr.toLocaleString('en-IN')} Cr</div>
                <div className="text-xs" style={{color: h.latest_results.revenue_yoy_pct >= 0 ? '#22c55e':'#ef4444'}}>
                  {fmt_pct(h.latest_results.revenue_yoy_pct, true)} YoY
                </div>
              </div>
              <div className="bg-slate-800/50 rounded-xl p-3">
                <div className="text-xs text-slate-500">Net Profit</div>
                <div className="font-semibold text-slate-100">₹{h.latest_results.net_profit_cr.toLocaleString('en-IN')} Cr</div>
                <div className="text-xs" style={{color: h.latest_results.profit_yoy_pct >= 0 ? '#22c55e':'#ef4444'}}>
                  {fmt_pct(h.latest_results.profit_yoy_pct, true)} YoY
                </div>
              </div>
              <div className="bg-slate-800/50 rounded-xl p-3">
                <div className="text-xs text-slate-500">EPS</div>
                <div className="font-semibold text-slate-100">₹{h.latest_results.eps.toFixed(2)}</div>
              </div>
            </div>
          )}

          {/* iNAV analysis */}
          {h.inav_analysis && (
            <div className="bg-slate-800/40 rounded-xl p-4">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs font-semibold text-cyan-400 uppercase tracking-wider">iNAV Analysis</span>
                <span className="text-xs px-2 py-0.5 rounded-full"
                  style={{
                    background: (h.inav_analysis.premium_discount_pct >= 0 ? '#22c55e' : '#ef4444') + '22',
                    color:       h.inav_analysis.premium_discount_pct >= 0 ? '#22c55e' : '#ef4444',
                    border:     `1px solid ${h.inav_analysis.premium_discount_pct >= 0 ? '#22c55e' : '#ef4444'}`,
                  }}>
                  {h.inav_analysis.label} ({fmt_pct(h.inav_analysis.premium_discount_pct, true)})
                </span>
              </div>
              <div className="grid grid-cols-3 gap-3 mt-2 text-sm">
                <div>
                  <div className="text-xs text-slate-500">iNAV</div>
                  <div className="font-semibold">₹{h.inav_analysis.inav.toFixed(4)}</div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">Market Price</div>
                  <div className="font-semibold">₹{h.inav_analysis.market_price.toFixed(2)}</div>
                </div>
                <div>
                  <div className="text-xs text-slate-500">P/D %</div>
                  <div className="font-semibold" style={{color: h.inav_analysis.premium_discount_pct >= 0 ? '#22c55e':'#ef4444'}}>
                    {fmt_pct(h.inav_analysis.premium_discount_pct, true)}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Historic iNAV chart */}
          <SvgInavChart records={h.historic_records} symbol={h.symbol}/>
        </div>
      )}
    </div>
  );
}

// ── Insight / Risk list ────────────────────────────────────────────────────
function BulletList({ items, colour }) {
  if (!items || items.length === 0) return <p className="text-xs text-slate-500">None reported.</p>;
  return (
    <ul className="space-y-2">
      {items.map((item, i) => (
        <li key={i} className="flex gap-2 text-sm text-slate-300">
          <span style={{color: colour}} className="shrink-0 mt-0.5">▸</span>
          <span>{item}</span>
        </li>
      ))}
    </ul>
  );
}

// ── Main Dashboard ─────────────────────────────────────────────────────────
function Dashboard() {
  const d = DATA;
  const s = d.portfolio_summary;
  const pnl_col = s.total_pnl >= 0 ? '#22c55e' : '#ef4444';
  const health_col = s.health_score >= 70 ? '#22c55e' : s.health_score >= 40 ? '#facc15' : '#ef4444';
  const countdown = useCountdown();
  const gen_dt = new Date(d.generated_at).toLocaleString('en-IN', {
    timeZone:'Asia/Kolkata', day:'2-digit', month:'short', year:'numeric',
    hour:'2-digit', minute:'2-digit'
  });

  return (
    <div className="min-h-screen bg-surface p-4 md:p-8 space-y-8 max-w-7xl mx-auto">

      {/* ── Header ── */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-extrabold text-slate-100 tracking-tight">
            🇮🇳 Portfolio Insight Dashboard
          </h1>
          <p className="text-sm text-slate-400 mt-1">
            {s.num_holdings} holdings · {s.etf_count} ETFs · {s.stock_count} stocks
            &nbsp;·&nbsp; Generated: {gen_dt} IST
          </p>
        </div>
        <div className="flex items-center gap-3 self-start md:self-auto flex-wrap">
          {d.comex.overall_signal && (
            <span className="text-sm px-4 py-1.5 rounded-full font-semibold"
              style={{
                background: signal_color(d.comex.overall_signal) + '22',
                color:       signal_color(d.comex.overall_signal),
                border:      `1px solid ${signal_color(d.comex.overall_signal)}`
              }}>
              COMEX: {d.comex.overall_signal}
            </span>
          )}
          <span className="text-xs px-3 py-1.5 rounded-full font-mono"
            style={{background:'#1e293b', color:'#64748b', border:'1px solid #334155'}}>
            ↻ {countdown}
          </span>
        </div>
      </div>

      {/* ── 4 metric cards ── */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard
          label="Portfolio Value"
          value={fmt_inr(s.total_value)}
          sub={`Invested ${fmt_inr(s.total_invested)}`}
          colour="#e2e8f0"
        />
        <MetricCard
          label="Total P&amp;L"
          value={fmt_inr(s.total_pnl)}
          sub={fmt_pct(s.total_pnl_percent, true)}
          colour={pnl_col}
        />
        <MetricCard
          label="Health Score"
          value={`${s.health_score.toFixed(1)} / 100`}
          sub={`Diversification: ${s.diversification_score.toFixed(1)}`}
          colour={health_col}
        />
        <MetricCard
          label="Allocation"
          value={`${s.etf_allocation_pct.toFixed(1)}% ETF`}
          sub={`${s.direct_equity_allocation_pct.toFixed(1)}% Direct Equity`}
          colour="#818cf8"
        />
      </div>

      {/* ── 2-col: COMEX + Sector ── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ComexPanel comex={d.comex}/>
        <SvgSectorChart data={d.sector_data}/>
      </div>

      {/* ── Holdings ── */}
      <div>
        <SectionTitle>📁 Holdings Deep-Dive</SectionTitle>
        <div className="space-y-4">
          {d.holdings.map(h => <HoldingCard key={h.symbol} h={h}/>)}
        </div>
      </div>

      {/* ── 3-col: Risks / Insights / Rebalancing ── */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-card rounded-2xl p-5 border border-border">
          <SectionTitle>⚠ Portfolio Risks</SectionTitle>
          <BulletList items={d.portfolio_risks} colour="#f97316"/>
        </div>
        <div className="bg-card rounded-2xl p-5 border border-border">
          <SectionTitle>💡 Actionable Insights</SectionTitle>
          <BulletList items={d.actionable_insights} colour="#818cf8"/>
        </div>
        <div className="bg-card rounded-2xl p-5 border border-border">
          <SectionTitle>⚖ Rebalancing Signals</SectionTitle>
          <BulletList items={d.rebalancing_signals} colour="#22d3ee"/>
        </div>
      </div>

      {/* ── Footer ── */}
      <p className="text-center text-xs text-slate-600 pb-4">
        Portfolio Insight · Data sourced from NSE / Yahoo Finance / COMEX · Not financial advice
      </p>
    </div>
  );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<Dashboard/>);
</script>
</body>
</html>
"""


# ── Agent class ─────────────────────────────────────────────────────────────────

class VisualizationAgent:
    """
    Converts a portfolio report dict → a self-contained HTML dashboard file.

    The output file is written to <output_dir>/dashboard.html and is ready
    to open in any modern browser with no server or build step required.
    """

    def __init__(self, output_dir: str = "./output"):
        self.output_dir = output_dir

    # ------------------------------------------------------------------
    def generate(self, report: dict[str, Any]) -> str:
        """
        Build the HTML dashboard from *report* and write it to disk.

        Returns the absolute path of the generated file.
        """
        os.makedirs(self.output_dir, exist_ok=True)

        dashboard_data = _build_dashboard_data(report)
        html = self._render_html(dashboard_data)

        out_path = os.path.join(self.output_dir, "dashboard.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)

        logger.info("Dashboard written to %s", out_path)
        return os.path.abspath(out_path)

    # ------------------------------------------------------------------
    def _render_html(self, data: dict[str, Any]) -> str:
        """Inject the portfolio data JSON into the HTML template."""
        data_json = json.dumps(data, ensure_ascii=False, indent=2)
        return _HTML_TEMPLATE.replace(
            "__PORTFOLIO_DATA_PLACEHOLDER__",
            data_json,
        )

    # ------------------------------------------------------------------
    @staticmethod
    def open_in_browser(path: str) -> None:
        """Open *path* in the system default browser.

        Skipped automatically when the NO_BROWSER environment variable is set
        (e.g. inside a Docker container where no display is available).
        The HTML file is still generated and accessible via the mounted volume.
        """
        if os.environ.get("NO_BROWSER"):
            logger.info("NO_BROWSER set — skipping browser open for: %s", path)
            return
        url = f"file://{os.path.abspath(path)}"
        logger.info("Opening dashboard in browser: %s", url)
        webbrowser.open(url)

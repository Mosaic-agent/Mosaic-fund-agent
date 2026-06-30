import React, { useState, useEffect, useRef, useCallback } from "react";
import { createChart, HistogramSeries } from "lightweight-charts";
import TradingChart from "./TradingChart";

const RANGES = [
  { label: "1M", days: 30 },
  { label: "3M", days: 90 },
  { label: "6M", days: 180 },
  { label: "1Y", days: 365 },
  { label: "ALL", days: null },
];

// ── Histogram chart for premium/discount bars ─────────────────────────────────
// Green bars = discount (value < 0), Red bars = premium (value >= 0)
function PremiumDiscountChart({ data = [], height = 220 }) {
  const containerRef = useRef(null);
  const chartRef     = useRef(null);
  const seriesRef    = useRef(null);
  const [activeRange, setActiveRange] = useState("1Y");

  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: {
        background: { color: "transparent" },
        textColor:  "#475569",
        fontSize:   11,
        fontFamily: "'JetBrains Mono', 'Courier New', monospace",
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.03)" },
        horzLines: { color: "rgba(255,255,255,0.04)" },
      },
      crosshair: {
        vertLine: { color: "#94A3B8", width: 1, style: 2, labelBackgroundColor: "#0D1117" },
        horzLine: { color: "#94A3B8", width: 1, style: 2, labelBackgroundColor: "#0D1117" },
      },
      rightPriceScale: {
        borderColor: "rgba(255,255,255,0.06)",
        textColor:   "#475569",
        scaleMargins: { top: 0.1, bottom: 0.1 },
      },
      timeScale: {
        borderColor: "rgba(255,255,255,0.06)",
        textColor:   "#475569",
        timeVisible: true,
        secondsVisible: false,
        fixRightEdge: true,
        tickMarkFormatter: (time) => {
          let d;
          if (typeof time === "string") d = new Date(time + "T00:00:00Z");
          else if (typeof time === "object" && time !== null && "year" in time)
            d = new Date(Date.UTC(time.year, time.month - 1, time.day));
          else d = new Date(time * 1000);
          return d.toLocaleDateString("en-IN", { day: "2-digit", month: "short" });
        },
      },
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true },
      handleScale:  { mouseWheel: true, pinch: true, axisPressedMouseMove: true },
    });

    const series = chart.addSeries(HistogramSeries, {
      priceLineVisible:   false,
      lastValueVisible:   false,
      base:               0,
    });

    chartRef.current  = chart;
    seriesRef.current = series;

    return () => {
      chart.remove();
      chartRef.current  = null;
      seriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!seriesRef.current || !data.length) return;
    // Color each bar: negative → green (discount), positive → red (premium)
    const colored = data.map(d => ({
      time:  d.time,
      value: d.value,
      color: d.value < 0 ? "#10B981" : "#EF4444",
    }));
    seriesRef.current.setData(colored);
    applyRange(activeRange, data);
  }, [data]);

  const applyRange = useCallback((range, src) => {
    const pts = src || data;
    if (!chartRef.current || !pts.length) return;
    const rangeObj = RANGES.find(r => r.label === range);
    if (!rangeObj || !rangeObj.days) { chartRef.current.timeScale().fitContent(); return; }
    const lastTime = pts[pts.length - 1].time;
    const toDate   = new Date(lastTime + "T00:00:00Z");
    const fromDate = new Date(toDate);
    fromDate.setDate(fromDate.getDate() - rangeObj.days);
    chartRef.current.timeScale().setVisibleRange({
      from: fromDate.toISOString().split("T")[0],
      to:   lastTime,
    });
  }, [data]);

  const handleRange = (range) => { setActiveRange(range); applyRange(range); };

  return (
    <div className="trading-chart-wrapper">
      <div className="tchart-header">
        <div className="tchart-left">
          <div className="chart-title">Premium / Discount % — GOLDBEES vs AMFI NAV</div>
          <div style={{ display: "flex", gap: 12, fontSize: 10, color: "var(--text-muted)", marginTop: 2 }}>
            <span style={{ color: "#EF4444" }}>■ Premium (price above NAV)</span>
            <span style={{ color: "#10B981" }}>■ Discount (price below NAV)</span>
            <span>· ±0.25% = fair-value band</span>
          </div>
        </div>
        <div className="range-picker">
          {RANGES.map(r => (
            <button
              key={r.label}
              className={`range-btn ${activeRange === r.label ? "active" : ""}`}
              onClick={() => handleRange(r.label)}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>
      {data.length === 0
        ? <div className="shimmer" style={{ width: "100%", height, borderRadius: 6 }} />
        : <div ref={containerRef} style={{ width: "100%", height }} />
      }
    </div>
  );
}

function pearson(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 2) return 0;
  const ma = a.slice(0, n).reduce((s, x) => s + x, 0) / n;
  const mb = b.slice(0, n).reduce((s, x) => s + x, 0) / n;
  let num = 0, da = 0, db = 0;
  for (let i = 0; i < n; i++) {
    const ai = a[i] - ma, bi = b[i] - mb;
    num += ai * bi; da += ai * ai; db += bi * bi;
  }
  return da && db ? num / Math.sqrt(da * db) : 0;
}

async function fetchQuery(sql) {
  const res = await fetch(`/api/query?sql=${encodeURIComponent(sql)}`);
  const data = await res.json();
  if (!data || !data.length || data[0].error) return [];
  return data;
}

function StatTile({ label, value, color }) {
  return (
    <div className="glass-card" style={{ padding: "12px 16px", flex: 1 }}>
      <div className="stat-label" style={{ fontSize: 10, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 4 }}>{label}</div>
      <div className="stat-value" style={{ fontSize: 18, fontWeight: 700, color: color || "var(--text-primary)", fontFamily: "var(--font-mono)" }}>{value}</div>
    </div>
  );
}

function Shimmer({ height = 200 }) {
  return <div className="shimmer" style={{ width: "100%", height, borderRadius: 8 }} />;
}

// ── COMEX Gold ───────────────────────────────────────────────────────────────
function GoldSection() {
  const [data, setData] = useState(null); // null=loading, []=empty, [...]=data

  useEffect(() => {
    fetchQuery(
      "SELECT trade_date as date, round(close, 2) as val FROM market_data.daily_prices FINAL WHERE symbol = 'GOLD' AND category = 'commodities' ORDER BY trade_date ASC LIMIT 1000"
    ).then(rows => {
      if (!rows.length) { setData([]); return; }
      setData(rows.map(r => ({ time: String(r.date).split(" ")[0], value: parseFloat(r.val) })));
    }).catch(() => setData([]));
  }, []);

  if (data === null) return <Shimmer height={300} />;
  if (!data.length) return (
    <div className="alert-card red">No COMEX gold data — run Import → commodities to populate.</div>
  );

  const vals   = data.map(d => d.value);
  const latest = vals[vals.length - 1];
  const high   = Math.max(...vals);
  const low    = Math.min(...vals);
  const ret    = vals.length > 1 ? ((latest - vals[0]) / vals[0] * 100) : 0;

  return (
    <div>
      <TradingChart data={data} color="gold" height={220} title="COMEX Gold — Daily Close (USD/troy oz)" defaultRange="1Y" />
      <div style={{ display: "flex", gap: 10, marginTop: 12 }}>
        <StatTile label="Latest"         value={`$${latest.toLocaleString(undefined, { minimumFractionDigits: 2 })}`} color="var(--gold)" />
        <StatTile label="2-Year High"    value={`$${high.toLocaleString(undefined, { minimumFractionDigits: 2 })}`} />
        <StatTile label="2-Year Low"     value={`$${low.toLocaleString(undefined, { minimumFractionDigits: 2 })}`} />
        <StatTile label="2-Year Return"  value={`${ret >= 0 ? "+" : ""}${ret.toFixed(1)}%`} color={ret >= 0 ? "var(--green)" : "var(--red)"} />
      </div>
    </div>
  );
}

// ── GOLDBEES NAV vs Price ────────────────────────────────────────────────────
function GoldbeesSection() {
  const [priceData, setPriceData] = useState(null);
  const [navData,   setNavData]   = useState(null);
  const [alert,     setAlert]     = useState(null); // null=loading

  useEffect(() => {
    Promise.all([
      fetchQuery("SELECT trade_date as date, argMax(close, imported_at) as val FROM market_data.daily_prices WHERE symbol = 'GOLDBEES' AND category = 'etfs' GROUP BY trade_date ORDER BY trade_date ASC LIMIT 1000"),
      fetchQuery("SELECT nav_date as date, if(nav > 100, nav/100, nav) as val FROM market_data.mf_nav FINAL WHERE symbol = 'GOLDBEES' ORDER BY nav_date ASC LIMIT 1000"),
      fetchQuery("SELECT round(p.close, 4) as market_close, round(n.nav_adj, 4) as amfi_nav, round((p.close - n.nav_adj) / n.nav_adj * 100, 3) as premium_disc_pct, p.trade_date FROM (SELECT trade_date, close FROM market_data.daily_prices FINAL WHERE symbol = 'GOLDBEES' AND category = 'etfs') p LEFT JOIN (SELECT nav_date AS trade_date, if(nav_date < '2019-12-23', nav / 100, nav) AS nav_adj FROM market_data.mf_nav FINAL WHERE symbol = 'GOLDBEES') n USING (trade_date) WHERE n.nav_adj > 0 ORDER BY p.trade_date DESC LIMIT 1"),
    ]).then(([pr, nv, al]) => {
      setPriceData(pr.map(r => ({ time: String(r.date).split(" ")[0], value: parseFloat(r.val) })));
      setNavData(nv.map(r => ({ time: String(r.date).split(" ")[0], value: parseFloat(r.val) })));
      setAlert(al[0] || null);
    }).catch(() => { setPriceData([]); setNavData([]); setAlert(null); });
  }, []);

  if (priceData === null) return <Shimmer height={420} />;

  const disc   = alert ? parseFloat(alert.premium_disc_pct) : null;
  const price  = alert ? parseFloat(alert.market_close) : null;
  const nav    = alert ? parseFloat(alert.amfi_nav) : null;
  const date   = alert ? String(alert.trade_date).split(" ")[0] : "";

  let alertEl = null;
  if (disc !== null) {
    if (disc <= -1.0) {
      alertEl = <div className="alert-card red">🚨 <strong>GOLDBEES Discount Alert</strong> — {date} | Market ₹{price?.toFixed(2)} at <strong>{disc > 0 ? "+" : ""}{disc?.toFixed(3)}%</strong> vs NAV ₹{nav?.toFixed(2)} | Discount &gt;−1% — potential buying opportunity</div>;
    } else if (disc < 0) {
      alertEl = <div className="alert-card amber">⚠ <strong>GOLDBEES at Discount</strong> — {date} | Market ₹{price?.toFixed(2)} at {disc?.toFixed(3)}% vs NAV ₹{nav?.toFixed(2)}</div>;
    } else {
      alertEl = <div className="alert-card green">✅ <strong>GOLDBEES at Premium</strong> — {date} | Market ₹{price?.toFixed(2)} at +{disc?.toFixed(3)}% vs NAV ₹{nav?.toFixed(2)}</div>;
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {alertEl}
      {priceData.length > 0
        ? <TradingChart data={priceData} color="cyan"   height={180} title="GOLDBEES — Market Close (₹)" defaultRange="2Y" />
        : <div style={{ fontSize: 12, color: "var(--text-muted)" }}>No ETF price data — import etfs.</div>
      }
      {navData.length > 0
        ? <TradingChart data={navData} color="purple" height={180} title="GOLDBEES — AMFI NAV (₹)" defaultRange="2Y" />
        : <div style={{ fontSize: 12, color: "var(--text-muted)" }}>No AMFI NAV data — import mf.</div>
      }
      <div style={{ fontSize: 11, color: "var(--text-muted)" }}>
        <strong style={{ color: "var(--cyan)" }}>Market Close</strong> = NSE last traded price (Yahoo Finance) · <strong style={{ color: "var(--purple)" }}>AMFI NAV</strong> = AMFI official NAV (MFAPI.in) · Gaps = holidays/Muhurat trading
      </div>
    </div>
  );
}

// ── Premium / Discount Analysis ──────────────────────────────────────────────
function PremiumSection() {
  const [rows,   setRows]  = useState(null);
  const [stats,  setStats] = useState(null);
  const [spread, setSpread] = useState([]);

  useEffect(() => {
    fetchQuery(
      "SELECT p.trade_date as date, round(p.close, 4) as price, round(n.nav_adj, 4) as nav, round((p.close - n.nav_adj) / n.nav_adj * 100, 3) as spread FROM (SELECT trade_date, close FROM market_data.daily_prices FINAL WHERE symbol = 'GOLDBEES' AND category = 'etfs') p LEFT JOIN (SELECT nav_date AS trade_date, if(nav_date < '2019-12-23', nav / 100, nav) AS nav_adj FROM market_data.mf_nav FINAL WHERE symbol = 'GOLDBEES') n USING (trade_date) WHERE n.nav_adj > 0 ORDER BY p.trade_date ASC LIMIT 2000"
    ).then(raw => {
      const filtered = raw.filter(r => Math.abs(parseFloat(r.spread)) <= 10);
      setRows(filtered);

      if (!filtered.length) { setStats(null); setSpread([]); return; }

      const spreads = filtered.map(r => parseFloat(r.spread));
      const prices  = filtered.map(r => parseFloat(r.price));
      const avg     = spreads.reduce((a, b) => a + b, 0) / spreads.length;
      const maxPrem = Math.max(...spreads);
      const maxDisc = Math.min(...spreads);
      const daysDisc = spreads.filter(s => s < -0.25).length;
      const daysPrem = spreads.filter(s => s > 0.25).length;

      const pctChange = prices.map((p, i) => i === 0 ? 0 : (p - prices[i - 1]) / prices[i - 1] * 100);
      const nextDay   = pctChange.slice(1);
      const corrSame  = pearson(spreads, pctChange);
      const corrNext  = pearson(spreads.slice(0, -1), nextDay);

      setStats({ avg, maxPrem, maxDisc, daysDisc, daysPrem, corrSame, corrNext });
      setSpread(filtered.map(r => ({ time: String(r.date).split(" ")[0], value: parseFloat(r.spread) })));
    }).catch(() => { setRows([]); });
  }, []);

  if (rows === null) return <Shimmer height={350} />;
  if (!rows.length) return <div className="alert-card red">No data — import etfs + mf.</div>;

  const fmt = (v, d = 3) => (v >= 0 ? "+" : "") + v.toFixed(d);

  return (
    <div>
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 16 }}>
        <StatTile label="Avg Spread"              value={fmt(stats.avg)} />
        <StatTile label="Max Premium"             value={fmt(stats.maxPrem)} color="var(--red)" />
        <StatTile label="Max Discount"            value={fmt(stats.maxDisc)} color="var(--green)" />
        <StatTile label="Days at Discount >0.25%" value={stats.daysDisc} />
        <StatTile label="Same-day Corr"           value={fmt(stats.corrSame)} color={stats.corrSame > 0 ? "var(--green)" : "var(--red)"} />
        <StatTile label="Next-day Corr"           value={fmt(stats.corrNext)} color={stats.corrNext > 0 ? "var(--green)" : "var(--red)"} />
      </div>
      <PremiumDiscountChart data={spread} height={220} />
    </div>
  );
}

// ── Main ExplorerWorkspace ───────────────────────────────────────────────────
export default function ExplorerWorkspace({ onActivity }) {
  const [section, setSection] = useState("gold");
  const [visited, setVisited] = useState({ gold: false, goldbees: false, premium: false });

  const activate = (s) => {
    setSection(s);
    setVisited(prev => ({ ...prev, [s]: true }));
  };

  // Mark initial section as visited
  useEffect(() => { setVisited(prev => ({ ...prev, gold: true })); }, []);

  const TABS = [
    { key: "gold",     label: "🪙 COMEX Gold" },
    { key: "goldbees", label: "📊 GOLDBEES NAV" },
    { key: "premium",  label: "↕ Premium / Discount" },
  ];

  return (
    <div>
      <div className="explorer-tabs">
        {TABS.map(t => (
          <button
            key={t.key}
            className={`explorer-tab ${section === t.key ? "active" : ""}`}
            onClick={() => activate(t.key)}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className="glass-card desk-card">
        {section === "gold"     && visited.gold     && <GoldSection />}
        {section === "goldbees" && visited.goldbees && <GoldbeesSection />}
        {section === "premium"  && visited.premium  && <PremiumSection />}
      </div>
    </div>
  );
}

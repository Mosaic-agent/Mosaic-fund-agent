import React, { useState, useEffect } from "react";
import { Play, ChevronRight, Cpu, RefreshCw, Wifi, WifiOff } from "lucide-react";
import TradingChart from "./TradingChart";

// FALLBACK_DATASETS: used ONLY when the API call fails (network error / ClickHouse down).
// These are never shown on initial load — state starts empty and shows loading skeletons.
const FALLBACK_DATASETS = {
  gold_usd: {
    title: "Gold vs USD Strategy Desk",
    regime: "—",
    briefOpps: [],
    briefRisks: [
      { name: "📉 USD Strength", change: "▼" },
      { name: "📉 High VIX", change: "▼" },
      { name: "📉 Crude Volatility", change: "▼" },
    ],
    briefSummary: "Connect to ClickHouse to load live market data.",
    chart1Title: "Gold Price (GOLDBEES)", chart2Title: "USDINR Exchange Rate",
    chart3Title: "US 10-Year Bond Yield", chart4Title: "Expected Log 5D Return",
    insightBody: "Run the agent pipeline to generate AI insights based on live ClickHouse data.",
    insightConfidence: "Model data not loaded — trigger pipeline above.",
    sqlQuery: "SELECT trade_date, close FROM market_data.daily_prices WHERE symbol = 'GOLDBEES' ORDER BY trade_date DESC LIMIT 30;",
    prompt: "Compare Gold with USD, Inflation, Interest Rates and explain current macro regime.",
  },
  india_macro: {
    title: "India Macro Indicators",
    regime: "—",
    briefOpps: [],
    briefRisks: [
      { name: "📉 INR Weakness", change: "▼" },
      { name: "📉 Crude Spike", change: "▼" },
      { name: "📉 FII Outflows", change: "▼" },
    ],
    briefSummary: "Connect to ClickHouse to load live macro data.",
    chart1Title: "Bank Nifty (BANKBEES)", chart2Title: "FII Net Flows (Cr)",
    chart3Title: "DII Net Flows (Cr)", chart4Title: "Composite Score Trend",
    insightBody: "Run the agent pipeline to generate AI insights based on live ClickHouse data.",
    insightConfidence: "Model data not loaded — trigger pipeline above.",
    sqlQuery: "SELECT trade_date, fii_net_cr, dii_net_cr FROM market_data.fii_dii_flows ORDER BY trade_date DESC LIMIT 30;",
    prompt: "Analyze recent FII net equity inflows and domestic macroeconomic indicators.",
  },
  nifty_momentum: {
    title: "Nifty Momentum Tracker",
    regime: "—",
    briefOpps: [],
    briefRisks: [
      { name: "📉 Profit Taking", change: "▼" },
      { name: "📉 Vol Expansion", change: "▼" },
      { name: "📉 Global Spill", change: "▼" },
    ],
    briefSummary: "Connect to ClickHouse to load live momentum data.",
    chart1Title: "Nifty 50 (NIFTYBEES)", chart2Title: "Volume Spread Index",
    chart3Title: "Gold Hedge (GOLDBEES)", chart4Title: "Volatility Expansion Ratio",
    insightBody: "Run the agent pipeline to generate AI insights based on live ClickHouse data.",
    insightConfidence: "Model data not loaded — trigger pipeline above.",
    sqlQuery: "SELECT trade_date, close, volume FROM market_data.daily_prices WHERE symbol = 'NIFTYBEES' ORDER BY trade_date DESC LIMIT 30;",
    prompt: "Scan the NSE market for volume breakouts and identify strongly bullish sectoral setups.",
  },
};

const CHART_COLORS = ["cyan", "magenta", "gold", "purple"];

const Q_MAP = {
  gold_usd: [
    "SELECT trade_date as date, close as val FROM market_data.daily_prices FINAL WHERE symbol = 'GOLDBEES' ORDER BY trade_date DESC LIMIT 365",
    "SELECT trade_date as date, close as val FROM market_data.fx_rates FINAL WHERE symbol = 'USDINR' ORDER BY trade_date DESC LIMIT 365",
    "SELECT trade_date as date, close as val FROM market_data.daily_prices FINAL WHERE symbol = 'US10Y' ORDER BY trade_date DESC LIMIT 365",
    "SELECT as_of as date, expected_return_pct as val FROM market_data.ml_predictions FINAL ORDER BY as_of DESC LIMIT 365",
  ],
  india_macro: [
    "SELECT trade_date as date, close as val FROM market_data.daily_prices FINAL WHERE symbol = 'BANKBEES' ORDER BY trade_date DESC LIMIT 365",
    "SELECT trade_date as date, fii_net_cr as val FROM market_data.fii_dii_flows FINAL ORDER BY trade_date DESC LIMIT 365",
    "SELECT trade_date as date, dii_net_cr as val FROM market_data.fii_dii_flows FINAL ORDER BY trade_date DESC LIMIT 365",
    "SELECT as_of as date, composite_score as val FROM market_data.signal_composite FINAL WHERE etf_symbol = 'GOLDBEES' ORDER BY as_of DESC LIMIT 365",
  ],
  nifty_momentum: [
    "SELECT trade_date as date, close as val FROM market_data.daily_prices FINAL WHERE symbol = 'NIFTYBEES' ORDER BY trade_date DESC LIMIT 365",
    "SELECT trade_date as date, volume as val FROM market_data.daily_prices FINAL WHERE symbol = 'NIFTYBEES' ORDER BY trade_date DESC LIMIT 365",
    "SELECT trade_date as date, close as val FROM market_data.daily_prices FINAL WHERE symbol = 'GOLDBEES' ORDER BY trade_date DESC LIMIT 365",
    "SELECT as_of as date, composite_score as val FROM market_data.signal_composite FINAL WHERE etf_symbol = 'NIFTYBEES' ORDER BY as_of DESC LIMIT 365",
  ],
};

// Returns [{ time: 'YYYY-MM-DD', value: number }, ...] sorted ascending
async function fetchVector(sql) {
  const res = await fetch(`/api/query?sql=${encodeURIComponent(sql)}`);
  const data = await res.json();
  if (data && data.length > 0 && !data[0].error) {
    data.sort((a, b) => new Date(a.date) - new Date(b.date));
    return data
      .map(row => ({
        time:  String(row.date).split(" ")[0], // "2026-06-25 00:00:00" → "2026-06-25"
        value: parseFloat(row.val ?? row.close ?? 0),
      }))
      .filter(p => !isNaN(p.value));
  }
  return null;
}

function computeMeta(arr) {
  if (!arr || arr.length < 2) return null;
  const latest = arr[arr.length - 1].value;
  const prev   = arr[arr.length - 2].value;
  const diff   = prev !== 0 ? ((latest - prev) / prev * 100) : 0;
  return {
    price:  latest.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    change: `${diff >= 0 ? "+" : ""}${diff.toFixed(2)}%`,
    isUp:   diff >= 0,
  };
}

// Skeleton loader for chart card
function ChartSkeleton({ title }) {
  return (
    <div className="glass-card chart-container span-6">
      <div className="chart-header">
        <div className="chart-title">{title}</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 4, alignItems: "flex-end" }}>
          <div className="shimmer" style={{ width: 70, height: 20, backgroundSize: "200% 100%", borderRadius: 4 }} />
          <div className="shimmer" style={{ width: 50, height: 14, backgroundSize: "200% 100%", borderRadius: 20 }} />
        </div>
      </div>
      <div className="shimmer" style={{ width: "100%", height: 165, backgroundSize: "200% 100%", borderRadius: 8 }} />
    </div>
  );
}

export default function DashboardWorkspace({ preset, onActivity }) {
  const fb = FALLBACK_DATASETS[preset] || FALLBACK_DATASETS.gold_usd;
  const queries = Q_MAP[preset] || Q_MAP.gold_usd;

  // All state starts empty — no static data pre-populated
  const [loading, setLoading] = useState(true);
  const [apiConnected, setApiConnected] = useState(null); // null = checking, true/false
  const [morningBrief, setMorningBrief] = useState({ regime: null, opps: [], risks: [], summary: null });
  const [charts, setCharts] = useState([null, null, null, null]); // null = loading
  const [metas, setMetas] = useState([null, null, null, null]);
  const [promptText, setPromptText] = useState(fb.prompt);
  const [sqlText, setSqlText] = useState(fb.sqlQuery);
  const [notebookResult, setNotebookResult] = useState("");
  const [notebookTab, setNotebookTab] = useState("sql");
  const [insightBody, setInsightBody] = useState(null);
  const [insightConf, setInsightConf] = useState(null);
  const [isAgentRunning, setIsAgentRunning] = useState(false);

  const chartTitles = [fb.chart1Title, fb.chart2Title, fb.chart3Title, fb.chart4Title];

  useEffect(() => {
    // Reset to loading state on preset change
    setLoading(true);
    setApiConnected(null);
    setMorningBrief({ regime: null, opps: [], risks: [], summary: null });
    setCharts([null, null, null, null]);
    setMetas([null, null, null, null]);
    setInsightBody(null);
    setInsightConf(null);
    setSqlText(fb.sqlQuery);
    setPromptText(fb.prompt);

    Promise.all([fetchMorningBrief(), fetchCharts()]).then(() => setLoading(false));
  }, [preset]);

  const fetchMorningBrief = async () => {
    try {
      // Regime from ML predictions
      const qReg = "SELECT regime_signal, prob_up FROM market_data.ml_predictions FINAL ORDER BY as_of DESC LIMIT 1";
      const resReg = await fetch(`/api/query?sql=${encodeURIComponent(qReg)}`);
      const dataReg = await resReg.json();

      let regime = null;
      if (dataReg?.length && !dataReg[0].error) {
        const pct = (parseFloat(dataReg[0].prob_up) * 100).toFixed(0);
        regime = `${dataReg[0].regime_signal} (${pct}% probability)`;
        setApiConnected(true);
      }

      // Top opportunities from signal_composite
      const qOpps = "SELECT etf_symbol, composite_score FROM market_data.signal_composite FINAL ORDER BY as_of DESC, composite_score DESC LIMIT 3";
      const resOpps = await fetch(`/api/query?sql=${encodeURIComponent(qOpps)}`);
      const dataOpps = await resOpps.json();
      let opps = [];
      if (dataOpps?.length && !dataOpps[0].error) {
        opps = dataOpps.map(r => ({
          name: `📈 ${r.etf_symbol}`,
          change: `Score: ${parseInt(r.composite_score)}`,
        }));
      }

      // Bottom risks (lowest scoring ETFs)
      const qRisks = "SELECT etf_symbol, composite_score FROM market_data.signal_composite FINAL ORDER BY as_of DESC, composite_score ASC LIMIT 3";
      const resRisks = await fetch(`/api/query?sql=${encodeURIComponent(qRisks)}`);
      const dataRisks = await resRisks.json();
      let risks = fb.briefRisks;
      if (dataRisks?.length && !dataRisks[0].error) {
        risks = dataRisks.map(r => ({
          name: `📉 ${r.etf_symbol}`,
          change: `Score: ${parseInt(r.composite_score)}`,
        }));
      }

      // Latest insight from ml_predictions
      const qInsight = "SELECT regime_signal, prob_up, expected_return_pct, cv_auc, hit_ratio FROM market_data.ml_predictions FINAL ORDER BY as_of DESC LIMIT 1";
      const resInsight = await fetch(`/api/query?sql=${encodeURIComponent(qInsight)}`);
      const dataInsight = await resInsight.json();
      if (dataInsight?.length && !dataInsight[0].error) {
        const row = dataInsight[0];
        const probPct = (parseFloat(row.prob_up) * 100).toFixed(0);
        const expRet = parseFloat(row.expected_return_pct).toFixed(2);
        setInsightBody(
          `ML model signals ${row.regime_signal} with ${probPct}% upward probability over the next 5 trading days. ` +
          `Expected log return: ${expRet >= 0 ? "+" : ""}${expRet}%. ` +
          `Pipeline is live and reading from ClickHouse.`
        );
        setInsightConf(
          `Model AUC: ${parseFloat(row.cv_auc || 0).toFixed(3)} | Hit Ratio: ${parseFloat(row.hit_ratio || 0).toFixed(1)}%`
        );
      }

      setMorningBrief({
        regime: regime || "No ML prediction data",
        opps: opps.length ? opps : [{ name: "No signal data", change: "—" }],
        risks,
        summary: opps.length
          ? `${opps.length} ETF signals loaded from ClickHouse. Latest regime: ${regime || "—"}.`
          : fb.briefSummary,
      });
    } catch (e) {
      // API unreachable — show connection error state
      setApiConnected(false);
      setMorningBrief({
        regime: "API Unreachable",
        opps: [{ name: "ClickHouse not connected", change: "—" }],
        risks: fb.briefRisks,
        summary: `Cannot reach /api/query. Check that ClickHouse is running. (${e.message})`,
      });
    }
  };

  const fetchCharts = async () => {
    const results = await Promise.allSettled(queries.map(q => fetchVector(q)));
    const data = results.map(r => r.status === "fulfilled" ? r.value : null);
    setCharts(data);
    setMetas(data.slice(0, 4).map(arr => computeMeta(arr)));
  };

  const refresh = () => {
    setLoading(true);
    setApiConnected(null);
    setCharts([null, null, null, null]);
    setMetas([null, null, null, null]);
    setMorningBrief({ regime: null, opps: [], risks: [], summary: null });
    setInsightBody(null);
    setInsightConf(null);
    Promise.all([fetchMorningBrief(), fetchCharts()]).then(() => setLoading(false));
  };

  const runNotebookSQL = async () => {
    setNotebookResult("Executing query...");
    try {
      const res = await fetch(`/api/query?sql=${encodeURIComponent(sqlText)}`);
      const data = await res.json();
      if (data?.length && !data[0].error) {
        setNotebookResult(JSON.stringify(data.slice(0, 10), null, 2));
      } else if (data?.[0]?.error) {
        setNotebookResult(`⚠ ClickHouse Error: ${data[0].error}`);
      } else {
        setNotebookResult("No records found.");
      }
    } catch (e) {
      setNotebookResult(`⚠ Connection failed: ${e.message}`);
    }
  };

  const triggerAgentPipeline = async () => {
    if (isAgentRunning) return;
    setIsAgentRunning(true);

    // Signal right panel immediately so animation starts regardless of API outcome
    const MIN_ANIM_MS = 9000; // 7 steps × ~1.3s each — let animation complete
    const animDone = new Promise(r => setTimeout(r, MIN_ANIM_MS));

    onActivity && onActivity({
      isRunning: true,
      label: "Running Quant Pipeline",
      logs: [`> ${promptText}`],
      workspaceOp: "agent",
    });

    try {
      const res = await fetch("/api/agent/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: promptText }),
      });
      const data = await res.json();
      if (data.status === "success") {
        if (data.insightBody)       setInsightBody(data.insightBody);
        if (data.insightConfidence) setInsightConf(data.insightConfidence);
      } else {
        setInsightBody(`Agent error: ${data.error || "Unknown error from server"}`);
      }
    } catch (e) {
      setInsightBody(`Connection error: ${e.message}`);
    }

    // Wait for animation to finish before signalling idle
    await animDone;
    setIsAgentRunning(false);
    onActivity && onActivity({ isRunning: false, label: "Pipeline complete", logs: [], workspaceOp: null });
    refresh();
  };

  const upColor = c => c && c.startsWith("+") ? "change-up" : "change-down";

  // Connection status badge
  const ConnStatus = () => {
    if (apiConnected === null) return (
      <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: "var(--text-muted)" }}>
        <span className="pulse-dot cyan" style={{ width: 6, height: 6 }} /> Connecting...
      </div>
    );
    if (apiConnected === false) return (
      <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: "var(--red)" }}>
        <WifiOff size={11} /> ClickHouse Unreachable
      </div>
    );
    return (
      <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: "var(--green)" }}>
        <Wifi size={11} /> Live Data
      </div>
    );
  };

  return (
    <div>
      {/* Morning Brief */}
      <div className="widgets-grid" style={{ marginBottom: 14 }}>
        <div className="glass-card brief-widget span-12 accent-cyan">
          <div className="brief-col">
            <div className="brief-label" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span>Market Regime</span>
              <ConnStatus />
            </div>
            {morningBrief.regime ? (
              <div className="brief-regime">{morningBrief.regime}</div>
            ) : (
              <div className="shimmer" style={{ width: 180, height: 22, marginTop: 6, backgroundSize: "200% 100%", borderRadius: 4 }} />
            )}
          </div>
          <div className="brief-col">
            <div className="brief-label">Top Opportunities</div>
            {morningBrief.opps.length === 0 ? (
              [1, 2, 3].map(i => (
                <div className="shimmer" key={i} style={{ width: "90%", height: 14, marginBottom: 5, backgroundSize: "200% 100%", borderRadius: 4 }} />
              ))
            ) : morningBrief.opps.map((o, i) => (
              <div className="brief-opp-row" key={i}>
                <span className="brief-opp-name">{o.name}</span>
                <span className="stat-delta-up" style={{ fontWeight: 700, fontSize: 12 }}>{o.change}</span>
              </div>
            ))}
          </div>
          <div className="brief-col">
            <div className="brief-label">Macro Risks</div>
            {morningBrief.risks.map((r, i) => (
              <div className="brief-opp-row" key={i}>
                <span className="brief-opp-name" style={{ color: "var(--text-muted)" }}>{r.name}</span>
                <span className="stat-delta-down" style={{ fontWeight: 700, fontSize: 12 }}>{r.change}</span>
              </div>
            ))}
          </div>
          <div className="brief-col">
            <div className="brief-label" style={{ display: "flex", justifyContent: "space-between" }}>
              <span>Morning Brief</span>
              <button
                onClick={refresh}
                style={{ background: "none", border: "none", color: "var(--text-muted)", cursor: "pointer", padding: 0 }}
              >
                <RefreshCw size={11} />
              </button>
            </div>
            {morningBrief.summary ? (
              <div className="brief-summary">{morningBrief.summary}</div>
            ) : (
              <>
                <div className="shimmer" style={{ width: "95%", height: 12, marginBottom: 4, backgroundSize: "200% 100%", borderRadius: 4 }} />
                <div className="shimmer" style={{ width: "80%", height: 12, backgroundSize: "200% 100%", borderRadius: 4 }} />
              </>
            )}
          </div>
        </div>
      </div>

      {/* Prompt Bar */}
      <div className="prompt-bar">
        <div className="prompt-input-wrapper">
          <input
            type="text"
            className="prompt-input"
            value={promptText}
            onChange={e => setPromptText(e.target.value)}
            placeholder="Ask Mosaic Agent to run pipeline analysis..."
            onKeyDown={e => e.key === "Enter" && triggerAgentPipeline()}
          />
          <button className="prompt-submit-btn" onClick={triggerAgentPipeline} disabled={isAgentRunning}>
            <ChevronRight size={18} color="#000" />
          </button>
        </div>
      </div>

      {/* Charts Grid */}
      <div className="widgets-grid" style={{ marginBottom: 14 }}>
        {chartTitles.map((title, idx) => {
          const chartData = charts[idx];
          const meta = metas[idx];
          if (chartData === null) return <ChartSkeleton key={idx} title={title} />;
          return (
            <div key={idx} className="glass-card chart-container span-6" style={{ padding: "14px 14px 8px" }}>
              <TradingChart
                data={chartData}
                color={CHART_COLORS[idx]}
                height={195}
                title={title}
                price={meta?.price}
                change={meta?.change}
                isUp={meta?.isUp}
              />
              <div className="chart-ai-actions">
                {["Explain", "Forecast", "Anomalies", "Compare"].map(a => (
                  <button key={a} className="ai-action-btn">{a}</button>
                ))}
              </div>
            </div>
          );
        })}
      </div>

      {/* AI Insights + Notebook + Sources */}
      <div className="widgets-grid">
        {/* AI Insight */}
        <div className="glass-card insight-widget span-12 accent-purple">
          <div className="insight-header">
            <Cpu size={13} color="var(--purple)" />
            AI Pipeline Forecast &amp; Allocation Advice
          </div>
          {insightBody ? (
            <>
              <div className="insight-body">{insightBody}</div>
              <div className="insight-meta">{insightConf}</div>
            </>
          ) : (
            <>
              <div className="shimmer" style={{ width: "100%", height: 14, marginBottom: 6, backgroundSize: "200% 100%", borderRadius: 4 }} />
              <div className="shimmer" style={{ width: "75%", height: 14, backgroundSize: "200% 100%", borderRadius: 4 }} />
            </>
          )}
        </div>

        {/* SQL Notebook */}
        <div className="glass-card notebook-widget span-8">
          <div className="notebook-tabs">
            {["sql", "python", "markdown"].map(tab => (
              <button
                key={tab}
                className={`notebook-tab ${notebookTab === tab ? "active" : ""}`}
                onClick={() => setNotebookTab(tab)}
              >
                {tab.toUpperCase()}
              </button>
            ))}
          </div>
          <div className="code-editor-wrapper">
            <textarea
              className="code-editor"
              value={sqlText}
              onChange={e => setSqlText(e.target.value)}
              spellCheck={false}
            />
            <button className="run-btn" onClick={runNotebookSQL}>
              <Play size={12} fill="currentColor" />
            </button>
          </div>
          {notebookResult && (
            <pre className="notebook-result">{notebookResult}</pre>
          )}
        </div>

        {/* Data Sources */}
        <div className="glass-card sources-widget span-4">
          <div className="sources-title">Ingested Data Sources</div>
          <div className="sources-grid">
            {[
              { name: "NSE India",     live: apiConnected !== false },
              { name: "Yahoo Finance", live: apiConnected !== false },
              { name: "ClickHouse",    live: apiConnected === true },
              { name: "CFTC COT",      live: apiConnected !== false },
              { name: "Screener.in",   live: apiConnected !== false },
              { name: "Zerodha Kite",  live: false },
            ].map(s => (
              <div key={s.name} className="source-item">
                <span className="source-name">{s.name}</span>
                <span className={`source-dot ${s.live ? "source-dot-live" : "source-dot-offline"}`} />
              </div>
            ))}
          </div>
          {apiConnected === false && (
            <div style={{ marginTop: 10, fontSize: 11, color: "var(--red)", lineHeight: 1.5 }}>
              ⚠ Cannot reach /api/query. Is the studio container running?
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

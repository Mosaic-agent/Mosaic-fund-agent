import React, { useState } from "react";

function formatMarkdown(text) {
  if (!text) return null;
  return text.split("\n").map((line, i) => {
    if (line.startsWith("### ")) return <h3 key={i} style={{ color: "var(--cyan)", fontSize: 12, textTransform: "uppercase", margin: "14px 0 6px" }}>{line.slice(4)}</h3>;
    if (line.startsWith("## "))  return <h2 key={i} style={{ color: "var(--text-primary)", fontSize: 13.5, margin: "14px 0 6px", fontWeight: 700 }}>{line.slice(3)}</h2>;
    if (line.startsWith("- "))   return <li key={i} style={{ marginLeft: 14, marginBottom: 4, color: "var(--text-secondary)" }}>{line.slice(2)}</li>;
    return <p key={i} style={{ margin: "3px 0", color: "var(--cyan)", fontFamily: "var(--font-mono)", fontSize: 12 }}>{line}</p>;
  });
}

export default function BacktestWorkspace({ onActivity }) {
  const [symbol, setSymbol]     = useState("GOLDBEES");
  const [maType, setMaType]     = useState("sma");
  const [fast, setFast]         = useState(50);
  const [slow, setSlow]         = useState(200);
  const [report, setReport]     = useState("");
  const [running, setRunning]   = useState(false);

  const runBacktest = async () => {
    setRunning(true);
    setReport("Simulating Moving Average Crossover Backtest on ClickHouse prices...");
    onActivity && onActivity({
      isRunning: true,
      label: "MA Crossover Backtest",
      workspaceOp: "backtest",
      logs: [`> ${symbol} ${maType.toUpperCase()} ${fast}/${slow} crossover`],
    });

    try {
      const res = await fetch("/api/backtest/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol, fast: parseInt(fast), slow: parseInt(slow), ma_type: maType }),
      });
      const data = await res.json();
      setReport(data.status === "success" ? data.report : `⚠ Backtester error: ${data.error}`);
    } catch (e) {
      setReport(`⚠ Connection error: ${e.message}`);
    }

    setRunning(false);
    onActivity && onActivity({ isRunning: false, label: "Backtest complete", workspaceOp: null, logs: [] });
  };

  return (
    <div className="glass-card desk-card">
      <div className="desk-title">📈 MA Crossover Backtester</div>
      <div className="desk-subtitle">
        Simulate SMA/EMA golden-cross / death-cross strategies on historical ClickHouse price data.
      </div>

      <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
        <input
          type="text"
          className="text-input"
          value={symbol}
          onChange={e => setSymbol(e.target.value.toUpperCase())}
          placeholder="Symbol"
          style={{ width: 130 }}
        />
        <select className="text-input" value={maType} onChange={e => setMaType(e.target.value)}>
          <option value="sma">SMA</option>
          <option value="ema">EMA</option>
        </select>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <span style={{ fontSize: 12, color: "var(--text-muted)" }}>Fast</span>
          <input
            type="number"
            className="text-input"
            value={fast}
            onChange={e => setFast(e.target.value)}
            style={{ width: 70 }}
          />
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <span style={{ fontSize: 12, color: "var(--text-muted)" }}>Slow</span>
          <input
            type="number"
            className="text-input"
            value={slow}
            onChange={e => setSlow(e.target.value)}
            style={{ width: 70 }}
          />
        </div>
        <button className="trigger-btn" onClick={runBacktest} disabled={running}>
          🚀 {running ? "Running..." : "Run Backtest"}
        </button>
      </div>

      {/* Preset strategies */}
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        <span style={{ fontSize: 11, color: "var(--text-muted)", alignSelf: "center" }}>Presets:</span>
        {[
          { label: "Golden Cross", fast: 50, slow: 200, ma: "sma" },
          { label: "MACD Proxy",   fast: 12, slow: 26,  ma: "ema" },
          { label: "Weekly Trend", fast: 20, slow: 50,  ma: "ema" },
        ].map(p => (
          <button
            key={p.label}
            className="ai-action-btn"
            style={{ fontSize: 11 }}
            onClick={() => { setFast(p.fast); setSlow(p.slow); setMaType(p.ma); }}
          >
            {p.label} ({p.fast}/{p.slow})
          </button>
        ))}
      </div>

      <div>
        <div style={{ fontSize: 12, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8, fontFamily: "var(--font-display)" }}>
          Strategy Performance Report
        </div>
        <div className="result-box">
          {running ? (
            <div style={{ display: "flex", alignItems: "center", gap: 10, color: "var(--cyan)" }}>
              <span className="pulse-dot cyan" />
              Simulating backtest trades on ClickHouse price history...
            </div>
          ) : report ? (
            <div>{formatMarkdown(report)}</div>
          ) : (
            <div className="empty-state" style={{ border: "none", padding: "30px 0" }}>
              <div className="empty-state-icon">📊</div>
              Configure parameters and click "Run Backtest" to simulate the crossover strategy.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

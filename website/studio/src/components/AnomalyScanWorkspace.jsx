import React, { useState } from "react";

function formatMarkdown(text) {
  if (!text) return null;
  const lines = text.split("\n");
  return lines.map((line, i) => {
    if (line.startsWith("### ")) {
      return <h3 key={i} style={{ color: "var(--cyan)", fontSize: 12, textTransform: "uppercase", margin: "14px 0 6px", letterSpacing: 0.6 }}>{line.slice(4)}</h3>;
    }
    if (line.startsWith("## ")) {
      return <h2 key={i} style={{ color: "var(--text-primary)", fontSize: 13.5, margin: "16px 0 6px", fontWeight: 700 }}>{line.slice(3)}</h2>;
    }
    if (line.startsWith("**") && line.endsWith("**")) {
      return <p key={i} style={{ color: "#fff", fontWeight: 700, margin: "4px 0" }}>{line.slice(2, -2)}</p>;
    }
    if (line.startsWith("- ")) {
      return <li key={i} style={{ marginLeft: 14, marginBottom: 4, color: "var(--text-secondary)" }}>{line.slice(2)}</li>;
    }
    if (line.startsWith("> ")) {
      return (
        <blockquote key={i} style={{ borderLeft: "2px solid var(--purple)", paddingLeft: 10, margin: "8px 0", color: "var(--text-muted)", fontStyle: "italic" }}>
          {line.slice(2)}
        </blockquote>
      );
    }
    return <p key={i} style={{ margin: "3px 0", color: "var(--text-secondary)" }}>{line}</p>;
  });
}

export default function AnomalyScanWorkspace({ onActivity }) {
  const [symbol, setSymbol] = useState("GOLDBEES");
  const [days, setDays] = useState("180");
  const [report, setReport] = useState("");
  const [running, setRunning] = useState(false);

  const runScan = async () => {
    setRunning(true);
    setReport("Running composite Isolation Forest + GARCH volatility anomaly scan...");
    onActivity && onActivity({
      isRunning: true,
      label: "GARCH+IF+PELT Anomaly Scan",
      workspaceOp: "anomaly",
      logs: [`> Scanning ${symbol} over ${days}d lookback`],
    });

    try {
      const res = await fetch("/api/anomaly/scan", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol, days: parseInt(days) }),
      });
      const data = await res.json();
      setReport(data.status === "success" ? data.report : `⚠ Scanner error: ${data.error}`);
    } catch (e) {
      setReport(`⚠ Connection error: ${e.message}`);
    }

    setRunning(false);
    onActivity && onActivity({ isRunning: false, label: "Anomaly scan complete", workspaceOp: null, logs: [] });
  };

  return (
    <div className="glass-card desk-card">
      <div className="desk-title">🔬 Volatility Anomaly &amp; Shock Attributor</div>
      <div className="desk-subtitle">
        4-step composite pipeline: MAD robust Z → GARCH(1,1) → Isolation Forest → PELT change-point detection.
      </div>

      <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
        <input
          type="text"
          className="text-input"
          value={symbol}
          onChange={e => setSymbol(e.target.value.toUpperCase())}
          placeholder="Ticker (e.g. GOLDBEES)"
          style={{ width: 180 }}
        />
        <select
          className="text-input"
          value={days}
          onChange={e => setDays(e.target.value)}
        >
          <option value="90">90 Days</option>
          <option value="180">180 Days</option>
          <option value="365">365 Days</option>
          <option value="730">730 Days</option>
        </select>
        <button className="trigger-btn" onClick={runScan} disabled={running}>
          🔬 {running ? "Scanning..." : "Scan Ticker"}
        </button>
      </div>

      <div>
        <div style={{ fontSize: 12, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8, fontFamily: "var(--font-display)" }}>
          Shock Attribution Report
        </div>
        <div className="result-box">
          {running ? (
            <div style={{ display: "flex", alignItems: "center", gap: 10, color: "var(--cyan)" }}>
              <span className="pulse-dot cyan" />
              Running GARCH+IF+PELT pipeline on ClickHouse data...
            </div>
          ) : report ? (
            <div>{formatMarkdown(report)}</div>
          ) : (
            <div className="empty-state" style={{ border: "none", padding: "30px 0" }}>
              <div className="empty-state-icon">🔬</div>
              Enter a ticker symbol and click "Scan Ticker" to run the anomaly detection pipeline.
            </div>
          )}
        </div>
      </div>

      {/* Pipeline info */}
      <div style={{
        background: "rgba(0,229,200,0.04)",
        border: "1px solid rgba(0,229,200,0.1)",
        borderRadius: "var(--radius-md)",
        padding: "12px 16px",
        fontSize: 11.5,
        color: "var(--text-muted)",
        lineHeight: 1.6,
      }}>
        <span style={{ color: "var(--cyan)", fontWeight: 700 }}>Pipeline:</span>{" "}
        MAD Z-score → GARCH(1,1) standardized residuals → Isolation Forest confidence multiplier → PELT change-point (rbf cost).
        Corporate actions (splits/bonuses) are automatically suppressed using NSE CA data.
        Regime labels: 🔀 Regime Shift (PELT-confirmed) | 🏦 Corporate Action (suppressed).
      </div>
    </div>
  );
}

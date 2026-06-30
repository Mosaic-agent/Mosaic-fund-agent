import React, { useState } from "react";

function formatMarkdown(text) {
  if (!text) return null;
  return text.split("\n").map((line, i) => {
    if (line.startsWith("### ")) return <h3 key={i} style={{ color: "var(--cyan)", fontSize: 12, textTransform: "uppercase", margin: "14px 0 6px" }}>{line.slice(4)}</h3>;
    if (line.startsWith("## "))  return <h2 key={i} style={{ color: "var(--text-primary)", fontSize: 13.5, margin: "14px 0 6px", fontWeight: 700 }}>{line.slice(3)}</h2>;
    if (line.startsWith("- "))   return <li key={i} style={{ marginLeft: 14, marginBottom: 4, color: "var(--text-secondary)" }}>{line.slice(2)}</li>;
    if (line.startsWith("> "))   return <blockquote key={i} style={{ borderLeft: "2px solid var(--purple)", paddingLeft: 10, margin: "8px 0", color: "var(--text-muted)", fontStyle: "italic" }}>{line.slice(2)}</blockquote>;
    return <p key={i} style={{ margin: "3px 0", color: "var(--text-secondary)" }}>{line}</p>;
  });
}

export default function DilutionWorkspace({ onActivity }) {
  const [symbol, setSymbol] = useState("");
  const [report, setReport] = useState("");
  const [running, setRunning] = useState(false);

  const runAudit = async () => {
    if (!symbol.trim()) { alert("Please enter a stock symbol."); return; }
    setRunning(true);
    setReport(`Scraping Screener.in shareholding pattern for ${symbol.toUpperCase()}...`);
    onActivity && onActivity({
      isRunning: true,
      label: "Promoter Dilution Audit",
      workspaceOp: "dilution",
      logs: [`> Auditing ${symbol.toUpperCase()} shareholding pattern`],
    });

    try {
      const res = await fetch("/api/dilution/check", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol: symbol.toUpperCase() }),
      });
      const data = await res.json();
      setReport(data.status === "success" ? data.report : `⚠ Auditor error: ${data.error}`);
    } catch (e) {
      setReport(`⚠ Connection error: ${e.message}`);
    }

    setRunning(false);
    onActivity && onActivity({ isRunning: false, label: "Audit complete", workspaceOp: null, logs: [] });
  };

  return (
    <div className="glass-card desk-card">
      <div className="desk-title">🕵️ Promoter Shareholding &amp; Dilution Auditor</div>
      <div className="desk-subtitle">
        Verify whether a promoter % drop represents an actual sale or equity dilution (QIP/rights/ESOP).
      </div>

      <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
        <input
          type="text"
          className="text-input"
          value={symbol}
          onChange={e => setSymbol(e.target.value)}
          placeholder="NSE Symbol (e.g. TECHNO, RELIANCE)"
          style={{ flex: 1, maxWidth: 280 }}
          onKeyDown={e => e.key === "Enter" && runAudit()}
        />
        <button className="trigger-btn" onClick={runAudit} disabled={running}>
          🕵️ {running ? "Auditing..." : "Run Audit"}
        </button>
      </div>

      <div>
        <div style={{ fontSize: 12, fontWeight: 700, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8, fontFamily: "var(--font-display)" }}>
          Auditor Analysis Summary
        </div>
        <div className="result-box">
          {running ? (
            <div style={{ display: "flex", alignItems: "center", gap: 10, color: "var(--cyan)" }}>
              <span className="pulse-dot cyan" />
              Scraping Screener.in shareholding data...
            </div>
          ) : report ? (
            <div>{formatMarkdown(report)}</div>
          ) : (
            <div className="empty-state" style={{ border: "none", padding: "30px 0" }}>
              <div className="empty-state-icon">🕵️</div>
              Enter an NSE stock symbol and run audit to check promoter dilution patterns.
            </div>
          )}
        </div>
      </div>

      {/* Dilution check guide */}
      <div style={{
        background: "rgba(245,158,11,0.04)",
        border: "1px solid rgba(245,158,11,0.12)",
        borderRadius: "var(--radius-md)",
        padding: "12px 16px",
        fontSize: 11.5,
        color: "var(--text-muted)",
        lineHeight: 1.6,
      }}>
        <span style={{ color: "var(--gold)", fontWeight: 700 }}>Dilution Check Rule:</span>{" "}
        A promoter-% drop with <strong style={{ color: "var(--text-primary)" }}>unchanged absolute share count</strong> = dilution (QIP/rights/ESOP), not a sale.
        A drop with <strong style={{ color: "var(--red)" }}>lower absolute share count</strong> = actual sell-down (red flag).
        Always cross-reference with annual report "Equity Capital" line.
      </div>
    </div>
  );
}

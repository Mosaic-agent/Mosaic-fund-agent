import React, { useState, useEffect } from "react";
import { Terminal, ExternalLink, ShieldCheck, ShieldAlert, RefreshCw, Key } from "lucide-react";
import TerminalLog from "./TerminalLog";

// ── Shoonya OAuth Authentication Card ────────────────────────────────────────
function ShoonyaAuthCard() {
  const [status,   setStatus]   = useState(null);  // null=loading
  const [loginUrl, setLoginUrl] = useState("");
  const [code,     setCode]     = useState("");
  const [busy,     setBusy]     = useState(false);
  const [message,  setMessage]  = useState("");
  const [msgType,  setMsgType]  = useState(""); // "success"|"error"

  const loadStatus = async () => {
    setStatus(null);
    try {
      const res  = await fetch("/api/shoonya/status");
      const data = await res.json();
      setStatus(data);
    } catch { setStatus({ configured: false, active: false }); }
  };

  const fetchLoginUrl = async () => {
    try {
      const res  = await fetch("/api/shoonya/login-url");
      const data = await res.json();
      if (data.url) {
        setLoginUrl(data.url);
        window.open(data.url, "_blank", "noopener");
      } else {
        setMessage(data.error || "Could not generate login URL"); setMsgType("error");
      }
    } catch (e) { setMessage(e.message); setMsgType("error"); }
  };

  const authenticate = async () => {
    if (!code.trim()) { setMessage("Paste the OAuth code from the redirect URL"); setMsgType("error"); return; }
    setBusy(true); setMessage("");
    try {
      const res  = await fetch("/api/shoonya/authenticate", {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ code: code.trim() }),
      });
      const data = await res.json();
      if (data.status === "success") {
        setMessage(`✓ ${data.message}`); setMsgType("success");
        setCode(""); setLoginUrl("");
        await loadStatus();
      } else {
        setMessage(data.error || "Authentication failed"); setMsgType("error");
      }
    } catch (e) { setMessage(e.message); setMsgType("error"); }
    setBusy(false);
  };

  useEffect(() => { loadStatus(); }, []);

  const isActive      = status?.active;
  const isConfigured  = status?.configured;

  return (
    <div className="glass-card" style={{ padding: "18px 20px", display: "flex", flexDirection: "column", gap: 14 }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <Key size={15} color="var(--cyan)" />
          <span style={{ fontSize: 13, fontWeight: 700, color: "var(--text-primary)" }}>Shoonya OAuth Session</span>
        </div>
        <button
          onClick={loadStatus}
          style={{ background: "none", border: "none", color: "var(--text-muted)", cursor: "pointer", padding: 4 }}
          title="Refresh status"
        >
          <RefreshCw size={12} />
        </button>
      </div>

      {/* Session status badge */}
      {status === null ? (
        <div className="shimmer" style={{ height: 32, borderRadius: 6 }} />
      ) : (
        <div style={{
          display: "flex", alignItems: "center", gap: 10,
          padding: "8px 14px", borderRadius: 8,
          border: `1px solid ${isActive ? "var(--green)" : isConfigured ? "var(--gold)" : "var(--red)"}`,
          background: isActive ? "rgba(16,185,129,0.07)" : isConfigured ? "rgba(245,158,11,0.07)" : "rgba(239,68,68,0.07)",
        }}>
          {isActive
            ? <ShieldCheck size={14} color="var(--green)" />
            : <ShieldAlert size={14} color={isConfigured ? "var(--gold)" : "var(--red)"} />
          }
          <div>
            <div style={{ fontSize: 12, fontWeight: 600, color: isActive ? "var(--green)" : isConfigured ? "var(--gold)" : "var(--red)" }}>
              {isActive ? "Session Active" : isConfigured ? "Session Expired / Not Authenticated" : "Not Configured"}
            </div>
            {isActive && status.saved_at && (
              <div style={{ fontSize: 10, color: "var(--text-muted)", marginTop: 1 }}>
                Authenticated at {status.saved_at.slice(0, 19)} · User: {status.user_id}
              </div>
            )}
            {!isConfigured && (
              <div style={{ fontSize: 10, color: "var(--text-muted)", marginTop: 1 }}>
                Set SHOONYA_USER_ID and SHOONYA_API_SECRET in .env
              </div>
            )}
          </div>
        </div>
      )}

      {/* Auth flow — only show when configured */}
      {status && isConfigured && (
        <>
          <div style={{ fontSize: 11, color: "var(--text-muted)", lineHeight: 1.6 }}>
            <strong style={{ color: "var(--text-secondary)" }}>How to authenticate:</strong><br />
            1. Click <em>Open Login Page</em> — log in with your password and TOTP in the browser.<br />
            2. After authorizing, copy the <code style={{ background: "rgba(255,255,255,0.06)", padding: "1px 5px", borderRadius: 3, fontFamily: "var(--font-mono)", fontSize: 10 }}>code=</code> value from the redirect URL (even if page shows 404).<br />
            3. Paste it below and click <em>Authenticate</em>.
          </div>

          <button
            className="btn-action"
            style={{ display: "flex", alignItems: "center", gap: 7, width: "fit-content" }}
            onClick={fetchLoginUrl}
          >
            <ExternalLink size={12} />
            Open Login Page
          </button>

          {loginUrl && (
            <div style={{ fontSize: 10, color: "var(--text-muted)", wordBreak: "break-all", fontFamily: "var(--font-mono)", padding: "6px 10px", background: "rgba(255,255,255,0.03)", borderRadius: 6, border: "1px solid var(--border)" }}>
              {loginUrl}
            </div>
          )}

          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <input
              type="text"
              className="text-input"
              value={code}
              onChange={e => setCode(e.target.value)}
              onKeyDown={e => e.key === "Enter" && authenticate()}
              placeholder="Paste OAuth code from redirect URL…"
              style={{ flex: 1, fontFamily: "var(--font-mono)", fontSize: 12 }}
              disabled={busy}
            />
            <button
              className="trigger-btn"
              style={{ padding: "8px 18px", fontSize: 12, whiteSpace: "nowrap" }}
              onClick={authenticate}
              disabled={busy || !code.trim()}
            >
              {busy ? "Authenticating…" : "Authenticate"}
            </button>
          </div>
        </>
      )}

      {/* Feedback message */}
      {message && (
        <div style={{
          fontSize: 12, padding: "8px 12px", borderRadius: 6,
          border: `1px solid ${msgType === "success" ? "var(--green)" : "var(--red)"}`,
          background: msgType === "success" ? "rgba(16,185,129,0.08)" : "rgba(239,68,68,0.08)",
          color: msgType === "success" ? "var(--green)" : "var(--red)",
        }}>
          {message}
        </div>
      )}
    </div>
  );
}

const CATEGORIES = ["etfs", "stocks", "mf", "fii_dii", "cot", "fx_rates"];

const SOURCES = [
  { value: "shoonya",  label: "Shoonya",       desc: "Finvasia broker feed — fastest, intraday ticks" },
  { value: "nse",      label: "NSE India",      desc: "NSE official EOD data — most reliable" },
  { value: "yfinance", label: "Yahoo Finance",  desc: "Global fallback — use when others unavailable" },
];

// Categories that need a price source
const PRICE_CATEGORIES = new Set(["etfs", "stocks"]);

export default function DataIngestWorkspace({ onActivity }) {
  const [ingestCategories, setIngestCategories] = useState({
    etfs: true, stocks: false, mf: true, fii_dii: true, cot: true, fx_rates: false,
  });
  const [ingestFullSync, setIngestFullSync] = useState(false);
  const [ingestSource, setIngestSource]     = useState("shoonya");
  const [ingestLogs, setIngestLogs]         = useState("");
  const [isIngestRunning, setIsIngestRunning] = useState(false);

  // Whether source selector is relevant given the current category selection
  const needsSource = Object.entries(ingestCategories).some(
    ([cat, on]) => on && PRICE_CATEGORIES.has(cat)
  );

  const startDataIngest = async () => {
    setIsIngestRunning(true);
    setIngestLogs("Starting ingestion process...\n");
    const categories = Object.keys(ingestCategories).filter(k => ingestCategories[k]).join(",");
    onActivity && onActivity({ isRunning: true, label: "Data Ingestion Running", workspaceOp: "ingest", logs: [`> Categories: ${categories}`] });

    try {
      const res = await fetch("/api/import/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ categories, full: ingestFullSync, source: needsSource ? ingestSource : null }),
      });
      const data = await res.json();
      if (data.status === "success") {
        setIngestLogs(prev => prev + `[Launch Success] PID: ${data.pid}\n\n`);
        pollIngestLogs();
      } else {
        setIngestLogs(prev => prev + `⚠ Import Error: ${data.error}\n`);
        setIsIngestRunning(false);
        onActivity && onActivity({ isRunning: false, label: "Ingest failed", workspaceOp: null, logs: [] });
      }
    } catch (e) {
      setIngestLogs(prev => prev + `⚠ Connection failed: ${e.message}\n`);
      setIsIngestRunning(false);
      onActivity && onActivity({ isRunning: false, label: "Ingest error", workspaceOp: null, logs: [] });
    }
  };

  const pollIngestLogs = () => {
    const timer = setInterval(async () => {
      try {
        const res = await fetch("/api/import/status");
        const data = await res.json();
        if (data.logs) setIngestLogs(data.logs);
        if (!data.running) {
          setIngestLogs(prev => prev + `\n[Process Terminated]`);
          setIsIngestRunning(false);
          clearInterval(timer);
          onActivity && onActivity({ isRunning: false, label: "Ingest complete", workspaceOp: null, logs: [] });
        }
      } catch (_) {
        clearInterval(timer);
        setIsIngestRunning(false);
      }
    }, 1500);
  };

  return (
    <div className="ingest-grid">
      {/* Left: Config */}
      <div className="glass-card desk-card">
        <div className="desk-title">📥 Pipeline Fetchers Configuration</div>
        <div className="desk-subtitle">Select data categories to ingest from live sources into ClickHouse.</div>

        <div className="check-list">
          {CATEGORIES.map(cat => (
            <label className="check-item" key={cat}>
              <input
                type="checkbox"
                checked={ingestCategories[cat]}
                onChange={e => setIngestCategories(prev => ({ ...prev, [cat]: e.target.checked }))}
              />
              {cat.toUpperCase()}
            </label>
          ))}
        </div>

        {/* Source selector — only shown when ETFs or stocks are selected */}
        {needsSource && (
          <div>
            <div style={{ fontSize: 12, fontWeight: 600, color: "var(--text-secondary)", marginBottom: 8, marginTop: 4 }}>
              Price Data Source
              <span style={{ fontSize: 10, color: "var(--text-muted)", fontWeight: 400, marginLeft: 6 }}>
                (applies to ETFs &amp; Stocks)
              </span>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {SOURCES.map(src => (
                <label
                  key={src.value}
                  style={{
                    display: "flex",
                    alignItems: "flex-start",
                    gap: 10,
                    padding: "8px 12px",
                    borderRadius: "var(--radius-sm)",
                    border: `1px solid ${ingestSource === src.value ? "var(--cyan)" : "var(--border)"}`,
                    background: ingestSource === src.value ? "var(--cyan-dim)" : "rgba(255,255,255,0.02)",
                    cursor: "pointer",
                    transition: "all 0.15s",
                  }}
                >
                  <input
                    type="radio"
                    name="ingestSource"
                    value={src.value}
                    checked={ingestSource === src.value}
                    onChange={() => setIngestSource(src.value)}
                    style={{ accentColor: "var(--cyan)", marginTop: 2, flexShrink: 0 }}
                  />
                  <div>
                    <div style={{ fontSize: 12, fontWeight: 600, color: ingestSource === src.value ? "var(--cyan)" : "var(--text-primary)" }}>
                      {src.label}
                    </div>
                    <div style={{ fontSize: 10, color: "var(--text-muted)", marginTop: 1 }}>{src.desc}</div>
                  </div>
                </label>
              ))}
            </div>
          </div>
        )}

        <div>
          <div className="toggle-row">
            <span>Force Complete History Backfill</span>
            <input
              type="checkbox"
              checked={ingestFullSync}
              onChange={e => setIngestFullSync(e.target.checked)}
            />
          </div>
        </div>

        <button
          className="trigger-btn"
          onClick={startDataIngest}
          disabled={isIngestRunning}
        >
          <Terminal size={14} />
          {isIngestRunning ? "Ingestion Running..." : "Launch Ingestion Task"}
        </button>

        {/* Quick SQL audits */}
        <div style={{ marginTop: 8 }}>
          <div className="desk-title" style={{ fontSize: 12, marginBottom: 8 }}>Quick Audit Queries</div>
          {[
            { label: "Latest watermarks",  sql: "SELECT category, max_date FROM market_data.import_watermarks ORDER BY max_date ASC LIMIT 10" },
            { label: "Daily prices count", sql: "SELECT count() FROM market_data.daily_prices FINAL" },
            { label: "FII/DII latest",     sql: "SELECT trade_date, fii_net_cr, dii_net_cr FROM market_data.fii_dii_flows FINAL ORDER BY trade_date DESC LIMIT 5" },
          ].map(q => (
            <button
              key={q.label}
              className="btn-action"
              style={{ width: "100%", justifyContent: "flex-start", marginBottom: 4, fontSize: 11.5 }}
              onClick={async () => {
                setIngestLogs(`> ${q.sql}\n\n`);
                try {
                  const res = await fetch(`/api/query?sql=${encodeURIComponent(q.sql)}`);
                  const data = await res.json();
                  setIngestLogs(`> ${q.sql}\n\n${JSON.stringify(data.slice(0, 5), null, 2)}`);
                } catch (e) {
                  setIngestLogs(`> Error: ${e.message}`);
                }
              }}
            >
              {q.label}
            </button>
          ))}
        </div>
      </div>

      {/* Right: Auth card + Logs */}
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <ShoonyaAuthCard />

        <div className="glass-card desk-card">
          <div className="desk-title">💻 Live Container STDOUT</div>
          <TerminalLog logs={ingestLogs} />
          {isIngestRunning && (
            <div style={{ marginTop: 10, display: "flex", alignItems: "center", gap: 8, fontSize: 12, color: "var(--cyan)" }}>
              <span className="pulse-dot cyan" />
              Ingest process running — polling for updates...
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

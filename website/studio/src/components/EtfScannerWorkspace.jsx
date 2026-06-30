import React, { useState } from "react";
import { Play } from "lucide-react";

const STCG_RATES = { equity: 0.208, commodity: null, debt: null };
const LTCG_RATES = { equity: 0.130, commodity: 0.208, debt: null };
const ROUND_TRIP = 0.10;

function signalClass(signal) {
  if (!signal) return "fair";
  if (signal.includes("HIGH PREMIUM"))  return "high-prem";
  if (signal.includes("MILD PREMIUM"))  return "mild-prem";
  if (signal.includes("GOOD DISCOUNT")) return "good-disc";
  if (signal.includes("MILD DISCOUNT")) return "mild-disc";
  return "fair";
}

function signalColor(signal) {
  if (!signal) return "#94a3b8";
  if (signal.includes("HIGH PREMIUM"))  return "#ef4444";
  if (signal.includes("MILD PREMIUM"))  return "#f59e0b";
  if (signal.includes("GOOD DISCOUNT")) return "#22c55e";
  if (signal.includes("MILD DISCOUNT")) return "#eab308";
  return "#94a3b8";
}

function StatPill({ icon, label, count, color }) {
  return (
    <div className="glass-card" style={{ padding: "10px 14px", flex: 1, textAlign: "center" }}>
      <div style={{ fontSize: 18 }}>{icon}</div>
      <div style={{ fontSize: 20, fontWeight: 700, color: color || "var(--text-primary)", fontFamily: "var(--font-mono)" }}>{count}</div>
      <div style={{ fontSize: 10, color: "var(--text-muted)", marginTop: 2 }}>{label}</div>
    </div>
  );
}

function ZScoreChart({ results }) {
  const actionable = results.filter(r => r.z_score != null);
  if (!actionable.length) return null;
  const maxAbs = Math.max(...actionable.map(r => Math.abs(r.z_score)), 1);

  return (
    <div>
      <div className="desk-title" style={{ fontSize: 12, marginBottom: 8 }}>Z-Score Distribution</div>
      <div className="zscore-chart">
        {actionable.map((r, i) => {
          const pct = Math.abs(r.z_score) / maxAbs * 100;
          const color = signalColor(r.signal);
          return (
            <div key={i} className="zscore-row">
              <div className="zscore-symbol">{r.symbol}</div>
              <div className="zscore-bar-track">
                <div className="zscore-bar-fill" style={{ width: `${pct}%`, background: color }} />
              </div>
              <div className="zscore-value">{r.z_score >= 0 ? "+" : ""}{r.z_score.toFixed(2)}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function PostTaxTable({ results, taxSlab }) {
  const slabRate = taxSlab === "30" ? 0.312 : 0.208;
  const rows = results.filter(r => r.expected_reversion_pct != null);
  if (!rows.length) return null;

  return (
    <div style={{ marginTop: 16 }}>
      <div className="desk-title" style={{ fontSize: 12, marginBottom: 8 }}>📊 Short-Term Trade Viability (Post-Tax)</div>
      <table className="desk-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Direction</th>
            <th>Gross %</th>
            <th>Net STCG %</th>
            <th>Net LTCG %</th>
            <th>Breakeven %</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => {
            const rev     = r.expected_reversion_pct;
            const tcls    = r.tax_class || "equity";
            const stcg    = tcls === "equity" ? slabRate : slabRate; // simplified
            const ltcg    = tcls === "equity" ? 0.130 : 0.208;
            const gross   = Math.abs(rev);
            const netStcg = gross * (1 - stcg) - ROUND_TRIP;
            const netLtcg = gross * (1 - ltcg) - ROUND_TRIP;
            const bkeven  = ROUND_TRIP / (1 - stcg);
            const dir     = rev > 0 ? "BUY (discount)" : "SELL / AVOID (premium)";
            return (
              <tr key={i}>
                <td style={{ fontWeight: 600, color: "var(--text-primary)" }}>{r.symbol}</td>
                <td style={{ color: rev > 0 ? "var(--green)" : "var(--red)" }}>{dir}</td>
                <td>{gross.toFixed(3)}%</td>
                <td style={{ color: netStcg >= 0 ? "var(--green)" : "var(--red)" }}>{netStcg >= 0 ? "+" : ""}{netStcg.toFixed(3)}%</td>
                <td style={{ color: netLtcg >= 0 ? "var(--green)" : "var(--red)" }}>{netLtcg >= 0 ? "+" : ""}{netLtcg.toFixed(3)}%</td>
                <td style={{ color: "var(--text-muted)" }}>{bkeven.toFixed(3)}%</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default function EtfScannerWorkspace({ onActivity }) {
  const [lookback,    setLookback]    = useState(30);
  const [zThreshold,  setZThreshold]  = useState(1.5);
  const [minSnaps,    setMinSnaps]    = useState(3);
  const [customSyms,  setCustomSyms]  = useState("");
  const [taxSlab,     setTaxSlab]     = useState("20");
  const [results,     setResults]     = useState(null);
  const [isRunning,   setIsRunning]   = useState(false);
  const [error,       setError]       = useState("");

  const runScanner = async () => {
    setIsRunning(true); setError(""); setResults(null);
    onActivity && onActivity({ isRunning: true, label: "ETF Scanner Running", workspaceOp: "scan" });
    try {
      const res = await fetch("/api/etf-scanner/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          lookback_days: lookback,
          z_threshold: zThreshold,
          min_snapshots: minSnaps,
          symbols: customSyms.trim()
            ? customSyms.split(",").map(s => s.trim().toUpperCase()).filter(Boolean)
            : [],
          tax_slab: taxSlab,
        }),
      });
      const data = await res.json();
      if (data.status === "success") setResults(data.results);
      else setError(data.error || "Scanner failed");
    } catch (e) {
      setError(e.message);
    }
    setIsRunning(false);
    onActivity && onActivity({ isRunning: false, label: "Scan complete", workspaceOp: null });
  };

  const nHighPrem  = results ? results.filter(r => r.signal?.includes("HIGH PREMIUM")).length  : 0;
  const nMildPrem  = results ? results.filter(r => r.signal?.includes("MILD PREMIUM")).length  : 0;
  const nFair      = results ? results.filter(r => r.signal?.includes("FAIR VALUE")).length    : 0;
  const nMildDisc  = results ? results.filter(r => r.signal?.includes("MILD DISCOUNT")).length : 0;
  const nGoodDisc  = results ? results.filter(r => r.signal?.includes("GOOD DISCOUNT")).length : 0;

  return (
    <div className="scanner-grid">
      {/* ── Config ───────────────────────────────────────────── */}
      <div className="glass-card desk-card scanner-config">
        <div>
          <div className="desk-title">🏦 ETF Premium / Discount Scanner</div>
          <div style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 4, lineHeight: 1.5 }}>
            Computes Z-score of today's premium vs rolling mean — flags ETFs trading unusually expensive or cheap vs iNAV.
          </div>
        </div>

        <div className="scanner-input-group">
          <div className="scanner-label">Lookback Window — <span style={{ color: "var(--cyan)" }}>{lookback} days</span></div>
          <input type="range" min={7} max={90} value={lookback} onChange={e => setLookback(+e.target.value)} className="scanner-range" />
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: "var(--text-muted)" }}>
            <span>7d</span><span>90d</span>
          </div>
        </div>

        <div className="scanner-input-group">
          <div className="scanner-label">Z-Score Threshold — <span style={{ color: "var(--cyan)" }}>{zThreshold.toFixed(2)}</span></div>
          <input type="range" min={0.5} max={3.0} step={0.25} value={zThreshold} onChange={e => setZThreshold(+e.target.value)} className="scanner-range" />
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: "var(--text-muted)" }}>
            <span>0.5</span><span>3.0</span>
          </div>
        </div>

        <div className="scanner-input-group">
          <div className="scanner-label">Min Hourly Buckets Required</div>
          <input
            type="number" min={1} max={50} value={minSnaps}
            onChange={e => setMinSnaps(+e.target.value)}
            className="text-input"
            style={{ width: "100%" }}
          />
        </div>

        <div className="scanner-input-group">
          <div className="scanner-label">Custom Symbols (leave blank for all defaults)</div>
          <input
            type="text"
            placeholder="e.g. GOLDBEES,NIFTYBEES,BANKBEES"
            value={customSyms}
            onChange={e => setCustomSyms(e.target.value)}
            className="text-input"
            style={{ width: "100%" }}
          />
        </div>

        <div className="scanner-input-group">
          <div className="scanner-label">Income Tax Slab (for STCG)</div>
          <div className="scanner-radio-group">
            {[
              { val: "20", label: "20% slab → 20.8% effective" },
              { val: "30", label: "30% slab → 31.2% effective" },
            ].map(opt => (
              <div
                key={opt.val}
                className={`scanner-radio-item ${taxSlab === opt.val ? "active" : ""}`}
                onClick={() => setTaxSlab(opt.val)}
              >
                <input type="radio" readOnly checked={taxSlab === opt.val} style={{ accentColor: "var(--cyan)" }} />
                {opt.label}
              </div>
            ))}
          </div>
        </div>

        <button className="trigger-btn" onClick={runScanner} disabled={isRunning} style={{ marginTop: "auto" }}>
          <Play size={14} />
          {isRunning ? "Scanning…" : "▶ Run Scanner"}
        </button>
      </div>

      {/* ── Results ───────────────────────────────────────────── */}
      <div style={{ overflow: "auto" }}>
        {error && (
          <div className="alert-card red" style={{ marginBottom: 12 }}>⚠ {error}</div>
        )}

        {isRunning && (
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {[1, 2, 3, 4, 5].map(i => (
              <div key={i} className="shimmer" style={{ width: "100%", height: 40, borderRadius: 8 }} />
            ))}
          </div>
        )}

        {!isRunning && results === null && !error && (
          <div className="glass-card" style={{ padding: 40, textAlign: "center", color: "var(--text-muted)" }}>
            <div style={{ fontSize: 32, marginBottom: 10 }}>🏦</div>
            <div style={{ fontSize: 14, fontWeight: 600, color: "var(--text-secondary)", marginBottom: 6 }}>ETF Premium / Discount Scanner</div>
            <div style={{ fontSize: 12 }}>Configure parameters on the left and click Run Scanner to see Z-score signals across all tracked ETFs.</div>
          </div>
        )}

        {results && results.length > 0 && (
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            {/* Stat pills */}
            <div style={{ display: "flex", gap: 8 }}>
              <StatPill icon="🔴" label="High Premium"  count={nHighPrem}  color="var(--red)" />
              <StatPill icon="🟡" label="Mild Premium"  count={nMildPrem}  color="var(--gold)" />
              <StatPill icon="⚪" label="Fair Value"    count={nFair} />
              <StatPill icon="🟡" label="Mild Discount" count={nMildDisc}  color="#eab308" />
              <StatPill icon="🟢" label="Good Discount" count={nGoodDisc}  color="var(--green)" />
            </div>

            {/* Z-score chart */}
            <div className="glass-card desk-card">
              <ZScoreChart results={results} />
            </div>

            {/* Results table */}
            <div className="glass-card desk-card">
              <div className="desk-title" style={{ fontSize: 12, marginBottom: 8 }}>Full Results</div>
              <div style={{ overflowX: "auto" }}>
                <table className="desk-table">
                  <thead>
                    <tr>
                      <th>Symbol</th>
                      <th>Signal</th>
                      <th>Latest (%)</th>
                      <th>{lookback}d Avg (%)</th>
                      <th>Std Dev</th>
                      <th>Z-Score</th>
                      <th>Snapshots</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((r, i) => (
                      <tr key={i}>
                        <td style={{ fontWeight: 700, color: "var(--text-primary)", fontFamily: "var(--font-mono)" }}>{r.symbol}</td>
                        <td>
                          <span className={`signal-badge ${signalClass(r.signal)}`}>
                            {r.signal || "—"}
                          </span>
                        </td>
                        <td style={{ fontFamily: "var(--font-mono)" }}>
                          {r.latest_premium != null ? `${r.latest_premium >= 0 ? "+" : ""}${r.latest_premium.toFixed(3)}%` : "—"}
                        </td>
                        <td style={{ fontFamily: "var(--font-mono)" }}>
                          {r.mean_premium != null ? `${r.mean_premium >= 0 ? "+" : ""}${r.mean_premium.toFixed(3)}%` : "—"}
                        </td>
                        <td style={{ fontFamily: "var(--font-mono)" }}>
                          {r.std_premium != null ? r.std_premium.toFixed(4) : "—"}
                        </td>
                        <td style={{ fontFamily: "var(--font-mono)", fontWeight: 700, color: signalColor(r.signal) }}>
                          {r.z_score != null ? `${r.z_score >= 0 ? "+" : ""}${r.z_score.toFixed(3)}` : "—"}
                        </td>
                        <td style={{ color: "var(--text-muted)" }}>{r.n_snapshots ?? "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <PostTaxTable results={results} taxSlab={taxSlab} />
            </div>
          </div>
        )}

        {results && results.length === 0 && (
          <div className="alert-card amber">
            No results — ensure iNAV snapshots are imported (Import → inav category).
          </div>
        )}
      </div>
    </div>
  );
}

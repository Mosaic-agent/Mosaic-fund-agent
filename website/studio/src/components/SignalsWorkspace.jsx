import React, { useState, useEffect, useCallback } from "react";
import { RefreshCw, Activity, TrendingUp, TrendingDown } from "lucide-react";

const REGIME_CLASS = {
  BUY:         "regime-buy",
  SELL:        "regime-sell",
  HOLD:        "regime-hold",
  WATCH_LONG:  "regime-watch-long",
  WATCH_SHORT: "regime-watch-short",
};

function getRegimeClass(regime) {
  if (!regime) return "regime-hold";
  const key = regime.toUpperCase().replace(/\s+/g, "_");
  return REGIME_CLASS[key] || "regime-hold";
}

function ScoreBar({ score }) {
  const val = parseFloat(score) || 0;
  const pct = Math.min(Math.abs(val), 100);
  const cls = val >= 10 ? "positive" : val <= -10 ? "negative" : "neutral";
  const color = val >= 10 ? "var(--green)" : val <= -10 ? "var(--red)" : "var(--gold)";
  return (
    <div className="score-bar-container">
      <div className="score-bar-track">
        <div className="score-bar-fill" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="score-value" style={{ color }}>{val.toFixed(0)}</span>
    </div>
  );
}

function ShimmerRow() {
  return (
    <tr>
      {[120, 90, 140, 80, 80, 90].map((w, i) => (
        <td key={i} style={{ padding: "12px" }}>
          <div className="shimmer" style={{ width: w, backgroundSize: "200% 100%" }} />
        </td>
      ))}
    </tr>
  );
}

export default function SignalsWorkspace({ onActivity }) {
  const [signals, setSignals] = useState([]);
  const [loading, setLoading] = useState(true);
  const [asOf, setAsOf] = useState("");
  const [sortKey, setSortKey] = useState("composite_score");
  const [sortDir, setSortDir] = useState("desc");
  const [stats, setStats] = useState({ total: 0, bullishPct: 0, avgProbUp: 0 });

  const fetchSignals = useCallback(async () => {
    setLoading(true);
    try {
      const sql = `SELECT etf_symbol, composite_score, regime_signal, prob_up, expected_return_pct, as_of FROM market_data.signal_composite WHERE as_of = (SELECT max(as_of) FROM market_data.signal_composite) ORDER BY composite_score DESC`;
      const res = await fetch(`/api/query?sql=${encodeURIComponent(sql)}`);
      const data = await res.json();
      if (data && data.length > 0 && !data[0].error) {
        setSignals(data);
        setAsOf(data[0].as_of || "");
        const total = data.length;
        const bullish = data.filter(r => ["BUY", "WATCH_LONG"].includes((r.regime_signal || "").toUpperCase())).length;
        const avgProb = data.reduce((acc, r) => acc + (parseFloat(r.prob_up) || 0), 0) / total;
        setStats({
          total,
          bullishPct: total > 0 ? ((bullish / total) * 100).toFixed(0) : 0,
          avgProbUp: (avgProb * 100).toFixed(1),
        });
      }
    } catch (_) {}
    setLoading(false);
  }, []);

  useEffect(() => { fetchSignals(); }, [fetchSignals]);

  const handleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortKey(key); setSortDir("desc"); }
  };

  const sorted = [...signals].sort((a, b) => {
    const av = parseFloat(a[sortKey]) || a[sortKey] || "";
    const bv = parseFloat(b[sortKey]) || b[sortKey] || "";
    if (av < bv) return sortDir === "asc" ? -1 : 1;
    if (av > bv) return sortDir === "asc" ? 1 : -1;
    return 0;
  });

  const runSignalRefresh = async () => {
    onActivity && onActivity({ isRunning: true, label: "Refreshing Signal Composite", workspaceOp: "signals", logs: ["> Running signal composite aggregator..."] });
    await new Promise(r => setTimeout(r, 2200));
    await fetchSignals();
    onActivity && onActivity({ isRunning: false, label: "Signals refreshed", logs: [], workspaceOp: null });
  };

  const SortIcon = ({ col }) => (
    <span style={{ fontSize: 9, marginLeft: 4, color: sortKey === col ? "var(--cyan)" : "var(--text-muted)" }}>
      {sortKey === col ? (sortDir === "desc" ? "▼" : "▲") : "⇅"}
    </span>
  );

  return (
    <div>
      {/* Stat tiles */}
      <div className="stat-tiles-row">
        <div className="stat-tile accent-cyan">
          <div className="stat-label">Total Signals</div>
          <div className="stat-value">{loading ? "—" : stats.total}</div>
          <div className="stat-delta" style={{ color: "var(--text-muted)", fontSize: 11 }}>
            {asOf ? `As of ${asOf}` : "Latest available"}
          </div>
        </div>
        <div className="stat-tile accent-green">
          <div className="stat-label">Bullish Regime %</div>
          <div className="stat-value" style={{ color: "var(--green)" }}>{loading ? "—" : `${stats.bullishPct}%`}</div>
          <div className="stat-delta stat-delta-up">BUY + WATCH_LONG</div>
        </div>
        <div className="stat-tile accent-purple">
          <div className="stat-label">Avg Prob Up</div>
          <div className="stat-value" style={{ color: "var(--purple)" }}>{loading ? "—" : `${stats.avgProbUp}%`}</div>
          <div className="stat-delta" style={{ color: "var(--text-muted)", fontSize: 11 }}>ML model mean</div>
        </div>
      </div>

      {/* Table card */}
      <div className="glass-card" style={{ overflow: "hidden" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "14px 18px", borderBottom: "1px solid var(--border)" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <Activity size={14} color="var(--cyan)" />
            <span style={{ fontWeight: 700, fontSize: 13, color: "var(--text-primary)" }}>ETF Signal Scoreboard</span>
            {asOf && <span style={{ fontSize: 11, color: "var(--text-muted)" }}>— {asOf}</span>}
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            <button className="btn-action" onClick={fetchSignals}>
              <RefreshCw size={11} /> Refresh
            </button>
            <button className="trigger-btn" style={{ padding: "7px 16px", fontSize: 12 }} onClick={runSignalRefresh}>
              ⚡ Run Signal Refresh
            </button>
          </div>
        </div>

        <div style={{ overflowX: "auto", maxHeight: "calc(100vh - 340px)", overflowY: "auto" }}>
          <table className="desk-table" style={{ tableLayout: "fixed" }}>
            <colgroup>
              <col style={{ width: "15%" }} />
              <col style={{ width: "18%" }} />
              <col style={{ width: "20%" }} />
              <col style={{ width: "12%" }} />
              <col style={{ width: "14%" }} />
              <col style={{ width: "12%" }} />
            </colgroup>
            <thead style={{ position: "sticky", top: 0, background: "var(--bg-sidebar)", zIndex: 2 }}>
              <tr>
                <th onClick={() => handleSort("etf_symbol")} style={{ cursor: "pointer" }}>
                  Symbol <SortIcon col="etf_symbol" />
                </th>
                <th onClick={() => handleSort("regime_signal")} style={{ cursor: "pointer" }}>
                  Regime <SortIcon col="regime_signal" />
                </th>
                <th onClick={() => handleSort("composite_score")} style={{ cursor: "pointer" }}>
                  Composite Score <SortIcon col="composite_score" />
                </th>
                <th onClick={() => handleSort("prob_up")} style={{ cursor: "pointer" }}>
                  Prob Up <SortIcon col="prob_up" />
                </th>
                <th onClick={() => handleSort("expected_return_pct")} style={{ cursor: "pointer" }}>
                  Exp Return <SortIcon col="expected_return_pct" />
                </th>
                <th>As Of</th>
              </tr>
            </thead>
            <tbody>
              {loading
                ? Array.from({ length: 8 }).map((_, i) => <ShimmerRow key={i} />)
                : sorted.length === 0
                  ? (
                    <tr>
                      <td colSpan={6}>
                        <div className="empty-state">
                          <div className="empty-state-icon">📊</div>
                          No signal data available. Run the signal composite aggregator to populate.
                        </div>
                      </td>
                    </tr>
                  )
                  : sorted.map((row, idx) => {
                    const probUp = parseFloat(row.prob_up) || 0;
                    const expRet = parseFloat(row.expected_return_pct) || 0;
                    const asOfDate = row.as_of ? String(row.as_of).split("T")[0] : "—";
                    return (
                      <tr key={idx}>
                        <td>
                          <span style={{ fontWeight: 700, color: "var(--text-primary)", fontFamily: "var(--font-mono)", fontSize: 13 }}>
                            {row.etf_symbol}
                          </span>
                        </td>
                        <td>
                          <span className={`regime-badge ${getRegimeClass(row.regime_signal)}`}>
                            {row.regime_signal || "—"}
                          </span>
                        </td>
                        <td>
                          <ScoreBar score={row.composite_score} />
                        </td>
                        <td>
                          <div className="prob-cell" style={{ color: probUp >= 0.5 ? "var(--green)" : "var(--red)" }}>
                            {probUp >= 0.5 ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
                            {(probUp * 100).toFixed(1)}%
                          </div>
                        </td>
                        <td>
                          <span style={{
                            fontFamily: "var(--font-mono)",
                            fontSize: 12.5,
                            fontWeight: 700,
                            color: expRet >= 0 ? "var(--green)" : "var(--red)",
                          }}>
                            {expRet >= 0 ? "+" : ""}{expRet.toFixed(2)}%
                          </span>
                        </td>
                        <td style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-muted)" }}>
                          {asOfDate}
                        </td>
                      </tr>
                    );
                  })
              }
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

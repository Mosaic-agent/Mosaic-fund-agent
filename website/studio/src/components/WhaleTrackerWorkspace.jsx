import React, { useState } from "react";
import { Eye } from "lucide-react";

const FUNDS = [
  { value: "DSP_MULTI_ASSET",       label: "DSP Multi Asset Allocation (152056)" },
  { value: "DSP_SMALL_CAP",         label: "DSP Small Cap Direct Growth (119212)" },
  { value: "DSP_MID_CAP",           label: "DSP Mid Cap Direct Growth (119071)" },
  { value: "DSP_FLEXI_CAP",         label: "DSP Flexi Cap Direct Growth (119076)" },
  { value: "DSP_LARGE_AND_MID_CAP", label: "DSP Large & Mid Cap (119218)" },
  { value: "DSP_TIGER",             label: "DSP TIGER Fund (119247)" },
];

export default function WhaleTrackerWorkspace({ onActivity }) {
  const [selectedFund, setSelectedFund] = useState("DSP_MULTI_ASSET");
  const [holdings, setHoldings] = useState([]);
  const [loading, setLoading] = useState(false);
  const [asOfMonth, setAsOfMonth] = useState("");

  const fetchHoldings = async () => {
    setLoading(true);
    setHoldings([]);
    onActivity && onActivity({ isRunning: true, label: "Fetching DSP Fund Disclosures", workspaceOp: "whale", logs: [`> ${selectedFund}`] });

    const sql = `
      SELECT security_name, pct_of_nav, market_value_cr, asset_type, as_of_month
      FROM market_data.mf_holdings FINAL
      WHERE fund_name = '${selectedFund}'
        AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE fund_name = '${selectedFund}')
      ORDER BY pct_of_nav DESC
      LIMIT 15
    `;

    try {
      const res = await fetch(`/api/query?sql=${encodeURIComponent(sql)}`);
      const data = await res.json();
      if (data && data.length > 0 && !data[0].error) {
        setHoldings(data);
        setAsOfMonth(data[0].as_of_month || "");
      }
    } catch (_) {}

    setLoading(false);
    onActivity && onActivity({ isRunning: false, label: "Holdings loaded", workspaceOp: null, logs: [] });
  };

  const totalNav = holdings.reduce((acc, h) => acc + (parseFloat(h.pct_of_nav) || 0), 0);

  return (
    <div className="glass-card desk-card" style={{ marginBottom: 0 }}>
      <div className="desk-title">📦 Cross-Fund Institutional Disclosures Explorer</div>
      <div className="desk-subtitle">Query latest DSP AMC fund portfolio disclosures from ClickHouse (mf_holdings).</div>

      <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
        <select
          className="text-input"
          value={selectedFund}
          onChange={e => setSelectedFund(e.target.value)}
          style={{ flex: 1, minWidth: 260 }}
        >
          {FUNDS.map(f => (
            <option key={f.value} value={f.value}>{f.label}</option>
          ))}
        </select>
        <button className="trigger-btn" onClick={fetchHoldings} disabled={loading}>
          <Eye size={14} />
          {loading ? "Loading..." : "Track Holdings"}
        </button>
      </div>

      {asOfMonth && (
        <div style={{ fontSize: 11.5, color: "var(--text-muted)" }}>
          Latest disclosure: <span style={{ color: "var(--cyan)", fontFamily: "var(--font-mono)" }}>{asOfMonth}</span>
          &nbsp;·&nbsp;Showing top {holdings.length} holdings&nbsp;·&nbsp;
          Top-15 NAV coverage: <span style={{ color: "var(--gold)" }}>{totalNav.toFixed(1)}%</span>
        </div>
      )}

      <div>
        {holdings.length === 0 && !loading ? (
          <div className="empty-state">
            <div className="empty-state-icon">📦</div>
            Select a fund and click "Track Holdings" to retrieve the latest ClickHouse disclosures.
          </div>
        ) : (
          <table className="desk-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Security Name</th>
                <th>Asset Type</th>
                <th>% of NAV</th>
                <th>Market Value (Cr)</th>
              </tr>
            </thead>
            <tbody>
              {loading
                ? Array.from({ length: 8 }).map((_, i) => (
                  <tr key={i}>
                    {[30, 200, 90, 70, 100].map((w, j) => (
                      <td key={j} style={{ padding: "12px" }}>
                        <div className="shimmer" style={{ width: w, height: 14, backgroundSize: "200% 100%", borderRadius: 4 }} />
                      </td>
                    ))}
                  </tr>
                ))
                : holdings.map((h, idx) => (
                  <tr key={idx}>
                    <td style={{ color: "var(--text-muted)", fontFamily: "var(--font-mono)", fontSize: 11 }}>{idx + 1}</td>
                    <td style={{ fontWeight: 700, color: "var(--text-primary)" }}>{h.security_name}</td>
                    <td>
                      <span style={{
                        background: "var(--blue-dim)", color: "var(--blue)",
                        padding: "2px 8px", borderRadius: 20, fontSize: 10.5, fontWeight: 700
                      }}>
                        {h.asset_type || "Equity"}
                      </span>
                    </td>
                    <td>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <div style={{
                          width: 48, height: 4, background: "rgba(255,255,255,0.06)",
                          borderRadius: 4, overflow: "hidden"
                        }}>
                          <div style={{
                            width: `${Math.min((parseFloat(h.pct_of_nav) / 15) * 100, 100)}%`,
                            height: "100%", background: "var(--cyan)", borderRadius: 4
                          }} />
                        </div>
                        <span style={{ fontWeight: 700, color: "var(--cyan)", fontFamily: "var(--font-mono)", fontSize: 13 }}>
                          {parseFloat(h.pct_of_nav).toFixed(2)}%
                        </span>
                      </div>
                    </td>
                    <td style={{ fontFamily: "var(--font-mono)", fontSize: 12.5 }}>
                      ₹{parseFloat(h.market_value_cr).toFixed(2)} Cr
                    </td>
                  </tr>
                ))
              }
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

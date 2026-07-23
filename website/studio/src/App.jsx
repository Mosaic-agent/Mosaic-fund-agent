import React, { useState, useEffect, useCallback } from "react";
import {
  Compass, Activity, ShieldAlert, Search,
  BarChart2, Database, TrendingUp, Zap, RefreshCw, MessageSquare
} from "lucide-react";
import DashboardWorkspace from "./components/DashboardWorkspace";
import SignalsWorkspace from "./components/SignalsWorkspace";
import DataIngestWorkspace from "./components/DataIngestWorkspace";
import WhaleTrackerWorkspace from "./components/WhaleTrackerWorkspace";
import AnomalyScanWorkspace from "./components/AnomalyScanWorkspace";
import DilutionWorkspace from "./components/DilutionWorkspace";
import BacktestWorkspace from "./components/BacktestWorkspace";
import ChatWorkspace from "./components/ChatWorkspace";
import ExplorerWorkspace from "./components/ExplorerWorkspace";
import EtfScannerWorkspace from "./components/EtfScannerWorkspace";

const NAVIGATION = [
  { key: "dashboard",     label: "Overview",       icon: Compass,     preset: "gold_usd" },
  { key: "signals",       label: "ETF Signals",    icon: Activity },
  { key: "explorer",      label: "Market Explorer",icon: BarChart2 },
  { key: "etf_scanner",   label: "ETF Monitor",    icon: Zap },
  { key: "whale_tracker", label: "Fund Holdings",  icon: TrendingUp },
  { key: "research",      label: "Research",       icon: Search },
  { key: "screener",      label: "Dilution Review",icon: ShieldAlert },
  { key: "data_ingest",   label: "Data Operations",icon: Database },
  { key: "chat",          label: "Research Chat",  icon: MessageSquare },
];

const WORKSPACE_TITLES = {
  dashboard:     "Market overview",
  markets:       "India macro",
  signals:       "ETF signals",
  screener:      "Dilution review",
  research:      "Research",
  strategies:    "Strategy backtest",
  agents:        "Nifty momentum",
  data_explorer: "Data operations",
  gold_usd:      "Gold and USD",
  india_macro:   "India macro",
  nifty_momentum:"Nifty momentum",
  whale_tracker: "Fund holdings",
  anomaly_scan:  "Anomaly review",
  data_ingest:   "Data operations",
};

const DASHBOARD_PRESETS = {
  dashboard:    "gold_usd",
  markets:      "india_macro",
  agents:       "nifty_momentum",
  gold_usd:     "gold_usd",
  india_macro:  "india_macro",
  nifty_momentum: "nifty_momentum",
};

const DASHBOARD_OPTIONS = [
  { key: "gold_usd", label: "Gold and USD", preset: "gold_usd" },
  { key: "india_macro", label: "India macro", preset: "india_macro" },
  { key: "nifty_momentum", label: "Nifty momentum", preset: "nifty_momentum" },
];

export default function App() {
  const [currentWorkspace, setCurrentWorkspace] = useState("dashboard");
  const [clock, setClock] = useState("");
  const [agentActivity, setAgentActivity] = useState({
    isRunning: false,
    label: "Idle — Ready",
    logs: [],
    phase: "",
    workspaceOp: null,
  });

  // Live clock
  useEffect(() => {
    const update = () => {
      const now = new Date();
      setClock(now.toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }));
    };
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, []);

  const reportActivity = useCallback((update) => {
    setAgentActivity(prev => ({ ...prev, ...update }));
  }, []);

  const isDashboard = (ws) => DASHBOARD_PRESETS[ws] !== undefined;

  const renderWorkspace = () => {
    const preset = DASHBOARD_PRESETS[currentWorkspace];
    if (preset) {
      return <DashboardWorkspace key={preset} preset={preset} onActivity={reportActivity} />;
    }
    switch (currentWorkspace) {
      case "signals":       return <SignalsWorkspace onActivity={reportActivity} />;
      case "chat":          return <ChatWorkspace onActivity={reportActivity} />;
      case "explorer":      return <ExplorerWorkspace onActivity={reportActivity} />;
      case "etf_scanner":   return <EtfScannerWorkspace onActivity={reportActivity} />;
      case "screener":      return <DilutionWorkspace onActivity={reportActivity} />;
      case "research":      return <AnomalyScanWorkspace onActivity={reportActivity} />;
      case "strategies":    return <BacktestWorkspace onActivity={reportActivity} />;
      case "data_explorer": return <DataIngestWorkspace onActivity={reportActivity} />;
      case "whale_tracker": return <WhaleTrackerWorkspace onActivity={reportActivity} />;
      case "anomaly_scan":  return <AnomalyScanWorkspace onActivity={reportActivity} />;
      case "data_ingest":   return <DataIngestWorkspace onActivity={reportActivity} />;
      default:              return <DashboardWorkspace preset="gold_usd" onActivity={reportActivity} />;
    }
  };

  const NavItem = ({ item }) => {
    const Icon = item.icon;
    const active = currentWorkspace === item.key;
    return (
      <div
        className={`nav-item ${active ? "active" : ""}`}
        onClick={() => setCurrentWorkspace(item.key)}
      >
        {Icon && <Icon size={13} />}
        {item.label}
      </div>
    );
  };

  const showDashSelect = isDashboard(currentWorkspace);
  const currentPreset = DASHBOARD_PRESETS[currentWorkspace];

  return (
    <div className="app-container">
      {/* ── LEFT SIDEBAR ──────────────────────────────────────── */}
      <aside className="sidebar">
        <div className="brand">
          <img
            className="brand-logo"
            src="https://mosaic-agent.github.io/Mosaic-fund-agent/logo.png"
            alt="Mosaic"
          />
          <div className="brand-title">Mosaic<span>Research</span></div>
        </div>

        <div className="nav-section" style={{ flex: 1 }}>
          <div className="nav-header">Research desk</div>
          {NAVIGATION.map(item => <NavItem key={item.key} item={item} />)}
        </div>

        <div className="sidebar-status">
          <span className="sidebar-status-dot" />
          Data service connected
        </div>
      </aside>

      {/* ── MAIN WORKSPACE ────────────────────────────────────── */}
      <main className="main-workspace">
        <header className="top-header">
          <div className="header-left">
            <span className="workspace-title">{WORKSPACE_TITLES[currentWorkspace] || "Mosaic Studio"}</span>
            {showDashSelect && (
              <select
                className="workspace-select"
                value={currentPreset}
                onChange={e => {
                  const found = DASHBOARD_OPTIONS.find(w => w.preset === e.target.value);
                  if (found) setCurrentWorkspace(found.key);
                  else setCurrentWorkspace(e.target.value);
                }}
              >
                <option value="gold_usd">Gold vs USD</option>
                <option value="india_macro">India Macro</option>
                <option value="nifty_momentum">Nifty Momentum</option>
              </select>
            )}
          </div>
          <div className="header-right">
            <span className={`run-status ${agentActivity.isRunning ? "working" : ""}`}>
              <span />
              {agentActivity.isRunning ? agentActivity.label : "Data service connected"}
            </span>
            <span className="header-clock">{clock}</span>
            <button
              className="btn-action"
              onClick={() => reportActivity({ isRunning: false, label: "Idle — Ready", logs: [], workspaceOp: null })}
            >
              <RefreshCw size={12} /> Refresh
            </button>
            <button
              className="btn-action btn-primary"
              onClick={() => setCurrentWorkspace("strategies")}
            >
              Run analysis
            </button>
          </div>
        </header>

        <div className="workspace-content fade-in">
          {renderWorkspace()}
        </div>
      </main>

    </div>
  );
}

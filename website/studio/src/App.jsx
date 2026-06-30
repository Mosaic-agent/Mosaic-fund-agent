import React, { useState, useEffect, useCallback } from "react";
import {
  Compass, Layers, Activity, ShieldAlert, Search, Cpu,
  BarChart2, Database, TrendingUp, Zap, RefreshCw, Settings, MessageSquare
} from "lucide-react";
import AgentPanel from "./components/AgentPanel";
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

const NAV_PLATFORM = [
  { key: "dashboard",     label: "Dashboard",     icon: Compass,       preset: "gold_usd" },
  { key: "markets",       label: "Markets",       icon: Layers,        preset: "india_macro" },
  { key: "signals",       label: "Signals",       icon: Activity },
  { key: "explorer",      label: "Explorer",      icon: BarChart2 },
  { key: "etf_scanner",   label: "ETF Scanner",   icon: Zap },
  { key: "chat",          label: "AI Chat",       icon: MessageSquare },
  { key: "screener",      label: "Screener",      icon: ShieldAlert },
  { key: "research",      label: "Research",      icon: Search },
  { key: "strategies",    label: "Strategies",    icon: TrendingUp },
  { key: "agents",        label: "Agents",        icon: Cpu,           preset: "nifty_momentum" },
  { key: "data_explorer", label: "Data Explorer", icon: Database },
];

const NAV_WORKSPACES = [
  { key: "gold_usd",        label: "⚡ Gold vs USD",     preset: "gold_usd" },
  { key: "india_macro",     label: "🇮🇳 India Macro",     preset: "india_macro" },
  { key: "nifty_momentum",  label: "📈 Nifty Momentum",  preset: "nifty_momentum" },
];

const NAV_SYSTEM = [
  { key: "whale_tracker", label: "📦 Whale Tracker" },
  { key: "anomaly_scan",  label: "🔬 Anomaly Scan" },
  { key: "data_ingest",   label: "📥 Data Ingest" },
];

const WORKSPACE_TITLES = {
  dashboard:     "Gold vs USD Analysis",
  markets:       "India Macro Dashboard",
  signals:       "ETF Signal Scoreboard",
  screener:      "Promoter Dilution Screener",
  research:      "Volatility Research",
  strategies:    "Strategy Backtester",
  agents:        "Nifty Momentum",
  data_explorer: "Data Explorer",
  gold_usd:      "Gold vs USD Analysis",
  india_macro:   "India Macro Dashboard",
  nifty_momentum:"Nifty Momentum Tracker",
  whale_tracker: "Institutional Whale Tracker",
  anomaly_scan:  "Anomaly Scanner",
  data_ingest:   "Data Ingest Pipeline",
};

const DASHBOARD_PRESETS = {
  dashboard:    "gold_usd",
  markets:      "india_macro",
  agents:       "nifty_momentum",
  gold_usd:     "gold_usd",
  india_macro:  "india_macro",
  nifty_momentum: "nifty_momentum",
};

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

  const NavItem = ({ item, isWorkspace = false }) => {
    const Icon = item.icon;
    const active = currentWorkspace === item.key;
    return (
      <div
        className={`nav-item ${isWorkspace ? "nav-workspace-item" : ""} ${active ? "active" : ""}`}
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
          <div className="brand-title">MOSAIC<span>Studio</span></div>
        </div>

        <div className="nav-section">
          <div className="nav-header">Platform</div>
          {NAV_PLATFORM.map(item => <NavItem key={item.key} item={item} />)}
        </div>

        <div className="nav-section">
          <div className="nav-header">Workspaces</div>
          {NAV_WORKSPACES.map(item => <NavItem key={item.key} item={item} isWorkspace />)}
        </div>

        <div className="nav-section" style={{ flex: 1 }}>
          <div className="nav-header">System Desk</div>
          {NAV_SYSTEM.map(item => <NavItem key={item.key} item={item} isWorkspace />)}
        </div>

        <div className="sidebar-status">
          <span className="sidebar-status-dot" />
          ClickHouse Connected
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
                  const found = NAV_WORKSPACES.find(w => w.preset === e.target.value);
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
              New Analysis
            </button>
          </div>
        </header>

        <div className="workspace-content fade-in">
          {renderWorkspace()}
        </div>
      </main>

      {/* ── RIGHT PANEL ───────────────────────────────────────── */}
      <aside className="right-panel">
        <AgentPanel activity={agentActivity} />
      </aside>
    </div>
  );
}

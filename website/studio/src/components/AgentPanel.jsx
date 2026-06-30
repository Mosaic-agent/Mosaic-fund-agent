import React, { useState, useEffect, useRef } from "react";
import { Cpu } from "lucide-react";

const STEPS = [
  "Planner Orchestrator",
  "Fetch Market Data",
  "Data Preprocessing",
  "Compute Indicators",
  "ML Prediction Target",
  "GARCH Volatility",
  "Build Narrative",
];

const EXEC_PHASES = [
  "User Query",
  "Planner",
  "Data Collection",
  "Analysis",
  "ML Pipeline",
  "Insight Generation",
];

function initTasks() {
  return STEPS.map(label => ({ label, status: "pending", progress: 0 }));
}

export default function AgentPanel({ activity }) {
  const [tasks, setTasks] = useState(initTasks());
  const [traceLogs, setTraceLogs] = useState([]);
  const [expandedPhase, setExpandedPhase] = useState(null);
  const [ingestLogs, setIngestLogs] = useState("");
  const [panelStats, setPanelStats] = useState({ close: null, regime: null, fiiNet: null });

  const logEndRef = useRef(null);
  const animRef = useRef(null);
  const pollRef = useRef(null);

  // Animate tasks when isRunning changes to true
  useEffect(() => {
    if (!activity.isRunning) {
      if (animRef.current) clearTimeout(animRef.current);
      return;
    }

    setTasks(initTasks());
    setTraceLogs([]);

    let step = 0;

    const advance = () => {
      if (step >= STEPS.length) return;

      setTasks(prev => {
        const copy = prev.map((t, i) => {
          if (i === step) return { ...t, status: "running", progress: 60 };
          if (i < step) return { ...t, status: "done", progress: 100 };
          return t;
        });
        return copy;
      });

      const ts = new Date().toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
      setTraceLogs(prev => [...prev, { text: `[${ts}] Executing: ${STEPS[step]}`, active: true }]);

      animRef.current = setTimeout(() => {
        setTasks(prev => {
          const copy = [...prev];
          copy[step] = { ...copy[step], status: "done", progress: 100 };
          return copy;
        });
        setTraceLogs(prev => prev.map((l, i) => i === prev.length - 1 ? { ...l, active: false } : l));
        step++;
        animRef.current = setTimeout(advance, 600);
      }, 900);
    };

    advance();
    return () => { if (animRef.current) clearTimeout(animRef.current); };
  }, [activity.isRunning]);

  // Append external logs from activity
  useEffect(() => {
    if (activity.logs && activity.logs.length > 0) {
      setTraceLogs(prev => [
        ...prev,
        ...activity.logs.map(l => ({ text: l, active: false }))
      ]);
    }
  }, [activity.logs]);

  // Auto-scroll trace
  useEffect(() => {
    if (logEndRef.current) {
      logEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [traceLogs]);

  // Poll /api/import/status when op === 'ingest'
  useEffect(() => {
    if (activity.workspaceOp === "ingest" && activity.isRunning) {
      pollRef.current = setInterval(async () => {
        try {
          const res = await fetch("/api/import/status");
          const data = await res.json();
          if (data.logs) setIngestLogs(data.logs);
          if (!data.running) clearInterval(pollRef.current);
        } catch (_) {}
      }, 1500);
      return () => clearInterval(pollRef.current);
    } else {
      if (pollRef.current) clearInterval(pollRef.current);
    }
  }, [activity.workspaceOp, activity.isRunning]);

  // Fetch panel stats on mount
  useEffect(() => {
    const fetchStats = async () => {
      try {
        const [rClose, rRegime, rFii] = await Promise.allSettled([
          fetch(`/api/query?sql=${encodeURIComponent("SELECT close FROM market_data.daily_prices FINAL WHERE symbol='GOLDBEES' ORDER BY trade_date DESC LIMIT 1")}`).then(r => r.json()),
          fetch(`/api/query?sql=${encodeURIComponent("SELECT regime_signal FROM market_data.ml_predictions FINAL ORDER BY as_of DESC LIMIT 1")}`).then(r => r.json()),
          fetch(`/api/query?sql=${encodeURIComponent("SELECT fii_net_cr FROM market_data.fii_dii_flows FINAL ORDER BY trade_date DESC LIMIT 1")}`).then(r => r.json()),
        ]);
        setPanelStats({
          close:   rClose.status   === "fulfilled" && rClose.value?.length   ? rClose.value[0].close          : null,
          regime:  rRegime.status  === "fulfilled" && rRegime.value?.length  ? rRegime.value[0].regime_signal  : null,
          fiiNet:  rFii.status     === "fulfilled" && rFii.value?.length     ? rFii.value[0].fii_net_cr        : null,
        });
      } catch (_) {}
    };
    fetchStats();
  }, []);

  const isRunning = activity.isRunning;
  const activePhaseIdx = isRunning ? Math.floor((tasks.filter(t => t.status === "done").length / STEPS.length) * EXEC_PHASES.length) : -1;

  return (
    <div className="agent-panel">
      {/* Header */}
      <div className="agent-header">
        <div className="agent-header-row">
          <div className="agent-title-text">
            <Cpu size={12} />
            MOSAIC AGENT
          </div>
          <span className="agent-badge">v2</span>
        </div>
        <div className={`agent-status ${isRunning ? "running" : ""}`}>
          <span className={`pulse-dot ${isRunning ? "purple" : "idle"}`} />
          {isRunning ? (activity.label || "Running Pipeline...") : "Idle — Ready"}
        </div>
      </div>

      <div className="agent-body">
        {/* Task checklist */}
        <div>
          <div className="trace-section-header">Execution Steps</div>
          <div className="agent-tasks">
            {tasks.map((task, idx) => (
              <div key={idx} className={`agent-task ${task.status}`}>
                <div className="agent-task-row">
                  <span className="agent-task-label">{task.label}</span>
                  <span className={`task-badge ${task.status}`}>
                    {task.status === "done" ? "✓ DONE" : task.status === "running" ? "⚡ RUN" : "PENDING"}
                  </span>
                </div>
                <div className="task-progress-bar">
                  <div
                    className="task-progress-fill"
                    style={{ width: `${task.progress}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Trace logs */}
        <div>
          <div className="trace-section-header">Trace Output</div>
          <div className="trace-log-box">
            {traceLogs.length === 0 ? (
              <div style={{ color: "var(--text-muted)", fontSize: "10.5px", paddingTop: 30, textAlign: "center" }}>
                Traces appear here during pipeline run.
              </div>
            ) : (
              traceLogs.map((log, i) => (
                <div key={i} className={`trace-row ${log.active ? "active" : ""}`}>{log.text}</div>
              ))
            )}
            <div ref={logEndRef} />
          </div>
          {ingestLogs && activity.workspaceOp === "ingest" && (
            <div className="trace-log-box" style={{ marginTop: 6, height: 80 }}>
              <div className="trace-row">{ingestLogs.slice(-600)}</div>
            </div>
          )}
        </div>

        {/* Execution trace */}
        <div>
          <div className="trace-section-header">Execution Trace</div>
          <div className="exec-trace-section">
            {EXEC_PHASES.map((phase, idx) => {
              const status = idx < activePhaseIdx ? "done" : idx === activePhaseIdx ? "running" : "pending";
              const expanded = expandedPhase === idx;
              return (
                <div key={idx}>
                  <div
                    className={`exec-trace-row ${status === "running" ? "active" : ""}`}
                    onClick={() => setExpandedPhase(expanded ? null : idx)}
                  >
                    <div className={`exec-trace-icon ${status}`}>
                      {status === "done" ? "✓" : status === "running" ? "⚡" : "○"}
                    </div>
                    <span className="exec-trace-label">{phase}</span>
                    <span style={{ fontSize: 10, color: "var(--text-muted)" }}>{expanded ? "▲" : "▼"}</span>
                  </div>
                  {expanded && (
                    <div style={{ padding: "6px 10px 6px 34px", fontSize: 11, color: "var(--text-muted)", fontFamily: "var(--font-mono)", borderBottom: "1px solid var(--border)" }}>
                      {status === "done" ? `Completed successfully` : status === "running" ? `Processing...` : "Queued"}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>

        {/* Panel Stats (Morning Brief mini) */}
        <div>
          <div className="trace-section-header">Market Snapshot</div>
          <div className="panel-stats">
            <div className="panel-stat-tile">
              <span className="panel-stat-label">GOLDBEES Close</span>
              <span className="panel-stat-value">{panelStats.close != null ? `₹${parseFloat(panelStats.close).toFixed(2)}` : "—"}</span>
            </div>
            <div className="panel-stat-tile">
              <span className="panel-stat-label">ML Regime</span>
              <span className="panel-stat-value" style={{ fontSize: 11, color: "var(--cyan)" }}>
                {panelStats.regime || "—"}
              </span>
            </div>
            <div className="panel-stat-tile">
              <span className="panel-stat-label">FII Net (Cr)</span>
              <span
                className="panel-stat-value"
                style={{ color: panelStats.fiiNet > 0 ? "var(--green)" : panelStats.fiiNet < 0 ? "var(--red)" : "var(--text-primary)" }}
              >
                {panelStats.fiiNet != null ? `₹${parseFloat(panelStats.fiiNet).toFixed(0)}` : "—"}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

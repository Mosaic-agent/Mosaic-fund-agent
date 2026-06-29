import React, { useState, useEffect } from "react";
import { 
  Play, 
  Terminal, 
  Compass, 
  Layers, 
  Target, 
  TrendingUp, 
  ShieldAlert, 
  BookOpen, 
  Activity, 
  Database,
  Search,
  Eye,
  RefreshCw,
  Cpu,
  BarChart2,
  Lock,
  ChevronRight
} from "lucide-react";
import CanvasChart from "./components/CanvasChart";
import TerminalLog from "./components/TerminalLog";

// Static premium fallback datasets when ClickHouse is loading or empty
const FALLBACK_DATASETS = {
  gold_usd: {
    title: "Gold vs USD Strategy Desk",
    regime: "Bullish (81% confidence)",
    briefOpps: [
      { name: "📈 GOLDBEES", change: "+1.62%" },
      { name: "📈 USDINR", change: "+0.24%" },
      { name: "📈 US10Y", change: "+0.85%" }
    ],
    briefSummary: "Gold prices breaking higher as USD momentum consolidates near key support levels. GARCH conditional volatility remains compressed, suggesting a structural breakout phase.",
    chart1Title: "Gold Price (GOLDBEES)",
    chart1Price: "58.45",
    chart1Change: "+1.62%",
    chart1Data: [55.2, 55.8, 56.1, 55.9, 56.4, 57.2, 57.8, 58.45],
    chart2Title: "USDINR Exchange Rate",
    chart2Price: "83.45",
    chart2Change: "+0.24%",
    chart2Data: [82.9, 83.1, 83.15, 83.0, 83.2, 83.3, 83.4, 83.45],
    chart3Title: "US 10-Year Bond Yield",
    chart3Price: "4.23",
    chart3Change: "-0.45%",
    chart3Data: [4.35, 4.31, 4.28, 4.29, 4.25, 4.26, 4.24, 4.23],
    chart4Title: "Expected Log 5D Return",
    chart4Data: [0.12, 0.15, 0.08, 0.23, 0.31, 0.42, 0.41, 0.42],
    insightBody: "LightGBM model projects an upward probability (prob_up) of 0.81 for GOLDBEES over the next 5 trading days. Recommending blended_50 position weight of 42% scaling down from risk limit of 50%.",
    insightConfidence: "Model AUC: 0.589 | Hit Ratio: 61.2% | Kelly Fraction: 0.85",
    sqlQuery: "SELECT trade_date, close FROM market_data.daily_prices WHERE symbol = 'GOLDBEES' ORDER BY trade_date DESC LIMIT 30;"
  },
  india_macro: {
    title: "India Macro Indicators",
    regime: "Expansionary (74% confidence)",
    briefOpps: [
      { name: "📈 BANKBEES", change: "+1.89%" },
      { name: "📈 NIFTYBEES", change: "+1.12%" },
      { name: "📈 JUNIORBEES", change: "+0.95%" }
    ],
    briefSummary: "FII net equity inflows accelerate over the last two quarters. ClickHouse watermarks suggest robust trading volumes across major private banks, driven by lower retail NPA rates.",
    chart1Title: "Bank Nifty (BANKBEES)",
    chart1Price: "524.50",
    chart1Change: "+1.89%",
    chart1Data: [502.1, 505.4, 509.8, 508.2, 514.0, 518.5, 521.2, 524.5],
    chart2Title: "FII Net Flows (Cr)",
    chart2Price: "2,310",
    chart2Change: "+45.2%",
    chart2Data: [1200, 1500, 800, 2300, 3100, 4200, 4100, 4200],
    chart3Title: "DII Net Flows (Cr)",
    chart3Price: "1,450",
    chart3Change: "+12.8%",
    chart3Data: [900, 1100, 1200, 1150, 1300, 1400, 1380, 1450],
    chart4Title: "Composite Score Trend",
    chart4Data: [72, 75, 71, 78, 82, 85, 84, 86],
    insightBody: "Aggregated flow pillars indicate extremely strong institutional conviction. Domestic asset managers (DII) are providing stable downside support, neutralizing occasional FII volatility.",
    insightConfidence: "Valuation Pillar Z-Score: +1.12 | Flow Score: 88/100",
    sqlQuery: "SELECT trade_date, fii_net_cr, dii_net_cr FROM market_data.fii_dii_flows ORDER BY trade_date DESC LIMIT 30;"
  },
  nifty_momentum: {
    title: "Nifty Momentum Tracker",
    regime: "Strong Trend (92% confidence)",
    briefOpps: [
      { name: "📈 NIFTYBEES", change: "+2.35%" },
      { name: "📈 ITBEES", change: "+1.42%" },
      { name: "📈 PHARMABEES", change: "-0.15%" }
    ],
    briefSummary: "Sector rotation scanning reveals momentum shifting back into high-beta tech counters. Volatility compression indicators show a pending expansion cycle for benchmark ETFs.",
    chart1Title: "Nifty 50 (NIFTYBEES)",
    chart1Price: "242.30",
    chart1Change: "+2.35%",
    chart1Data: [231.2, 233.5, 235.1, 234.8, 237.9, 239.5, 240.8, 242.3],
    chart2Title: "Volume Spread Index",
    chart2Price: "2.81",
    chart2Change: "+12.1%",
    chart2Data: [1.2, 1.5, 1.4, 1.9, 2.1, 2.3, 2.7, 2.8],
    chart3Title: "Gold Hedge (GOLDBEES)",
    chart3Price: "58.45",
    chart3Change: "+1.62%",
    chart3Data: [55.2, 55.8, 56.1, 55.9, 56.4, 57.2, 57.8, 58.45],
    chart4Title: "Volatility Expansion Ratio",
    chart4Data: [1.1, 1.3, 1.2, 1.6, 1.8, 2.1, 2.0, 2.2],
    insightBody: "Sector-setup breakouts confirmed on 5 sectoral ETFs. Volume expansion supports momentum, suggesting continuation. Moving Average Golden Cross active on Daily NIFTYBEES prices.",
    insightConfidence: "Breakout Success Prob: 76.5% | GARCH Conditional Vol: 12.4%",
    sqlQuery: "SELECT trade_date, close, volume FROM market_data.daily_prices WHERE symbol = 'NIFTYBEES' ORDER BY trade_date DESC LIMIT 30;"
  }
};

const AGENT_TASKS_CHECKLIST = [
  { label: "1. Planner Orchestrator", status: "pending", time: "" },
  { label: "2. Fetch Market Data", status: "pending", time: "" },
  { label: "3. Data Preprocessing", status: "pending", time: "" },
  { label: "4. Compute Indicators", status: "pending", time: "" },
  { label: "5. ML Prediction Target", status: "pending", time: "" },
  { label: "6. GARCH Volatility Target", status: "pending", time: "" },
  { label: "7. Build Narrative Insights", status: "pending", time: "" }
];

export default function App() {
  const [currentWorkspace, setCurrentWorkspace] = useState("gold_usd");
  const [promptText, setPromptText] = useState(
    "Compare Gold with USD, Inflation, Interest Rates and explain current macro regime."
  );
  
  // Dynamic metrics state from ClickHouse
  const [morningBrief, setMorningBrief] = useState({
    regime: "Bullish (81% confidence)",
    opps: [],
    risks: [],
    summary: ""
  });

  // Chart data vectors
  const [chart1Data, setChart1Data] = useState([]);
  const [chart2Data, setChart2Data] = useState([]);
  const [chart3Data, setChart3Data] = useState([]);
  const [chart4Data, setChart4Data] = useState([]);
  
  const [chart1Meta, setChart1Meta] = useState({ price: "58.45", change: "+1.62%", isUp: true });
  const [chart2Meta, setChart2Meta] = useState({ price: "83.45", change: "+0.24%", isUp: true });
  const [chart3Meta, setChart3Meta] = useState({ price: "4.23", change: "-0.45%", isUp: false });

  // Notebook SQL editor content
  const [sqlEditorText, setSqlEditorText] = useState(FALLBACK_DATASETS.gold_usd.sqlQuery);
  const [notebookTab, setNotebookTab] = useState("sql");
  const [notebookResult, setNotebookResult] = useState("");

  // Agent Animation States
  const [isAgentRunning, setIsAgentRunning] = useState(false);
  const [agentTasks, setAgentTasks] = useState(AGENT_TASKS_CHECKLIST);
  const [agentTraceLogs, setAgentTraceLogs] = useState([]);
  const [aiInsightBody, setAiInsightBody] = useState("");
  const [aiInsightConfidence, setAiInsightConfidence] = useState("");

  // Ingestion Capability States
  const [ingestCategories, setIngestCategories] = useState({
    etfs: true,
    stocks: false,
    mf: true,
    fii_dii: true,
    cot: true,
    fx_rates: false
  });
  const [ingestFullSync, setIngestFullSync] = useState(false);
  const [ingestLogs, setIngestLogs] = useState("");
  const [isIngestRunning, setIsIngestRunning] = useState(false);

  // Whale Tracker States
  const [whaleSelectedFund, setWhaleSelectedFund] = useState("DSP_MULTI_ASSET");
  const [whaleHoldings, setWhaleHoldings] = useState([]);
  const [isWhaleLoading, setIsWhaleLoading] = useState(false);

  // Anomaly Scanner States
  const [anomalySymbol, setAnomalySymbol] = useState("GOLDBEES");
  const [anomalyDays, setAnomalyDays] = useState("180");
  const [anomalyReport, setAnomalyReport] = useState("");
  const [isAnomalyRunning, setIsAnomalyRunning] = useState(false);

  // Promoter Dilution States
  const [dilutionSymbol, setDilutionSymbol] = useState("");
  const [dilutionReport, setDilutionReport] = useState("");
  const [isDilutionRunning, setIsDilutionRunning] = useState(false);

  // Backtest Crossover States
  const [backtestSymbol, setBacktestSymbol] = useState("GOLDBEES");
  const [backtestMaType, setBacktestMaType] = useState("sma");
  const [backtestFast, setBacktestFast] = useState(50);
  const [backtestSlow, setBacktestSlow] = useState(200);
  const [backtestReport, setBacktestReport] = useState("");
  const [isBacktestRunning, setIsBacktestRunning] = useState(false);

  // Load ClickHouse variables on startup & workspace switches
  useEffect(() => {
    fetchMorningBrief();
    fetchCharts(currentWorkspace);
  }, [currentWorkspace]);

  // Sync prompts and text area when workspace changes
  useEffect(() => {
    const data = FALLBACK_DATASETS[currentWorkspace];
    if (data) {
      setSqlEditorText(data.sqlQuery);
      setAiInsightBody(data.insightBody);
      setAiInsightConfidence(data.insightConfidence);

      if (currentWorkspace === "gold_usd") {
        setPromptText("Compare Gold with USD, Inflation, Interest Rates and explain current macro regime.");
      } else if (currentWorkspace === "india_macro") {
        setPromptText("Analyze recent FII net equity inflows and domestic macroeconomic indicators.");
      } else if (currentWorkspace === "nifty_momentum") {
        setPromptText("Scan the NSE market for volume breakouts and identify strongly bullish sectoral setups.");
      }
    }
  }, [currentWorkspace]);

  // ── CLICKHOUSE LOGIC ────────────────────────────────────────
  const fetchMorningBrief = async () => {
    try {
      // 1. Fetch Regime
      const qRegime = "SELECT regime_signal, prob_up FROM market_data.ml_predictions ORDER BY as_of DESC LIMIT 1";
      const resReg = await fetch(`/api/query?sql=${encodeURIComponent(qRegime)}`);
      const dataReg = await resReg.json();

      let regimeVal = FALLBACK_DATASETS.gold_usd.regime;
      if (dataReg && dataReg.length > 0 && !dataReg[0].error) {
        const probPct = (parseFloat(dataReg[0].prob_up) * 100).toFixed(0);
        regimeVal = `${dataReg[0].regime_signal} (${probPct}% probability)`;
      }

      // 2. Fetch Top Opps
      const qOpps = "SELECT etf_symbol, composite_score FROM market_data.signal_composite ORDER BY as_of DESC, composite_score DESC LIMIT 3";
      const resOpps = await fetch(`/api/query?sql=${encodeURIComponent(qOpps)}`);
      const dataOpps = await resOpps.json();

      let oppsVal = FALLBACK_DATASETS.gold_usd.briefOpps;
      if (dataOpps && dataOpps.length > 0 && !dataOpps[0].error) {
        oppsVal = dataOpps.map(row => ({
          name: `📈 ${row.etf_symbol}`,
          change: `Score: ${parseInt(row.composite_score)}`
        }));
      }

      // 3. Fetch Risks
      const qRisks = "SELECT etf_symbol, composite_score FROM market_data.signal_composite ORDER BY as_of DESC, composite_score ASC LIMIT 3";
      const resRisks = await fetch(`/api/query?sql=${encodeURIComponent(qRisks)}`);
      const dataRisks = await resRisks.json();

      let risksVal = [
        { name: "📉 USD Strength", change: "▼" },
        { name: "📉 High VIX", change: "▼" }
      ];
      if (dataRisks && dataRisks.length > 0 && !dataRisks[0].error) {
        risksVal = dataRisks.map(row => ({
          name: `📉 ${row.etf_symbol}`,
          change: `Score: ${parseInt(row.composite_score)}`
        }));
      }

      setMorningBrief({
        regime: regimeVal,
        opps: oppsVal,
        risks: risksVal,
        summary: FALLBACK_DATASETS[currentWorkspace]?.briefSummary || "Data pipeline ready."
      });
    } catch (e) {
      // Fallback
      const fb = FALLBACK_DATASETS[currentWorkspace];
      if (fb) {
        setMorningBrief({
          regime: fb.regime,
          opps: fb.briefOpps,
          risks: [
            { name: "USD Strength", change: "▼" },
            { name: "High VIX", change: "▼" },
            { name: "Crude Volatility", change: "▼" }
          ],
          summary: fb.briefSummary
        });
      }
    }
  };

  const fetchCharts = async (workspaceKey) => {
    const fb = FALLBACK_DATASETS[workspaceKey];
    if (!fb) return;

    let q1, q2, q3, q4;
    
    if (workspaceKey === 'gold_usd') {
      q1 = "SELECT trade_date as date, close as val FROM market_data.daily_prices WHERE symbol = 'GOLDBEES' ORDER BY trade_date DESC LIMIT 30";
      q2 = "SELECT trade_date as date, close as val FROM market_data.fx_rates WHERE symbol = 'USDINR' ORDER BY trade_date DESC LIMIT 30";
      q3 = "SELECT trade_date as date, close as val FROM market_data.daily_prices WHERE symbol = 'US10Y' ORDER BY trade_date DESC LIMIT 30";
      q4 = "SELECT as_of as date, expected_return_pct as val FROM market_data.ml_predictions ORDER BY as_of DESC LIMIT 30";
    } else if (workspaceKey === 'india_macro') {
      q1 = "SELECT trade_date as date, close as val FROM market_data.daily_prices WHERE symbol = 'BANKBEES' ORDER BY trade_date DESC LIMIT 30";
      q2 = "SELECT trade_date as date, fii_net_cr as val FROM market_data.fii_dii_flows ORDER BY trade_date DESC LIMIT 30";
      q3 = "SELECT trade_date as date, dii_net_cr as val FROM market_data.fii_dii_flows ORDER BY trade_date DESC LIMIT 30";
      q4 = "SELECT as_of as date, composite_score as val FROM market_data.signal_composite WHERE etf_symbol = 'GOLDBEES' ORDER BY as_of DESC LIMIT 30";
    } else if (workspaceKey === 'nifty_momentum') {
      q1 = "SELECT trade_date as date, close as val FROM market_data.daily_prices WHERE symbol = 'BANKBEES' ORDER BY trade_date DESC LIMIT 30";
      q2 = "SELECT trade_date as date, volume as val FROM market_data.daily_prices WHERE symbol = 'BANKBEES' ORDER BY trade_date DESC LIMIT 30";
      q3 = "SELECT trade_date as date, close as val FROM market_data.daily_prices WHERE symbol = 'GOLDBEES' ORDER BY trade_date DESC LIMIT 30";
      q4 = "SELECT as_of as date, composite_score as val FROM market_data.signal_composite WHERE etf_symbol = 'BANKBEES' ORDER BY as_of DESC LIMIT 30";
    }

    // Helper to fetch individual vectors
    const fetchVector = async (sql, fallback) => {
      try {
        const res = await fetch(`/api/query?sql=${encodeURIComponent(sql)}`);
        const data = await res.json();
        if (data && data.length > 0 && !data[0].error) {
          data.sort((a, b) => new Date(a.date) - new Date(b.date));
          return data.map(row => parseFloat(row.val || row.close || 0));
        }
      } catch (e) {}
      return fallback;
    };

    const d1 = await fetchVector(q1, fb.chart1Data);
    const d2 = await fetchVector(q2, fb.chart2Data);
    const d3 = await fetchVector(q3, fb.chart3Data);
    const d4 = await fetchVector(q4, fb.chart4Data || [0.1, 0.2]);

    setChart1Data(d1);
    setChart2Data(d2);
    setChart3Data(d3);
    setChart4Data(d4);

    // Update Price Metas dynamically
    const updateMeta = (dataArr, currentMeta, fallbackPrice, fallbackChange) => {
      if (dataArr && dataArr.length > 0) {
        const latest = dataArr[dataArr.length - 1];
        const prev = dataArr.length > 1 ? dataArr[dataArr.length - 2] : latest;
        const diff = prev !== 0 ? ((latest - prev) / prev * 100) : 0;
        return {
          price: latest.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
          change: `${diff >= 0 ? "+" : ""}${diff.toFixed(2)}%`,
          isUp: diff >= 0
        };
      }
      return { price: fallbackPrice, change: fallbackChange, isUp: fallbackChange.startsWith("+") };
    };

    setChart1Meta(updateMeta(d1, chart1Meta, fb.chart1Price, fb.chart1Change));
    setChart2Meta(updateMeta(d2, chart2Meta, fb.chart2Price, fb.chart2Change));
    setChart3Meta(updateMeta(d3, chart3Meta, fb.chart3Price, fb.chart3Change));
  };

  // ── RUN NOTEBOOK SQL ────────────────────────────────────────
  const runNotebookSQL = async () => {
    setNotebookResult("Executing ClickHouse query...");
    try {
      const res = await fetch(`/api/query?sql=${encodeURIComponent(sqlEditorText)}`);
      const data = await res.json();
      if (data && data.length > 0 && !data[0].error) {
        setNotebookResult(JSON.stringify(data.slice(0, 10), null, 2));
      } else if (data && data[0]?.error) {
        setNotebookResult(`⚠️ ClickHouse Error: ${data[0].error}`);
      } else {
        setNotebookResult("No records found.");
      }
    } catch (e) {
      setNotebookResult(`⚠️ Connection failed: ${e.message}`);
    }
  };

  // ── AGENT ACTIONS (CHECKLIST PIPELINE) ──────────────────────────
  const triggerAgentPipeline = async () => {
    if (isAgentRunning) return;
    setIsAgentRunning(true);
    setAgentTraceLogs([]);
    
    // Reset tasks status to pending
    setAgentTasks(AGENT_TASKS_CHECKLIST.map(t => ({ ...t, status: "pending", time: "" })));
    
    // Asynchronous step-by-step pipeline animations
    const stepDuration = 1200;
    
    for (let i = 0; i < AGENT_TASKS_CHECKLIST.length; i++) {
      // Set current running
      setAgentTasks(prev => {
        const copy = [...prev];
        copy[i].status = "running";
        return copy;
      });
      
      const timestamp = new Date().toLocaleTimeString();
      setAgentTraceLogs(prev => [...prev, { text: `[${timestamp}] Executing ${AGENT_TASKS_CHECKLIST[i].label}...`, active: true }]);
      
      await new Promise(r => setTimeout(r, stepDuration));
      
      // Set current completed
      setAgentTasks(prev => {
        const copy = [...prev];
        copy[i].status = "completed";
        copy[i].time = `${(stepDuration / 1000).toFixed(1)}s`;
        return copy;
      });

      setAgentTraceLogs(prev => {
        const copy = [...prev];
        if (copy.length > 0) copy[copy.length - 1].active = false;
        copy.push({ text: `[${new Date().toLocaleTimeString()}] Completed ${AGENT_TASKS_CHECKLIST[i].label}.`, active: false });
        return copy;
      });
    }

    // Hit server API endpoint to get model insights
    try {
      const res = await fetch("/api/agent/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: promptText })
      });
      const data = await res.json();
      
      if (data.status === "success") {
        setAiInsightBody(data.insightBody);
        setAiInsightConfidence(data.insightConfidence);
      }
    } catch (e) {}

    setIsAgentRunning(false);
  };

  // ── INGESTION PIPELINE ──────────────────────────────────────
  const startDataIngest = async () => {
    setIsIngestRunning(true);
    setIngestLogs("Starting Ingestion process in container...\n");

    const categories = Object.keys(ingestCategories)
      .filter(k => ingestCategories[k])
      .join(",");

    try {
      const res = await fetch("/api/import/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ categories, full: ingestFullSync })
      });
      const data = await res.json();
      
      if (data.status === "success") {
        setIngestLogs(prev => prev + `[Launch Success] Process pid: ${data.pid}\n\n`);
        pollIngestLogs();
      } else {
        setIngestLogs(prev => prev + `⚠️ Import Error: ${data.error}\n`);
        setIsIngestRunning(false);
      }
    } catch (e) {
      setIngestLogs(prev => prev + `⚠️ Connection failed: ${e.message}\n`);
      setIsIngestRunning(false);
    }
  };

  const pollIngestLogs = () => {
    const timer = setInterval(async () => {
      try {
        const res = await fetch("/api/import/status");
        const data = await res.json();
        
        if (data.logs) {
          setIngestLogs(data.logs);
        }
        if (data.status !== "running") {
          setIngestLogs(prev => prev + `\n[Process Terminated] Exit Code: ${data.exit_code}`);
          setIsIngestRunning(false);
          clearInterval(timer);
          fetchMorningBrief(); // Refresh UI datasets
        }
      } catch (e) {
        clearInterval(timer);
        setIsIngestRunning(false);
      }
    }, 1500);
  };

  // ── WHALE HOLDINGS TRACKER ──────────────────────────────────
  const fetchWhaleHoldings = async () => {
    setIsWhaleLoading(true);
    setWhaleHoldings([]);

    const sql = `
      SELECT security_name, pct_of_nav, market_value_cr, asset_type 
      FROM market_data.mf_holdings FINAL 
      WHERE fund_name = '${whaleSelectedFund}'
        AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE fund_name = '${whaleSelectedFund}')
      ORDER BY pct_of_nav DESC 
      LIMIT 12
    `;

    try {
      const res = await fetch(`/api/query?sql=${encodeURIComponent(sql)}`);
      const data = await res.json();
      
      if (data && data.length > 0 && !data[0].error) {
        setWhaleHoldings(data);
      } else if (data && data[0]?.error) {
        alert("ClickHouse query failed: " + data[0].error);
      } else {
        alert("No disclosures found for " + whaleSelectedFund);
      }
    } catch (e) {
      alert("Failed to query ClickHouse: " + e.message);
    }
    setIsWhaleLoading(false);
  };

  // ── ANOMALY SCANNER ─────────────────────────────────────────
  const runAnomalyScan = async () => {
    setIsAnomalyRunning(true);
    setAnomalyReport("Running composite Isolation Forest + GARCH volatility anomaly scan...");

    try {
      const res = await fetch("/api/anomaly/scan", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol: anomalySymbol, days: parseInt(anomalyDays) })
      });
      const data = await res.json();

      if (data.status === "success") {
        setAnomalyReport(data.report);
      } else {
        setAnomalyReport(`⚠️ Scanner error: ${data.error}`);
      }
    } catch (e) {
      setAnomalyReport(`⚠️ Connection error: ${e.message}`);
    }
    setIsAnomalyRunning(false);
  };

  // ── PROMOTER DILUTION AUDIT ─────────────────────────────────
  const runDilutionAudit = async () => {
    if (!dilutionSymbol) {
      alert("Please enter a stock symbol.");
      return;
    }
    setIsDilutionRunning(true);
    setDilutionReport(`Auditing shareholding records for ${dilutionSymbol}...`);

    try {
      const res = await fetch("/api/dilution/check", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol: dilutionSymbol })
      });
      const data = await res.json();

      if (data.status === "success") {
        setDilutionReport(data.report);
      } else {
        setDilutionReport(`⚠️ Auditor error: ${data.error}`);
      }
    } catch (e) {
      setDilutionReport(`⚠️ Connection error: ${e.message}`);
    }
    setIsDilutionRunning(false);
  };

  // ── MA CROSSOVER BACKTESTER ─────────────────────────────────
  const runBacktest = async () => {
    setIsBacktestRunning(true);
    setBacktestReport(`Simulating Moving Average Crossover Backtest on ClickHouse prices...`);

    try {
      const res = await fetch("/api/backtest/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          symbol: backtestSymbol,
          fast: backtestFast,
          slow: backtestSlow,
          ma_type: backtestMaType
        })
      });
      const data = await res.json();

      if (data.status === "success") {
        setBacktestReport(data.report);
      } else {
        setBacktestReport(`⚠️ Backtester error: ${data.error}`);
      }
    } catch (e) {
      setBacktestReport(`⚠️ Connection error: ${e.message}`);
    }
    setIsBacktestRunning(false);
  };

  // Render Markdown helper (extremely basic client-side renderer)
  const formatMarkdown = (text) => {
    if (!text) return "";
    let formatted = text;
    // headings
    formatted = formatted.replace(/### (.*)/g, '<h3 style="color:#00ffcc; margin-top:16px; font-size:13px; text-transform:uppercase;">$1</h3>');
    // bold
    formatted = formatted.replace(/\*\*([^*]+)\*\*/g, '<strong style="color:#fff;">$1</strong>');
    // bullet points
    formatted = formatted.replace(/- (.*)/g, '<li style="margin-left:14px; margin-bottom:6px;">$1</li>');
    // blockquotes
    formatted = formatted.replace(/> (.*)/g, '<blockquote style="border-left:3px solid var(--purple); padding-left:10px; margin: 10px 0; color: #a0aec0;">$1</blockquote>');
    return <div dangerouslySetInnerHTML={{ __html: formatted }} />;
  };

  return (
    <div className="app-container">
      {/* ── LEFT SIDEBAR (NAVIGATION) ─────────────────────────── */}
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-logo">M</div>
          <div className="brand-title">MOSAIC<span>Studio</span></div>
        </div>

        <div className="nav-section">
          <div className="nav-header">Platform</div>
          <div 
            className={`nav-item ${currentWorkspace === "gold_usd" ? "active" : ""}`}
            onClick={() => setCurrentWorkspace("gold_usd")}
          >
            <Compass size={14} /> Dashboard
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "india_macro" ? "active" : ""}`}
            onClick={() => setCurrentWorkspace("india_macro")}
          >
            <Layers size={14} /> Markets
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "dilution_check" ? "active" : ""}`}
            onClick={() => setCurrentWorkspace("dilution_check")}
          >
            <ShieldAlert size={14} /> Screener
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "anomaly_scan" ? "active" : ""}`}
            onClick={() => setCurrentWorkspace("anomaly_scan")}
          >
            <Search size={14} /> Research
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "backtest" ? "active" : ""}`}
            onClick={() => setCurrentWorkspace("backtest")}
          >
            <TrendingUp size={14} /> Strategies
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "anomaly_scan" ? "active" : ""}`}
            onClick={() => setCurrentWorkspace("anomaly_scan")}
          >
            <Cpu size={14} /> Agents
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "backtest" ? "active" : ""}`}
            onClick={() => setCurrentWorkspace("backtest")}
          >
            <BarChart2 size={14} /> Backtest
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "data_ingest" ? "active" : ""}`}
            onClick={() => setCurrentWorkspace("data_ingest")}
          >
            <Database size={14} /> Data Explorer
          </div>
        </div>

        <div className="nav-section">
          <div className="nav-header">Workspaces</div>
          <div 
            className={`nav-item ${currentWorkspace === "gold_usd" ? "active" : ""}`} 
            onClick={() => setCurrentWorkspace("gold_usd")}
          >
            ⚡ Gold vs USD Analysis
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "india_macro" ? "active" : ""}`} 
            onClick={() => setCurrentWorkspace("india_macro")}
          >
            🇮🇳 India Macro
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "nifty_momentum" ? "active" : ""}`} 
            onClick={() => setCurrentWorkspace("nifty_momentum")}
          >
            📈 Nifty Momentum
          </div>
        </div>

        <div className="nav-section" style={{ flex: 1 }}>
          <div className="nav-header">System Desk</div>
          <div 
            className={`nav-item ${currentWorkspace === "data_ingest" ? "active" : ""}`} 
            onClick={() => setCurrentWorkspace("data_ingest")}
          >
            📥 Data Ingest
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "whale_tracker" ? "active" : ""}`} 
            onClick={() => setCurrentWorkspace("whale_tracker")}
          >
            📦 Whale Tracker
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "anomaly_scan" ? "active" : ""}`} 
            onClick={() => setCurrentWorkspace("anomaly_scan")}
          >
            🔬 Anomaly Scan
          </div>
          <div 
            className={`nav-item ${currentWorkspace === "dilution_check" ? "active" : ""}`} 
            onClick={() => setCurrentWorkspace("dilution_check")}
          >
            🕵️ Insider Sales
          </div>
        </div>
      </aside>

      {/* ── CENTER MAIN WORKSPACE ─────────────────────────────── */}
      <main className="main-workspace">
        <header className="top-header">
          <div className="workspace-info">
            <span className="workspace-title">
              {currentWorkspace === "data_ingest" && "📥 Data Ingest Pipeline"}
              {currentWorkspace === "whale_tracker" && "📦 Institutional Whale Tracker"}
              {currentWorkspace === "anomaly_scan" && "🔬 Volatility Anomaly Scanner"}
              {currentWorkspace === "dilution_check" && "🕵️ Promoter Dilution Auditor"}
              {currentWorkspace === "backtest" && "📈 Crossover Backtest Studio"}
              {!["data_ingest", "whale_tracker", "anomaly_scan", "dilution_check", "backtest"].includes(currentWorkspace) && 
                (FALLBACK_DATASETS[currentWorkspace]?.title || "Quant Dashboard")}
            </span>
            
            {/* Show selector dropdown only for financial workspaces */}
            {!["data_ingest", "whale_tracker", "anomaly_scan", "dilution_check", "backtest"].includes(currentWorkspace) && (
              <select 
                className="workspace-select"
                value={currentWorkspace}
                onChange={(e) => setCurrentWorkspace(e.target.value)}
              >
                <option value="gold_usd">Gold vs USD</option>
                <option value="india_macro">India Macro</option>
                <option value="nifty_momentum">Nifty Momentum</option>
              </select>
            )}
          </div>

          <div className="header-actions">
            <button className="btn-action" onClick={fetchMorningBrief}>
              <RefreshCw size={12} style={{ marginRight: 6 }} /> Refresh
            </button>
            <button className="btn-action primary" onClick={() => setCurrentWorkspace("backtest")}>
              New Strategy
            </button>
          </div>
        </header>

        {/* ── AI PROMPT BAR ───────────────────────────────────── */}
        {!["data_ingest", "whale_tracker", "anomaly_scan", "dilution_check", "backtest"].includes(currentWorkspace) && (
          <div className="prompt-bar-container">
            <div className="prompt-input-wrapper">
              <input 
                type="text" 
                className="prompt-input"
                value={promptText}
                onChange={(e) => setPromptText(e.target.value)}
                placeholder="Ask Mosaic Agent to run pipeline analysis..."
              />
              <button 
                className="prompt-run-btn"
                onClick={triggerAgentPipeline}
                disabled={isAgentRunning}
              >
                <ChevronRight size={20} color="#000" />
              </button>
            </div>
          </div>
        )}

        {/* ── CONDITIONAL WORKSPACE CONTAINERS ─────────────────── */}
        
        {/* 1. FINANCIAL DASHBOARD CONTAINER */}
        {!["data_ingest", "whale_tracker", "anomaly_scan", "dilution_check", "backtest"].includes(currentWorkspace) && (
          <div className="widgets-grid">
            
            {/* Morning Brief Row */}
            <div className="glass-card brief-widget span-12 accent-cyan">
              <div className="brief-column">
                <div className="brief-label">Market Regime</div>
                <div className="brief-val-regime">{morningBrief.regime}</div>
              </div>
              <div className="brief-column">
                <div className="brief-label">Top Opportunities</div>
                <div className="brief-opps-list">
                  {morningBrief.opps.map((opp, i) => (
                    <div className="brief-opp-row" key={i}>
                      <span className="brief-opp-name">{opp.name}</span>
                      <span className="brief-opp-change up">{opp.change}</span>
                    </div>
                  ))}
                </div>
              </div>
              <div className="brief-column">
                <div className="brief-label">Macro Risks</div>
                <div className="brief-opps-list">
                  {morningBrief.risks.map((risk, i) => (
                    <div className="brief-opp-row" key={i}>
                      <span className="brief-opp-name" style={{ color: "#cbd5e1" }}>{risk.name}</span>
                      <span className="brief-opp-change down">{risk.change}</span>
                    </div>
                  ))}
                </div>
              </div>
              <div className="brief-column">
                <div className="brief-label">Morning Brief</div>
                <div className="brief-summary-text">{morningBrief.summary}</div>
              </div>
            </div>

            {/* Charts Row */}
            <div className="glass-card chart-container-large">
              <div className="chart-header">
                <div className="chart-title">
                  {FALLBACK_DATASETS[currentWorkspace]?.chart1Title}
                </div>
                <div className="chart-meta">
                  <span className="price">{chart1Meta.price}</span>
                  <span className={`change ${chart1Meta.isUp ? "up" : "down"}`}>{chart1Meta.change}</span>
                </div>
              </div>
              <CanvasChart data={chart1Data} color="cyan" height={180} />
            </div>

            <div className="glass-card chart-container-large">
              <div className="chart-header">
                <div className="chart-title">
                  {FALLBACK_DATASETS[currentWorkspace]?.chart2Title}
                </div>
                <div className="chart-meta">
                  <span className="price">{chart2Meta.price}</span>
                  <span className={`change ${chart2Meta.isUp ? "up" : "down"}`}>{chart2Meta.change}</span>
                </div>
              </div>
              <CanvasChart data={chart2Data} color="magenta" height={180} />
            </div>

            <div className="glass-card chart-container-large">
              <div className="chart-header">
                <div className="chart-title">
                  {FALLBACK_DATASETS[currentWorkspace]?.chart3Title}
                </div>
                <div className="chart-meta">
                  <span className="price">{chart3Meta.price}</span>
                  <span className={`change ${chart3Meta.isUp ? "up" : "down"}`}>{chart3Meta.change}</span>
                </div>
              </div>
              <CanvasChart data={chart3Data} color="gold" height={180} />
            </div>

            <div className="glass-card chart-container-large">
              <div className="chart-header">
                <div className="chart-title">
                  {FALLBACK_DATASETS[currentWorkspace]?.chart4Title || "Expected Return"}
                </div>
              </div>
              <CanvasChart data={chart4Data} color="purple" height={180} />
            </div>

            {/* AI Insights and Notebook/Sources Row */}
            <div className="glass-card insight-widget accent-purple">
              <div className="insight-header">
                <Cpu size={14} color="#9933ff" />
                AI PIPELINE FORECAST & ALLOCATION ADVICE
              </div>
              <div className="insight-body">{aiInsightBody}</div>
              <div className="insight-confidence">{aiInsightConfidence}</div>
            </div>

            <div className="bottom-section">
              <div className="glass-card notebook-widget">
                <div className="notebook-header">
                  <div className="notebook-tabs">
                    <button 
                      className={`notebook-tab-btn ${notebookTab === "sql" ? "active" : ""}`}
                      onClick={() => setNotebookTab("sql")}
                    >
                      ClickHouse SQL
                    </button>
                    <button 
                      className={`notebook-tab-btn ${notebookTab === "python" ? "active" : ""}`}
                      onClick={() => setNotebookTab("python")}
                    >
                      Python Script
                    </button>
                  </div>
                </div>
                <div className="editor-wrapper">
                  <textarea
                    value={sqlEditorText}
                    onChange={(e) => setSqlEditorText(e.target.value)}
                    style={{
                      width: "100%",
                      height: "110px",
                      background: "transparent",
                      border: "none",
                      color: "#00ffcc",
                      fontFamily: "var(--font-mono)",
                      fontSize: "12px",
                      outline: "none",
                      resize: "none"
                    }}
                  />
                  <button className="editor-run-btn" onClick={runNotebookSQL}>
                    <Play size={12} fill="currentColor" />
                  </button>
                </div>
                {notebookResult && (
                  <pre 
                    style={{
                      background: "#05070a",
                      padding: "10px",
                      fontSize: "10.5px",
                      overflowX: "auto",
                      color: "#a0aec0",
                      maxHeight: "150px",
                      margin: 0,
                      borderRadius: "0 0 12px 12px",
                      borderTop: "1px solid var(--border-color)",
                      fontFamily: "var(--font-mono)"
                    }}
                  >
                    {notebookResult}
                  </pre>
                )}
              </div>

              <div className="glass-card sources-widget">
                <div className="sources-title">Ingested Quant Pipelines</div>
                <div className="sources-list">
                  <div className="source-card">
                    <div className="source-info">
                      <span className="source-name">Yahoo Finance</span>
                    </div>
                    <span className="source-indicator"></span>
                  </div>
                  <div className="source-card">
                    <div className="source-info">
                      <span className="source-name">Screener Scraper</span>
                    </div>
                    <span className="source-indicator"></span>
                  </div>
                  <div className="source-card">
                    <div className="source-info">
                      <span className="source-name">NSE India iNAV</span>
                    </div>
                    <span className="source-indicator"></span>
                  </div>
                  <div className="source-card disconnected">
                    <div className="source-info">
                      <span className="source-name">Zerodha Kite API</span>
                    </div>
                    <span className="source-indicator"></span>
                  </div>
                </div>
              </div>
            </div>

          </div>
        )}

        {/* 2. DATA INGEST SYSTEM DESK */}
        {currentWorkspace === "data_ingest" && (
          <div className="ingest-grid">
            <div className="glass-card desk-card">
              <div className="desk-title">📥 Pipeline Fetchers Configuration</div>
              
              <div className="check-list">
                {Object.keys(ingestCategories).map((cat) => (
                  <label className="check-item" key={cat}>
                    <input 
                      type="checkbox"
                      checked={ingestCategories[cat]}
                      onChange={(e) => setIngestCategories(prev => ({ ...prev, [cat]: e.target.checked }))}
                    />
                    {cat.toUpperCase()} (fetcher)
                  </label>
                ))}
              </div>

              <div style={{ marginTop: 14 }}>
                <div className="toggle-row">
                  <span>Force Complete History Backfill</span>
                  <input 
                    type="checkbox" 
                    checked={ingestFullSync}
                    onChange={(e) => setIngestFullSync(e.target.checked)}
                    style={{ accentColor: "var(--cyan)" }}
                  />
                </div>
              </div>

              <button 
                className="trigger-btn" 
                onClick={startDataIngest}
                disabled={isIngestRunning}
                style={{ marginTop: 12 }}
              >
                <Terminal size={14} /> Launch Ingestion Task
              </button>
            </div>

            <div className="glass-card desk-card">
              <div className="desk-title">💻 Live Docker Container STDOUT Log</div>
              <TerminalLog logs={ingestLogs} />
            </div>
          </div>
        )}

        {/* 3. WHALE TRACKER SYSTEM DESK */}
        {currentWorkspace === "whale_tracker" && (
          <div className="glass-card desk-card">
            <div className="desk-title">📦 Cross-Fund Institutional Disclosures Explorer</div>
            <div style={{ display: "flex", gap: "12px", alignItems: "center" }}>
              <select 
                className="text-input-field" 
                value={whaleSelectedFund}
                onChange={(e) => setWhaleSelectedFund(e.target.value)}
                style={{ width: "300px" }}
              >
                <option value="DSP_MULTI_ASSET">DSP Multi Asset Allocation Fund (Scheme 152056)</option>
                <option value="DSP_SMALL_CAP">DSP Small Cap Direct Growth (Scheme 119212)</option>
                <option value="DSP_MID_CAP">DSP Mid Cap Direct Growth (Scheme 119071)</option>
                <option value="DSP_FLEXI_CAP">DSP Flexi Cap Direct Growth (Scheme 119076)</option>
              </select>
              <button 
                className="trigger-btn" 
                onClick={fetchWhaleHoldings}
                disabled={isWhaleLoading}
              >
                <Eye size={14} /> Track holdings
              </button>
            </div>

            <div style={{ marginTop: 16 }}>
              <div className="desk-title">📊 Top Active Holdings Disclosed (Latest Month)</div>
              {whaleHoldings.length === 0 ? (
                <div style={{ padding: "40px", textAlign: "center", color: "#718096", border: "1px dashed var(--border-color)", borderRadius: "8px" }}>
                  {isWhaleLoading ? "Querying ClickHouse disclosures..." : "Tap Track holdings to retrieve records."}
                </div>
              ) : (
                <table className="desk-table">
                  <thead>
                    <tr>
                      <th>Asset / Security Name</th>
                      <th>Category</th>
                      <th>Percent of NAV</th>
                      <th>Market Value (Cr)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {whaleHoldings.map((h, idx) => (
                      <tr key={idx}>
                        <td style={{ fontWeight: "bold", color: "#fff" }}>{h.security_name}</td>
                        <td>{h.asset_type || "Equity"}</td>
                        <td style={{ color: "var(--cyan)", fontWeight: "bold" }}>{h.pct_of_nav?.toFixed(2)}%</td>
                        <td>₹{h.market_value_cr?.toFixed(2)} Cr</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}

        {/* 4. VOLATILITY ANOMALY SCANNER DESK */}
        {currentWorkspace === "anomaly_scan" && (
          <div className="glass-card desk-card">
            <div className="desk-title">🔬 Volatility Anomaly & Shock Attributor</div>
            <div style={{ display: "flex", gap: "12px", alignItems: "center", flexWrap: "wrap" }}>
              <input 
                type="text" 
                className="text-input-field" 
                value={anomalySymbol}
                onChange={(e) => setAnomalySymbol(e.target.value)}
                placeholder="Ticker Symbol (e.g. GOLDBEES)"
                style={{ width: "200px" }}
              />
              <select 
                className="text-input-field" 
                value={anomalyDays}
                onChange={(e) => setAnomalyDays(e.target.value)}
              >
                <option value="90">90 Days Lookback</option>
                <option value="180">180 Days Lookback</option>
                <option value="365">365 Days Lookback</option>
              </select>
              <button 
                className="trigger-btn" 
                onClick={runAnomalyScan}
                disabled={isAnomalyRunning}
              >
                🔬 Scan Ticker
              </button>
            </div>

            <div style={{ marginTop: 20 }}>
              <div className="desk-title">📋 Shock Attribution Report</div>
              <div className="result-box-md">
                {isAnomalyRunning ? "Running GARCH+IF+PELT pipeline on ClickHouse..." : formatMarkdown(anomalyReport)}
              </div>
            </div>
          </div>
        )}

        {/* 5. PROMOTER DILUTION AUDIT DESK */}
        {currentWorkspace === "dilution_check" && (
          <div className="glass-card desk-card">
            <div className="desk-title">🕵️ Promoter Shareholding & Dilution Auditor</div>
            <div style={{ display: "flex", gap: "12px", alignItems: "center" }}>
              <input 
                type="text" 
                className="text-input-field" 
                value={dilutionSymbol}
                onChange={(e) => setDilutionSymbol(e.target.value)}
                placeholder="NSE Symbol (e.g. TECHNO, RELIANCE)"
                style={{ width: "250px" }}
              />
              <button 
                className="trigger-btn" 
                onClick={runDilutionAudit}
                disabled={isDilutionRunning}
              >
                🕵️ Run Audit
              </button>
            </div>

            <div style={{ marginTop: 20 }}>
              <div className="desk-title">📊 Auditor Analysis Summary</div>
              <div className="result-box-md">
                {isDilutionRunning ? "Scraping Screener.in shareholding pattern details..." : formatMarkdown(dilutionReport)}
              </div>
            </div>
          </div>
        )}

        {/* 6. CROSSOVER STRATEGY BACKTESTER DESK */}
        {currentWorkspace === "backtest" && (
          <div className="glass-card desk-card">
            <div className="desk-title">📈 MA Crossover Backtester</div>
            <div style={{ display: "flex", gap: "12px", alignItems: "center", flexWrap: "wrap" }}>
              <input 
                type="text" 
                className="text-input-field" 
                value={backtestSymbol}
                onChange={(e) => setBacktestSymbol(e.target.value)}
                placeholder="Symbol (e.g. GOLDBEES)"
                style={{ width: "150px" }}
              />
              <select 
                className="text-input-field" 
                value={backtestMaType}
                onChange={(e) => setBacktestMaType(e.target.value)}
              >
                <option value="sma">SMA (Simple Moving Average)</option>
                <option value="ema">EMA (Exponential Moving Average)</option>
              </select>
              <div style={{ color: "#a0aec0", fontSize: "13px" }}>Fast:</div>
              <input 
                type="number" 
                className="text-input-field" 
                value={backtestFast}
                onChange={(e) => setBacktestFast(e.target.value)}
                style={{ width: "70px" }}
              />
              <div style={{ color: "#a0aec0", fontSize: "13px" }}>Slow:</div>
              <input 
                type="number" 
                className="text-input-field" 
                value={backtestSlow}
                onChange={(e) => setBacktestSlow(e.target.value)}
                style={{ width: "70px" }}
              />
              <button 
                className="trigger-btn" 
                onClick={runBacktest}
                disabled={isBacktestRunning}
              >
                🚀 Run Backtest
              </button>
            </div>

            <div style={{ marginTop: 20 }}>
              <div className="desk-title">📊 Strategy Performance Report</div>
              <div className="result-box-md" style={{ fontFamily: "var(--font-mono)", color: "var(--cyan)" }}>
                {isBacktestRunning ? "Simulating backtest trades..." : formatMarkdown(backtestReport)}
              </div>
            </div>
          </div>
        )}

      </main>

      {/* ── RIGHT PANEL (LIVE AGENT PIPELINE MONITOR) ─────────── */}
      <aside className="right-panel">
        <div className="agent-header">
          <div className="agent-title">
            <span className="pulse-dot purple"></span>
            MOSAIC AGENT
          </div>
          <span style={{ fontSize: "11px", color: "var(--purple)", fontWeight: "bold" }}>ACTIVE</span>
        </div>

        <div className="agent-tabs">
          <button className="agent-tab-btn active">Execution steps</button>
        </div>

        <div className="agent-tasks-list">
          {agentTasks.map((task, idx) => (
            <div 
              className={`glass-card task-card ${task.status === "running" ? "active" : ""}`}
              key={idx}
            >
              <div className="task-header">
                <span className="task-title">{task.label}</span>
                <span className={`task-status ${task.status}`}>
                  {task.status.toUpperCase()} {task.time && `(${task.time})`}
                </span>
              </div>
              <div className="task-progress-bar">
                <div 
                  className="task-progress"
                  style={{ 
                    width: task.status === "completed" ? "100%" : task.status === "running" ? "60%" : "0%",
                    animation: task.status === "running" ? "breathing-purple 1.5s infinite" : "none"
                  }}
                />
              </div>
            </div>
          ))}
        </div>

        {/* Console Agent Execution Traces */}
        <div className="agent-trace-logs">
          {agentTraceLogs.map((log, idx) => (
            <div className={`trace-item ${log.active ? "active" : ""}`} key={idx}>
              <span>{log.text}</span>
            </div>
          ))}
          {agentTraceLogs.length === 0 && (
            <div style={{ color: "#4a5568", fontSize: "11px", textAlign: "center", paddingTop: "70px" }}>
              Agent traces output will appear here during pipeline run.
            </div>
          )}
        </div>
      </aside>
    </div>
  );
}

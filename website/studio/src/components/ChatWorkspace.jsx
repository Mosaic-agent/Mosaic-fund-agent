import React, { useState, useEffect, useRef, useCallback } from "react";
import { Send, Plus, MessageSquare, Zap } from "lucide-react";

// ── Intent badge colour map ────────────────────────────────────────────────
const INTENT_COLORS = {
  signal:       "#00E5C8",
  macro:        "#F59E0B",
  deepdive:     "#8B5CF6",
  india_equity: "#10B981",
  equity:       "#10B981",
  intl_etf:     "#3B82F6",
  news:         "#3B82F6",
  database:     "#64748B",
  db:           "#64748B",
  code:         "#64748B",
  research:     "#8B5CF6",
  main:         "#475569",
};

// ── Slash command quick-fill definitions ──────────────────────────────────
const SLASH_CMDS = [
  { cmd: "/signals",        label: "/signals",       hint: "ETF composite scores",    intent: "signal" },
  { cmd: "/macro",          label: "/macro",         hint: "Macro + FII/DII scan",   intent: "macro" },
  { cmd: "/deepdive ",      label: "/deepdive",      hint: "Research any stock",      intent: "deepdive" },
  { cmd: "/help",           label: "/help",          hint: "Show all commands",       intent: "main" },
  { cmd: "/analyze",        label: "/analyze",       hint: "Portfolio analysis",      intent: "main" },
];

// ── Starter capability cards shown on empty state ────────────────────────
const CAPS = [
  { icon: "📊", title: "Signal Dashboard",   desc: "ETF composite scores, regime, prob_up",   prompt: "/signals" },
  { icon: "🌍", title: "Macro Scanner",       desc: "FII/DII flows, COMEX, global themes",     prompt: "/macro" },
  { icon: "🔬", title: "Deep Dive",           desc: "India or US stock research note",          prompt: "/deepdive GOLDBEES" },
  { icon: "📈", title: "Anomaly Detection",   desc: "GARCH+IF+PELT price shock attribution",   prompt: "Scan GOLDBEES for anomalies in the last 180 days" },
  { icon: "🐋", title: "Whale Tracker",       desc: "DSP multi-asset institutional holdings",   prompt: "What are the top DSP Multi Asset holdings this month?" },
  { icon: "⚡", title: "GOLDBEES Signal",     desc: "ML prediction + Kelly weight + regime",    prompt: "What is the current GOLDBEES signal and recommended allocation?" },
];

// ── Lightweight markdown → HTML renderer ─────────────────────────────────
function renderMD(text) {
  if (!text) return "";
  let out = text;

  // Fenced code blocks first (before inline code)
  out = out.replace(/```[\w]*\n?([\s\S]*?)```/g, (_, code) =>
    `<pre><code>${code.replace(/</g, "&lt;").replace(/>/g, "&gt;")}</code></pre>`
  );

  // Headings
  out = out.replace(/^### (.+)$/gm, "<h3>$1</h3>");
  out = out.replace(/^## (.+)$/gm, "<h2>$1</h2>");
  out = out.replace(/^# (.+)$/gm, "<h1>$1</h1>");

  // Bold + italic
  out = out.replace(/\*\*\*(.+?)\*\*\*/g, "<strong><em>$1</em></strong>");
  out = out.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
  out = out.replace(/\*(.+?)\*/g, "<em>$1</em>");

  // Inline code
  out = out.replace(/`([^`]+)`/g, "<code>$1</code>");

  // Blockquotes
  out = out.replace(/^> (.+)$/gm, "<blockquote>$1</blockquote>");

  // Horizontal rule
  out = out.replace(/^---+$/gm, "<hr/>");

  // Unordered lists
  out = out.replace(/((?:^[-*+] .+\n?)+)/gm, (block) => {
    const items = block.trim().split("\n").map(l =>
      `<li>${l.replace(/^[-*+] /, "")}</li>`
    ).join("");
    return `<ul>${items}</ul>`;
  });

  // Numbered lists
  out = out.replace(/((?:^\d+\. .+\n?)+)/gm, (block) => {
    const items = block.trim().split("\n").map(l =>
      `<li>${l.replace(/^\d+\. /, "")}</li>`
    ).join("");
    return `<ol>${items}</ol>`;
  });

  // Double newline → paragraph break (only outside pre/code)
  out = out.replace(/\n\n+/g, "<br/><br/>");
  out = out.replace(/\n/g, "<br/>");

  return out;
}

// ── Typing indicator component ────────────────────────────────────────────
function TypingIndicator() {
  return (
    <div className="chat-msg-agent-wrapper">
      <div className="chat-msg-agent" style={{ padding: "12px 16px" }}>
        <div className="typing-indicator">
          <div className="typing-dot" />
          <div className="typing-dot" />
          <div className="typing-dot" />
        </div>
      </div>
    </div>
  );
}

// ── Intent badge ─────────────────────────────────────────────────────────
function IntentBadge({ intent }) {
  if (!intent || intent === "main") return null;
  const color = INTENT_COLORS[intent] || INTENT_COLORS.main;
  return (
    <span className="intent-badge" style={{ color, borderColor: color }}>
      {intent.replace("_", " ")}
    </span>
  );
}

// ── Single message bubble ────────────────────────────────────────────────
function ChatMessage({ msg, onSuggestionClick }) {
  if (msg.role === "user") {
    return (
      <div className="chat-msg-user">
        {msg.content}
      </div>
    );
  }

  if (msg.loading) {
    return <TypingIndicator />;
  }

  return (
    <div className="chat-msg-agent-wrapper">
      <IntentBadge intent={msg.intent} />
      <div
        className="chat-msg-agent"
        dangerouslySetInnerHTML={{ __html: renderMD(msg.content) }}
      />
      {msg.suggestions && msg.suggestions.length > 0 && (
        <div className="chat-suggestions">
          {msg.suggestions.map((s, i) => (
            <button
              key={i}
              className="chat-suggestion-pill"
              onClick={() => onSuggestionClick(s)}
            >
              {s}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Thread sidebar item ───────────────────────────────────────────────────
function ThreadItem({ thread, active, onClick }) {
  return (
    <div
      className={`chat-thread-item ${active ? "active" : ""}`}
      onClick={onClick}
    >
      <div className="thread-prompt">{thread.prompt || thread.message || "Untitled"}</div>
      <div className="thread-ts">{thread.timestamp}</div>
    </div>
  );
}

// ── Main ChatWorkspace ───────────────────────────────────────────────────
export default function ChatWorkspace({ onActivity }) {
  const [threadId, setThreadId]     = useState(null);
  const [messages, setMessages]     = useState([]);
  const [input, setInput]           = useState("");
  const [isLoading, setIsLoading]   = useState(false);
  const [threads, setThreads]       = useState([]);
  const [activeThreadId, setActiveThreadId] = useState(null);
  const [slashOpen, setSlashOpen]   = useState(false);
  const [slashFilter, setSlashFilter] = useState([]);

  const messagesEndRef = useRef(null);
  const textareaRef    = useRef(null);

  // Load thread history on mount
  useEffect(() => {
    fetch("/api/chat/threads")
      .then(r => r.json())
      .then(d => setThreads(d.threads || []))
      .catch(() => {});
  }, []);

  // Auto-scroll on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Auto-resize textarea
  const resizeTextarea = () => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, 120) + "px";
  };

  const handleInputChange = (e) => {
    const val = e.target.value;
    setInput(val);
    resizeTextarea();

    // Slash autocomplete
    if (val.startsWith("/") && !val.includes(" ")) {
      const q = val.toLowerCase();
      setSlashFilter(SLASH_CMDS.filter(c => c.cmd.startsWith(q)));
      setSlashOpen(true);
    } else {
      setSlashOpen(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
    if (e.key === "Escape") setSlashOpen(false);
  };

  const newChat = async () => {
    try {
      const res = await fetch("/api/chat/new", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
      const d   = await res.json();
      setThreadId(d.thread_id);
      setActiveThreadId(d.thread_id);
    } catch (_) {
      setThreadId(crypto.randomUUID());
    }
    setMessages([]);
    setInput("");
  };

  const resumeThread = (t) => {
    setActiveThreadId(t.thread_id || null);
    setThreadId(t.thread_id || null);
    setMessages([]);
  };

  const sendMessage = useCallback(async (overrideText) => {
    const text = (overrideText !== undefined ? overrideText : input).trim();
    if (!text || isLoading) return;

    setInput("");
    setSlashOpen(false);
    if (textareaRef.current) textareaRef.current.style.height = "auto";

    const tid = threadId || crypto.randomUUID();
    if (!threadId) setThreadId(tid);

    // Detect forced intent from slash command
    let forcedIntent = null;
    const matchedSlash = SLASH_CMDS.find(c => text.startsWith(c.cmd.trim()) && c.intent !== "main");
    if (matchedSlash) forcedIntent = matchedSlash.intent;

    // Append user message
    setMessages(prev => [...prev, { role: "user", content: text }]);
    setIsLoading(true);

    // Append loading placeholder
    const loadingId = Date.now();
    setMessages(prev => [...prev, { role: "agent", loading: true, id: loadingId }]);

    onActivity && onActivity({
      isRunning: true,
      label: "Mosaic Agent thinking...",
      workspaceOp: "chat",
      logs: [`> ${text.slice(0, 80)}`],
    });

    try {
      // POST returns immediately with a job_id — agent runs in background
      const res  = await fetch("/api/chat", {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ message: text, thread_id: tid, forced_intent: forcedIntent }),
      });
      const init = await res.json();

      if (!init.job_id) throw new Error(init.error || "No job_id returned");
      if (init.thread_id) setThreadId(init.thread_id);

      // Poll /api/chat/status?job=<id> every 2 s until done
      await new Promise((resolve) => {
        const poll = setInterval(async () => {
          try {
            const sr   = await fetch(`/api/chat/status?job=${init.job_id}`);
            const data = await sr.json();

            if (data.status === "done") {
              clearInterval(poll);
              if (data.thread_id) setThreadId(data.thread_id);
              fetch("/api/chat/threads").then(r => r.json()).then(d => setThreads(d.threads || [])).catch(() => {});
              setMessages(prev =>
                prev.map(m => m.id === loadingId
                  ? { role: "agent", content: data.response, intent: data.intent, suggestions: data.suggestions || [] }
                  : m
                )
              );
              resolve();
            } else if (data.status === "error") {
              clearInterval(poll);
              setMessages(prev =>
                prev.map(m => m.id === loadingId
                  ? { role: "agent", content: `⚠ Agent error: ${data.error}`, intent: "main", suggestions: [] }
                  : m
                )
              );
              resolve();
            }
            // status === "running" → keep polling
          } catch (_) {}
        }, 2000);
      });

    } catch (e) {
      setMessages(prev =>
        prev.map(m => m.id === loadingId
          ? { role: "agent", content: `⚠ Connection failed: ${e.message}`, intent: "main", suggestions: [] }
          : m
        )
      );
    }

    setIsLoading(false);
    onActivity && onActivity({ isRunning: false, label: "Chat idle", workspaceOp: null, logs: [] });
  }, [input, threadId, isLoading, onActivity]);

  const fillSlash = (cmd) => {
    setInput(cmd);
    setSlashOpen(false);
    textareaRef.current?.focus();
  };

  const isEmpty = messages.length === 0;

  return (
    <div className="chat-layout">
      {/* ── Thread Sidebar ──────────────────────────────────────────────── */}
      <div className="chat-thread-sidebar">
        <button className="chat-thread-new-btn" onClick={newChat}>
          <Plus size={12} style={{ marginRight: 6 }} />
          New Chat
        </button>

        {threads.length === 0 && (
          <div style={{ fontSize: 11, color: "var(--text-muted)", textAlign: "center", paddingTop: 20 }}>
            No past conversations
          </div>
        )}
        {[...threads].reverse().map((t, i) => (
          <ThreadItem
            key={i}
            thread={t}
            active={activeThreadId === t.thread_id}
            onClick={() => resumeThread(t)}
          />
        ))}
      </div>

      {/* ── Chat Panel ──────────────────────────────────────────────────── */}
      <div className="chat-main">

        {/* Messages */}
        <div className="chat-messages">
          {isEmpty ? (
            <div className="chat-empty-state">
              <div className="chat-empty-icon">
                <MessageSquare size={40} strokeWidth={1} color="var(--cyan)" />
              </div>
              <div className="chat-empty-title">Mosaic Agent</div>
              <div className="chat-empty-subtitle">
                Multi-turn AI with full intent routing — signals, macro, deep-dives, equity research, anomaly detection, and more. Persistent memory across sessions.
              </div>
              <div className="chat-caps-grid">
                {CAPS.map((c, i) => (
                  <div key={i} className="chat-cap-card" onClick={() => sendMessage(c.prompt)}>
                    <div className="cap-icon">{c.icon}</div>
                    <div className="cap-title">{c.title}</div>
                    <div className="cap-desc">{c.desc}</div>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            messages.map((msg, i) => (
              <ChatMessage
                key={i}
                msg={msg}
                onSuggestionClick={(s) => sendMessage(s)}
              />
            ))
          )}
          <div ref={messagesEndRef} />
        </div>

        {/* Input area */}
        <div className="chat-input-area">
          {/* Slash command quick bar */}
          <div className="chat-slash-bar">
            {SLASH_CMDS.map(c => (
              <button
                key={c.cmd}
                className="chat-slash-btn"
                title={c.hint}
                onClick={() => fillSlash(c.cmd)}
              >
                {c.label}
              </button>
            ))}
          </div>

          {/* Slash autocomplete dropdown */}
          {slashOpen && slashFilter.length > 0 && (
            <div style={{
              background: "var(--bg-card)",
              border: "1px solid var(--border-bright)",
              borderRadius: "var(--radius-md)",
              overflow: "hidden",
              marginBottom: 4,
            }}>
              {slashFilter.map((c, i) => (
                <div
                  key={i}
                  onClick={() => fillSlash(c.cmd)}
                  style={{
                    padding: "8px 14px",
                    cursor: "pointer",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    fontSize: 12,
                    color: "var(--text-secondary)",
                    borderBottom: i < slashFilter.length - 1 ? "1px solid var(--border)" : "none",
                  }}
                  onMouseEnter={e => e.currentTarget.style.background = "rgba(255,255,255,0.04)"}
                  onMouseLeave={e => e.currentTarget.style.background = "transparent"}
                >
                  <span style={{ fontFamily: "var(--font-mono)", color: "var(--cyan)" }}>{c.cmd.trim()}</span>
                  <span style={{ fontSize: 11, color: "var(--text-muted)" }}>{c.hint}</span>
                </div>
              ))}
            </div>
          )}

          <div className="chat-input-row">
            <textarea
              ref={textareaRef}
              className="chat-textarea"
              rows={1}
              value={input}
              onChange={handleInputChange}
              onKeyDown={handleKeyDown}
              placeholder="Ask anything — or use /signals, /macro, /deepdive TICKER…"
              disabled={isLoading}
            />
            <button
              className="chat-send-btn"
              onClick={() => sendMessage()}
              disabled={isLoading || !input.trim()}
            >
              {isLoading
                ? <Zap size={16} color="#000" style={{ animation: "breathing 1s infinite" }} />
                : <Send size={16} color="#000" />
              }
            </button>
          </div>

          {threadId && (
            <div style={{ fontSize: 10, color: "var(--text-muted)", fontFamily: "var(--font-mono)", paddingLeft: 2 }}>
              thread: {threadId.slice(0, 8)}…
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

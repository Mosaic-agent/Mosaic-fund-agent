import os
import sys
import json
import uuid
import threading
import contextlib
import datetime
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import webbrowser
from http.server import SimpleHTTPRequestHandler, HTTPServer
from socketserver import ThreadingTCPServer


def _json_default(obj):
    """JSON serializer for types pandas/ClickHouse return that json.dumps can't handle."""
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return str(obj)
    if hasattr(obj, "item"):  # numpy scalar
        return obj.item()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# Add project root to sys.path so we can import from src/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import subprocess

# Global process tracker and log path
IMPORT_PROCESS = None
LOG_FILE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "output", "import_run.log"))

# ── Chat checkpointer — singleton SqliteSaver kept alive for the server lifetime
_chat_exit_stack = contextlib.ExitStack()
_CHAT_CHECKPOINTER = None
_CHAT_LOCK = threading.Lock()

def _get_chat_checkpointer():
    global _CHAT_CHECKPOINTER
    if _CHAT_CHECKPOINTER is not None:
        return _CHAT_CHECKPOINTER
    with _CHAT_LOCK:
        if _CHAT_CHECKPOINTER is not None:
            return _CHAT_CHECKPOINTER
        os.makedirs("output", exist_ok=True)
        from langgraph.checkpoint.sqlite import SqliteSaver
        _CHAT_CHECKPOINTER = _chat_exit_stack.enter_context(
            SqliteSaver.from_conn_string("output/checkpoints.db")
        )
        return _CHAT_CHECKPOINTER

# ── Chat job queue — runs agent in background thread, result polled by client
_CHAT_JOBS = {}   # job_id → { status, response, intent, suggestions, error }
_JOBS_LOCK = threading.Lock()

def _run_chat_job(job_id, message, thread_id, forced_intent):
    """Run agent.chat() in a background thread; store result in _CHAT_JOBS."""
    try:
        from src.agents.mosaic_fund_agent import MosaicFundAgent
        checkpointer = _get_chat_checkpointer()
        agent = MosaicFundAgent(checkpointer=checkpointer)

        intent = forced_intent or "main"
        if not forced_intent:
            try:
                from src.agents.sub_agents import route_intent
                intent = route_intent(message)
            except Exception:
                pass

        response = agent.chat(message, thread_id=thread_id, forced_intent=forced_intent)

        suggestions = []
        try:
            from src.commands.chat_cmd import _get_suggestions
            suggestions = (_get_suggestions(message, response, intent) or [])[:3]
        except Exception:
            pass

        with _JOBS_LOCK:
            _CHAT_JOBS[job_id] = {
                "status": "done",
                "response": response,
                "intent": intent,
                "suggestions": suggestions,
                "thread_id": thread_id,
            }
    except Exception as exc:
        with _JOBS_LOCK:
            _CHAT_JOBS[job_id] = {"status": "error", "error": str(exc)}

# Memory-based history store
PROMPT_HISTORY = [
    {"prompt": "Compare Gold with USD, Inflation, Interest Rates and explain current macro regime.", "timestamp": "2026-06-28 10:15:00"},
    {"prompt": "Scan tracked ETFs for any volume-volatility setups.", "timestamp": "2026-06-28 14:32:00"},
    {"prompt": "Which ETFs in India are currently bearish based on their 5d, 20d, and 60d lookback trends?", "timestamp": "2026-06-29 09:44:00"}
]

class StudioRequestHandler(SimpleHTTPRequestHandler):
    def translate_path(self, path):
        # Serve files from website/studio/dist (React/Vite compiled output).
        # SPA fallback: any path that doesn't resolve to an existing file on disk
        # (e.g. /signals, /markets, /screener) falls back to index.html so the
        # React router handles it client-side.
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "website", "studio", "dist"))

        parsed_url = urllib.parse.urlparse(path)
        clean_path = parsed_url.path

        if clean_path == "/" or clean_path == "":
            return os.path.join(root, "index.html")

        candidate = os.path.join(root, clean_path.lstrip("/"))

        # If the candidate file exists on disk, serve it (JS/CSS/image assets).
        # Otherwise fall back to index.html for React SPA client-side routing.
        if os.path.isfile(candidate):
            return candidate
        return os.path.join(root, "index.html")

    def _send_json(self, code, data):
        body = json.dumps(data, default=_json_default).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path
        query_params = urllib.parse.parse_qs(parsed_url.query)

        # ── API Route: /api/query ─────────────────────────────────────────────
        if path == "/api/query":
            sql = query_params.get("sql", [""])[0]
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            
            if not sql.strip():
                self.wfile.write(json.dumps([]).encode("utf-8"))
                return
                
            try:
                from src.ui.app import _get_pool
                df = _get_pool().query_df(sql)
                data = df.to_dict(orient="records")
                self.wfile.write(json.dumps(data, default=_json_default).encode("utf-8"))
            except Exception as e:
                self.wfile.write(json.dumps([{"error": str(e)}]).encode("utf-8"))
            return

        # ── API Route: /api/import/status ─────────────────────────────────────
        if path == "/api/import/status":
            global IMPORT_PROCESS
            running = False
            if IMPORT_PROCESS is not None:
                running = (IMPORT_PROCESS.poll() is None)
            
            logs = ""
            if os.path.exists(LOG_FILE_PATH):
                try:
                    with open(LOG_FILE_PATH, "r") as f:
                        logs = f.read()
                except Exception as e:
                    logs = f"Error reading log file: {str(e)}"
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "running": running,
                "logs": logs
            }).encode("utf-8"))
            return

        # ── API Route: /api/history ───────────────────────────────────────────
        if path == "/api/history":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(PROMPT_HISTORY).encode("utf-8"))
            return

        # ── API Route: /api/shoonya/status ───────────────────────────────────
        if path == "/api/shoonya/status":
            try:
                from src.importer.fetchers.shoonya_fetcher import _load_cached_session
                from config.settings import settings
                user = getattr(settings, "shoonya_user_id", "")
                secret = getattr(settings, "shoonya_api_secret", "")
                configured = bool(user and secret)
                session = _load_cached_session() if configured else None
                self._send_json(200, {
                    "configured": configured,
                    "active": bool(session and session.get("susertoken")),
                    "user_id": user if configured else "",
                    "saved_at": session.get("saved_at", "") if session else "",
                })
            except Exception as exc:
                self._send_json(200, {"configured": False, "active": False, "error": str(exc)})
            return

        # ── API Route: /api/shoonya/login-url ────────────────────────────────
        if path == "/api/shoonya/login-url":
            try:
                from config.settings import settings
                user = getattr(settings, "shoonya_user_id", "")
                if not user:
                    self._send_json(400, {"status": "error", "error": "SHOONYA_USER_ID not set in .env"})
                    return
                url = f"https://api.shoonya.com/OAuthlogin/investor-entry-level/login?api_key={user}&route_to={user}"
                self._send_json(200, {"status": "ok", "url": url, "user_id": user})
            except Exception as exc:
                self._send_json(500, {"status": "error", "error": str(exc)})
            return

        # ── API Route: /api/chat/threads ──────────────────────────────────────
        if path == "/api/chat/threads":
            try:
                checkpointer = _get_chat_checkpointer()
                threads_map = {}
                for cp_tuple in checkpointer.list(None):
                    cfg = cp_tuple.config
                    tid = cfg.get("configurable", {}).get("thread_id")
                    if not tid:
                        continue
                    
                    msgs = cp_tuple.checkpoint.get("channel_values", {}).get("messages", [])
                    ts_val = cp_tuple.checkpoint.get("ts")
                    
                    if tid not in threads_map or len(msgs) > len(threads_map[tid]["messages"]):
                        threads_map[tid] = {
                            "ts": ts_val,
                            "messages": msgs
                        }
                
                threads_list = []
                for tid, data in threads_map.items():
                    ts_val = data["ts"]
                    ts_str = ""
                    if ts_val:
                        if hasattr(ts_val, "strftime"):
                            ts_str = ts_val.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            ts_str = str(ts_val).split(".")[0].replace("T", " ")
                    
                    first_prompt = "Untitled"
                    for m in data["messages"]:
                        if m.__class__.__name__ == "HumanMessage" or getattr(m, "type", None) == "human":
                            first_prompt = str(m.content)
                            if "[End of context]\n" in first_prompt:
                                first_prompt = first_prompt.split("[End of context]\n", 1)[1].strip()
                            break
                    
                    if len(first_prompt) > 80:
                        first_prompt = first_prompt[:77] + "..."
                        
                    threads_list.append({
                        "thread_id": tid,
                        "prompt": first_prompt,
                        "timestamp": ts_str
                    })
                
                threads_list.sort(key=lambda x: x["timestamp"], reverse=True)
                self._send_json(200, {"threads": threads_list[:30]})
            except Exception as exc:
                import sys
                print(f"Error fetching threads from checkpoints: {exc}", file=sys.stderr)
                self._send_json(200, {"threads": PROMPT_HISTORY[-30:]})
            return

        # ── API Route: /api/chat/messages ─────────────────────────────────────
        if path == "/api/chat/messages":
            params = urllib.parse.parse_qs(parsed_url.query)
            thread_id = params.get("thread_id", [""])[0]
            if not thread_id:
                self._send_json(400, {"status": "error", "error": "Missing thread_id parameter"})
                return
            
            try:
                checkpointer = _get_chat_checkpointer()
                config = {"configurable": {"thread_id": thread_id}}
                checkpoint_tuple = checkpointer.get(config)
                
                serialized_messages = []
                if checkpoint_tuple:
                    checkpoint = checkpoint_tuple.checkpoint
                    msgs = checkpoint.get("channel_values", {}).get("messages", [])
                    for msg in msgs:
                        role = None
                        if msg.__class__.__name__ == "HumanMessage" or getattr(msg, "type", None) == "human":
                            role = "user"
                        elif msg.__class__.__name__ == "AIMessage" or getattr(msg, "type", None) == "ai":
                            role = "agent"
                        
                        if role:
                            content = getattr(msg, "content", "")
                            if isinstance(content, list):
                                text_blocks = []
                                for block in content:
                                    if isinstance(block, dict) and block.get("type") == "text":
                                        text_blocks.append(block.get("text", ""))
                                    elif isinstance(block, str):
                                        text_blocks.append(block)
                                content = "".join(text_blocks)
                            else:
                                content = str(content)
                                
                            if "[End of context]\n" in content:
                                content = content.split("[End of context]\n", 1)[1].strip()
                                
                            serialized_messages.append({
                                "role": role,
                                "content": content,
                            })
                
                self._send_json(200, {
                    "status": "success",
                    "thread_id": thread_id,
                    "messages": serialized_messages
                })
            except Exception as exc:
                import sys
                print(f"Error loading chat messages for thread {thread_id}: {exc}", file=sys.stderr)
                self._send_json(500, {"status": "error", "error": str(exc)})
            return

        # ── API Route: /api/chat/status ───────────────────────────────────────
        if path == "/api/chat/status":
            params = urllib.parse.parse_qs(parsed_url.query)
            job_id = params.get("job", [""])[0]
            with _JOBS_LOCK:
                job = _CHAT_JOBS.get(job_id)
            if job is None:
                self._send_json(404, {"status": "not_found"})
            else:
                self._send_json(200, job)
            return

        # Default handler serves static files
        super().do_GET()

    def do_POST(self):
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path

        # ── API Route: /api/import/run ────────────────────────────────────────
        if path == "/api/import/run":
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            
            try:
                body = json.loads(post_data.decode("utf-8"))
                categories = body.get("categories", "")
                full       = body.get("full", False)
                dry_run    = body.get("dry_run", False)
                source     = body.get("source", "")  # shoonya | nse | yfinance

                # Build command args
                cmd = [sys.executable, "src/main.py", "import"]
                if categories:
                    cmd.extend(["--category", categories])
                if source and source in ("shoonya", "nse", "yfinance"):
                    cmd.extend(["--source", source])
                if full:
                    cmd.append("--full")
                if dry_run:
                    cmd.append("--dry-run")
                
                # Make sure output directory exists
                os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
                
                # Launch background process
                global IMPORT_PROCESS
                log_file = open(LOG_FILE_PATH, "w")
                IMPORT_PROCESS = subprocess.Popen(
                    cmd,
                    stdout=log_file,
                    stderr=log_file,
                    text=True
                )
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "success",
                    "pid": IMPORT_PROCESS.pid,
                    "command": " ".join(cmd)
                }).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "error",
                    "error": str(e)
                }).encode("utf-8"))
            return

        # ── API Route: /api/anomaly/scan ──────────────────────────────────────
        if path == "/api/anomaly/scan":
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            
            try:
                body = json.loads(post_data.decode("utf-8"))
                symbol = body.get("symbol", "").strip().upper()
                days = int(body.get("days", 90))
                
                # search_anomaly_events is a LangChain @tool; call .func to bypass the wrapper
                from src.tools.market.equity import search_anomaly_events
                report = search_anomaly_events.func(symbol=symbol, days=days)
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "success",
                    "report": report
                }).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "error",
                    "error": str(e)
                }).encode("utf-8"))
            return

        # ── API Route: /api/dilution/check ────────────────────────────────────
        if path == "/api/dilution/check":
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            
            try:
                body = json.loads(post_data.decode("utf-8"))
                symbol = body.get("symbol", "").strip().upper()
                
                # 1. Fetch Screener Pattern
                # get_shareholding_pattern is a LangChain @tool (StructuredTool),
                # so call .func directly to bypass the tool wrapper.
                from src.tools.earnings_scraper import get_shareholding_pattern
                shp = get_shareholding_pattern.func(symbol)
                
                # 2. Check if a dilution event occurred (look for QIP, preferential)
                # We also run a Google News check or use a prompt-like response pattern
                analysis = f"### promoter Dilution Audit: {symbol}\n\n"
                
                if "error" in shp:
                    analysis += f"⚠️ Screener.in shareholding data could not be fetched for {symbol}.\n"
                else:
                    promoter_pct = shp.get("promoter_pct", 0.0)
                    promoter_delta = shp.get("promoter_pct_qoq_delta", 0.0)
                    latest_q = shp.get("latest_quarter", "Recent")
                    
                    analysis += f"**Latest Quarter ({latest_q}) Stats:**\n"
                    analysis += f"- Promoter Holding: **{promoter_pct}%** (QoQ change: `{promoter_delta:+.2f}%`)\n"
                    analysis += f"- FII Holding: **{shp.get('fii_pct', 0.0)}%**\n"
                    analysis += f"- DII Holding: **{shp.get('dii_pct', 0.0)}%**\n\n"
                    
                    if promoter_delta < 0:
                        analysis += "🔴 **Promoter % Drop Detected.** Running dilution audit checklist:\n\n"
                        # Perform automated check for QIP or Preferential allotment news
                        query = urllib.parse.quote(f"{symbol} QIP preferential allotment debt reduction share capital")
                        url = f"https://news.google.com/rss/search?q={query}"
                        
                        dilution_hints = []
                        try:
                            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                            with urllib.request.urlopen(req, timeout=5) as response:
                                html = response.read().decode('utf-8')
                                root = ET.fromstring(html)
                                for item in root.findall('.//item')[:5]:
                                    title = item.find('title').text
                                    dilution_hints.append(title)
                        except Exception:
                            pass
                        
                        if dilution_hints:
                            analysis += "**Detected Corporate Events / Share Capital Announcements:**\n"
                            for hint in dilution_hints:
                                analysis += f"- *{hint}*\n"
                            analysis += "\n"
                        
                        analysis += "> [!IMPORTANT]\n"
                        analysis += "> **Dilution vs Sale Rule:** A promoter-% drop with **unchanged absolute share count** = dilution (QIP / preferential allotment / ESOP / M&A shares), not sale. A promoter-% drop with **lower absolute share count** = actual sale (red flag).\n"
                        analysis += f"> **Action:** Cross-reference Screener's shareholding % for {symbol} with the Annual Report's 'Equity Capital' row to confirm absolute shares outstanding before concluding a promoter sell-down.\n"
                    else:
                        analysis += "🟢 **No Promoter % Drop Detected.** Promoter holding remains stable QoQ.\n"
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "success",
                    "report": analysis,
                    "raw_data": shp
                }).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "error",
                    "error": str(e)
                }).encode("utf-8"))
            return

        # ── API Route: /api/backtest/run ──────────────────────────────────────
        if path == "/api/backtest/run":
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            
            try:
                body = json.loads(post_data.decode("utf-8"))
                symbol = body.get("symbol", "GOLDBEES").strip().upper()
                fast = int(body.get("fast", 50))
                slow = int(body.get("slow", 200))
                ma_type = body.get("ma_type", "sma").lower()
                
                # Import backtest function
                from src.scripts.market.ma_crossover_backtest import run_crossover_backtest
                metrics = run_crossover_backtest(symbol, fast, slow, ma_type, plot=False)
                
                # Format CLI text output report
                report = (
                    f"### MA Crossover Strategy Performance: {symbol}\n\n"
                    f"Parameters: {ma_type.upper()}({fast}, {slow})\n"
                    f"-----------------------------------------\n"
                    f"Total Returns:            {metrics.get('total_return_pct', 0.0):.2f}%\n"
                    f"Benchmark Returns:        {metrics.get('bench_return_pct', 0.0):.2f}%\n"
                    f"Max Drawdown:             {metrics.get('max_drawdown_pct', 0.0):.2f}%\n"
                    f"Win Rate:                 {metrics.get('win_rate_pct', 0.0):.2f}%\n"
                    f"Profit Factor:            {metrics.get('profit_factor', 1.0):.2f}\n"
                    f"Sharpe Ratio:             {metrics.get('sharpe_ratio', 0.0):.2f}\n"
                    f"Total Trades:             {metrics.get('total_trades', 0)}\n"
                    f"-----------------------------------------\n"
                    f"Verdict: Strategy {'OUTPERFORMED' if metrics.get('total_return_pct', 0.0) > metrics.get('bench_return_pct', 0.0) else 'UNDERPERFORMED'} benchmark."
                )
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "success",
                    "report": report
                }).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "error",
                    "error": str(e)
                }).encode("utf-8"))
            return

        # ── API Route: /api/agent/run ─────────────────────────────────────────
        if path == "/api/agent/run":
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            
            try:
                body = json.loads(post_data.decode("utf-8"))
                prompt = body.get("prompt", "")
                
                # Append to history
                PROMPT_HISTORY.append({
                    "prompt": prompt,
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                
                # Execute agent
                from src.agents.mosaic_fund_agent import MosaicFundAgent
                agent = MosaicFundAgent()
                response = agent.ask(prompt)

                # Extract a confidence/metadata line from the response if possible
                lines = [l.strip() for l in response.strip().splitlines() if l.strip()]
                conf_line = next(
                    (l for l in lines if any(k in l.lower() for k in ["auc", "hit ratio", "prob", "confidence", "regime", "signal"])),
                    f"Agent response generated — {len(lines)} insights"
                )

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "success",
                    "response": response,
                    "insightBody": response,
                    "insightConfidence": conf_line,
                }).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "error",
                    "error": str(e)
                }).encode("utf-8"))
            return

        # ── API Route: /api/shoonya/authenticate ─────────────────────────────
        if path == "/api/shoonya/authenticate":
            content_length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_length).decode("utf-8"))
            auth_code = body.get("code", "").strip()

            if not auth_code:
                self._send_json(400, {"status": "error", "error": "OAuth code is required"})
                return
            try:
                from config.settings import settings
                from src.importer.fetchers.shoonya_fetcher import _save_session

                user   = getattr(settings, "shoonya_user_id", "")
                secret = getattr(settings, "shoonya_api_secret", "")

                if not all([user, secret]):
                    self._send_json(400, {"status": "error", "error": "SHOONYA_USER_ID / SHOONYA_API_SECRET not configured in .env"})
                    return

                from NorenRestApiPy.NorenApi import NorenApi  # type: ignore

                class ShoonyaApiPy(NorenApi):
                    def __init__(self):
                        NorenApi.__init__(
                            self,
                            host="https://api.shoonya.com/NorenWClientAPI",
                            websocket="wss://api.shoonya.com/NorenWSAPI/",
                        )

                user_clean = user.replace("_U", "")
                api    = ShoonyaApiPy()
                result = api.getAccessToken(
                    authcode=auth_code,
                    Secret_Code=secret,
                    client_id=user,
                    UID=user_clean,
                )

                if result is not None:
                    asc_tok, usrid, ref_tok, actid = result
                    susertoken = getattr(api, "_NorenApi__susertoken", asc_tok)
                    _save_session({
                        "susertoken": susertoken,
                        "access_token": asc_tok,
                        "userid": usrid,
                        "accountid": actid,
                    })
                    self._send_json(200, {
                        "status": "success",
                        "message": f"Authenticated as {usrid}",
                        "user_id": usrid,
                        "account_id": actid,
                    })
                else:
                    self._send_json(400, {"status": "error", "error": "Token generation failed — check the OAuth code and try again"})
            except ImportError:
                self._send_json(500, {"status": "error", "error": "NorenRestApiPy not installed — pip install NorenRestApiPy"})
            except Exception as exc:
                self._send_json(500, {"status": "error", "error": str(exc)})
            return

        # ── API Route: /api/chat/new ──────────────────────────────────────────
        if path == "/api/chat/new":
            self._send_json(200, {"thread_id": str(uuid.uuid4())})
            return

        # ── API Route: /api/chat ──────────────────────────────────────────────
        # Non-blocking: spawns a background thread and returns a job_id immediately.
        # Client polls GET /api/chat/status?job=<id> until status is "done"/"error".
        if path == "/api/chat":
            content_length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_length).decode("utf-8"))
            message       = body.get("message", "").strip()
            thread_id     = body.get("thread_id") or str(uuid.uuid4())
            forced_intent = body.get("forced_intent")

            if not message:
                self._send_json(400, {"status": "error", "error": "Empty message"})
                return

            # Record in history
            PROMPT_HISTORY.append({
                "prompt": message[:80],
                "thread_id": thread_id,
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

            # Create job, mark running, spawn thread
            job_id = str(uuid.uuid4())
            with _JOBS_LOCK:
                _CHAT_JOBS[job_id] = {"status": "running", "thread_id": thread_id}

            t = threading.Thread(
                target=_run_chat_job,
                args=(job_id, message, thread_id, forced_intent),
                daemon=True,
            )
            t.start()

            # Return immediately — client will poll /api/chat/status?job=<job_id>
            self._send_json(200, {
                "status":    "running",
                "job_id":    job_id,
                "thread_id": thread_id,
            })
            return

        # ── API Route: /api/etf-scanner/run ──────────────────────────────────
        if path == "/api/etf-scanner/run":
            content_length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_length).decode("utf-8"))
            lookback_days = int(body.get("lookback_days", 30))
            z_threshold   = float(body.get("z_threshold", 1.5))
            min_snapshots = int(body.get("min_snapshots", 3))
            symbols       = body.get("symbols", [])
            try:
                from src.tools.domestic_etf_scanner import scan_domestic_etfs, DOMESTIC_ETF_SYMBOLS
                from src.ui.app import _get_pool
                sym_list = symbols if symbols else DOMESTIC_ETF_SYMBOLS
                with _get_pool().acquire() as ch_client:
                    results = scan_domestic_etfs(
                        ch_client=ch_client,
                        symbols=sym_list,
                        lookback_days=lookback_days,
                        z_high=z_threshold,
                        z_low=-z_threshold,
                        z_mild_high=z_threshold - 0.5,
                        z_mild_low=-(z_threshold - 0.5),
                        min_snapshots=min_snapshots,
                    )
                clean = []
                for r in results:
                    clean.append({k: (float(v) if isinstance(v, float) else v) for k, v in r.items()})
                self._send_json(200, {"status": "success", "results": clean})
            except Exception as exc:
                self._send_json(500, {"status": "error", "error": str(exc)})
            return

        self.send_error(404, "Not Found")

def run_server(host="localhost", port=8502):
    server_address = (host, port)
    # Using ThreadingTCPServer to avoid blocking the main thread during agent runs
    class ThreadedHTTPServer(ThreadingTCPServer, HTTPServer):
        allow_reuse_address = True

    httpd = ThreadedHTTPServer(server_address, StudioRequestHandler)
    print(f"Mosaic Studio Workspace active at http://{host}:{port}/")
    webbrowser.open(f"http://{host}:{port}/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down Mosaic Studio Server...")
        httpd.shutdown()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Mosaic Fund Agent Studio workspace server.")
    parser.add_argument("--port", type=int, default=8502, help="Port to serve the UI on.")
    parser.add_argument("--host", type=str, default="localhost", help="Host address to bind to.")
    args = parser.parse_args()
    run_server(host=args.host, port=args.port)

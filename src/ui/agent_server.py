import os
import sys
import json
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import webbrowser
from http.server import SimpleHTTPRequestHandler, HTTPServer
from socketserver import ThreadingTCPServer

# Add project root to sys.path so we can import from src/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import subprocess

# Global process tracker and log path
IMPORT_PROCESS = None
LOG_FILE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "output", "import_run.log"))

# Memory-based history store
PROMPT_HISTORY = [
    {"prompt": "Compare Gold with USD, Inflation, Interest Rates and explain current macro regime.", "timestamp": "2026-06-28 10:15:00"},
    {"prompt": "Scan tracked ETFs for any volume-volatility setups.", "timestamp": "2026-06-28 14:32:00"},
    {"prompt": "Which ETFs in India are currently bearish based on their 5d, 20d, and 60d lookback trends?", "timestamp": "2026-06-29 09:44:00"}
]

class StudioRequestHandler(SimpleHTTPRequestHandler):
    def translate_path(self, path):
        # Override translate_path to serve files from the website/studio/dist directory (React compiled Vite app)
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "website", "studio", "dist"))
        
        # Strip query parameters for local file resolution
        parsed_url = urllib.parse.urlparse(path)
        clean_path = parsed_url.path
        
        if clean_path == "/" or clean_path == "":
            clean_path = "/index.html"
            
        return os.path.join(root, clean_path.lstrip("/"))

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
                # Use pool singleton from app.py
                from src.ui.app import _get_pool
                df = _get_pool().query_df(sql)
                # Parse datetime columns to string to avoid JSON serialisation errors
                for col in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
                    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
                for col in df.select_dtypes(include=["date"]).columns:
                    df[col] = df[col].astype(str)
                    
                data = df.to_dict(orient="records")
                self.wfile.write(json.dumps(data).encode("utf-8"))
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
                full = body.get("full", False)
                dry_run = body.get("dry_run", False)
                
                # Build command args
                cmd = [sys.executable, "src/main.py", "import"]
                if categories:
                    cmd.extend(["--category", categories])
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
                    "status": "started",
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
                
                # Call search_anomaly_events
                from src.tools.market.equity import search_anomaly_events
                report = search_anomaly_events(symbol=symbol, days=days)
                
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
                from src.tools.earnings_scraper import get_shareholding_pattern
                shp = get_shareholding_pattern(symbol)
                
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
                from datetime import datetime
                PROMPT_HISTORY.append({
                    "prompt": prompt,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                
                # Execute agent
                from src.agents.mosaic_fund_agent import MosaicFundAgent
                agent = MosaicFundAgent()
                response = agent.ask(prompt)
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "success",
                    "response": response
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

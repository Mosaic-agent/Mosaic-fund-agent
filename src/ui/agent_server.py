import os
import sys
import json
import urllib.parse
import webbrowser
from http.server import SimpleHTTPRequestHandler, HTTPServer
from socketserver import ThreadingTCPServer

# Add project root to sys.path so we can import from src/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Memory-based history store
PROMPT_HISTORY = [
    {"prompt": "Compare Gold with USD, Inflation, Interest Rates and explain current macro regime.", "timestamp": "2026-06-28 10:15:00"},
    {"prompt": "Scan tracked ETFs for any volume-volatility setups.", "timestamp": "2026-06-28 14:32:00"},
    {"prompt": "Which ETFs in India are currently bearish based on their 5d, 20d, and 60d lookback trends?", "timestamp": "2026-06-29 09:44:00"}
]

class StudioRequestHandler(SimpleHTTPRequestHandler):
    def translate_path(self, path):
        # Override translate_path to serve files from the website/ directory
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "website"))
        
        # Strip query parameters for local file resolution
        parsed_url = urllib.parse.urlparse(path)
        clean_path = parsed_url.path
        
        if clean_path == "/" or clean_path == "":
            clean_path = "/app.html"
            
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

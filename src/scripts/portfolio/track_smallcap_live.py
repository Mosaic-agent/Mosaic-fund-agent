from __future__ import annotations
import sys, time, threading, argparse, os, select, termios, tty
from pathlib import Path
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

import clickhouse_connect
from src.data_importer.fetchers.shoonya_fetcher import get_shoonya_api

console = Console(color_system="truecolor", force_terminal=True)

SPINNER_FRAMES = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
PULSE_FRAMES = ['🟢 LIVE', '❇️ LIVE', '🟢 LIVE', '✳️ LIVE']

class TraderTerminalHUD:
    def __init__(self, limit: int = 100, page: int = 1, page_size: int = 15, sort_by: str = "gainers", filter_str: str = ""):
        self.limit = limit
        self.current_page = page
        self.page_size = page_size
        self.sort_by = sort_by
        self.filter_str = filter_str.upper().strip()
        self.total_pages = 1
        self.api = get_shoonya_api()
        self.tokens_map = {}
        self.live_state = {}
        self.alerts_log = []
        self._lock = threading.Lock()
        self._ticks_count = 0
        self._start_time = time.time()
        self._total_duration = 3600 # 1 hour default
        self._frame_idx = 0
        self._running = True

    def resolve_tokens(self):
        cache_file = Path("output/.cache/smallcap_tokens.json")
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        if cache_file.exists():
            import json
            try:
                with open(cache_file, "r") as f:
                    cached_data = json.load(f)
                if len(cached_data) >= 80:
                    self.tokens_map = cached_data
                    for tok in self.tokens_map:
                        self.live_state[tok] = {"ltp": 0.0, "pc": 0.0, "vol": 0, "vwap": 0.0, "open": 0.0, "high": 0.0, "low": 0.0}
                    console.print(f"[bold green]✓ Loaded {len(self.tokens_map)} Small Cap instruments (Instant Startup)![/bold green]")
                    return
            except Exception:
                pass

        console.print("[bold cyan]🔍 Ingesting institutional Small Cap basket & mapping Shoonya NSE tokens...[/bold cyan]")
        client = clickhouse_connect.get_client(host="localhost", port=8123, database="market_data")
        q = f"""
        SELECT security_name, any(isin) as isin, count(DISTINCT fund_name) as fund_count, round(sum(market_value_cr), 2) as total_val_cr
        FROM market_data.mf_holdings FINAL
        WHERE lower(asset_type) = 'equity'
          AND fund_name IN ('NIPPON_INDIA_SMALL_CAP_FUND', 'HDFC_SMALL_CAP', 'DSP_SMALL_CAP', 'QUANT_SMALL_CAP', 'ICICI_SMALLCAP', 'KOTAK_SMALL_CAP', 'BAJAJ_FINSERV_SMALL_CAP_FUND', 'Axis Small Cap Fund', 'Motilal Oswal Small Cap Fund', 'Abakkus Small Cap Fund', 'Canara Robeco Small Cap Fund', 'Mirae Asset Small Cap Fund', 'Invesco India Smallcap Fund', 'Helios Small Cap Fund', 'RELIANCE_SMALL_CAP_FUND')
          AND security_name != '' AND security_name NOT LIKE '%TREPS%' AND security_name NOT LIKE '%Repo%'
          AND security_name NOT IN ('HDFC Bank Limited', 'State Bank of India', 'ICICI Bank Limited')
        GROUP BY security_name
        ORDER BY fund_count DESC, total_val_cr DESC
        LIMIT {self.limit}
        """
        df = client.query_df(q)
        for idx, r in df.iterrows():
            sec_name = r["security_name"]
            kw = sec_name.split()[0].replace(",", "").replace(".", "").upper()
            try:
                res = self.api.searchscrip(exchange="NSE", searchtext=kw)
                if res and res.get("stat") == "Ok" and res.get("values"):
                    val = next((v for v in res["values"] if v.get("instname") == "EQ" or v.get("tsym", "").endswith("-EQ")), res["values"][0])
                    tok = val.get("token")
                    sym = val.get("tsym", "").replace("-EQ", "")
                    self.tokens_map[tok] = {"symbol": sym, "company": sec_name[:24], "funds": r["fund_count"], "val_cr": r["total_val_cr"]}
                    self.live_state[tok] = {"ltp": 0.0, "pc": 0.0, "vol": 0, "vwap": 0.0, "open": 0.0, "high": 0.0, "low": 0.0}
            except Exception:
                continue
        try:
            import json
            with open(cache_file, "w") as f:
                json.dump(self.tokens_map, f, indent=2)
        except Exception:
            pass
        console.print(f"[bold green]✓ Ready! Tracking {len(self.tokens_map)} Small Cap instruments simultaneously.[/bold green]")

    def on_tick(self, tick):
        if not tick or "tk" not in tick:
            return
        tok = tick["tk"]
        if tok not in self.tokens_map:
            return
        with self._lock:
            self._ticks_count += 1
            st = self.live_state[tok]
            prev_ltp = st["ltp"]
            if "lp" in tick: st["ltp"] = float(tick["lp"])
            if "pc" in tick: st["pc"] = float(tick["pc"])
            if "v" in tick: st["vol"] = int(tick["v"])
            if "ap" in tick: st["vwap"] = float(tick["ap"])
            if "o" in tick: st["open"] = float(tick["o"])
            if "h" in tick: st["high"] = float(tick["h"])
            if "l" in tick: st["low"] = float(tick["l"])

            sym = self.tokens_map[tok]["symbol"]
            if st["pc"] >= 5.0 and prev_ltp > 0 and st["ltp"] > prev_ltp:
                msg = f"🚀 [bold cyan]{sym}[/bold cyan] surges to [bold green]₹{st['ltp']:,.2f}[/bold green] ([bold green]+{st['pc']:.2f}%[/bold green]) | VWAP: ₹{st['vwap']:.2f}"
                if not any(sym in a for a in self.alerts_log[-3:]):
                    self.alerts_log.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def get_signal_badge(self, st: dict) -> str:
        ltp, vwap, pc, open_p = st["ltp"], st["vwap"], st["pc"], st["open"]
        if ltp == 0:
            return "[dim]—[/dim]"
        if pc >= 5.0 and ltp >= vwap:
            return "[bold black on bright_green] 🚀 BREAKOUT [/]"
        elif open_p > 0 and abs(open_p - st["low"]) <= 0.05 and pc > 1.0:
            return "[bold white on green] 🐂 OPEN=LOW [/]"
        elif vwap > 0 and ltp > vwap and pc > 2.0:
            return "[bold green] 🔥 STRONG BUY[/]"
        elif vwap > 0 and abs(ltp - vwap) / vwap <= 0.003:
            return "[bold cyan] 🎯 AT VWAP [/]"
        elif pc <= -3.0 and ltp < vwap:
            return "[bold white on red] 🔻 BREAKDOWN [/]"
        elif ltp < vwap and pc < -1.0:
            return "[bold red] ⚠️ WEAK [/]"
        elif pc > 0:
            return "[green] ↗ MILD UP [/]"
        else:
            return "[dim] 💤 RANGING [/]"

    def get_range_bar(self, low: float, high: float, ltp: float, width: int = 8) -> str:
        if high <= low or ltp <= 0:
            return "[dim]—[/dim]"
        pos = min(1.0, max(0.0, (ltp - low) / (high - low)))
        idx = int(pos * (width - 1))
        bar = list("─" * width)
        bar[idx] = "●"
        color = "green" if pos >= 0.7 else ("red" if pos <= 0.3 else "yellow")
        return f"[{color}]{''.join(bar)}[/{color}] [dim]{int(pos*100)}%[/dim]"

    def render_dashboard(self) -> Layout:
        self._frame_idx = (self._frame_idx + 1) % len(SPINNER_FRAMES)
        spinner = SPINNER_FRAMES[self._frame_idx]
        pulse = PULSE_FRAMES[self._frame_idx % len(PULSE_FRAMES)]

        elapsed = time.time() - self._start_time
        pct_done = min(1.0, elapsed / self._total_duration if self._total_duration > 0 else 1.0)
        time_bar = f"[cyan]{'█' * int(15 * pct_done)}[/cyan][dim]{'░' * (15 - int(15 * pct_done))}[/dim] {int(pct_done * 100)}%"

        with self._lock:
            adv = sum(1 for s in self.live_state.values() if s["pc"] > 0 and s["ltp"] > 0)
            dec = sum(1 for s in self.live_state.values() if s["pc"] < 0 and s["ltp"] > 0)
            unch = sum(1 for s in self.live_state.values() if s["pc"] == 0 and s["ltp"] > 0)
            tot_turnover = sum((s["vol"] * s["vwap"]) for s in self.live_state.values()) / 1e7

            items = list(self.tokens_map.items())
            if self.filter_str:
                items = [(tok, info) for tok, info in items if self.filter_str in info["symbol"].upper() or self.filter_str in info["company"].upper()]
            if self.sort_by == "gainers":
                items.sort(key=lambda x: self.live_state[x[0]]["pc"], reverse=True)
            elif self.sort_by == "losers":
                items.sort(key=lambda x: self.live_state[x[0]]["pc"])
            elif self.sort_by == "volume":
                items.sort(key=lambda x: self.live_state[x[0]]["vol"], reverse=True)
            elif self.sort_by == "turnover":
                items.sort(key=lambda x: self.live_state[x[0]]["vol"] * self.live_state[x[0]]["vwap"], reverse=True)

            total_items = len(items)
            self.total_pages = max(1, (total_items + self.page_size - 1) // self.page_size)
            if self.current_page > self.total_pages: self.current_page = 1
            if self.current_page < 1: self.current_page = self.total_pages
            start_idx = (self.current_page - 1) * self.page_size
            end_idx = min(start_idx + self.page_size, total_items)
            page_items = items[start_idx:end_idx]

        top_l1 = f"[bold white]{spinner} SHOONYA TRADER PRO HUD[/bold white] │ {pulse} │ [bold white]Session:[/bold white] [cyan]FN203617[/cyan] │ [bold white]Tracked:[/bold white] [green]{len(self.tokens_map)} Stocks[/green] │ [bold white]Turnover:[/bold white] [bold green]₹{tot_turnover:,.1f} Cr[/bold green]"
        top_l2 = f"[bold white]Market Breadth:[/bold white] [bold green]▲ {adv} Adv[/bold green]  [bold red]▼ {dec} Dec[/bold red]  [dim]■ {unch} Flat[/dim] │ [bold white]Session:[/bold white] {time_bar} │ [bold yellow]PAGE {self.current_page}/{self.total_pages} (Stocks {start_idx+1}–{end_idx})[/bold yellow]"
        header_panel = Panel(f"{top_l1}\n{top_l2}", border_style="bright_blue", padding=(0, 1))



        # Main Table
        t = Table(expand=True, box=None, padding=(0, 1))
        t.add_column("#", justify="right", style="dim", width=3)
        t.add_column("Symbol", style="bold cyan", width=11)
        t.add_column("LTP (₹)", justify="right", style="bold green", width=11)
        t.add_column("Change (%)", justify="right", width=10)
        t.add_column("Signal / Action", justify="center", width=16)
        t.add_column("VWAP (₹)", justify="right", style="white", width=11)
        t.add_column("VWAP Spread", justify="center", width=11)
        t.add_column("Day Range Gauge", justify="center", width=16)
        t.add_column("Volume / Turnover", justify="right", style="yellow", width=18)
        t.add_column("Consensus", justify="center", style="magenta", width=8)

        for idx, (tok, info) in enumerate(page_items, start=start_idx + 1):
            st = self.live_state[tok]
            ret = st["pc"]
            ret_str = f"[bold green]+{ret:.2f}%[/bold green]" if ret > 0 else (f"[bold red]{ret:.2f}%[/bold red]" if ret < 0 else "0.00%")
            sig_badge = self.get_signal_badge(st)
            
            vwap_spread = round(((st["ltp"] - st["vwap"]) / st["vwap"]) * 100, 2) if st["vwap"] > 0 else 0.0
            spread_str = f"[green]▲ +{vwap_spread:.1f}%[/green]" if vwap_spread > 0 else (f"[red]▼ {vwap_spread:.1f}%[/red]" if vwap_spread < 0 else "[dim]0.0%[/dim]")
            
            range_bar = self.get_range_bar(st["low"], st["high"], st["ltp"])
            turnover_cr = round((st["vol"] * st["vwap"]) / 1e7, 1) if st["vwap"] > 0 else 0.0
            vol_turnover_str = f"{st['vol']:,} [dim](₹{turnover_cr}Cr)[/dim]" if st["vol"] > 0 else "—"

            t.add_row(
                str(idx),
                info["symbol"],
                f"₹{st['ltp']:,.2f}" if st["ltp"] > 0 else "—",
                ret_str if st["ltp"] > 0 else "—",
                sig_badge,
                f"₹{st['vwap']:,.2f}" if st["vwap"] > 0 else "—",
                spread_str if st["ltp"] > 0 else "—",
                range_bar,
                vol_turnover_str,
                f"{info['funds']} AMCs"
            )

        # Keyboard Navigation Footer
        foot_l1 = f"🚨 [bold yellow]LATEST TRIGGER:[/bold yellow] {self.alerts_log[-1] if self.alerts_log else '[dim]Monitoring for volume/breakout spikes...[/dim]'}"
        foot_l2 = "[bold cyan][N] Next Page[/bold cyan] │ [bold cyan][P] Prev Page[/bold cyan] │ [bold cyan][1-7] Direct Page[/bold cyan] │ [bold yellow][G] Gainers[/bold yellow] │ [bold red][L] Losers[/bold red] │ [bold magenta][V] Volume[/bold magenta] │ [bold green][C] Consensus[/bold green] │ [bold red][Q] Quit[/bold red]"
        footer_panel = Panel(f"{foot_l1}\n{foot_l2}", border_style="dim", padding=(0, 1))


        layout = Layout()
        layout.split_column(
            Layout(header_panel, size=4),
            Layout(Panel(t, title=f"[bold yellow]⚡ ACTIVE WATCHLIST [PAGE {self.current_page}/{self.total_pages} | {start_idx+1}–{end_idx} OF {total_items} | SORT: {self.sort_by.upper()}] [/bold yellow]", border_style="yellow")),
            Layout(footer_panel, size=4)
        )
        return layout

    def keyboard_listener(self):
        """Non-blocking terminal hotkey listener."""
        if not sys.stdin.isatty():
            return
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            while self._running:
                r, _, _ = select.select([sys.stdin], [], [], 0.1)
                if r:
                    ch = sys.stdin.read(1)
                    if ch in ('n', 'N', ' '): # Next Page or Spacebar
                        self.current_page = (self.current_page % self.total_pages) + 1
                    elif ch in ('p', 'P'): # Prev Page
                        self.current_page = self.total_pages if self.current_page <= 1 else self.current_page - 1
                    elif ch in ('1', '2', '3', '4', '5', '6', '7'): # Direct Page Jump
                        target_p = int(ch)
                        if target_p <= self.total_pages:
                            self.current_page = target_p
                    elif ch in ('g', 'G'): # Sort by Gainers
                        self.sort_by = "gainers"
                    elif ch in ('l', 'L'): # Sort by Losers
                        self.sort_by = "losers"
                    elif ch in ('v', 'V'): # Sort by Volume
                        self.sort_by = "volume"
                    elif ch in ('t', 'T'): # Sort by Turnover
                        self.sort_by = "turnover"
                    elif ch in ('c', 'C'): # Sort by MF Consensus
                        self.sort_by = "consensus"
                    elif ch in ('q', 'Q', ''): # Quit on 'q' or Ctrl+C
                        self._running = False
                        break
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    def start(self, duration_sec: int = 3600):
        self._total_duration = duration_sec
        self._start_time = time.time()
        self.resolve_tokens()
        sub_keys = [f"NSE|{tok}" for tok in self.tokens_map.keys()]

        def on_open():
            console.print(f"[bold green]✓ Shoonya WebSocket connected! Subscribed to {len(sub_keys)} Small Cap tokens simultaneously.[/bold green]")
            self.api.subscribe(sub_keys)

        self.api.start_websocket(subscribe_callback=self.on_tick, socket_open_callback=on_open)

        # Start background non-blocking keyboard listener thread
        kb_thread = threading.Thread(target=self.keyboard_listener, daemon=True)
        kb_thread.start()

        with Live(self.render_dashboard(), refresh_per_second=4, console=console) as live:
            t_end = time.time() + duration_sec
            while self._running and time.time() < t_end:
                time.sleep(0.2)
                live.update(self.render_dashboard())

        self._running = False
        self.api.close_websocket()
        console.print(f"\n[bold yellow]🔌 WebSocket session closed. Total ticks captured: {self._ticks_count:,}[/bold yellow]\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trader Keyboard-Interactive Live Small Cap Shoonya WebSocket Monitor")
    parser.add_argument("--limit", type=int, default=100, help="Number of consensus stocks to track")
    parser.add_argument("--duration", type=int, default=3600, help="Streaming duration in seconds (default: 3600s = 1 hour)")
    parser.add_argument("--page", type=int, default=1, help="Starting page number")
    parser.add_argument("--page-size", type=int, default=15, help="Stocks per page")
    parser.add_argument("--sort", type=str, default="gainers", choices=["consensus", "gainers", "losers", "volume", "turnover"], help="Initial sort order")
    parser.add_argument("--filter", "--symbol", type=str, default="", help="Filter by symbol")
    args = parser.parse_args()

    tracker = TraderTerminalHUD(
        limit=args.limit,
        page=args.page,
        page_size=args.page_size,
        sort_by=args.sort,
        filter_str=args.filter
    )
    tracker.start(duration_sec=args.duration)

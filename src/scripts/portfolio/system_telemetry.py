"""
src/scripts/portfolio/system_telemetry.py
─────────────────────────────────────────
Live system telemetry dashboard for the Mosaic platform.
Displays host resources, local Ollama engine status, docker containers,
ClickHouse database stats, SQLite semantic cache, and external API caches.

Run:
  ALLOW_LOCAL_RUN=1 python src/scripts/portfolio/system_telemetry.py [--live] [--prompt "hello"]
"""
import os
import re
import sys
import json
import time
import shutil
import platform
import subprocess
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.columns import Columns
from rich.layout import Layout
from config.settings import settings

console = Console()

def get_mac_memory() -> dict:
    """Gets host RAM usage on macOS via system commands."""
    try:
        # Total memory
        total_bytes = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"]).strip())
        total_gb = total_bytes / (1024 ** 3)

        # Free/active memory via vm_stat
        vm_output = subprocess.check_output(["vm_stat"]).decode("utf-8")
        page_size = 4096  # default page size on macOS
        
        free_pages = 0
        active_pages = 0
        speculative_pages = 0
        
        for line in vm_output.splitlines():
            if "Pages free:" in line:
                free_pages = int(line.split()[-1].strip("."))
            elif "Pages active:" in line:
                active_pages = int(line.split()[-1].strip("."))
            elif "Pages speculative:" in line:
                speculative_pages = int(line.split()[-1].strip("."))
                
        free_gb = ((free_pages + speculative_pages) * page_size) / (1024 ** 3)
        used_gb = total_gb - free_gb
        
        return {
            "total": round(total_gb, 1),
            "used": round(used_gb, 1),
            "free": round(free_gb, 1),
            "pct": round((used_gb / total_gb) * 100, 1),
            "platform": "macOS (Host System)"
        }
    except Exception:
        return {"total": "N/A", "used": "N/A", "free": "N/A", "pct": "N/A", "platform": "macOS"}

def get_linux_memory() -> dict:
    """Gets RAM usage on Linux systems (including cgroups checks for Docker containers)."""
    try:
        meminfo = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    name = parts[0].strip()
                    val_str = parts[1].strip().split()[0]
                    meminfo[name] = int(val_str)
        
        total_kb = meminfo.get("MemTotal", 0)
        available_kb = meminfo.get("MemAvailable", 0)
        
        total_gb = total_kb / (1024 * 1024)
        free_gb = available_kb / (1024 * 1024)
        
        # Check cgroups limits (Docker Container memory limits)
        limit_bytes = None
        usage_bytes = None
        
        # cgroups v2
        cgroup2_max = Path("/sys/fs/cgroup/memory.max")
        cgroup2_current = Path("/sys/fs/cgroup/memory.current")
        # cgroups v1
        cgroup1_limit = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        cgroup1_usage = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
        
        if cgroup2_max.exists() and cgroup2_current.exists():
            try:
                max_val = cgroup2_max.read_text().strip()
                curr_val = cgroup2_current.read_text().strip()
                if max_val != "max":
                    limit_bytes = int(max_val)
                    usage_bytes = int(curr_val)
            except Exception:
                pass
        elif cgroup1_limit.exists() and cgroup1_usage.exists():
            try:
                limit_bytes = int(cgroup1_limit.read_text().strip())
                usage_bytes = int(cgroup1_usage.read_text().strip())
            except Exception:
                pass
                
        if limit_bytes and limit_bytes < total_kb * 1024:
            total_gb = limit_bytes / (1024 ** 3)
            used_gb = usage_bytes / (1024 ** 3)
            free_gb = total_gb - used_gb
            return {
                "total": round(total_gb, 1),
                "used": round(used_gb, 1),
                "free": round(free_gb, 1),
                "pct": round((used_gb / total_gb) * 100, 1),
                "platform": "Linux (Docker Container)"
            }
            
        used_gb = total_gb - free_gb
        return {
            "total": round(total_gb, 1),
            "used": round(used_gb, 1),
            "free": round(free_gb, 1),
            "pct": round((used_gb / total_gb) * 100, 1),
            "platform": "Linux (Host/VM)"
        }
    except Exception:
        return {"total": "N/A", "used": "N/A", "free": "N/A", "pct": "N/A", "platform": "Linux"}

def get_system_memory() -> dict:
    """Gets host RAM usage depending on platform."""
    if platform.system() == "Darwin":
        return get_mac_memory()
    else:
        return get_linux_memory()

def get_ollama_api_status() -> dict:
    """Fetches local Ollama status via Ollama REST API endpoints."""
    url = settings.llm_base_url
    status = {
        "model": "None Loaded",
        "engine": "Offline / Unreachable",
        "peak_mem": "N/A",
        "last_request": "N/A",
        "cache_hit_pct": "N/A",
        "port": "11434",
        "status_msg": "Offline"
    }
    if not url:
        return status
        
    port_match = re.search(r":(\d+)", url)
    if port_match:
        status["port"] = port_match.group(1)
        
    if "/v1" in url:
        ollama_url = url.replace("/v1", "/api/ps")
    else:
        ollama_url = url.rstrip("/") + "/api/ps"
        
    try:
        req = urllib.request.Request(ollama_url, method="GET")
        with urllib.request.urlopen(req, timeout=2.0) as response:
            data = json.loads(response.read().decode("utf-8"))
            models = data.get("models", [])
            status["status_msg"] = "Online"
            if models:
                model_names = [m.get("name", "Unknown") for m in models]
                status["model"] = ", ".join(model_names)
                
                model_info = models[0]
                details = model_info.get("details", {})
                param_size = details.get("parameter_size", "")
                quant = details.get("quantization_level", "")
                engine_str = "Ollama"
                if param_size:
                    engine_str += f" {param_size}"
                if quant:
                    engine_str += f" ({quant})"
                status["engine"] = engine_str
                
                vram = model_info.get("size_vram", 0)
                if vram > 0:
                    status["peak_mem"] = f"{round(vram / (1024 ** 3), 2)} GB VRAM"
                else:
                    size = model_info.get("size", 0)
                    status["peak_mem"] = f"{round(size / (1024 ** 3), 2)} GB (RAM)"
            else:
                status["model"] = "None Loaded (Idle)"
                status["engine"] = "Ollama Engine"
    except Exception as e:
        status["status_msg"] = "Unreachable"
        status["engine"] = "Ollama Offline"
    return status

def get_ollama_status() -> dict:
    """Reads local Ollama engine status via API and fallback to local server.log."""
    if settings.llm_local_disabled or not settings.is_local_model:
        return {
            "model": f"{settings.llm_model} ({settings.llm_provider.upper()})",
            "engine": f"Cloud Provider: {settings.llm_provider.upper()}",
            "peak_mem": "N/A",
            "last_request": "N/A",
            "cache_hit_pct": "N/A",
            "port": "N/A",
            "status_msg": "Cloud (Local Disabled)"
        }

    status = get_ollama_api_status()
    
    # Try reading server.log on host
    log_path = Path.home() / ".ollama" / "logs" / "server.log"
    if log_path.exists():
        try:
            content = log_path.read_text()
            lines = content.splitlines()
            
            # Detect Last Completion Time
            for line in reversed(lines):
                if "POST /v1/completions" in line or "POST /v1/chat/completions" in line:
                    took_match = re.search(r"took=([0-9a-zA-Z\.\s\µ]+)", line)
                    status_match = re.search(r"status=\"([^\"]+)\"", line)
                    if took_match and status_match:
                        status["last_request"] = f"{took_match.group(1)} ({status_match.group(1)})"
                        break
                        
            # Detect Cache Hits
            for line in reversed(lines):
                if "cache hit" in line:
                    tot_match = re.search(r"total=(\d+)", line)
                    match_match = re.search(r"matched=(\d+)", line)
                    if tot_match and match_match:
                        total = int(tot_match.group(1))
                        matched = int(match_match.group(1))
                        if total > 0:
                            status["cache_hit_pct"] = f"{round((matched / total) * 100, 1)}% ({matched}/{total} tokens)"
                            break
        except Exception:
            pass
            
    return status

def get_host_ollama_resources() -> dict:
    """Gets CPU and Memory usage of Ollama processes on the host macOS/Linux."""
    # First, try to read from shared host_telemetry.json (populated by host wrapper)
    db_path = Path(__file__).parent.parent.parent / "host_telemetry.json"
    if db_path.exists():
        try:
            with open(db_path, "r") as f:
                data = json.load(f)
                ts = data.get("timestamp", 0)
                # If fresh (within 2 minutes), return it
                if time.time() - ts < 120:
                    return data
        except Exception:
            pass

    res = {"cpu": 0.0, "mem": 0.0, "pids": []}
    try:
        cmd = ["ps", "-ax", "-o", "pid,%cpu,%mem,comm"]
        output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode("utf-8")
        
        total_cpu = 0.0
        total_mem = 0.0
        pids = []
        
        for line in output.splitlines():
            line_lower = line.lower()
            if "ollama" in line_lower and "grep" not in line_lower and "system_telemetry" not in line_lower:
                parts = line.strip().split()
                if len(parts) >= 4:
                    try:
                        pid = int(parts[0])
                        cpu = float(parts[1])
                        mem = float(parts[2])
                        total_cpu += cpu
                        total_mem += mem
                        pids.append(pid)
                    except ValueError:
                        pass
        if pids:
            res["cpu"] = round(total_cpu, 1)
            res["mem"] = round(total_mem, 1)
            res["pids"] = pids
    except Exception:
        pass
    return res

def get_docker_stats() -> tuple[list[dict], str | None]:
    """Gets CPU and Memory stats for running Docker containers."""
    try:
        if not shutil.which("docker"):
            return [], "Docker CLI not installed in this environment"
            
        cmd = ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"]
        output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=3.0).decode("utf-8").strip()
        if not output:
            return [], "No running containers found"
            
        containers = []
        for line in output.splitlines():
            parts = line.split("\t")
            if len(parts) == 3:
                containers.append({
                    "name": parts[0],
                    "cpu": parts[1],
                    "mem": parts[2]
                })
        return containers, None
    except subprocess.TimeoutExpired:
        return [], "Query timed out (is Docker daemon running?)"
    except Exception as e:
        return [], f"Failed to connect to Docker daemon: {e}"

def get_clickhouse_stats() -> list[dict]:
    """Fetches row counts and memory sizes of ClickHouse tables."""
    try:
        from src.db.pool import get_pool
        client = get_pool().get_client()
        query = """
            SELECT 
                name, 
                formatReadableSize(total_bytes) AS size, 
                total_rows AS rows
            FROM system.tables 
            WHERE database = 'market_data' AND total_rows > 0
            ORDER BY total_bytes DESC
        """
        rows = client.query(query).result_rows
        client.close()
        return [{"table": r[0], "size": r[1], "rows": r[2]} for r in rows]
    except Exception:
        return []

def get_llm_cache_stats() -> dict:
    """Gets SQLite database size and count for llm_cache."""
    db_path = Path(settings.output_dir) / ".cache" / "llm_cache.db"
    if not db_path.exists():
        return {"size": "0 B", "entries": 0, "status": "No Cache File"}
        
    size_bytes = db_path.stat().st_size
    size_formatted = f"{round(size_bytes / 1024, 1)} kB" if size_bytes < 1024 * 1024 else f"{round(size_bytes / (1024 * 1024), 2)} MB"
    
    entries = 0
    status = "Active"
    try:
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM llm_cache")
        entries = cur.fetchone()[0]
        conn.close()
    except Exception as e:
        status = f"Error: {e}"
        
    return {"size": size_formatted, "entries": entries, "status": status}

def get_external_api_cache_stats() -> dict:
    """Gets total files and size of cached API responses."""
    cache_dir = Path(settings.output_dir) / ".cache"
    if not cache_dir.exists():
        return {"count": 0, "size": "0 B", "files": []}
        
    json_files = list(cache_dir.glob("*.json"))
    total_size = sum(f.stat().st_size for f in json_files)
    size_formatted = f"{round(total_size / 1024, 1)} kB" if total_size < 1024 * 1024 else f"{round(total_size / (1024 * 1024), 2)} MB"
    
    files_list = []
    for f in json_files:
        name = f.stem
        # Skip temporary config stats
        if name == "last_query_telemetry":
            continue
        age = time.time() - f.stat().st_mtime
        if age < 60:
            age_formatted = f"{int(age)}s ago"
        elif age < 3600:
            age_formatted = f"{int(age // 60)}m ago"
        else:
            age_formatted = f"{round(age / 3600, 1)}h ago"
        files_list.append((name, age_formatted, f.stat().st_size, age))
        
    # Sort files by age (newest first)
    files_list.sort(key=lambda x: x[3])
    
    return {
        "count": len(json_files),
        "size": size_formatted,
        "files": [(f[0], f[1], f[2]) for f in files_list]
    }

def get_last_query_telemetry() -> dict:
    """Reads the token speed and count metrics of the last LLM query."""
    db_path = Path(settings.output_dir) / ".cache" / "last_query_telemetry.json"
    if not db_path.exists():
        return {
            "completion_tokens": "N/A",
            "prompt_tokens": "N/A",
            "total_tokens": "N/A",
            "elapsed_seconds": "N/A",
            "token_speed": "N/A",
            "model_name": "N/A",
            "timestamp": None
        }
    try:
        with open(db_path, "r") as f:
            data = json.load(f)
            # If timestamp is older than 1 hour, mark it as stale
            stale = False
            ts = data.get("timestamp")
            if ts:
                age = time.time() - ts
                if age > 3600:
                    stale = True
            data["stale"] = stale
            return data
    except Exception:
        return {
            "completion_tokens": "N/A",
            "prompt_tokens": "N/A",
            "total_tokens": "N/A",
            "elapsed_seconds": "N/A",
            "token_speed": "N/A",
            "model_name": "N/A",
            "timestamp": None
        }

def run_test_prompt(prompt: str):
    """Executes a prompt using the configured LLM and displays response time/cache hit status."""
    from src.utils.llm_cache import setup_llm_cache
    setup_llm_cache()
    
    provider = settings.llm_provider.lower()
    llm = None
    
    console.print(f"\n[bold magenta]🚀 Dispatching Prompt to active LLM client...[/bold magenta]")
    console.print(f"[bold dim]Prompt: \"{prompt}\"[/bold dim]\n")
    
    start_time = time.time()
    try:
        if provider == "openrouter":
            from langchain_openai import ChatOpenAI
            llm = ChatOpenAI(
                model=settings.llm_model,
                base_url="https://openrouter.ai/api/v1",
                api_key=settings.openrouter_api_key,
                temperature=0,
                max_tokens=settings.llm_token_budget,
            )
        elif settings.llm_base_url:
            from langchain_openai import ChatOpenAI
            llm = ChatOpenAI(
                model=settings.llm_model,
                base_url=settings.llm_base_url,
                api_key=settings.openai_api_key or "local",
                temperature=0,
                max_tokens=settings.llm_token_budget,
                extra_body={"options": {"num_ctx": settings.llm_context_window}} if "ollama" in settings.llm_base_url or "localhost" in settings.llm_base_url else None
            )
        elif provider == "anthropic":
            from langchain_anthropic import ChatAnthropic
            llm = ChatAnthropic(
                model=settings.llm_model,
                api_key=settings.anthropic_api_key,
                temperature=0,
                max_tokens=settings.llm_token_budget
            )
        else:
            from langchain_openai import ChatOpenAI
            llm = ChatOpenAI(
                model=settings.llm_model,
                api_key=settings.openai_api_key,
                temperature=0,
                max_tokens=settings.llm_token_budget
            )
            
        if llm:
            from langchain_core.messages import HumanMessage
            response = llm.invoke([HumanMessage(content=prompt)])
            elapsed = time.time() - start_time
            
            # Capture token usage
            token_usage = response.response_metadata.get("token_usage", {})
            completion_tokens = token_usage.get("completion_tokens", 0)
            prompt_tokens = token_usage.get("prompt_tokens", 0)
            total_tokens = token_usage.get("total_tokens", 0)
            token_speed = completion_tokens / elapsed if elapsed > 0 else 0
            
            stats = {
                "completion_tokens": completion_tokens,
                "prompt_tokens": prompt_tokens,
                "total_tokens": total_tokens,
                "elapsed_seconds": round(elapsed, 3),
                "token_speed": round(token_speed, 1),
                "model_name": settings.llm_model,
                "timestamp": time.time()
            }
            
            cache_dir = Path(settings.output_dir) / ".cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            with open(cache_dir / "last_query_telemetry.json", "w") as f:
                json.dump(stats, f)
            
            # Print response panel
            console.print(Panel(
                response.content,
                title=f"[bold green]LLM Response (took {round(elapsed, 3)} seconds)[/bold green]",
                border_style="green"
            ))
        else:
            console.print("[bold red]Error: Could not initialize LLM client.[/bold red]")
            
    except Exception as e:
        console.print(f"[bold red]LLM Execution Failed: {e}[/bold red]")

def get_dashboard_renderable() -> Group:
    """Assembles all telemetry widgets into a Group of Panels/Tables."""
    mem = get_system_memory()
    ollama = get_ollama_status()
    containers, container_err = get_docker_stats()
    ch_stats = get_clickhouse_stats()
    llm_cache = get_llm_cache_stats()
    api_cache = get_external_api_cache_stats()
    
    # ── 1. Host Resources Panel
    host_table = Table.grid(expand=True)
    host_table.add_column(ratio=1)
    host_table.add_column(ratio=1)
    
    ram_used_str = f"{mem['used']} GB ({mem['pct']}%"
    if mem['pct'] != "N/A":
        ram_used_str += ")"
    else:
        ram_used_str = "N/A"
        
    host_table.add_row(
        f"[bold cyan]RAM Installed:[/bold cyan] {mem['total']} GB",
        f"[bold cyan]RAM Used:[/bold cyan] {ram_used_str}"
    )
    host_table.add_row(
        f"[bold cyan]RAM Free:[/bold cyan] {mem['free']} GB",
        f"[bold cyan]Platform OS:[/bold cyan] {mem['platform']}"
    )
    host_panel = Panel(host_table, title="[bold white]💻 Host System Status[/bold white]", border_style="cyan")
    
    # ── 2. Ollama Status Panel
    ollama_table = Table.grid(expand=True)
    ollama_table.add_column(ratio=4)
    ollama_table.add_column(ratio=6)
    
    # Get last query telemetry
    lq = get_last_query_telemetry()
    lq_speed = f"{lq['token_speed']} tok/s" if lq['token_speed'] != "N/A" else "N/A"
    lq_tokens = f"{lq['completion_tokens']} output / {lq['prompt_tokens']} input" if lq['completion_tokens'] != "N/A" else "N/A"
    lq_time = f"{lq['elapsed_seconds']}s" if lq['elapsed_seconds'] != "N/A" else "N/A"
    
    # Get host processes resources
    host_proc = get_host_ollama_resources()
    proc_str = f"CPU: {host_proc['cpu']}%  RAM: {host_proc['mem']}% (Host PIDs: {', '.join(map(str, host_proc['pids']))})" if host_proc['pids'] else "CPU/RAM: N/A (Docker container)"
    
    ollama_table.add_row(
        f"[bold green]Active Model:[/bold green] [bold white]{ollama['model']}[/bold white]",
        f"[bold green]Inference Engine:[/bold green] {ollama['engine']}"
    )
    ollama_table.add_row(
        f"[bold green]Active Port:[/bold green] {ollama['port']} ({ollama.get('status_msg', 'N/A')})",
        f"[bold green]Host Process Load:[/bold green] {proc_str}"
    )
    ollama_table.add_row(
        f"[bold green]Last Prompt Hit Rate:[/bold green] {ollama['cache_hit_pct']}",
        f"[bold green]Last Query Latency:[/bold green] {ollama['last_request']}"
    )
    ollama_table.add_row(
        f"[bold green]Last Prompt Speed:[/bold green] [bold yellow]{lq_speed}[/bold yellow] ({lq_time})",
        f"[bold green]Last Token Count:[/bold green] {lq_tokens}"
    )
    
    ollama_panel = Panel(ollama_table, title="[bold white]🤖 Local Ollama Inference Telemetry[/bold white]", border_style="green")
    
    # ── 3. Docker Containers Table
    docker_table = Table(title="🐳 Active Docker Containers (via Host Daemon)", border_style="blue", show_header=True, expand=True)
    docker_table.add_column("Container Name")
    docker_table.add_column("CPU %", justify="right")
    docker_table.add_column("Memory Usage", justify="right")
    
    if containers:
        for c in containers:
            docker_table.add_row(c["name"], c["cpu"], c["mem"])
    else:
        err_msg = container_err or "No active containers statistics found"
        docker_table.add_row(f"[dim]{err_msg}[/dim]", "0.0%", "0 B")
        
    # ── 4. ClickHouse & Cache Panel
    db_table = Table(title="🗄️ ClickHouse Database Tables (market_data)", border_style="yellow", show_header=True, expand=True)
    db_table.add_column("Table Name")
    db_table.add_column("Rows", justify="right")
    db_table.add_column("Data Size", justify="right")
    
    if ch_stats:
        for t in ch_stats[:8]:  # Top 8 tables
            db_table.add_row(t["table"], f"{t['rows']:,}", t["size"])
    else:
        db_table.add_row("[dim]Could not query ClickHouse database[/dim]", "0", "0 B")
        
    # ── 5. SQLite & API Cache info
    cache_grid = Table.grid(expand=True)
    cache_grid.add_column(ratio=1)
    cache_grid.add_column(ratio=1)
    cache_grid.add_row(
        f"[bold yellow]SQLite LLM Cache Size:[/bold yellow] {llm_cache['size']}",
        f"[bold yellow]Total Cache Entries:[/bold yellow] {llm_cache['entries']}"
    )
    cache_grid.add_row(
        f"[bold yellow]SQLite LLM Cache Status:[/bold yellow] {llm_cache['status']}",
        f"[bold yellow]API Disk Cache Files:[/bold yellow] {api_cache['count']} ({api_cache['size']})"
    )
    cache_panel = Panel(cache_grid, title="[bold white]⚡ Local Caching Infrastructure (SQLite & Disk)[/bold white]", border_style="yellow")

    # ── 6. Disk API Cache Files Table
    api_table = Table(title="🔌 Disk-Cached API Responses (Google News, COMEX, etc.)", border_style="yellow", show_header=True, expand=True)
    api_table.add_column("Cache Key (Filename)")
    api_table.add_column("Last Updated", justify="right")
    api_table.add_column("Size", justify="right")
    
    if api_cache["files"]:
        for name, age_str, size_bytes in api_cache["files"][:5]:
            size_formatted = f"{round(size_bytes / 1024, 1)} kB" if size_bytes < 1024 * 1024 else f"{round(size_bytes / (1024 * 1024), 2)} MB"
            api_table.add_row(name, age_str, size_formatted)
    else:
        api_table.add_row("[dim]No disk-cached API responses found[/dim]", "N/A", "0 B")

    # Main dashboard header with dynamic time
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = Panel(
        f"[bold white]MOSAIC SYSTEM TELEMETRY & INFERENCE MONITOR[/bold white]\n"
        f"[dim]Live updates as of: {now_str} IST | Hostname: {platform.node()}[/dim]",
        border_style="bold magenta",
        expand=True
    )
    
    return Group(
        header,
        host_panel,
        ollama_panel,
        docker_table,
        db_table,
        cache_panel,
        api_table
    )

def get_compact_telemetry_renderable() -> Panel:
    """Assembles a compact, 1-line panel of live telemetry stats for chat overlay."""
    try:
        mem = get_system_memory()
        ollama = get_ollama_status()
        llm_cache = get_llm_cache_stats()
        api_cache = get_external_api_cache_stats()
        lq = get_last_query_telemetry()
        
        # Build text string
        ram_str = f"[cyan]RAM: {mem['used']}/{mem['total']} GB ({mem['pct']}%)[/cyan]"
        
        model_name = ollama['model']
        if len(model_name) > 25:
            model_name = model_name[:22] + "..."
        model_str = f"[green]LLM: {model_name}[/green]"
        
        speed_val = lq.get("token_speed", "N/A")
        speed_str = f" [yellow]({speed_val} tok/s)[/yellow]" if speed_val != "N/A" else ""
        cache_str = f"[yellow]Cache: {llm_cache['entries']}{speed_str}[/yellow]"
        
        api_str = f"[magenta]API: {api_cache['count']} cached[/magenta]"
        
        content = f"{ram_str}  •  {model_str}  •  {cache_str}  •  {api_str}"
        return Panel(content, title="[bold dim white]⚡ System Telemetry Overlay[/bold dim white]", border_style="dim white", expand=True)
    except Exception as e:
        return Panel(f"[red]Error fetching telemetry: {e}[/red]", title="⚡ System Telemetry Overlay", border_style="dim red")

def render_dashboard(live: bool = False, prompt: str = None):
    """Renders the dashboard either once or continuously in live mode."""
    if prompt:
        run_test_prompt(prompt)
        
    if live:
        from rich.live import Live
        console.clear()
        with Live(get_dashboard_renderable(), console=console, refresh_per_second=0.5) as live_display:
            try:
                while True:
                    time.sleep(2.0)
                    live_display.update(get_dashboard_renderable())
            except KeyboardInterrupt:
                console.print("\n[bold yellow]Telemetry monitor stopped.[/bold yellow]")
    else:
        if prompt:
            time.sleep(1.0)
        console.clear()
        console.print(get_dashboard_renderable())

if __name__ == "__main__":
    # Allow running with python src/scripts/portfolio/system_telemetry.py --live --prompt "hello"
    live_mode = "--live" in sys.argv or "-l" in sys.argv
    
    prompt_val = None
    for idx, arg in enumerate(sys.argv):
        if arg in ("--prompt", "-p") and idx + 1 < len(sys.argv):
            prompt_val = sys.argv[idx + 1]
            break
            
    render_dashboard(live=live_mode, prompt=prompt_val)

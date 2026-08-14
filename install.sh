#!/usr/bin/env bash
# ── Mosaic Fund Agent — Unified Docker Installer ────────────────────────────
#
# One script to rule them all: detects Docker, runs pre-flight checks,
# configures .env, builds/launches the full stack, and prints a service map.
#
# Usage:
#   chmod +x install.sh
#   ./install.sh
#
# Safe to run multiple times (idempotent). Never deletes existing data volumes.
# Works on Linux and macOS.

set -euo pipefail

# ── CLI Flags ────────────────────────────────────────────────────────────────
AUTO_YES=false
for arg in "$@"; do
  case "$arg" in
    -y|--yes) AUTO_YES=true ;;
  esac
done

# ── Constants ────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

MIN_DOCKER_VERSION="20.10"
MIN_DISK_MB=5120  # ~5 GB

# Ports used by the compose stack (host side)
declare -A PORT_MAP=(
  [8123]="ClickHouse HTTP"
  [9000]="ClickHouse Native"
  [6333]="Qdrant REST"
  [6334]="Qdrant gRPC"
  [8501]="Streamlit UI"
  [8502]="Report Server"
  [11434]="Ollama"
)

# ── Color / Formatting Helpers ───────────────────────────────────────────────
# Uses tput when available; falls back to raw ANSI codes; disables color
# entirely when stdout is not a terminal (piped / redirected).

setup_colors() {
  if [[ -t 1 ]] && command -v tput &>/dev/null && [[ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]]; then
    BOLD="$(tput bold 2>/dev/null || true)"
    RED="$(tput setaf 1 2>/dev/null || true)"
    GREEN="$(tput setaf 2 2>/dev/null || true)"
    YELLOW="$(tput setaf 3 2>/dev/null || true)"
    BLUE="$(tput setaf 4 2>/dev/null || true)"
    MAGENTA="$(tput setaf 5 2>/dev/null || true)"
    CYAN="$(tput setaf 6 2>/dev/null || true)"
    WHITE="$(tput setaf 7 2>/dev/null || true)"
    DIM="$(tput dim 2>/dev/null || true)"
    RESET="$(tput sgr0 2>/dev/null || true)"
  elif [[ -t 1 ]]; then
    BOLD="\033[1m"
    RED="\033[31m"
    GREEN="\033[32m"
    YELLOW="\033[33m"
    BLUE="\033[34m"
    MAGENTA="\033[35m"
    CYAN="\033[36m"
    WHITE="\033[37m"
    DIM="\033[2m"
    RESET="\033[0m"
  else
    BOLD="" RED="" GREEN="" YELLOW="" BLUE="" MAGENTA="" CYAN="" WHITE="" DIM="" RESET=""
  fi
}
setup_colors

# ── Logging Helpers ──────────────────────────────────────────────────────────

STEP=0

step() {
  STEP=$((STEP + 1))
  printf "\n${BOLD}${BLUE}━━━ Step %d: %s ━━━${RESET}\n" "$STEP" "$1"
}

ok()   { printf "  ${GREEN}✔${RESET} %s\n" "$1"; }
warn() { printf "  ${YELLOW}⚠${RESET} %s\n" "$1"; }
err()  { printf "  ${RED}✖${RESET} %s\n" "$1"; }
info() { printf "  ${CYAN}ℹ${RESET} %s\n" "$1"; }

die() {
  err "$1"
  exit 1
}

# ── Spinner ──────────────────────────────────────────────────────────────────
# Runs a command in the background and shows a spinner while it executes.
# Usage: spin "message" command arg1 arg2 ...

SPIN_PID=""

_cleanup_spinner() {
  if [[ -n "${SPIN_PID:-}" ]]; then
    kill "$SPIN_PID" 2>/dev/null || true
    wait "$SPIN_PID" 2>/dev/null || true
    SPIN_PID=""
    printf "\r\033[K"  # clear spinner line
  fi
}
trap '_cleanup_spinner' EXIT INT TERM

spin() {
  local msg="$1"; shift
  local frames=("⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏")

  # Run the actual command, capturing output to a temp file
  local tmpfile
  tmpfile="$(mktemp)"

  "$@" > "$tmpfile" 2>&1 &
  local cmd_pid=$!

  # Animate spinner
  (
    local i=0
    while kill -0 "$cmd_pid" 2>/dev/null; do
      printf "\r  ${CYAN}%s${RESET} %s" "${frames[$((i % ${#frames[@]}))]}" "$msg"
      i=$((i + 1))
      sleep 0.1
    done
  ) &
  SPIN_PID=$!

  # Wait for the real command
  local exit_code=0
  wait "$cmd_pid" || exit_code=$?

  # Stop spinner
  kill "$SPIN_PID" 2>/dev/null || true
  wait "$SPIN_PID" 2>/dev/null || true
  SPIN_PID=""
  printf "\r\033[K"  # clear spinner line

  if [[ $exit_code -eq 0 ]]; then
    ok "$msg"
  else
    err "$msg (exit code $exit_code)"
    # Show last 20 lines of output on failure for debugging
    printf "${DIM}"
    tail -20 "$tmpfile" | sed 's/^/    /'
    printf "${RESET}\n"
  fi

  rm -f "$tmpfile"
  return $exit_code
}

# ── OS Detection ─────────────────────────────────────────────────────────────

detect_os() {
  case "$(uname -s)" in
    Linux*)  HOST_OS="linux" ;;
    Darwin*) HOST_OS="macos" ;;
    *)       HOST_OS="unknown" ;;
  esac
}
detect_os

# ── Banner ───────────────────────────────────────────────────────────────────

print_banner() {
  printf "${BOLD}${MAGENTA}"
  cat << 'EOF'

  ╔══════════════════════════════════════════════════════════════╗
  ║                                                              ║
  ║   🪙  Mosaic Fund Agent — Docker Installer                   ║
  ║                                                              ║
  ║   Unified setup: Docker → Pre-flight → .env → Build → Run   ║
  ║                                                              ║
  ╚══════════════════════════════════════════════════════════════╝

EOF
  printf "${RESET}"
  info "Host OS: ${BOLD}${HOST_OS}${RESET}  |  $(date '+%Y-%m-%d %H:%M:%S')"
}
print_banner

# ═════════════════════════════════════════════════════════════════════════════
#  STEP 1 — Docker Detection
# ═════════════════════════════════════════════════════════════════════════════

step "Docker Detection"

# 1a. Check if docker binary exists
if ! command -v docker &>/dev/null; then
  err "Docker is not installed."
  printf "\n"
  printf "  ${BOLD}Install Docker:${RESET}\n"
  printf "  ${CYAN}Linux (apt):${RESET}   sudo apt update && sudo apt install -y docker.io docker-compose-v2\n"
  printf "  ${CYAN}Linux (dnf):${RESET}   sudo dnf install -y docker docker-compose-plugin\n"
  printf "  ${CYAN}Linux (pacman):${RESET} sudo pacman -S docker docker-compose\n"
  printf "  ${CYAN}macOS (brew):${RESET}  brew install --cask docker\n"
  printf "  ${CYAN}Windows:${RESET}       winget install Docker.DockerDesktop\n"
  printf "\n"
  printf "  Or visit: ${BOLD}https://docs.docker.com/get-docker/${RESET}\n"
  die "Please install Docker and re-run this script."
fi
ok "Docker binary found: $(command -v docker)"

# 1b. Check if Docker daemon is running
if ! docker info &>/dev/null; then
  err "Docker daemon is not running."
  printf "\n"
  if [[ "$HOST_OS" == "linux" ]]; then
    printf "  ${BOLD}Start Docker:${RESET}\n"
    printf "    sudo systemctl start docker\n"
    printf "    sudo systemctl enable docker   ${DIM}# start on boot${RESET}\n"
  elif [[ "$HOST_OS" == "macos" ]]; then
    printf "  ${BOLD}Start Docker:${RESET}\n"
    printf "    Open Docker Desktop from Applications, or:\n"
    printf "    open -a Docker\n"
  fi
  die "Please start the Docker daemon and re-run this script."
fi
ok "Docker daemon is running"

# 1c. Check docker compose v2 (not legacy docker-compose)
if docker compose version &>/dev/null; then
  COMPOSE_VERSION="$(docker compose version --short 2>/dev/null || docker compose version 2>/dev/null)"
  ok "Docker Compose v2: ${COMPOSE_VERSION}"
else
  err "docker compose (v2) is not available."
  info "The legacy 'docker-compose' (v1) is not supported."
  if [[ "$HOST_OS" == "linux" ]]; then
    info "Install: sudo apt install docker-compose-v2  (or equivalent)"
  fi
  die "Please install Docker Compose v2 and re-run this script."
fi

# 1d. Print Docker version info
DOCKER_VERSION="$(docker version --format '{{.Server.Version}}' 2>/dev/null || docker version 2>/dev/null | head -2)"
ok "Docker Engine: ${DOCKER_VERSION}"

# ═════════════════════════════════════════════════════════════════════════════
#  STEP 2 — Pre-flight Checks
# ═════════════════════════════════════════════════════════════════════════════

step "Pre-flight Checks"

# 2a. Minimum Docker version check
check_docker_version() {
  local version
  version="$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo "0.0")"
  # Strip anything after the patch number (e.g. "-ce", "+azure")
  version="${version%%[-+]*}"

  local major minor
  IFS='.' read -r major minor _ <<< "$version"
  major="${major:-0}"
  minor="${minor:-0}"

  local req_major req_minor
  IFS='.' read -r req_major req_minor <<< "$MIN_DOCKER_VERSION"

  if (( major > req_major )) || { (( major == req_major )) && (( minor >= req_minor )); }; then
    ok "Docker version ${version} ≥ ${MIN_DOCKER_VERSION}"
    return 0
  else
    err "Docker version ${version} < ${MIN_DOCKER_VERSION} (minimum required)"
    die "Please upgrade Docker and re-run this script."
  fi
}
check_docker_version

# 2b. Disk space check
check_disk_space() {
  local avail_kb
  if [[ "$HOST_OS" == "macos" ]]; then
    avail_kb=$(df -k "$SCRIPT_DIR" | awk 'NR==2 {print $4}')
  else
    avail_kb=$(df -k "$SCRIPT_DIR" | awk 'NR==2 {print $4}')
  fi
  local avail_mb=$((avail_kb / 1024))
  local avail_gb=$(awk "BEGIN {printf \"%.1f\", $avail_mb / 1024}")

  if (( avail_mb >= MIN_DISK_MB )); then
    ok "Disk space: ${avail_gb} GB available (need ~5 GB)"
  else
    warn "Low disk space: ${avail_gb} GB available (recommend ≥ 5 GB)"
    warn "Images and data may not fit. Proceed with caution."
  fi
}
check_disk_space

# 2c. Port availability checks
check_ports() {
  local port_warnings=0
  for port in "${!PORT_MAP[@]}"; do
    local service="${PORT_MAP[$port]}"
    local in_use=false
    local occupant=""

    # Try multiple methods to detect port usage
    if command -v ss &>/dev/null; then
      if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
        in_use=true
        occupant="$(ss -tlnp 2>/dev/null | grep ":${port} " | awk '{print $NF}' | head -1)"
      fi
    elif command -v lsof &>/dev/null; then
      if lsof -iTCP:"$port" -sTCP:LISTEN -P -n &>/dev/null; then
        in_use=true
        occupant="$(lsof -iTCP:"$port" -sTCP:LISTEN -P -n 2>/dev/null | awk 'NR==2 {print $1 " (PID " $2 ")"}' )"
      fi
    elif command -v netstat &>/dev/null; then
      if netstat -tlnp 2>/dev/null | grep -q ":${port} "; then
        in_use=true
      fi
    fi

    if $in_use; then
      if [[ -n "$occupant" ]]; then
        warn "Port ${port} (${service}) is in use by: ${occupant}"
      else
        warn "Port ${port} (${service}) is already in use"
      fi
      port_warnings=$((port_warnings + 1))
    else
      ok "Port ${port} (${service}) is free"
    fi
  done

  if (( port_warnings > 0 )); then
    printf "\n"
    info "Some ports are in use. This is fine if you already run those services"
    info "locally (e.g. a host Ollama or ClickHouse). Docker will fail to bind"
    info "only if the port is occupied by a ${BOLD}different${RESET} process."
  fi
}
check_ports

# ═════════════════════════════════════════════════════════════════════════════
#  STEP 3 — Environment Setup (.env)
# ═════════════════════════════════════════════════════════════════════════════

step "Environment Setup"

find_python() {
  if command -v python3 &>/dev/null; then
    echo "python3"
  elif command -v python &>/dev/null; then
    echo "python"
  else
    echo ""
  fi
}

PYTHON_CMD="$(find_python)"

if [[ ! -f .env ]]; then
  info "No .env file found — first-time setup required."
  printf "\n"

  if [[ -n "$PYTHON_CMD" ]]; then
    info "Launching setup wizard…"
    printf "\n"
    "$PYTHON_CMD" setup_wizard.py
    printf "\n"
    if [[ -f .env ]]; then
      ok "Configuration saved to .env"
    else
      # Wizard was cancelled or failed — fall back to template
      warn "Wizard did not create .env — copying from .env.example"
      cp .env.example .env
      warn "Please edit .env manually and add your API keys."
    fi
  elif [[ -f .env.example ]]; then
    warn "Python not found — cannot run interactive setup wizard."
    cp .env.example .env
    warn "Created .env from template. Please edit it and add your API keys."
  else
    die "No .env, no .env.example, and no Python available. Cannot configure."
  fi
else
  ok "Found existing .env file"

  # Show a quick summary of key settings
  printf "\n  ${DIM}─── Current Configuration ───${RESET}\n"
  while IFS='=' read -r key value; do
    # Skip comments and blank lines
    [[ "$key" =~ ^#.*$ || -z "$key" ]] && continue
    # Strip surrounding whitespace
    key="$(echo "$key" | xargs)"
    value="$(echo "$value" | xargs)"
    # Only show interesting keys, mask sensitive values
    case "$key" in
      LLM_PROVIDER|LLM_MODEL|LLM_BASE_URL)
        printf "  ${CYAN}%-20s${RESET} %s\n" "$key" "$value"
        ;;
      *API_KEY*|*SECRET*)
        if [[ -n "$value" && "$value" != "your_"* ]]; then
          masked="${value:0:6}…${value: -4}"
          printf "  ${CYAN}%-20s${RESET} %s\n" "$key" "$masked"
        else
          printf "  ${CYAN}%-20s${RESET} ${DIM}(not set)${RESET}\n" "$key"
        fi
        ;;
    esac
  done < .env
  printf "  ${DIM}─────────────────────────────${RESET}\n\n"

  # Ask if user wants to reconfigure (skip when non-interactive / no TTY / --yes)
  reconfigure="n"
  if ! $AUTO_YES && [[ -t 0 ]]; then
    printf "  ${YELLOW}?${RESET} Reconfigure .env? (y/N): "
    read -r reconfigure 2>/dev/null || reconfigure="n"
    reconfigure="${reconfigure:-n}"
  else
    info "Keeping existing .env (auto)"
  fi

  if [[ "${reconfigure,,}" == "y" ]]; then
    if [[ -n "$PYTHON_CMD" ]]; then
      "$PYTHON_CMD" setup_wizard.py
      ok "Configuration updated"
    else
      warn "Python not available — please edit .env manually."
    fi
  else
    ok "Keeping existing configuration"
  fi
fi

# ═════════════════════════════════════════════════════════════════════════════
#  STEP 4 — Build & Launch
# ═════════════════════════════════════════════════════════════════════════════

step "Build & Launch Containers"

# Ensure output/reports exists so the file server works from the start
mkdir -p output/reports 2>/dev/null || true

# 4a. Pull external images first (shows download progress natively)
info "Pulling base images (this may take a while on first run)…"
printf "\n"
docker compose pull clickhouse qdrant ollama 2>&1 | sed 's/^/    /'
printf "\n"
ok "Base images pulled"

# 4b. Build the app image
info "Building Mosaic application image…"
printf "\n"
docker compose build --progress=plain 2>&1 | tail -30 | sed 's/^/    /'
printf "\n"
ok "Application image built"

# 4c. Start all services
info "Starting services…"
printf "\n"
docker compose up -d 2>&1 | sed 's/^/    /'
printf "\n"
ok "Containers started"

# 4d. Wait for ClickHouse healthcheck
info "Waiting for ClickHouse to become healthy…"
HEALTH_TIMEOUT=120
ELAPSED=0
while (( ELAPSED < HEALTH_TIMEOUT )); do
  CH_STATUS="$(docker inspect --format='{{.State.Health.Status}}' "$(docker compose ps -q clickhouse 2>/dev/null)" 2>/dev/null || echo "unknown")"
  if [[ "$CH_STATUS" == "healthy" ]]; then
    break
  fi
  printf "\r  ${CYAN}⏳${RESET} ClickHouse: ${YELLOW}%s${RESET} (%ds / %ds)  " "$CH_STATUS" "$ELAPSED" "$HEALTH_TIMEOUT"
  sleep 3
  ELAPSED=$((ELAPSED + 3))
done
printf "\r\033[K"
if [[ "$CH_STATUS" == "healthy" ]]; then
  ok "ClickHouse is healthy"
else
  warn "ClickHouse health status: ${CH_STATUS} (timed out after ${HEALTH_TIMEOUT}s)"
  warn "The service may still be starting. Check: docker compose logs clickhouse"
fi

# 4e. Wait for Qdrant to accept connections
info "Waiting for Qdrant to accept connections…"
ELAPSED=0
QDRANT_READY=false
while (( ELAPSED < 60 )); do
  if curl -sf http://localhost:6333/healthz &>/dev/null || curl -sf http://localhost:6333/ &>/dev/null; then
    QDRANT_READY=true
    break
  fi
  printf "\r  ${CYAN}⏳${RESET} Qdrant: ${YELLOW}waiting${RESET} (%ds / 60s)  " "$ELAPSED"
  sleep 2
  ELAPSED=$((ELAPSED + 2))
done
printf "\r\033[K"
if $QDRANT_READY; then
  ok "Qdrant is ready"
else
  warn "Qdrant did not respond within 60s — it may still be starting."
fi

# 4f. Initialize database schemas and metadata RAG in Qdrant
info "Initializing database schemas and metadata RAG…"
printf "\n"
docker compose run --rm --entrypoint python mosaic src/scripts/db_metadata_init.py 2>&1 | sed 's/^/    /'
DB_INIT_EXIT=$?
printf "\n"
if [[ $DB_INIT_EXIT -eq 0 ]]; then
  ok "Database initialization complete"
else
  warn "Database initialization exited with code ${DB_INIT_EXIT}"
  warn "You can retry later: docker compose run --rm --entrypoint python mosaic src/scripts/db_metadata_init.py"
fi

# ═════════════════════════════════════════════════════════════════════════════
#  STEP 5 — Service Status & Port Discovery
# ═════════════════════════════════════════════════════════════════════════════

step "Service Status"

# Collect container info
get_container_status() {
  local svc="$1"
  local container_id
  container_id="$(docker compose ps -q "$svc" 2>/dev/null || echo "")"

  if [[ -z "$container_id" ]]; then
    echo "not_found|||"
    return
  fi

  local name state health
  name="$(docker inspect --format='{{.Name}}' "$container_id" 2>/dev/null | sed 's/^\///')"
  state="$(docker inspect --format='{{.State.Status}}' "$container_id" 2>/dev/null || echo "unknown")"

  # Health status (if healthcheck defined)
  health="$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}' "$container_id" 2>/dev/null || echo "n/a")"

  if [[ "$health" != "n/a" ]]; then
    echo "${state}|${health}|${name}"
  else
    echo "${state}||${name}"
  fi
}

colorize_status() {
  local status="$1"
  case "$status" in
    healthy|running)  printf "${GREEN}%s${RESET}" "$status" ;;
    starting)         printf "${YELLOW}%s${RESET}" "$status" ;;
    unhealthy|exited) printf "${RED}%s${RESET}" "$status" ;;
    *)                printf "${DIM}%s${RESET}" "$status" ;;
  esac
}

# Define the services table data
# Format: service_name|compose_service|port_mapping|url
declare -a TABLE_DATA=(
  "ClickHouse|clickhouse|8123:8123, 9000:9000|http://localhost:8123"
  "Qdrant|qdrant|6333:6333, 6334:6334|http://localhost:6333/dashboard"
  "Ollama|ollama|11434:11434|http://localhost:11434"
  "Streamlit UI|ui|8501:8501|http://localhost:8501"
  "Report Server|files|8502:8502|http://localhost:8502"
  "Mosaic Agent|mosaic|—|docker compose run mosaic"
  "Ollama Init|ollama-init|—|one-shot initializer"
)

# Print the table
printf "\n"
printf "  ${BOLD}┌──────────────────┬─────────────────────┬─────────────────────┬───────────┬──────────────────────────────────┐${RESET}\n"
printf "  ${BOLD}│ %-16s │ %-19s │ %-19s │ %-9s │ %-32s│${RESET}\n" "Service" "Container" "Ports (host:ctr)" "Status" "URL / Access"
printf "  ${BOLD}├──────────────────┼─────────────────────┼─────────────────────┼───────────┼──────────────────────────────────┤${RESET}\n"

for row in "${TABLE_DATA[@]}"; do
  IFS='|' read -r svc_name compose_svc ports url <<< "$row"

  # Get live status
  IFS='|' read -r state health container_name <<< "$(get_container_status "$compose_svc")"

  # Determine display status
  if [[ "$health" == "healthy" ]]; then
    display_status="healthy"
  elif [[ -n "$health" && "$health" != "n/a" ]]; then
    display_status="$health"
  elif [[ "$state" == "running" ]]; then
    display_status="running"
  elif [[ "$state" == "exited" ]]; then
    # For one-shot containers (like ollama-init), exited 0 is fine
    display_status="done"
  else
    display_status="${state:-—}"
  fi

  # Truncate container name if too long
  if [[ ${#container_name} -gt 19 ]]; then
    container_name="${container_name:0:16}…"
  fi

  # Print row with colorized status
  printf "  │ %-16s │ %-19s │ %-19s │ " "$svc_name" "${container_name:-—}" "$ports"
  colorize_status "$display_status"
  # Pad status field (9 visible chars, accounting for color codes)
  pad=$((9 - ${#display_status}))
  printf "%*s" "$pad" ""
  printf " │ %-32s│\n" "$url"
done

printf "  ${BOLD}└──────────────────┴─────────────────────┴─────────────────────┴───────────┴──────────────────────────────────┘${RESET}\n"

# ═════════════════════════════════════════════════════════════════════════════
#  Done!
# ═════════════════════════════════════════════════════════════════════════════

printf "\n"
printf "  ${BOLD}${GREEN}══════════════════════════════════════════════════════════════${RESET}\n"
printf "  ${BOLD}${GREEN}  🎉 Mosaic Fund Agent is ready!${RESET}\n"
printf "  ${BOLD}${GREEN}══════════════════════════════════════════════════════════════${RESET}\n"
printf "\n"
printf "  ${BOLD}Quick Start:${RESET}\n"
printf "    ${CYAN}▸${RESET} Open the dashboard:   ${BOLD}http://localhost:8501${RESET}\n"
printf "    ${CYAN}▸${RESET} Browse reports:        ${BOLD}http://localhost:8502${RESET}\n"
printf "    ${CYAN}▸${RESET} Qdrant dashboard:      ${BOLD}http://localhost:6333/dashboard${RESET}\n"
printf "    ${CYAN}▸${RESET} Interactive chat:       ${BOLD}./mosaic.sh${RESET}\n"
printf "    ${CYAN}▸${RESET} Run analysis:           ${BOLD}docker compose run --rm mosaic analyze${RESET}\n"
printf "    ${CYAN}▸${RESET} Stop everything:        ${BOLD}./stop.sh${RESET}  or  ${BOLD}docker compose down${RESET}\n"
printf "\n"

if [[ "$HOST_OS" == "macos" ]]; then
  info "${DIM}macOS note: Ollama runs on CPU inside Docker (no Metal/MPS).${RESET}"
  info "${DIM}For faster local LLM, install Ollama on the host: brew install ollama${RESET}"
  printf "\n"
fi

# Try to open the dashboard in a browser
open_browser() {
  local url="$1"
  if [[ "$HOST_OS" == "macos" ]]; then
    open "$url" 2>/dev/null || true
  elif [[ "$HOST_OS" == "linux" ]]; then
    if command -v xdg-open &>/dev/null; then
      xdg-open "$url" 2>/dev/null || true
    fi
  fi
}

# Only open browser if we're running interactively
if [[ -t 0 ]]; then
  open_browser "http://localhost:8501"
fi

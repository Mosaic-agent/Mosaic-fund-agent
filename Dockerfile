# ── Mosaic Fund Agent — Dockerfile ───────────────────────────────────────────
#
# Build:   docker compose build
# Run:     docker compose run mosaic analyze --demo
#          docker compose run mosaic ask "what is my riskiest holding?"
#          docker compose run mosaic comex
#

FROM python:3.11-slim

# lxml (BeautifulSoup backend) needs gcc + libxml2/libxslt headers at build time.
# These are NOT needed at runtime so we clean up apt lists to keep the layer lean.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libgomp1 \
        libxml2-dev \
        libxslt1-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first so this layer is cached separately from
# source code — rebuilds are fast when only source files change.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY config/ config/
COPY src/ src/

# Pre-create the output directory (reports + cache both land here).
# At runtime this is replaced by the host-mounted volume, but the directory
# must exist for the fallback path when no volume is mounted.
RUN mkdir -p /app/output

# Disable Python output buffering so logs appear in real-time in the terminal.
ENV PYTHONUNBUFFERED=1

# Suppress webbrowser.open() inside the container (no display available).
# The HTML dashboard is still generated and available via the mounted volume.
ENV NO_BROWSER=1

# ENTRYPOINT is always the CLI; CMD provides a safe no-auth default.
# Override CMD by appending the desired subcommand to `docker compose run mosaic`.
ENTRYPOINT ["python", "src/main.py"]
CMD ["analyze", "--demo"]

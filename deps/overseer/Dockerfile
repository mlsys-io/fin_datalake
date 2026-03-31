# =============================================================================
# AI Lakehouse Overseer - Autonomic Controller
# =============================================================================
FROM python:3.12-slim as builder

# 1. System dependencies for high-performance networking and build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Install app-code dependencies (caching layer)
# We copy the pyproject.toml first to cache the pip install
COPY app-code /app/app-code
RUN pip install --no-cache-dir "/app/app-code[overseer]"

# 3. Development Mode Installation (so source changes are reflected if mounted)
# But for production, we just want the package available
RUN pip install -e "/app/app-code[overseer]"

# -----------------------------------------------------------------------------
# Final Image
# -----------------------------------------------------------------------------
FROM python:3.12-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy app code
COPY app-code /app/app-code

# Set Python Path to include app-code
ENV PYTHONPATH="/app/app-code"
ENV PYTHONUNBUFFERED=1

# Overseer uses its own loop, no web server required
CMD ["python", "-m", "overseer.main"]

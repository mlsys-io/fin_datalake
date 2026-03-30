# Use a slim Python image for the Gateway
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies (curl for k8s health probes)
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy only the necessary source code to /app
# Context is assumed to be PROJECT_ROOT
COPY app-code /app/app-code
COPY README.md /app/README.md

# Install the etl-framework with [gateway] extra
# This ensures fastapi, uvicorn, sqlalchemy, aiosqlite, and mcp are installed
RUN pip install --no-cache-dir -e "/app/app-code[gateway]"

# Set Python path so 'import gateway' and 'import etl' work
ENV PYTHONPATH="/app/app-code"

# Expose the default FastAPI port
EXPOSE 8000

# Metadata
ENV GATEWAY_VERSION="0.1.0"
ENV PYTHONUNBUFFERED=1

# Command to run the Gateway
# We use --host 0.0.0.0 to bind to all interfaces inside the container
CMD ["uvicorn", "gateway.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Custom Ray image with ETL dependencies + application packages
# Pin Ray so the cluster runtime matches local client environments.
FROM rayproject/ray:2.54.0-py312

# Copy the full app-code tree so Serve replicas can import agents, pipelines,
# gateway, overseer, and etl modules during deserialization.
COPY app-code /home/ray/app-code

# Keep direct imports working for local-style module paths.
ENV PYTHONPATH="/home/ray/app-code"

WORKDIR /home/ray

# Install dependencies
RUN pip install --no-cache-dir -U \
    # Prefect (needed for task deserialization on workers)
    prefect>=2.14 \
    prefect-ray>=0.3 \
    # Data processing
    "numpy>=2.0.0" \
    "pandas>=2.0,<3.0.0" \
    "pyarrow>=14.0" \
    deltalake>=0.14 \
    # Utilities
    loguru>=0.7 \
    requests>=2.28 \
    websocket-client>=1.6 \
    python-dotenv>=1.0 \
    # LLM integrations
    langchain-core>=0.3.0 \
    langchain-google-genai>=2.0.0 \
    # Connectors
    confluent-kafka>=2.3 \
    psycopg2-binary>=2.9 \
    pymilvus>=2.3 \
    thrift>=0.16

# Install the project package metadata and all importable modules.
# A regular install is sufficient in the image; editable installs require
# write access to the source tree and are only useful for local development.
RUN pip install --no-cache-dir /home/ray/app-code

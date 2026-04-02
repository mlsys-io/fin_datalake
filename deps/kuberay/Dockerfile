# Custom Ray image with ETL dependencies + etl package
# Using latest Ray with Python 3.12
FROM rayproject/ray:latest-py312

# Copy only the etl package (from build context = project root)
COPY app-code/etl /home/ray/etl

# Add to Python path so 'import etl' works
ENV PYTHONPATH="/home/ray"

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

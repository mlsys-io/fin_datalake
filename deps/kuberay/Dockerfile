# Custom Ray image with ETL dependencies - Python 3.12 compatible
# Using latest Ray with Python 3.12
FROM rayproject/ray:latest-py312

# Install ETL framework dependencies + Prefect (needed for task execution)
RUN pip install --no-cache-dir \
    # Prefect (needed for task deserialization on workers)
    prefect>=2.14 \
    prefect-ray>=0.3 \
    # Data processing
    pandas>=2.0 \
    pyarrow>=14.0 \
    deltalake>=0.14 \
    # Utilities
    loguru>=0.7 \
    requests>=2.28 \
    websocket-client>=1.6 \
    # Connectors
    confluent-kafka>=2.3 \
    psycopg2-binary>=2.9 \
    pymilvus>=2.3 \
    thrift>=0.16

WORKDIR /home/ray

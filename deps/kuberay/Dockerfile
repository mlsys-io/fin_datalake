# Custom Ray image with ETL dependencies - Python 3.12 compatible
# Using latest Ray with Python 3.12
FROM rayproject/ray:latest-py312

# Install ETL framework dependencies
RUN pip install --no-cache-dir \
    pandas>=2.0 \
    pyarrow>=14.0 \
    deltalake>=0.14 \
    loguru>=0.7 \
    requests>=2.28 \
    websocket-client>=1.6 \
    confluent-kafka>=2.3 \
    psycopg2-binary>=2.9 \
    pymilvus>=2.3 \
    thrift>=0.16

WORKDIR /home/ray

# Custom Ray image with ETL dependencies
FROM rayproject/ray:2.9.3-py311

# Copy requirements file
COPY ../../app-code/requirements-worker.txt /tmp/requirements-worker.txt

# Install ETL framework dependencies
RUN pip install --no-cache-dir -r /tmp/requirements-worker.txt

# Optional: Add your ETL framework code
# COPY app-code/etl /home/ray/etl

WORKDIR /home/ray

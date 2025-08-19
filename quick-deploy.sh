helm repo add --no-update minio https://charts.min.io
helm repo add --no-update timescale https://charts.timescale.com
helm repo add --no-update spark-operator https://kubeflow.github.io/spark-operator
helm repo update

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Deploying Minio..."
helm upgrade --install minio minio/minio --version 5.4.0 -f $SCRIPT_DIR/helm-config/minio.yaml
echo "Minio deployed!"

echo "Deploying Hive Metastore..."
git clone https://github.com/ssl-hep/hive-metastore
cd hive-metastore && helm dependency update && cd ..
helm upgrade --install hive-metastore hive-metastore -f $SCRIPT_DIR/helm-config/hive.yaml
rm -rf hive-metastore
echo "Hive Metastore deployed!"

echo "Deploying TimescaleDB..."
helm upgrade --install timescaledb timescale/timescaledb-single -f $SCRIPT_DIR/helm-config/timescaledb.yaml
echo "TimescaleDB deployed!"

echo "Deploying Spark Operator..."
helm upgrade --install spark-operator spark-operator/spark-operator --namespace spark-operator --create-namespace
echo "Spark Operator deployed!"
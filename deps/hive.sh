MINIO_ENDPOINT=$1
MINIO_USERNAME=$2
MINIO_PASSWORD=$3
MINIO_CERT=${4-$HOME/.mc/certs/CAs/public.crt}

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")

helm repo add bitnami https://charts.bitnami.com/bitnami
kubectl get namespace | grep -q "^hive " || kubectl create namespace hive # Create namespace if not exists

helm install hms-db bitnami/postgresql -n hive -f $SCRIPT_DIR/hive/hms-db.yaml

echo "Deployed PostgreSQL for Hive Metastore"

kubectl delete configmap minio-ca -n hive 2>/dev/null
kubectl create configmap minio-ca -n hive --from-file=public.crt=$MINIO_CERT
echo "Created ConfigMap for MinIO CA certificate"

kubectl delete secret minio-creds -n hive 2>/dev/null
kubectl create secret generic minio-creds -n hive \
  --from-literal=username=$MINIO_USERNAME --from-literal=password=$MINIO_PASSWORD
echo "Created Secret for MinIO credentials"

helm install hms $SCRIPT_DIR/hive/hms-chart -n hive --set s3.endpoint=$MINIO_ENDPOINT
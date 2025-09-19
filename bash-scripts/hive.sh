MINIO_ENDPOINT=$1
MINIO_USERNAME=$2
MINIO_PASSWORD=$3
MINIO_CERT=${4-$HOME/.mc/certs/CAs/public.crt}

helm repo add bitnami https://charts.bitnami.com/bitnami
kubectl create namespace hive

kubectl -n hive apply -f hms-chart/pvc/pv.yaml
kubectl -n hive apply -f hms-chart/pvc/pvc.yaml
echo "Allocated Persistent Volume for Hive Metastore DB"

helm upgrade -i hms-db bitnami/postgresql -n hive \
  --set auth.database=hivemetastore \
  --set persistence.existingClaim=hms-metastore-pvc \
  --set volumePermissions.enabled=true
echo "Deployed PostgreSQL for Hive Metastore"

kubectl delete configmap minio-ca -n hive 2>/dev/null
kubectl create configmap minio-ca -n hive --from-file=public.crt=$MINIO_CERT
echo "Created ConfigMap for MinIO CA certificate"

kubectl delete secret minio-creds -n hive 2>/dev/null
kubectl create secret generic minio-creds -n hive \
  --from-literal=username=$MINIO_USERNAME --from-literal=password=$MINIO_PASSWORD
echo "Created Secret for MinIO credentials"

helm upgrade -i hms ./hms-chart -n hive --set s3.endpoint=$MINIO_ENDPOINT
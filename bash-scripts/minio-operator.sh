helm install \
  --namespace minio-operator \
  --create-namespace \
  operator minio-operator/operator
kubectl create namespace minio-tenant
kubectl -n minio-tenant create secret generic minio-env \
  --from-literal=config.env='export MINIO_ROOT_USER=hive
export MINIO_ROOT_PASSWORD=hivehive'
helm install tenant minio-operator/tenant --namespace minio-tenant -f helm-config/minio-operator.yaml
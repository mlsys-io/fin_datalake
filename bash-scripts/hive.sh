helm repo add bitnami https://charts.bitnami.com/bitnami
kubectl create namespace hive

kubectl -n hive apply -f hms-chart/pvc/pv.yaml
kubectl -n hive apply -f hms-chart/pvc/pvc.yaml

helm upgrade -i hms-db bitnami/postgresql -n hive \
  --set auth.database=hivemetastore \
  --set persistence.existingClaim=hms-metastore-pvc \
  --set volumePermissions.enabled=true

export AWS_ACCESS_KEY_ID='hive'
export AWS_SECRET_ACCESS_KEY='hivehive'
kubectl -n hive create secret generic hms-s3-secret \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"

helm upgrade -i hms ./hms-chart -n hive --create-namespace

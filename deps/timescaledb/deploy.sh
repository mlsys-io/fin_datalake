helm install pgo oci://registry.developers.crunchydata.com/crunchydata/pgo \
  -n postgres-operator --create-namespace

kubectl -n postgres-operator create configmap tsdb-init-sql --from-file=timescaledb/init.sql

kubectl -n postgres-operator apply -f values.yaml
# kubectl -n postgres-operator delete postgrescluster tsdb
kubectl -n postgres-operator get pods -l postgres-operator.crunchydata.com/cluster=tsdb

kubectl get secret tsdb-pguser-app -n postgres-operator -o json \
| jq 'del(.metadata.namespace,.metadata.uid,.metadata.resourceVersion,.metadata.creationTimestamp,.metadata.annotations,.metadata.ownerReferences)' \
| kubectl apply -n default -f -

# kubectl -n postgres-operator exec -it tsdb-instance1-v6zs-0 -- psql -U postgres -d app
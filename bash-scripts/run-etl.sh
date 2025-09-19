kubectl delete rayjob ray-etl || true 2> /dev/null
kubectl apply -f app/ray-workflow/etl.yaml
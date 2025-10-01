kubectl delete rayjob ray-etl; kubectl apply -f app/etl.yaml

kubectl delete rayjob ray-summarize; kubectl apply -f app/summarize.yaml

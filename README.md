# Deployment of Spark + MinIO + Hive + TimescaleDB + DeltaLake in K8S. 

All commands below are idempotent to avoid unintentional failure due to repeatedly applying one operation multiple times. 

### Quick Deployment

This script requires an existing installation of Kubernetes with Helm. 

```bash
sh quick-deploy.sh
```

### Test of Deployment

#### Preparation

Firstly, upload our files to a configmap with the following command.

```bash
kubectl create configmap python-cmap --from-file=python/ --dry-run=client -o yaml | kubectl replace -f -
```

Then, upload the news file using mc: 
```bash
kubectl port-forward svc/minio 9000:9000
mc alias set myminio http://localhost:9000 hive hivehive
mc mirror --exclude "*.html"  $HOME/dataset/News myminio/raw-dataset/News
```

#### News combination

Read the news json and aggregate all summary values together. This step tests the correctness of Spark + MinIO. 

```bash
kubectl apply -f app/combine-news.yaml
```

#### ETL from Json to Delta Lake File (Still in MinIO)

Test Spark + MinIO + Hive + Delta Lake
```bash
kubectl apply -f app/json2delta.yaml
```

#### ETL from Delta Lake to TimescaleDB

Test Spark + MinIO + Hive + TimescaleDB + Delta Lake (All together)
```bash
kubectl apply -f app/delta2timescale.yaml
```
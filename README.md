# Deployment of Spark + MinIO + Hive + TimescaleDB + DeltaLake in K8S. 

All commands below are idempotent to avoid unintentional failure due to repeatedly applying one operation multiple times. 

### Quick Deployment

This script requires an existing installation of Kubernetes with Helm. 

```bash
sh quick-deploy.sh
```

### Basic Test

#### Preparation

Firstly, upload our files to a configmap with the following command.

```bash
kubectl create configmap python-cmap --from-file=python/ --dry-run=client -o yaml | kubectl replace -f -
kubectl create configmap cs4221-cmap --from-file=python/cs4221 --dry-run=client -o yaml | kubectl replace -f -
```

Then, upload the news file using mc: 
```bash
kubectl -n minio-tenant port-forward svc/myminio-hl 9000
mc alias set myminio https://localhost:9000 hive hivehive
mc admin update myminio
mc mb myminio/raw-dataset myminio/hive myminio/delta-lake
mc mirror  $HOME/dataset-slim myminio/raw-dataset
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

### Workloads

Firstly, upload our files to a configmap with the following command.

```bash
kubectl create configmap cs4221-cmap --from-file=python/cs4221 --dry-run=client -o yaml | kubectl replace -f -
```

Then, upload the news file using mc: 
```bash
kubectl -n minio-tenant port-forward svc/myminio-hl 9000
mc alias set myminio https://localhost:9000 hive hivehive
mc admin update myminio
mc mb myminio/raw-dataset myminio/hive myminio/delta-lake
mc mirror  $HOME/dataset-slim myminio/raw-dataset
```

#### Preprocessing: Load the data to Delta Lake

```bash
kubectl delete sparkapp bronze && kubectl apply -f app/cs4221/bronze.yaml
kubectl logs bronze-driver > test-bronze.log
```

#### Preprocessing: ETL on Delta Lake

```bash
kubectl delete sparkapp silver && kubectl apply -f app/cs4221/silver.yaml
kubectl logs silver-driver > test-silver.log
```

#### Workload1 of CS4221

The first step loads the data from disk, selects useful columns, and saves them in TSDB. Embeddings are also generated in this phase. 
```bash
kubectl delete sparkapp w1-step1-etl-tsdb; kubectl apply -f app/cs4221/w1-step1-etl-tsdb.yaml
kubectl logs w1-step1-etl-tsdb-driver > test-w1-step1-etl-tsdb.log
```

The second step executes the SQL query to detect the significant price changes. 
```bash
kubectl delete sparkapp w1-step2-sql; kubectl apply -f app/cs4221/w1-step2-sql.yaml
kubectl logs w1-step2-sql-driver > test-w1-step2-sql.log
```

The final step performs the RAG to explain the changes using relevant documents. 
```bash
kubectl delete sparkapp w1-step3-rag; kubectl apply -f app/cs4221/w1-step3-rag.yaml
kubectl logs w1-step3-rag-driver > test-w1-step3-rag.log
```

To view the result, log in the timescaledb pod and run
```sql
SELECT symbol, bucket_start,
       ROUND((100*change_ratio)::numeric, 2) AS pct,
       LEFT(answer->>'summary', 160) AS summary
FROM public.w1_explanations
ORDER BY created_at DESC
LIMIT 10;
```

#### Workload2 of CS4221

TODO
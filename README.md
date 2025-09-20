# The AI Lakehouse Project

### Deployment

#### Installing K0S

The following instructions are from the official K0S documentation.
```bash
curl --proto '=https' --tlsv1.2 -sSf https://get.k0s.sh | sudo sh
sudo k0s install controller --single
sudo k0s start
```

Install `kubectl` and set up the connection. 
```bash
curl -LO https://dl.k8s.io/$(k0s version | sed -r "s/\+k0s\.[[:digit:]]+$//")/kubernetes-client-linux-amd64.tar.gz
tar -zxvf kubernetes-client-linux-amd64.tar.gz kubernetes/client/bin/kubectl
sudo mv kubernetes/client/bin/kubectl /usr/local/bin/kubectl

mkdir -p $HOME/.kube
sudo cp /var/lib/k0s/pki/admin.conf $HOME/.kube/config
sudo chown $USER $HOME/.kube/config
```

Install Helm. 
```bash
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

#### Set up Hive Metastore service

```bash
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml
kubectl annotate storageclass local-path storageclass.kubernetes.io/is-default-class="true" --overwrite

bash deps/hive.sh https://localhost:4000 $MINIO_USERNAME $MINIO_PASSWORD
```

#### Set up KubeRay

```bash
bash deps/kuberay.sh
```

#### Deploy the job

Register the CA.
```bash
kubectl delete configmap minio-ca 2>/dev/null
kubectl create configmap minio-ca --from-file=public.crt=$MINIO_CERT
kubectl delete secret minio-creds 2>/dev/null
kubectl create secret generic minio-creds \
  --from-literal=username=$MINIO_USERNAME --from-literal=password=$MINIO_PASSWORD
```

Run the ETL job. 
```bash
bash app/run-etl.sh
```
helm repo add kuberay https://ray-project.github.io/kuberay-helm
helm repo update
helm install kuberay-operator kuberay/kuberay-operator -n etl-compute
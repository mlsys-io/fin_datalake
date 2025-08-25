helm repo add ollama-helm https://otwld.github.io/ollama-helm/
helm repo update
helm install ollama ollama-helm/ollama \
  --namespace ollama \
  --create-namespace -f helm-config/ollama.yaml
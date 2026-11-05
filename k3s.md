# Observability API on k3s

The API runs on k3s with autoscaling (1–5 pods). Everything else still runs with Docker Compose.

## Setup

**1. Install k3s** (skip on the group VM, it's already installed)
```bash
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--disable traefik" sh -
mkdir -p ~/.kube && sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config && sudo chown $USER ~/.kube/config
echo 'export KUBECONFIG=~/.kube/config' >> ~/.bashrc && source ~/.bashrc
```

**2. Build the image**
```bash
cd ~/INFOMCEC
docker compose --profile dev build observability-api
docker save infomcec-observability-api:latest | sudo k3s ctr images import -
```

**3. Create the DB secret**
```bash
source .env
VM_IP=$(hostname -I | awk '{print $1}')
kubectl create secret generic observability-db \
  --from-literal=DATABASE_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${VM_IP}:${POSTGRES_PORT}/${POSTGRES_DB}"
```

**4. Deploy**
```bash
docker rm -f observability-api 2>/dev/null
kubectl apply -f k8s/observability-api.yaml
```

**5. Check**
```bash
kubectl get pods,svc,hpa
curl http://localhost:3003/db
```

## After changing API code
```bash
docker compose --profile dev build observability-api
docker save infomcec-observability-api:latest | sudo k3s ctr images import -
kubectl rollout restart deployment observability-api
```

## Roll back to Docker
```bash
kubectl delete -f k8s/observability-api.yaml
docker compose --profile dev up -d observability-api
```

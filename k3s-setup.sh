# build image
cd ~/INFOMCEC
docker compose --profile dev build observability-api
docker save infomcec-observability-api:latest | sudo k3s ctr images import 

# create db secret
source .env
VM_IP=$(hostname -I | awk '{print $1}')
kubectl create secret generic observability-db \
  --from-literal=DATABASE_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${VM_IP}:${POSTGRES_PORT}/${POSTGRES_DB}"

# deploy
docker rm -f observability-api 2>/dev/null
kubectl apply -f k8s/observability-api.yaml

# health check
kubectl get pods,svc,hpa
curl http://localhost:3003/db

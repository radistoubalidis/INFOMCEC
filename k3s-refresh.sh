# refresh after API changes
docker compose --profile dev build observability-api
docker save infomcec-observability-api:latest | sudo k3s ctr images import -
kubectl rollout restart deployment observability-api

# rollback docker
kubectl delete -f k8s/observability-api.yaml
docker compose --profile dev up -d observability-api
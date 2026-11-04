# 1. Stop the services that write to Redis
docker compose stop consumer notifier

# 2. Clear Redis completely
docker compose exec redis redis-cli FLUSHALL SYNC

# 3. Verify it is empty
docker compose exec redis redis-cli DBSIZE

# 4. Restart
docker restart redis notifier
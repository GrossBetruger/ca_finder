docker run --name shill \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=shilldb \
  -p 3000:5432 \
  -d ankane/pgvector

sleep 1 

docker exec -it shill psql -U postgres -d shilldb -c "CREATE EXTENSION IF NOT EXISTS vector;"


set -a
source ../.env
set +a

curl -X POST -H "Content-Type: application/json" -d @postgres-sink.json http://localhost:${KAFKA_CONNECT_PORT}/connectors

# Launching time, otherwise comman bellow will arise error
sleep 10

curl http://localhost:${KAFKA_CONNECT_PORT}/connectors/postgres-jsonb-sink/status

#!/bin/bash
set -e

KAFKA_BIN="/opt/kafka/bin"
CONFIG_FILE="/etc/kafka/server.properties"
LOG_DIR="/var/lib/kafka/data"

if [ ! -f "$LOG_DIR/meta.properties" ]; then
    echo "=== [INIT] Initialization new Kafka cluster (KRaft) ==="

    CLUSTER_ID=$($KAFKA_BIN/kafka-storage.sh random-uuid)
    echo "=== [INIT] Generated Cluster ID: $CLUSTER_ID ==="

    $KAFKA_BIN/kafka-storage.sh format -t $CLUSTER_ID -c $CONFIG_FILE
    echo "=== [INIT] Storage formatting completed ==="

else
    echo "=== [INIT] Storage already formatted, skip step ==="
fi

echo "=== [START] Launch Apache Kafka ==="
exec $KAFKA_BIN/kafka-server-start.sh $CONFIG_FILE
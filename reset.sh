#!/bin/bash
# reset.sh — Clean Kafka topics and Spark output directories

set -e

echo "🧹 Stopping any existing Spark streams (manual step if needed)..."
echo "🗑️  Deleting Kafka topic: uber_topic"
docker exec kafka-devops /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic uber_topic 2>/dev/null || true

sleep 2

echo "🆕 Creating Kafka topic: uber_topic"
docker exec kafka-devops /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic uber_topic --partitions 4 --replication-factor 1

echo " Removing Spark output and checkpoint directories"
rm -rf /tmp/uber_stream_output

echo "✅ Reset complete. Ready for a fresh run!"
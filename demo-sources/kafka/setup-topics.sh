#!/bin/bash
# Create Kafka topics for ETL demo
# Run this after Kafka is started
#
# Usage:
#   ./setup-topics.sh             # Use localhost:9092
#   ./setup-topics.sh kafka:9092  # Custom bootstrap server

set -e

BOOTSTRAP="${1:-localhost:9092}"

echo "🔧 Setting up Kafka topics on $BOOTSTRAP"
echo ""

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
for i in {1..30}; do
    if kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list &>/dev/null; then
        echo "✅ Kafka is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Kafka not available after 30 seconds"
        exit 1
    fi
    sleep 1
done

echo ""
echo "📋 Creating topics..."

# OHLC events - high throughput, multiple partitions
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic ohlc-events \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete
echo "   ✅ ohlc-events (3 partitions, 24h retention)"

# News events - lower throughput, single partition for ordering
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic news-events \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=86400000
echo "   ✅ news-events (1 partition, 24h retention)"

# Order events - medium throughput
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic orders \
    --partitions 2 \
    --replication-factor 1 \
    --config retention.ms=172800000
echo "   ✅ orders (2 partitions, 48h retention)"

# Trade events - high throughput
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic trades \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=86400000
echo "   ✅ trades (4 partitions, 24h retention)"

# Dead letter queue for failed messages
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic dlq \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=604800000
echo "   ✅ dlq (7 day retention)"

echo ""
echo "📊 Topic summary:"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list

echo ""
echo "✅ All topics created successfully"
echo ""
echo "🚀 To start producing:"
echo "   python producer.py --bootstrap $BOOTSTRAP --topic ohlc-events"

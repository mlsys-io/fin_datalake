"""
Kafka producer that generates simulated OHLC and event data.
Run this during demos to show real-time data ingestion.

Topics produced:
  - ohlc-events: Simulated price ticks for multiple symbols
  - news-events: Simulated news headlines
  - orders: Simulated order events

Usage:
  python producer.py --bootstrap localhost:9092 --topic ohlc-events
  
For K8s:
  python producer.py --bootstrap kafka.demo-sources.svc:9092
"""
import argparse
import json
import time
import random
import signal
import sys
from datetime import datetime
from typing import Dict, Any

try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    print("kafka-python not installed. Run: pip install kafka-python")
    sys.exit(1)


# ============ Configuration ============
SYMBOLS = {
    "BTCUSD": {"base": 42000, "volatility": 0.002},
    "ETHUSD": {"base": 2800, "volatility": 0.003},
    "AAPL": {"base": 185, "volatility": 0.001},
    "GOOGL": {"base": 140, "volatility": 0.001},
    "MSFT": {"base": 380, "volatility": 0.001},
}

# Track current prices for random walk
current_prices: Dict[str, float] = {sym: cfg["base"] for sym, cfg in SYMBOLS.items()}

# Graceful shutdown
running = True


def signal_handler(sig, frame):
    global running
    print("\n🛑 Shutting down producer...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============ Data Generators ============
def generate_ohlc_tick(symbol: str) -> Dict[str, Any]:
    """Generate a single OHLC tick with random walk."""
    config = SYMBOLS[symbol]
    volatility = config["volatility"]
    
    # Random walk
    change = random.uniform(-volatility, volatility)
    current_prices[symbol] *= (1 + change)
    price = current_prices[symbol]
    
    return {
        "symbol": symbol,
        "timestamp": datetime.now().isoformat(),
        "epoch_ms": int(datetime.now().timestamp() * 1000),
        "open": round(price * random.uniform(0.999, 1.0), 2),
        "high": round(price * random.uniform(1.0, 1.002), 2),
        "low": round(price * random.uniform(0.998, 1.0), 2),
        "close": round(price, 2),
        "volume": random.randint(100, 10000),
        "trade_count": random.randint(10, 500)
    }


def generate_news_event() -> Dict[str, Any]:
    """Generate a simulated news event."""
    headlines = [
        "Breaking: Market volatility increases on economic data",
        "Analysts upgrade rating amid strong earnings",
        "Sector rotation observed as investors seek value",
        "Central bank signals policy shift",
        "Trading volumes surge on institutional activity"
    ]
    
    symbol = random.choice(list(SYMBOLS.keys()))
    return {
        "id": random.randint(10000, 99999),
        "timestamp": datetime.now().isoformat(),
        "symbol": symbol,
        "headline": f"{random.choice(headlines)} - {symbol}",
        "source": random.choice(["Reuters", "Bloomberg", "CNBC"]),
        "sentiment": random.choice(["positive", "negative", "neutral"]),
        "impact_score": round(random.uniform(0, 1), 2)
    }


def generate_order_event() -> Dict[str, Any]:
    """Generate a simulated order event."""
    symbol = random.choice(list(SYMBOLS.keys()))
    price = current_prices[symbol]
    
    return {
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "timestamp": datetime.now().isoformat(),
        "symbol": symbol,
        "side": random.choice(["buy", "sell"]),
        "quantity": round(random.uniform(0.1, 100), 4),
        "price": round(price * random.uniform(0.999, 1.001), 2),
        "order_type": random.choice(["market", "limit"]),
        "status": "filled"
    }


# ============ Producer Logic ============
def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create Kafka producer with retry logic."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"✅ Connected to Kafka at {bootstrap_servers}")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"⏳ Waiting for Kafka... retry in {wait_time}s ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                raise


def produce_loop(producer: KafkaProducer, topic: str, interval: float):
    """Main production loop."""
    message_count = 0
    
    print(f"🚀 Starting producer for topic '{topic}' (interval: {interval}s)")
    print("   Press Ctrl+C to stop\n")
    
    while running:
        try:
            if topic == "ohlc-events":
                for symbol in SYMBOLS:
                    data = generate_ohlc_tick(symbol)
                    producer.send(topic, value=data)
                    message_count += 1
                    print(f"📤 [{message_count}] {symbol}: ${data['close']:.2f}")
                    
            elif topic == "news-events":
                data = generate_news_event()
                producer.send(topic, value=data)
                message_count += 1
                print(f"📰 [{message_count}] {data['symbol']}: {data['headline'][:50]}...")
                
            elif topic == "orders":
                data = generate_order_event()
                producer.send(topic, value=data)
                message_count += 1
                print(f"📋 [{message_count}] {data['side'].upper()} {data['quantity']} {data['symbol']} @ ${data['price']}")
            
            producer.flush()
            time.sleep(interval)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(1)
    
    producer.close()
    print(f"\n✅ Producer stopped. Total messages sent: {message_count}")


# ============ Main ============
def main():
    parser = argparse.ArgumentParser(description="Kafka Demo Data Producer")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="ohlc-events", choices=["ohlc-events", "news-events", "orders"])
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between messages")
    args = parser.parse_args()
    
    producer = create_producer(args.bootstrap)
    produce_loop(producer, args.topic, args.interval)


if __name__ == "__main__":
    main()

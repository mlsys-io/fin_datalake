# Demo Sources for ETL Pipeline

This directory contains mock data sources for demonstrating ETL pipeline capabilities.

## Components

| Source | Description | K8s NodePort | Local |
|--------|-------------|--------------|-------|
| **API** | FastAPI server with OHLC, news, orders endpoints | `30800` | `:8000` |
| **WebSocket** | Mock streaming price data (similar to Bitstamp) | `30876` | `:8765` |
| **Kafka** | Message broker with OHLC/news/order topics | `30909` | `:9092` |
| **Static** | CSV files served via nginx | `30880` | local files |
| **SQLite** | Database with stocks/trades/portfolios/holdings | `30808` | local |

## Quick Start (K8s)

```bash
# Deploy all sources
./deploy.sh apply

# Check status
./deploy.sh status

# Remove
./deploy.sh delete
```

## Quick Start (Local)

```bash
# 1. Setup SQLite database
cd sqlite && python setup_db.py

# 2. Start Kafka (Docker)
cd kafka && docker-compose up -d

# 3. Run FastAPI
cd api && pip install -r requirements.txt && uvicorn main:app --port 8000

# 4. Run WebSocket server
cd websocket && pip install websockets && python server.py

# 5. Start Kafka producer
cd kafka && pip install kafka-python && python producer.py
```

## K8s Manual Deployment

```bash
# 1. Create namespace and configmaps
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmaps.yaml

# 2. Deploy services
kubectl apply -f k8s/websocket.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/static-server.yaml

# 3. Build and deploy API (requires Docker)
cd api && docker build -t demo-api:latest .
kubectl apply -f k8s/demo-api.yaml
```

## Endpoints

### API Server (`<node-ip>:30800`)
- `GET /api/ohlc/{symbol}` - Historical OHLC data
- `GET /api/ohlc?page=1&limit=50` - Paginated OHLC
- `GET /api/news` - News feed
- `GET /api/posts` - JSONPlaceholder compatible
- `GET /api/orders` - Sample orders
- `GET /api/customers` - Sample customers
- `GET /health` - Health check

### WebSocket (`ws://<node-ip>:30876`)
Streams real-time trade events:
```json
{"event": "trade", "data": {"symbol": "BTCUSD", "price": 42150.23}}
```

### Kafka (`<node-ip>:30909`)
Topics: `ohlc-events`, `news-events`, `orders`, `trades`

### Static Files (`http://<node-ip>:30880/data/`)
- `/data/customers.csv`
- `/data/orders.csv`

### SQLite (Local only)
Generated via `sqlite/setup_db.py`:
- Tables: `stocks`, `trades`, `portfolios`, `holdings`

"""
FastAPI server providing demo endpoints for ETL pipelines.

Endpoints:
  - GET /api/ohlc/{symbol}  - Historical OHLC data
  - GET /api/ohlc           - Paginated OHLC for all symbols
  - GET /api/news           - Paginated news feed
  - GET /api/posts          - JSONPlaceholder-compatible endpoint
  - GET /api/orders         - Sample orders for join demos
  - GET /api/customers      - Sample customers for join demos

Run locally:
  uvicorn main:app --reload --port 8000

Build for K8s:
  docker build -t demo-api:latest .
"""
from fastapi import FastAPI, Query
from typing import List, Optional
import random
from datetime import datetime, timedelta

app = FastAPI(
    title="ETL Demo API",
    version="1.0.0",
    description="Mock data API for ETL pipeline demonstrations"
)

# ============ Configuration ============
SYMBOLS = ["BTCUSD", "ETHUSD", "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
NEWS_SOURCES = ["Reuters", "Bloomberg", "WSJ", "CNBC", "MarketWatch"]
CATEGORIES = ["stocks", "crypto", "forex", "bonds", "commodities"]
PRODUCTS = ["Widget A", "Widget B", "Gadget X", "Tool Y", "Device Z"]
REGIONS = ["APAC", "EMEA", "AMER"]


# ============ Data Generators ============
def generate_ohlc_history(symbol: str, days: int = 30) -> List[dict]:
    """Generate realistic-looking OHLC data with random walk."""
    # Set base price based on symbol
    base_prices = {
        "BTCUSD": 42000, "ETHUSD": 2800, "AAPL": 185,
        "GOOGL": 140, "MSFT": 380, "AMZN": 155, "TSLA": 220
    }
    base_price = base_prices.get(symbol, random.uniform(50, 500))
    
    data = []
    for i in range(days):
        date = datetime.now() - timedelta(days=days - i)
        volatility = 0.02 if symbol in ["BTCUSD", "ETHUSD"] else 0.01
        
        open_price = base_price * random.uniform(1 - volatility, 1 + volatility)
        high_price = open_price * random.uniform(1.0, 1 + volatility * 2)
        low_price = open_price * random.uniform(1 - volatility * 2, 1.0)
        close_price = random.uniform(low_price, high_price)
        
        data.append({
            "symbol": symbol,
            "date": date.strftime("%Y-%m-%d"),
            "timestamp": int(date.timestamp()),
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": random.randint(100000, 10000000)
        })
        base_price = close_price  # Random walk
    
    return data


def generate_news(count: int = 100) -> List[dict]:
    """Generate sample news articles."""
    news = []
    headlines = [
        "Market rallies on strong earnings reports",
        "Fed signals potential rate adjustment",
        "Tech sector leads market gains",
        "Crypto volatility continues amid regulations",
        "Global markets react to economic data",
        "Analysts upgrade outlook for Q2",
        "Trading volumes hit record highs",
        "Institutional investors increase positions"
    ]
    
    for i in range(count):
        category = random.choice(CATEGORIES)
        related_symbol = random.choice(SYMBOLS)
        timestamp = datetime.now() - timedelta(hours=i * 2)
        
        news.append({
            "id": 1000 + i,
            "headline": f"{random.choice(headlines)} - {related_symbol}",
            "summary": f"Analysis and market commentary regarding {related_symbol} "
                      f"in the {category} sector. Market participants remain focused on key developments.",
            "source": random.choice(NEWS_SOURCES),
            "category": category,
            "datetime": int(timestamp.timestamp()),
            "related": related_symbol,
            "url": f"https://demo.news/article/{1000 + i}",
            "image": f"https://picsum.photos/seed/{1000 + i}/800/400"
        })
    
    return news


# ============ API Endpoints ============

@app.get("/")
def root():
    """API health and info endpoint."""
    return {
        "status": "ok",
        "service": "ETL Demo API",
        "version": "1.0.0",
        "endpoints": [
            "/api/ohlc/{symbol}",
            "/api/ohlc",
            "/api/news",
            "/api/posts",
            "/api/orders",
            "/api/customers",
            "/health"
        ]
    }


@app.get("/health")
def health():
    """Health check endpoint."""
    return {
        "healthy": True,
        "timestamp": datetime.now().isoformat(),
        "uptime_check": "ok"
    }


@app.get("/api/ohlc/{symbol}")
def get_ohlc_by_symbol(symbol: str, days: int = Query(default=30, le=365)):
    """Get historical OHLC data for a specific symbol."""
    return {
        "symbol": symbol.upper(),
        "days": days,
        "data": generate_ohlc_history(symbol.upper(), days)
    }


@app.get("/api/ohlc")
def list_ohlc(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, le=200),
    symbol: Optional[str] = None
):
    """Paginated OHLC data for all symbols (or filtered by symbol)."""
    symbols_to_fetch = [symbol.upper()] if symbol else SYMBOLS
    
    all_data = []
    for sym in symbols_to_fetch:
        all_data.extend(generate_ohlc_history(sym, 30))
    
    # Sort by timestamp descending
    all_data.sort(key=lambda x: x["timestamp"], reverse=True)
    
    # Paginate
    total = len(all_data)
    start = (page - 1) * limit
    end = start + limit
    
    return {
        "page": page,
        "limit": limit,
        "total": total,
        "total_pages": (total // limit) + 1,
        "data": all_data[start:end]
    }


@app.get("/api/news")
def get_news(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, le=100),
    category: Optional[str] = None
):
    """Paginated news feed."""
    all_news = generate_news(200)
    
    # Filter by category if provided
    if category:
        all_news = [n for n in all_news if n["category"] == category.lower()]
    
    total = len(all_news)
    start = (page - 1) * limit
    end = start + limit
    
    return {
        "page": page,
        "limit": limit,
        "total": total,
        "total_pages": (total // limit) + 1,
        "data": all_news[start:end]
    }


@app.get("/api/posts")
def get_posts():
    """JSONPlaceholder-compatible endpoint for demo_pipeline.py."""
    return [
        {
            "userId": (i % 10) + 1,
            "id": i,
            "title": f"Demo Post Title {i}",
            "body": f"This is the body content of post {i}. It contains sample text for ETL demonstration purposes."
        }
        for i in range(1, 101)
    ]


@app.get("/api/orders")
def get_orders(limit: int = Query(default=100, le=1000)):
    """Sample orders for join demos."""
    orders = []
    for i in range(1, limit + 1):
        order_date = datetime.now() - timedelta(days=random.randint(1, 90))
        orders.append({
            "order_id": f"ORD-{i:04d}",
            "customer_id": f"C{(i % 20) + 1:03d}",
            "product": random.choice(PRODUCTS),
            "quantity": random.randint(1, 50),
            "price": round(random.uniform(10, 500), 2),
            "order_date": order_date.strftime("%Y-%m-%d"),
            "status": random.choice(["completed", "pending", "shipped"])
        })
    return orders


@app.get("/api/customers")
def get_customers():
    """Sample customers for join demos."""
    return [
        {
            "customer_id": f"C{i:03d}",
            "name": f"Company {i} Inc.",
            "email": f"contact@company{i}.com",
            "region": random.choice(REGIONS),
            "tier": random.choice(["gold", "silver", "bronze"]),
            "created_at": (datetime.now() - timedelta(days=random.randint(100, 1000))).strftime("%Y-%m-%d")
        }
        for i in range(1, 21)
    ]


# ============ Main ============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

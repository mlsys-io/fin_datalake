"""
Creates a SQLite database with sample financial data.
This is used for demonstrating database source connectors.

Usage:
    cd demo-sources/sqlite
    python setup_db.py

The generated demo.db can be used with:
    - DatabaseReader in db_source_demo.py
    - Any SQLite-compatible ETL pipeline
"""
import sqlite3
import random
import os
from datetime import datetime, timedelta

# Database path - use env var for K8s, fallback for local
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "demo.db"))


def setup():
    """Create and populate the demo database."""
    # Remove existing database if present
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"🗑️  Removed existing database")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # ============ Create Tables ============
    
    # Stocks table - daily OHLC data
    c.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Trades table - individual trade events
    c.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            side TEXT CHECK(side IN ('buy', 'sell')) NOT NULL,
            quantity REAL NOT NULL,
            price REAL NOT NULL,
            order_type TEXT CHECK(order_type IN ('market', 'limit')) DEFAULT 'market'
        )
    ''')
    
    # Portfolios table - for joins demo
    c.execute('''
        CREATE TABLE IF NOT EXISTS portfolios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            strategy TEXT CHECK(strategy IN ('growth', 'value', 'balanced', 'income')),
            risk_level TEXT CHECK(risk_level IN ('low', 'medium', 'high')),
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Holdings table - linked to portfolios
    c.execute('''
        CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            quantity REAL NOT NULL,
            avg_cost REAL NOT NULL,
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (portfolio_id) REFERENCES portfolios(portfolio_id)
        )
    ''')
    
    print("📋 Created tables: stocks, trades, portfolios, holdings")
    
    # ============ Insert Sample Data ============
    
    # Stock symbols and their base prices
    symbols = {
        "AAPL": 185.0,
        "GOOGL": 140.0,
        "MSFT": 380.0,
        "AMZN": 155.0,
        "TSLA": 220.0,
        "META": 350.0,
        "NVDA": 480.0,
        "SPY": 450.0
    }
    
    # Generate 90 days of OHLC data for each symbol
    print("📈 Generating stock data (90 days × 8 symbols)...")
    stock_data = []
    for symbol, base_price in symbols.items():
        price = base_price
        for i in range(90):
            date = (datetime.now() - timedelta(days=90 - i)).strftime("%Y-%m-%d")
            volatility = 0.02  # 2% daily volatility
            
            # OHLC with random walk
            open_price = price
            high_price = price * (1 + random.uniform(0, volatility * 1.5))
            low_price = price * (1 - random.uniform(0, volatility * 1.5))
            close_price = random.uniform(low_price, high_price)
            volume = random.randint(100000, 5000000)
            
            stock_data.append((symbol, date, round(open_price, 2), round(high_price, 2),
                              round(low_price, 2), round(close_price, 2), volume))
            
            # Next day starts at previous close
            price = close_price
    
    c.executemany(
        "INSERT INTO stocks (symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        stock_data
    )
    print(f"   ✅ Inserted {len(stock_data)} stock records")
    
    # Generate trade data (last 7 days, high frequency)
    print("💱 Generating trade data...")
    trade_data = []
    for i in range(2000):
        symbol = random.choice(list(symbols.keys()))
        timestamp = (datetime.now() - timedelta(minutes=random.randint(1, 10080))).isoformat()
        side = random.choice(["buy", "sell"])
        quantity = round(random.uniform(1, 100), 2)
        # Price near current price with some noise
        price = symbols[symbol] * random.uniform(0.95, 1.05)
        order_type = random.choice(["market", "limit"])
        
        trade_data.append((symbol, timestamp, side, quantity, round(price, 2), order_type))
    
    c.executemany(
        "INSERT INTO trades (symbol, timestamp, side, quantity, price, order_type) VALUES (?, ?, ?, ?, ?, ?)",
        trade_data
    )
    print(f"   ✅ Inserted {len(trade_data)} trade records")
    
    # Create portfolios
    print("💼 Generating portfolios and holdings...")
    portfolios = [
        ("PF-001", "Growth Fund Alpha", "growth", "high"),
        ("PF-002", "Value Investors", "value", "medium"),
        ("PF-003", "Balanced Allocation", "balanced", "medium"),
        ("PF-004", "Income Generator", "income", "low"),
        ("PF-005", "Tech Focus", "growth", "high"),
    ]
    c.executemany(
        "INSERT INTO portfolios (portfolio_id, name, strategy, risk_level) VALUES (?, ?, ?, ?)",
        portfolios
    )
    
    # Create holdings for each portfolio
    holdings_data = []
    for pf_id, _, _, _ in portfolios:
        # Each portfolio holds 3-6 random stocks
        num_holdings = random.randint(3, 6)
        held_symbols = random.sample(list(symbols.keys()), num_holdings)
        for symbol in held_symbols:
            quantity = round(random.uniform(10, 500), 2)
            avg_cost = symbols[symbol] * random.uniform(0.85, 1.0)  # Bought at discount
            holdings_data.append((pf_id, symbol, quantity, round(avg_cost, 2)))
    
    c.executemany(
        "INSERT INTO holdings (portfolio_id, symbol, quantity, avg_cost) VALUES (?, ?, ?, ?)",
        holdings_data
    )
    print(f"   ✅ Inserted {len(portfolios)} portfolios with {len(holdings_data)} holdings")
    
    # ============ Create Indexes ============
    c.execute("CREATE INDEX idx_stocks_symbol ON stocks(symbol)")
    c.execute("CREATE INDEX idx_stocks_date ON stocks(date)")
    c.execute("CREATE INDEX idx_trades_symbol ON trades(symbol)")
    c.execute("CREATE INDEX idx_trades_timestamp ON trades(timestamp)")
    c.execute("CREATE INDEX idx_holdings_portfolio ON holdings(portfolio_id)")
    print("🔍 Created indexes for query performance")
    
    conn.commit()
    conn.close()
    
    # Print summary
    file_size = os.path.getsize(DB_PATH) / 1024
    print(f"\n✅ SQLite database created at: {DB_PATH}")
    print(f"   Size: {file_size:.1f} KB")
    print("\n📊 Tables summary:")
    print("   - stocks:     8 symbols × 90 days = 720 records")
    print("   - trades:     2000 trade events")
    print("   - portfolios: 5 portfolios")
    print(f"   - holdings:   {len(holdings_data)} positions")


def show_sample():
    """Display sample data from each table."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    print("\n📋 Sample data:")
    
    print("\n--- stocks (5 rows) ---")
    for row in c.execute("SELECT * FROM stocks LIMIT 5"):
        print(dict(row))
    
    print("\n--- trades (5 rows) ---")
    for row in c.execute("SELECT * FROM trades LIMIT 5"):
        print(dict(row))
    
    print("\n--- portfolios ---")
    for row in c.execute("SELECT * FROM portfolios"):
        print(dict(row))
    
    print("\n--- holdings (sample) ---")
    for row in c.execute("SELECT * FROM holdings LIMIT 5"):
        print(dict(row))
    
    conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SQLite Demo Database Setup")
    parser.add_argument("--show", action="store_true", help="Show sample data after creation")
    args = parser.parse_args()
    
    setup()
    if args.show:
        show_sample()

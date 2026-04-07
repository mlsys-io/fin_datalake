"""
Mock WebSocket server streaming real-time price data.
Alternative to using external services like Bitstamp.

Streams:
  - Simulated trade events (price, amount, timestamp)
  - Updates every 500ms per symbol

Usage:
  python server.py --port 8765
  
Connect with: ws://localhost:8765
"""
import asyncio
import argparse
import json
import random
import signal
from datetime import datetime
from typing import Dict, Set

try:
    import websockets
    from websockets.server import serve
except ImportError:
    print("websockets not installed. Run: pip install websockets")
    exit(1)


# ============ Configuration ============
SYMBOLS = {
    "BTCUSD": 42000.0,
    "ETHUSD": 2800.0,
}

# Track connected clients
connected_clients: Set = set()


# ============ Price Simulation ============
async def simulate_prices():
    """Continuously update prices with random walk."""
    while True:
        for symbol in SYMBOLS:
            # Random walk with slight upward bias
            change = random.uniform(-0.001, 0.0012)
            SYMBOLS[symbol] *= (1 + change)
        await asyncio.sleep(0.1)


# ============ WebSocket Handler ============
async def handle_client(websocket, path):
    """Handle a single WebSocket client connection."""
    client_addr = websocket.remote_address
    print(f"🔌 Client connected: {client_addr}")
    connected_clients.add(websocket)
    
    try:
        # Send welcome message
        await websocket.send(json.dumps({
            "event": "connected",
            "message": "Welcome to ETL Demo WebSocket",
            "symbols": list(SYMBOLS.keys())
        }))
        
        # Stream price updates
        while True:
            for symbol, price in SYMBOLS.items():
                # Simulate a trade event (similar to Bitstamp format)
                trade = {
                    "event": "trade",
                    "channel": "live_trades",
                    "data": {
                        "id": random.randint(100000, 999999),
                        "symbol": symbol,
                        "price": round(price, 2),
                        "price_str": f"{price:.2f}",
                        "amount": round(random.uniform(0.001, 2.0), 6),
                        "amount_str": f"{random.uniform(0.001, 2.0):.6f}",
                        "type": random.choice([0, 1]),  # 0=buy, 1=sell
                        "timestamp": datetime.now().isoformat(),
                        "microtimestamp": str(int(datetime.now().timestamp() * 1000000))
                    }
                }
                
                try:
                    await websocket.send(json.dumps(trade))
                except websockets.ConnectionClosed:
                    break
                    
            await asyncio.sleep(0.5)  # 2 updates per second per symbol
            
    except websockets.ConnectionClosed:
        pass
    finally:
        connected_clients.discard(websocket)
        print(f"🔌 Client disconnected: {client_addr}")


async def broadcast_stats():
    """Periodically broadcast connection stats."""
    while True:
        if connected_clients:
            stats = {
                "event": "stats",
                "connected_clients": len(connected_clients),
                "symbols": {sym: round(price, 2) for sym, price in SYMBOLS.items()},
                "timestamp": datetime.now().isoformat()
            }
            
            # Broadcast to all clients
            disconnected = set()
            for client in connected_clients:
                try:
                    await client.send(json.dumps(stats))
                except websockets.ConnectionClosed:
                    disconnected.add(client)
            
            connected_clients.difference_update(disconnected)
            
        await asyncio.sleep(10)  # Stats every 10 seconds


# ============ Main ============
async def main(host: str, port: int):
    print(f"🚀 WebSocket server starting on ws://{host}:{port}")
    print("   Press Ctrl+C to stop\n")
    
    # Start background tasks
    asyncio.create_task(simulate_prices())
    asyncio.create_task(broadcast_stats())
    
    # Start WebSocket server
    async with serve(handle_client, host, port):
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock WebSocket Price Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8765, help="Port to listen on")
    args = parser.parse_args()
    
    try:
        asyncio.run(main(args.host, args.port))
    except KeyboardInterrupt:
        print("\n✅ Server stopped")

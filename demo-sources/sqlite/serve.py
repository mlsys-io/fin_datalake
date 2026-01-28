"""
Simple HTTP server to serve SQLite database and provide query API.
This runs inside K8s pod after setup_db.py creates the database.

Endpoints:
  - GET /         - Service info
  - GET /health   - Health check
  - GET /download - Download demo.db file
  - GET /api/{table} - Query table (stocks, trades, portfolios, holdings)
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import sqlite3
import json
import os
from urllib.parse import urlparse, parse_qs

DB_PATH = "/data/demo.db"


class SQLiteHandler(BaseHTTPRequestHandler):
    """HTTP request handler for SQLite database access."""
    
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        
        if path == "/health":
            self._send_json({"healthy": True, "db_exists": os.path.exists(DB_PATH)})
            
        elif path == "/":
            self._send_json({
                "service": "SQLite Demo Server",
                "version": "1.0.0",
                "endpoints": {
                    "/health": "Health check",
                    "/download": "Download demo.db file",
                    "/api/stocks": "Query stocks table",
                    "/api/trades": "Query trades table",
                    "/api/portfolios": "Query portfolios table",
                    "/api/holdings": "Query holdings table",
                    "/api/{table}?limit=N": "Limit results (default 100)"
                }
            })
            
        elif path == "/download":
            self._serve_db_file()
            
        elif path.startswith("/api/"):
            table = path.split("/api/")[1].split("?")[0]
            query_params = parse_qs(parsed.query)
            limit = int(query_params.get("limit", [100])[0])
            self._query_table(table, limit)
            
        else:
            self._send_error(404, "Not found")
    
    def _send_json(self, data, status=200):
        """Send JSON response."""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode())
    
    def _send_error(self, status, message):
        """Send error response."""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"error": message}).encode())
    
    def _serve_db_file(self):
        """Serve the SQLite database file for download."""
        if not os.path.exists(DB_PATH):
            self._send_error(404, "Database not found")
            return
        
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Disposition", "attachment; filename=demo.db")
        self.send_header("Content-Length", str(os.path.getsize(DB_PATH)))
        self.end_headers()
        
        with open(DB_PATH, "rb") as f:
            self.wfile.write(f.read())
    
    def _query_table(self, table, limit):
        """Query a table and return results as JSON."""
        allowed_tables = ["stocks", "trades", "portfolios", "holdings"]
        
        if table not in allowed_tables:
            self._send_error(400, f"Invalid table. Allowed: {allowed_tables}")
            return
        
        if not os.path.exists(DB_PATH):
            self._send_error(503, "Database not ready")
            return
        
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Safe query (table name validated above)
            cursor.execute(f"SELECT * FROM {table} LIMIT ?", (limit,))
            rows = cursor.fetchall()
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Convert to list of dicts
            data = [dict(zip(columns, row)) for row in rows]
            
            # Get total count
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total = cursor.fetchone()[0]
            
            conn.close()
            
            self._send_json({
                "table": table,
                "limit": limit,
                "count": len(data),
                "total": total,
                "data": data
            })
            
        except Exception as e:
            self._send_error(500, str(e))
    
    def log_message(self, format, *args):
        """Custom log format."""
        print(f"[SQLite] {args[0]} {args[1]} {args[2]}")


def main():
    """Start the HTTP server."""
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    
    print(f"🗄️  SQLite HTTP Server starting on {host}:{port}")
    print(f"   Database: {DB_PATH}")
    print(f"   Exists: {os.path.exists(DB_PATH)}")
    
    server = HTTPServer((host, port), SQLiteHandler)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n✅ Server stopped")
        server.shutdown()


if __name__ == "__main__":
    main()

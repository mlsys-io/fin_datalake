import asyncio
import os
import sys
from sqlalchemy import text

# Ensure app-code is in PYTHONPATH
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
APP_DIR = os.path.join(PROJECT_ROOT, "app-code")
sys.path.insert(0, APP_DIR)

# Set DB location
os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{os.path.join(APP_DIR, 'gateway.db')}")

from gateway.db.session import init_db, engine

async def check():
    await init_db()
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
        tables = [row[0] for row in result]
        print("Tables found:", tables)
        if "users" in tables and "api_keys" in tables:
            print("✅ DB schema verified.")
        else:
            print("❌ DB schema missing tables!")
            sys.exit(1)

if __name__ == "__main__":
    asyncio.run(check())

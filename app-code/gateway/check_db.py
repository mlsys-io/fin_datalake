import asyncio
from sqlalchemy import text
from gateway.db.session import init_db, engine


async def check():
    await init_db()
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
        tables = [row[0] for row in result]
        print("Tables created:", tables)
        assert "users" in tables, "users table missing!"
        assert "api_keys" in tables, "api_keys table missing!"
    print("OK — DB schema verified.")


asyncio.run(check())

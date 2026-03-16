import asyncio
import os
import sys

# Ensure app-code is in PYTHONPATH
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
APP_DIR = os.path.join(PROJECT_ROOT, "app-code")
sys.path.insert(0, APP_DIR)

from gateway.db.session import init_db, AsyncSessionLocal
from gateway.db.crud import create_user

async def seed():
    # Set DB location if not set
    os.environ.setdefault("DATABASE_URL", f"sqlite+aiosqlite:///{os.path.join(APP_DIR, 'gateway.db')}")
    
    await init_db()
    
    async with AsyncSessionLocal() as db:
        try:
            user1 = await create_user(
                db=db, 
                username="garret", 
                password="password123", 
                role_names=["Admin", "DataEngineer"]
            )
            print(f"Created user: {user1.username} (Roles: {user1.role_names})")
        except ValueError as e:
            print(f"Skipped garret: {e}")
            
        try:
            user2 = await create_user(
                db=db, 
                username="analyst_bob", 
                password="password123", 
                role_names=["Analyst"]
            )
            print(f"Created user: {user2.username} (Roles: {user2.role_names})")
        except ValueError as e:
            print(f"Skipped analyst_bob: {e}")

if __name__ == "__main__":
    asyncio.run(seed())

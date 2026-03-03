import asyncio
from gateway.db.session import init_db, AsyncSessionLocal
from gateway.db.crud import create_user

async def seed():
    # Ensure tables exist
    await init_db()
    
    async with AsyncSessionLocal() as db:
        try:
            # Create an Admin user
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
            # Create a read-only Analyst user
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

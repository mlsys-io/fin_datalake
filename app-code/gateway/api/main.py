"""
create_app: FastAPI Application Factory.

Wires together:
  - The InterfaceRegistry (with all adapters)
  - Middleware (auth, CORS, logging)
  - Routers (auth, intent)

Usage:
    # For production (K8s / Docker):
    # uvicorn gateway.api.main:app --host 0.0.0.0 --port 8000

    # For development:
    # uvicorn gateway.api.main:create_app --factory --reload
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# -- Routers --
from gateway.api.routers import auth as auth_router
from gateway.api.routers import intent as intent_router

# -- Registry --
from gateway.core.registry import build_default_registry

# -- Database --
from gateway.db.session import init_db


def create_app() -> FastAPI:
    """
    Application factory.
    Creates and configures the FastAPI app with all middleware and routers.
    """
    app = FastAPI(
        title="AI Lakehouse Gateway",
        description=(
            "Unified protocol-agnostic interface to the AI Lakehouse. "
            "Supports REST (this interface), MCP (for AI assistants), and CLI."
        ),
        version="0.1.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
    )

    # -------------------------------------------------------------------------
    # CORS (allow Launchpad SPA to call this API on the same origin via Nginx)
    # -------------------------------------------------------------------------
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],   # Tightened by Nginx in prod
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -------------------------------------------------------------------------
    # Dependency: Attach the registry instance to app state.
    # Accessed in routes via: request.app.state.registry
    # -------------------------------------------------------------------------
    @app.on_event("startup")
    async def startup():
        await init_db()                          # Create tables if they don't exist
        app.state.registry = build_default_registry()

    # -------------------------------------------------------------------------
    # Routers
    # -------------------------------------------------------------------------
    app.include_router(auth_router.router,   prefix="/api/v1/auth",   tags=["Auth"])
    app.include_router(intent_router.router, prefix="/api/v1",        tags=["Intent"])

    # -------------------------------------------------------------------------
    # Health Check (unauthenticated — used by K8s liveness probe)
    # -------------------------------------------------------------------------
    @app.get("/healthz", tags=["System"])
    def health_check():
        return {"status": "ok"}

    return app


# Expose a top-level `app` for uvicorn
app = create_app()

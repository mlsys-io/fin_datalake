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

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# -- Routers --
from gateway.api.routers import auth as auth_router
from gateway.api.routers import intent as intent_router
from gateway.api.routers import agents as agents_router
from gateway.api.routers import system as system_router
from gateway.api.routers import alerts as alerts_router

# -- Registry --
from gateway.core.registry import build_default_registry

# -- Database --
from gateway.db.session import init_db
from gateway.api.errors import normalize_error_payload
from gateway.services.system import build_readiness_report


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
        from gateway.core.rbac import load_roles
        from gateway.core.ray_client import init_gateway_ray

        await load_roles()                       # Load dynamic RBAC roles FIRST
        await init_db()                          # Create tables + auto-provision (needs roles)
        app.state.ray_ready = init_gateway_ray()
        app.state.registry = build_default_registry()

    @app.exception_handler(HTTPException)
    async def http_exception_handler(_: Request, exc: HTTPException):
        payload = normalize_error_payload(exc.detail)
        return JSONResponse(
            status_code=exc.status_code,
            content=payload,
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(_: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content=normalize_error_payload(
                {
                    "detail": "Request validation failed.",
                    "code": "validation_error",
                    "context": {"errors": exc.errors()},
                }
            ),
        )

    # -------------------------------------------------------------------------
    # Routers
    # -------------------------------------------------------------------------
    app.include_router(auth_router.router,   prefix="/api/v1/auth",   tags=["Auth"])
    app.include_router(intent_router.router, prefix="/api/v1",        tags=["Intent"])
    app.include_router(agents_router.router, prefix="/api/v1/agents", tags=["Agents"])
    app.include_router(system_router.router, prefix="/api/v1/system", tags=["System Control"])
    app.include_router(alerts_router.router, prefix="/api/v1/stream", tags=["Streaming"])

    # -------------------------------------------------------------------------
    # Health Check (unauthenticated — used by K8s liveness probe)
    # -------------------------------------------------------------------------
    @app.get("/healthz", tags=["System"])
    def health_check():
        return {"status": "ok"}

    @app.get("/readyz", tags=["System"])
    async def readiness_check():
        report = await build_readiness_report(app)
        status_code = 200 if report["ready"] else 503
        return JSONResponse(status_code=status_code, content=report)

    return app


# Expose a top-level `app` for uvicorn
app = create_app()

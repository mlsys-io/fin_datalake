"""
Auth Router — /api/v1/auth

Handles user identity management endpoints.

Endpoints:
  POST /login          — Username + Password → JWT HttpOnly cookie
  POST /logout         — Clear the cookie
  GET  /me             — Current user identity + permissions
  GET  /verify         — Nginx auth_request target (200 OK or 401)
  POST /api-keys       — Generate a new API Key for the current user
  GET  /api-keys       — List current user's active API keys (prefixes only)
  DELETE /api-keys/{id}— Revoke an API Key by ID
"""

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.api.deps import create_jwt, get_current_user, get_db
from gateway.db import crud
from gateway.models.user import User

router = APIRouter()


# ---------------------------------------------------------------------------
# Request/Response Schemas
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str
    password: str

class MeResponse(BaseModel):
    username: str
    email: str | None
    roles: list[str]
    permissions: list[str]

class CreateAPIKeyRequest(BaseModel):
    description: str = "Default API Key"

class CreateAPIKeyResponse(BaseModel):
    key: str
    prefix: str
    description: str
    note: str = "This key is only shown ONCE. Store it securely."


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/login", summary="Authenticate and receive a session cookie")
async def login(
    body: LoginRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """
    Validate credentials and set a signed JWT in an HttpOnly cookie.

    The cookie (`gateway_token`) is read by the browser on subsequent requests
    and by the Nginx `auth_request` sub-requests to the `/verify` endpoint.
    """
    user = await crud.authenticate_user(db, body.username, body.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password.",
        )

    token = create_jwt(user.username)
    response.set_cookie(
        key="gateway_token",
        value=token,
        httponly=True,
        samesite="lax",
        # secure=True,   # Uncomment when served behind HTTPS (Nginx + TLS)
        max_age=60 * 480,  # 8 hours, matching JWT_EXPIRE_MINUTES
    )
    return {
        "message": f"Logged in as '{user.username}'.",
        "roles": user.role_names,
    }


@router.post("/logout", summary="Clear the session cookie")
def logout(response: Response):
    """Delete the gateway_token cookie, ending the browser session."""
    response.delete_cookie("gateway_token")
    return {"message": "Logged out successfully."}


@router.get("/me", response_model=MeResponse, summary="Get current user identity")
def get_me(current_user: User = Depends(get_current_user)):
    """
    Returns the authenticated user's identity and their effective permissions.

    Used by the Launchpad React app on startup to:
    - Confirm the user is logged in (re-direct to /login if 401).
    - Determine which UI elements to show/hide.
    """
    return MeResponse(
        username=current_user.username,
        email=current_user.email,
        roles=current_user.role_names,
        permissions=[p.value for p in current_user.all_permissions()],
    )


@router.get(
    "/verify",
    summary="Nginx auth_request target — returns 200 OK if authenticated",
    status_code=status.HTTP_200_OK,
)
def verify_session(
    response: Response,
    current_user: User = Depends(get_current_user),
):
    """
    Used by Nginx's `auth_request` directive to protect upstream services.

    Nginx calls this endpoint silently before proxying /prefect/, /ray/, etc.
    - If the user is logged in: FastAPI returns 200 → Nginx proxies the request.
    - If the user is NOT logged in: get_current_user raises 401 → Nginx
      catches it and redirects the user to the login page.

    We also pass the username as a header so upstreams know who the user is.
    """
    response.headers["X-Remote-User"] = current_user.username
    return Response(status_code=200, headers={"X-Remote-User": current_user.username})


@router.post(
    "/api-keys",
    response_model=CreateAPIKeyResponse,
    summary="Generate a new API Key for CLI / MCP access",
)
async def create_api_key(
    body: CreateAPIKeyRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Generate a long-lived API Key tied to the current user.

    The full key is returned ONLY ONCE. Copy it to your CLI config:
        etl configure --key <KEY>

    Or add it to your Cursor MCP config:
        "env": { "GATEWAY_API_KEY": "<KEY>" }
    """
    raw_key = await crud.create_api_key(db, current_user.username, body.description)
    return CreateAPIKeyResponse(
        key=raw_key,
        prefix=raw_key[:16],
        description=body.description,
    )


@router.get("/api-keys", summary="List your active API keys (prefixes only)")
async def list_api_keys(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Returns a list of active API key summaries (never the full key)."""
    keys = await crud.list_api_keys(db, current_user.username)
    return {"api_keys": keys}


@router.delete("/api-keys/{key_id}", summary="Revoke an API Key")
async def revoke_api_key(
    key_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Permanently revoke an API Key by ID. The key cannot be restored."""
    revoked = await crud.revoke_api_key(db, key_id, current_user.username)
    if not revoked:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"API key '{key_id}' not found or does not belong to you.",
        )
    return {"message": f"API key '{key_id}' has been revoked."}

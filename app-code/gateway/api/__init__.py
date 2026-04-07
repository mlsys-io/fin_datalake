"""
FastAPI Interface Translator — /api

This package is the REST Protocol Translator for the Gateway.
Its ONLY job is to:
  1. Parse HTTP requests (JSON body, headers, cookies).
  2. Authenticate & resolve the User from JWT or API Key.
  3. Build a UserIntent.
  4. Call registry.route(user, intent) and return the result.

It does NOT contain business logic — that lives in the adapters.

Package Structure:
  api/
  ├── main.py             — FastAPI app factory (create_app)
  ├── deps.py             — Shared FastAPI dependencies (get_registry, get_current_user)
  ├── routers/
  │   ├── auth.py         — POST /auth/login, POST /auth/logout, GET /auth/me
  │   └── intent.py       — POST /v1/intent  (the universal endpoint)
  └── middleware/
      └── auth.py         — JWT + API Key extraction middleware
"""

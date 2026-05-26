"""
routes/gateway_routes.py — NEUROAUTH Gateway & Auth Routes

FASE 2: Backend gateway that replaces direct frontend-to-Make calls.
All Make webhook communication goes through here.
Auth endpoints for Google OAuth + JWT issuance.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
from functools import wraps

import requests
from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

gateway_bp = Blueprint("gateway", __name__)

# ── Config from env ──────────────────────────────────────────────
JWT_SECRET = os.environ.get("JWT_SECRET", "neuroauth-default-secret-CHANGE-ME")
MAKE_WEBHOOK_PROFILE = os.environ.get("MAKE_WEBHOOK_PROFILE", "")
MAKE_WEBHOOK_GENERAL = os.environ.get("MAKE_WEBHOOK_GENERAL", "")
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "https://neuroauth.com.br").split(",")


def _cors(resp):
    """Add CORS headers for allowed frontend origins."""
    origin = request.headers.get("Origin", "")
    if origin in ALLOWED_ORIGINS:
        resp.headers["Access-Control-Allow-Origin"] = origin
    else:
        resp.headers["Access-Control-Allow-Origin"] = ALLOWED_ORIGINS[0]
    resp.headers["Access-Control-Allow-Methods"] = "POST,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    return resp


def _make_jwt(payload: dict, exp_seconds: int = 3600) -> str:
    """Create a simple HMAC-SHA256 JWT (no external deps)."""
    import base64
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).rstrip(b"=").decode()
    payload["iat"] = int(time.time())
    payload["exp"] = int(time.time()) + exp_seconds
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    sig_input = f"{header}.{body}".encode()
    sig = base64.urlsafe_b64encode(
        hmac.new(JWT_SECRET.encode(), sig_input, hashlib.sha256).digest()
    ).rstrip(b"=").decode()
    return f"{header}.{body}.{sig}"


def _verify_jwt(token: str) -> dict | None:
    """Verify HMAC-SHA256 JWT. Returns payload or None."""
    import base64
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header, body, sig = parts
        sig_input = f"{header}.{body}".encode()
        expected = base64.urlsafe_b64encode(
            hmac.new(JWT_SECRET.encode(), sig_input, hashlib.sha256).digest()
        ).rstrip(b"=").decode()
        if not hmac.compare_digest(sig, expected):
            return None
        padding = 4 - len(body) % 4
        payload = json.loads(base64.urlsafe_b64decode(body + "=" * padding))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None


def require_jwt(f):
    """Decorator: require valid JWT in Authorization header."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return _cors(jsonify({"error": "missing_token"})), 401
        payload = _verify_jwt(auth[7:])
        if not payload:
            return _cors(jsonify({"error": "invalid_token"})), 401
        request.jwt_payload = payload
        return f(*args, **kwargs)
    return decorated


# ── OPTIONS preflight ────────────────────────────────────────────
@gateway_bp.route("/api/make-proxy", methods=["OPTIONS"])
@gateway_bp.route("/auth/google", methods=["OPTIONS"])
@gateway_bp.route("/decide", methods=["OPTIONS"])
def options_preflight():
    resp = jsonify({"ok": True})
    return _cors(resp), 204


# ── POST /auth/google ────────────────────────────────────────────
@gateway_bp.route("/auth/google", methods=["POST"])
def auth_google():
    """
    Validate Google OAuth credential and issue internal JWT.
    Expects: { "credential": "<google_id_token>" }
    Returns: { "jwt": "<internal_jwt>", "user": {...} }
    """
    data = request.get_json(silent=True) or {}
    credential = data.get("credential", "")
    if not credential:
        return _cors(jsonify({"error": "missing_credential"})), 400

    # Validate with Google
    try:
        google_resp = requests.get(
            "https://oauth2.googleapis.com/tokeninfo",
            params={"id_token": credential},
            timeout=10,
        )
        if google_resp.status_code != 200:
            return _cors(jsonify({"error": "invalid_google_token"})), 401
        gdata = google_resp.json()
    except Exception as e:
        logger.error(f"Google token validation failed: {e}")
        return _cors(jsonify({"error": "google_validation_failed"})), 500

    # Build user profile
    user = {
        "email": gdata.get("email", ""),
        "name": gdata.get("name", gdata.get("email", "")),
        "picture": gdata.get("picture", ""),
        "sub": gdata.get("sub", ""),
    }

    # Issue internal JWT
    jwt_token = _make_jwt({"sub": user["email"], "name": user["name"]})
    logger.info(f"JWT issued for {user['email']}")

    resp = jsonify({"jwt": jwt_token, "user": user})
    return _cors(resp), 200


# ── POST /api/make-proxy ─────────────────────────────────────────
@gateway_bp.route("/api/make-proxy", methods=["POST"])
@require_jwt
def make_proxy():
    """
    Gateway proxy to Make.com webhooks.
    Frontend sends data here; backend forwards to Make.
    Requires valid JWT.
    """
    data = request.get_json(silent=True) or {}
    webhook_type = data.pop("_webhook_type", "profile")

    if webhook_type == "profile":
        webhook_url = MAKE_WEBHOOK_PROFILE
    else:
        webhook_url = MAKE_WEBHOOK_GENERAL

    if not webhook_url:
        logger.error(f"Webhook URL not configured for type: {webhook_type}")
        return _cors(jsonify({"error": "webhook_not_configured"})), 500

    # Inject authenticated user info
    data["_authenticated_by"] = request.jwt_payload.get("sub", "unknown")
    data["_timestamp"] = time.time()

    try:
        make_resp = requests.post(webhook_url, json=data, timeout=15)
        result = {
            "status": make_resp.status_code,
            "body": make_resp.text[:500],
        }
        logger.info(f"Make proxy: {webhook_type} -> {make_resp.status_code}")
        return _cors(jsonify(result)), make_resp.status_code
    except Exception as e:
        logger.error(f"Make proxy error: {e}")
        return _cors(jsonify({"error": str(e)})), 502

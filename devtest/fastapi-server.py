#!/usr/bin/env python3
"""
FastAPI server for Coframe using AuthMiddleware wrapper.

Clean implementation showing how to use server_utils.AuthMiddleware
for consistent authentication and token refresh across frameworks.
"""

import sys
import os
sys.path.append("..")

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any

import coframe
import coframe.server_utils as srv
from coframe.utils import get_app

# ============================================================================
# Initialize Coframe
# ============================================================================

print("Initializing Coframe...")

# Load plugins
plugins = coframe.plugins.PluginsManager()
plugins.load_config("config.yaml")
coframe.utils.register_standard_handlers(plugins)
plugins.load_plugins()

# Setup database
coframe_app = get_app()
coframe_app.calc_db(plugins)

db_url = 'sqlite:///devtest.sqlite'
import model  # type: ignore
coframe_app.initialize_db(db_url, model)

# Get command processor
command_processor = coframe_app.cp

# Configuration
SECRET_KEY = os.environ.get('SECRET_KEY', 'development-secret-key')
api_prefix = f"/{plugins.config.get('api', {}).get('prefix', 'api')}"

# Initialize AuthMiddleware (wrapper with config)
auth = srv.AuthMiddleware(plugins.config, SECRET_KEY)

print("✓ Coframe initialized")
print(f"✓ API prefix: {api_prefix}")
print(f"✓ JWT expiration: {auth.jwt_expiration_hours}h")
print(f"✓ Refresh interval: {auth.refresh_interval_minutes}min")

# ============================================================================
# FastAPI App Setup
# ============================================================================

app = FastAPI(
    title="Coframe API (FastAPI + AuthMiddleware)",
    description="Framework-agnostic server with automatic token refresh",
    version="2.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Token Refresh Middleware
# ============================================================================
@app.middleware("http")
async def token_refresh_middleware(request: Request, call_next):
    """
    Middleware that adds X-New-Token header if token was refreshed.

    This implements the Coframe protocol for automatic token refresh.
    """
    response = await call_next(request)

    # If token was refreshed, add to response header
    if hasattr(request.state, 'new_token'):
        response.headers['X-New-Token'] = request.state.new_token

    return response


# ============================================================================
# Authentication Dependency (using AuthMiddleware)
# ============================================================================
async def get_current_user(request: Request) -> Dict[str, Any]:
    """
    Extract and validate JWT token, with automatic refresh.

    Uses AuthMiddleware for consistent, framework-agnostic logic.
    """
    # Extract token from header
    token, error = auth.extract_token(request.headers.get("authorization"))
    if error:
        raise HTTPException(status_code=401, detail=error)

    # Decode and check if refresh is needed
    payload, new_token, error = auth.decode_and_refresh(token)
    if error:
        raise HTTPException(status_code=401, detail=error)

    # If token was refreshed, save for middleware
    if new_token:
        request.state.new_token = new_token

    return payload


# ============================================================================
# Routes
# ============================================================================
@app.get('/')
async def home():
    """Root endpoint"""
    return {
        'message': 'Coframe API Server (FastAPI + AuthMiddleware)',
        'version': plugins.config.get('version', '0.0.0'),
        'protocol': 'Coframe v2.0',
        'info': '/info',
        'api': f'{api_prefix}/...'
    }


@app.get('/info')
async def app_info():
    """Application information"""
    result = srv.get_app_info(plugins.config, api_prefix)
    return result


# ============================================================================
# Authentication Endpoints
# ============================================================================
@app.post(f'{api_prefix}/auth/login')
async def login(data: dict):
    """Login endpoint (using AuthMiddleware)"""
    try:
        # Use AuthMiddleware.login() for consistent behavior
        result = auth.login(command_processor, data)
        return result
    except Exception as e:
        return {'status': 'error', 'message': str(e), 'status_code': 500}


@app.post(f'{api_prefix}/auth/update_context')
async def update_context(
    data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Update user context and get new token"""
    result = srv.handle_update_context(
        current_user,
        data,
        SECRET_KEY,
        auth.jwt_expiration_hours
    )
    return result


# ============================================================================
# Database CRUD Endpoints
# ============================================================================
@app.get(f'{api_prefix}/db/{{table}}')
async def db_list(
    table: str,
    current_user: dict = Depends(get_current_user)
):
    """List all records in table"""
    result = srv.handle_db_operation(
        command_processor,
        'get',
        table,
        context=current_user
    )
    return result


@app.get(f'{api_prefix}/db/{{table}}/{{id}}')
async def db_get(
    table: str,
    id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get single record by ID"""
    result = srv.handle_db_operation(
        command_processor,
        'get',
        table,
        record_id=id,
        context=current_user
    )
    return result


@app.post(f'{api_prefix}/db/{{table}}')
async def db_create(
    table: str,
    data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Create new record"""
    result = srv.handle_db_operation(
        command_processor,
        'create',
        table,
        data=data,
        context=current_user
    )
    return result


@app.put(f'{api_prefix}/db/{{table}}/{{id}}')
async def db_update(
    table: str,
    id: str,
    data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Update existing record"""
    result = srv.handle_db_operation(
        command_processor,
        'update',
        table,
        record_id=id,
        data=data,
        context=current_user
    )
    return result


@app.delete(f'{api_prefix}/db/{{table}}/{{id}}')
async def db_delete(
    table: str,
    id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete record"""
    result = srv.handle_db_operation(
        command_processor,
        'delete',
        table,
        record_id=id,
        context=current_user
    )
    return result


# ============================================================================
# Query Endpoint
# ============================================================================
@app.post(f'{api_prefix}/query')
async def query(
    data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Execute dynamic query"""
    result = srv.handle_query(
        command_processor,
        data,
        context=current_user
    )
    return result


# ============================================================================
# File Reading Endpoint
# ============================================================================
@app.post(f'{api_prefix}/read_file')
async def read_file(
    data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Read file from allowed directories"""
    result = srv.handle_generic_endpoint(
        command_processor,
        'read_file',
        data,
        context=current_user
    )
    return result


# ============================================================================
# Generic Endpoint Dispatcher
# ============================================================================
@app.post(f'{api_prefix}/endpoint/{{operation}}')
async def generic_endpoint(
    operation: str,
    data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Generic endpoint for custom operations"""
    result = srv.handle_generic_endpoint(
        command_processor,
        operation,
        data,
        context=current_user
    )
    return result


# ============================================================================
# User Profile Endpoints
# ============================================================================
@app.get(f'{api_prefix}/profile')
async def get_profile(current_user: dict = Depends(get_current_user)):
    """Get current user profile"""
    # Remove sensitive fields
    user_data = {k: v for k, v in current_user.items() if k not in ['exp', 'iat', 'last_refresh']}
    return {
        'status': 'success',
        'data': user_data,
        'status_code': 200
    }


@app.get(f'{api_prefix}/users/me')
async def get_current_user_alias(current_user: dict = Depends(get_current_user)):
    """Alias for get_profile"""
    return await get_profile(current_user)

# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == '__main__':
    import uvicorn
    # Read port from config (default to 8300 if not specified)
    port = plugins.config.get('api', {}).get('port', 8300)
    print(f"\n🚀 Starting Coframe FastAPI server (v2) on port {port}")
    print(f"📖 OpenAPI docs: http://localhost:{port}/docs")
    print(f"🔄 Auto-refresh enabled: every {auth.refresh_interval_minutes} minutes\n")
    uvicorn.run(
        app,
        host='0.0.0.0',
        port=port,
        log_level='info'
    )

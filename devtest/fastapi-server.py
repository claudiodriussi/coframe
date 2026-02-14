#!/usr/bin/env python3
"""
Example FastAPI server using AsyncCommandProcessor.

This demonstrates how to integrate Coframe with FastAPI using
the new async compatibility layer.

Run with: uvicorn fastapi-server-example:app --reload
Or: python fastapi-server-example.py

Requirements:
    pip install fastapi uvicorn python-multipart
"""

import sys
import os
sys.path.append("..")

from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any
import jwt
from datetime import datetime, timedelta

import coframe
from coframe.utils import get_app
from coframe.endpoints import AsyncCommandProcessor

# ============================================================================
# Initialize Coframe (identical to Flask setup)
# ============================================================================

print("Initializing Coframe...")

# Load plugins
plugins = coframe.plugins.PluginsManager()
plugins.load_config("config.yaml")
coframe.utils.register_standard_handlers(plugins)  # Register column merge handlers
plugins.load_plugins()

# Setup database
coframe_app = get_app()
coframe_app.calc_db(plugins)

db_url = 'sqlite:///devtest.sqlite'
import model  # type: ignore
coframe_app.initialize_db(db_url, model)

# Create async processor wrapper
sync_processor = coframe_app.cp
async_processor = AsyncCommandProcessor(sync_processor, max_workers=10)

print(f"✓ Coframe initialized with {len(plugins.plugins)} plugins")

# ============================================================================
# FastAPI App Setup
# ============================================================================

app = FastAPI(
    title="Coframe API (FastAPI)",
    description="Data-driven backend with async support",
    version="1.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
SECRET_KEY = os.environ.get('SECRET_KEY', 'development-secret-key')
JWT_EXPIRATION_HOURS = 24

# Get API prefix from configuration
api_prefix = f"/{plugins.config.get('api', {}).get('prefix', 'api')}/api"
print(f"✓ API endpoints available at: {api_prefix}")

# ============================================================================
# Authentication Dependencies
# ============================================================================


async def get_current_user(authorization: Optional[str] = Header(None)) -> Dict[str, Any]:
    """
    FastAPI dependency for JWT authentication.
    Extracts user context from Bearer token.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing authentication token")

    if not authorization.startswith('Bearer '):
        raise HTTPException(status_code=401, detail="Invalid authentication scheme")

    token = authorization.split(' ')[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


# ============================================================================
# Public Endpoints (No Auth)
# ============================================================================

@app.get('/')
async def root():
    """API root - health check"""
    return {
        "status": "success",
        "message": "Coframe FastAPI server is running",
        "framework": "FastAPI",
        "async_mode": True
    }


@app.get('/info')
async def app_info():
    """Application information"""
    return {
        'status': 'success',
        'data': {
            'application': plugins.config.get('name', 'Unknown'),
            'version': plugins.config.get('version', '0.0.0'),
            'description': plugins.config.get('description', ''),
            'framework': 'FastAPI',
            'async_mode': True,
            'coframe_api_prefix': api_prefix,
            'available_endpoints': {
                'health': '/',
                'info': '/info',
                'auth_login': f'{api_prefix}/auth/login',
                'generic_endpoint': f'{api_prefix}/endpoint/<operation>',
                'database_get': f'{api_prefix}/db/<table>',
                'query_execute': f'{api_prefix}/query'
            }
        }
    }


# ============================================================================
# Authentication Endpoints
# ============================================================================

@app.post(f'{api_prefix}/auth/login')
async def login(credentials: Dict[str, str]):
    """
    User login endpoint.
    Returns JWT token on success.

    Body:
        {
            "username": "string",
            "password": "string"
        }
    """
    username = credentials.get('username')
    password = credentials.get('password')

    if not username or not password:
        raise HTTPException(status_code=400, detail="Username and password required")

    # Execute auth command via async processor
    command = {
        "operation": "auth",
        "parameters": {
            "username": username,
            "password": password
        },
        "context": {}
    }

    result = await async_processor.send_async(command)

    if result.get('status') != 'success':
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Generate JWT token
    user_context = result['data']['context']
    token_payload = {
        **user_context,
        'exp': datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
    }

    token = jwt.encode(token_payload, SECRET_KEY, algorithm='HS256')

    return {
        'token': token,
        'user': user_context
    }


# ============================================================================
# Protected Endpoints (Require Auth)
# ============================================================================

@app.post(f'{api_prefix}/endpoint/{{operation}}')
async def execute_endpoint(
    operation: str,
    data: Dict[str, Any],
    user: Dict = Depends(get_current_user)
):
    """
    Generic endpoint that can execute any Coframe operation.

    URL params:
        operation: Name of the operation to execute

    Body:
        Command parameters (dict)

    Returns:
        Operation result
    """
    command = {
        'operation': operation,
        'parameters': data,
        'context': user
    }

    result = await async_processor.send_async(command)

    if result.get('status') == 'error':
        raise HTTPException(
            status_code=result.get('code', 500),
            detail=result.get('message')
        )

    return result


@app.get(f'{api_prefix}/db/{{table}}')
async def db_get_all(
    table: str,
    start: int = 0,
    limit: int = 100,
    order_by: Optional[str] = None,
    order_dir: str = 'asc',
    user: Dict = Depends(get_current_user)
):
    """
    Get all records from a table with pagination.

    URL params:
        table: Table name

    Query params:
        start: Offset (default 0)
        limit: Max records (default 100)
        order_by: Column to sort by
        order_dir: Sort direction (asc/desc)
    """
    command = {
        "operation": "db",
        "parameters": {
            "table": table,
            "method": "get",
            "start": start,
            "limit": limit,
            "order_by": order_by,
            "order_dir": order_dir
        },
        "context": user
    }

    result = await async_processor.send_async(command)

    if result.get('status') == 'error':
        raise HTTPException(
            status_code=result.get('code', 500),
            detail=result.get('message')
        )

    return result


@app.get(f'{api_prefix}/db/{{table}}/{{id}}')
async def db_get_one(
    table: str,
    id: str,
    user: Dict = Depends(get_current_user)
):
    """Get a single record by ID"""
    command = {
        "operation": "db",
        "parameters": {
            "table": table,
            "method": "get",
            "id": id
        },
        "context": user
    }

    result = await async_processor.send_async(command)

    if result.get('status') == 'error':
        raise HTTPException(
            status_code=result.get('code', 500),
            detail=result.get('message')
        )

    return result


@app.post(f'{api_prefix}/db/{{table}}')
async def db_create(
    table: str,
    data: Dict[str, Any],
    user: Dict = Depends(get_current_user)
):
    """Create a new record"""
    # Convert types if needed
    data = coframe.utils.json_to_model_types(data, table)

    command = {
        "operation": "db",
        "parameters": {
            "table": table,
            "method": "create",
            "data": data
        },
        "context": user
    }

    result = await async_processor.send_async(command)

    if result.get('status') == 'error':
        raise HTTPException(
            status_code=result.get('code', 500),
            detail=result.get('message')
        )

    return result


@app.put(f'{api_prefix}/db/{{table}}/{{id}}')
async def db_update(
    table: str,
    id: str,
    data: Dict[str, Any],
    user: Dict = Depends(get_current_user)
):
    """Update a record"""
    data = coframe.utils.json_to_model_types(data, table)

    command = {
        "operation": "db",
        "parameters": {
            "table": table,
            "method": "update",
            "id": id,
            "data": data
        },
        "context": user
    }

    result = await async_processor.send_async(command)

    if result.get('status') == 'error':
        raise HTTPException(
            status_code=result.get('code', 500),
            detail=result.get('message')
        )

    return result


@app.delete(f'{api_prefix}/db/{{table}}/{{id}}')
async def db_delete(
    table: str,
    id: str,
    user: Dict = Depends(get_current_user)
):
    """Delete a record"""
    command = {
        "operation": "db",
        "parameters": {
            "table": table,
            "method": "delete",
            "id": id
        },
        "context": user
    }

    result = await async_processor.send_async(command)

    if result.get('status') == 'error':
        raise HTTPException(
            status_code=result.get('code', 500),
            detail=result.get('message')
        )

    return result


@app.post(f'{api_prefix}/query')
async def execute_query(
    data: Dict[str, Any],
    user: Dict = Depends(get_current_user)
):
    """Execute a dynamic query"""
    command = {
        "operation": "query",
        "parameters": {
            "format": data.get('format', 'tuples'),
            "query": data.get('query')
        },
        "context": user
    }

    result = await async_processor.send_async(command)

    if result.get('status') == 'error':
        raise HTTPException(
            status_code=result.get('code', 500),
            detail=result.get('message')
        )

    return result


# ============================================================================
# Startup/Shutdown Events
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Called when server starts"""
    port = plugins.config.get('api', {}).get('port', 8300)
    print("\n" + "=" * 60)
    print("🚀 FastAPI server starting...")
    print("=" * 60)
    print("✓ Async mode enabled")
    print("✓ Thread pool size: 10")
    print(f"✓ API prefix: {api_prefix}")
    print(f"✓ OpenAPI docs: http://localhost:{port}/docs")
    print("=" * 60 + "\n")


@app.on_event("shutdown")
async def shutdown_event():
    """Called when server shuts down - cleanup resources"""
    print("\n🛑 Shutting down async processor...")
    async_processor.shutdown()
    print("✓ Cleanup complete\n")


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == '__main__':
    import uvicorn
    # Read port from config (default to 8300 if not specified)
    port = plugins.config.get('api', {}).get('port', 8300)
    print(f"Starting Coframe FastAPI server on port {port}")
    uvicorn.run(
        app,
        host='0.0.0.0',
        port=port,
        log_level='info'
    )

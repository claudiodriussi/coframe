#!/usr/bin/env python3
"""
Flask server for Coframe using AuthMiddleware wrapper.

Clean implementation showing how to use server_utils.AuthMiddleware
for consistent authentication and token refresh across frameworks.
"""

import sys
import os
sys.path.append("..")

from flask import Flask, request, jsonify, g
from flask_cors import CORS
from functools import wraps

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
# Flask App Setup
# ============================================================================

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY
CORS(app)


# ============================================================================
# Token Refresh Hook
# ============================================================================
@app.after_request
def add_refresh_token(response):
    """
    Add X-New-Token header if token was refreshed.

    This implements the Coframe protocol for automatic token refresh.
    """
    if hasattr(g, 'new_token'):
        response.headers['X-New-Token'] = g.new_token
    return response


# ============================================================================
# Authentication Decorator (using AuthMiddleware)
# ============================================================================
def login_required(f):
    """
    Authentication decorator with automatic token refresh.

    Uses AuthMiddleware for consistent, framework-agnostic logic.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Extract token from header
        token, error = auth.extract_token(request.headers.get('Authorization'))
        if error:
            return jsonify({'status': 'error', 'message': error}), 401

        # Decode and check if refresh is needed
        payload, new_token, error = auth.decode_and_refresh(token)
        if error:
            return jsonify({'status': 'error', 'message': error}), 401

        # Save user context
        g.user_context = payload

        # If token was refreshed, save for after_request hook
        if new_token:
            g.new_token = new_token

        return f(*args, **kwargs)

    return decorated_function


# ============================================================================
# Routes
# ============================================================================
@app.route('/', methods=['GET'])
def home():
    """Root endpoint"""
    return jsonify({
        'message': 'Coframe API Server (Flask + AuthMiddleware)',
        'version': plugins.config.get('version', '0.0.0'),
        'protocol': 'Coframe v2.0',
        'info': '/info',
        'api': f'{api_prefix}/...'
    })


@app.route('/info', methods=['GET'])
def app_info():
    """Application information"""
    result = srv.get_app_info(plugins.config, api_prefix)
    return jsonify(result), result.get('status_code', 200)


# ============================================================================
# Authentication Endpoints
# ============================================================================
@app.route(f'{api_prefix}/auth/login', methods=['POST'])
def login():
    """Login endpoint (using AuthMiddleware)"""
    try:
        data = request.json
        # Use AuthMiddleware.login() for consistent behavior
        result = auth.login(command_processor, data)
        return jsonify(result), result.get('status_code', 200)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route(f'{api_prefix}/auth/update_context', methods=['POST'])
@login_required
def update_context():
    """Update user context and get new token"""
    data = request.json
    result = srv.handle_update_context(
        g.user_context,
        data,
        SECRET_KEY,
        auth.jwt_expiration_hours
    )
    return jsonify(result), result.get('status_code', 200)


# ============================================================================
# Database CRUD Endpoints
# ============================================================================
@app.route(f'{api_prefix}/db/<table>', methods=['GET'])
@login_required
def db_list(table):
    """List all records in table"""
    result = srv.handle_db_operation(
        command_processor,
        'get',
        table,
        context=g.user_context
    )
    return jsonify(result), result.get('status_code', 200)


@app.route(f'{api_prefix}/db/<table>/<id>', methods=['GET'])
@login_required
def db_get(table, id):
    """Get single record by ID"""
    result = srv.handle_db_operation(
        command_processor,
        'get',
        table,
        record_id=id,
        context=g.user_context
    )
    return jsonify(result), result.get('status_code', 200)


@app.route(f'{api_prefix}/db/<table>', methods=['POST'])
@login_required
def db_create(table):
    """Create new record"""
    data = request.json
    result = srv.handle_db_operation(
        command_processor,
        'create',
        table,
        data=data,
        context=g.user_context
    )
    return jsonify(result), result.get('status_code', 200)


@app.route(f'{api_prefix}/db/<table>/<id>', methods=['PUT'])
@login_required
def db_update(table, id):
    """Update existing record"""
    data = request.json
    result = srv.handle_db_operation(
        command_processor,
        'update',
        table,
        record_id=id,
        data=data,
        context=g.user_context
    )
    return jsonify(result), result.get('status_code', 200)


@app.route(f'{api_prefix}/db/<table>/<id>', methods=['DELETE'])
@login_required
def db_delete(table, id):
    """Delete record"""
    result = srv.handle_db_operation(
        command_processor,
        'delete',
        table,
        record_id=id,
        context=g.user_context
    )
    return jsonify(result), result.get('status_code', 200)


# ============================================================================
# Query Endpoint
# ============================================================================
@app.route(f'{api_prefix}/query', methods=['POST'])
@login_required
def query():
    """Execute dynamic query"""
    data = request.json
    result = srv.handle_query(
        command_processor,
        data,
        context=g.user_context
    )
    return jsonify(result), result.get('status_code', 200)


# ============================================================================
# File Reading Endpoint
# ============================================================================
@app.route(f'{api_prefix}/read_file', methods=['POST'])
@login_required
def read_file():
    """Read file from allowed directories"""
    data = request.json
    result = srv.handle_generic_endpoint(
        command_processor,
        'read_file',
        data,
        context=g.user_context
    )
    return jsonify(result), result.get('status_code', 200)


# ============================================================================
# Generic Endpoint Dispatcher
# ============================================================================
@app.route(f'{api_prefix}/endpoint/<operation>', methods=['POST'])
@login_required
def generic_endpoint(operation):
    """Generic endpoint for custom operations"""
    data = request.json
    result = srv.handle_generic_endpoint(
        command_processor,
        operation,
        data,
        context=g.user_context
    )
    return jsonify(result), result.get('status_code', 200)


# ============================================================================
# User Profile Endpoints
# ============================================================================
@app.route(f'{api_prefix}/profile', methods=['GET'])
@login_required
def get_profile():
    """Get current user profile"""
    # Remove sensitive fields
    user_data = {k: v for k, v in g.user_context.items() if k not in ['exp', 'iat', 'last_refresh']}
    return jsonify({
        'status': 'success',
        'data': user_data,
        'status_code': 200
    }), 200


@app.route(f'{api_prefix}/users/me', methods=['GET'])
@login_required
def get_current_user_alias():
    """Alias for get_profile"""
    return get_profile()


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == '__main__':
    # Read port from config (default to 8300 if not specified)
    port = plugins.config.get('api', {}).get('port', 8300)
    print(f"\n🚀 Starting Coframe Flask server (v2) on port {port}")
    print(f"🔄 Auto-refresh enabled: every {auth.refresh_interval_minutes} minutes\n")
    app.run(debug=True, host='0.0.0.0', port=port)

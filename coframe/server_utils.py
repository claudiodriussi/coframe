"""
Framework-agnostic server utilities for Coframe.

All handlers return plain dict with:
- 'status': 'success' | 'error'
- 'data': response payload (on success)
- 'message': error message (on error)
- 'status_code': HTTP status code

This allows the same logic to be used with Flask, FastAPI, Django, or any other framework.
"""

from datetime import datetime, timezone, timedelta
import jwt
from typing import Dict, Any, Optional, Tuple


# ============================================
# JWT Token Management
# ============================================

def decode_and_check_refresh(
    token: str,
    secret_key: str,
    jwt_expiration_hours: int = 24,
    refresh_interval_minutes: int = 20
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Decode JWT token and check if it needs refresh.

    Framework-agnostic function that handles token decoding and automatic
    refresh based on last_refresh timestamp.

    Args:
        token: JWT token string
        secret_key: Secret key for JWT decoding
        jwt_expiration_hours: Token lifetime in hours
        refresh_interval_minutes: Refresh after this inactivity

    Returns:
        Tuple of (payload, new_token, error):
        - payload: Decoded token payload (or None if error)
        - new_token: New refreshed token (or None if not needed)
        - error: Error message (or None if success)

    Example:
        >>> payload, new_token, error = decode_and_check_refresh(
        ...     token='eyJhbGc...',
        ...     secret_key='secret',
        ...     jwt_expiration_hours=24,
        ...     refresh_interval_minutes=20
        ... )
        >>> if error:
        ...     return {'status': 'error', 'message': error, 'status_code': 401}
        >>> if new_token:
        ...     # Include new_token in response
        ...     response['new_token'] = new_token
    """
    try:
        # Decode token
        payload = jwt.decode(token, secret_key, algorithms=['HS256'])

        # Check if refresh is needed
        last_refresh = payload.get('last_refresh', 0)
        now = datetime.now(timezone.utc).timestamp()
        refresh_interval_seconds = refresh_interval_minutes * 60

        new_token = None
        if now - last_refresh > refresh_interval_seconds:
            # Generate new token with extended expiration
            new_payload = {**payload}
            new_payload['exp'] = datetime.now(timezone.utc) + timedelta(hours=jwt_expiration_hours)
            new_payload['last_refresh'] = now
            new_payload.pop('iat', None)  # Remove old issued-at

            new_token = jwt.encode(new_payload, secret_key, algorithm='HS256')

        return payload, new_token, None

    except jwt.ExpiredSignatureError:
        return None, None, 'Token expired'
    except jwt.InvalidTokenError as e:
        return None, None, f'Invalid token: {str(e)}'
    except Exception as e:
        return None, None, f'Token decode error: {str(e)}'


def extract_bearer_token(authorization_header: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract JWT token from Authorization header.

    Framework-agnostic function to parse Bearer token.

    Args:
        authorization_header: Authorization header value (e.g., "Bearer eyJhbGc...")

    Returns:
        Tuple of (token, error):
        - token: Extracted token string (or None if error)
        - error: Error message (or None if success)

    Example:
        >>> token, error = extract_bearer_token(request.headers.get('Authorization'))
        >>> if error:
        ...     return {'status': 'error', 'message': error, 'status_code': 401}
    """
    if not authorization_header:
        return None, 'Missing authorization header'

    if not authorization_header.startswith('Bearer '):
        return None, 'Invalid authorization header format'

    token = authorization_header.split(' ')[1]
    return token, None


# ============================================
# Authentication Handlers
# ============================================

def handle_auth(
    command_processor,
    data: Dict[str, Any],
    secret_key: str,
    jwt_expiration_hours: int = 24,
    context_fields: Optional[list] = None
) -> Dict[str, Any]:
    """
    Framework-agnostic authentication handler.

    Args:
        command_processor: Coframe command processor instance
        data: Request data with 'username' and 'password'
        secret_key: JWT secret key
        jwt_expiration_hours: Token expiration in hours
        context_fields: Fields to include in JWT payload

    Returns:
        Dict with status, token, user, and status_code
    """
    if not data or not data.get('username') or not data.get('password'):
        return {
            'status': 'error',
            'message': 'Username and password are required',
            'status_code': 400
        }

    try:
        command = {
            "operation": "auth",
            "parameters": {
                "username": data['username'],
                "password": data['password']
            }
        }

        result = command_processor.send(command)

        if result.get('status') == 'success':
            # Extract user context from auth result
            user_data = result.get('data', {}).get('context', {})

            # Build JWT payload
            now = datetime.now(timezone.utc)
            payload = {
                'username': user_data.get('username'),
                'exp': now + timedelta(hours=jwt_expiration_hours),
                'last_refresh': now.timestamp()  # Track last refresh for auto-refresh
            }

            # Add context fields to payload
            if context_fields:
                for field in context_fields:
                    if field in user_data:
                        payload[field] = user_data[field]

            # Generate token
            token = jwt.encode(payload, secret_key, algorithm='HS256')

            return {
                'status': 'success',
                'data': {
                    'token': token,
                    'user': user_data
                },
                'status_code': 200
            }
        else:
            return {
                'status': 'error',
                'message': result.get('message', 'Authentication failed'),
                'status_code': 401
            }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e),
            'status_code': 500
        }


def handle_update_context(
    current_context: Dict[str, Any],
    updates: Dict[str, Any],
    secret_key: str,
    jwt_expiration_hours: int = 24
) -> Dict[str, Any]:
    """
    Framework-agnostic context update handler.

    Args:
        current_context: Current user context from JWT
        updates: Fields to update in context
        secret_key: JWT secret key
        jwt_expiration_hours: Token expiration in hours

    Returns:
        Dict with new token and updated context
    """
    try:
        # Merge updates into current context
        new_context = {**current_context}
        new_context.update(updates)

        # Remove 'exp' and 'iat' if present
        new_context.pop('exp', None)
        new_context.pop('iat', None)

        # Add new expiration
        new_context['exp'] = datetime.now(timezone.utc) + timedelta(hours=jwt_expiration_hours)

        # Generate new token
        new_token = jwt.encode(new_context, secret_key, algorithm='HS256')

        return {
            'status': 'success',
            'data': {
                'token': new_token,
                'context': new_context
            },
            'status_code': 200
        }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e),
            'status_code': 500
        }


def handle_db_operation(
    command_processor,
    operation: str,
    table: str,
    record_id: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Framework-agnostic database operation handler.

    Args:
        command_processor: Coframe command processor
        operation: 'get', 'create', 'update', 'delete'
        table: Table name
        record_id: Record ID (for get, update, delete)
        data: Record data (for create, update)
        context: User context

    Returns:
        Dict with status, data, and status_code
    """
    try:
        command = {
            "operation": "db",
            "parameters": {
                "operation": operation,
                "table": table
            }
        }

        if record_id:
            command["parameters"]["id"] = record_id

        if data:
            command["parameters"]["data"] = data

        if context:
            command["context"] = context

        result = command_processor.send(command)

        if result.get('status') == 'success':
            return {
                'status': 'success',
                'data': result.get('data'),
                'status_code': 200
            }
        else:
            return {
                'status': 'error',
                'message': result.get('message', 'Operation failed'),
                'status_code': 400
            }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e),
            'status_code': 500
        }


def handle_query(
    command_processor,
    query_data: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Framework-agnostic query handler.

    Args:
        command_processor: Coframe command processor
        query_data: Query definition (table, fields, filters, etc.)
        context: User context

    Returns:
        Dict with status, data, and status_code
    """
    try:
        command = {
            "operation": "query",
            "parameters": query_data
        }

        if context:
            command["context"] = context

        result = command_processor.send(command)

        if result.get('status') == 'success':
            return {
                'status': 'success',
                'data': result.get('data'),
                'status_code': 200
            }
        else:
            return {
                'status': 'error',
                'message': result.get('message', 'Query failed'),
                'status_code': 400
            }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e),
            'status_code': 500
        }


def handle_generic_endpoint(
    command_processor,
    operation: str,
    data: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Framework-agnostic generic endpoint handler.

    Args:
        command_processor: Coframe command processor
        operation: Endpoint operation name
        data: Operation parameters
        context: User context

    Returns:
        Dict with status, data, and status_code
    """
    try:
        command = {
            "operation": operation,
            "parameters": data
        }

        if context:
            command["context"] = context

        result = command_processor.send(command)

        if result.get('status') == 'success':
            return {
                'status': 'success',
                'data': result.get('data'),
                'status_code': 200
            }
        else:
            return {
                'status': 'error',
                'message': result.get('message', 'Operation failed'),
                'status_code': 400
            }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e),
            'status_code': 500
        }


# ============================================
# Authentication Middleware (Optional Wrapper)
# ============================================

class AuthMiddleware:
    """
    Optional wrapper class for authentication logic.

    Provides a cleaner interface for servers to handle authentication
    with automatic token refresh.

    Example usage in FastAPI:
        >>> auth = AuthMiddleware(plugins.config, SECRET_KEY)
        >>>
        >>> async def get_current_user(request: Request):
        ...     token, error = auth.extract_token(request.headers.get('Authorization'))
        ...     if error:
        ...         raise HTTPException(401, detail=error)
        ...
        ...     payload, new_token, error = auth.decode_and_refresh(token)
        ...     if error:
        ...         raise HTTPException(401, detail=error)
        ...
        ...     if new_token:
        ...         request.state.new_token = new_token
        ...
        ...     return payload
    """

    def __init__(self, config: Dict[str, Any], secret_key: str):
        """
        Initialize auth middleware with configuration.

        Args:
            config: Coframe configuration dict (from plugins.config)
            secret_key: Secret key for JWT encoding/decoding
        """
        self.config = config
        self.secret_key = secret_key

        # Extract auth configuration
        auth_config = config.get('authentication', {})
        self.jwt_expiration_hours = auth_config.get('jwt_expiration_hours', 24)
        self.refresh_interval_minutes = auth_config.get('jwt_refresh_interval_minutes', 20)
        self.context_fields = auth_config.get('context_fields', [])

    def extract_token(self, authorization_header: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract JWT token from Authorization header.

        Returns:
            Tuple of (token, error)
        """
        return extract_bearer_token(authorization_header)

    def decode_and_refresh(self, token: str) -> Tuple[Optional[Dict], Optional[str], Optional[str]]:
        """
        Decode JWT token and check if refresh is needed.

        Returns:
            Tuple of (payload, new_token, error)
        """
        return decode_and_check_refresh(
            token,
            self.secret_key,
            self.jwt_expiration_hours,
            self.refresh_interval_minutes
        )

    def login(self, command_processor, credentials: Dict[str, str]) -> Dict[str, Any]:
        """
        Handle login using configured parameters.

        Args:
            command_processor: Coframe command processor
            credentials: {'username': '...', 'password': '...'}

        Returns:
            Response dict with token and user data
        """
        return handle_auth(
            command_processor,
            credentials,
            self.secret_key,
            self.jwt_expiration_hours,
            self.context_fields
        )


# ============================================
# App Info Handler
# ============================================

def get_app_info(plugins_config: Dict[str, Any], api_prefix: str) -> Dict[str, Any]:
    """
    Framework-agnostic app info handler.

    Args:
        plugins_config: Plugins configuration dict
        api_prefix: API prefix (e.g., '/coframe' or '/api/v1')

    Returns:
        Dict with app information
    """
    return {
        'status': 'success',
        'data': {
            'application': plugins_config.get('name', 'Unknown'),
            'version': plugins_config.get('version', '0.0.0'),
            'description': plugins_config.get('description', ''),
            'coframe_api_prefix': api_prefix,
            'available_endpoints': {
                'home': '/',
                'app_info': '/info',
                'coframe_auth': f'{api_prefix}/auth/login',
                'coframe_auth_update': f'{api_prefix}/auth/update_context',
                'coframe_database': f'{api_prefix}/db/<table>',
                'coframe_query': f'{api_prefix}/query',
                'coframe_files': f'{api_prefix}/read_file',
                'coframe_commands': f'{api_prefix}/endpoint/<operation>'
            }
        },
        'status_code': 200
    }

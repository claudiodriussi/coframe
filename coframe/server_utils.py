"""
Framework-agnostic server utilities for Coframe.

All handlers return plain dict with:
- 'status': 'success' | 'error'
- 'data': response payload (on success)
- 'message': error message (on error)
- 'status_code': HTTP status code

This allows the same logic to be used with Flask, FastAPI, Django, or any other framework.
"""

from datetime import datetime, timezone, timedelta, date
import traceback as _traceback
import jwt
from typing import Dict, Any, Optional, Tuple


def _error_response(message: str, status_code: int = 500,
                    error_type: Optional[str] = None,
                    traceback: Optional[str] = None) -> Dict[str, Any]:
    """Build a uniform error response dict."""
    r: Dict[str, Any] = {'status': 'error', 'message': message, 'status_code': status_code}
    if error_type:
        r['error_type'] = error_type
    if traceback:
        r['traceback'] = traceback
    return r


def _error_from_exc(e: Exception, status_code: int = 500) -> Dict[str, Any]:
    """Build error response from a live exception (captures current traceback)."""
    return _error_response(
        message=str(e),
        status_code=status_code,
        error_type=type(e).__name__,
        traceback=_traceback.format_exc()
    )


def _error_from_result(result: Dict[str, Any], default_message: str = 'Operation failed',
                       status_code: int = 400) -> Dict[str, Any]:
    """Build error response propagating traceback from a CommandResult dict."""
    return _error_response(
        message=result.get('message', default_message),
        status_code=status_code,
        error_type=result.get('error_type'),
        traceback=result.get('traceback')
    )


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
                'last_refresh': now.timestamp(),  # Track last refresh for auto-refresh
                # Operational ("working") date — a framework context field, always
                # present, so every endpoint can read it via the context (default
                # date for new records, accounting period selection, ...). The user
                # can override it via update_context; the merged value survives
                # auto-refresh. Default = server system date.
                # Seam: default source/timezone configurable later via env.config.
                'op_date': date.today().isoformat(),
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
        return _error_from_exc(e)


# Context fields the client may always set via update_context, regardless of
# app config. These are framework-owned, non-identity fields.
FRAMEWORK_UPDATABLE_FIELDS = frozenset({'op_date'})


def custom_context_fields(config: Dict[str, Any]) -> list:
    """
    App-defined context fields a client is allowed to set via update_context:
    configured `context_fields` that are NOT columns of the user table.

    Identity columns (id, email, is_active, is_admin, ...) come from the user
    record and must stay server-authoritative — a client must never be able to
    set them in its own token. Requires the coframe app to be loaded.
    """
    import coframe.utils
    app = coframe.utils.get_app()
    auth = config.get('authentication', {})
    user_model = app.models.get(auth.get('user_table', 'User'))
    if user_model is None:
        return []
    user_cols = {c.key for c in user_model.__table__.columns}
    return [f for f in auth.get('context_fields', []) if f not in user_cols]


def handle_update_context(
    current_context: Dict[str, Any],
    updates: Dict[str, Any],
    secret_key: str,
    jwt_expiration_hours: int = 24,
    allowed_fields: Optional[list] = None
) -> Dict[str, Any]:
    """
    Framework-agnostic context update handler.

    Args:
        current_context: Current user context from JWT
        updates: Fields to update in context (filtered against the allowlist)
        secret_key: JWT secret key
        jwt_expiration_hours: Token expiration in hours
        allowed_fields: App-defined fields the client may set. Framework fields
            (FRAMEWORK_UPDATABLE_FIELDS) are always allowed. Any other key in
            `updates` is dropped — this is the guard against a client escalating
            its own token (e.g. sending is_admin=True).

    Returns:
        Dict with new token and updated context
    """
    try:
        # Allowlist: only framework fields + app-declared custom fields may be
        # set by the client. Everything else (identity, JWT machinery) is dropped.
        allowed = set(FRAMEWORK_UPDATABLE_FIELDS)
        if allowed_fields:
            allowed |= set(allowed_fields)
        filtered = {k: v for k, v in (updates or {}).items() if k in allowed}

        # Merge the filtered updates into current context
        new_context = {**current_context}
        new_context.update(filtered)

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
        return _error_from_exc(e)


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
            return {'status': 'success', 'data': result.get('data'), 'status_code': 200}
        else:
            return _error_from_result(result, 'Operation failed')

    except Exception as e:
        return _error_from_exc(e)


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
            return {'status': 'success', 'data': result.get('data'), 'status_code': 200}
        else:
            return _error_from_result(result, 'Query failed')

    except Exception as e:
        return _error_from_exc(e)


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
            response = {'status': 'success', 'data': result.get('data'), 'status_code': 200}
            # What the endpoint had to say about what it did: an operation called
            # from a button reports in one line, and the envelope carries it.
            if result.get('message'):
                response['message'] = result['message']
            return response
        else:
            return _error_from_result(result, 'Operation failed')

    except Exception as e:
        return _error_from_exc(e)


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

    def update_context(self, current_context: Dict[str, Any],
                       updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a context update: reissue a JWT with the (allowlisted) updates.

        The allowlist is framework fields + this app's custom context fields, so
        a client can set op_date / declared custom fields but never identity.
        """
        return handle_update_context(
            current_context,
            updates,
            self.secret_key,
            self.jwt_expiration_hours,
            allowed_fields=custom_context_fields(self.config)
        )


# ============================================
# Flask JSON date/time serialization fix
# ============================================

def configure_flask_json_dates(app) -> None:
    """
    Make Flask serialize date/datetime/time as ISO 8601 (Flask only).

    Flask's DefaultJSONProvider serializes date/datetime via http_date()
    (RFC 1123, e.g. "Thu, 16 Jul 2026 13:21:19 GMT") instead of ISO — but the
    `db`/`query` endpoints only accept ISO 8601 back on write
    (datetime.fromisoformat in endpoint_db.py/utils.py). Round-tripping an
    unmodified date field through a form (GET -> display -> save unmodified)
    then crashes SQLAlchemy. Use ISO on the way out too, matching
    querybuilder.JSONEncoder's convention already used elsewhere.

    FastAPI's default encoder (jsonable_encoder) already emits ISO 8601, so
    it needs no equivalent call.

    Usage:
        >>> app = Flask(__name__)
        >>> configure_flask_json_dates(app)
    """
    from datetime import date, datetime, time
    from flask.json.provider import DefaultJSONProvider

    class ISOJSONProvider(DefaultJSONProvider):
        @staticmethod
        def default(o):
            if isinstance(o, (datetime, date, time)):
                return o.isoformat()
            return DefaultJSONProvider.default(o)

    app.json = ISOJSONProvider(app)


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


# ============================================
# Route registration
# ============================================
#
# The canonical routes — login, context refresh, the dispatcher that carries
# every other operation, and info — registered on whatever the caller hands
# over. `app.route` and `Blueprint.route` (and `after_request`) share a
# signature, so one function serves both cases and the caller's choice of
# target is what decides the scope:
#
#     srv.register_flask(app, coframe_app, plugins, SECRET_KEY)
#
#         coframe owns the process: the routes and the after_request hook are
#         the application's, which is what a standalone server wants.
#
#     bp = Blueprint('coframe', __name__)
#     srv.register_flask(bp, coframe_app, plugins, SECRET_KEY)
#     host_app.register_blueprint(bp)
#
#         coframe is a guest: everything registered here lives inside the
#         blueprint, and the host's own routes and hooks are untouched.
#
# Neither form touches anything outside its target — no CORS, no static
# catch-all, no route at the root, and no replacement of the application's
# JSON provider: coframe's own responses are serialized here, with the ISO
# 8601 dates its endpoints accept back on write.

def _prefixes(plugins_config: Dict[str, Any],
              prefix: Optional[str],
              endpoint_prefix: Optional[str]) -> Tuple[str, str]:
    """Resolve the API prefixes, falling back to config.yaml."""
    api = plugins_config.get('api', {})
    if prefix is None:
        prefix = '/' + api.get('prefix', 'coframe').strip('/')
    if endpoint_prefix is None:
        endpoint_prefix = api.get('endpoint_prefix', 'endpoint').strip('/')
    return prefix.rstrip('/'), endpoint_prefix.strip('/')


def register_flask(target, coframe_app, plugins, secret_key: str, *,
                   prefix: Optional[str] = None,
                   endpoint_prefix: Optional[str] = None,
                   auth: Optional['AuthMiddleware'] = None) -> 'AuthMiddleware':
    """
    Register coframe's routes on a Flask application or Blueprint.

    Args:
        target:          a Flask app or a Blueprint — anything with .route()
                         and .after_request()
        coframe_app:     the coframe application (BaseApp)
        plugins:         the PluginsManager
        secret_key:      key the JWT is signed with
        prefix:          path the routes hang from, config.yaml's `api.prefix`
                         by default. Pass '' when the Blueprint carries its own
                         url_prefix and the host decides where it is mounted.
        endpoint_prefix: dispatcher segment, `api.endpoint_prefix` by default
        auth:            an AuthMiddleware to share with the rest of the
                         process — a server-rendered page that logs a person in
                         through the same identity passes the one it has

    Returns:
        The AuthMiddleware in use, so the caller can authenticate by other
        doors without building a second one.
    """
    import json
    from functools import wraps
    from flask import Response, g, request

    from coframe.db import BaseApp
    from coframe.i18n import set_locale
    from coframe.querybuilder import JSONEncoder

    config = plugins.config
    prefix, endpoint_prefix = _prefixes(config, prefix, endpoint_prefix)
    auth = auth or AuthMiddleware(config, secret_key)
    command_processor = coframe_app.cp

    def reply(result: Dict[str, Any], status_code: Optional[int] = None) -> Any:
        """Send a handler result with the status code it carries.

        Deliberately not `jsonify`: that goes through the application's JSON
        provider — the host's, in a mounted deployment — and inherits both its
        date format and its key sorting, which raises on the mixed-type keys
        some descriptors carry.
        """
        if status_code is None:
            status_code = result.get('status_code', result.get('code', 200))
        return Response(json.dumps(result, cls=JSONEncoder, sort_keys=False),
                        status=status_code, mimetype='application/json')

    @target.teardown_request
    def coframe_clear_context(exception=None):
        """Leave the thread as it was found.

        Every dispatch sets the context of the user it serves, so within
        coframe's own surface a leftover is overwritten. It is a guest that
        pays for it: the worker goes back to the pool carrying an identity, and
        whatever the host serves next on that thread — a page, another API —
        inherits a user nobody chose, with the query behaviors filtering
        accordingly.
        """
        BaseApp.set_context(None)

    @target.after_request
    def coframe_token_refresh(response):
        """Hand a refreshed token back in X-New-Token whenever one was issued."""
        if hasattr(g, 'coframe_new_token'):
            response.headers['X-New-Token'] = g.coframe_new_token
        return response

    def authenticated(view):
        """Validate the bearer token, refresh it when due, set the locale."""

        @wraps(view)
        def wrapper(*args, **kwargs):
            token, error = auth.extract_token(request.headers.get('Authorization'))
            if error:
                return reply({'status': 'error', 'message': error}, 401)

            payload, new_token, error = auth.decode_and_refresh(token)
            if error:
                return reply({'status': 'error', 'message': error}, 401)
            if new_token:
                g.coframe_new_token = new_token

            g.user_context = payload
            set_locale(payload.get('locale') or config.get('locale', 'en'))
            return view(*args, **kwargs)

        return wrapper

    @target.route(f'{prefix}/info', methods=['GET'])
    def coframe_info():
        return reply(get_app_info(config, prefix))

    @target.route(f'{prefix}/auth/login', methods=['POST'])
    def coframe_login():
        try:
            return reply(auth.login(command_processor, request.json))
        except Exception as e:
            return reply({'status': 'error', 'message': str(e)}, 500)

    @target.route(f'{prefix}/auth/update_context', methods=['POST'])
    @authenticated
    def coframe_update_context():
        return reply(auth.update_context(g.user_context, request.json))

    @target.route(f'{prefix}/{endpoint_prefix}/<operation>', methods=['POST'])
    @authenticated
    def coframe_dispatch(operation: str):
        """Everything that is not authentication: db, query, get_page, get_menu…"""
        return reply(handle_generic_endpoint(
            command_processor, operation, request.json, context=g.user_context))

    return auth


def register_fastapi(target, coframe_app, plugins, secret_key: str, *,
                     prefix: Optional[str] = None,
                     endpoint_prefix: Optional[str] = None,
                     auth: Optional['AuthMiddleware'] = None) -> 'AuthMiddleware':
    """
    Register coframe's routes on a FastAPI application or APIRouter.

    Same arguments and same four routes as `register_flask`. The refreshed
    token is written on the response the dependency is handed, so no
    application-wide middleware is needed to carry it out — which is what lets
    an APIRouter be a valid target.
    """
    from fastapi import Depends, HTTPException, Request, Response

    from coframe.db import BaseApp
    from coframe.i18n import set_locale

    config = plugins.config
    prefix, endpoint_prefix = _prefixes(config, prefix, endpoint_prefix)
    auth = auth or AuthMiddleware(config, secret_key)
    command_processor = coframe_app.cp

    async def clear_context():
        """Leave the worker as it was found — see register_flask's teardown."""
        yield
        BaseApp.set_context(None)

    cleanup = [Depends(clear_context)]

    async def current_user(request: Request, response: Response) -> dict:
        """Validate the bearer token, refresh it when due, set the locale."""
        token, error = auth.extract_token(request.headers.get('authorization'))
        if error:
            raise HTTPException(status_code=401, detail=error)

        payload, new_token, error = auth.decode_and_refresh(token)
        if error:
            raise HTTPException(status_code=401, detail=error)
        if new_token:
            response.headers['X-New-Token'] = new_token

        set_locale(payload.get('locale') or config.get('locale', 'en'))
        return payload

    @target.get(f'{prefix}/info', dependencies=cleanup)
    def coframe_info():
        return get_app_info(config, prefix)

    @target.post(f'{prefix}/auth/login', dependencies=cleanup)
    def coframe_login(data: dict):
        try:
            return auth.login(command_processor, data)
        except Exception as e:
            return {'status': 'error', 'message': str(e), 'status_code': 500}

    @target.post(f'{prefix}/auth/update_context', dependencies=cleanup)
    def coframe_update_context(data: dict, user: dict = Depends(current_user)):
        return auth.update_context(user, data)

    @target.post(f'{prefix}/{endpoint_prefix}/{{operation}}', dependencies=cleanup)
    def coframe_dispatch(operation: str, data: dict, user: dict = Depends(current_user)):
        """Everything that is not authentication: db, query, get_page, get_menu…"""
        return handle_generic_endpoint(command_processor, operation, data, context=user)

    return auth

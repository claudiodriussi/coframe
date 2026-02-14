import sys
import datetime
import os
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from functools import wraps
import jwt
sys.path.append("..")
import model  # noqa: E402
import coframe  # noqa: E402
from coframe.utils import get_app, seek  # noqa: E402

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'development-secret-key')
app.config['JWT_EXPIRATION_DELTA'] = datetime.timedelta(days=1)
CORS(app)

# Initialize Coframe
plugins = coframe.plugins.PluginsManager()
plugins.load_config("config.yaml")
coframe.utils.register_standard_handlers(plugins)  # Register column merge handlers
plugins.load_plugins()

# Initialize database and command processor
coframe_app = get_app()
coframe_app.calc_db(plugins)

# Load model
db_url = 'sqlite:///devtest.sqlite'  # Configure your database URL here
coframe_app.initialize_db(db_url, model)
command_processor = coframe_app.cp

# Get API prefix from configuration
api_prefix = f"/{plugins.config.get('api', {}).get('prefix', 'api')}/api"
print(f"Coframe API endpoints will be available under: {api_prefix}")


# Authentication decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = None

        # Check if token is in the Authorization header
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            if auth_header.startswith('Bearer '):
                token = auth_header.split(' ')[1]

        # If no token provided
        if not token:
            return jsonify({'message': 'Authentication token is missing!'}), 401

        try:
            # Decode the token
            payload = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            g.user_context = payload
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Authentication token has expired!'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid authentication token!'}), 401

        return f(*args, **kwargs)

    return decorated_function


# Local application routes (not part of Coframe)
@app.route('/info', methods=['GET'])
def app_info():
    """Application Information endpoint - local endpoint"""
    return jsonify({
        'status': 'success',
        'data': {
            'application': plugins.config.get('name', 'Unknown'),
            'version': plugins.config.get('version', '0.0.0'),
            'description': plugins.config.get('description', ''),
            'coframe_api_prefix': api_prefix,
            'available_endpoints': {
                'app_info': '/info',
                'coframe_auth': f'{api_prefix}/auth/login',
                'coframe_database': f'{api_prefix}/db/<table>',
                'coframe_query': f'{api_prefix}/query',
                'coframe_files': f'{api_prefix}/read_file',
                'coframe_commands': f'{api_prefix}/endpoint/<operation>'
            }
        }
    })


# Coframe routes (using configured prefix)
@app.route(f'{api_prefix}/auth/login', methods=['POST'])
def login():
    """Login endpoint"""
    data = request.json

    if not data or not data.get('username') or not data.get('password'):
        return jsonify({'message': 'Username and password are required'}), 400

    try:
        # Create authentication command
        command = {
            "operation": "auth",
            "parameters": {
                "username": data.get('username'),
                "password": data.get('password')
            }
        }

        # Execute command
        result = command_processor.send(command)

        if result.get('status') != 'success':
            return jsonify({'message': 'Invalid credentials'}), 401

        # Get user context from result
        user_context = result.get('data', {}).get('context', {})

        # Create JWT token
        token_payload = {
            'user_id': user_context.get('id'),
            'username': data.get('username'),
            'exp': datetime.datetime.utcnow() + app.config['JWT_EXPIRATION_DELTA']
        }

        # Add any additional context to the token
        for key, value in user_context.items():
            if key != 'id':  # Already added
                token_payload[key] = value

        token = jwt.encode(token_payload, app.config['SECRET_KEY'], algorithm='HS256')

        return jsonify({
            'token': token,
            'user': user_context
        }), 200

    except Exception as e:
        app.logger.error(f"Login error: {str(e)}")
        return jsonify({'message': 'An error occurred during authentication'}), 500


@app.route(f'{api_prefix}/auth/update_context', methods=['POST'])
@login_required
def update_user_context():
    """Update user context and generate a new token"""
    try:
        data = request.json

        # Create command to update context
        command = {
            "operation": "update_context",
            "parameters": data,
            "context": g.user_context
        }

        # Execute command
        result = command_processor.send(command)

        # Check if operation was successful
        if result.get('status') != 'success':
            return jsonify(result), result.get('code', 400)

        # Get updated context from result
        updated_context = result.get('data', {}).get('context', {})

        # Create new JWT token with updated context
        token_payload = {
            'exp': datetime.datetime.utcnow() + app.config['JWT_EXPIRATION_DELTA']
        }

        # Add all context fields to the token
        for key, value in updated_context.items():
            token_payload[key] = value

        # Generate the token
        new_token = jwt.encode(token_payload, app.config['SECRET_KEY'], algorithm='HS256')

        return jsonify({
            'status': 'success',
            'token': new_token,
            'context': updated_context
        }), 200

    except Exception as e:
        app.logger.error(f"Context update error: {str(e)}")
        return jsonify({'message': 'An error occurred while updating context'}), 500


@app.route(f'{api_prefix}/db/<table>', methods=['GET'])
@login_required
def db_get_all(table):
    """Get all records from a table"""
    try:
        command = {
            "operation": "db",
            "parameters": {
                "table": table,
                "method": "get",
                "start": int(request.args.get('start', 0)),
                "limit": int(request.args.get('limit', 100)),
                "order_by": request.args.get('order_by'),
                "order_dir": request.args.get('order_dir', 'asc')
            },
            "context": g.user_context
        }

        result = command_processor.send(command)
        return jsonify(result), result.get('code', 200)

    except Exception as e:
        app.logger.error(f"Error in db_get_all: {str(e)}")
        return jsonify({'message': str(e)}), 500


@app.route(f'{api_prefix}/db/<table>/<id>', methods=['GET'])
@login_required
def db_get_one(table, id):
    """Get a single record by ID"""
    try:
        command = {
            "operation": "db",
            "parameters": {
                "table": table,
                "method": "get",
                "id": id
            },
            "context": g.user_context
        }

        result = command_processor.send(command)
        return jsonify(result), result.get('code', 200)

    except Exception as e:
        app.logger.error(f"Error in db_get_one: {str(e)}")
        return jsonify({'message': str(e)}), 500


@app.route(f'{api_prefix}/db/<table>', methods=['POST'])
@login_required
def db_create(table):
    """Create a new record"""
    try:
        data = request.json

        # convert json if needed
        data = coframe.utils.json_to_model_types(data, table)
        command = {
            "operation": "db",
            "parameters": {
                "table": table,
                "method": "create",
                "data": data
            },
            "context": g.user_context
        }

        result = command_processor.send(command)
        return jsonify(result), result.get('code', 201)

    except Exception as e:
        app.logger.error(f"Error in db_create: {str(e)}")
        return jsonify({'message': str(e)}), 500


@app.route(f'{api_prefix}/db/<table>/<id>', methods=['PUT'])
@login_required
def db_update(table, id):
    """Update a record"""
    try:
        data = request.json

        # convert json if needed
        data = coframe.utils.json_to_model_types(data, table)
        command = {
            "operation": "db",
            "parameters": {
                "table": table,
                "method": "update",
                "id": id,
                "data": data
            },
            "context": g.user_context
        }

        result = command_processor.send(command)
        return jsonify(result), result.get('code', 200)

    except Exception as e:
        app.logger.error(f"Error in db_update: {str(e)}")
        return jsonify({'message': str(e)}), 500


@app.route(f'{api_prefix}/db/<table>/<id>', methods=['DELETE'])
@login_required
def db_delete(table, id):
    """Delete a record"""
    try:
        command = {
            "operation": "db",
            "parameters": {
                "table": table,
                "method": "delete",
                "id": id
            },
            "context": g.user_context
        }

        result = command_processor.send(command)
        return jsonify(result), result.get('code', 200)

    except Exception as e:
        app.logger.error(f"Error in db_delete: {str(e)}")
        return jsonify({'message': str(e)}), 500


@app.route(f'{api_prefix}/query', methods=['POST'])
@login_required
def execute_query():
    """Execute a dynamic query"""
    try:
        data = request.json

        command = {
            "operation": "query",
            "parameters": {
                "format": data.get('format', 'tuples'),
                "query": data.get('query')
            },
            "context": g.user_context
        }

        result = command_processor.send(command)
        return jsonify(result), result.get('code', 200)

    except Exception as e:
        app.logger.error(f"Error in execute_query: {str(e)}")
        return jsonify({'message': str(e)}), 500


@app.route(f'{api_prefix}/read_file', methods=['POST'])
@login_required
def read_file():
    """Read a file from filesystem, it recognize some suffixes"""
    try:
        data = request.json

        command = {
            "operation": "read_file",
            "parameters": data,
            "context": g.user_context
        }
        result = command_processor.send(command)
        return jsonify(result), result.get('code', 200)

    except Exception as e:
        app.logger.error(f"Error in generic_endpoint: {str(e)}")
        return jsonify({'message': str(e)}), 500


@app.route(f'{api_prefix}/endpoint/<operation>', methods=['POST'])
@login_required
def generic_endpoint(operation):
    """Generic endpoint that can call any Coframe operation"""
    try:
        data = request.json

        command = {
            "operation": operation,
            "parameters": data,
            "context": g.user_context
        }

        result = command_processor.send(command)
        return jsonify(result), result.get('code', 200)

    except Exception as e:
        app.logger.error(f"Error in generic_endpoint: {str(e)}")
        return jsonify({'message': str(e)}), 500


@app.route(f'{api_prefix}/profile', methods=['GET'])
@login_required
def get_profile():
    """Get current user profile"""
    try:
        user_id = g.user_context.get('user_id')
        user = seek('User', {'id': user_id})

        if not user:
            return jsonify({'message': 'User not found'}), 404

        # Convert to dictionary (without sensitive fields)
        from coframe.utils import serialize_model
        user_dict = serialize_model(user)

        # Remove sensitive fields
        if 'password' in user_dict:
            del user_dict['password']

        return jsonify({
            'status': 'success',
            'data': user_dict
        }), 200

    except Exception as e:
        app.logger.error(f"Error in get_profile: {str(e)}")
        return jsonify({'message': str(e)}), 500


@app.route(f'{api_prefix}/users/me', methods=['GET'])
@login_required
def get_current_user():
    """Alias for get_profile"""
    return get_profile()


# Run the application
if __name__ == '__main__':
    # Read port from config (default to 8300 if not specified)
    port = plugins.config.get('api', {}).get('port', 8300)
    print(f"Starting Coframe Flask server on port {port}")
    app.run(debug=True, host='0.0.0.0', port=port)

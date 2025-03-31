import coframe
from coframe.endpoints import endpoint
from coframe.querybuilder import DynamicQueryBuilder
from typing import Dict, Any, Optional
from sqlalchemy import and_, or_, desc, asc


@endpoint('db')
def db_operations(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generic database CRUD endpoint.

    Parameters:
        - table: Name of the table/model to operate on
        - method: Operation to perform (get, create, update, delete)
        - id: Optional ID for single record operations
        - data: Data for create/update operations
        - query: Query parameters for filtering (dict with field:value pairs)
        - start: Pagination start index (default: 0)
        - limit: Pagination limit (default: 100)
        - order_by: Field to order by
        - order_dir: Direction of ordering ('asc' or 'desc')

    Returns:
        Dictionary with operation results
    """
    try:
        # Validate required parameters
        table_name = data.get('table')
        method = data.get('method', 'get').lower()

        if not table_name:
            return {"status": "error", "message": "Table name is required", "code": 400}

        # Get database app and model class
        app = coframe.utils.get_app()
        model_class = app.find_model_class(table_name)

        if not model_class:
            return {"status": "error", "message": f"Table '{table_name}' not found", "code": 404}

        # Execute the requested method
        if method == 'get':
            return handle_get(app, model_class, data)
        elif method == 'create':
            return handle_create(app, model_class, data)
        elif method == 'update':
            return handle_update(app, model_class, data)
        elif method == 'delete':
            return handle_delete(app, model_class, data)
        else:
            return {"status": "error", "message": f"Unsupported method: '{method}'", "code": 400}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e), "code": 500}


def handle_get(app, model_class, params: Dict[str, Any]) -> Dict[str, Any]:
    """Handle GET operations (list or single record)"""
    record_id = params.get('id')
    start = int(params.get('start', 0))
    limit = int(params.get('limit', 100))
    query_filters = params.get('query', {})
    order_by = params.get('order_by')
    order_dir = params.get('order_dir', 'asc')

    with app.get_session() as session:
        if record_id:
            # Get single record
            record = session.query(model_class).get(record_id)
            if not record:
                return {"status": "error", "message": f"Record with id {record_id} not found", "code": 404}

            return {
                "status": "success",
                "data": coframe.utils.serialize_model(record),
                "code": 200
            }
        else:
            # List records with filtering and pagination
            query = session.query(model_class)

            # Apply filters
            if query_filters:
                filter_conditions = build_filters(model_class, query_filters)
                if filter_conditions:
                    query = query.filter(filter_conditions)

            # Apply ordering
            if order_by:
                column = getattr(model_class, order_by, None)
                if column:
                    if order_dir.lower() == 'desc':
                        query = query.order_by(desc(column))
                    else:
                        query = query.order_by(asc(column))

            # Get total count (before pagination)
            total_count = query.count()

            # Apply pagination
            query = query.offset(start).limit(limit)

            # Execute query and serialize results
            records = query.all()
            result = [coframe.utils.serialize_model(record) for record in records]

            return {
                "status": "success",
                "data": {
                    "records": result,
                    "total": total_count,
                    "start": start,
                    "limit": limit
                },
                "code": 200
            }


def handle_create(app, model_class, params: Dict[str, Any]) -> Dict[str, Any]:
    """Handle CREATE operations"""
    record_data = params.get('data')

    if not record_data:
        return {"status": "error", "message": "No data provided for creation", "code": 400}

    # Create new instance
    try:
        new_record = model_class(**record_data)

        with app.get_session() as session:
            session.add(new_record)
            session.commit()

            # Refresh to get generated IDs and other database defaults
            session.refresh(new_record)

            return {
                "status": "success",
                "data": coframe.utils.serialize_model(new_record),
                "message": "Record created successfully",
                "code": 201
            }
    except Exception as e:
        return {"status": "error", "message": f"Creation failed: {str(e)}", "code": 400}


def handle_update(app, model_class, params: Dict[str, Any]) -> Dict[str, Any]:
    """Handle UPDATE operations"""
    record_id = params.get('id')
    if not record_id:
        return {"status": "error", "message": "Record ID is required for updates", "code": 400}

    record_data = params.get('data')
    if not record_data:
        return {"status": "error", "message": "No data provided for update", "code": 400}

    with app.get_session() as session:
        # Find the record
        record = session.query(model_class).get(record_id)
        if not record:
            return {"status": "error", "message": f"Record with id {record_id} not found", "code": 404}

        # Update record attributes
        for key, value in record_data.items():
            if hasattr(record, key):
                setattr(record, key, value)

        try:
            session.commit()
            return {
                "status": "success",
                "data": coframe.utils.serialize_model(record),
                "message": "Record updated successfully",
                "code": 200
            }
        except Exception as e:
            session.rollback()
            return {"status": "error", "message": f"Update failed: {str(e)}", "code": 400}


def handle_delete(app, model_class, params: Dict[str, Any]) -> Dict[str, Any]:
    """Handle DELETE operations"""
    record_id = params.get('id')
    if not record_id:
        return {"status": "error", "message": "Record ID is required for deletion", "code": 400}

    with app.get_session() as session:
        # Find the record
        record = session.query(model_class).get(record_id)
        if not record:
            return {"status": "error", "message": f"Record with id {record_id} not found", "code": 404}

        try:
            session.delete(record)
            session.commit()
            return {
                "status": "success",
                "message": "Record deleted successfully",
                "code": 200
            }
        except Exception as e:
            session.rollback()
            return {"status": "error", "message": f"Deletion failed: {str(e)}", "code": 400}


def build_filters(model_class, query_filters: Dict[str, Any]) -> Optional[Any]:
    """
    Build SQLAlchemy filter conditions from query parameters.

    Supports:
    - Exact match: {"field": value}
    - Operators: {"field__op": value} where op can be:
      - eq: Equal
      - neq: Not equal
      - gt: Greater than
      - gte: Greater than or equal
      - lt: Less than
      - lte: Less than or equal
      - like: LIKE pattern
      - ilike: Case-insensitive LIKE
      - in: IN a list of values
      - between: Between two values (provide [min, max] list)
    - Logical OR: {"$or": [{condition1}, {condition2}, ...]}
    """
    if not query_filters:
        return None

    conditions = []

    # Handle special $or operator
    if "$or" in query_filters:
        or_conditions = []
        for or_filter in query_filters["$or"]:
            or_condition = build_filters(model_class, or_filter)
            if or_condition is not None:
                or_conditions.append(or_condition)

        if or_conditions:
            conditions.append(or_(*or_conditions))

        # Remove $or from further processing
        query_filters = {k: v for k, v in query_filters.items() if k != "$or"}

    # Process standard filters
    for key, value in query_filters.items():
        if '__' in key:
            field, operator = key.split('__', 1)
        else:
            field, operator = key, 'eq'

        if not hasattr(model_class, field):
            continue

        column = getattr(model_class, field)

        if operator == 'eq':
            conditions.append(column == value)
        elif operator == 'neq':
            conditions.append(column != value)
        elif operator == 'gt':
            conditions.append(column > value)
        elif operator == 'gte':
            conditions.append(column >= value)
        elif operator == 'lt':
            conditions.append(column < value)
        elif operator == 'lte':
            conditions.append(column <= value)
        elif operator == 'like':
            conditions.append(column.like(value))
        elif operator == 'ilike':
            conditions.append(column.ilike(value))
        elif operator == 'in':
            conditions.append(column.in_(value))
        elif operator == 'between':
            # Value should be a list/tuple with exactly 2 elements [min, max]
            if isinstance(value, (list, tuple)) and len(value) == 2:
                conditions.append(column.between(value[0], value[1]))

    if conditions:
        return and_(*conditions)
    return None


@endpoint('query')
def db_query(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a query in DynamicQueryBuilder format

    Parameters:
        - format: The desired format for the result from the ones provided by execute_query method
        - query: Dict for DynamicQueryBuilder

    Returns:
        Dictionary with query result
    """
    format = data.get("format", "tuples")
    query = data.get("query")
    if not query:
        return {"status": "error", "message": "Query not defined", "code": 400}
    try:
        app = coframe.utils.get_app()
        with app.get_session() as session:
            builder = DynamicQueryBuilder(session, app.models)
            data = builder.execute_query(query, result_format=format)
            return {
                "status": "success",
                "data": data
            }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e), "code": 500}


@endpoint('auth')
def authenticate(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Authenticate a user and return a context for subsequent operations.

    Parameters:
        - username: User identifier
        - password: User credential

    Returns:
        Dictionary with authentication result and context
    """
    try:
        username = data.get('username')
        password = data.get('password')

        if not username or not password:
            return {
                "status": "error",
                "message": "Username and password are required",
                "code": 400
            }

        # Get authentication configuration
        app = coframe.utils.get_app()
        config = app.pm.config.get('authentication', {})
        user_table = config.get('user_table', 'User')
        name_field = config.get('username_field', 'username')
        pass_field = config.get('password_field', 'password')
        context_fields = config.get('context_fields', ['id'])

        # Find the user
        user = coframe.utils.seek(user_table, {name_field: username})

        if not user:
            return {
                "status": "error",
                "message": "Invalid credentials",
                "code": 401
            }

        # Verify password - production version
        '''
        hashed_password = getattr(user, pass_field)
        if not bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8')):
            return {
                "status": "error",
                "message": "Invalid credentials",
                "code": 401
            }
        '''
        # Verify password - For development/testing
        if getattr(user, pass_field) != password:
            return {
                "status": "error",
                "message": "Invalid credentials",
                "code": 401
            }

        # Check if user is active
        if hasattr(user, 'is_active') and not user.is_active:
            return {
                "status": "error",
                "message": "Account is inactive",
                "code": 401
            }

        # Build context with selected fields
        context = {}
        for field in context_fields:
            if hasattr(user, field):
                context[field] = getattr(user, field)
        context['username'] = username

        return {
            "status": "success",
            "data": {
                "authenticated": True,
                "context": context
            },
            "code": 200
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e), "code": 500}

import datetime
from typing import List, Dict, Any
from sqlalchemy import inspection
import coframe


def get_app():
    """
    Get the current DB application instance.

    This function centralizes access to the application instance,
    making it easier to change the implementation if needed.

    Returns:
        The current DB application instance
    """
    return coframe.db.Base.__coframe_app__


def serialize_model(model, include_relationships=False):
    """
    Convert SQLAlchemy model instance to dictionary.

    Args:
        model: SQLAlchemy model instance
        include_relationships: Whether to include relationship attributes

    Returns:
        Dictionary representation of the model
    """
    result = {}
    # Add columns
    for column in inspection.inspect(model.__class__).columns:
        result[column.name] = getattr(model, column.name)

    # Optionally add relationships
    if include_relationships:
        for relationship in inspection.inspect(model.__class__).relationships:
            rel_name = relationship.key
            rel_value = getattr(model, rel_name)

            # Handle different types of relationships
            if rel_value is None:
                result[rel_name] = None
            elif isinstance(rel_value, list):
                # Many relationship - serialize IDs only
                result[rel_name] = [item.id if hasattr(item, 'id') else str(item) for item in rel_value]
            else:
                # Single relationship - serialize ID only
                result[rel_name] = rel_value.id if hasattr(rel_value, 'id') else str(rel_value)

    return result


def seek(table_name, filters):
    """
    Generic seek function to find a record by filters.

    Args:
        table_name: Name of the table/model to query
        filters: Dictionary of field:value pairs for filtering

    Returns:
        First matching record or None if not found
    """
    app = get_app()
    model_class = app.find_model_class(table_name)
    if not model_class:
        raise ValueError(f"Table '{table_name}' not found")

    with app.get_session() as session:
        query = session.query(model_class)

        # Apply filters
        for field, value in filters.items():
            if hasattr(model_class, field):
                query = query.filter(getattr(model_class, field) == value)
            else:
                raise ValueError(f"Field '{field}' not found in table '{table_name}'")

        # Return first match or None
        return query.first()


def json_to_model_types(data, table_name):
    """
    Convert JSON data types to appropriate SQLAlchemy model types,
    with special handling based on database type.

    Args:
        data: Dictionary of values from JSON
        table_name: Name of the table/model

    Returns:
        Dictionary with values converted to appropriate Python types
    """

    app = get_app()
    model_class = app.find_model_class(table_name)
    if not data or not model_class or not hasattr(model_class, '__table__'):
        return data

    # Skip conversion for databases that handle JSON conversion well
    if app.db_type.lower() in ('postgresql', 'mysql', 'mariadb'):
        return data

    result = data.copy()

    for column in model_class.__table__.columns:
        column_name = column.name

        # Skip if column not in data
        if column_name not in result:
            continue

        value = result[column_name]

        # Skip None values
        if value is None:
            continue

        # Get Python type for the column
        try:
            python_type = column.type.python_type
        except NotImplementedError:
            # Some SQLAlchemy types don't have a direct Python type
            continue

        # Convert based on target type
        if python_type == datetime.date and isinstance(value, str):
            try:
                result[column_name] = datetime.date.fromisoformat(value)
            except ValueError:
                pass
        elif python_type == datetime.datetime and isinstance(value, str):
            try:
                result[column_name] = datetime.datetime.fromisoformat(value)
            except ValueError:
                pass

    return result


def register_standard_handlers(pm) -> None:
    """
    Register standard merge handlers for common data patterns.

    This function sets up handlers for merging lists that contain items
    with a 'name' field, such as table columns or type columns.

    Args:
        pm: PluginsManager instance
    """

    def merge_by_name(base_list: List[Dict[str, Any]],
                      new_list: List[Dict[str, Any]],
                      plugin) -> List[Dict[str, Any]]:
        """
        Merge two lists of dictionaries by their 'name' field.

        When items with the same 'name' are found, the attributes from
        the new item are merged into the existing item, with new values
        overwriting old ones.

        Args:
            base_list: Base list to merge into
            new_list: New list to merge from
            plugin: Plugin providing the new items

        Returns:
            Merged list with combined items
        """
        result = []

        # Copy base list items
        for item in base_list:
            result.append(item.copy())

        # Process new list items
        for new_item in new_list:
            name = new_item.get('name')

            if not name:
                # If no name, just append
                new_copy = new_item.copy()
                if '_plugin' not in new_copy:
                    new_copy['_plugin'] = plugin
                result.append(new_copy)
                continue

            # Find existing item with same name
            existing = None
            for item in result:
                if item.get('name') == name:
                    existing = item
                    break

            if existing:
                # Merge attributes (new values overwrite old ones)
                coframe.deep_merge(existing, new_item)
                # Update plugin marker
                existing['_plugin'] = plugin
            else:
                # Add new item
                new_copy = new_item.copy()
                if '_plugin' not in new_copy:
                    new_copy['_plugin'] = plugin
                result.append(new_copy)

        return result

    # Register handlers for common patterns
    pm.register_merge_handler('tables.*.columns', merge_by_name)
    pm.register_merge_handler('types.*.columns', merge_by_name)

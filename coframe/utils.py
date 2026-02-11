import datetime
import logging
import logging.handlers
import importlib
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Sequence, Tuple
from io import StringIO
from sqlalchemy import inspection


def autoimport(file: str, package: str) -> None:
    """
    Automatically import all modules in the same directory of the package.

    Args:
        file: The file path of the package's __init__.py
        package: The package name to import modules from
    """
    package_dir = Path(file).resolve().parent

    for file in package_dir.glob("*.py"):
        if file.name == "__init__.py":
            continue
        module_name = file.stem
        module = importlib.import_module(f".{module_name}", package=package)
        globals()[module_name] = module


def deep_merge(a: Dict[str, Any], b: Dict[str, Any], path: Optional[List[str]] = None) -> None:
    """
    Merge dictionary b into dictionary a recursively.

    Args:
        a: Target dictionary to merge into
        b: Source dictionary to merge from
        path: Current path in the recursive merge process, used for tracking nested keys
    """
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                deep_merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass
            else:
                a[key] = b[key]
        else:
            a[key] = b[key]


def get_logger(name: str,
               handlers: Union[logging.Handler, Sequence[logging.Handler]] = logging.StreamHandler(),
               formatter: Optional[logging.Formatter] = None,
               level: int = logging.INFO) -> logging.Logger:
    """
    Create and configure a logger with specified handlers, formatter and level.

    Args:
        name: The name of the logger
        handlers: Single handler or sequence of handlers to attach to the logger
        formatter: Optional formatter to apply to all handlers
        level: The logging level to set (default: INFO)

    Returns:
        A configured logger object
    """
    logger = logging.getLogger(name)

    if not isinstance(handlers, (list, tuple)):
        handlers = [handlers]

    for handler in handlers:
        if formatter:
            handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)

    return logger


def set_formatter(logger: logging.Logger, new_format: str) -> Optional[str]:
    """
    Set a new formatter for all handlers in the logger and return the old format string if present.

    Args:
        logger: The logger to modify
        new_format: The new format string to apply

    Returns:
        The old format string if a formatter was present, None otherwise
    """
    new_formatter = logging.Formatter(new_format)
    old_format = None

    for handler in logger.handlers:
        formatter = handler.formatter
        if formatter:
            old_format = formatter._fmt
        handler.setFormatter(new_formatter)

    return old_format


def logging_to_file(logger: logging.Logger,
                    filename: Optional[str] = None,
                    preserve_format: bool = True) -> Tuple[List[logging.Handler], Optional[StringIO]]:
    """
    Redirect logging to a file or StringIO, preserving the current formatter if requested.

    Args:
        logger: The logger to modify
        filename: The filename to redirect to, or None to use StringIO
        preserve_format: Whether to preserve the current formatter format

    Returns:
        A tuple of (original_handlers, string_io) where string_io is only
        provided if filename is None
    """
    # Save original handlers
    original_handlers = logger.handlers.copy()

    # Get formatter from existing handlers if available and requested
    original_format = None
    if preserve_format and logger.handlers:
        for handler in logger.handlers:
            if handler.formatter:
                original_format = handler.formatter._fmt
                break

    # Clear existing handlers
    logger.handlers = []

    if filename:
        # Create file handler
        file_handler = logging.FileHandler(filename, mode='w')
        if original_format:
            file_handler.setFormatter(logging.Formatter(original_format))
        logger.addHandler(file_handler)
        return original_handlers, None
    else:
        # Use StringIO for capturing
        string_io = StringIO()
        string_handler = logging.StreamHandler(string_io)
        if original_format:
            string_handler.setFormatter(logging.Formatter(original_format))
        logger.addHandler(string_handler)
        return original_handlers, string_io


def restore_logging(logger: logging.Logger, original_handlers: List[logging.Handler]) -> None:
    """
    Restore original handlers to a logger.

    Args:
        logger: The logger to restore
        original_handlers: The original handlers to restore
    """
    logger.handlers = original_handlers


def get_app():
    """
    Get the current DB application instance.

    This function centralizes access to the application instance,
    making it easier to change the implementation if needed.

    Returns:
        The current DB application instance
    """
    # Import here to avoid circular dependency
    from coframe.db import Base
    return Base.__coframe_app__


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
                deep_merge(existing, new_item)
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

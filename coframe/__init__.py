import logging
import importlib
import logging.handlers
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Sequence


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


autoimport(__file__, __package__)


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

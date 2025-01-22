import logging
import importlib
import logging.handlers
from pathlib import Path


def autoimport(file, package):
    """Automatically import all modules in the same directory of the package."""
    package_dir = Path(file).resolve().parent

    for file in package_dir.glob("*.py"):
        if file.name == "__init__.py":
            continue
        module_name = file.stem
        module = importlib.import_module(f".{module_name}", package=package)
        globals()[module_name] = module


autoimport(__file__, __package__)


def get_logger(name: str,
               handlers: logging.handlers = logging.StreamHandler(),
               formatter: logging.Formatter = None,
               level: int = logging.INFO):
    """
    Return an easy and convenient logger with all needed.

    Args:
        name: the name of logger
        handlers: optional handlers
        formatter: optional formatter
        level: the default logging level

    Returns:
        an logger object.
    """

    logger = logging.getLogger(name)

    if not issubclass(type(handlers), (list, tuple)):
        handlers = [handlers]

    for handler in handlers:
        if formatter:
            handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)

    return logger


def set_formatter(logger: logging.Logger, new_format: str) -> str | None:
    """
    Set a new formatter for the logger and return the old formatter string if
    it is present

    Args:
        logger: the logger
        new_format: the new format string

    Returns:
        the old formatter, if present
    """
    new_formatter = logging.Formatter(new_format)
    for handler in logger.handlers:
        formatter = handler.formatter
        handler.setFormatter(new_formatter)
    if formatter:
        return formatter._fmt
